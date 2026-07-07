import {
	applyChangeset,
	type Changeset,
	type SyncChange,
} from "./changeset.ts";
import type { SyncNotification } from "./notifications.ts";
import type {
	Authority,
	ChangesetEntry,
	SyncMember,
	SyncTransport,
} from "./types.ts";
import { outbox } from "./outbox.ts";

// Changesets larger than this are "bulk" (e.g. a simulation, which mutates
// hundreds of records). They're only published by the host, and are split into
// chunks so each fits in one Firestore doc.
const MAX_SYNC_CHANGES = 200;

// Each chunk stays well under Firestore's 1 MB/doc limit - capped by record
// count and by serialized size (whichever hits first).
const MAX_CHUNK_RECORDS = 100;
const MAX_CHUNK_BYTES = 700_000;

const makeId = (): string => {
	if (typeof crypto !== "undefined" && crypto.randomUUID) {
		return crypto.randomUUID();
	}
	// Fallback for environments without crypto.randomUUID.
	return `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
};

// Draft actions are turn-based - the game only enables the pick for whoever is
// on the clock - so their changesets may broadcast from ANY device, not just
// the wheel-holder. (Everything else that mutates in bulk is a sim and stays
// wheel-holder-only.) Matches an action label like "main.draftUser" or
// "playMenu.untilYourNextPick".
const isDraftAction = (action: string): boolean => {
	const name = action.includes(".")
		? action.slice(action.indexOf(".") + 1)
		: action;
	return (
		name === "draftUser" ||
		name === "onePick" ||
		name === "untilYourNextPick" ||
		name === "untilEnd"
	);
};

const chunkChanges = (changes: SyncChange[]): SyncChange[][] => {
	const chunks: SyncChange[][] = [];
	let current: SyncChange[] = [];
	let bytes = 0;

	for (const change of changes) {
		const size = JSON.stringify(change).length;
		if (
			current.length > 0 &&
			(current.length >= MAX_CHUNK_RECORDS || bytes + size > MAX_CHUNK_BYTES)
		) {
			chunks.push(current);
			current = [];
			bytes = 0;
		}
		current.push(change);
		bytes += size;
	}
	if (current.length > 0) {
		chunks.push(current);
	}
	return chunks;
};

// Connects the local change-capture layer to a transport (Firebase or a fake).
// - Local actions → onLocalChangeset() → publish to the shared log.
// - Remote entries → handleEntry() → applyChangeset() into the local cache.
// Small changes go as one entry; bulk changes (sims) are host-only and chunked.
// It ignores its own entries and dedups by entry id, so echoes/replays are safe.
export class SyncEngine {
	private transport: SyncTransport;

	// Who currently holds "the wheel" (may advance the league). Kept in sync with
	// the shared control doc via subscribeAuthority. Undefined until someone
	// claims it. This device is the authority when authority.holderId === our id.
	private authority: Authority | undefined;

	// If the user chose "take the wheel" when connecting, claim it on start.
	private claimOnStart: boolean;

	private onAuthorityChange:
		| ((authority: Authority | undefined) => void)
		| undefined;

	private authorityUnsubscribe: (() => void) | undefined;

	// This device's display name, used as the author of push notifications
	// ("Alex completed a trade") and as the wheel-holder's name. Set when push is
	// enabled.
	localName = "A league-mate";

	private onWatermark: ((seq: number) => void) | undefined;

	private unsubscribe: (() => void) | undefined;

	// Entry ids we've produced or applied - prevents re-applying (and thus
	// re-broadcasting) the same change.
	private seen = new Set<string>();

	// Buffers incoming bulk chunks until a whole batch has arrived.
	private pendingBatches = new Map<
		string,
		{ count: number; chunks: Map<number, SyncChange[]> }
	>();

	// Highest entry seq (server-timestamp) we've seen, and the highest we've
	// persisted as the catch-up watermark.
	private maxSeq: number;

	private persistedSeq: number;

	// Set when a remote changeset failed to apply. While set, we STOP advancing
	// the persisted watermark, so a reconnect re-fetches (and re-applies) from
	// before the failure instead of skipping it forever. Cleared by a full
	// resync, which re-applies the whole log from scratch.
	private applyFailed = false;

	// The room code, used to scope the durable outbox (undefined = don't use the
	// outbox, e.g. the in-memory test transport).
	private code: string | undefined;

	// Reports live upload progress while (re)publishing, so the UI can show a
	// "keep the app open" indicator with a real count. undefined = idle.
	private onUploadProgress:
		| ((progress: { done: number; total: number } | undefined) => void)
		| undefined;

	constructor(
		transport: SyncTransport,
		options: {
			isHost?: boolean;
			initialWatermark?: number;
			onWatermark?: (seq: number) => void;
			onAuthorityChange?: (authority: Authority | undefined) => void;
			code?: string;
			onUploadProgress?: (
				progress: { done: number; total: number } | undefined,
			) => void;
		} = {},
	) {
		this.transport = transport;
		this.claimOnStart = options.isHost ?? false;
		this.onWatermark = options.onWatermark;
		this.onAuthorityChange = options.onAuthorityChange;
		this.maxSeq = options.initialWatermark ?? 0;
		this.persistedSeq = options.initialWatermark ?? 0;
		this.code = options.code;
		this.onUploadProgress = options.onUploadProgress;
	}

	get clientId(): string {
		return this.transport.clientId;
	}

	// Does THIS device currently hold the wheel (i.e. may it advance the league)?
	isAuthority(): boolean {
		return (
			this.authority !== undefined &&
			this.authority.holderId === this.transport.clientId
		);
	}

	getAuthority(): Authority | undefined {
		return this.authority;
	}

	// Back-compat alias: "host" now means "current wheel-holder". Used by the
	// notification builder to decide who narrates a sim.
	getIsHost(): boolean {
		return this.isAuthority();
	}

	// Claim the wheel for this device. Optimistically flips local state so
	// advancing unlocks immediately; the shared-doc subscription then confirms
	// (and would correct us if someone claimed at the same instant).
	async claimAuthority(): Promise<void> {
		const holder: Authority = {
			holderId: this.transport.clientId,
			holderName: this.localName,
		};
		this.authority = holder;
		this.onAuthorityChange?.(holder);
		await this.transport.claimAuthority?.(holder.holderId, holder.holderName);
	}

	// Register this device for push in the room (records its FCM token). No-op if
	// the transport doesn't support push (e.g. the in-memory test transport).
	async registerMember(member: SyncMember) {
		this.localName = member.name || this.localName;
		await this.transport.registerMember?.(this.transport.clientId, member);
	}

	// Enqueue a push notification for the other devices in the room.
	async publishNotification(notification: SyncNotification) {
		await this.transport.publishNotification?.({
			...notification,
			authorId: this.transport.clientId,
			authorName: this.localName,
		});
	}

	start() {
		if (this.unsubscribe) {
			return;
		}
		this.unsubscribe = this.transport.subscribe({
			onEntry: (entry) => this.handleEntry(entry),
			onBatchProcessed: () => this.advanceWatermark(),
		});

		// Watch who holds the wheel, so every device agrees on who may advance.
		this.authorityUnsubscribe = this.transport.subscribeAuthority?.(
			(authority) => {
				this.authority = authority;
				this.onAuthorityChange?.(authority);
			},
		);

		// If the user chose to take the wheel on connect, claim it now.
		if (this.claimOnStart) {
			void this.claimAuthority();
		}
	}

	// Persist the watermark only when there are no half-received bulk batches, so
	// a reconnect never skips past chunks it hasn't fully applied. (Whole-record
	// applies are idempotent, so re-fetching a bit on reconnect is harmless.)
	private advanceWatermark() {
		// Don't skip past a half-received bulk batch, or past a changeset that
		// failed to apply - either would silently and permanently drop data.
		if (this.pendingBatches.size > 0 || this.applyFailed) {
			return;
		}
		if (this.maxSeq > this.persistedSeq) {
			this.persistedSeq = this.maxSeq;
			this.onWatermark?.(this.maxSeq);
		}
	}

	stop() {
		this.unsubscribe?.();
		this.unsubscribe = undefined;
		this.authorityUnsubscribe?.();
		this.authorityUnsubscribe = undefined;
	}

	// Publish a changeset produced by a local action.
	async onLocalChangeset(changeset: Changeset, action: string) {
		if (changeset.changes.length === 0) {
			return;
		}

		// Send as a single doc only if it fits in one Firestore doc by BOTH record
		// count AND serialized size. A changeset can be <= MAX_SYNC_CHANGES records
		// yet still blow past Firestore's ~1 MB/doc limit - e.g. advancing to free
		// agency turns over many large player records at once. A single addDoc then
		// throws and the change silently never reaches the room (while a phone push
		// still goes out, so it *looks* like it synced). When it doesn't fit, fall
		// through to the chunked bulk path, which byte-caps every chunk.
		const fitsInOneDoc =
			changeset.changes.length <= MAX_SYNC_CHANGES &&
			JSON.stringify(changeset).length <= MAX_CHUNK_BYTES;

		if (fitsInOneDoc) {
			const id = makeId();
			this.seen.add(id);
			await this.publishEntry({
				id,
				authorId: this.transport.clientId,
				action,
				changeset,
			});
			return;
		}

		// Bulk change (e.g. a sim, a phase advance, or a big draft advance).
		// Normally only the wheel-holder broadcasts these. Draft actions are exempt:
		// whoever is on the clock drafts their own pick, so their (possibly large)
		// draft changeset must sync from any device.
		if (!this.isAuthority() && !isDraftAction(action)) {
			console.warn(
				`[sync] Skipping bulk change from "${action}" (${changeset.changes.length} records) - only the wheel-holder broadcasts sims.`,
			);
			return;
		}

		await this.publishBulk(changeset, action);
	}

	private async publishBulk(changeset: Changeset, action: string) {
		const chunks = chunkChanges(changeset.changes);
		const batchId = makeId();

		this.onUploadProgress?.({ done: 0, total: chunks.length });
		try {
			for (let i = 0; i < chunks.length; i++) {
				const id = makeId();
				this.seen.add(id);
				await this.publishEntry({
					id,
					authorId: this.transport.clientId,
					action,
					batchId,
					chunkIndex: i,
					chunkCount: chunks.length,
					changeset: { changes: chunks[i]! },
				});
				this.onUploadProgress?.({ done: i + 1, total: chunks.length });
			}
		} finally {
			this.onUploadProgress?.(undefined);
		}
	}

	// Publish one entry through the durable outbox: record it first, so an upload
	// interrupted by the tab closing mid-send is retried on the next launch, then
	// remove it once Firestore confirms. Outbox ops are best-effort (they never
	// break the publish); on the test transport (no code) they're skipped.
	private async publishEntry(entry: Omit<ChangesetEntry, "seq">) {
		if (this.code !== undefined) {
			await outbox.add(this.code, entry);
		}
		await this.transport.publish(entry);
		if (this.code !== undefined) {
			await outbox.remove(entry.id);
		}
	}

	// Re-send anything a previous session produced but never confirmed uploaded
	// (interrupted mid-publish - e.g. the app was closed before a chunked season
	// rollover finished). Called on connect so a stranded batch completes itself.
	// Safe to re-send: entries carry a stable id (and chunk metadata), so the
	// receiver dedups; we re-add to `seen` so we don't re-apply our own echo.
	async flushOutbox() {
		if (this.code === undefined) {
			return;
		}
		const pending = await outbox.pending(this.code);
		if (pending.length === 0) {
			return;
		}
		this.onUploadProgress?.({ done: 0, total: pending.length });
		try {
			for (let i = 0; i < pending.length; i++) {
				const entry = pending[i]!;
				this.seen.add(entry.id);
				try {
					await this.transport.publish(entry);
					await outbox.remove(entry.id);
				} catch (error) {
					// Leave it queued for the next flush rather than dropping it.
					console.error("[sync] outbox flush: re-publish failed", error);
				}
				this.onUploadProgress?.({ done: i + 1, total: pending.length });
			}
		} finally {
			this.onUploadProgress?.(undefined);
		}
	}

	// Apply an entry from the shared log. Returns whether it was applied (false
	// if it was our own, a duplicate, or a not-yet-complete bulk chunk). Never
	// throws.
	async handleEntry(entry: ChangesetEntry): Promise<boolean> {
		// Track ordering position for the watermark even for entries we skip
		// (our own / already-seen) - they're still "caught up" past.
		if (entry.seq > this.maxSeq) {
			this.maxSeq = entry.seq;
		}

		if (entry.authorId === this.transport.clientId) {
			return false;
		}
		if (this.seen.has(entry.id)) {
			return false;
		}
		this.seen.add(entry.id);

		if (
			entry.batchId !== undefined &&
			entry.chunkIndex !== undefined &&
			entry.chunkCount !== undefined
		) {
			return this.handleChunk(
				entry.batchId,
				entry.chunkIndex,
				entry.chunkCount,
				entry.changeset,
			);
		}

		return this.apply(entry.changeset);
	}

	private async handleChunk(
		batchId: string,
		chunkIndex: number,
		chunkCount: number,
		changeset: Changeset,
	): Promise<boolean> {
		let batch = this.pendingBatches.get(batchId);
		if (!batch) {
			batch = { count: chunkCount, chunks: new Map() };
			this.pendingBatches.set(batchId, batch);
		}
		batch.chunks.set(chunkIndex, changeset.changes);

		if (batch.chunks.size < batch.count) {
			// Still waiting for the rest of the batch.
			return false;
		}

		this.pendingBatches.delete(batchId);

		const changes: SyncChange[] = [];
		for (let i = 0; i < batch.count; i++) {
			const chunk = batch.chunks.get(i);
			if (chunk) {
				changes.push(...chunk);
			}
		}

		return this.apply({ changes });
	}

	private async apply(changeset: Changeset): Promise<boolean> {
		try {
			await applyChangeset(changeset);
		} catch (error) {
			console.error("Failed to apply remote changeset", error);
			// Blocks the watermark from advancing past this entry, so it's retried on
			// the next reconnect instead of skipped. We deliberately do NOT kick off a
			// full-log resync here - re-reading and re-applying the entire history on
			// every hiccup is brutally slow on a phone. Recovery paths: the live
			// subscription, the periodic catchUp(), or the manual "Force full resync".
			this.applyFailed = true;
			return false;
		}
		return true;
	}

	// A cheap, targeted catch-up: fetch only the entries AFTER our watermark and
	// run them through the normal handler. Cheap because it's usually empty or a
	// handful of entries (not the whole log), and it doesn't rely on Firestore's
	// real-time push - which stalls badly on a throttled phone, so a big change
	// could take many minutes to arrive on its own. Safe to call on a timer / when
	// the app regains focus. Completes a batch that was still missing a chunk, and
	// applies anything the live subscription hasn't delivered yet.
	async catchUp(): Promise<void> {
		if (!this.transport.fetchEntriesSince) {
			return;
		}
		let entries: ChangesetEntry[];
		try {
			entries = await this.transport.fetchEntriesSince(this.persistedSeq);
		} catch {
			return;
		}
		for (const entry of entries.sort((a, b) => a.seq - b.seq)) {
			await this.handleEntry(entry);
		}
		this.advanceWatermark();
	}

	// The watermark we've durably caught up through (server-timestamp millis).
	getPersistedSeq(): number {
		return this.persistedSeq;
	}

	// Read the entire shared change log once (not a live subscription), for the
	// sync-activity panel and for a full resync. Empty if the transport can't.
	async fetchLog(): Promise<ChangesetEntry[]> {
		const entries = (await this.transport.fetchAllEntries?.()) ?? [];
		return entries.sort((a, b) => a.seq - b.seq);
	}

	// Force a full catch-up: re-read the WHOLE log and re-apply every entry from
	// the beginning, in order. Safe to run anytime because every change is a
	// whole-record write (idempotent) - applying old-then-new in timestamp order
	// always lands on the current shared state. This is the manual recovery for a
	// device that silently diverged (e.g. an apply that failed and got skipped).
	// Own entries are re-applied too; they just rewrite our own latest values.
	async resyncAll(): Promise<{
		total: number;
		applied: number;
		incomplete: number;
		failed: boolean;
	}> {
		const entries = await this.fetchLog();

		// Start clean so nothing is deduped away and no half-batch lingers.
		this.pendingBatches.clear();
		this.applyFailed = false;

		let applied = 0;
		let newMaxSeq = this.maxSeq;
		for (const entry of entries) {
			if (entry.seq > newMaxSeq) {
				newMaxSeq = entry.seq;
			}
			this.seen.add(entry.id);

			const ok =
				entry.batchId !== undefined &&
				entry.chunkIndex !== undefined &&
				entry.chunkCount !== undefined
					? await this.handleChunk(
							entry.batchId,
							entry.chunkIndex,
							entry.chunkCount,
							entry.changeset,
						)
					: await this.apply(entry.changeset);
			if (ok) {
				applied++;
			}
		}

		// Batches still missing a chunk after reading the WHOLE log: those chunks
		// genuinely aren't in the cloud (a publish that never finished), so this
		// device can't catch up from the log at all.
		const incomplete = this.pendingBatches.size;
		this.pendingBatches.clear();
		this.maxSeq = Math.max(this.maxSeq, newMaxSeq);

		// Only bank the watermark if EVERYTHING re-applied cleanly. If any entry
		// failed to apply OR a batch is missing a chunk, leave the watermark where
		// it was - never skip past unapplied data and silently diverge. A fully
		// clean pass also resets the auto-resync budget.
		if (!this.applyFailed && incomplete === 0) {
			if (newMaxSeq > this.persistedSeq) {
				this.persistedSeq = newMaxSeq;
				this.onWatermark?.(this.persistedSeq);
			}
		}

		return {
			total: entries.length,
			applied,
			incomplete,
			failed: this.applyFailed,
		};
	}
}
