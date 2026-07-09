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
import { shouldTraceSyncLabel, syncDebugLog } from "./debugLog.ts";

// Changesets larger than this are "bulk" (e.g. a simulation, which mutates
// hundreds of records). They're only published by the host, and are split into
// chunks so each fits in one Firestore doc.
const MAX_SYNC_CHANGES = 200;

// Backlog drain paging. Entries can be up to ~700 KB (a bulk-sim chunk), so a
// page of this many is a few MB per fetch - enough to make real progress, small
// enough not to time out / exhaust memory on a phone. MAX_PAGES bounds one
// catchUp() call so it can't spin forever while the head keeps moving; the timer
// resumes on the next tick from the banked watermark.
const CATCH_UP_PAGE_SIZE = 25;
const CATCH_UP_MAX_PAGES = 40;

// Only surface the "catching up …%" indicator once the backlog is at least this
// many entries behind - a handful of missed changes catches up near-instantly
// and shouldn't flash a progress bar.
const CATCH_UP_PROGRESS_MIN = 30;

// How long a "room is advancing" lease lasts. Generous enough to cover a single
// day's sim + upload even on a slow phone; it's re-stamped when a bulk upload
// starts and cleared as soon as the advance is published, so this only actually
// matters as a crash-recovery ceiling (a simmer that dies mid-sim unblocks the
// room after this).
const ROOM_BUSY_LEASE_MS = 45_000;

// Each chunk stays well under Firestore's 1 MB/doc limit - capped by record
// count and by serialized size (whichever hits first).
const MAX_CHUNK_RECORDS = 100;
const MAX_CHUNK_BYTES = 700_000;
const READY_TTL = 10_000;
const READY_TIMEOUT = 5_000;

const makeId = (): string => {
	if (typeof crypto !== "undefined" && crypto.randomUUID) {
		return crypto.randomUUID();
	}
	// Fallback for environments without crypto.randomUUID.
	return `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
};

// Draft actions are turn-based - the game only enables the pick for whoever is
// on the clock - so their changesets may broadcast from ANY device, not just
// the sim authority. (Everything else that mutates in bulk is a sim and stays
// sim authority-only.) Matches an action label like "main.draftUser" or
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

	// Who currently holds sim authority (may advance the league). Kept in sync with
	// the shared control doc via subscribeAuthority. Undefined until someone
	// claims it. This device is the authority when authority.holderId === our id.
	private authority: Authority | undefined;

	// If the user chose "sim here" when connecting, claim it on start.
	private claimOnStart: boolean;

	private onAuthorityChange:
		| ((authority: Authority | undefined) => void)
		| undefined;

	private onReadyChange: ((ready: boolean) => void) | undefined;

	private authorityUnsubscribe: (() => void) | undefined;

	// This device's display name, used as the author of push notifications
	// ("Alex completed a trade") and as the sim authority's name. Set when push is
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

	// Guards catchUp() against reentrancy, so the connect-time drain and the
	// periodic timer can't fetch the same pages at once.
	private catchingUp = false;

	// The room code, used to scope the durable outbox (undefined = don't use the
	// outbox, e.g. the in-memory test transport).
	private code: string | undefined;

	// Reports live upload progress while (re)publishing, so the UI can show a
	// "keep the app open" indicator with a real count. undefined = idle.
	private onUploadProgress:
		| ((progress: { done: number; total: number } | undefined) => void)
		| undefined;

	// Fires once a local change is CONFIRMED uploaded, so the UI can flash a brief
	// "synced ✓" - on any device, for any change (a trade/signing, not just a sim).
	private onUploadComplete: (() => void) | undefined;

	// Reports backlog-drain progress while catching up after an absence (entries
	// applied / total). undefined = caught up / trivial gap.
	private onCatchUpProgress:
		| ((progress: { done: number; total: number } | undefined) => void)
		| undefined;

	// Live drain progress: total entries to apply this drain (undefined = not
	// showing progress), and how many we've applied so far. Persist across the
	// multiple catchUp() calls a big drain spans.
	private catchUpTotal: number | undefined;
	private catchUpDone = 0;

	private readyUntil = 0;

	private readyProbe: Promise<void> | undefined;

	private listenerHealthy = true;

	private authorityListenerHealthy = true;

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
			onUploadComplete?: () => void;
			onCatchUpProgress?: (
				progress: { done: number; total: number } | undefined,
			) => void;
			onReadyChange?: (ready: boolean) => void;
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
		this.onUploadComplete = options.onUploadComplete;
		this.onCatchUpProgress = options.onCatchUpProgress;
		this.onReadyChange = options.onReadyChange;
	}

	get clientId(): string {
		return this.transport.clientId;
	}

	// Is THIS device currently in charge of simming (i.e. may it advance the league)?
	isAuthority(): boolean {
		return (
			this.authority !== undefined &&
			this.authority.holderId === this.transport.clientId
		);
	}

	getAuthority(): Authority | undefined {
		return this.authority;
	}

	// Mark the room "actively advancing" for a lease window, so followers hold off
	// on conflict-prone edits while a sim/phase/draft is running or still
	// uploading (before it shows up in the change log). Only the sim authority may
	// write this. Fire-and-forget - it must never add latency to a sim.
	markRoomBusy(): void {
		if (!this.isAuthority()) {
			return;
		}
		void this.transport.publishBusy?.(Date.now() + ROOM_BUSY_LEASE_MS);
	}

	// Release the "actively advancing" lease once the advance has been published
	// (its seq is now visible to followers, so the caught-up check takes over).
	clearRoomBusy(): void {
		if (!this.isAuthority()) {
			return;
		}
		void this.transport.publishBusy?.(0);
	}

	// Is SOMEONE ELSE mid-advance right now? True only for followers - the
	// sim authority is the one doing the advancing, so it never blocks itself.
	isRoomBusy(): boolean {
		if (this.isAuthority()) {
			return false;
		}
		const busyUntil = this.authority?.busyUntil;
		return busyUntil !== undefined && Date.now() < busyUntil;
	}

	// Has this device applied everything it has seen from the log? False while a
	// bulk sim is mid-transfer, an apply failed, or newer entries haven't been
	// applied yet - i.e. while acting now would be acting on a stale world.
	isCaughtUp(): boolean {
		return (
			this.pendingBatches.size === 0 &&
			!this.applyFailed &&
			this.persistedSeq >= this.maxSeq
		);
	}

	private markNotReady() {
		const wasReady = this.isReady();
		this.readyUntil = 0;
		if (wasReady) {
			this.onReadyChange?.(false);
		}
	}

	isReady(): boolean {
		return (
			this.listenerHealthy &&
			this.authorityListenerHealthy &&
			Date.now() < this.readyUntil
		);
	}

	private async withReadyTimeout(promise: Promise<void>) {
		let timeoutID: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				promise,
				new Promise<void>((_resolve, reject) => {
					timeoutID = setTimeout(() => {
						reject(new Error("Cloud sync readiness check timed out."));
					}, READY_TIMEOUT);
				}),
			]);
		} finally {
			if (timeoutID !== undefined) {
				clearTimeout(timeoutID);
			}
		}
	}

	async ensureReady(force = false): Promise<void> {
		if (!this.listenerHealthy || !this.authorityListenerHealthy) {
			this.markNotReady();
			throw new Error("Cloud sync listeners are not ready.");
		}
		if (!force && Date.now() < this.readyUntil) {
			return;
		}
		if (this.readyProbe) {
			return this.readyProbe;
		}

		this.readyProbe = (async () => {
			try {
				if (this.transport.ping) {
					await this.withReadyTimeout(this.transport.ping());
				}
				const wasReady = this.isReady();
				this.readyUntil = Date.now() + READY_TTL;
				if (!wasReady && this.isReady()) {
					this.onReadyChange?.(true);
				}
			} catch (error) {
				this.markNotReady();
				throw error;
			}
		})().finally(() => {
			this.readyProbe = undefined;
		});

		return this.readyProbe;
	}

	// Back-compat alias: "host" now means "current sim authority". Used by the
	// notification builder to decide who narrates a sim.
	getIsHost(): boolean {
		return this.isAuthority();
	}

	// Claim sim authority for this device. Optimistically flips local state so
	// advancing unlocks immediately; the shared-doc subscription then confirms
	// (and would correct us if someone claimed at the same instant).
	async claimAuthority(): Promise<void> {
		const holder: Authority = {
			holderId: this.transport.clientId,
			holderName: this.localName,
		};
		await this.ensureReady();
		try {
			await this.transport.claimAuthority?.(holder.holderId, holder.holderName);
		} catch (error) {
			this.markNotReady();
			throw error;
		}
		this.authority = holder;
		this.onAuthorityChange?.(holder);
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
		// Watch who is in charge of simming, so every device agrees on who may advance.
		// This is tiny (one doc) and needed immediately, so it always starts now -
		// unlike the changes subscription, which waits until the backlog is drained
		// (see startChangesSubscription) so its initial snapshot isn't the whole log.
		if (this.authorityUnsubscribe === undefined) {
			this.authorityUnsubscribe = this.transport.subscribeAuthority?.(
				(authority) => {
					this.authority = authority;
					this.authorityListenerHealthy = true;
					this.onAuthorityChange?.(authority);
				},
				() => {
					const wasReady = this.isReady();
					this.authorityListenerHealthy = false;
					this.authority = undefined;
					this.readyUntil = 0;
					this.onAuthorityChange?.(undefined);
					if (wasReady) {
						this.onReadyChange?.(false);
					}
				},
			);
		}

		// If the user chose to sim here on connect, claim it now.
		if (this.claimOnStart) {
			void this.claimAuthority().catch(() => undefined);
		}
	}

	// Start the live changes subscription. Deferred until the caller has drained
	// the backlog via catchUp() and moved the transport watermark to the head, so
	// the real-time listener's initial snapshot is just the live tail rather than
	// a re-load of everything we just caught up on.
	startChangesSubscription() {
		if (this.unsubscribe) {
			return;
		}
		this.unsubscribe = this.transport.subscribe({
			onEntry: (entry) => this.handleEntry(entry),
			onError: () => {
				const wasReady = this.isReady();
				this.listenerHealthy = false;
				this.readyUntil = 0;
				if (wasReady) {
					this.onReadyChange?.(false);
				}
			},
			onBatchProcessed: () => this.advanceWatermark(),
		});
		this.listenerHealthy = true;
	}

	// Has the live changes subscription been started yet?
	hasChangesSubscription(): boolean {
		return this.unsubscribe !== undefined;
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
		this.listenerHealthy = false;
		this.authorityListenerHealthy = false;
		this.markNotReady();
	}

	// Publish a changeset produced by a local action.
	async onLocalChangeset(changeset: Changeset, action: string) {
		if (changeset.changes.length === 0) {
			return;
		}
		const trace = shouldTraceSyncLabel(action);
		if (trace) {
			syncDebugLog("engine:onLocalChangeset-start", {
				action,
				records: changeset.changes.length,
				isAuthority: this.isAuthority(),
				authorityHolderId: this.authority?.holderId,
				clientId: this.transport.clientId,
				isCaughtUp: this.isCaughtUp(),
				isReady: this.isReady(),
			});
		}

		try {
			await this.ensureReady();
			if (trace) {
				syncDebugLog("engine:ensureReady-ok", {
					action,
					isReady: this.isReady(),
				});
			}
		} catch (error) {
			if (trace) {
				syncDebugLog("engine:ensureReady-failed", { action, error });
			}
			throw error;
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
			if (trace) {
				syncDebugLog("engine:single-entry-publish-start", {
					action,
					id,
					records: changeset.changes.length,
				});
			}
			// Show the same cloud indicator as a sim (total 1), so a plain
			// transaction on any device gets upload feedback + a confirm tick.
			this.onUploadProgress?.({ done: 0, total: 1 });
			try {
				await this.publishEntry({
					id,
					authorId: this.transport.clientId,
					action,
					changeset,
				});
				this.onUploadComplete?.();
				if (trace) {
					syncDebugLog("engine:single-entry-publish-confirmed", {
						action,
						id,
						records: changeset.changes.length,
					});
				}
			} catch (error) {
				this.markNotReady();
				if (trace) {
					syncDebugLog("engine:single-entry-publish-failed", {
						action,
						id,
						error,
					});
				}
				throw error;
			} finally {
				this.onUploadProgress?.(undefined);
			}
			return;
		}

		// Bulk change (e.g. a sim, a phase advance, or a big draft advance).
		// The worker action guard is responsible for preventing a follower from
		// starting these. Once a local bulk mutation exists, we must publish it (or
		// throw and restore it for retry) rather than silently dropping it. A transient
		// authority-listener blip after a single/live game can otherwise make the
		// next day sim look exactly like a local-only file: no upload progress, no
		// cloud entry, and the drained changeset is gone.
		if (!this.isAuthority() && !isDraftAction(action)) {
			console.warn(
				`[sync] Publishing bulk change from "${action}" (${changeset.changes.length} records) even though local sim authority is not currently confirmed.`,
			);
			syncDebugLog("engine:bulk-authority-not-confirmed", {
				action,
				records: changeset.changes.length,
				authorityHolderId: this.authority?.holderId,
				clientId: this.transport.clientId,
			});
		}

		await this.publishBulk(changeset, action);
	}

	private async publishBulk(changeset: Changeset, action: string) {
		const chunks = chunkChanges(changeset.changes);
		const batchId = makeId();
		const trace = shouldTraceSyncLabel(action);
		if (trace) {
			syncDebugLog("engine:bulk-publish-start", {
				action,
				batchId,
				records: changeset.changes.length,
				chunks: chunks.length,
				isAuthority: this.isAuthority(),
			});
		}

		// Re-stamp the busy lease now that the (possibly long) upload is starting,
		// so it can't expire mid-transfer and let a follower slip an edit into the
		// tail of the upload window.
		this.markRoomBusy();

		// Build every chunk entry and queue them ALL in the durable outbox BEFORE
		// uploading any. This is what guarantees a sim's delta can never be half-sent
		// or lost: if the upload is interrupted or fails partway (the connection
		// drops mid-sim, the tab is closed, a chunk write errors), every
		// not-yet-confirmed chunk still sits in the outbox, and flushOutbox re-sends
		// it on the next tick / on reconnect. Entries carry stable ids + batchId +
		// chunkIndex/chunkCount, so receivers dedup and reassemble the full batch.
		// Previously each chunk was only queued right before its own upload, so a
		// failure partway left the LATER chunks never queued - lost - which stranded
		// every follower on an incomplete batch. (No-op without a room code - the
		// in-memory test transport.)
		const entries: Omit<ChangesetEntry, "seq">[] = chunks.map((chunk, i) => ({
			id: makeId(),
			authorId: this.transport.clientId,
			action,
			batchId,
			chunkIndex: i,
			chunkCount: chunks.length,
			changeset: { changes: chunk },
		}));
		for (const entry of entries) {
			this.seen.add(entry.id);
			if (this.code !== undefined) {
				await outbox.add(this.code, entry);
			}
		}
		if (trace) {
			syncDebugLog("engine:bulk-outbox-queued", {
				action,
				batchId,
				chunks: entries.length,
				hasOutbox: this.code !== undefined,
			});
		}

		this.onUploadProgress?.({ done: 0, total: entries.length });
		try {
			for (let i = 0; i < entries.length; i++) {
				if (trace) {
					syncDebugLog("engine:bulk-chunk-publish-start", {
						action,
						batchId,
						chunkIndex: i,
						chunkCount: entries.length,
						entryId: entries[i]!.id,
						records: entries[i]!.changeset.changes.length,
					});
				}
				await this.publishEntry(entries[i]!);
				this.onUploadProgress?.({ done: i + 1, total: entries.length });
				if (trace) {
					syncDebugLog("engine:bulk-chunk-publish-confirmed", {
						action,
						batchId,
						chunkIndex: i,
						chunkCount: entries.length,
						entryId: entries[i]!.id,
					});
				}
			}
			this.onUploadComplete?.();
			if (trace) {
				syncDebugLog("engine:bulk-publish-confirmed", {
					action,
					batchId,
					chunks: entries.length,
				});
			}
		} catch (error) {
			this.markNotReady();
			if (trace) {
				syncDebugLog("engine:bulk-publish-failed", {
					action,
					batchId,
					error,
				});
			}
			throw error;
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
		let pending: Omit<ChangesetEntry, "seq">[];
		try {
			pending = await outbox.pending(this.code);
		} catch (error) {
			this.markNotReady();
			console.error(
				"[sync] outbox flush: could not read pending uploads",
				error,
			);
			return;
		}
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
					this.markNotReady();
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

	// Drain everything after our watermark, PAGE BY PAGE, and bank the durable
	// watermark after each page. This is the workhorse that lets a device which has
	// been away for weeks actually catch up: pulling the whole backlog in one query
	// times out / runs out of memory on a phone and makes zero progress, so it
	// never converges. Paging fixes both - bounded memory per fetch, and durable
	// progress banked per page so an interruption resumes instead of restarting.
	//
	// A `fetchCursor` walks forward through the backlog independent of the durable
	// watermark: whole-record bulk sims are chunked across many entries, so the
	// durable watermark can't advance until a batch is fully assembled (which may
	// straddle several pages), but we must keep fetching past it to gather the rest
	// of the batch. The watermark still only advances over fully-applied,
	// gap-free history (advanceWatermark guards that), so resume-after-interrupt
	// stays correct.
	//
	// Reentrancy-guarded so the connect-time drain and the periodic timer don't
	// double-fetch. Returns true when it has drained all the way to the head this
	// pass (so the caller may safely start the live subscription from there);
	// false if a fetch failed, an apply failed, or it stopped early with more to
	// go - in which case the next tick resumes from the banked watermark.
	async catchUp(): Promise<boolean> {
		if (!this.transport.fetchEntriesSince || this.catchingUp) {
			return false;
		}
		this.catchingUp = true;
		try {
			// On a fresh drain (not one already in progress), measure how far behind
			// we are so the UI can show a real total + ETA. Cheap server-side count.
			if (this.catchUpTotal === undefined && this.transport.countEntriesSince) {
				try {
					const remaining = await this.transport.countEntriesSince(
						this.persistedSeq,
					);
					if (remaining >= CATCH_UP_PROGRESS_MIN) {
						this.catchUpTotal = remaining;
						this.catchUpDone = 0;
						this.reportCatchUp();
					}
				} catch {
					// A failed count just means no progress bar; the drain still runs.
				}
			}

			let fetchCursor = this.persistedSeq;
			// A bounded number of pages per call, so a single catchUp() can't spin
			// forever if the head keeps moving; the next tick picks up where we left.
			for (let page = 0; page < CATCH_UP_MAX_PAGES; page++) {
				let entries: ChangesetEntry[];
				try {
					entries = await this.transport.fetchEntriesSince(
						fetchCursor,
						CATCH_UP_PAGE_SIZE,
					);
				} catch {
					return false;
				}
				if (entries.length === 0) {
					// Nothing after the cursor - we're at the head.
					this.finishCatchUp();
					return true;
				}

				for (const entry of entries.sort((a, b) => a.seq - b.seq)) {
					await this.handleEntry(entry);
					this.catchUpDone += 1;
					if (entry.seq > fetchCursor) {
						fetchCursor = entry.seq;
					}
				}
				this.advanceWatermark();
				this.reportCatchUp();

				// A changeset failed to apply: the watermark is now pinned here and
				// paging further just piles unusable entries into memory. Stop and let
				// a resync / retry recover, rather than draining the whole log for
				// nothing.
				if (this.applyFailed) {
					return false;
				}

				// Short page → we've reached the head.
				if (entries.length < CATCH_UP_PAGE_SIZE) {
					this.finishCatchUp();
					return true;
				}
			}
			// Hit the per-call page cap with full pages still coming: more to drain.
			return false;
		} finally {
			this.catchingUp = false;
		}
	}

	// Emit current drain progress (no-op unless a progress total is set - i.e. the
	// gap was big enough to bother showing).
	private reportCatchUp() {
		if (this.catchUpTotal === undefined) {
			return;
		}
		this.onCatchUpProgress?.({
			done: Math.min(this.catchUpDone, this.catchUpTotal),
			total: this.catchUpTotal,
		});
	}

	// Clear the drain progress once we've reached the head.
	private finishCatchUp() {
		if (this.catchUpTotal !== undefined) {
			this.catchUpTotal = undefined;
			this.catchUpDone = 0;
			this.onCatchUpProgress?.(undefined);
		}
	}

	// Is the cloud connection actually live right now? The sim/advance/transaction
	// guard calls this so we never mutate the shared league while only *looking*
	// connected. `force` demands a real round-trip (no recent-contact shortcut),
	// used before a sim so a silently-dead socket can't slip a sim through and then
	// fail to upload. A transport without the probe (the test fake) is treated as live.
	async verifyConnection(force = false): Promise<boolean> {
		return (await this.transport.verifyConnection?.(force)) ?? true;
	}

	// Ms since last confirmed live contact with the cloud (undefined if untracked).
	// Drives the header status dot - a soft signal kept fresh by the subscription/
	// catch-up while healthy, going stale when the connection quietly dies.
	contactAge(): number | undefined {
		const at = this.transport.getLastContactAt?.();
		return at === undefined ? undefined : Date.now() - at;
	}

	// The watermark we've durably caught up through (server-timestamp millis).
	getPersistedSeq(): number {
		return this.persistedSeq;
	}

	// Read the entire shared change log once (not a live subscription), for a full
	// resync. Empty if the transport can't. Can be large - prefer fetchRecentLog
	// for anything that only needs recent activity.
	async fetchLog(): Promise<ChangesetEntry[]> {
		const entries = (await this.transport.fetchAllEntries?.()) ?? [];
		return entries.sort((a, b) => a.seq - b.seq);
	}

	// The most recent `n` entries, oldest-first, for the activity panel - so it
	// renders a bounded list instead of pulling the whole log. Falls back to the
	// full log for a transport without the paged read (the in-memory test fake).
	async fetchRecentLog(n: number): Promise<ChangesetEntry[]> {
		const entries = this.transport.fetchRecentEntries
			? await this.transport.fetchRecentEntries(n)
			: ((await this.transport.fetchAllEntries?.()) ?? []);
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
