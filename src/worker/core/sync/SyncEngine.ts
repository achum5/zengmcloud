import {
	applyChangeset,
	type Changeset,
	type SyncChange,
} from "./changeset.ts";
import type { ChangesetEntry, SyncTransport } from "./types.ts";

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

	private isHost: boolean;

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

	constructor(
		transport: SyncTransport,
		options: {
			isHost?: boolean;
			initialWatermark?: number;
			onWatermark?: (seq: number) => void;
		} = {},
	) {
		this.transport = transport;
		this.isHost = options.isHost ?? false;
		this.onWatermark = options.onWatermark;
		this.maxSeq = options.initialWatermark ?? 0;
		this.persistedSeq = options.initialWatermark ?? 0;
	}

	get clientId(): string {
		return this.transport.clientId;
	}

	start() {
		if (this.unsubscribe) {
			return;
		}
		this.unsubscribe = this.transport.subscribe({
			onEntry: (entry) => this.handleEntry(entry),
			onBatchProcessed: () => this.advanceWatermark(),
		});
	}

	// Persist the watermark only when there are no half-received bulk batches, so
	// a reconnect never skips past chunks it hasn't fully applied. (Whole-record
	// applies are idempotent, so re-fetching a bit on reconnect is harmless.)
	private advanceWatermark() {
		if (this.pendingBatches.size > 0) {
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
	}

	// Publish a changeset produced by a local action.
	async onLocalChangeset(changeset: Changeset, action: string) {
		if (changeset.changes.length === 0) {
			return;
		}

		if (changeset.changes.length <= MAX_SYNC_CHANGES) {
			const id = makeId();
			this.seen.add(id);
			await this.transport.publish({
				id,
				authorId: this.transport.clientId,
				action,
				changeset,
			});
			return;
		}

		// Bulk change (e.g. a sim). Only the host broadcasts these.
		if (!this.isHost) {
			console.warn(
				`[sync] Skipping bulk change from "${action}" (${changeset.changes.length} records) - only the host broadcasts sims.`,
			);
			return;
		}

		await this.publishBulk(changeset, action);
	}

	private async publishBulk(changeset: Changeset, action: string) {
		const chunks = chunkChanges(changeset.changes);
		const batchId = makeId();

		for (let i = 0; i < chunks.length; i++) {
			const id = makeId();
			this.seen.add(id);
			await this.transport.publish({
				id,
				authorId: this.transport.clientId,
				action,
				batchId,
				chunkIndex: i,
				chunkCount: chunks.length,
				changeset: { changes: chunks[i]! },
			});
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
			return false;
		}
		return true;
	}
}
