import type { Changeset } from "./changeset.ts";

// One entry in the shared change log - a changeset one device produced, tagged
// with who made it and its ordering position. This is what gets stored in
// Firestore (or any transport) and replayed to every client.
export type ChangesetEntry = {
	// Unique per entry, for dedup / idempotency.
	id: string;
	// Which client produced it (so clients skip their own).
	authorId: string;
	// Monotonic ordering position, assigned by the transport/server.
	seq: number;
	// e.g. "main.proposeTrade" - for debugging/inspection.
	action: string;
	changeset: Changeset;

	// Bulk changes (e.g. a simulation) are too big for one Firestore doc, so the
	// host splits them into chunks that share a batchId. Receivers buffer chunks
	// until all `chunkCount` have arrived, then apply the reassembled changeset.
	// Absent on normal (single-doc) changes.
	batchId?: string;
	chunkIndex?: number;
	chunkCount?: number;
};

export interface SyncSubscriber {
	// Handle one entry from the shared log. Returns whether it was applied.
	onEntry(entry: ChangesetEntry): Promise<boolean> | boolean;

	// Called after a batch of entries (e.g. a Firestore snapshot) has all been
	// processed - a safe point to advance the persisted watermark.
	onBatchProcessed?(): void;
}

// Pluggable backend. The engine talks only to this interface, so the same logic
// runs over an in-memory fake (tests) or Firebase (production).
export interface SyncTransport {
	readonly clientId: string;

	// Publish a locally-produced entry. The transport assigns `seq`, so callers
	// pass everything except that.
	publish(entry: Omit<ChangesetEntry, "seq">): Promise<void>;

	// Subscribe to the ordered stream of entries after our watermark (including
	// history we missed, then live updates). Returns an unsubscribe function.
	subscribe(subscriber: SyncSubscriber): () => void;
}
