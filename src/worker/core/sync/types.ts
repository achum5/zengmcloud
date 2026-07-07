import type { Changeset } from "./changeset.ts";
import type { SyncNotification } from "./notifications.ts";

// One device's push registration in a league room, stored at
// leagues/{code}/members/{uid}. The Cloud Function reads these to know where to
// send pushes (and which team each person manages, for targeting).
export type SyncMember = {
	fcmToken: string;
	name: string;
	// The team this device currently manages (its userTid), so notifications can
	// be targeted to the people a change actually affects.
	tid: number;
};

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

// Who currently holds "the wheel" - the one device allowed to advance the
// league (sim/draft/phase change). Stored at leagues/{code}/control/authority
// and watched by everyone, so all devices agree on who's in control. Undefined
// means nobody has claimed it yet (a brand-new room).
export type Authority = {
	holderId: string;
	holderName: string;
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

	// One-shot read of the ENTIRE change log, ordered oldest-first (not a live
	// subscription). Powers the sync-activity view and full-resync recovery.
	// Optional so the in-memory test transport can skip it.
	fetchAllEntries?(): Promise<ChangesetEntry[]>;

	// Upsert this room's registry doc (listable on the admin page) and stamp the
	// league fingerprint. Optional so the in-memory test transport can skip it.
	touchRoom?(leagueId?: string): Promise<void>;

	// Read this room's registry doc (its league fingerprint), if any. Optional so
	// the in-memory test transport can skip it.
	getRoomInfo?(): Promise<{ leagueId?: string } | undefined>;

	// Push-notification support. Optional so the in-memory test transport can
	// skip it. registerMember records this device's FCM token in the room;
	// publishNotification enqueues a push for the Cloud Function to fan out.
	registerMember?(uid: string, member: SyncMember): Promise<void>;
	publishNotification?(
		notification: SyncNotification & { authorId: string; authorName: string },
	): Promise<void>;

	// "Wheel" (advance-authority) support. Optional so the in-memory test
	// transport can skip it. claimAuthority makes this device the sole holder;
	// subscribeAuthority watches who currently holds it (undefined until someone
	// claims). Returns an unsubscribe function.
	claimAuthority?(holderId: string, holderName: string): Promise<void>;
	subscribeAuthority?(
		onChange: (authority: Authority | undefined) => void,
	): () => void;
}
