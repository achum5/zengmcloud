import type { Changeset } from "./changeset.ts";
import type { SyncedAutoPlay } from "../../../common/types.ts";
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
	// Server-relative ms (the holder's clock) until which the wheel-holder is
	// actively advancing the league - a sim/phase/draft that's running or still
	// uploading, and so not yet visible to followers via the change log. While
	// this is in the future, followers refuse conflict-prone edits (trades,
	// signings, roster/lineup changes) so a stale whole-record write can't clobber
	// the sim's results. It's a lease, so a simmer that crashes mid-sim can't lock
	// the room forever - it just expires.
	busyUntil?: number;
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
	// subscription). Powers full-resync recovery. Optional so the in-memory test
	// transport can skip it.
	fetchAllEntries?(): Promise<ChangesetEntry[]>;

	// The most recent `n` entries, oldest-first. For the activity panel, so it
	// never reads the whole log. Optional so the test transport can skip it.
	fetchRecentEntries?(n: number): Promise<ChangesetEntry[]>;

	// Read entries after a server-timestamp, oldest-first. With `pageLimit`,
	// returns just one bounded page so a large backlog can be drained page by page.
	// Optional so the in-memory test transport can skip it.
	fetchEntriesSince?(
		sinceMs: number,
		pageLimit?: number,
	): Promise<ChangesetEntry[]>;

	// Move the watermark the live subscription starts from (called after the
	// backlog drain so the subscription's initial snapshot is just the live tail).
	updateSince?(ts: number): void;

	// Is the connection ACTUALLY live right now (not just "we have a transport
	// object")? Cheap on recent contact, else a real timed round-trip. The
	// sim/advance/transaction guard uses this to refuse to mutate when the app
	// only looks connected. Optional; absent transport ⇒ treated as live.
	verifyConnection?(): Promise<boolean>;

	// Epoch ms of last confirmed live contact. Powers the header status dot (a
	// soft/passive signal; verifyConnection is the precise gate). Optional.
	getLastContactAt?(): number;

	// Upsert this room's registry doc (listable on the admin page) and stamp the
	// league fingerprint. Optional so the in-memory test transport can skip it.
	touchRoom?(leagueId?: string): Promise<void>;

	// Read this room's registry doc (its league fingerprint), if any. Optional so
	// the in-memory test transport can skip it.
	getRoomInfo?(): Promise<{ leagueId?: string } | undefined>;

	// Share / watch the simmer's auto-play schedule for the room. Optional so the
	// in-memory test transport can skip them.
	publishAutoPlay?(state: SyncedAutoPlay): Promise<void>;
	subscribeAutoPlay?(
		onChange: (state: SyncedAutoPlay | undefined) => void,
	): () => void;

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

	// Stamp/clear the wheel-holder's "actively advancing" lease on the shared
	// authority doc (see Authority.busyUntil). Pass 0 to clear.
	publishBusy?(busyUntil: number): Promise<void>;
}
