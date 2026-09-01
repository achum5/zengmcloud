import type { SimDayClaimDoc } from "./simDayClaimPolicy.ts";
import type { Changeset } from "./changeset.ts";
import type { SyncedAutoPlay } from "../../../common/types.ts";
import type { SyncNotification } from "./notifications.ts";
import type { LeaguePosition } from "./leaguePosition.ts";
import type { LiveGameChatMessage } from "../../../common/liveGameChat.ts";

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
	// The changes themselves, for a single-doc entry. Empty ({ changes: [] }) on
	// a payload-part entry (see payloadPart) - the real content is in the joined
	// parts.
	changeset: Changeset;

	// Bulk changes (e.g. a simulation) are too big for one Firestore doc, so the
	// author splits them into chunks that share a batchId. Receivers buffer
	// chunks until all `chunkCount` have arrived, then apply the reassembled
	// changeset. Absent on normal (single-doc) changes.
	batchId?: string;
	chunkIndex?: number;
	chunkCount?: number;

	// New-format bulk chunk: a slice of the SERIALIZED whole changeset, split at
	// the string level. Unlike the legacy per-record chunking (which shipped
	// each chunk as its own valid changeset), this can carry a record of ANY
	// size - a single record bigger than one Firestore doc previously produced
	// an unshippable chunk that wedged the upload queue forever. Receivers
	// concatenate all parts of a batch in index order, then deserialize.
	// Entries without this field are legacy record-level chunks and are still
	// applied the old way.
	payloadPart?: string;

	// Display metadata for the sync-activity page, carried on payload-part
	// entries because their content is not independently parseable: how many
	// records the whole batch touches, and which gameAttributes keys it carries.
	records?: number;
	attrs?: string[];
};

// A live-sim broadcast in progress. Whoever live-sims a game - the person in
// charge of simming or anyone playing out their own - publishes the (immutable)
// play-by-play once, then heartbeats a moving cursor; every other device
// navigates to the live game and replays in lockstep, seeing exactly what the
// simmer sees (until they leave). Stored at leagues/{code}/control/
// liveBroadcast (the cursor/meta) with the payload split across
// control/liveBroadcastData{i} docs (see FirebaseTransport). `expiresAt` is a
// lease so a simmer that crashes mid-broadcast can't lock followers forever.
export type LiveBroadcastMeta = {
	holderId: string;
	active: boolean;
	gid: number;
	byName: string;
	// "PHO @ DAL" - what the pill shows, stamped by the broadcaster so viewers
	// don't need the game record just to label a button.
	label?: string;
	// Events the broadcaster has consumed so far - the lockstep position followers
	// seek to. Always a deterministic pause boundary, so no drift.
	cursor: number;
	paused: boolean;
	speed: number;
	gameOver: boolean;
	// Distinguishes one broadcast from the next (so followers know when to re-load
	// the payload and re-navigate). The broadcaster's clock, ms.
	startedAt: number;
	// How many liveBroadcastData{i} docs make up the payload.
	chunkCount: number;
	// Server-relative ms (broadcaster's clock) until which this broadcast is
	// considered live. Re-stamped on every heartbeat; once it passes with no
	// update, followers treat the broadcast as ended (crash recovery).
	expiresAt: number;
};

// The subset of LiveBroadcastMeta a single write sets. Every write merges onto
// the shared doc and (in the transport) stamps holderId, so the security rule
// (holderId == auth.uid) always passes and only the broadcaster can write.
export type LiveBroadcastUpdate = {
	active?: boolean;
	gid?: number;
	byName?: string;
	// Dead field, still written as false. Broadcasts used to come in two kinds
	// and this said which; a device on a build that still reads it must not
	// inherit a leftover true from an older broadcast on the merged doc.
	optIn?: boolean;
	label?: string;
	cursor?: number;
	paused?: boolean;
	speed?: number;
	gameOver?: boolean;
	startedAt?: number;
	chunkCount?: number;
	expiresAt?: number;
};

// A live draft-lottery reveal in progress. Whoever runs the lottery (the
// simmer) heartbeats how many picks they've revealed so far; every other
// device replays the reveal in lockstep on its own lottery page. The lottery
// RESULT travels through the normal change log - this doc only carries the
// reveal position. `expiresAt` is a lease so a broadcaster that disappears
// mid-reveal can't hide the (already-synced) result from viewers forever.
export type LotteryRevealMeta = {
	holderId: string;
	active: boolean;
	season: number;
	// How many picks have been revealed so far (-1 = none yet).
	revealed: number;
	byName: string;
	// Distinguishes one reveal from the next (the broadcaster's clock, ms).
	startedAt: number;
	expiresAt: number;
};

// The subset of LotteryRevealMeta a single write sets (merged onto the doc;
// the transport stamps holderId).
export type LotteryRevealUpdate = {
	active?: boolean;
	season?: number;
	revealed?: number;
	byName?: string;
	startedAt?: number;
	expiresAt?: number;
};

// Who currently holds sim authority - the one device allowed to advance the
// league (sim/draft/phase change). Stored at leagues/{code}/control/authority
// and watched by everyone, so all devices agree on who's in control. Undefined
// means nobody has claimed it yet (a brand-new room).
export type Authority = {
	holderId: string;
	holderName: string;
	// Server-relative ms (the holder's clock) until which the sim authority is
	// actively advancing the league - a sim/phase/draft that's running or still
	// uploading, and so not yet visible to followers via the change log. While
	// this is in the future, followers refuse conflict-prone edits (trades,
	// signings, roster/lineup changes) so a stale whole-record write can't clobber
	// the sim's results. It's a lease, so a simmer that crashes mid-sim can't lock
	// the room forever - it just expires.
	busyUntil?: number;
	// How far the holder's league has actually progressed. Followers compare it
	// against their own to notice they are missing data even when their sync
	// engine believes it is caught up - see leaguePosition.ts.
	position?: LeaguePosition;
};

// One device's draft ready state, stored under its uid in the shared
// control/draftReady doc. `untilPick` is the OVERALL pick number (1-based
// across the draft) this device is ready through; `draftKey` scopes it to one
// specific draft ("{season}-{phase}") so stale entries from a previous season
// never count; `tid` is the team this ready covers (readiness is counted per
// user team, so multi-device users can't deadlock or double-count the room).
export type DraftReadyEntry = {
	untilPick: number;
	draftKey: string;
	tid: number;
	name?: string;
};

// One team's free-agency board: the ranked list of free agents (pids, best
// first) it wants a shot at on the next FA day. Published per device; the
// newest entry per team wins.
// One finished trivia game, as it travels between devices in a room. The
// `replay` field is what makes an entry more than a scoreline: it's enough to
// rebuild the exact board or team-season the author played.
export type TriviaScoreEntry = {
	id: string;
	game: string;
	ts: number;
	score: number;
	label: string;
	detail: string;
	progress?: { done: number; total: number };
	// Per-cell rarity points, in reading order; null = unsolved. Renders the
	// colored square block without needing the board itself.
	cells?: (number | null)[];
	replay?:
		| { kind: "grid"; code: string }
		| { kind: "team"; season: number; tid: number };
	// Who played it, stamped on publish.
	byName?: string;
	byTid?: number;
	// The team the game was ABOUT (roster quizzes), which is not the same thing.
	tid?: number;
	season?: number;
};

export type FaBoardEntry = {
	season: number;
	tid: number;
	pids: number[];
	at: number; // client clock, newest-entry-per-team tiebreak
	name?: string;
};

export interface SyncSubscriber {
	// Handle one entry from the shared log. Returns whether it was applied.
	onEntry(entry: ChangesetEntry): Promise<boolean> | boolean;

	onError?(error: unknown): void;

	// Called after a batch of entries (e.g. a Firestore snapshot) has all been
	// processed - a safe point to advance the persisted watermark.
	onBatchProcessed?(): void;
}

// Pluggable backend. The engine talks only to this interface, so the same logic
// runs over an in-memory fake (tests) or Firebase (production).
// A periodic full-state checkpoint of the league, published by the sim
// authority. This is what makes recovery bounded FOREVER: a device too far
// behind for the delta log (or one whose gap was pruned away) restores the
// snapshot and replays only the tail since it, instead of needing a log that
// reaches back to wherever it left off. It is also what makes pruning the log
// safe at all.
export type RoomSnapshotMeta = {
	// The publisher's log watermark at publish time: the snapshot's state
	// contains every entry at or below this seq.
	seq: number;
	// Epoch ms when published.
	at: number;
	byName: string;
	chunkCount: number;
	position?: LeaguePosition;
	// Which set of chunk docs this snapshot's payload lives in. Every publish
	// writes a FRESH generation and only then points the meta at it, so a
	// publish in progress can never overwrite the payload the current meta
	// still refers to. Absent on snapshots from before generations existed,
	// which used one fixed set of doc ids and could be torn mid-publish.
	generation?: string;
};

// --- Sync v2 (version chain) wire types --------------------------------------

// The room's v2 pointer document. THE source of truth for a v2 room: `version`
// only ever moves by compare-and-set, so the chain cannot fork at the source.
export type V2StateDoc = {
	version: number;
	authorId: string;
	byName: string;
	at: number;
	// The action that produced the newest version (display/notifications).
	action?: string;
	// The newest version's serialized payload, carried inline when small so
	// receivers apply it straight off the pointer push with no further reads.
	// Large payloads (sim days) omit this and are fetched from chunk docs.
	inlineDelta?: string;
	// Newest full-state checkpoint, if any: which version it captures, how
	// many chunks its payload spans, and which publish generation the chunks
	// live under (absent for checkpoints from older builds).
	checkpointVersion?: number;
	checkpointChunkCount?: number;
	checkpointGeneration?: string;
};

export interface SyncTransport {
	readonly clientId: string;

	// Lightweight write used as a liveness check before risky local actions. If
	// this fails or times out, the caller should assume publishes are not safe.
	ping?(): Promise<void>;

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

	// Every chunk entry of one bulk batch, fetched DIRECTLY by batchId - no seq
	// range, no watermark. This is the batch-rescue path: it can recover chunks
	// that sit below a device's watermark (e.g. after an interrupted upload was
	// finished much later), which a seq-ordered fetch can never see again.
	fetchBatchEntries?(batchId: string): Promise<ChangesetEntry[]>;

	// The most recent `n` entries, oldest-first. For the activity panel, so it
	// never reads the whole log. Optional so the test transport can skip it.
	fetchRecentEntries?(n: number): Promise<ChangesetEntry[]>;

	// Count of entries still after a watermark (cheap server aggregate), for the
	// catch-up progress total. Optional so the test transport can skip it.
	countEntriesSince?(sinceMs: number): Promise<number>;

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

	// Room snapshot (full-state checkpoint) support. publishRoomSnapshot writes
	// the chunked payload FIRST and the meta doc last, so a reader that sees a
	// meta doc can always fetch a complete payload; it returns the chunk count.
	// deleteEntriesBefore prunes log entries older than the given seq (only ever
	// called with the PREVIOUS snapshot's seq, so the log always covers at least
	// one full snapshot interval). All optional so the in-memory test transport
	// can skip them.
	publishRoomSnapshot?(
		meta: Omit<RoomSnapshotMeta, "chunkCount">,
		serialized: string,
	): Promise<number>;
	fetchRoomSnapshotMeta?(): Promise<RoomSnapshotMeta | undefined>;

	// ---- Room <-> league binding --------------------------------------------
	// A room is claimed by exactly one league lineage, permanently. Claiming is
	// transactional first-writer-wins: the returned id is the room's binding
	// after the call, whether that is the id passed in (claim succeeded) or an
	// earlier claimant's (compare and refuse). Optional like everything else so
	// the in-memory test transports simply lack it.
	fetchRoomLeagueId?(): Promise<string | undefined>;
	claimRoomLeagueId?(leagueId: string): Promise<string>;

	// ---- Sync v2 (version chain) ------------------------------------------
	// All optional: the in-memory test transport and older builds simply lack
	// them, and a v2 room is only ever joined by code that checks.
	// Server-only unless allowCache - the head probe must fail loudly rather
	// than confirm a stale cached pointer (see FirebaseTransport).
	fetchRoomV2State?(options?: {
		allowCache?: boolean;
	}): Promise<V2StateDoc | undefined>;
	// Tear down and rebuild the backend connection - the first remedy for a
	// silently wedged channel (dead listeners + hanging reads).
	cycleNetwork?(): Promise<void>;
	// The escalation, for a wedge that cycling did not clear: discard the client
	// entirely, build a new one, and re-establish every listener on it. Roughly
	// what closing and reopening the app does, without closing the app.
	hardRestart?(): Promise<void>;
	// onError (optional) fires when the underlying listener terminates, so the
	// engine can re-establish it instead of silently dropping to timer pacing.
	subscribeRoomV2State?(
		onChange: (state: V2StateDoc) => void,
		onError?: (error: unknown) => void,
	): () => void;
	// Write version N's delta payload chunks. Chunk docs are IMMUTABLE (their
	// ids embed the version), and chunk 0 carries the chunkCount, so a reader
	// never needs out-of-band metadata.
	publishV2Delta?(
		meta: { version: number; authorId: string; action: string; at: number },
		serialized: string,
	): Promise<number>;
	// Compare-and-set the pointer from expectedVersion to next.version. False
	// means someone else won; the caller must catch up, never overwrite.
	commitV2Version?(
		next: {
			version: number;
			authorId: string;
			byName: string;
			at: number;
			action: string;
			inlineDelta?: string;
		},
		expectedVersion: number,
	): Promise<boolean>;
	// Both of the above, as ONE transaction. Only usable for a payload small
	// enough to be a single chunk; a big one still has to write its chunks
	// before the pointer can point at them. Same contract as commitV2Version -
	// false means someone else won the slot and nothing was written.
	publishAndCommitV2Version?(
		next: {
			version: number;
			authorId: string;
			byName: string;
			at: number;
			action: string;
			inlineDelta?: string;
		},
		serialized: string,
		expectedVersion: number,
	): Promise<boolean>;
	fetchV2Delta?(version: number): Promise<
		| {
				serialized: string;
				authorId: string;
				action: string;
				at: number;
		  }
		| undefined
	>;
	// Checkpoint chunks land at generation-unique doc ids and the pointer
	// flips to them only at commit, so a reader can never see two publishes
	// spliced together.
	publishV2Checkpoint?(
		version: number,
		serialized: string,
		generation?: string,
	): Promise<number>;
	// Point the state doc at a published checkpoint (transactional merge that
	// never touches `version`).
	commitV2Checkpoint?(
		version: number,
		chunkCount: number,
		generation?: string,
	): Promise<boolean>;
	fetchV2Checkpoint?(
		version: number,
		chunkCount: number,
		generation?: string,
	): Promise<string | undefined>;
	deleteV2DeltasBefore?(version: number): Promise<number>;
	fetchRoomSnapshotData?(
		chunkCount: number,
		generation?: string,
	): Promise<string | undefined>;
	deleteEntriesBefore?(seqMs: number): Promise<number>;

	// Is the connection ACTUALLY live right now (not just "we have a transport
	// object")? Cheap on recent contact, else a real timed round-trip. The
	// sim/advance/transaction guard uses this to refuse to mutate when the app
	// only looks connected. `force` skips the recent-contact shortcut and always
	// does the real round-trip (used before a sim, where a stale "recent contact"
	// isn't proof the socket is live now). Optional; absent transport ⇒ treated as live.
	verifyConnection?(force?: boolean): Promise<boolean>;

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
	// Partial so a device can refresh single fields (e.g. its current tid after
	// a team switch) without clobbering its stored FCM token.
	registerMember?(uid: string, member: Partial<SyncMember>): Promise<void>;
	publishNotification?(
		notification: SyncNotification & { authorId: string; authorName: string },
	): Promise<void>;

	// "Sim authority" (advance-authority) support. Optional so the in-memory test
	// transport can skip it. claimAuthority makes this device the sole holder;
	// subscribeAuthority watches who currently holds it (undefined until someone
	// claims). Returns an unsubscribe function.
	claimAuthority?(holderId: string, holderName: string): Promise<void>;
	subscribeAuthority?(
		onChange: (authority: Authority | undefined) => void,
		onError?: (error: unknown) => void,
	): () => void;

	// Stamp/clear the sim authority's "actively advancing" lease on the shared
	// authority doc (see Authority.busyUntil). Pass 0 to clear.
	publishBusy?(busyUntil: number, position?: LeaguePosition): Promise<void>;

	// Draft ready-up support (see draftReady.ts). publishDraftReady merges THIS
	// device's ready entry onto the shared doc (null clears it); clearUids also
	// nulls the entries of the caller's own team's OTHER devices, so team-level
	// readiness follows the team's latest action from any of its devices;
	// subscribeDraftReady watches everyone's entries; claimDraftAdvance
	// atomically claims the right to sim one specific pick - it returns true for
	// exactly one caller per (draftKey, pick) per lease window, so two devices
	// can never both sim the same pick. Optional so the in-memory test transport
	// can skip them.
	publishDraftReady?(
		entry: DraftReadyEntry | null,
		clearUids?: string[],
	): Promise<void>;
	subscribeDraftReady?(
		onChange: (
			ready: Record<string, DraftReadyEntry | null> | undefined,
		) => void,
	): () => void;
	claimDraftAdvance?(
		draftKey: string,
		pick: number,
		leaseMs: number,
	): Promise<boolean>;
	// Marks the caller's claimed step finished (closing its crash-recovery
	// re-claim window; see advanceClaimPolicy.ts). Best-effort.
	completeDraftAdvance?(draftKey: string, pick: number): Promise<void>;

	// Schedule-day sim fence (see simDayClaimPolicy.ts). claimSimDay atomically
	// claims one slice of a schedule day (whole day or one live-simmed game) -
	// it returns true for exactly one caller per (season, day, games), ever, so
	// two devices can never both sim the same games and double-apply their
	// aggregates. completeSimDay closes the claim's crash-recovery window.
	// Optional so the in-memory test transport can skip them.
	claimSimDay?(
		stageKey: string,
		day: number,
		gids: number[],
		leaseMs: number,
	): Promise<boolean>;
	// Completion is scoped to the gids whose results the caller durably
	// queued - see simDayClaimPolicy.ts for why a day-level mark wedged rooms.
	completeSimDay?(stageKey: string, day: number, gids: number[]): Promise<void>;
	// The fence document as it stands, for a device deciding whether a result
	// it queued while offline is still the room's to receive (see
	// revalidateQueuedSingleGame in simDayFence.ts). A plain read; the decision
	// is made with the same pure policy the claim transaction applies.
	readSimDayClaim?(): Promise<SimDayClaimDoc | undefined>;

	// Free-agency board support (see faBoard.ts). Each device publishes its
	// team's ranked free-agent list (null clears it); everyone subscribes but the
	// UI keeps boards blind until the day resolves. Same per-uid merge semantics
	// as publishDraftReady.
	publishFaBoard?(entry: FaBoardEntry | null): Promise<void>;
	subscribeFaBoard?(
		onChange: (boards: Record<string, FaBoardEntry | null> | undefined) => void,
	): () => void;

	// Shared trivia scoreboard (see triviaScores.ts). Same per-client merge
	// semantics as the FA board, but each bucket is a LIST of recent results
	// rather than a single entry.
	publishTriviaScores?(entries: TriviaScoreEntry[]): Promise<void>;
	subscribeTriviaScores?(
		onChange: (
			scores: Record<string, TriviaScoreEntry[] | null> | undefined,
		) => void,
	): () => void;

	// Live-sim broadcast support (see LiveBroadcastMeta). Optional so the
	// in-memory test transport can skip it.
	//
	// publishLiveBroadcastData writes the immutable play-by-play payload (a
	// serialized string) split across as many docs as needed, and returns the
	// chunk count. publishLiveBroadcast merges the cursor/meta doc (holderId
	// stamped by the transport). subscribeLiveBroadcast watches that doc.
	// fetchLiveBroadcastData reassembles the payload string from its chunks (or
	// undefined if a chunk is missing). clearLiveBroadcast marks the broadcast
	// ended and removes the payload docs.
	// Live game chat. Many devices write concurrently, so messages merge into
	// ONE control doc keyed by message id - the same per-key merge the FA board
	// and draft ready-up use, which is what lets it pass the deployed
	// control-doc rule (holderId == the writer's own uid) with no rules change.
	publishLiveChatMessage?(message: LiveGameChatMessage): Promise<void>;
	subscribeLiveChat?(
		onChange: (messages: LiveGameChatMessage[]) => void,
	): () => void;
	// Wipe the doc when a new broadcast starts, so last game's chat cannot
	// bleed into this one. Broadcaster only.
	clearLiveChat?(): Promise<void>;

	publishLiveBroadcast?(update: LiveBroadcastUpdate): Promise<void>;
	publishLiveBroadcastData?(gid: number, serialized: string): Promise<number>;
	subscribeLiveBroadcast?(
		onChange: (meta: LiveBroadcastMeta | undefined) => void,
	): () => void;
	fetchLiveBroadcastData?(chunkCount: number): Promise<string | undefined>;
	clearLiveBroadcast?(chunkCount: number): Promise<void>;

	// Live lottery-reveal broadcast (see LotteryRevealMeta). Optional so the
	// in-memory test transport can skip it.
	publishLotteryReveal?(update: LotteryRevealUpdate): Promise<void>;
	subscribeLotteryReveal?(
		onChange: (meta: LotteryRevealMeta | undefined) => void,
	): () => void;
}
