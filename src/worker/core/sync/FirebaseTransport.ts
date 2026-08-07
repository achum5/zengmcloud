import {
	getFirestore,
	collection,
	doc,
	addDoc,
	setDoc,
	getDoc,
	getDocFromServer,
	getCountFromServer,
	disableNetwork,
	enableNetwork,
	getDocsFromServer,
	deleteDoc,
	limit,
	onSnapshot,
	query,
	orderBy,
	runTransaction,
	startAfter,
	where,
	Timestamp,
	serverTimestamp,
	type CollectionReference,
	type Firestore,
	type QueryDocumentSnapshot,
} from "firebase/firestore";
import { getFirebaseApp } from "./firebaseApp.ts";
import { ttlAtMsFor } from "../../../common/syncRetention.ts";
import {
	decideAdvanceClaim,
	type AdvanceClaimDoc,
} from "./advanceClaimPolicy.ts";
import { decideSimDayClaim, type SimDayClaimDoc } from "./simDayClaimPolicy.ts";
import { syncDebugLog } from "./debugLog.ts";

// Page size / cap for the whole-log read. 200 keeps each request small enough
// to finish on a phone even when the page is full of bulk-sim chunks; the cap
// is a runaway guard, not a real limit (200k entries is far past any league).
const FULL_LOG_PAGE_SIZE = 200;
const FULL_LOG_MAX_PAGES = 1000;
import { deserializeChangeset, serializeChangeset } from "./serialize.ts";
import type { SyncedAutoPlay } from "../../../common/types.ts";
import type { SyncNotification } from "./notifications.ts";
import type { LiveGameChatMessage } from "../../../common/liveGameChat.ts";
import { parseLeaguePosition, type LeaguePosition } from "./leaguePosition.ts";
import type {
	Authority,
	ChangesetEntry,
	V2StateDoc,
	DraftReadyEntry,
	FaBoardEntry,
	TriviaScoreEntry,
	LiveBroadcastMeta,
	LiveBroadcastUpdate,
	LotteryRevealMeta,
	LotteryRevealUpdate,
	SyncMember,
	SyncSubscriber,
	SyncTransport,
} from "./types.ts";

// A room has exactly one "sim authority" doc recording who may advance the league.
const AUTHORITY_DOC_ID = "authority";

// The live-sim broadcast cursor/meta doc, and the prefix for its payload chunks
// (control/liveBroadcastData0, ...1, ...). All under control/, so the generic
// control-doc security rule (write requires holderId == auth.uid) already covers
// them - no rules change needed.
const LIVE_BROADCAST_DOC_ID = "liveBroadcast";
const LIVE_BROADCAST_DATA_PREFIX = "liveBroadcastData";

// Draft ready-up docs, also under control/ so the same holderId rule covers
// them. draftReady holds everyone's ready entries (each device merges only its
// own, keyed by uid - Firestore merge writes are per-field, so concurrent
// merges by different devices never clobber each other); draftAdvance is the
// atomic claim for who sims the next pick.
const DRAFT_READY_DOC_ID = "draftReady";
const FA_BOARD_DOC_ID = "faBoard";
// Live game chat, one doc per room, message-keyed (see publishLiveChatMessage).
const LIVE_CHAT_DOC_ID = "liveChat";
const TRIVIA_SCORES_DOC_ID = "triviaScores";
const DRAFT_ADVANCE_DOC_ID = "draftAdvance";

// The per-season fence over schedule-day sims: which day (and which of its
// games) has been claimed, ever. See simDayClaimPolicy.ts for why day sims
// need a server-side fence in addition to the advisory authority doc.
const SIM_DAY_DOC_ID = "simDay";

// The live lottery-reveal cursor doc: whoever runs the lottery heartbeats how
// many picks they've revealed, and every other device replays the reveal in
// lockstep. The RESULT travels through the normal change log; this doc only
// carries the reveal position.
const LOTTERY_REVEAL_DOC_ID = "lotteryReveal";

// Keep each payload chunk well under Firestore's 1 MB/doc limit.
const LIVE_BROADCAST_CHUNK_BYTES = 700_000;
const ROOM_SNAPSHOT_DOC_ID = "roomSnapshot";
const ROOM_SNAPSHOT_DATA_PREFIX = "roomSnapshotData";

// ---- Sync v2 (version chain) doc ids ----------------------------------------
// All under the existing `control` collection so the current security rules
// (holderId must be the writer's own uid) cover them without any rules change.
const V2_STATE_DOC_ID = "v2state";
const v2DeltaDocId = (version: number, index: number) =>
	`v2delta_${version}_${index}`;
// Generation-unique when a generation is given (every new publish), so two
// publishes can never interleave chunk writes into the same documents - the
// corruption that once made a room's checkpoint unreadable for every joiner.
// The un-suffixed form remains readable for checkpoints from older builds.
const v2CheckpointDocId = (
	version: number,
	index: number,
	generation?: string,
) =>
	generation
		? `v2checkpoint_${version}_${generation}_${index}`
		: `v2checkpoint_${version}_${index}`;

// If we've had confirmed contact with Firestore within this window, treat the
// connection as live without a round-trip; otherwise verifyConnection() probes.
// Sized to comfortably cover the 15s background catch-up poll (which does a
// real server read and refreshes contact), so a healthy connection answers
// non-forced checks instantly - otherwise every interactive worker call in an
// idle room would pay a blocking probe.
const CONNECTION_FRESH_MS = 20_000;
// A publish attempt that the server hasn't acked within this long counts as
// failed. Critical: while offline, Firestore's setDoc does NOT reject - it
// buffers the write and resolves only on server ack - so an untimed publish
// hangs forever and its retry loop never even gets to retry. If the buffered
// write does land later, the retry overwrites the same doc id, so no duplicate.
const PUBLISH_ACK_TIMEOUT_MS = 12_000;
// A liveness probe that doesn't answer within this long counts as "not connected"
// (a silently-dropped listener won't error, it just never responds).
const CONNECTION_PROBE_TIMEOUT_MS = 6000;

const withTimeout = <T>(promise: Promise<T>, ms: number): Promise<T> =>
	new Promise((resolve, reject) => {
		const id = setTimeout(() => reject(new Error("timeout")), ms);
		promise.then(
			(value) => {
				clearTimeout(id);
				resolve(value);
			},
			(error) => {
				clearTimeout(id);
				reject(error);
			},
		);
	});

// Firestore-backed transport. Each shared league is a room keyed by its code;
// changes live in `leagues/{code}/changes`, ordered by server timestamp. The
// changeset is stored as a serialized string (see serialize.ts) to preserve
// Infinity/NaN and avoid Firestore's nested-array restrictions.
export class FirebaseTransport implements SyncTransport {
	readonly clientId: string;

	private db: Firestore;

	private code: string;

	private changesRef: CollectionReference;

	// Server-timestamp watermark: we only fetch changes newer than this, so a
	// reconnecting device catches up on exactly what it missed.
	private sinceTs: number;

	// When we last confirmed a live round-trip to Firestore (any successful read,
	// write, or real-time delivery). Powers verifyConnection().
	private lastContactAt = Date.now();

	constructor(
		code: string,
		clientId: string,
		options: { sinceTs?: number } = {},
	) {
		this.clientId = clientId;
		this.sinceTs = options.sinceTs ?? 0;
		this.code = code;
		this.db = getFirestore(getFirebaseApp());
		this.changesRef = collection(this.db, "leagues", code, "changes");
	}

	private markContact() {
		this.lastContactAt = Date.now();
	}

	// When we last confirmed live contact (ms epoch). Powers the header status dot.
	getLastContactAt(): number {
		return this.lastContactAt;
	}

	// Is the cloud connection ACTUALLY live right now? Cheap when we've had recent
	// confirmed contact; otherwise does a real, timed round-trip. This is what lets
	// the sim/advance guard refuse to advance when the app only *looks* connected
	// (a silently-dropped listener, an expired token, a resumed-from-suspend tab) -
	// which would otherwise advance locally and never reach the shared log.
	//
	// `force` skips the recent-contact shortcut and ALWAYS does the real round-trip.
	// The sim/advance guard passes force, because "we heard from the server 5s ago"
	// is not proof the connection is live NOW - a socket can die silently between a
	// snapshot and the sim, and a sim that then fails to upload strands every other
	// device. For a high-stakes, room-forking advance, the round-trip is worth it.
	async verifyConnection(force = false): Promise<boolean> {
		if (!force && Date.now() - this.lastContactAt < CONNECTION_FRESH_MS) {
			return true;
		}
		try {
			// getDocFromServer, not getDoc: a plain getDoc can be answered from
			// Firestore's offline cache and falsely report "connected". We want a
			// real server round-trip.
			await withTimeout(
				getDocFromServer(doc(this.db, "leagues", this.code)),
				CONNECTION_PROBE_TIMEOUT_MS,
			);
			this.markContact();
			return true;
		} catch {
			return false;
		}
	}

	// Publish the simmer's auto-play schedule so every device can show the same
	// schedule + countdown. We ride it on the authority ("who's simming") doc,
	// which every device already reads reliably - only the simmer can auto-play,
	// and it already holds this doc, so the merge write passes the same rule that
	// governs sim authority (holderId stays == our uid). This avoids depending on a
	// separate read rule for the registry doc.
	async publishAutoPlay(state: SyncedAutoPlay) {
		// Firestore rejects ANY write containing `undefined`, and the "off"/paused
		// snapshots carry nextRunAt: undefined - so they silently failed and
		// followers kept a stale countdown forever. Store null instead.
		await setDoc(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			{
				autoPlay: {
					enabled: state.enabled,
					nextRunAt: state.nextRunAt ?? null,
					rules: state.rules,
				},
			},
			{ merge: true },
		);
	}

	// Watch the room's auto-play schedule (carried on the authority doc). Fires
	// with the current value, then on every change. Undefined when nobody is
	// auto-playing. An error handler surfaces permission problems instead of
	// silently delivering nothing.
	subscribeAutoPlay(onChange: (state: SyncedAutoPlay | undefined) => void) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			(snapshot) => {
				const data = snapshot.data();
				const raw = data?.autoPlay as any;
				// nextRunAt is stored as null when paused/off (Firestore can't store
				// undefined); normalize back for the UI.
				onChange(
					raw
						? {
								enabled: !!raw.enabled,
								nextRunAt: raw.nextRunAt ?? undefined,
								rules: Array.isArray(raw.rules) ? raw.rules : [],
							}
						: undefined,
				);
			},
			(error) => {
				console.error("Auto-play schedule subscription failed", error);
			},
		);
	}

	// Read the room's registry doc, if any. `leagueId` is the fingerprint of the
	// league file this room belongs to (see connect.ts). Undefined if the room
	// has no registry doc yet.
	async getRoomInfo(): Promise<{ leagueId?: string } | undefined> {
		const snap = await getDoc(doc(this.db, "leagues", this.code));
		if (!snap.exists()) {
			return undefined;
		}
		const data = snap.data();
		return {
			leagueId: typeof data.leagueId === "string" ? data.leagueId : undefined,
		};
	}

	// Upsert the room's registry doc (leagues/{code}) so the admin page can list
	// every code that's been used, and stamp the league fingerprint so a device
	// can refuse to connect a different league to this room. Rooms are otherwise
	// created implicitly by writing to subcollections, leaving no listable parent.
	async touchRoom(leagueId?: string) {
		await setDoc(
			doc(this.db, "leagues", this.code),
			{
				code: this.code,
				updatedAt: serverTimestamp(),
				...(leagueId ? { leagueId } : {}),
			},
			{ merge: true },
		);
	}

	// Record (or refresh) this device's push registration in the room, so the
	// Cloud Function knows where to send notifications. Keyed by uid, so each
	// device has exactly one entry that updates in place; a partial member
	// merges onto the stored one (e.g. refreshing just the tid).
	async registerMember(uid: string, member: Partial<SyncMember>) {
		await setDoc(
			doc(this.db, "leagues", this.code, "members", uid),
			{ ...member, updatedAt: serverTimestamp() },
			{ merge: true },
		);
	}

	async ping() {
		await setDoc(
			doc(this.db, "leagues", this.code, "members", this.clientId),
			{ lastPingAt: serverTimestamp() },
			{ merge: true },
		);
	}

	// Enqueue a push. The Cloud Function triggers on this doc, looks up member
	// tokens, and delivers to everyone else's phones - so it works even when
	// their app is fully closed.
	async publishNotification(
		notification: SyncNotification & { authorId: string; authorName: string },
	) {
		await addDoc(collection(this.db, "leagues", this.code, "notifications"), {
			title: notification.title,
			body: notification.body,
			authorId: notification.authorId,
			authorName: notification.authorName,
			// Firestore rejects `undefined`; null means "everyone in the room".
			targetTids: notification.targetTids ?? null,
			// League-relative deep-link path ("" = app root), resolved to the
			// recipient's own lid on their device.
			path: notification.path ?? "",
			ts: serverTimestamp(),
		});
	}

	// Claim sim authority: become the sole device allowed to advance the league. The
	// security rules only permit writing holderId === your own uid, so you can
	// only ever sim here for yourself, never assign it to someone else.
	async claimAuthority(holderId: string, holderName: string) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			{ holderId, holderName, updatedAt: serverTimestamp() },
		);
	}

	// Stamp (or clear, with 0) the "actively advancing" lease. Merges onto the
	// authority doc the holder already owns, so it passes the same rule as the
	// sim authority itself. Followers read this to know a sim is in flight (see
	// Authority.busyUntil).
	async publishBusy(busyUntil: number, position?: LeaguePosition) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			// The position rides along on the lease write rather than costing its
			// own doc write: the holder already stamps this doc at the start and end
			// of every advance, which is exactly when the league has moved.
			{ busyUntil, ...(position ? { position } : {}) },
			{ merge: true },
		);
	}

	// Merge THIS device's draft ready entry onto the shared ready doc (null
	// clears it). Firestore merges are per-field, so devices writing their own
	// uid-keyed entries never clobber each other; holderId is stamped to our own
	// uid on every write so the control-doc rule passes.
	async publishDraftReady(entry: DraftReadyEntry | null, clearUids?: string[]) {
		// Readiness is per TEAM, so a device may also clear the entries of its
		// OWN team's other devices (clearUids) - otherwise "Not ready" on one
		// device couldn't revoke a ready published from another. The control-doc
		// rule only requires holderId to be our own uid.
		const ready: Record<string, DraftReadyEntry | null> = {
			[this.clientId]: entry,
		};
		for (const uid of clearUids ?? []) {
			ready[uid] = null;
		}
		await setDoc(
			doc(this.db, "leagues", this.code, "control", DRAFT_READY_DOC_ID),
			{
				holderId: this.clientId,
				ready,
				updatedAt: serverTimestamp(),
			},
			{ merge: true },
		);
		this.markContact();
	}

	// ---- Live game chat ------------------------------------------------------
	// Keyed by MESSAGE id rather than by device, so two people typing at the
	// same instant merge instead of overwriting each other. holderId is stamped
	// to our own uid like every other control-doc write, so the deployed rule
	// passes and no rules republish is needed for this feature.
	async publishLiveChatMessage(message: LiveGameChatMessage): Promise<void> {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", LIVE_CHAT_DOC_ID),
			{
				holderId: this.clientId,
				messages: { [message.id]: message },
				updatedAt: serverTimestamp(),
			},
			{ merge: true },
		);
		this.markContact();
	}

	subscribeLiveChat(
		onChange: (messages: LiveGameChatMessage[]) => void,
	): () => void {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", LIVE_CHAT_DOC_ID),
			(snap) => {
				const raw = snap.data()?.messages;
				if (!raw || typeof raw !== "object") {
					onChange([]);
					return;
				}
				this.markContact();
				onChange(
					Object.values(raw).filter(
						(m: any) =>
							m &&
							typeof m.id === "string" &&
							typeof m.cursor === "number" &&
							typeof m.text === "string",
					) as LiveGameChatMessage[],
				);
			},
			() => {
				// Chat dropping out must never be treated as a sync failure.
			},
		);
	}

	// A whole-document overwrite (no merge) so the previous game's messages are
	// actually gone rather than merged into the new broadcast.
	async clearLiveChat(): Promise<void> {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", LIVE_CHAT_DOC_ID),
			{
				holderId: this.clientId,
				messages: {},
				updatedAt: serverTimestamp(),
			},
		);
		this.markContact();
	}

	// Merge THIS device's free-agency board entry onto the shared board doc
	// (null clears it). Same per-uid merge semantics as publishDraftReady.
	async publishFaBoard(entry: FaBoardEntry | null) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", FA_BOARD_DOC_ID),
			{
				holderId: this.clientId,
				boards: { [this.clientId]: entry },
				updatedAt: serverTimestamp(),
			},
			{ merge: true },
		);
		this.markContact();
	}

	async publishTriviaScores(entries: TriviaScoreEntry[]) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", TRIVIA_SCORES_DOC_ID),
			{
				holderId: this.clientId,
				scores: { [this.clientId]: entries },
				updatedAt: serverTimestamp(),
			},
			{ merge: true },
		);
		this.markContact();
	}

	// Watch the room's trivia results. Fires with the current map, then on every
	// change.
	subscribeTriviaScores(
		onChange: (
			scores: Record<string, TriviaScoreEntry[] | null> | undefined,
		) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", TRIVIA_SCORES_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				onChange(
					data && typeof data.scores === "object" && data.scores !== null
						? (data.scores as Record<string, TriviaScoreEntry[] | null>)
						: undefined,
				);
			},
			(error) => {
				console.error("Trivia scores subscription failed", error);
			},
		);
	}

	// Watch everyone's free-agency board entries. Fires with the current map,
	// then on every change.
	subscribeFaBoard(
		onChange: (boards: Record<string, FaBoardEntry | null> | undefined) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", FA_BOARD_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				onChange(
					data && typeof data.boards === "object" && data.boards !== null
						? (data.boards as Record<string, FaBoardEntry | null>)
						: undefined,
				);
			},
			(error) => {
				console.error("FA board subscription failed", error);
			},
		);
	}

	// Watch everyone's draft ready entries. Fires with the current map, then on
	// every change.
	subscribeDraftReady(
		onChange: (
			ready: Record<string, DraftReadyEntry | null> | undefined,
		) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", DRAFT_READY_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				onChange(
					data && typeof data.ready === "object" && data.ready !== null
						? (data.ready as Record<string, DraftReadyEntry | null>)
						: undefined,
				);
			},
			(error) => {
				console.error("Draft ready subscription failed", error);
			},
		);
	}

	// Merge the lottery-reveal cursor doc (see LOTTERY_REVEAL_DOC_ID). holderId
	// is stamped so the control-doc rule passes.
	async publishLotteryReveal(update: LotteryRevealUpdate) {
		const clean: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(update)) {
			if (value !== undefined) {
				clean[key] = value;
			}
		}
		await setDoc(
			doc(this.db, "leagues", this.code, "control", LOTTERY_REVEAL_DOC_ID),
			{ ...clean, holderId: this.clientId, updatedAt: serverTimestamp() },
			{ merge: true },
		);
		this.markContact();
	}

	// Watch the lottery-reveal cursor. Fires with the current value, then on
	// every heartbeat. Undefined when no reveal is being broadcast.
	subscribeLotteryReveal(
		onChange: (meta: LotteryRevealMeta | undefined) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", LOTTERY_REVEAL_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				if (
					data &&
					data.active &&
					typeof data.holderId === "string" &&
					typeof data.season === "number"
				) {
					onChange({
						holderId: data.holderId,
						active: true,
						season: data.season,
						revealed: typeof data.revealed === "number" ? data.revealed : -1,
						byName: typeof data.byName === "string" ? data.byName : "Someone",
						startedAt: typeof data.startedAt === "number" ? data.startedAt : 0,
						expiresAt: typeof data.expiresAt === "number" ? data.expiresAt : 0,
					});
				} else {
					onChange(undefined);
				}
			},
			(error) => {
				console.error("Lottery reveal subscription failed", error);
			},
		);
	}

	// Atomically claim the right to sim one specific step. The transaction
	// applies decideAdvanceClaim: exactly one device per (draftKey, step) wins
	// within the lease window, AND a step at or below the stage's high-water mark
	// can never be claimed again (except crash recovery of the newest,
	// uncompleted step) - so a stale rejoining device can't re-sim finished
	// steps and publish regressed state as new history.
	async claimDraftAdvance(
		draftKey: string,
		pick: number,
		leaseMs: number,
	): Promise<boolean> {
		const ref = doc(
			this.db,
			"leagues",
			this.code,
			"control",
			DRAFT_ADVANCE_DOC_ID,
		);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const data = snap.data() as AdvanceClaimDoc | undefined;
				const decision = decideAdvanceClaim(data, {
					draftKey,
					pick,
					now: Date.now(),
					leaseMs,
				});
				if (!decision.grant) {
					throw new Error(`Advance claim rejected: ${decision.reason}`);
				}
				tx.set(ref, {
					holderId: this.clientId,
					draftKey,
					pick,
					at: Date.now(),
					maxPick: decision.maxPick,
					completed: false,
				});
			});
			this.markContact();
			return true;
		} catch {
			return false;
		}
	}

	// Mark this device's claimed step as finished, closing its crash-recovery
	// re-claim window. Best-effort: a failure just leaves the lease to expire.
	async completeDraftAdvance(draftKey: string, pick: number): Promise<void> {
		const ref = doc(
			this.db,
			"leagues",
			this.code,
			"control",
			DRAFT_ADVANCE_DOC_ID,
		);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const data = snap.data() as AdvanceClaimDoc | undefined;
				if (
					data &&
					data.draftKey === draftKey &&
					data.pick === pick &&
					data.holderId === this.clientId
				) {
					tx.set(ref, { ...data, completed: true });
				}
			});
			this.markContact();
		} catch {
			// Lease expiry covers it.
		}
	}

	// Atomically claim the right to sim one slice of a schedule day (the whole
	// day, or a single live-simmed game). The transaction applies
	// decideSimDayClaim: a day below the season's high-water mark can never be
	// claimed again, and within the newest day an already-claimed game can never
	// be claimed again (except crash recovery of an uncompleted claim). This is
	// what makes a concurrent double-sim of the same day - which doubles every
	// read-modify-write aggregate while the game records collide by gid -
	// impossible, no matter what the advisory authority doc says.
	async claimSimDay(
		stageKey: string,
		day: number,
		gids: number[],
		leaseMs: number,
	): Promise<boolean> {
		const ref = doc(this.db, "leagues", this.code, "control", SIM_DAY_DOC_ID);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const data = snap.data() as SimDayClaimDoc | undefined;
				const decision = decideSimDayClaim(data, {
					stageKey,
					day,
					gids,
					now: Date.now(),
					leaseMs,
				});
				if (!decision.grant) {
					throw new Error(`Sim day claim rejected: ${decision.reason}`);
				}
				tx.set(ref, {
					holderId: this.clientId,
					stageKey,
					day: decision.day,
					gids: decision.gids,
					at: Date.now(),
					maxDay: decision.maxDay,
					completed: false,
				});
			});
			this.markContact();
			return true;
		} catch {
			return false;
		}
	}

	// Mark this device's claimed day-slice as finished, closing its
	// crash-recovery re-claim window. Best-effort: a failure just leaves the
	// lease to expire.
	async completeSimDay(stageKey: string, day: number): Promise<void> {
		const ref = doc(this.db, "leagues", this.code, "control", SIM_DAY_DOC_ID);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const data = snap.data() as SimDayClaimDoc | undefined;
				if (
					data &&
					data.stageKey === stageKey &&
					data.day === day &&
					data.holderId === this.clientId
				) {
					tx.set(ref, { ...data, completed: true });
				}
			});
			this.markContact();
		} catch {
			// Lease expiry covers it.
		}
	}

	// Watch who currently is in charge of simming. Fires immediately with the current
	// holder (or undefined if nobody has claimed it yet), then on every change.
	subscribeAuthority(
		onChange: (authority: Authority | undefined) => void,
		onError?: (error: unknown) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				if (data && typeof data.holderId === "string") {
					onChange({
						holderId: data.holderId,
						holderName:
							typeof data.holderName === "string" ? data.holderName : "Someone",
						busyUntil:
							typeof data.busyUntil === "number" ? data.busyUntil : undefined,
						position: parseLeaguePosition(data.position),
					});
				} else {
					onChange(undefined);
				}
			},
			onError,
		);
	}

	// Merge the live-sim broadcast cursor/meta doc. Always stamps holderId (== our
	// uid) so the control-doc rule passes, and strips undefined (Firestore rejects
	// it). Called once to open the broadcast and then rapidly to heartbeat the
	// cursor, so the payload is kept in SEPARATE docs (below) - this doc stays tiny.
	async publishLiveBroadcast(update: LiveBroadcastUpdate) {
		const clean: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(update)) {
			if (value !== undefined) {
				clean[key] = value;
			}
		}
		await setDoc(
			doc(this.db, "leagues", this.code, "control", LIVE_BROADCAST_DOC_ID),
			{ ...clean, holderId: this.clientId, updatedAt: serverTimestamp() },
			{ merge: true },
		);
		this.markContact();
	}

	// Write the immutable play-by-play payload (a serialized string) as one or
	// more docs, each under Firestore's size limit. Written BEFORE the meta doc
	// flips active, so a follower reacting to active:true can always read a
	// complete payload. Returns the number of chunks (recorded in the meta doc).
	async publishLiveBroadcastData(gid: number, serialized: string) {
		const chunks: string[] = [];
		for (let i = 0; i < serialized.length; i += LIVE_BROADCAST_CHUNK_BYTES) {
			chunks.push(serialized.slice(i, i + LIVE_BROADCAST_CHUNK_BYTES));
		}
		if (chunks.length === 0) {
			chunks.push("");
		}
		for (let i = 0; i < chunks.length; i++) {
			await setDoc(
				doc(
					this.db,
					"leagues",
					this.code,
					"control",
					`${LIVE_BROADCAST_DATA_PREFIX}${i}`,
				),
				{
					holderId: this.clientId,
					gid,
					data: chunks[i],
					updatedAt: serverTimestamp(),
				},
			);
		}
		this.markContact();
		return chunks.length;
	}

	// Watch the live-sim broadcast cursor/meta doc. Fires with the current value,
	// then on every heartbeat. Undefined when no broadcast is active.
	subscribeLiveBroadcast(
		onChange: (meta: LiveBroadcastMeta | undefined) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", LIVE_BROADCAST_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				if (
					data &&
					data.active &&
					typeof data.holderId === "string" &&
					typeof data.gid === "number"
				) {
					onChange({
						holderId: data.holderId,
						active: true,
						gid: data.gid,
						byName: typeof data.byName === "string" ? data.byName : "Someone",
						cursor: typeof data.cursor === "number" ? data.cursor : 0,
						paused: !!data.paused,
						speed: typeof data.speed === "number" ? data.speed : 7,
						gameOver: !!data.gameOver,
						startedAt: typeof data.startedAt === "number" ? data.startedAt : 0,
						chunkCount:
							typeof data.chunkCount === "number" ? data.chunkCount : 0,
						expiresAt: typeof data.expiresAt === "number" ? data.expiresAt : 0,
					});
				} else {
					onChange(undefined);
				}
			},
			(error) => {
				console.error("Live broadcast subscription failed", error);
			},
		);
	}

	// Reassemble the play-by-play payload from its chunk docs, in order. Returns
	// undefined if any chunk is missing (a broadcast that was cleared, or a
	// partial write) so the caller can bail instead of playing a corrupt payload.
	async fetchLiveBroadcastData(chunkCount: number) {
		let out = "";
		for (let i = 0; i < chunkCount; i++) {
			const snap = await getDoc(
				doc(
					this.db,
					"leagues",
					this.code,
					"control",
					`${LIVE_BROADCAST_DATA_PREFIX}${i}`,
				),
			);
			if (!snap.exists()) {
				return undefined;
			}
			out += (snap.data().data as string | undefined) ?? "";
		}
		this.markContact();
		return out;
	}

	// End the broadcast: flip the meta doc inactive (so followers unlock now) and
	// delete the payload chunks. Best-effort on the deletes - the lease would
	// expire them anyway if a delete fails.
	async clearLiveBroadcast(chunkCount: number) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", LIVE_BROADCAST_DOC_ID),
			{ holderId: this.clientId, active: false, updatedAt: serverTimestamp() },
			{ merge: true },
		);
		for (let i = 0; i < chunkCount; i++) {
			try {
				await deleteDoc(
					doc(
						this.db,
						"leagues",
						this.code,
						"control",
						`${LIVE_BROADCAST_DATA_PREFIX}${i}`,
					),
				);
			} catch {
				// Best-effort; the lease handles a leftover payload.
			}
		}
		this.markContact();
	}

	async publish(entry: Omit<ChangesetEntry, "seq">) {
		const payload = {
			id: entry.id,
			authorId: entry.authorId,
			action: entry.action,
			changeset: serializeChangeset(entry.changeset),
			// Chunk metadata (present only for bulk changes). Firestore rejects
			// undefined, so only include when set.
			...(entry.batchId !== undefined
				? {
						batchId: entry.batchId,
						chunkIndex: entry.chunkIndex,
						chunkCount: entry.chunkCount,
					}
				: {}),
			// String-part payload + display metadata (new-format bulk chunks).
			...(entry.payloadPart !== undefined
				? { payloadPart: entry.payloadPart }
				: {}),
			...(entry.records !== undefined ? { records: entry.records } : {}),
			...(entry.attrs !== undefined ? { attrs: entry.attrs } : {}),
			ts: serverTimestamp(),
			// When this entry becomes eligible for deletion. Inert until the
			// Firestore TTL policy on this field is enabled (see RETENTION.md), and
			// ignored entirely by readers - a device that is too far behind is
			// detected from the real contents of the log, not from this stamp.
			ttlAt: Timestamp.fromMillis(ttlAtMsFor(Date.now())),
		};

		// Retry transient failures (a network blip, a brief Firestore hiccup). A
		// dropped publish is unrecoverable - the change tracker was already drained,
		// so catch-up/resync can't replay a change that never reached the log.
		// A permanent failure exhausts the retries and throws, which leaves the
		// entry queued in the outbox for a later drain.
		const ref = doc(this.changesRef, entry.id);

		// Did a PREVIOUS attempt's buffered write actually land? The change log is
		// append-only (rules: update = false), so once the doc exists, every
		// re-send is rejected with permission-denied forever - even though OUR
		// content is already there. Without this check, one ack timeout whose
		// write later landed permanently wedged the FIFO upload queue behind an
		// "unpublishable" entry. Entry ids are uuids only we generated, so an
		// existing doc with our authorId IS this publish, delivered.
		const alreadyDelivered = async (): Promise<boolean> => {
			try {
				const snap = await withTimeout(
					getDocFromServer(ref),
					PUBLISH_ACK_TIMEOUT_MS,
				);
				return snap.exists() && snap.data()?.authorId === this.clientId;
			} catch {
				return false;
			}
		};

		let lastError: unknown;
		for (let attempt = 0; attempt < 3; attempt++) {
			try {
				await withTimeout(setDoc(ref, payload), PUBLISH_ACK_TIMEOUT_MS);
				this.markContact();
				return;
			} catch (error) {
				lastError = error;
				if (await alreadyDelivered()) {
					this.markContact();
					return;
				}
				await new Promise((resolve) => setTimeout(resolve, 300 * 2 ** attempt));
			}
		}
		throw lastError;
	}

	// Parse a stored change doc into a ChangesetEntry, or undefined if it has no
	// server timestamp yet (a pending local write not yet confirmed).
	private parseEntry(
		docSnap: QueryDocumentSnapshot,
	): ChangesetEntry | undefined {
		const data = docSnap.data();
		if (!data.ts) {
			return undefined;
		}
		return {
			id: data.id,
			authorId: data.authorId,
			action: data.action,
			seq: typeof data.ts.toMillis === "function" ? data.ts.toMillis() : 0,
			changeset: deserializeChangeset(data.changeset),
			batchId: data.batchId,
			chunkIndex: data.chunkIndex,
			chunkCount: data.chunkCount,
			payloadPart:
				typeof data.payloadPart === "string" ? data.payloadPart : undefined,
			records: typeof data.records === "number" ? data.records : undefined,
			attrs: Array.isArray(data.attrs) ? data.attrs : undefined,
		};
	}

	// Reads that catch-up and recovery depend on go straight to the SERVER.
	//
	// Plain getDocs() can wait indefinitely when the SDK's connection is wedged -
	// it is allowed to serve from cache or hold out for the server, and it does not
	// consider "I cannot reach the server" an error. That is how a device ended up
	// pinned at "catching up 0%": the aggregate count (which always hits the
	// server) answered "92 entries to go", and the very next getDocs() for those
	// entries never came back. getDocsFromServer rejects promptly instead, which is
	// the failure the retry path is built for. These reads must be authoritative
	// anyway - serving the backlog from a stale local cache would be wrong.
	// The whole log, oldest-first, for full-resync recovery.
	//
	// PAGED, not one query. As a single unbounded read this is the request that
	// wedged a phone: the connect-time auto-resync runs it on every connect
	// while its marker is set, and on a league deep into a season the log is
	// thousands of documents including every bulk-sim chunk. That request never
	// came back, so the resync never finished, so the marker never cleared, so
	// the next connect tried the same thing - with the catch-up drain stuck
	// behind it showing 0% the whole time.
	//
	// Paged with startAfter on the document itself rather than a timestamp
	// cursor: two entries can share a millisecond, and a "ts >" cursor would
	// silently skip the second one. A resync that quietly drops entries is worse
	// than a slow one.
	async fetchAllEntries(): Promise<ChangesetEntry[]> {
		const entries: ChangesetEntry[] = [];
		let after: QueryDocumentSnapshot | undefined;

		for (let page = 0; page < FULL_LOG_MAX_PAGES; page++) {
			const snapshot = await getDocsFromServer(
				query(
					this.changesRef,
					orderBy("ts"),
					...(after ? [startAfter(after)] : []),
					limit(FULL_LOG_PAGE_SIZE),
				),
			);
			this.markContact();
			for (const docSnap of snapshot.docs) {
				const entry = this.parseEntry(docSnap);
				if (entry) {
					entries.push(entry);
				}
			}
			syncDebugLog("transport:full-log-page", {
				page,
				docs: snapshot.size,
				entriesSoFar: entries.length,
			});
			if (snapshot.size < FULL_LOG_PAGE_SIZE) {
				return entries;
			}
			after = snapshot.docs.at(-1);
			if (!after) {
				return entries;
			}
		}

		// Hit the page cap. Returning a TRUNCATED log would make the resync look
		// complete while missing the newest entries - exactly the "conclusive"
		// signal that clears the recovery marker. Refuse instead.
		throw new Error(
			`Change log is longer than ${FULL_LOG_MAX_PAGES * FULL_LOG_PAGE_SIZE} entries`,
		);
	}

	// Every chunk of one bulk batch, straight from the log by batchId - no seq
	// range, no watermark, so it finds chunks a device's ordered fetches can no
	// longer reach (below its watermark). Single-field equality query, so no
	// composite index is needed; callers sort by chunkIndex themselves.
	async fetchBatchEntries(batchId: string): Promise<ChangesetEntry[]> {
		const snapshot = await getDocsFromServer(
			query(this.changesRef, where("batchId", "==", batchId)),
		);
		this.markContact();
		const entries: ChangesetEntry[] = [];
		for (const docSnap of snapshot.docs) {
			const entry = this.parseEntry(docSnap);
			if (entry) {
				entries.push(entry);
			}
		}
		return entries;
	}

	// Read the entries after a given server-timestamp, oldest-first. With
	// `pageLimit` this returns just ONE bounded page (the oldest that many),
	// letting the engine drain a huge backlog page by page instead of pulling the
	// whole thing in one query - which on a phone that's been away for weeks would
	// time out / run out of memory and never complete. Without a limit it reads
	// everything after `sinceMs` (used where the set is known to be small).
	async fetchEntriesSince(
		sinceMs: number,
		pageLimit?: number,
	): Promise<ChangesetEntry[]> {
		const constraints = [
			where("ts", ">", Timestamp.fromMillis(sinceMs)),
			orderBy("ts"),
			...(pageLimit !== undefined ? [limit(pageLimit)] : []),
		];
		const snapshot = await getDocsFromServer(
			query(this.changesRef, ...constraints),
		);
		this.markContact();
		const entries: ChangesetEntry[] = [];
		for (const docSnap of snapshot.docs) {
			const entry = this.parseEntry(docSnap);
			if (entry) {
				entries.push(entry);
			}
		}
		return entries;
	}

	// Move the watermark the live subscription starts from. Called after the
	// paginated backlog drain so the real-time listener's INITIAL snapshot is
	// small (just the live tail), instead of re-loading the whole backlog we just
	// drained.
	updateSince(ts: number) {
		this.sinceTs = ts;
	}

	// ---- Room snapshot (full-state checkpoint) -----------------------------

	// Every publish writes its payload to a FRESH generation of chunk docs and
	// only then repoints the meta. Writing to fixed doc ids (the old behavior)
	// meant a multi-minute publish overwrote, one doc at a time, the very
	// payload the live meta still pointed at - so any device restoring during
	// that window reassembled a mix of two snapshots and got a corrupt league.
	// Generations make the payload immutable once written: nothing a publisher
	// does can damage the snapshot readers are currently allowed to see.
	async publishRoomSnapshot(
		meta: { seq: number; at: number; byName: string; position?: unknown },
		serialized: string,
	): Promise<number> {
		const generation = `${meta.seq}-${this.clientId}`;
		const chunks: string[] = [];
		for (let i = 0; i < serialized.length; i += LIVE_BROADCAST_CHUNK_BYTES) {
			chunks.push(serialized.slice(i, i + LIVE_BROADCAST_CHUNK_BYTES));
		}
		if (chunks.length === 0) {
			chunks.push("");
		}

		const previous = await this.fetchRoomSnapshotMeta();

		for (let i = 0; i < chunks.length; i++) {
			await setDoc(
				doc(
					this.db,
					"leagues",
					this.code,
					"control",
					`${ROOM_SNAPSHOT_DATA_PREFIX}_${generation}_${i}`,
				),
				{
					holderId: this.clientId,
					index: i,
					data: chunks[i],
					updatedAt: serverTimestamp(),
				},
			);
		}
		await setDoc(
			doc(this.db, "leagues", this.code, "control", ROOM_SNAPSHOT_DOC_ID),
			{
				holderId: this.clientId,
				seq: meta.seq,
				at: meta.at,
				byName: meta.byName,
				position: meta.position ?? null,
				chunkCount: chunks.length,
				generation,
				updatedAt: serverTimestamp(),
			},
		);
		this.markContact();

		// Only now is the previous generation unreachable. Best effort: a leaked
		// chunk doc costs storage, a prematurely deleted one costs a league.
		if (previous?.generation !== undefined) {
			for (let i = 0; i < previous.chunkCount; i++) {
				try {
					await deleteDoc(
						doc(
							this.db,
							"leagues",
							this.code,
							"control",
							`${ROOM_SNAPSHOT_DATA_PREFIX}_${previous.generation}_${i}`,
						),
					);
				} catch {
					// Housekeeping only.
				}
			}
		}

		return chunks.length;
	}

	async fetchRoomSnapshotMeta() {
		const snap = await getDoc(
			doc(this.db, "leagues", this.code, "control", ROOM_SNAPSHOT_DOC_ID),
		);
		this.markContact();
		const data = snap.data();
		if (
			!data ||
			typeof data.seq !== "number" ||
			typeof data.chunkCount !== "number"
		) {
			return undefined;
		}
		return {
			seq: data.seq,
			at: typeof data.at === "number" ? data.at : 0,
			byName: typeof data.byName === "string" ? data.byName : "Someone",
			chunkCount: data.chunkCount,
			position: data.position ?? undefined,
			generation:
				typeof data.generation === "string" ? data.generation : undefined,
		};
	}

	async fetchRoomSnapshotData(chunkCount: number, generation?: string) {
		let out = "";
		for (let i = 0; i < chunkCount; i++) {
			const snap = await getDoc(
				doc(
					this.db,
					"leagues",
					this.code,
					"control",
					generation === undefined
						? `${ROOM_SNAPSHOT_DATA_PREFIX}${i}`
						: `${ROOM_SNAPSHOT_DATA_PREFIX}_${generation}_${i}`,
				),
			);
			if (!snap.exists()) {
				return undefined;
			}
			const data = snap.data();
			if (typeof data.data !== "string") {
				return undefined;
			}
			out += data.data;
		}
		this.markContact();
		return out;
	}

	// ---- Sync v2 (version chain) -------------------------------------------
	//
	// The pointer doc is the room. It moves ONLY by compare-and-set inside a
	// Firestore transaction, so two writers cannot both advance it - the chain
	// cannot fork at the source. Delta and checkpoint payload docs embed their
	// version in the doc id and are never rewritten: publishing is always
	// content first, pointer last, onto ids nobody else will ever write.

	// Server-only by default. The pointer is the protocol: a cached read here
	// made a device target an already-committed version 0.3s after its OWN
	// commit (getDoc served the listener's not-yet-updated view), producing
	// CAS-loss storms - and with a dead listener the cache never advances at
	// all, so a cached answer let the 5s head probe "confirm" a stale head and
	// go back to sleep while the room moved on. A probe must fail loudly, not
	// lie comfortably. allowCache exists for exactly one caller: connect-time
	// protocol detection, where a stale answer and no answer are equally
	// harmless.
	async fetchRoomV2State(options?: {
		allowCache?: boolean;
	}): Promise<V2StateDoc | undefined> {
		const ref = doc(this.db, "leagues", this.code, "control", V2_STATE_DOC_ID);
		let snap;
		if (options?.allowCache) {
			try {
				snap = await getDocFromServer(ref);
			} catch {
				snap = await getDoc(ref);
			}
		} else {
			snap = await getDocFromServer(ref);
		}
		this.markContact();
		return this.parseV2State(snap.data());
	}

	// ---- Room <-> league binding ---------------------------------------------
	// One tiny doc that says which league lineage this room belongs to. Written
	// once, transactionally, by the first league to connect after this existed;
	// every later connect compares against it and refuses on mismatch. This is
	// the front-door half of the cross-league contamination fix - the payload
	// identity check in roomSnapshot.ts is the back-door half.

	async fetchRoomLeagueId(): Promise<string | undefined> {
		const ref = doc(this.db, "leagues", this.code, "control", "league");
		const snap = await getDocFromServer(ref);
		this.markContact();
		const value = snap.data()?.leagueId;
		return typeof value === "string" && value !== "" ? value : undefined;
	}

	async claimRoomLeagueId(leagueId: string): Promise<string> {
		const ref = doc(this.db, "leagues", this.code, "control", "league");
		let bound = leagueId;
		await runTransaction(this.db, async (tx) => {
			const snap = await tx.get(ref);
			const existing = snap.data()?.leagueId;
			if (typeof existing === "string" && existing !== "") {
				bound = existing;
				return;
			}
			// holderId stamped to our own uid, like every other control-doc
			// write: the deployed security rule (write requires holderId ==
			// auth.uid) covers this doc with no rules republish.
			tx.set(ref, { leagueId, holderId: this.clientId, at: Date.now() });
		});
		this.markContact();
		return bound;
	}

	// Force the SDK to tear down and rebuild its backend channel. The one known
	// cure for a wedged WebChannel (Safari is fond of killing the stream in a
	// way the SDK doesn't notice): listeners go quiet and server reads hang
	// until the connection is cycled.
	async cycleNetwork(): Promise<void> {
		try {
			await disableNetwork(this.db);
		} finally {
			await enableNetwork(this.db);
		}
	}

	private parseV2State(data: any): V2StateDoc | undefined {
		if (!data || typeof data.version !== "number") {
			return undefined;
		}
		return {
			version: data.version,
			authorId: typeof data.authorId === "string" ? data.authorId : "",
			byName: typeof data.byName === "string" ? data.byName : "Someone",
			at: typeof data.at === "number" ? data.at : 0,
			action: typeof data.action === "string" ? data.action : undefined,
			inlineDelta: typeof data.delta === "string" ? data.delta : undefined,
			checkpointVersion:
				typeof data.checkpointVersion === "number"
					? data.checkpointVersion
					: undefined,
			checkpointChunkCount:
				typeof data.checkpointChunkCount === "number"
					? data.checkpointChunkCount
					: undefined,
			checkpointGeneration:
				typeof data.checkpointGeneration === "string"
					? data.checkpointGeneration
					: undefined,
		};
	}

	subscribeRoomV2State(
		onChange: (state: V2StateDoc) => void,
		onError?: (error: unknown) => void,
	): () => void {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", V2_STATE_DOC_ID),
			(snap) => {
				const state = this.parseV2State(snap.data());
				if (state) {
					this.markContact();
					onChange(state);
				}
			},
			// Without this, a terminated listener (permissions blip, stream error)
			// died SILENTLY and the device dropped to timer-paced catch-up - which
			// looked like "picks take 30 seconds to arrive".
			(error) => {
				syncDebugLog("v2:state-listener-error", { error: String(error) });
				onError?.(error);
			},
		);
	}

	// Write version N's payload chunks. Chunk 0 carries the chunkCount, so the
	// payload is self-describing - a reader needs nothing but the version.
	//
	// Runs as ONE transaction that also READS the version pointer: if the slot
	// is already committed (pointer >= N), the write aborts with "v2-slot-taken"
	// instead of overwriting a version's payload after the room accepted it.
	// Without this, a device publishing from a stale pointer read could replace
	// an already-committed version's content with a DIFFERENT changeset - the
	// committed chain would then replay the wrong records onto every device.
	// Reading the pointer in the transaction also makes the write serializable
	// against commits: a commit landing mid-write forces a re-run, which then
	// sees the taken slot and aborts.
	async publishV2Delta(
		meta: { version: number; authorId: string; action: string; at: number },
		serialized: string,
	): Promise<number> {
		const chunks: string[] = [];
		for (let i = 0; i < serialized.length; i += LIVE_BROADCAST_CHUNK_BYTES) {
			chunks.push(serialized.slice(i, i + LIVE_BROADCAST_CHUNK_BYTES));
		}
		if (chunks.length === 0) {
			chunks.push("");
		}
		await runTransaction(this.db, async (tx) => {
			const stateSnap = await tx.get(
				doc(this.db, "leagues", this.code, "control", V2_STATE_DOC_ID),
			);
			const currentVersion = stateSnap.data()?.version;
			if (
				typeof currentVersion === "number" &&
				currentVersion >= meta.version
			) {
				throw new Error("v2-slot-taken");
			}
			for (let i = 0; i < chunks.length; i++) {
				tx.set(
					doc(
						this.db,
						"leagues",
						this.code,
						"control",
						v2DeltaDocId(meta.version, i),
					),
					{
						holderId: this.clientId,
						version: meta.version,
						index: i,
						chunkCount: chunks.length,
						data: chunks[i],
						authorId: meta.authorId,
						action: meta.action,
						at: meta.at,
						updatedAt: serverTimestamp(),
						// Age out like v1's changes do. These used to be pruned when a
						// checkpoint superseded them; nothing builds checkpoints any
						// more, so without a TTL they would accumulate forever. Safe to
						// stamp on delta chunks alone: a Firestore TTL policy only
						// touches documents that HAVE the field, so the state pointer,
						// the live broadcast and the chat sitting in this same
						// collection are untouched.
						ttlAt: Timestamp.fromMillis(ttlAtMsFor(Date.now())),
					},
				);
			}
		});
		this.markContact();
		return chunks.length;
	}

	// THE compare-and-set. True: the pointer moved expectedVersion -> next.
	// False: someone else moved it first; the caller catches up and never
	// overwrites. Checkpoint fields are preserved untouched.
	//
	// Beyond the pointer CAS, the transaction verifies the payload it is about
	// to commit is OURS: two devices racing for the same slot both write chunks
	// before either commits, and the loser's chunks can land on top of the
	// winner's. Committing then would put one device's action label on another
	// device's records. Reading chunk 0 in the same transaction (matched by
	// authorId + the publish's `at` stamp) means a commit can only succeed over
	// the exact payload this device just wrote.
	async commitV2Version(
		next: {
			version: number;
			authorId: string;
			byName: string;
			at: number;
			action: string;
			inlineDelta?: string;
		},
		expectedVersion: number,
	): Promise<boolean> {
		const ref = doc(this.db, "leagues", this.code, "control", V2_STATE_DOC_ID);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const current = snap.data();
				const currentVersion =
					current && typeof current.version === "number" ? current.version : 0;
				if (currentVersion !== expectedVersion) {
					throw new Error("cas-conflict");
				}
				// VERSION 0 IS THE ROOM-INITIALIZATION COMMIT AND HAS NO PAYLOAD.
				// It is the write that brings a v2 room into existence, before any
				// delta can exist; every real delta is version 1 or higher. The
				// ownership check below therefore has to skip it - when it did
				// not, it looked for a v2delta_0_0 document that by definition is
				// never written, threw cas-conflict, and room creation silently
				// fell back to v1. "I ticked v2 and got a v1 room" was this.
				if (next.version > 0) {
					const chunk0 = await tx.get(
						doc(
							this.db,
							"leagues",
							this.code,
							"control",
							v2DeltaDocId(next.version, 0),
						),
					);
					const chunkData = chunk0.data();
					if (
						!chunkData ||
						chunkData.authorId !== next.authorId ||
						chunkData.at !== next.at
					) {
						throw new Error("cas-conflict");
					}
				}
				tx.set(ref, {
					holderId: this.clientId,
					version: next.version,
					authorId: next.authorId,
					byName: next.byName,
					at: next.at,
					action: next.action,
					delta: next.inlineDelta ?? null,
					checkpointVersion:
						current && typeof current.checkpointVersion === "number"
							? current.checkpointVersion
							: null,
					checkpointChunkCount:
						current && typeof current.checkpointChunkCount === "number"
							? current.checkpointChunkCount
							: null,
					checkpointGeneration:
						current && typeof current.checkpointGeneration === "string"
							? current.checkpointGeneration
							: null,
					updatedAt: serverTimestamp(),
				});
			});
			this.markContact();
			return true;
		} catch (error) {
			if ((error as Error)?.message === "cas-conflict") {
				return false;
			}
			throw error;
		}
	}

	// Write the payload AND move the pointer in ONE transaction. Every ordinary
	// edit - a note, a roster move, a trade - is small enough to take this path,
	// and it halves the round trips: publishing used to be two transactions back
	// to back, which on a phone is most of a second per change. Filing a whole
	// season of team recaps was thirty of those in a row.
	//
	// Only for a payload that fits in a single chunk. A bigger one has to write
	// its chunks first (publishV2Delta) so the pointer never points at a payload
	// that isn't fully there.
	//
	// It also retires the ownership check the split needed. That check existed
	// because two devices racing for the same slot could both write chunks, and
	// the loser's could land on top of the winner's between its write and its
	// commit. There is no "between" here: a transaction either writes both docs
	// or neither, so the winner's payload and pointer are inseparable.
	async publishAndCommitV2Version(
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
	): Promise<boolean> {
		if (serialized.length > LIVE_BROADCAST_CHUNK_BYTES) {
			throw new Error("v2-payload-not-single-chunk");
		}
		const ref = doc(this.db, "leagues", this.code, "control", V2_STATE_DOC_ID);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const current = snap.data();
				const currentVersion =
					current && typeof current.version === "number" ? current.version : 0;
				if (currentVersion !== expectedVersion) {
					throw new Error("cas-conflict");
				}
				tx.set(
					doc(
						this.db,
						"leagues",
						this.code,
						"control",
						v2DeltaDocId(next.version, 0),
					),
					{
						holderId: this.clientId,
						version: next.version,
						index: 0,
						chunkCount: 1,
						data: serialized,
						authorId: next.authorId,
						action: next.action,
						at: next.at,
						updatedAt: serverTimestamp(),
						ttlAt: Timestamp.fromMillis(ttlAtMsFor(Date.now())),
					},
				);
				tx.set(ref, {
					holderId: this.clientId,
					version: next.version,
					authorId: next.authorId,
					byName: next.byName,
					at: next.at,
					action: next.action,
					delta: next.inlineDelta ?? null,
					checkpointVersion:
						current && typeof current.checkpointVersion === "number"
							? current.checkpointVersion
							: null,
					checkpointChunkCount:
						current && typeof current.checkpointChunkCount === "number"
							? current.checkpointChunkCount
							: null,
					checkpointGeneration:
						current && typeof current.checkpointGeneration === "string"
							? current.checkpointGeneration
							: null,
					updatedAt: serverTimestamp(),
				});
			});
			this.markContact();
			return true;
		} catch (error) {
			if ((error as Error)?.message === "cas-conflict") {
				return false;
			}
			throw error;
		}
	}

	async fetchV2Delta(version: number): Promise<
		| {
				serialized: string;
				authorId: string;
				action: string;
				at: number;
		  }
		| undefined
	> {
		const first = await getDoc(
			doc(this.db, "leagues", this.code, "control", v2DeltaDocId(version, 0)),
		);
		if (!first.exists()) {
			return undefined;
		}
		const head = first.data();
		if (typeof head.data !== "string" || typeof head.chunkCount !== "number") {
			return undefined;
		}
		let out = head.data;
		for (let i = 1; i < head.chunkCount; i++) {
			const snap = await getDoc(
				doc(this.db, "leagues", this.code, "control", v2DeltaDocId(version, i)),
			);
			const data = snap.data();
			// Every chunk must agree with chunk 0 about who wrote it and how many
			// chunks there are. A zombie ex-authority retrying an uncommitted
			// publish can overwrite individual chunk docs while the real winner's
			// commit stands; a mixed read must come back "unreadable" (the caller
			// retries) rather than reassembling two writers' bytes into one
			// payload.
			if (
				!snap.exists() ||
				typeof data?.data !== "string" ||
				data.authorId !== head.authorId ||
				data.chunkCount !== head.chunkCount
			) {
				return undefined;
			}
			out += data.data;
		}
		this.markContact();
		return {
			serialized: out,
			authorId: typeof head.authorId === "string" ? head.authorId : "",
			action: typeof head.action === "string" ? head.action : "",
			at: typeof head.at === "number" ? head.at : 0,
		};
	}

	async publishV2Checkpoint(
		version: number,
		serialized: string,
		generation?: string,
	): Promise<number> {
		const chunks: string[] = [];
		for (let i = 0; i < serialized.length; i += LIVE_BROADCAST_CHUNK_BYTES) {
			chunks.push(serialized.slice(i, i + LIVE_BROADCAST_CHUNK_BYTES));
		}
		if (chunks.length === 0) {
			chunks.push("");
		}
		for (let i = 0; i < chunks.length; i++) {
			await setDoc(
				doc(
					this.db,
					"leagues",
					this.code,
					"control",
					v2CheckpointDocId(version, i, generation),
				),
				{
					holderId: this.clientId,
					version,
					index: i,
					chunkCount: chunks.length,
					data: chunks[i],
					generation: generation ?? null,
					updatedAt: serverTimestamp(),
				},
			);
		}
		this.markContact();
		return chunks.length;
	}

	// Point the room at a published checkpoint. Transactional so it can never
	// clobber a concurrent version bump - it rewrites ONLY the checkpoint
	// fields around whatever version is current.
	async commitV2Checkpoint(
		version: number,
		chunkCount: number,
		generation?: string,
	): Promise<boolean> {
		const ref = doc(this.db, "leagues", this.code, "control", V2_STATE_DOC_ID);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				if (!snap.exists()) {
					throw new Error("no-state");
				}
				tx.update(ref, {
					holderId: this.clientId,
					checkpointVersion: version,
					checkpointChunkCount: chunkCount,
					checkpointGeneration: generation ?? null,
					updatedAt: serverTimestamp(),
				});
			});
			this.markContact();
			return true;
		} catch {
			return false;
		}
	}

	async fetchV2Checkpoint(
		version: number,
		chunkCount: number,
		generation?: string,
	): Promise<string | undefined> {
		let out = "";
		for (let i = 0; i < chunkCount; i++) {
			const snap = await getDoc(
				doc(
					this.db,
					"leagues",
					this.code,
					"control",
					v2CheckpointDocId(version, i, generation),
				),
			);
			const data = snap.data();
			if (!snap.exists() || typeof data?.data !== "string") {
				return undefined;
			}
			out += data.data;
		}
		this.markContact();
		return out;
	}

	// Prune delta docs for versions below `version` (already covered by a
	// checkpoint). Chunk 0 tells us each version's chunkCount.
	async deleteV2DeltasBefore(version: number): Promise<number> {
		let deleted = 0;
		for (let v = Math.max(1, version - 500); v < version; v++) {
			const first = await getDoc(
				doc(this.db, "leagues", this.code, "control", v2DeltaDocId(v, 0)),
			);
			if (!first.exists()) {
				continue;
			}
			const chunkCount =
				typeof first.data().chunkCount === "number"
					? first.data().chunkCount
					: 1;
			for (let i = 0; i < chunkCount; i++) {
				try {
					await deleteDoc(
						doc(this.db, "leagues", this.code, "control", v2DeltaDocId(v, i)),
					);
					deleted += 1;
				} catch {
					// Housekeeping.
				}
			}
		}
		return deleted;
	}

	// Prune log entries older than seqMs, in pages so one call never hangs on a
	// giant backlog. Safe by protocol: only ever called with the PREVIOUS
	// snapshot's seq, so everything deleted is covered by two snapshots.
	async deleteEntriesBefore(seqMs: number): Promise<number> {
		let deleted = 0;
		for (let page = 0; page < 20; page++) {
			const snapshot = await getDocsFromServer(
				query(
					this.changesRef,
					where("ts", "<", Timestamp.fromMillis(seqMs)),
					orderBy("ts"),
					limit(200),
				),
			);
			if (snapshot.empty) {
				break;
			}
			for (const docSnap of snapshot.docs) {
				await deleteDoc(docSnap.ref);
				deleted++;
			}
			if (snapshot.docs.length < 200) {
				break;
			}
		}
		this.markContact();
		return deleted;
	}

	// The seq (server-timestamp millis) of the OLDEST entry still in the log, or
	// undefined if the log is empty. One doc read, used on connect to tell
	// "behind" apart from "so far behind that the entries we need were deleted".
	async fetchOldestEntrySeq(): Promise<number | undefined> {
		const snapshot = await getDocsFromServer(
			query(this.changesRef, orderBy("ts"), limit(1)),
		);
		this.markContact();
		for (const docSnap of snapshot.docs) {
			const entry = this.parseEntry(docSnap);
			if (entry) {
				return entry.seq;
			}
		}
		return undefined;
	}

	// How many entries are still after our watermark, via a cheap server-side
	// aggregate count (no docs read). Powers the "catching up …%" progress total
	// so a returning device can show how far it has to go.
	async countEntriesSince(sinceMs: number): Promise<number> {
		const snap = await getCountFromServer(
			query(this.changesRef, where("ts", ">", Timestamp.fromMillis(sinceMs))),
		);
		this.markContact();
		return snap.data().count;
	}

	// The most recent `n` entries (returned oldest-first, like fetchAllEntries).
	// The sync-activity panel only shows recent activity, so it must never read the
	// whole (possibly enormous) log just to render a list - that's what left it
	// stuck on "Loading…" for a device far behind.
	async fetchRecentEntries(n: number): Promise<ChangesetEntry[]> {
		// Paged, walking backwards from the head, for the same reason the full log
		// is: `n` is now big enough (a bounded resync window) that one query for
		// all of it is the same unbounded request that wedged a phone.
		const collected: ChangesetEntry[] = [];
		let after: QueryDocumentSnapshot | undefined;
		while (collected.length < n) {
			const pageSize = Math.min(FULL_LOG_PAGE_SIZE, n - collected.length);
			const page = await getDocsFromServer(
				query(
					this.changesRef,
					orderBy("ts", "desc"),
					...(after ? [startAfter(after)] : []),
					limit(pageSize),
				),
			);
			this.markContact();
			for (const docSnap of page.docs) {
				const entry = this.parseEntry(docSnap);
				if (entry) {
					collected.push(entry);
				}
			}
			if (page.size < pageSize) {
				break;
			}
			after = page.docs.at(-1);
			if (!after) {
				break;
			}
		}
		// Newest-first above; callers want oldest-first.
		return collected.reverse();
	}

	subscribe(subscriber: SyncSubscriber) {
		// Only entries after our watermark - the initial snapshot is the catch-up
		// (everything we missed), and later snapshots are live updates. Pending
		// local writes have a null ts and simply don't match `ts > x` until the
		// server confirms them.
		const q = query(
			this.changesRef,
			where("ts", ">", Timestamp.fromMillis(this.sinceTs)),
			orderBy("ts"),
		);

		// Process snapshots one at a time, in order, since applying is async.
		let chain: Promise<void> = Promise.resolve();

		const unsub = onSnapshot(
			q,
			(snapshot) => {
				// Any delivery (even an empty one) means the listener is live.
				this.markContact();
				const entries: ChangesetEntry[] = [];
				for (const change of snapshot.docChanges()) {
					if (change.type !== "added") {
						continue;
					}
					const entry = this.parseEntry(change.doc);
					if (entry) {
						entries.push(entry);
					}
				}

				if (entries.length === 0) {
					return;
				}

				syncDebugLog("transport:changes-snapshot", {
					added: entries.length,
					firstSeq: entries[0]!.seq,
					lastSeq: entries[entries.length - 1]!.seq,
				});

				chain = chain.then(async () => {
					for (const entry of entries) {
						await subscriber.onEntry(entry);
					}
					subscriber.onBatchProcessed?.();
				});
			},
			(error) => {
				// A failed listener (e.g. it choked on a huge initial snapshot, or the
				// token expired) must not silently kill sync: the paginated catch-up
				// timer is the backstop that keeps draining regardless.
				console.error("Changes subscription failed", error);
				subscriber.onError?.(error);
			},
		);

		return unsub;
	}
}
