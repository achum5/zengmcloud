import { SyncEngine } from "./SyncEngine.ts";
import { FirebaseTransport } from "./FirebaseTransport.ts";
import { outbox } from "./outbox.ts";
import { ensureAnonymousAuth } from "./auth.ts";
import { setApplyGuard } from "./applyGuard.ts";
import { setupDraftReady, teardownDraftReady } from "./draftReady.ts";
import { setupFaBoard, teardownFaBoard } from "./faBoard.ts";
import { getSyncEngine, setSyncEngine } from "./engineHolder.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, local, lock, toUI } from "../../util/index.ts";
import { env } from "../../util/env.ts";
import { ERROR_MESSAGE_SYNC_ROOM_MISMATCH } from "../../../common/constants.ts";
import { serializeChangeset, deserializeChangeset } from "./serialize.ts";
import { syncDebugLog } from "./debugLog.ts";
import type { LiveBroadcastMeta } from "./types.ts";

// This device's catch-up watermark for a league, stored in the durable meta DB
// so it survives refreshes - so we only replay what we missed.
const loadWatermark = async (lid: number | undefined): Promise<number> => {
	if (typeof lid !== "number") {
		return 0;
	}
	const league = await idb.meta.get("leagues", lid);
	return league?.syncWatermark ?? 0;
};

const loadPersistedSyncSession = async (
	lid: number | undefined,
): Promise<{ code: string; isHost: boolean } | undefined> => {
	if (typeof lid !== "number") {
		return undefined;
	}
	const league = await idb.meta.get("leagues", lid);
	const code = league?.syncCode;
	if (typeof code === "string" && code.trim() !== "") {
		return { code: code.trim(), isHost: !!league?.syncIsHost };
	}
	return undefined;
};

const savePersistedSyncSession = async (
	lid: number | undefined,
	session: { code: string; isHost: boolean },
) => {
	if (typeof lid !== "number") {
		return;
	}
	const league = await idb.meta.get("leagues", lid);
	if (league) {
		league.syncCode = session.code.trim();
		league.syncIsHost = session.isHost;
		await idb.meta.put("leagues", league);
	}
};

const clearPersistedSyncSession = async (lid: number | undefined) => {
	if (typeof lid !== "number") {
		return;
	}
	const league = await idb.meta.get("leagues", lid);
	if (league) {
		delete league.syncCode;
		delete league.syncIsHost;
		await idb.meta.put("leagues", league);
	}
};

// The league file's room-binding fingerprint (see League.syncLeagueId).
const loadSyncLeagueId = async (
	lid: number | undefined,
): Promise<string | undefined> => {
	if (typeof lid !== "number") {
		return undefined;
	}
	const league = await idb.meta.get("leagues", lid);
	return typeof league?.syncLeagueId === "string"
		? league.syncLeagueId
		: undefined;
};

const saveSyncLeagueId = async (lid: number | undefined, leagueId: string) => {
	if (typeof lid !== "number") {
		return;
	}
	const league = await idb.meta.get("leagues", lid);
	if (league) {
		league.syncLeagueId = leagueId;
		await idb.meta.put("leagues", league);
	}
};

const makeLeagueId = (): string => {
	if (typeof crypto !== "undefined" && crypto.randomUUID) {
		return crypto.randomUUID();
	}
	return `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
};

// Minimum spacing between DURABLE watermark banks (cache flush + meta write).
// During chained ready-up picks a watermark advance fires per pick; flushing
// and writing meta every time hammered mobile browsers' IndexedDB ("meta
// database error event" / transaction aborts on iOS under write pressure).
// Skipping a bank is always safe: the in-memory watermark keeps advancing for
// dedup, and on a crash the device just re-fetches a few already-applied
// entries (idempotent whole-record writes).
const WATERMARK_BANK_MIN_MS = 4000;
let lastWatermarkBankAt = 0;
let watermarkTrailingTimer: ReturnType<typeof setTimeout> | undefined;

const saveWatermark = async (lid: number | undefined, ts: number) => {
	if (typeof lid !== "number") {
		return;
	}

	if (Date.now() - lastWatermarkBankAt < WATERMARK_BANK_MIN_MS) {
		// Too soon after the last durable bank. Schedule a trailing bank of
		// whatever the watermark is by then, so the tail of a burst still lands.
		if (watermarkTrailingTimer === undefined) {
			watermarkTrailingTimer = setTimeout(() => {
				watermarkTrailingTimer = undefined;
				const engine = getSyncEngine();
				if (engine) {
					void saveWatermark(lid, engine.getPersistedSeq());
				}
			}, WATERMARK_BANK_MIN_MS);
		}
		return;
	}

	// Never let the DURABLE watermark run ahead of the DURABLE league data. The
	// watermark lives in idb.meta and is written immediately; the data it accounts
	// for lives in the in-memory cache and only reaches idb.league on a flush. If
	// the app is killed in between (e.g. iOS backgrounding the PWA), the watermark
	// would claim "caught up" while the applied data was never saved - and since a
	// device won't re-fetch anything at/below its watermark, that change is lost
	// locally forever even though it's still in the cloud. So flush the cache FIRST,
	// then record the watermark.
	//
	// Skip entirely while a local sim / phase change / autoplay is running: those
	// batch their own flushes for speed, and a mid-sim flush here would both fight
	// that batching and (worse) bank a watermark ahead of not-yet-flushed sim data.
	// The periodic catch-up re-runs this once things settle, so nothing is skipped
	// permanently - the in-memory watermark keeps advancing for dedup regardless.
	if (lock.get("gameSim") || lock.get("newPhase") || local.autoPlayUntil) {
		return;
	}
	try {
		await idb.cache.flush();
	} catch {
		// A failed flush means the data isn't durable, so don't bank a watermark
		// past it - just try again on the next tick.
		return;
	}

	const league = await idb.meta.get("leagues", lid);
	if (league && (league.syncWatermark ?? 0) < ts) {
		league.syncWatermark = ts;
		await idb.meta.put("leagues", league);
	}
	lastWatermarkBankAt = Date.now();
};

// The room we're currently connected to (if any), so the UI can reflect
// connection state - including after an auto-reconnect it didn't drive.
let currentCode: string | undefined;

// Which league (lid) the current sync session belongs to. A sync room is tied to
// ONE league file, so switching to a DIFFERENT league must drop the session. This
// lets us disconnect precisely (connected-for-a-different-lid) instead of guessing
// from "was a league previously open", which missed the case of loading a new
// league straight from the New League page (where the previous lid was already
// cleared, so the old connection leaked into the new file's sync page).
let connectedLid: number | undefined;

// The lid the live sync session belongs to (undefined when not connected).
export const getConnectedLid = (): number | undefined => connectedLid;
// Name of whoever currently is in charge of simming (for display), from the shared doc.
let currentHostName: string | undefined;
let currentCloudReady = false;

// Whether this device is *supposed* to be in a sync session. Stays true across
// the async reconnect after a refresh, and even if that reconnect fails - so we
// can gate simming until the connection is actually live, instead of letting
// the device sim while offline and silently diverge from the league.
let syncRequired = false;

// A low-frequency timer that force-fetches anything after our watermark, so a
// device doesn't sit waiting on Firestore's real-time push (which stalls on a
// throttled phone - a big change could otherwise take many minutes to arrive).
// Worker timers are themselves throttled when the tab is backgrounded, so this
// effectively runs while the app is in the foreground, which is exactly when we
// want it. Undefined when not connected.
let catchUpTimer: ReturnType<typeof setInterval> | undefined;
// The live changes listener delivers new deltas in real time for free; this
// poll is only a BACKSTOP for a silently-dropped listener. So it runs
// infrequently, and each tick skips its (billed) catch-up read when the listener
// has proven itself alive by delivering within this window - which eliminates
// the steady-state read cost in an active room. A quiet/idle room has no
// deliveries, so it still probes once per interval to detect a dead listener.
const CATCH_UP_INTERVAL_MS = 30000;

// A delivery from the live CHANGES listener this recently proves game data is
// still flowing, so the poll can skip its confirming read (when also fully
// caught up - see the catchUpTimer). Kept BELOW the poll interval on purpose:
// if this window equalled the interval, a delivery's age would sit at ~the
// interval at each tick and sometimes read as "fresh", skipping the probe - a
// beat that fires only every other tick. Below the interval, a quiet listener
// reliably probes every tick.
const LISTENER_FRESH_MS = 20000;

// How many recent log entries the activity panel reads. Bounded so it renders a
// list instead of pulling a whole season's worth of change docs.
const SYNC_ACTIVITY_LIMIT = 200;

// Drop outbox entries older than this (a room the user never returned to), so a
// permanently-failed upload can't make the outbox grow without bound.
const OUTBOX_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;

// The header status dot goes red if we haven't confirmed live cloud contact in
// this long. Must exceed the catch-up interval so a HEALTHY but idle connection
// (which the backstop poll refreshes once per CATCH_UP_INTERVAL_MS) stays green;
// a genuinely dead one goes red after this.
const HEALTH_STALE_MS = 45000;
const HEALTH_TICK_MS = 5000;
let healthTimer: ReturnType<typeof setInterval> | undefined;
let lastHealthPushed: boolean | undefined;

// Monotonic count of confirmed uploads; the UI flashes a "synced ✓" when it ticks.
let uploadOkCounter = 0;

// The tid last written to this device's member doc. Targeted notifications are
// delivered by matching the member doc's tid, which was previously stamped only
// when push was first enabled - so switching teams silently un-targeted the
// device (its sim results / playoff pings went to the old team). The health
// tick re-stamps it whenever it drifts.
let lastMemberTid: number | undefined;

// How many local deltas are queued (durably) but not yet confirmed in the
// cloud. Mirrored to the UI so unuploaded changes are always visible instead of
// silently waiting.
let lastPendingUploads = 0;
const pushPendingUploads = (count: number) => {
	if (count !== lastPendingUploads) {
		lastPendingUploads = count;
		void toUI("updateLocal", [{ mpPendingUploads: count }]);
	}
};

const pushHealth = () => {
	const age = getSyncEngine()?.contactAge();
	const healthy = age !== undefined && age < HEALTH_STALE_MS;
	if (healthy !== lastHealthPushed) {
		lastHealthPushed = healthy;
		void toUI("updateLocal", [{ mpSyncHealthy: healthy }]);
	}
};

// Whether conflict-prone edits are blocked right now (the sim authority is
// mid-sim, or this device hasn't caught up). Pushed to the UI so the header can
// show a "simming…" indicator - so a blocked trade/roster move reads as expected
// rather than a glitch. Only meaningful on a follower; the sim authority is never
// blocked. Pushed on change from the authority subscription, the watermark
// advance, and the health tick (which also catches a lease that quietly expired).
let lastEditsPausedPushed: boolean | undefined;
const pushEditsPaused = () => {
	const engine = getSyncEngine();
	const paused =
		engine !== undefined &&
		!engine.isAuthority() &&
		(engine.isRoomBusy() || !engine.isCaughtUp());
	if (paused !== lastEditsPausedPushed) {
		lastEditsPausedPushed = paused;
		void toUI("updateLocal", [{ mpEditsPaused: paused }]);
	}
};

// Unconditionally re-assert the FULL multiplayer-sync UI state from the engine's
// actual state. The individual mpSync* fields are driven by several independent
// updateLocal pushes, and the UI's own resetLeague() zeroes them; either can
// leave the UI out of sync with a still-connected engine - e.g. showing "nobody
// simming" with the Play menu unlocked while the worker is really connected and
// following (the sim is correctly gated server-side, but the UI looks wrong
// until a manual refresh). This reads the engine directly (not the clobberable
// UI mirrors), so calling it heals any such drift. Called on every health tick
// and whenever the UI asks to re-sync, so the fix is automatic. Deliberately NOT
// deduped: the whole point is to overwrite a UI that has silently drifted, which
// the worker can't detect. Redundant identical pushes don't re-render the UI
// (selectors compare shallowly).
const pushSyncStateFull = () => {
	const engine = getSyncEngine();
	const authority = engine?.getAuthority();
	const age = engine?.contactAge();
	const healthy = age !== undefined && age < HEALTH_STALE_MS;
	const editsPaused =
		engine !== undefined &&
		!engine.isAuthority() &&
		(engine.isRoomBusy() || !engine.isCaughtUp());

	// Keep the dedup sentinels consistent so the event-driven pushers agree.
	lastHealthPushed = healthy;
	lastEditsPausedPushed = editsPaused;
	currentHostName = authority?.holderName;

	void toUI("updateLocal", [
		{
			mpSyncActive: engine !== undefined,
			mpSyncReconnecting: isReconnecting(),
			mpSyncIsHost: engine?.isAuthority() ?? false,
			mpSyncHostName: authority?.holderName,
			mpSyncReady: engine !== undefined && currentCloudReady,
			mpSyncHealthy: healthy,
			mpEditsPaused: editsPaused,
			mpPendingUploads: lastPendingUploads,
		},
	]);
};

// Re-push the current sync state to the UI on demand. The UI calls this when it
// may have drifted from the engine - notably when an auto-reconnect finds the
// worker's engine ALREADY connected (so it does no connect that would push
// state) but the UI local state was reset (e.g. the tab reloaded while a
// persistent worker kept the engine alive).
export const refreshSyncUIState = () => {
	pushSyncStateFull();
};

// The live transport + auto-play subscription for the current room, so the
// simmer can publish its schedule and every device can watch it.
let currentTransport: FirebaseTransport | undefined;
let autoPlayUnsub: (() => void) | undefined;

// ---------------------------------------------------------------------------
// Live-sim broadcast (Mode B: lockstep). When the sim authority live-sims a game,
// it publishes the immutable play-by-play once and heartbeats a moving cursor;
// every follower navigates to the live game and replays to that cursor, so all
// devices see exactly what the simmer sees, live. See types.ts LiveBroadcastMeta.
// ---------------------------------------------------------------------------

// How long a broadcast stays "live" without a heartbeat before followers treat
// it as ended (crash recovery). Generously above the ~400ms UI heartbeat.
const LIVE_BROADCAST_LEASE_MS = 12_000;

// The subscription every device keeps on the broadcast meta doc.
let liveBroadcastUnsub: (() => void) | undefined;

// Set on the BROADCASTER while it's broadcasting (for heartbeats + teardown).
let activeBroadcast:
	| { gid: number; startedAt: number; chunkCount: number; byName: string }
	| undefined;

// Set on a FOLLOWER while it's watching someone else's broadcast. Tracks which
// broadcast (startedAt) we've already navigated to, and the lease expiry so a
// crashed broadcaster unlocks us.
let followedBroadcast:
	| { startedAt: number; gid: number; expiresAt: number }
	| undefined;

// The followed broadcast's game payload, kept for the liveGame view to serve on
// ANY load of the page while the broadcast is live - the navigation that
// delivers it through the router can be silently dropped when the follower is
// already parked on the live game page (same-URL refreshes queue behind other
// updates and a navigation clears the queue), and that stale-props remount once
// produced a 206-point box score of two games merged. Cleared when the
// broadcast ends.
let followedBroadcastPayload:
	| { startedAt: number; gid: number; playByPlay: any[]; boxScore: any }
	| undefined;

export const getFollowedBroadcastPayload = () => followedBroadcastPayload;

// Start broadcasting a live sim to the room. No-op unless connected AND this
// device is in charge of simming (so single-player / followers never touch the cloud).
// The play-by-play + a snapshot of the game record go out ONCE as payload
// chunks; the moving cursor is heartbeated separately (updateLiveBroadcast).
export const startLiveBroadcast = async (gid: number, playByPlay: any[]) => {
	const engine = getSyncEngine();
	const transport = currentTransport;
	if (
		!engine ||
		!transport ||
		!engine.isAuthority() ||
		!transport.publishLiveBroadcast ||
		!transport.publishLiveBroadcastData
	) {
		return;
	}

	try {
		// The follower rebuilds the live sim from this exact game record (the same
		// one liveGame.ts would load from idb), so include it in the payload rather
		// than depend on the separate changeset sync having landed the game row yet.
		const boxScore = await idb.getCopy.games({ gid });
		if (!boxScore) {
			return;
		}

		const serialized = serializeChangeset({ boxScore, playByPlay });
		const chunkCount = await transport.publishLiveBroadcastData(
			gid,
			serialized,
		);

		const startedAt = Date.now();
		const byName = engine.localName;
		activeBroadcast = { gid, startedAt, chunkCount, byName };

		// Payload is written; now flip the meta doc active so followers react.
		await transport.publishLiveBroadcast({
			active: true,
			gid,
			byName,
			cursor: 0,
			paused: false,
			speed: 7,
			gameOver: false,
			startedAt,
			chunkCount,
			expiresAt: Date.now() + LIVE_BROADCAST_LEASE_MS,
		});

		// Tell our own UI it's broadcasting, so the LiveGame view heartbeats the
		// cursor and shows the "broadcasting" banner.
		void toUI("updateLocal", [
			{
				mpLiveBroadcast: {
					active: true,
					gid,
					byName,
					isBroadcaster: true,
					startedAt,
					cursor: 0,
					paused: false,
					gameOver: false,
				},
			},
		]);
	} catch (error) {
		console.error("startLiveBroadcast failed", error);
	}
};

// Heartbeat the broadcaster's playback position to the room. Called ~every
// 400ms from the LiveGame view while broadcasting. Cheap merge write; no-op if
// we're not actually broadcasting.
export const updateLiveBroadcast = async (update: {
	cursor: number;
	paused: boolean;
	speed: number;
	gameOver: boolean;
}) => {
	const transport = currentTransport;
	const broadcast = activeBroadcast;
	if (!transport || !broadcast || !transport.publishLiveBroadcast) {
		return;
	}

	// Mirror to our own UI first (instant, can't fail), then push to the cloud.
	void toUI("updateLocal", [
		{
			mpLiveBroadcast: {
				active: true,
				gid: broadcast.gid,
				byName: broadcast.byName,
				isBroadcaster: true,
				startedAt: broadcast.startedAt,
				cursor: update.cursor,
				paused: update.paused,
				gameOver: update.gameOver,
			},
		},
	]);

	try {
		await transport.publishLiveBroadcast({
			active: true,
			cursor: update.cursor,
			paused: update.paused,
			speed: update.speed,
			gameOver: update.gameOver,
			expiresAt: Date.now() + LIVE_BROADCAST_LEASE_MS,
		});
	} catch {
		// A dropped heartbeat is harmless - the next one re-stamps the lease.
	}
};

// End the current broadcast (the simmer left the live game, or it's being torn
// down). Unlocks followers immediately and removes the payload. Idempotent.
export const endLiveBroadcast = async () => {
	const transport = currentTransport;
	const broadcast = activeBroadcast;
	activeBroadcast = undefined;

	void toUI("updateLocal", [{ mpLiveBroadcast: undefined }]);

	if (!transport || !broadcast || !transport.clearLiveBroadcast) {
		return;
	}
	try {
		await transport.clearLiveBroadcast(broadcast.chunkCount);
	} catch (error) {
		console.error("endLiveBroadcast failed", error);
	}
};

// Handle one snapshot of the broadcast meta doc on a device that is NOT the
// broadcaster - i.e. drive the follower experience: navigate to the live game
// on a new broadcast, then keep the lockstep cursor flowing to the UI, and
// unlock when it ends.
const handleLiveBroadcastMeta = async (
	meta: LiveBroadcastMeta | undefined,
	clientId: string,
	transport: FirebaseTransport,
) => {
	// Our own broadcast is driven locally (startLiveBroadcast / updateLiveBroadcast);
	// ignore the echo so we don't treat ourselves as a follower.
	if (meta && meta.holderId === clientId) {
		return;
	}

	// No live broadcast (ended, expired, or none): release any follow we had, and
	// unfreeze the header score ticker (liveGameInProgress) it was watching under.
	if (!meta || !meta.active || meta.expiresAt < Date.now()) {
		if (followedBroadcast) {
			followedBroadcast = undefined;
			followedBroadcastPayload = undefined;
			void toUI("updateLocal", [
				{ mpLiveBroadcast: undefined, liveGameInProgress: false },
			]);
		}
		return;
	}

	// A new broadcast (or the first we've seen): load the payload and navigate
	// this device into the live game. Guard on startedAt so the rapid cursor
	// heartbeats that follow don't re-navigate.
	if (!followedBroadcast || followedBroadcast.startedAt !== meta.startedAt) {
		followedBroadcast = {
			startedAt: meta.startedAt,
			gid: meta.gid,
			expiresAt: meta.expiresAt,
		};
		// Freeze the header score ticker right now - BEFORE the game result can sync
		// in - exactly like the simmer, whose liveGameInProgress is set before the
		// game is even written. The follower's own onLiveSimOver clears it at game
		// over, revealing the final score in the header just as it does for the simmer.
		void toUI("updateLocal", [{ liveGameInProgress: true }]);
		try {
			const serialized = await transport.fetchLiveBroadcastData?.(
				meta.chunkCount,
			);
			if (!serialized) {
				// Payload not fully there (cleared or mid-write) - drop the follow so a
				// later snapshot can retry.
				followedBroadcast = undefined;
				return;
			}
			const { boxScore, playByPlay } = deserializeChangeset(serialized);
			followedBroadcastPayload = {
				startedAt: meta.startedAt,
				gid: meta.gid,
				playByPlay,
				boxScore,
			};
			// Same navigation the simmer's own live sim uses, so the exact same view
			// path renders it. fromAction bypasses the deep-link guard; mpFollower
			// tells the view to use the payload's game record.
			await toUI("realtimeUpdate", [
				["gameSim"],
				helpers.leagueUrl(["live_game"]),
				{
					gidOneGame: meta.gid,
					playByPlay,
					boxScore,
					fromAction: true,
					mpFollower: true,
				},
			]);
		} catch (error) {
			console.error("Failed to start following live broadcast", error);
			followedBroadcast = undefined;
			return;
		}
	} else {
		followedBroadcast.expiresAt = meta.expiresAt;
	}

	// Push the live cursor/state so the LiveGame view seeks to the simmer's spot.
	void toUI("updateLocal", [
		{
			mpLiveBroadcast: {
				active: true,
				gid: meta.gid,
				byName: meta.byName,
				isBroadcaster: false,
				startedAt: meta.startedAt,
				cursor: meta.cursor,
				paused: meta.paused,
				gameOver: meta.gameOver,
			},
		},
	]);
};

// A crashed broadcaster stops heartbeating but never writes active:false, and
// onSnapshot won't fire again, so the follower must time the lease out itself.
// Called from the health tick.
const checkLiveBroadcastLease = () => {
	if (followedBroadcast && Date.now() > followedBroadcast.expiresAt) {
		followedBroadcast = undefined;
		followedBroadcastPayload = undefined;
		void toUI("updateLocal", [
			{ mpLiveBroadcast: undefined, liveGameInProgress: false },
		]);
	}
};

// ---------------------------------------------------------------------------
// Live lottery reveal. Whoever runs the draft lottery heartbeats how many
// picks they've revealed; every other device navigates to the lottery page and
// replays the reveal in lockstep. The result data arrives via the normal
// change log - this only carries the reveal position.
// ---------------------------------------------------------------------------

let lotteryRevealUnsub: (() => void) | undefined;

// The reveal this device is currently FOLLOWING (not its own), so a new
// broadcast navigates exactly once and expiry can release it.
let followedLotteryReveal: { startedAt: number; expiresAt: number } | undefined;

// Publish this device's reveal state (it just ran the lottery / revealed
// another pick). Called from the UI via the worker API. Best-effort.
export const publishLotteryRevealState = async (update: {
	active: boolean;
	season?: number;
	revealed?: number;
	startedAt?: number;
}) => {
	const transport = currentTransport;
	const engine = getSyncEngine();
	if (!transport?.publishLotteryReveal || !engine) {
		return;
	}
	try {
		await transport.publishLotteryReveal({
			...update,
			byName: engine.localName,
			expiresAt: Date.now() + LOTTERY_REVEAL_LEASE_MS,
		});
	} catch (error) {
		console.error("publishLotteryRevealState failed", error);
	}
};

// How long a reveal stays live without a heartbeat before viewers treat it as
// over and just show the (already-synced) final result.
const LOTTERY_REVEAL_LEASE_MS = 45_000;

const handleLotteryRevealMeta = (
	meta: import("./types.ts").LotteryRevealMeta | undefined,
	clientId: string,
) => {
	// Our own reveal is driven locally; ignore the echo.
	if (meta && meta.holderId === clientId) {
		return;
	}

	if (!meta || !meta.active || meta.expiresAt < Date.now()) {
		if (followedLotteryReveal) {
			followedLotteryReveal = undefined;
			void toUI("updateLocal", [{ mpLotteryReveal: undefined }]);
		}
		return;
	}

	// A new reveal: navigate this device to the lottery page so everyone
	// watches the picks flip together.
	if (
		!followedLotteryReveal ||
		followedLotteryReveal.startedAt !== meta.startedAt
	) {
		followedLotteryReveal = {
			startedAt: meta.startedAt,
			expiresAt: meta.expiresAt,
		};
		void toUI("realtimeUpdate", [[], helpers.leagueUrl(["draft_lottery"])]);
	} else {
		followedLotteryReveal.expiresAt = meta.expiresAt;
	}

	void toUI("updateLocal", [
		{
			mpLotteryReveal: {
				season: meta.season,
				revealed: meta.revealed,
				byName: meta.byName,
				startedAt: meta.startedAt,
			},
		},
	]);
};

// Time out a reveal whose broadcaster vanished, so viewers fall back to the
// synced final result instead of staring at a frozen board. Health tick.
const checkLotteryRevealLease = () => {
	if (followedLotteryReveal && Date.now() > followedLotteryReveal.expiresAt) {
		followedLotteryReveal = undefined;
		void toUI("updateLocal", [{ mpLotteryReveal: undefined }]);
	}
};

// Drain the backlog page by page (resumable, bounded memory), then - once truly
// caught up to the head - move the live subscription's watermark to the head and
// start it. Deferring the real-time listener this way keeps its initial snapshot
// to just the live tail, instead of re-loading the entire backlog we just
// drained (which on a long absence would time out and wedge). Idempotent: the
// subscription starts at most once; later calls just keep the tail drained.
// Counts driveCatchUp ticks so the log can show how many passes a device has
// spent stuck on the catching-up indicator without converging.
let driveCatchUpTicks = 0;
// Set while a productive-page-cap re-drive is already queued, so we don't stack
// multiple immediate re-drives on top of the 15s poll timer.
let driveCatchUpChained = false;

const driveCatchUp = async () => {
	const engine = getSyncEngine();
	if (!engine) {
		return;
	}
	driveCatchUpTicks += 1;
	const before = engine.getCatchUpDiagnostics();
	const reachedHead = await engine.catchUp();
	// A reconnect may have replaced the engine while the drain was in flight
	// (catchUp() aborts once its engine is stopped). Everything below - starting
	// the live subscription, moving the transport watermark - must act on the
	// CURRENT session, not the torn-down one.
	if (getSyncEngine() !== engine) {
		return;
	}
	const after = engine.getCatchUpDiagnostics();

	// One line per catch-up pass. The telling signals for an "infinitely
	// catching up" device: reachedHead stays false while `behind` never reaches
	// 0 (head moving faster than we drain, or fetches failing), or `behind` is 0
	// yet the indicator persists because a bulk batch is stuck (pendingBatches)
	// or an apply is pinned (applyFailed) - isCaughtUp() then never becomes true.
	syncDebugLog("connect:drive-catchup", {
		tick: driveCatchUpTicks,
		reachedHead,
		caughtUp: after.caughtUp,
		behindBefore: before.behind,
		behindAfter: after.behind,
		persistedSeq: after.persistedSeq,
		maxSeq: after.maxSeq,
		watermarkAdvanced: after.persistedSeq > before.persistedSeq,
		pendingBatches: after.pendingBatches,
		pendingBatchDetail: after.pendingBatchDetail,
		applyFailed: after.applyFailed,
		failedApplies: after.failedApplies,
		progressDone: after.progressDone,
		progressTotal: after.progressTotal,
		liveSubscription: after.liveSubscription,
	});

	// Only go live once the drain has actually reached the head - otherwise the
	// subscription's initial snapshot would re-load the still-undrained backlog.
	// (A failed fetch returns false, so we don't prematurely go live either.)
	if (reachedHead && !engine.hasChangesSubscription()) {
		currentTransport?.updateSince(engine.getPersistedSeq());
		engine.startChangesSubscription();
	}
	// Catching up may have just unblocked edits.
	pushEditsPaused();

	// A big backlog is drained in bounded chunks (catchUp() caps its pages so it
	// can't spin forever), and there's no live subscription yet - so without this,
	// each chunk would wait a full poll interval for the next, and a large room
	// crawls in (backlog / chunk) 30-second steps that look like "infinitely
	// catching up". If we didn't reach the head but DID make real progress, keep
	// draining right away instead of idling. Progress is NOT just the watermark:
	// while a bulk batch is mid-assembly the watermark is pinned by design, but
	// fetching further (maxSeq) or applying more entries (progressDone) is real
	// forward motion and must keep the chain alive - otherwise wedge RECOVERY
	// (re-fetching a pinned tail to rebuild a batch) is the exact flow that
	// crawls.
	const madeProgress =
		after.persistedSeq > before.persistedSeq ||
		after.maxSeq > before.maxSeq ||
		after.progressDone > before.progressDone;
	// A sweep that just RESET a dead batch owes an immediate rebuild pass: the
	// reset exists only to confirm-by-refetch and then abandon, and pausing a
	// full poll tick between those steps kept losing races against phone
	// screen-lock (the app suspends, in-memory recovery state restarts, the
	// abandon never runs). Bounded: each reset batch gets exactly one chained
	// rebuild, after which it either completes or is abandoned.
	const owesRebuild = engine.hasRebuildingBatches();
	if (
		((!reachedHead && madeProgress) || (reachedHead && owesRebuild)) &&
		!after.applyFailed &&
		!driveCatchUpChained &&
		getSyncEngine() !== undefined
	) {
		driveCatchUpChained = true;
		setTimeout(() => {
			driveCatchUpChained = false;
			void driveCatchUp();
		}, 50);
	}
};

// Called from the UI (on the simmer's device) whenever its auto-play schedule
// changes, to broadcast it to the room. No-op if not connected.
export const publishAutoPlayState = async (
	state: import("../../../common/types.ts").SyncedAutoPlay,
) => {
	try {
		await currentTransport?.publishAutoPlay?.(state);
	} catch (error) {
		// The schedule ride-along on the authority doc requires being in charge of simming;
		// a stale writer (just stopped being in charge of simming) is denied - harmless, since claiming
		// sim authority already cleared the old schedule.
		console.error("publishAutoPlayState failed", error);
	}
};

export const getSyncRequired = () => syncRequired;

// Called before cloud-tracked worker mutations. This is a worker-side backstop
// for a missed/delayed UI auto-reconnect: if the league metadata says this file
// belongs to a sync room, never let the worker treat it as a local-only league.
export const restoreSyncRequiredFromMeta = async () => {
	if (syncRequired || getSyncEngine() !== undefined) {
		return syncRequired;
	}
	const lid = g.get("lid");
	const session = await loadPersistedSyncSession(
		typeof lid === "number" ? lid : undefined,
	);
	if (!session) {
		return false;
	}
	syncRequired = true;
	currentCode = session.code;
	void toUI("updateLocal", [
		{ mpSyncActive: true, mpSyncReady: false, mpSyncReconnecting: true },
	]);
	return true;
};

// True while we intend to be synced but aren't connected yet (reconnecting or
// offline). The sim authority guard uses this to pause simming; the UI shows it.
export const isReconnecting = () =>
	syncRequired && getSyncEngine() === undefined;

// Called by the UI's auto-reconnect the instant it knows this league should be
// synced - before the async connect finishes - so simming is gated during the
// whole reconnect window, not just once it completes.
export const markSyncRequired = async (sessionFromUI?: {
	code: string;
	isHost?: boolean;
}) => {
	syncRequired = true;
	const trimmedFromUI = sessionFromUI?.code?.trim();
	const lid = g.get("lid");
	const lidNumber = typeof lid === "number" ? lid : undefined;
	if (trimmedFromUI) {
		currentCode = trimmedFromUI;
		// Deliberately NOT persisted to the league meta here: only a VALIDATED
		// connect (connectSharedLeague, after the room-binding check) may record a
		// session on the league. Persisting the UI's stored session blindly let a
		// stale localStorage entry (e.g. from a recycled lid) stamp a brand-new
		// league file as belonging to an old room.
	}
	const session = await loadPersistedSyncSession(lidNumber);
	if (session) {
		currentCode = session.code;
	}
	if (getSyncEngine() === undefined) {
		void toUI("updateLocal", [
			{
				mpSyncActive: true,
				mpSyncReady: false,
				mpSyncReconnecting: true,
			},
		]);
	}
};

const pushReadyToUI = (ready: boolean) => {
	currentCloudReady = ready;
	void toUI("updateLocal", [{ mpSyncReady: ready }]);
};

const refreshReady = async () => {
	const engine = getSyncEngine();
	if (!engine) {
		pushReadyToUI(false);
		return false;
	}

	try {
		await engine.ensureReady();
		const cloudReady = engine.isReady();
		pushReadyToUI(cloudReady);
		return cloudReady;
	} catch {
		pushReadyToUI(false);
		return false;
	}
};

export const checkSyncReady = async () => {
	return refreshReady();
};

export const getSyncStatus = async () => {
	const engine = getSyncEngine();
	const ready = await refreshReady();
	return {
		connected: engine !== undefined,
		reconnecting: isReconnecting(),
		ready,
		code: currentCode,
		// "host" now means "current sim authority", read live from the engine.
		isHost: engine?.isAuthority() ?? false,
		hostName: currentHostName,
	};
};

// Push the current sim authority state into reactive UI local state so the Play menu,
// draft, and sync page can reflect who's in control without polling.
const pushAuthorityToUI = (isHost: boolean, hostName: string | undefined) => {
	void toUI("updateLocal", [
		{
			mpSyncIsHost: isHost,
			mpSyncHostName: hostName,
			mpSyncReady: currentCloudReady,
		},
	]);
};

// Sim here on this device (become the one allowed to advance the league).
export const claimSyncAuthority = async () => {
	await getSyncEngine()?.claimAuthority();
};

// One row in the sync-activity view: a single change (or a whole bulk batch,
// collapsed into one row), with whether THIS device has caught up through it.
export type SyncActivityItem = {
	key: string;
	action: string;
	// Server-timestamp millis (0 for a not-yet-confirmed write).
	ts: number;
	// How many records this change touched (summed across a bulk batch).
	records: number;
	// Did this device produce it?
	mine: boolean;
	// Is it at or below our durable catch-up watermark (i.e. accounted for)?
	caughtUp: boolean;
	// Which gameAttributes keys this change carried (e.g. "phase", "daysLeft"), so
	// the sync log makes it obvious whether a phase change actually shipped.
	attrs: string[];
};

// Read the whole shared change log and report, per change, whether this device
// has applied/caught up through it. Bulk batches (sims) collapse to one row.
export const getSyncActivity = async (): Promise<{
	connected: boolean;
	watermark: number;
	items: SyncActivityItem[];
}> => {
	const engine = getSyncEngine();
	if (!engine) {
		return { connected: false, watermark: 0, items: [] };
	}

	// Only the most recent activity - never read the whole log (which can be huge
	// for a long-running league) just to render this list.
	const entries = await engine.fetchRecentLog(SYNC_ACTIVITY_LIMIT);
	const watermark = engine.getPersistedSeq();
	const clientId = engine.clientId;

	// Collapse chunked bulk batches (which share a batchId) into a single row.
	// gameAttributes keys carried by an entry (phase, daysLeft, etc.). String-part
	// entries aren't independently parseable, so they carry this as metadata.
	const attrsOf = (entry: (typeof entries)[number]): string[] =>
		entry.attrs ??
		entry.changeset.changes
			.filter((c) => c.store === "gameAttributes")
			.map((c) => String(c.id));

	const byKey = new Map<string, SyncActivityItem>();
	for (const entry of entries) {
		const key = entry.batchId ?? entry.id;
		const records = entry.records ?? entry.changeset.changes.length;
		const mine = entry.authorId === clientId;
		const existing = byKey.get(key);
		if (existing) {
			// String-part entries carry the WHOLE batch's record count as metadata
			// (take the max); legacy chunks each carry their own slice (sum them).
			existing.records =
				entry.records !== undefined
					? Math.max(existing.records, records)
					: existing.records + records;
			existing.ts = Math.max(existing.ts, entry.seq);
			existing.caughtUp = mine || existing.ts <= watermark;
			for (const attr of attrsOf(entry)) {
				if (!existing.attrs.includes(attr)) {
					existing.attrs.push(attr);
				}
			}
		} else {
			byKey.set(key, {
				key,
				action: entry.action,
				ts: entry.seq,
				records,
				mine,
				caughtUp: mine || entry.seq <= watermark,
				attrs: attrsOf(entry),
			});
		}
	}

	// Newest first.
	const items = [...byKey.values()].sort((a, b) => b.ts - a.ts);
	return { connected: true, watermark, items };
};

// A one-shot, self-describing snapshot of THIS device's sync state, for pasting
// into a diagnosis. Prepended to the copied debug logs so a capture is useful on
// its own - it identifies whose device it is and whether it's caught up, stuck,
// or has a dead listener - even if the log buffer is thin.
export const getSyncDebugSnapshot = async (): Promise<string> => {
	const engine = getSyncEngine();
	const lines: string[] = [];
	lines.push("=== MP SYNC SNAPSHOT ===");
	// The build the WORKER is running. Compared against the UI's version in the
	// copy header, this exposes a stale worker/service-worker cache instantly —
	// the "your fix isn't actually running on my phone yet" case.
	lines.push(`workerVersion=${env.bbgmVersion}`);
	try {
		lines.push(
			`lid=${g.get("lid")} userTid=${g.get("userTid")} season=${g.get("season")} phase=${g.get("phase")}`,
		);
	} catch {
		// g may be unavailable very early; not worth failing the snapshot.
	}
	lines.push(
		`connected=${engine !== undefined} reconnecting=${isReconnecting()} room=${currentCode ?? "—"} inChargeOfSimming=${engine?.isAuthority() ?? false} simmer=${currentHostName ?? "—"}`,
	);
	if (engine) {
		const d = engine.getCatchUpDiagnostics();
		const contactAge = engine.contactAge();
		const lastDelivery = engine.getLastChangesDeliveryAt();
		lines.push(
			`caughtUp=${d.caughtUp} behind=${d.behind} persistedSeq=${d.persistedSeq} maxSeq=${d.maxSeq}`,
		);
		lines.push(
			`liveListener=${d.liveSubscription} lastChangesDeliveryMsAgo=${lastDelivery > 0 ? Date.now() - lastDelivery : "never"} contactAgeMs=${contactAge ?? "—"}`,
		);
		lines.push(
			`catchingUp=${d.catchingUp} pendingBatches=${d.pendingBatches} rebuilding=${d.rebuilding} applyFailed=${d.applyFailed} failedApplies=${d.failedApplies} progress=${d.progressDone}/${d.progressTotal}`,
		);
		if (d.pendingBatches > 0) {
			lines.push(`pendingBatchDetail=${JSON.stringify(d.pendingBatchDetail)}`);
		}
	}
	lines.push(`at=${new Date().toISOString()}`);
	lines.push("=== LOG ===");
	return lines.join("\n");
};

// Force a full catch-up: re-read the entire log and re-apply it from scratch.
// The one-click fix for a device that silently diverged.
export const resyncSharedLeague = async (): Promise<{
	total: number;
	applied: number;
	incomplete: number;
	failed: boolean;
}> => {
	const engine = getSyncEngine();
	if (!engine) {
		throw new Error("Not connected to a sync room.");
	}
	return engine.resyncAll();
};

// Join a shared-league sync room. All devices using the same `code` see each
// other's changes. Everyone should already be on the same league file - on
// connect we catch up on everything that happened since we were last synced,
// then stay live.
// `explicit` marks a join the user asked for by name (typing the code on the
// sync page, or choosing a room at league creation). Only an explicit join may
// BIND a league file to a room; automatic reconnects merely resume a binding
// that already exists.
let connectQueue: Promise<unknown> = Promise.resolve();

export const connectSharedLeague = async (options: {
	code: string;
	isHost?: boolean;
	explicit?: boolean;
}) => {
	// Serialize connects. Two connects racing (e.g. the UI auto-reconnect and an
	// explicit join firing together) each tore down the other's half-built
	// session and left TWO live engines draining the same log concurrently -
	// interleaved catch-up passes with different watermarks churning forever.
	// The second caller now waits for the first to finish, then (if it's a
	// redundant reconnect to the same room) becomes a no-op inside doConnect.
	const run = connectQueue.then(
		() => doConnectSharedLeague(options),
		() => doConnectSharedLeague(options),
	);
	connectQueue = run.then(
		() => undefined,
		() => undefined,
	);
	return run;
};

const doConnectSharedLeague = async ({
	code,
	isHost = false,
	explicit = false,
}: {
	code: string;
	isHost?: boolean;
	explicit?: boolean;
}) => {
	const trimmed = code.trim();
	if (!trimmed) {
		throw new Error("A league code is required.");
	}

	// An automatic reconnect that finds a live session for this exact room and
	// league is redundant - reconnecting anyway would tear down a healthy engine
	// and replay its catch-up. Only an EXPLICIT join (the user typed/chose the
	// code) forces a fresh connect.
	{
		const existing = getSyncEngine();
		const lidNow = g.get("lid");
		if (
			!explicit &&
			existing !== undefined &&
			currentCode === trimmed &&
			connectedLid !== undefined &&
			connectedLid === (typeof lidNow === "number" ? lidNow : undefined)
		) {
			syncDebugLog("connect:duplicate-skipped", {
				code: trimmed,
				lid: connectedLid,
			});
			syncRequired = true;
			pushSyncStateFull();
			return {
				connected: true,
				code: trimmed,
				isHost,
				clientId: existing.clientId,
			};
		}
	}

	// Tear down any existing live session first, but do not erase the persisted
	// room intent while reconnecting/switching transports.
	await teardownSharedLeague({ clearPersisted: false });

	// From here on this device is committed to the session, so simming stays
	// gated through the whole async connect (and if it throws) - never sim
	// offline and diverge.
	syncRequired = true;

	// Authenticate - the uid is our stable, rule-enforceable sync identity.
	const clientId = await ensureAnonymousAuth();

	const lid = g.get("lid");
	const lidNumber = typeof lid === "number" ? lid : undefined;
	connectedLid = lidNumber;
	const watermark = await loadWatermark(lid);

	// The position this connect will start catching up FROM. If the file was a
	// fresh, up-to-date export of this room, the import should have stamped a
	// near-head watermark here; a 0 means the checkpoint wasn't applied and this
	// device will replay the entire room history (see import:checkpoint).
	syncDebugLog("connect:initial-watermark", {
		lid: lidNumber,
		watermark,
		explicit,
	});

	const transport = new FirebaseTransport(trimmed, clientId, {
		sinceTs: watermark,
	});

	// Room ↔ league-file binding check. Each room carries a fingerprint
	// (leagueId) of the league file it belongs to, and each synced league stores
	// the same fingerprint in its meta row. Without this, any stale session
	// pointer could silently connect the WRONG file to a room and cross-pollute
	// both. Rules:
	//   - Fingerprints on both sides must match, always.
	//   - An unbound league may bind to a room only on an EXPLICIT join (the
	//     user typed/chose the code), or - legacy grandfather - when this league's
	//     meta already carries a validated session for this exact room.
	//   - Binding is recorded after connect succeeds (meta + room registry).
	const metaLeagueId = await loadSyncLeagueId(lidNumber);
	const priorSession = await loadPersistedSyncSession(lidNumber);
	const roomLeagueId = (await transport.getRoomInfo())?.leagueId;
	const refuse = async () => {
		// Only scrub the persisted session if it pointed at the room being
		// refused - a failed explicit join to some OTHER room must not erase the
		// league's valid session.
		if (priorSession?.code === trimmed) {
			await clearPersistedSyncSession(lidNumber);
		}
		syncRequired = false;
		currentCode = undefined;
		connectedLid = undefined;
		void toUI("updateLocal", [
			{ mpSyncActive: false, mpSyncReady: false, mpSyncReconnecting: false },
		]);
		throw new Error(ERROR_MESSAGE_SYNC_ROOM_MISMATCH);
	};
	if (
		metaLeagueId !== undefined &&
		roomLeagueId !== undefined &&
		metaLeagueId !== roomLeagueId
	) {
		await refuse();
	}
	if (
		metaLeagueId === undefined &&
		!explicit &&
		priorSession?.code !== trimmed
	) {
		await refuse();
	}
	const boundLeagueId = metaLeagueId ?? roomLeagueId ?? makeLeagueId();

	const engine = new SyncEngine(transport, {
		isHost: false,
		initialWatermark: watermark,
		code: trimmed,
		onWatermark: (seq) => {
			void saveWatermark(lid, seq);
			// Catching up may have just unblocked edits - refresh the indicator.
			pushEditsPaused();
		},
		onAuthorityChange: (authority) => {
			currentHostName = authority?.holderName;
			pushAuthorityToUI(
				authority?.holderId === clientId,
				authority?.holderName,
			);
			// The busy lease rides on this doc, so a flip here means edits just got
			// blocked or unblocked - update the header indicator immediately.
			pushEditsPaused();
		},
		// Live upload progress → UI, so any device shows a cloud indicator (with a
		// count for big changes) while a change uploads.
		onUploadProgress: (progress) => {
			void toUI("updateLocal", [{ mpSyncUpload: progress }]);
		},
		// A confirmed upload bumps a counter the UI watches to flash "synced ✓".
		onUploadComplete: () => {
			uploadOkCounter += 1;
			void toUI("updateLocal", [{ mpSyncUploadOk: uploadOkCounter }]);
		},
		// Backlog-drain progress → UI, so a device catching up after an absence
		// shows how far along it is and roughly how much longer.
		onCatchUpProgress: (progress) => {
			// Log every set/clear of the indicator so an "infinitely catching up"
			// device shows exactly when the bar appears and whether it's ever
			// cleared (progress === undefined) between passes.
			syncDebugLog("connect:catchup-indicator", {
				showing: progress !== undefined,
				done: progress?.done,
				total: progress?.total,
			});
			void toUI("updateLocal", [{ mpCatchUp: progress }]);
		},
		onReadyChange: (ready) => {
			pushReadyToUI(ready);
		},
		// Queued-but-unconfirmed upload count → UI, so a delta that hasn't reached
		// the cloud is always visible in the header, never silently waiting.
		onPendingChange: (count) => {
			pushPendingUploads(count);
		},
	});
	engine.start();
	setSyncEngine(engine);
	currentCode = trimmed;
	currentHostName = undefined;
	currentCloudReady = false;

	// Hard write-boundary guard: remote changesets may only be applied while the
	// loaded league is still the one this session was opened for. Protects the
	// window where a session outlives a league switch (e.g. mid-import) even if
	// a teardown call is missed.
	setApplyGuard(() => {
		const currentLid = g.get("lid");
		return connectedLid === undefined || currentLid === connectedLid;
	});

	try {
		await engine.ensureReady();
		if (isHost) {
			await engine.claimAuthority();
		}
	} catch (error) {
		await teardownSharedLeague({ clearPersisted: false });
		throw error;
	}

	await savePersistedSyncSession(lid, { code: trimmed, isHost });

	// Record the room ↔ league binding on both sides: the fingerprint in this
	// league's meta row, and (via the registry doc, which also makes the room
	// listable on the admin page) on the room itself. Registry write is
	// best-effort; the meta write is what future reconnect validation reads.
	if (metaLeagueId === undefined) {
		await saveSyncLeagueId(lidNumber, boundLeagueId);
	}
	void transport.touchRoom?.(boundLeagueId);

	// Finish any upload a previous session left unconfirmed (interrupted mid-send
	// or wedged before a refresh), drop long-dead outbox entries, and surface the
	// initial queued count. Never blocks the connect.
	void engine.drainOutbox();
	void engine.pendingUploadCount().then(pushPendingUploads, () => {});
	void outbox.prune(OUTBOX_MAX_AGE_MS);

	// Drive the header connection dot from confirmed live contact, and the
	// "simming…" edits-paused indicator (which also needs a tick to notice a busy
	// lease expiring with no accompanying event).
	lastHealthPushed = undefined;
	lastEditsPausedPushed = undefined;
	pushHealth();
	pushEditsPaused();
	if (healthTimer !== undefined) {
		clearInterval(healthTimer);
	}
	lastMemberTid = undefined;
	healthTimer = setInterval(() => {
		// Re-assert the whole sync UI state (not just health/edits) so a UI that
		// drifted from the engine - stale "nobody simming", an unlocked Play menu -
		// heals itself within a tick instead of needing a manual refresh.
		pushSyncStateFull();
		// Unlock a follower whose broadcaster went away without a clean end.
		checkLiveBroadcastLease();
		checkLotteryRevealLease();
		// Keep kicking the upload drain while anything is queued - it self-retries
		// with backoff, but a persistent tick guarantees a stalled queue can never
		// sit idle (coalesced, so redundant kicks are nearly free).
		if (lastPendingUploads > 0) {
			void getSyncEngine()?.drainOutbox();
		}
		// Keep this device's member doc pointing at the team it currently
		// manages, so targeted notifications keep reaching it after a team switch.
		try {
			const tid = g.get("userTid");
			if (typeof tid === "number" && tid !== lastMemberTid) {
				lastMemberTid = tid;
				void currentTransport?.registerMember?.(clientId, { tid }).catch(() => {
					// Retry on a later tick.
					lastMemberTid = undefined;
				});
			}
		} catch {
			// g may be mid-reload; try again next tick.
		}
	}, HEALTH_TICK_MS);

	// Watch the shared auto-play schedule so every device shows the same schedule
	// + countdown, and keep a transport handle so the simmer can publish its own.
	currentTransport = transport;
	autoPlayUnsub = transport.subscribeAutoPlay?.((autoPlay) => {
		void toUI("updateLocal", [{ mpAutoPlay: autoPlay }]);
	});

	// Watch for a live-sim broadcast. On a follower this navigates into the live
	// game and drives lockstep playback; on the broadcaster its own echo is
	// ignored (handled locally).
	activeBroadcast = undefined;
	followedBroadcast = undefined;
	liveBroadcastUnsub = transport.subscribeLiveBroadcast?.((meta) => {
		void handleLiveBroadcastMeta(meta, clientId, transport);
	});

	// Draft ready-up: watch everyone's ready state and auto-advance CPU picks
	// once every user team has readied (see draftReady.ts). No-op outside the
	// draft phase.
	setupDraftReady(transport);

	// Free-agency boards: watch everyone's ranked FA lists so the day advance
	// can resolve them (see faBoard.ts). No-op outside free agency.
	setupFaBoard(transport);

	// Watch for a live lottery reveal (someone running the lottery while
	// everyone watches the picks flip in lockstep).
	followedLotteryReveal = undefined;
	lotteryRevealUnsub = transport.subscribeLotteryReveal?.((meta) => {
		handleLotteryRevealMeta(meta, clientId);
	});

	// Kick off the initial paginated backlog drain now (it also starts the live
	// changes subscription once caught up). Runs in the background so connect
	// doesn't block on a device that's been away a long time.
	void driveCatchUp();

	// Poll to keep draining / pick up anything the real-time subscription hasn't
	// delivered yet (and to start that subscription once the initial drain lands).
	if (catchUpTimer !== undefined) {
		clearInterval(catchUpTimer);
	}
	catchUpTimer = setInterval(() => {
		const engine = getSyncEngine();
		// Skip the billed catch-up read ONLY when there is provably nothing to do:
		// the live CHANGES listener delivered an entry within LISTENER_FRESH_MS and
		// everything seen has been applied (isCaughtUp). Two hard-won subtleties:
		//   - The freshness signal must be the changes listener SPECIFICALLY, never
		//     the transport's global contact time: any listener refreshes that (the
		//     authority doc heartbeats constantly during a sim), so a follower whose
		//     changes listener silently died looked "fresh" while game data stopped
		//     arriving - and the skipped backstop meant it could NEVER catch up
		//     while the room was active (the exact time it most needed to).
		//   - isCaughtUp alone is also not enough: it is relative to what this
		//     device has SEEN, so a dead listener yields a confidently-wrong true.
		// An idle room delivers nothing, so it still probes once per interval - the
		// unavoidable price of detecting a dead listener. The saving lands in
		// active rooms, where the old poll re-read a log the listener had already
		// delivered. During the initial backlog drain this always runs.
		const lastDelivery = engine?.getLastChangesDeliveryAt() ?? 0;
		const changesFresh = Date.now() - lastDelivery < LISTENER_FRESH_MS;
		const subscribed = engine?.hasChangesSubscription?.() ?? false;
		const caughtUp = engine?.isCaughtUp() ?? false;
		if (!(subscribed && changesFresh && caughtUp)) {
			void driveCatchUp();
		}
		// Second, independent kick for the upload drain (it also self-retries with
		// backoff after a failure); a no-op when the outbox is empty.
		void engine?.drainOutbox();
	}, CATCH_UP_INTERVAL_MS);

	// Turn on change capture so local actions get published to the room.
	changeTracker.enable();
	changeTracker.reset();

	// Let the UI hide single-player-only chrome (e.g. the multi-team switcher)
	// and clear the "reconnecting" state.
	void toUI("updateLocal", [
		{
			mpSyncActive: true,
			mpSyncReady: engine.isReady(),
			mpSyncReconnecting: false,
		},
	]);

	return { connected: true, code: trimmed, isHost, clientId };
};

export const teardownSharedLeague = async ({
	clearPersisted = false,
}: { clearPersisted?: boolean } = {}) => {
	const lid = g.get("lid");
	const lidToClear =
		connectedLid ?? (typeof lid === "number" ? lid : undefined);
	if (catchUpTimer !== undefined) {
		clearInterval(catchUpTimer);
		catchUpTimer = undefined;
	}
	if (healthTimer !== undefined) {
		clearInterval(healthTimer);
		healthTimer = undefined;
	}
	if (watermarkTrailingTimer !== undefined) {
		clearTimeout(watermarkTrailingTimer);
		watermarkTrailingTimer = undefined;
	}
	lastHealthPushed = undefined;
	lastEditsPausedPushed = undefined;
	autoPlayUnsub?.();
	autoPlayUnsub = undefined;
	liveBroadcastUnsub?.();
	liveBroadcastUnsub = undefined;
	lotteryRevealUnsub?.();
	lotteryRevealUnsub = undefined;
	followedLotteryReveal = undefined;
	teardownDraftReady();
	teardownFaBoard();
	// Best-effort: end our own broadcast so we don't leave the room locked.
	if (activeBroadcast) {
		void endLiveBroadcast();
	}
	activeBroadcast = undefined;
	followedBroadcast = undefined;
	followedBroadcastPayload = undefined;
	currentTransport = undefined;
	lastPendingUploads = 0;
	void toUI("updateLocal", [
		{
			mpAutoPlay: undefined,
			mpSyncUpload: undefined,
			mpSyncHealthy: false,
			mpEditsPaused: false,
			mpCatchUp: undefined,
			mpLiveBroadcast: undefined,
			mpLotteryReveal: undefined,
			mpPendingUploads: 0,
		},
	]);
	const engine = getSyncEngine();
	if (engine) {
		engine.stop();
		setSyncEngine(undefined);
	}
	setApplyGuard(undefined);
	currentCode = undefined;
	connectedLid = undefined;
	currentHostName = undefined;
	currentCloudReady = false;
	if (clearPersisted) {
		await clearPersistedSyncSession(lidToClear);
	}

	// Explicit disconnect clears the intent, so single-player simming works again.
	syncRequired = false;

	void toUI("updateLocal", [
		{ mpSyncActive: false, mpSyncReady: false, mpSyncReconnecting: false },
	]);
	pushAuthorityToUI(false, undefined);

	// Leave the tracker enabled in dev (the console logger uses it); otherwise
	// turn it back off so single-player has zero overhead.
	if (process.env.NODE_ENV !== "development") {
		changeTracker.disable();
	}

	return { connected: false };
};

export const disconnectSharedLeague = () =>
	teardownSharedLeague({ clearPersisted: true });
