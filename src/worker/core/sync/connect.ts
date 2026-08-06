import { RESYNC_WINDOW_ENTRIES, SyncEngine } from "./SyncEngine.ts";
import { SyncEngineV2 } from "./v2/engine.ts";
import { FirebaseTransport } from "./FirebaseTransport.ts";
import { outbox } from "./outbox.ts";
import { ensureAnonymousAuth } from "./auth.ts";
import { setActiveFirebaseConfig } from "./firebaseApp.ts";
import type { FirebaseConfig } from "./firebaseConfig.ts";
import { setApplyGuard } from "./applyGuard.ts";
import { setupDraftReady, teardownDraftReady } from "./draftReady.ts";
import { setupSimDayFence, teardownSimDayFence } from "./simDayFence.ts";
import { setupFaBoard, teardownFaBoard } from "./faBoard.ts";
import {
	beginLiveChat,
	persistLiveChatToReplay,
	setupLiveChat,
	teardownLiveChat,
} from "./liveChat.ts";
import { setupTriviaScores, teardownTriviaScores } from "./triviaScores.ts";
import { getSyncEngine, setSyncEngine } from "./engineHolder.ts";
import { setLiveWatchGate } from "./liveWatchGate.ts";
import {
	readLocalLeagueId,
	resolveLeagueIdentity,
	writeLocalLeagueId,
} from "./leagueIdentity.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, local, lock, logEvent, toUI } from "../../util/index.ts";
import { env } from "../../util/env.ts";
import { ERROR_MESSAGE_SYNC_ROOM_MISMATCH } from "../../../common/constants.ts";
import { serializeChangeset, deserializeChangeset } from "./serialize.ts";
import {
	findStrandedScheduleRows,
	sweepPhantomScheduleRows,
	flushDeferredRefreshAfterLive,
} from "./changeset.ts";
import { syncDebugLog } from "./debugLog.ts";
import { repairLeagueHistory } from "./historyRepair.ts";
import {
	maybePublishRoomSnapshot,
	restoreFromRoomSnapshot,
} from "./roomSnapshot.ts";
import {
	describePosition,
	getLeaguePosition,
	isAheadOfPosition,
	isBehindPosition,
} from "./leaguePosition.ts";
import { checkLeagueIntegrity } from "./leagueIntegrity.ts";
import { decideMissingDataWarning } from "./missingDataWarning.ts";
import { resetSnapshotRestoreBackoff } from "./snapshotRestoreBackoff.ts";
import { readRecoveryAttempt } from "./recoveryBreadcrumb.ts";
import { endLotteryReveal } from "./notifications.ts";
import {
	isTooFarBehind,
	RETENTION_DAYS,
} from "../../../common/syncRetention.ts";
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

// Durable "this device silently skipped a bulk change" marker (see
// League.syncResyncNeeded). Set by the engine when it abandons a batch; read on
// connect to trigger a self-healing full-log resync. Survives reloads, unlike the
// engine's in-memory abandoned-batch set.
// How much of the log the recovery paths re-read: RESYNC_WINDOW_ENTRIES, from
// the engine, so every automatic replay is bounded the same way. Unbounded
// reads take minutes on a phone and have to COMPLETE to do any good, so they
// never did.

const loadResyncNeeded = async (lid: number | undefined): Promise<boolean> => {
	if (typeof lid !== "number") {
		return false;
	}
	const league = await idb.meta.get("leagues", lid);
	return !!league?.syncResyncNeeded;
};

const saveResyncNeeded = async (lid: number | undefined, value: boolean) => {
	if (typeof lid !== "number") {
		return;
	}
	const league = await idb.meta.get("leagues", lid);
	if (league) {
		if (value) {
			league.syncResyncNeeded = true;
		} else {
			delete league.syncResyncNeeded;
			// Healed, so the clock behind the user-facing warning starts over.
			delete league.syncMissingDataSince;
		}
		await idb.meta.put("leagues", league);
	}
};

// Record this sighting of missing-with-no-checkpoint against the durable stamp,
// and report whether the gap has now outlasted the grace period (see
// missingDataWarning.ts).
const noteMissingDataAndShouldWarn = async (
	lid: number | undefined,
	alreadyWarned: boolean,
) => {
	if (typeof lid !== "number") {
		return false;
	}
	const league = await idb.meta.get("leagues", lid);
	if (!league) {
		return false;
	}
	const decision = decideMissingDataWarning({
		since: league.syncMissingDataSince,
		alreadyWarned,
		now: Date.now(),
	});
	if (league.syncMissingDataSince !== decision.since) {
		league.syncMissingDataSince = decision.since;
		await idb.meta.put("leagues", league);
	}
	return decision.warn;
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
// Whether the header catch-up pill is currently shown (a defined mpCatchUp was
// pushed and not yet cleared). Lets a drive pass that ends fully caught up clear
// a pill that was left showing without a matching clear (see driveCatchUp).
let catchUpPillShowing = false;
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
			mpSyncProtocol:
				engine === undefined
					? undefined
					: engine instanceof SyncEngineV2
						? ("v2" as const)
						: ("classic" as const),
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

// A foreground kick from the UI: the user just brought the app back (tab
// visible, window focused, PWA resumed), which is precisely when a
// backgrounded browser's parked connection has left the screen stale. Sync
// NOW - probe the head, drain anything queued, re-assert the UI state -
// instead of waiting out the next timer tick.
export const syncNudge = () => {
	const engine = getSyncEngine();
	if (!engine) {
		return;
	}
	if (engine instanceof SyncEngineV2) {
		void engine.probeHead();
	} else {
		void engine.catchUp();
	}
	void engine.drainOutbox();
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
// `over` flips when THIS device's playback reached the game's final play: the
// show is finished on this screen even though the broadcaster may still be
// parked on their live game page (their exit is what ends the broadcast doc).
// Only the spoiler gate reads it - the follow itself stays, so the
// broadcaster's continuing heartbeats don't re-navigate or re-freeze us.
let followedBroadcast:
	| { startedAt: number; gid: number; expiresAt: number; over?: boolean }
	| undefined;

// Whether WE (as a follower) froze the header score ticker (liveGameInProgress)
// for a broadcast. Tracked separately from followedBroadcast so that if we bail
// out of a follow attempt (payload not ready, error) - which clears
// followedBroadcast - the freeze is still guaranteed to get released. Otherwise
// the "Live game in progress" banner sticks on forever.
let followerFroze = false;
const unfreezeFollower = () => {
	if (followerFroze) {
		followerFroze = false;
		void toUI("updateLocal", [
			{ mpLiveBroadcast: undefined, liveGameInProgress: false },
		]);
		// Anything that synced in during the playback repainted nothing (spoiler
		// gate); now that the show is over, paint it all.
		flushDeferredRefreshAfterLive();
	}
};

// The apply layer asks this before repainting: remote data landing while this
// device is watching a broadcast must not spoil the game mid-playback.
setLiveWatchGate(
	() =>
		(followedBroadcast !== undefined && !followedBroadcast.over) ||
		followerFroze,
);

// The follower's live game page declared the playback over (final play reached,
// or the user left the page - the same two moments the local device unlocks).
// Release the visual gate NOW instead of waiting for the broadcaster to leave
// their screen, so the box score header and everything else deferred paints the
// moment the game goes final here. Strict gid match: a replay of some other
// game ending in another tab must not unlock a broadcast still playing.
export const markFollowedBroadcastOver = (gid?: number) => {
	if (
		followedBroadcast === undefined ||
		followedBroadcast.over ||
		gid === undefined ||
		followedBroadcast.gid !== gid
	) {
		return;
	}
	followedBroadcast.over = true;
	followerFroze = false;
};

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

		// Scope chat to THIS broadcast and wipe the room's chat doc, so last
		// game's conversation cannot appear over this one.
		await beginLiveChat(gid, startedAt, true);

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

	// Save the conversation into the replay before the room's copy goes away.
	// Broadcaster only: liveGamePlayByPlay is a synced store, so every watcher
	// writing its own copy would put several devices in a publish race over
	// one row.
	await persistLiveChatToReplay(broadcast !== undefined);

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
	// Clear the freeze even if followedBroadcast is already gone (a bailed follow
	// attempt) so the banner can never get stranded on.
	if (!meta || !meta.active || meta.expiresAt < Date.now()) {
		if (followedBroadcast || followerFroze) {
			followedBroadcast = undefined;
			followedBroadcastPayload = undefined;
			followerFroze = false;
			void toUI("updateLocal", [
				{ mpLiveBroadcast: undefined, liveGameInProgress: false },
			]);
			flushDeferredRefreshAfterLive();
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
		// Accept only this broadcast's chat (the broadcaster does the clearing).
		void beginLiveChat(meta.gid, meta.startedAt, false);
		// Freeze the header score ticker right now - BEFORE the game result can sync
		// in - exactly like the simmer, whose liveGameInProgress is set before the
		// game is even written. The follower's own onLiveSimOver clears it at game
		// over, revealing the final score in the header just as it does for the simmer.
		void toUI("updateLocal", [{ liveGameInProgress: true }]);
		followerFroze = true;
		try {
			const serialized = await transport.fetchLiveBroadcastData?.(
				meta.chunkCount,
			);
			if (!serialized) {
				// Payload not fully there (cleared or mid-write) - drop the follow so a
				// later snapshot can retry, and release the freeze we just set.
				followedBroadcast = undefined;
				unfreezeFollower();
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
			unfreezeFollower();
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
		followerFroze = false;
		void toUI("updateLocal", [
			{ mpLiveBroadcast: undefined, liveGameInProgress: false },
		]);
		flushDeferredRefreshAfterLive();
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
	// The reveal just finished (host reached the last pick, or left the page):
	// release the lottery push we held back so it wouldn't spoil the still-
	// animating reveal. The result itself already synced via the change log, so
	// this only fans out the (now safe) "#1 pick" notification.
	if (update.active === false) {
		const held = endLotteryReveal();
		const releaseEngine = getSyncEngine();
		if (releaseEngine) {
			for (const notification of held) {
				void releaseEngine.publishNotification(notification).catch((error) => {
					console.error(
						"[sync] Failed to publish held lottery notification",
						error,
					);
				});
			}
		}
	}

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

	// Self-heal a stuck pill: if this pass reached the head and we're genuinely
	// caught up (nothing pending) with no active progress total, but the pill is
	// still showing, clear it. finishCatchUp only emits a clear when a progress
	// TOTAL had been set, so a session that caught up without ever showing a total
	// (e.g. a near-caught-up reconnect) wouldn't otherwise hide a pill left over
	// from a prior engine. Only ever CLEARS, and only when provably done.
	if (
		reachedHead &&
		after.caughtUp &&
		after.progressTotal === undefined &&
		catchUpPillShowing
	) {
		catchUpPillShowing = false;
		void toUI("updateLocal", [{ mpCatchUp: undefined }]);
	}

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
	if (engine instanceof SyncEngineV2) {
		lines.push(engine.getV2SnapshotLine());
	}
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
	// Survives a crash, unlike the log below - which is the whole point. A
	// recovery still recorded as in flight means the app died inside it, and
	// names which one.
	try {
		const attempt = await readRecoveryAttempt(g.get("lid"));
		if (attempt) {
			lines.push(
				`unfinishedRecovery=${attempt.op} failures=${attempt.failures} startedMsAgo=${Date.now() - attempt.startedAt}`,
			);
		}
	} catch {
		// Diagnostics must never be the thing that fails.
	}
	lines.push(`at=${new Date().toISOString()}`);
	lines.push("=== LOG ===");
	return lines.join("\n");
};

// May THIS device advance the shared league right now? Connection and sim
// authority are checked elsewhere; this is the health half - the checks that
// were missing when a device whose local state was corrupted (parked at a
// phantom phase boundary by a bad replay) passed every existing guard and
// simmed its corruption straight into the shared log for everyone. A device
// may not advance while it is mid-repair, flagged for repair, or standing
// somewhere other than where the room's stamped position says the league is.
// Used by the worker's timeline-advance guard and as the auto-play
// scheduler's preflight, so an unattended timer can never fire from a state
// a human would look at and say "that's not our league".
// Is the repair flag still describing a real problem?
//
// Two independent checks, and both have to agree that nothing is wrong. The
// watermark says whether this device has read everything the room has
// published; the schedule says whether a day the league already played past is
// still sitting here unplayed, which is what a genuinely dropped changeset
// looks like in the data. A skipped bulk change that mattered shows up in the
// second even when the first looks clean, which is the whole reason the
// stranded-row check exists.
const noDamageToRepair = async (engine: {
	isCaughtUp: () => boolean;
}): Promise<boolean> => {
	if (!engine.isCaughtUp()) {
		return false;
	}
	try {
		const stranded = await findStrandedScheduleRows();
		return stranded.gids.length === 0;
	} catch {
		// Can't prove it's fine, so don't claim it is.
		return false;
	}
};

export const getSimSafety = async (): Promise<
	{ safe: true } | { safe: false; reason: string }
> => {
	const engine = getSyncEngine();
	if (!engine) {
		return { safe: true };
	}

	if (engine.isBusyApplying()) {
		return {
			safe: false,
			reason: "Still applying changes from the cloud. Try again in a moment.",
		};
	}

	const lid = g.get("lid");
	if (await loadResyncNeeded(lid)) {
		// On v2 the flag is v1 bookkeeping with no v1 healer running: the chain
		// IS the repair, so "caught up" means healed - clear it and move on.
		// Left in place, it blocked every sim forever with a promise
		// ("will self-heal shortly") that nothing on v2 was going to keep.
		if (engine instanceof SyncEngineV2 && engine.isCaughtUp()) {
			await saveResyncNeeded(lid, false);
			syncDebugLog("v2:cleared-stale-resync-flag", { lid });
		} else if (await noDamageToRepair(engine)) {
			// v1, and the same promise was being broken here for a different
			// reason. The flag says this device once skipped a bulk change; the
			// ONLY thing that clears it is a successful room-snapshot restore. A
			// room that has never published a snapshot has nothing to restore, so
			// the flag is permanent and every sim is refused forever - "I click
			// sim and nothing happens", exactly as reported, on a device that had
			// drained to the head of the log with nothing missing.
			//
			// The flag is evidence something WAS skipped. It is not evidence
			// anything is STILL missing. When both available checks say otherwise
			// - drained to the head, and no schedule row stranded behind the days
			// the league has played - there is nothing left to repair, so stop
			// pretending there is.
			await saveResyncNeeded(lid, false);
			syncDebugLog("connect:cleared-stale-resync-flag", { lid });
		} else {
			if (engine instanceof SyncEngineV2) {
				void engine.catchUp();
			}
			return {
				safe: false,
				reason:
					"This device is flagged for a repair pass and will self-heal shortly. Try again in a minute.",
			};
		}
	}

	// A device whose league fails the catastrophe check (stripped rosters, no
	// teams) must not sim: results computed from a broken league are broken
	// results, and once published they become everyone's. Position guards below
	// can't catch this - a damaged device can be at exactly the right (season,
	// phase, day) with half its players missing.
	const integrityProblems = await checkLeagueIntegrity();
	if (integrityProblems.length > 0) {
		return {
			safe: false,
			reason: `This device's copy of the league looks damaged (${integrityProblems[0]}). Use Force Resync on the sync page to restore it from the room - simming now would spread the damage.`,
		};
	}

	// The position-stamp comparison below is a v1 heuristic: v1's watermark
	// could lie, so the room's stamped position served as a second opinion. A
	// v2 device that is caught up has PROVEN its state (applied version ==
	// CAS-committed room version) - strictly stronger evidence than a stamp
	// that only updates on advances and whose "day" arithmetic disagrees with
	// computed positions in the playoffs. Judging v2 by the stamp blocked sims
	// with a false "this device reads as X but the room says Y".
	if (engine instanceof SyncEngineV2) {
		return { safe: true };
	}

	const announced = engine.getAuthority()?.position;
	if (announced) {
		let position;
		try {
			position = await getLeaguePosition();
		} catch {
			return { safe: true };
		}

		// "Ahead" of a stamp a conclusive full replay has proven stale is not a
		// health problem - the log agrees with this device, the stamp is just old
		// (see checkBehindAuthority). Blocking on it froze every action in the
		// room until someone restamped.
		if (
			isAheadOfPosition(position, announced) &&
			!isBehindPosition(position, announced) &&
			staleStampKey !== undefined &&
			describePosition(announced) === staleStampKey
		) {
			return { safe: true };
		}

		if (
			isBehindPosition(position, announced) ||
			isAheadOfPosition(position, announced)
		) {
			// Durable, so the next connect replays the log and puts it right even
			// if this session never gets the chance.
			await saveResyncNeeded(lid, true);
			return {
				safe: false,
				reason: `This device reads as ${describePosition(position)} but the room's last known position is ${describePosition(announced)}. It will repair itself shortly - simming now would fork the league.`,
			};
		}
	}

	return { safe: true };
};

// Force a full catch-up: re-read the log's tail and re-apply it from scratch.
// The one-click fix for a device that silently diverged. Bounded like every
// other replay - the truly unbounded read never finished on a phone, which
// made the one button sold as the big hammer the one recovery that couldn't
// complete. A deeper window than the automatic paths, since a person pressing
// the button has judged something is wrong and will wait for it.
const MANUAL_RESYNC_WINDOW_ENTRIES = 10_000;

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
	// Give an in-flight drain a moment to wind down rather than shouldering it
	// aside; past that, the person clicked the button for a reason.
	await engine.waitUntilIdle(30_000);

	// V2 has its own checkpoint, and must never come near the code below: that
	// path reads the V1 snapshot docs, which a v2 room never writes. In a room
	// that has only ever been v2 the read finds nothing and the whole button
	// degrades to an ordinary catch-up - a no-op exactly when the version
	// counter agrees with the room and the DATABASE is what's wrong, which is
	// the only reason anyone presses this. In a room upgraded from v1 it is
	// worse: the stale v1 snapshot restores over the league while
	// syncV2AppliedVersion stays at the head, so the device is rolled back and
	// then believes it is caught up.
	if (engine instanceof SyncEngineV2) {
		return engine.forceCheckpointRestore();
	}

	// The button means "my league looks wrong - make it match the room". The
	// trustworthy way to do that is the checkpoint: restore the room's snapshot
	// (a complete, consistent base - works no matter how far behind or how
	// corrupted this device is) and replay only the tail after it. A windowed
	// replay onto live state is the fallback for rooms with no snapshot, not
	// the tool of choice: re-applying old changesets over current data visibly
	// rewinds the league, and anything the guards decline strands old values.
	try {
		const restored = await restoreFromRoomSnapshot(engine);
		if (restored) {
			const drained = await engine.catchUp();
			return {
				total: 1,
				applied: drained ? 1 : 0,
				incomplete: drained ? 0 : 1,
				failed: !drained,
			};
		}
	} catch (error) {
		console.error("Snapshot restore during manual resync failed", error);
	}

	// The checkpoint didn't restore. If the room HAS one, it was refused for
	// cause (poisoned, unreadable) - and the windowed replay is not a fallback,
	// it is the league-wrecker: re-applying old history over live state is what
	// wiped a league tonight. Refuse and say what actually fixes it. Only a
	// room that has never published a checkpoint at all gets the replay, since
	// there the log IS the complete history and this button is the only tool.
	const meta = await engine.transport.fetchRoomSnapshotMeta?.();
	if (meta !== undefined) {
		throw new Error(
			"The room's checkpoint is damaged, so this device can't safely resync from it yet. Whoever is in charge of simming just needs the app open for a few minutes - a fresh checkpoint publishes automatically - then try again.",
		);
	}

	return engine.resyncAll({ windowEntries: MANUAL_RESYNC_WINDOW_ENTRIES });
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

// How long a follower may sit out of step with the position the sim authority
// announced before we stop assuming it's just propagation delay. A day's
// changeset is chunked across many entries, so a few seconds off is completely
// normal - in either direction, since the authority stamps its position as a
// separate write from the changeset it describes.
const BEHIND_GRACE_MS = 30 * 1000;

// Only one recovery at a time, and remember when we first noticed.
let behindSince: number | undefined;
let healingBehind = false;

// The missed-data heal for the CURRENT session (set on connect, singleflight),
// so an engine that skips a batch mid-session can trigger it immediately
// instead of leaving the device visibly broken until the next launch.
let healingMissedData = false;
let healMissedDataNow: ((trigger: string) => Promise<void>) | undefined;

// Whether the "missing some league data" warning has already been shown this
// session. The heal retries on a timer and on every engine skip, and one
// standing warning is the whole message - repeating it just stacks toasts.
let warnedMissingData = false;

// Test-only: this checker deliberately keeps state between ticks (grace
// timing, one-recovery-at-a-time, the proven-stale stamp), and tests need each
// scenario to start from a device that has noticed nothing yet.
export const resetBehindAuthorityStateForTesting = () => {
	behindSince = undefined;
	healingBehind = false;
	staleStampKey = undefined;
	staleStampHydrated = true;
};

// An announced position stamp a conclusive full replay has PROVEN stale: the
// log was re-read end to end, it agreed with this device, and this device was
// still "ahead" of the stamp - which can only mean the stamp is old news (the
// authority missed a restamp), because every state-changing byte in the room
// travels through that same log. Remembered so the checker stops grinding
// replays against it and the sim guard stops blocking actions over it; cleared
// the moment the announced stamp changes.
let staleStampKey: string | undefined;
let staleStampHydrated = false;

// The verdict survives reloads. Without this, every page load re-noticed the
// same stale stamp and re-proved it expensively - to the user, the season
// visibly "re-simming itself" on every refresh until the authority restamped.
const loadStaleStampKey = async () => {
	if (staleStampHydrated) {
		return;
	}
	staleStampHydrated = true;
	try {
		const lid = g.get("lid");
		if (typeof lid === "number") {
			const league = await idb.meta.get("leagues", lid);
			if (typeof (league as any)?.syncStaleStampKey === "string") {
				staleStampKey = (league as any).syncStaleStampKey;
			}
		}
	} catch {
		// Advisory state; absent is fine.
	}
};

const saveStaleStampKey = async (value: string | undefined) => {
	staleStampKey = value;
	try {
		const lid = g.get("lid");
		if (typeof lid !== "number") {
			return;
		}
		const league = await idb.meta.get("leagues", lid);
		if (league) {
			if (value === undefined) {
				delete (league as any).syncStaleStampKey;
			} else {
				(league as any).syncStaleStampKey = value;
			}
			await idb.meta.put("leagues", league);
		}
	} catch {
		// Advisory state; the in-memory value still holds for this session.
	}
};

// The check the engine cannot do for itself.
//
// "Am I caught up?" is answered by comparing a watermark against the highest
// entry the engine has been HANDED - so an entry that never arrived leaves it
// confidently, silently behind, showing the next day as upcoming and waiting
// forever. Reconnecting doesn't help, because a fresh catch-up starts from the
// same banked watermark and the server agrees there is nothing after it.
//
// The sim authority stamps how far the league has actually got onto the
// authority doc, which every device already watches. Comparing that against
// local data is evidence from outside the broken component.
//
// The two ways a room can be out of step look identical from a follower's
// screen, and this tells them apart: if a full re-read of the log closes the
// gap, this device had lost entries; if it doesn't, the advance was never
// uploaded and the gap is on the simmer's side. Both are worth saying out loud
// - the whole reason this went unnoticed is that neither said anything.
//
// A follower can also end up PAST the room, which is worse, because none of the
// engine's own bookkeeping will ever call it a problem: it looks caught up, it
// isn't behind on anything, and it will happily sit there. That is not a
// theoretical state - a phase scored against the wrong season applied an old
// offseason's free agency over a live regular season, and the device stayed in
// free agency with nothing watching for it. Same evidence, same recovery.
// Exported for tests; production calls it only from the health tick.
export const checkBehindAuthority = async () => {
	const engine = getSyncEngine();
	if (!engine || healingBehind) {
		return;
	}

	// A bulk pass is mid-recovery (a reimport's backlog drain, a replay). Its
	// position is legitimately in motion - a reading taken now says nothing,
	// and healing "over" it would mean two appliers interleaving, which is
	// worse than either problem this check exists to catch. Look again after
	// it finishes.
	if (engine.isBusyApplying()) {
		return;
	}

	const announced = engine.getAuthority()?.position;
	// Nobody has announced a position (an older client is simming).
	if (!announced) {
		behindSince = undefined;
		return;
	}

	let localPosition;
	try {
		localPosition = await getLeaguePosition();
	} catch {
		return;
	}
	const ahead = isAheadOfPosition(localPosition, announced);

	// The AUTHORITY compares against its own stamp. Normally they agree, but a
	// missed restamp (historically: a fire-and-forget live sim of the season's
	// final game, which stamped before the game had simulated) leaves the room
	// stamped in the past - and then every caught-up follower reads as "ahead of
	// the room" and grinds full-log replays forever, while the sim guard blocks
	// the whole room from advancing. Only the authority can fix the stamp, so it
	// falls through to the shared ahead-branch below: a conclusive replay
	// validates local state against the log and restamps from the healed truth
	// (rather than blindly trusting a DB that could itself be the corrupt party).
	if (engine.isAuthority() && !ahead) {
		behindSince = undefined;
		return;
	}

	if (!ahead && !isBehindPosition(localPosition, announced)) {
		behindSince = undefined;
		return;
	}

	// A stamp already proven stale by a conclusive replay: nothing new to do
	// until the authority restamps. The moment the announced value changes, the
	// proof no longer applies and checking resumes.
	await loadStaleStampKey();
	if (staleStampKey !== undefined) {
		if (ahead && describePosition(announced) === staleStampKey) {
			behindSince = undefined;
			return;
		}
		if (describePosition(announced) !== staleStampKey) {
			await saveStaleStampKey(undefined);
		}
	}

	// Out of step. Give the ordinary path time to deliver before doing anything -
	// a sim that just finished is legitimately still arriving, and the authority
	// stamps its position separately from the changeset, so either side can lead
	// for a moment.
	const now = Date.now();
	if (behindSince === undefined) {
		behindSince = now;
		return;
	}
	if (now - behindSince < BEHIND_GRACE_MS) {
		return;
	}

	healingBehind = true;
	try {
		syncDebugLog("connect:behind-authority", {
			direction: ahead ? "ahead" : "behind",
			local: describePosition(localPosition),
			announced: describePosition(announced),
			engine: engine.getCatchUpDiagnostics(),
		});

		// Past the room. There is nothing to download - the entries that describe
		// where the league really is were already delivered, and something applied
		// over the top of them. Only replaying the log in order can undo that, and
		// a bounded window is enough: the goal is to land on the room's CURRENT
		// state, not to recover ancient history.
		if (ahead) {
			// THE RULE THAT ENDS THIS CLASS OF INCIDENT: a device that has drained
			// the shared log to its head cannot be "ahead of the room" - every
			// state-changing byte in the room travels through that log, and this
			// device has all of it. One genuine server round-trip to the head is
			// the whole proof. The full-log replay this used to run instead proved
			// nothing more - its apply ceiling is capped by the very stamp under
			// test, so it declined most of what it read and always landed exactly
			// where the device already was, while looking to the person watching
			// like the whole season re-simming itself. On the authority the replay
			// still runs (below): there it has a real job, restamping from
			// validated state.
			if (!engine.isAuthority()) {
				const drained = await engine.catchUp();
				const announcedFresh = engine.getAuthority()?.position ?? announced;
				const stillAhead = isAheadOfPosition(
					await getLeaguePosition(),
					announcedFresh,
				);
				if (drained && stillAhead) {
					await saveStaleStampKey(describePosition(announcedFresh));
					behindSince = undefined;
					try {
						await saveResyncNeeded(g.get("lid"), false);
					} catch {
						// Advisory.
					}
					syncDebugLog("connect:stale-stamp-verdict", {
						local: describePosition(await getLeaguePosition()),
						announced: describePosition(announcedFresh),
					});
					console.log(
						`[sync] This device holds the entire shared log and is ahead of the room's stamp (${describePosition(announcedFresh)}) - the stamp is out of date. Waiting for whoever is in charge of simming to advance or update, which refreshes it.`,
					);
					return;
				}
				if (drained && !stillAhead) {
					behindSince = undefined;
					return;
				}
				// Could not reach the head conclusively - fall through to the
				// replay, which can page through a backlog plain catch-up could not.
			}

			const result = await engine.resyncAll({
				windowEntries: RESYNC_WINDOW_ENTRIES,
			});
			const after = await getLeaguePosition();
			syncDebugLog("connect:ahead-authority-resynced", {
				...result,
				after: describePosition(after),
			});

			// Compare against the CURRENT stamp, not the one captured before the
			// replay - when this device is the authority, the conclusive replay
			// just restamped it (SyncEngine restamps after every clean pass), and
			// that IS the fix.
			const announcedNow = engine.getAuthority()?.position ?? announced;

			if (!isAheadOfPosition(after, announcedNow)) {
				behindSince = undefined;
				console.log(
					`[sync] This device had got ahead of the room; replaying the shared log put it back to ${describePosition(after)}.`,
				);
			} else if (
				!result.failed &&
				result.incomplete === 0 &&
				result.total > 0
			) {
				// The verdict that matters. The entire log was re-read and re-applied
				// cleanly, and this device STILL reads ahead of the stamp - so the
				// LOG agrees with this device, and the stamp is simply stale (the
				// authority missed a restamp). That is not corruption and cannot be
				// repaired from here; grinding the same replay every 30 seconds
				// looked to the user like the season re-simming itself in a loop,
				// and the durable repair flag it kept setting blocked every action.
				// Stand down until the stamp changes, and clear the flag - a device
				// the full log agrees with has nothing to repair.
				await saveStaleStampKey(describePosition(announcedNow));
				behindSince = undefined;
				try {
					await saveResyncNeeded(g.get("lid"), false);
				} catch {
					// The flag is advisory; failing to clear it is not worth failing on.
				}
				console.log(
					`[sync] This device is at ${describePosition(after)} and a full replay of the shared log agrees. The room's stamp (${describePosition(announcedNow)}) is out of date - waiting for whoever is in charge of simming to advance or reconnect, which refreshes it.`,
				);
			} else {
				// The replay didn't conclude (fetch failure, window exhausted), so
				// nothing was proven either way. Do not keep grinding the log every
				// 30 seconds; try again on the next grace window.
				behindSince = now;
				console.error(
					`[sync] This device reads as ${describePosition(after)} but the room is on ${describePosition(announcedNow)}, and replaying the shared log did not resolve it.`,
				);
			}
			return;
		}

		// Cheap first: an ordinary catch-up. Covers a listener that died quietly
		// while the watermark was still honest.
		await engine.catchUp();
		if (!isBehindPosition(await getLeaguePosition(), announced)) {
			behindSince = undefined;
			return;
		}

		// Still behind, so this device's STATE is suspect, not just its download
		// position. The trustworthy repair is the checkpoint: restore the room's
		// snapshot (a complete, consistent base) and take the tail forward from
		// there. NOT a windowed replay onto live state - re-applying old
		// changesets over current data is archaeology: the league visibly rewinds
		// as old vintages land, and any unit the guards then decline strands
		// those old values in place. That is the "device suddenly goes way back
		// in time and the league comes back subtly wrong" failure, and it was the
		// FIRST resort here. Now it's the fallback for rooms with no snapshot.
		let conclusive = false;
		let restored = false;
		try {
			restored =
				(await restoreFromRoomSnapshot(engine, { automatic: true })) !==
				undefined;
			if (restored) {
				conclusive = await engine.catchUp();
				const healed = await getLeaguePosition();
				if (!isBehindPosition(healed, announced)) {
					behindSince = undefined;
					console.log(
						`[sync] Restored the room's snapshot and caught up to ${describePosition(healed)}.`,
					);
					return;
				}
			}
		} catch (error) {
			console.error("Snapshot restore during behind-recovery failed", error);
		}

		if (!restored) {
			// No usable checkpoint (none published yet, or the published one was
			// refused). The windowed replay used to run here, and replaying old
			// history over a live database is the documented league-wrecker - so
			// it no longer runs ANYWHERE automatically. Stand down until the next
			// grace window; the authority publishes a usable checkpoint within
			// minutes (poison eviction + first-checkpoint publish), and the next
			// pass restores it.
			syncDebugLog("connect:behind-no-usable-checkpoint", {
				announced: describePosition(announced),
			});
			console.error(
				"[sync] Behind the room with no usable checkpoint to restore yet. Waiting for one - whoever is in charge of simming just needs the app open for a few minutes.",
			);
			behindSince = now;
			return;
		}

		const after = await getLeaguePosition();

		// The advance is not in the cloud, so no amount of downloading will find
		// it: it is still sitting in the simming device's upload queue.
		if (conclusive) {
			behindSince = now;
			logEvent({
				type: "error",
				text: "The latest sim hasn't been uploaded yet. Nothing to fix here - ask whoever is in charge of simming to open the app until their queued uploads finish.",
				saveToDb: false,
				persistent: true,
			});
			console.error(
				`[sync] Behind the room (${describePosition(after)} vs ${describePosition(announced)}) and the change log does not contain the gap - the simmer has not uploaded it.`,
			);
		}
	} catch (error) {
		syncDebugLog("connect:behind-authority-failed", { error });
	} finally {
		healingBehind = false;
		// Re-arm the grace window on EVERY exit that didn't resolve the problem.
		//
		// The branches above each set this themselves - except one, and it is the
		// one a device missing data actually takes: the snapshot restored fine,
		// but the catch-up after it could not reach the head, because there IS a
		// gap (that is the whole complaint). That path fell out of the function
		// with behindSince untouched, so the next five-second health tick walked
		// straight past the grace check and restored the entire league again.
		// Forever. On a phone that is a tab the OS kills, reloads, and kills
		// again - "I click sim and the page crashes and reloads".
		if (behindSince !== undefined) {
			behindSince = Date.now();
		}
	}
};

export const connectSharedLeague = async (options: {
	code: string;
	isHost?: boolean;
	explicit?: boolean;
	// Create this room on the v2 sync protocol (version chain). Only honored on
	// an explicit host join of a room with no v1 history; an existing room's
	// protocol is always auto-detected regardless of this flag.
	v2?: boolean;
	// A bring-your-own-Firestore project for this room. Omitted for ordinary
	// rooms, which use the built-in default project.
	firebaseConfig?: FirebaseConfig;
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
	v2 = false,
	firebaseConfig,
}: {
	code: string;
	isHost?: boolean;
	explicit?: boolean;
	v2?: boolean;
	firebaseConfig?: FirebaseConfig;
}) => {
	const trimmed = code.trim();
	if (!trimmed) {
		throw new Error("A league code is required.");
	}

	// Never connect while the league itself is still loading (or an import is
	// mid-flight). The UI's auto-reconnect fires as soon as it sees a lid, which
	// on a SharedWorker (desktop) races the worker's own league switch: the
	// connect flow then reads `g` inside the reset window ("Attempt to get
	// g.userTid while it is not already set") or opens a transaction on the
	// league DB while beforeLeague is closing the previous handle ("The database
	// connection is closing"). Wait for the load to finish; if it doesn't within
	// the deadline, report not-connected - the auto-reconnect retries with
	// backoff, and the sim gate (markSyncRequired) is already holding sims.
	{
		const deadline = Date.now() + 30_000;
		while (!local.leagueLoaded && Date.now() < deadline) {
			await new Promise((resolve) => {
				setTimeout(resolve, 250);
			});
		}
		if (!local.leagueLoaded) {
			syncDebugLog("connect:league-not-loaded", { code: trimmed });
			return { connected: false };
		}
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

	// Point Firebase at this room's project - a bring-your-own-Firestore project
	// when the caller supplied one, or the built-in default otherwise - BEFORE
	// the first Firebase touch (auth) below. With no custom config this resets to
	// the default project, so an ordinary room is unaffected.
	setActiveFirebaseConfig(firebaseConfig);

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

	// Which protocol is this room on? Detected from the room itself - the
	// pointer doc exists if and only if the room runs the v2 version chain. The
	// `v2` option can only INITIALIZE a fresh room (explicit host join, no v1
	// history), never convert one.
	// allowCache: protocol detection tolerates a briefly-unreachable server (a
	// stale answer and no answer are equally harmless here - the protocol never
	// changes after room creation).
	let v2State = await transport
		.fetchRoomV2State({ allowCache: true })
		.catch(() => undefined);
	if (v2State === undefined && v2 && explicit && isHost) {
		const v1Entries = await transport.countEntriesSince(0).catch(() => 1);
		if (v1Entries === 0) {
			const initialized = await transport
				.commitV2Version(
					{
						version: 0,
						authorId: clientId,
						byName: "Host",
						at: Date.now(),
						action: "init",
					},
					0,
				)
				.catch(() => false);
			if (initialized) {
				v2State = await transport.fetchRoomV2State().catch(() => undefined);
				syncDebugLog("connect:v2-room-initialized", { code: trimmed });
			}
		}
	}
	const isV2 = v2State !== undefined;

	// Room <-> league binding, both protocols, checked BEFORE anything can
	// move in either direction. A room is claimed by exactly one league
	// lineage, permanently; a league carries its identity in gameAttributes
	// (so it travels inside every checkpoint and export). A league that
	// arrives at a room claimed by a DIFFERENT lineage is refused at the door
	// - the failure mode this kills is the one that corrupted a main save
	// twice: wrong-league data reaching a league DB through a room, whether
	// via a zombie engine, a second tab, an old build, or a reused room code
	// still holding another league's state.
	// The discriminator is EXPLICIT vs AUTOMATIC, exactly as the older
	// room-fingerprint check above uses it, because that is what actually
	// separates the two situations:
	//
	//   - Typing a room code and pressing Connect IS the statement "this
	//     league belongs in this room". A league-mate joining with a copy that
	//     minted its own identity (or an older copy, or a re-created room) must
	//     always be able to do this, or the protection locks legitimate players
	//     out of their own league with no way back. It adopts the room's
	//     identity.
	//   - An AUTOMATIC reconnect carries no such intent, so a mismatch there is
	//     reported loudly - but it does NOT block the connect. See the comment
	//     on the "refused" branch below for why that check had to give way.
	{
		const outcome = await resolveLeagueIdentity({
			localId: await readLocalLeagueId().catch(() => undefined),
			explicit,
			fetchRoomLeagueId: () => transport.fetchRoomLeagueId(),
			claimRoomLeagueId: (id) => transport.claimRoomLeagueId(id),
		});
		if (outcome.action === "unverified") {
			// Could not reach the binding doc. Connecting anyway is safe: the
			// payload provenance check still refuses a wrong-league restore, and
			// blocking here would strand a device on a flaky network with a
			// spinner it can never get past.
			syncDebugLog("connect:league-identity-unverified", {
				code: trimmed,
				error: outcome.error,
			});
		} else if (outcome.action === "refused") {
			// ADVISORY, NOT BLOCKING - a deliberate retreat.
			//
			// Identities are minted per league COPY, so two devices holding the
			// SAME league can hold different ones: whichever claimed the room
			// first wins, and every other device mismatches forever. Refusing on
			// that stopped automatic reconnects working on every device but one -
			// a constant, certain harm - in exchange for a check that cannot
			// distinguish "same league, minted separately" from "different
			// league". Only manual Connect got through, because that rebinds.
			//
			// The check that CAN tell them apart is the payload provenance test
			// at restore time: it compares the identity carried inside the data
			// against this league's own and refuses a wrong-league restore with
			// nothing touched. That is where the damage would actually happen,
			// and it does not depend on this one.
			//
			// So: adopt the room's identity, connect, and say so. Once every
			// device has converged on the room's identity - which now happens by
			// itself, since a device without one adopts - a mismatch will mean
			// something again.
			syncDebugLog("connect:league-identity-adopted-on-mismatch", {
				code: trimmed,
				local: outcome.local,
				room: outcome.room,
			});
			if (!explicit) {
				logEvent({
					type: "error",
					text: `This league and room ${trimmed} were carrying different sync ids, so this device adopted the room's. If you did not expect this league to be in room ${trimmed}, disconnect before simming.`,
					saveToDb: false,
					persistent: true,
				});
			}
			try {
				await writeLocalLeagueId(outcome.room);
			} catch (error) {
				syncDebugLog("connect:league-identity-write-failed", {
					error: String(error),
				});
			}
		} else if (outcome.action !== "matched") {
			// Writing the identity is local-only, and failing to record it must
			// not block the connect either - the next connect re-derives it.
			try {
				await writeLocalLeagueId(outcome.id);
			} catch (error) {
				syncDebugLog("connect:league-identity-write-failed", {
					error: String(error),
				});
			}
			syncDebugLog(
				outcome.action === "rebound"
					? "connect:league-identity-rebound"
					: "connect:league-identity-bound",
				{ code: trimmed, action: outcome.action },
			);
		}
	}

	// Marker <-> room binding. A v2 applied-version marker is meaningful only
	// within ONE room's chain, but it lives in the league DB - so a COPY of a
	// league that synced in some other room arrives carrying that room's
	// number, and every downstream decision (caught-up, publish target,
	// checkpoint freshness) would believe it. That is how two rooms sharing a
	// league lineage cross-contaminated: copies pass the room fingerprint
	// check by design (all members import the same file), so the fingerprint
	// cannot catch this. If this league copy is not bound to THIS room, zero
	// the marker: it joins cleanly through the room's checkpoint, exactly like
	// a fresh device.
	if (isV2) {
		try {
			const roomRow = await (idb.league as any).get(
				"gameAttributes",
				"syncV2Room",
			);
			const boundRoom =
				typeof roomRow?.value === "string" ? roomRow.value : undefined;
			if (boundRoom !== trimmed) {
				const transaction = (idb.league as any).transaction(
					"gameAttributes",
					"readwrite",
				);
				transaction.objectStore("gameAttributes").put({
					key: "syncV2AppliedVersion",
					value: 0,
				});
				transaction.objectStore("gameAttributes").put({
					key: "syncV2Room",
					value: trimmed,
				});
				await transaction.done;
				syncDebugLog("connect:v2-room-binding-reset", {
					previous: boundRoom,
					code: trimmed,
				});
			}
		} catch (error) {
			// Refuse to connect rather than run on an unverified marker.
			throw new Error(
				`Could not verify this league's sync state: ${String(error)}`,
			);
		}
	}

	// Retention gap check. Catch-up is a `ts >` range read, so a device whose
	// watermark predates everything left in the log finds nothing missing and
	// would declare itself current while holding stale records - silently, which
	// is the worst way for this system to fail. Compare against the oldest entry
	// that actually survives and refuse to connect rather than diverge.
	//
	// Only reachable once the log is being trimmed AND this device has been away
	// longer than the retention window; a device inside the window sees its own
	// already-applied entries as the oldest and passes.
	if (!isV2) {
		let oldestSeq: number | undefined;
		let probed = true;
		try {
			oldestSeq = await transport.fetchOldestEntrySeq();
		} catch (error) {
			// Never turn a flaky read into a lockout - if we can't see the log, fall
			// through and let the normal catch-up machinery deal with it.
			probed = false;
			syncDebugLog("connect:oldest-entry-probe-failed", { error });
		}
		if (probed && isTooFarBehind(watermark, oldestSeq)) {
			syncDebugLog("connect:too-far-behind", {
				lid: lidNumber,
				watermark,
				oldestSeq,
				retentionDays: RETENTION_DAYS,
			});
			// Same shape as the room-mismatch refusal below: drop the half-set
			// session state and tell the UI we're not connected, then throw so an
			// explicit join shows the reason.
			//
			// The persisted room binding is deliberately KEPT (unlike a mismatch
			// refusal): the fix is to re-import this league, and once that lands a
			// near-head watermark the next automatic reconnect just works, with no
			// need to re-enter the code.
			syncRequired = false;
			connectedLid = undefined;
			void toUI("updateLocal", [
				{ mpSyncActive: false, mpSyncReady: false, mpSyncReconnecting: false },
			]);
			throw new Error(
				`This device is too far behind to catch up. The league's change history only goes back ${RETENTION_DAYS} days, and this copy last synced before that. Ask whoever is in charge of simming to send you a fresh export of the league, then import it and rejoin.`,
			);
		}
	}

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

	const engine = isV2
		? new SyncEngineV2(transport, {
				isHost,
				code: trimmed,
				onAuthorityChange: (authority) => {
					currentHostName = authority?.holderName;
					pushAuthorityToUI(
						authority?.holderId === clientId,
						authority?.holderName,
					);
					pushEditsPaused();
				},
				onReadyChange: (ready) => {
					pushReadyToUI(ready);
				},
				onPendingChange: (count) => {
					pushPendingUploads(count);
				},
				onUploadComplete: () => {
					uploadOkCounter += 1;
					void toUI("updateLocal", [{ mpSyncUploadOk: uploadOkCounter }]);
				},
				// The header's catching-up indicator. V2 only reports when the
				// device is visibly behind and working on it (a multi-version walk,
				// or fetches failing and retrying) - exactly when a quiet screen
				// would otherwise read as "is this thing broken?".
				onCatchUpProgress: (progress) => {
					syncDebugLog("connect:catchup-indicator", {
						showing: progress !== undefined,
						done: progress?.done,
						total: progress?.total,
					});
					catchUpPillShowing = progress !== undefined;
					void toUI("updateLocal", [{ mpCatchUp: progress }]);
				},
			})
		: new SyncEngine(transport, {
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
					catchUpPillShowing = progress !== undefined;
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
				// The engine just abandoned a bulk change whose chunks weren't in the log -
				// it silently skipped shared state. Persist a durable marker (so even a
				// reload heals), then heal NOW: the last device that waited for its next
				// launch spent an evening visibly missing a day of games. The delay lets
				// the abandoning pass finish; the heal itself also waits for idle.
				onResyncNeeded: () => {
					void saveResyncNeeded(lid, true);
					setTimeout(() => {
						void healMissedDataNow?.("engine-skip");
					}, 5000);
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

	// Self-heal a device that silently skipped a bulk change in a prior session
	// (a chunked batch abandoned with its chunks missing, banking the watermark
	// past it - the failure that stranded two follower devices on the old phase
	// after a ready-up advance). The gap is durable (syncResyncNeeded), so a
	// normal catch-up won't re-fetch below the watermark; a full-log resync
	// (idempotent, safe anytime - same as the manual "Force full resync") re-reads
	// and re-applies the whole log in order to land on the room's real state.
	// Clear the marker only after a resync that actually read the log AND applied
	// it cleanly, so an offline/empty read or a still-missing chunk retries on the
	// next connect instead of clearing the flag and staying stuck. Never blocks
	// the connect.
	const healMissedData = async (trigger: string) => {
		if (healingMissedData || isV2) {
			// v2 has exactly one recovery (engine.catchUp) and its own checkpoint
			// protocol; none of this machinery applies.
			return;
		}
		healingMissedData = true;
		try {
			// Two independent reasons to recover. The MARKER is the engine telling
			// on itself: it knowingly skipped a bulk change. The STRANDED ROWS are
			// evidence from the data, and they catch the case the marker can't - a
			// day's changeset that went missing without the engine ever noticing
			// (a dropped delivery, a watermark banked past an entry that never
			// arrived).
			const strandedBefore = await findStrandedScheduleRows();
			const markerSet = await loadResyncNeeded(lid);
			if (strandedBefore.gids.length > 0) {
				console.error(
					`[sync] This device never received day ${strandedBefore.days.join(", ")} of the current season - the league has played through day ${strandedBefore.maxPlayedDay}. Re-reading the shared log to recover it.`,
				);
				syncDebugLog("connect:stranded-schedule-detected", {
					days: strandedBefore.days,
					rows: strandedBefore.gids.length,
					maxPlayedDay: strandedBefore.maxPlayedDay,
				});
				// Durable, so a reload mid-recovery still heals.
				await saveResyncNeeded(lid, true);
			} else if (!markerSet) {
				return;
			}

			// Before reaching for the checkpoint: if the flag is the only thing
			// wrong, retire it. A room that never published a snapshot has nothing
			// to restore, and the flag's only other exit is that restore - so
			// without this the device stays flagged forever, refusing every sim,
			// while being demonstrably whole. (Stranded rows above are the other
			// half of the evidence; if there were any, this doesn't fire.)
			if (
				strandedBefore.gids.length === 0 &&
				markerSet &&
				(await noDamageToRepair(engine))
			) {
				await saveResyncNeeded(lid, false);
				syncDebugLog("connect:cleared-stale-resync-flag", { trigger });
				console.log(
					"[sync] Cleared this device's repair flag: it has read everything the room published and nothing is missing.",
				);
				return;
			}

			// The connect-time drain may be mid-backlog (a reimport can have months
			// to replay), and that drain, run to completion, IS the correct
			// recovery. Starting a replay on top of it meant two appliers
			// interleaving - whichever wrote last won, and when that was the
			// slower, older pass, the device got dragged to a state the room left
			// long ago. Wait for idle; if the drain is still healthily working
			// after all this, skip - the durable marker retries on the next
			// connect.
			const idle = await engine.waitUntilIdle(10 * 60 * 1000);
			if (!idle) {
				syncDebugLog("connect:auto-resync-skipped-busy", {
					markerSet,
					stranded: strandedBefore.gids.length,
				});
				return;
			}

			syncDebugLog("connect:auto-resync-start", {
				trigger,
				markerSet,
				stranded: strandedBefore.gids.length,
			});

			// Checkpoint-first, exactly like every other recovery. The windowed
			// replay can only re-apply what it can reach and what the guards will
			// accept; the checkpoint rewinds to a complete consistent base and the
			// tail replay walks forward IN ORDER through the missed day and
			// everything after it, bypassing the dedup that remembers the skipped
			// entries as already seen. That bypass is the whole point: an abandoned
			// batch's entries stay marked seen, which is precisely why a plain
			// catch-up could never backfill them.
			try {
				const restored = await restoreFromRoomSnapshot(engine, {
					automatic: true,
				});
				if (restored) {
					await engine.catchUp();
					const strandedNow = await findStrandedScheduleRows();
					if (strandedNow.gids.length === 0) {
						await saveResyncNeeded(lid, false);
						syncDebugLog("connect:auto-resync-healed-from-snapshot", {
							trigger,
						});
						console.log(
							"[sync] Recovered the missing day(s) from the room's checkpoint.",
						);
						return;
					}
				}
			} catch (error) {
				syncDebugLog("connect:auto-resync-snapshot-failed", { error });
			}

			// THE CHECKPOINT IS THE ONLY AUTOMATIC HEAL. THERE IS NO FALLBACK.
			//
			// The fallback this used to have - engine.resyncAll, the windowed replay
			// of the shared log over live state - is the thing that wiped a league
			// tonight, with the receipts in the sync log: the room's checkpoint was
			// rightly refused as poisoned, the heal fell through to the replay, and
			// the replay walked ~1000 old changesets from LAST SEASON'S OFFSEASON
			// over the current league (playMenu.untilResignPlayers is right there in
			// the capture), applying 936 of 2000 with the guards declining the rest.
			// Whole-record last-write-wins from months-old history over a live
			// database is not recovery, it is the wipe - and then the stranded-rows
			// "last resort" deleted 425 schedule rows from the wreckage it had just
			// made.
			//
			// So: missing data with no usable checkpoint means WAIT, visibly. The
			// durable marker stays set (blocking sims here), the authority publishes
			// a fresh checkpoint within minutes (poison eviction + first-checkpoint
			// publish in roomSnapshot.ts), and the next heal attempt restores it.
			// Waiting cannot damage anything; the replay provably can.
			syncDebugLog("connect:auto-resync-no-usable-checkpoint", { trigger });
			console.error(
				"[sync] This device is missing shared data and the room has no usable checkpoint yet. Waiting - whoever is in charge of simming just needs to have the app open for a few minutes, then this heals automatically.",
			);

			// Say it out loud only once the gap has actually outlasted the wait the
			// message describes, and only once per session. This branch runs on every
			// connect and on every engine skip, so without a gate an ordinary gap -
			// the kind that heals as soon as the simmer opens the app - greeted the
			// user with a persistent red error every single launch.
			const shouldWarn = await noteMissingDataAndShouldWarn(
				lid,
				warnedMissingData,
			);
			if (shouldWarn) {
				warnedMissingData = true;
				// "It repairs itself" stops being true once the automatic repair has
				// been switched off for killing the app. Say the thing the user can
				// actually act on instead.
				const stalled = await readRecoveryAttempt(lid);
				logEvent({
					type: "error",
					text:
						stalled !== undefined
							? "This device is missing some league data, and repairing it automatically didn't finish. Use Force Resync on the Multiplayer Sync page."
							: "This device is missing some league data. It will repair itself automatically once the person in charge of simming has the app open for a few minutes.",
					saveToDb: false,
					persistent: true,
				});
			} else {
				syncDebugLog("connect:missing-data-warning-suppressed", {
					trigger,
					pastGrace: shouldWarn,
					alreadyWarned: warnedMissingData,
				});
			}
		} catch (error) {
			syncDebugLog("connect:auto-resync-failed", { error: String(error) });
		} finally {
			healingMissedData = false;
		}
	};
	healMissedDataNow = healMissedData;
	warnedMissingData = false;
	void healMissedData("connect");

	// Played-game invariant sweep (v1-log fallout only): drop any schedule row
	// whose game already
	// exists (a phantom "upcoming" copy of a played game, left by a partially
	// applied or abandoned changeset in some prior session). Runs once per
	// connect regardless of whether anything new syncs in, so a device carrying
	// this corruption heals just by opening the league.
	if (!isV2) {
		void (async () => {
			try {
				const removed = await sweepPhantomScheduleRows();
				if (removed > 0) {
					syncDebugLog("connect:phantom-schedule-swept", { removed });
				}
			} catch (error) {
				syncDebugLog("connect:phantom-schedule-sweep-failed", {
					error: String(error),
				});
			}
		})();
	}

	// Same spirit for finished-season history: recompute playoffRoundsWon from
	// each season's bracket and fix what a past rough recovery left stale (the
	// "??? champion"). Runs once per connect, in the background.
	void (async () => {
		try {
			await repairLeagueHistory("connect");
		} catch (error) {
			syncDebugLog("connect:history-repair-failed", { error });
		}
	})();

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
		if (isV2) {
			// V2's whole health story: ask the server where the head is (the live
			// listener is the fast path, but this probe is what bounds staleness
			// when a listener dies silently - one tiny doc read per tick), and
			// keep the room's checkpoint fresh (authority). Both self-throttled
			// and single-flighted. Publishing needs no per-role work here - every
			// device uploads its own changes as versions.
			const engineNow = getSyncEngine();
			if (engineNow instanceof SyncEngineV2) {
				// First, before the probe: if the head has been known-ahead of the
				// applied version for two ticks, say so on screen and make sure a
				// catch-up is actually in flight. This is the only indicator for a
				// silently slow apply (nothing failing, gap too small for the walk
				// to report itself).
				engineNow.reportIfStuckBehind();
				void engineNow.probeHead();
				void engineNow.maybePublishCheckpoint();
			}
		} else {
			// Notice, and fix, being silently behind the rest of the room.
			void checkBehindAuthority();
			// On the authority: checkpoint the league once enough log has
			// accumulated, and prune entries the previous checkpoint already
			// covers. Self-throttled.
			const engineForSnapshot = getSyncEngine();
			if (engineForSnapshot) {
				void maybePublishRoomSnapshot(engineForSnapshot);
			}
		}
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
	followerFroze = false;
	liveBroadcastUnsub = transport.subscribeLiveBroadcast?.((meta) => {
		void handleLiveBroadcastMeta(meta, clientId, transport);
	});

	// Draft ready-up: watch everyone's ready state and auto-advance CPU picks
	// once every user team has readied (see draftReady.ts). No-op outside the
	// draft phase.
	setupDraftReady(transport);

	// Schedule-day sim fence: exactly one device per (season, day, games) may
	// sim, no matter what the advisory authority doc says (see simDayFence.ts).
	setupSimDayFence(transport);

	// Free-agency boards: watch everyone's ranked FA lists so the day advance
	// can resolve them (see faBoard.ts). No-op outside free agency.
	setupFaBoard(transport);
	setupLiveChat(transport);
	setupTriviaScores(transport);

	// Watch for a live lottery reveal (someone running the lottery while
	// everyone watches the picks flip in lockstep).
	followedLotteryReveal = undefined;
	lotteryRevealUnsub = transport.subscribeLotteryReveal?.((meta) => {
		handleLotteryRevealMeta(meta, clientId);
	});

	// Kick off the initial catch-up now, in the background so connect doesn't
	// block on a device that's been away a long time. V2: the single bounded
	// chain walk. V1: the paginated backlog drain (which also starts the live
	// changes subscription once caught up).
	if (isV2) {
		void engine.catchUp();
	} else {
		void driveCatchUp();
	}

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
		if (isV2) {
			// V2 staleness detection lives entirely in the 5s head probe
			// (server-fresh, timed, wedge-detecting, and it fires catch-up the
			// moment the head is past what's applied) - a 30s pointer re-read
			// on top of it bought nothing. This tick stays as the outbox's
			// unconditional backstop kick.
			void engine?.drainOutbox();
			return;
		}
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
	// Arm the invisible-write canary: any write outside a capture/apply window
	// while synced is a silent room fork in the making - report it loudly.
	changeTracker.setCanary(true);

	// Let the UI hide single-player-only chrome (e.g. the multi-team switcher)
	// and clear the "reconnecting" state.
	void toUI("updateLocal", [
		{
			mpSyncActive: true,
			mpSyncProtocol: isV2 ? ("v2" as const) : ("classic" as const),
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
	teardownSimDayFence();
	teardownFaBoard();
	teardownLiveChat();
	teardownTriviaScores();
	// Best-effort: end our own broadcast so we don't leave the room locked.
	if (activeBroadcast) {
		void endLiveBroadcast();
	}
	activeBroadcast = undefined;
	followedBroadcast = undefined;
	followedBroadcastPayload = undefined;
	followerFroze = false;
	currentTransport = undefined;
	lastPendingUploads = 0;
	catchUpPillShowing = false;
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
	// The missed-data heal closes over this session's engine; a torn-down
	// session must not be healable.
	healMissedDataNow = undefined;
	warnedMissingData = false;
	// A different room (or a fresh session on the same one) deserves a clean
	// shot at its snapshot rather than inheriting the last one's backoff.
	resetSnapshotRestoreBackoff();
	currentHostName = undefined;
	currentCloudReady = false;
	if (clearPersisted) {
		await clearPersistedSyncSession(lidToClear);
		// An explicit disconnect also drops any bring-your-own-Firestore project,
		// so a later default connect (or push) uses the built-in project. A plain
		// reconnect/league-switch teardown keeps it, since the next connect sets it
		// explicitly anyway.
		setActiveFirebaseConfig(undefined);
	}

	// Explicit disconnect clears the intent, so single-player simming works again.
	syncRequired = false;

	void toUI("updateLocal", [
		{
			mpSyncActive: false,
			mpSyncProtocol: undefined,
			mpSyncReady: false,
			mpSyncReconnecting: false,
		},
	]);
	pushAuthorityToUI(false, undefined);

	// Leave the tracker enabled in dev (the console logger uses it); otherwise
	// turn it back off so single-player has zero overhead. The canary is always
	// disarmed - uncaptured writes are normal outside a synced session.
	changeTracker.setCanary(false);
	if (process.env.NODE_ENV !== "development") {
		changeTracker.disable();
	}

	return { connected: false };
};

export const disconnectSharedLeague = () =>
	teardownSharedLeague({ clearPersisted: true });
