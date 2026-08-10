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
import { g, helpers, local, logEvent, toUI } from "../../util/index.ts";
import { env } from "../../util/env.ts";
import { ERROR_MESSAGE_SYNC_ROOM_MISMATCH } from "../../../common/constants.ts";
import { serializeChangeset, deserializeChangeset } from "./serialize.ts";
import { flushDeferredRefreshAfterLive } from "./changeset.ts";
import { syncDebugLog } from "./debugLog.ts";
import { repairLeagueHistory } from "./historyRepair.ts";
import { checkLeagueIntegrity } from "./leagueIntegrity.ts";
import { readRecoveryAttempt } from "./recoveryBreadcrumb.ts";
import { endLotteryReveal } from "./notifications.ts";
import {} from "../../../common/syncRetention.ts";
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

// This device's display name in shared rooms. Device-scoped rather than
// per-league, because the same person on the same browser is the same person in
// every room they join. Unset (or blank) means "fall back to the team I manage".
export const loadSyncDeviceName = async (): Promise<string | undefined> => {
	const stored = await idb.meta.get("attributes", "syncDeviceName");
	return typeof stored === "string" && stored.trim() !== ""
		? stored.trim()
		: undefined;
};

// What everyone ELSE sees next to this device's sims, notes, cards and
// notifications. The engine ships with the placeholder "You", and nothing ever
// replaced it - the one writer, registerMember, is only reached by enabling push
// notifications, and its single caller passes no name. So every shared string in
// the room read "You": "You is in charge of simming", notes authored by "You".
// Prefer an explicit name, then the team this device manages (already unique per
// person in a shared league, and needs no setup), then a neutral fallback.
export const resolveSyncLocalName = async (): Promise<string> => {
	const explicit = await loadSyncDeviceName();
	if (explicit !== undefined) {
		return explicit;
	}
	try {
		const tid = g.get("userTid");
		const team = g.get("teamInfoCache")[tid];
		if (team) {
			return `${team.region} ${team.name}`;
		}
	} catch {
		// g isn't loaded (or this league has no team for the tid); the neutral
		// fallback below is still better than the placeholder.
	}
	return "Another device";
};

// A room that has been running since before devices had real names still holds
// an authority doc whose holderName is the old "You" placeholder, and that
// renders as "You is in charge of simming" on every OTHER device. Treat it as
// unnamed so the existing "Another device" fallbacks take over; it fixes itself
// for real the next time that device claims.
const displayHolderName = (holderName: string | undefined) =>
	holderName === "You" ? undefined : holderName;

// Recompute this device's display name and push it everywhere the room reads it.
// Cheap and idempotent: returns without a single write when nothing changed.
// Never throws - the local name is applied before any cloud write is attempted,
// and a failed write is retried by the next refresh. A rename must not be able
// to fail a connect.
export const refreshSyncLocalName = async () => {
	const engine = getSyncEngine();
	if (!engine) {
		return;
	}
	try {
		await engine.setLocalName(await resolveSyncLocalName());
	} catch {}
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
let watermarkTrailingTimer: ReturnType<typeof setTimeout> | undefined;

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
	currentHostName = displayHolderName(authority?.holderName);

	void toUI("updateLocal", [
		{
			mpSyncActive: engine !== undefined,
			mpSyncReconnecting: isReconnecting(),
			mpSyncIsHost: engine?.isAuthority() ?? false,
			mpSyncHostName: currentHostName,
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
	void engine.probeHead();
	void engine.drainOutbox();
	pushSyncStateFull();
};

// The live transport + auto-play subscription for the current room, so the
// simmer can publish its schedule and every device can watch it.
let currentTransport: FirebaseTransport | undefined;
let autoPlayUnsub: (() => void) | undefined;

// When the room's auto-play is next due to fire, as published by whichever
// device is running it. Undefined when nobody is auto-playing.
let roomAutoPlayNextRunAt: number | undefined;

export const getRoomAutoPlayNextRunAt = () => roomAutoPlayNextRunAt;

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

// Set while a productive-page-cap re-drive is already queued, so we don't stack
// multiple immediate re-drives on top of the 15s poll timer.

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

	// A leftover repair flag from the old protocol. Nothing sets it any more and
	// nothing can act on it, so clear it rather than let it block sims forever.
	const lid = g.get("lid");
	if (await loadResyncNeeded(lid)) {
		await saveResyncNeeded(lid, false);
		syncDebugLog("connect:cleared-legacy-resync-flag", { lid });
	}

	// A device whose league fails the catastrophe check (stripped rosters, no
	// teams) must not sim: results computed from a broken league are broken
	// results, and once published they become everyone's.
	const integrityProblems = await checkLeagueIntegrity();
	if (integrityProblems.length > 0) {
		return {
			safe: false,
			reason: `This device's copy of the league looks damaged (${integrityProblems[0]}). Ask whoever is in charge of simming for a fresh export and re-import it - simming now would spread the damage.`,
		};
	}

	// No position-stamp comparison. That was the old protocol's second opinion,
	// needed because its watermark could lie about being caught up. A device on
	// the chain that is caught up has PROVEN it - applied version equals the
	// CAS-committed room version - and the guard above this already refused to
	// advance without that. Judging it a second time against a stamp that only
	// moves on advances, and whose day arithmetic disagrees with computed
	// positions in the playoffs, produced false refusals and the "heal" that
	// followed them is what rewound people's leagues.
	return { safe: true };
};

// Force a full catch-up: re-read the log's tail and re-apply it from scratch.
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

	// THERE IS NO SAFE WAY TO REBUILD A DIVERGED v1 DEVICE FROM HERE, so this
	// no longer pretends there is.
	//
	// It used to try two things. The room's snapshot - but nothing publishes
	// those any more, so any that survive are frozen at the date they were
	// written and restoring one is a time machine, not a repair. Then, failing
	// that, a windowed replay: re-reading ten thousand old changesets and
	// applying them over the live database. That is the one this file has
	// warned about in capitals for months - "re-applying old history over a
	// live database is not recovery, it is the wipe" - and it is what sent a
	// league-mate's file back to the start of the season after a toast in this
	// same build told him to press this button.
	//
	// A device whose data has genuinely diverged needs a fresh export from
	// someone who is correct. Saying so is not a worse answer than a replay; it
	// is the same answer without destroying the league on the way to it.
	throw new Error(
		"This device can't be repaired from the cloud. Ask whoever is in charge of simming for a fresh export of the league, then import it and rejoin the room.",
	);
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
	firebaseConfig,
}: {
	code: string;
	isHost?: boolean;
	explicit?: boolean;
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

	// THERE IS ONE PROTOCOL. Every room runs the version chain: a single integer
	// in the cloud, every change claims N+1 by compare-and-set, every device
	// remembers the last version it applied and asks for the ones after it. No
	// watermarks, no reassembly, no position stamps, no repair passes.
	//
	// The old protocol tried to understand the whole league - comparing where
	// each device thought it was, hunting for damage, restoring whole-file
	// snapshots over live data. Every league-wrecking incident came from that
	// machinery, so it is gone rather than merely discouraged.
	//
	// allowCache: detection tolerates a briefly-unreachable server (a stale
	// answer and no answer are equally harmless - a room's protocol never
	// changes).
	let v2State = await transport
		.fetchRoomV2State({ allowCache: true })
		.catch(() => undefined);
	if (v2State === undefined) {
		// No pointer yet. Either a brand-new room, which gets one, or a room from
		// before the chain existed, which cannot be converted in place: its
		// history is in a format nothing reads any more, and pretending otherwise
		// would silently start everyone from a blank slate.
		//
		// Unknown counts as "has history". A failed read must never be the reason
		// a v2 pointer gets minted into a room that already holds v1 entries.
		const legacyEntries = await transport
			.countEntriesSince(0)
			.catch(() => undefined);
		if (legacyEntries === undefined || legacyEntries > 0) {
			await teardownSharedLeague({ clearPersisted: false });
			void toUI("updateLocal", [{ mpSyncActive: false }]);
			throw new Error(
				"This room uses the old sync protocol, which has been removed. Create a new room, then have everyone import the same league export and join it.",
			);
		}
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
	if (v2State === undefined) {
		await teardownSharedLeague({ clearPersisted: false });
		void toUI("updateLocal", [{ mpSyncActive: false }]);
		throw new Error(
			"Couldn't set up this room. Check your connection and try again.",
		);
	}

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

	// Retention gap check. Catch-up is a `ts >` range read, so a device whose
	// watermark predates everything left in the log finds nothing missing and
	// would declare itself current while holding stale records - silently, which
	// is the worst way for this system to fail. Compare against the oldest entry
	// that actually survives and refuse to connect rather than diverge.
	//
	// Only reachable once the log is being trimmed AND this device has been away
	// longer than the retention window; a device inside the window sees its own
	// already-applied entries as the oldest and passes.
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

	const engine = new SyncEngineV2(transport, {
		isHost,
		code: trimmed,
		onAuthorityChange: (authority) => {
			currentHostName = displayHolderName(authority?.holderName);
			pushAuthorityToUI(authority?.holderId === clientId, currentHostName);
			pushEditsPaused();
		},
		onReadyChange: (ready) => {
			pushReadyToUI(ready);
		},
		onPendingChange: (count) => {
			pushPendingUploads(count);
		},
		onUploadingChange: (progress) => {
			void toUI("updateLocal", [{ mpSyncUpload: progress }]);
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
			void toUI("updateLocal", [{ mpCatchUp: progress }]);
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
		// Between ready and the claim: the authority doc is the first shared doc
		// that would otherwise be stamped with the placeholder name.
		await refreshSyncLocalName();
		if (isHost) {
			// Only if nobody is in charge of simming - see claimAuthorityIfVacant.
			// isHost is persisted, so claiming outright here meant every reconnect
			// of the room's creator stole simming from whoever was actually using
			// it, and then sat on it.
			await engine.claimAuthorityIfVacant();
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
	// Played-game invariant sweep (v1-log fallout only): drop any schedule row
	// whose game already
	// exists (a phantom "upcoming" copy of a played game, left by a partially
	// applied or abandoned changeset in some prior session). Runs once per
	// connect regardless of whether anything new syncs in, so a device carrying
	// this corruption heals just by opening the league.
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
				// A team switch also renames this device when no explicit name is
				// set, since the fallback is the team it manages.
				void refreshSyncLocalName();
			}
		} catch {
			// g may be mid-reload; try again next tick.
		}
	}, HEALTH_TICK_MS);

	// Watch the shared auto-play schedule so every device shows the same schedule
	// + countdown, and keep a transport handle so the simmer can publish its own.
	currentTransport = transport;
	autoPlayUnsub = transport.subscribeAutoPlay?.((autoPlay) => {
		// Kept here as well as pushed to the UI: the own-game sim gate runs in the
		// worker and needs to know how close the room's scheduled sim is.
		roomAutoPlayNextRunAt =
			typeof autoPlay?.nextRunAt === "number" ? autoPlay.nextRunAt : undefined;
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
	void engine.catchUp();

	// Poll to keep draining / pick up anything the real-time subscription hasn't
	// delivered yet (and to start that subscription once the initial drain lands).
	if (catchUpTimer !== undefined) {
		clearInterval(catchUpTimer);
	}
	// Backstop kick for the upload queue. Staleness itself needs no polling any
	// more: the pointer listener delivers, and the 5s head probe is a
	// server-fresh read that catches a listener which died without saying so.
	catchUpTimer = setInterval(() => {
		void getSyncEngine()?.drainOutbox();
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
	roomAutoPlayNextRunAt = undefined;
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
