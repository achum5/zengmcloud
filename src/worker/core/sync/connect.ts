import { SyncEngine } from "./SyncEngine.ts";
import { FirebaseTransport } from "./FirebaseTransport.ts";
import { outbox } from "./outbox.ts";
import { ensureAnonymousAuth } from "./auth.ts";
import { getSyncEngine, setSyncEngine } from "./engineHolder.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { idb } from "../../db/index.ts";
import { g, local, lock, toUI } from "../../util/index.ts";

// This device's catch-up watermark for a league, stored in the durable meta DB
// so it survives refreshes - so we only replay what we missed.
const loadWatermark = async (lid: number | undefined): Promise<number> => {
	if (typeof lid !== "number") {
		return 0;
	}
	const league = await idb.meta.get("leagues", lid);
	return league?.syncWatermark ?? 0;
};

const saveWatermark = async (lid: number | undefined, ts: number) => {
	if (typeof lid !== "number") {
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
// Name of whoever currently holds the wheel (for display), from the shared doc.
let currentHostName: string | undefined;

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
const CATCH_UP_INTERVAL_MS = 15000;

// How many recent log entries the activity panel reads. Bounded so it renders a
// list instead of pulling a whole season's worth of change docs.
const SYNC_ACTIVITY_LIMIT = 200;

// Drop outbox entries older than this (a room the user never returned to), so a
// permanently-failed upload can't make the outbox grow without bound.
const OUTBOX_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;

// The header status dot goes red if we haven't confirmed live cloud contact in
// this long. Must exceed the catch-up interval so a HEALTHY connection (which
// catch-up refreshes every 15s) stays green; a dead one goes red after this.
const HEALTH_STALE_MS = 30000;
const HEALTH_TICK_MS = 5000;
let healthTimer: ReturnType<typeof setInterval> | undefined;
let lastHealthPushed: boolean | undefined;

// Monotonic count of confirmed uploads; the UI flashes a "synced ✓" when it ticks.
let uploadOkCounter = 0;

const pushHealth = () => {
	const age = getSyncEngine()?.contactAge();
	const healthy = age !== undefined && age < HEALTH_STALE_MS;
	if (healthy !== lastHealthPushed) {
		lastHealthPushed = healthy;
		void toUI("updateLocal", [{ mpSyncHealthy: healthy }]);
	}
};

// Whether conflict-prone edits are blocked right now (the wheel-holder is
// mid-sim, or this device hasn't caught up). Pushed to the UI so the header can
// show a "simming…" indicator - so a blocked trade/roster move reads as expected
// rather than a glitch. Only meaningful on a follower; the wheel-holder is never
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

// The live transport + auto-play subscription for the current room, so the
// simmer can publish its schedule and every device can watch it.
let currentTransport: FirebaseTransport | undefined;
let autoPlayUnsub: (() => void) | undefined;

// Drain the backlog page by page (resumable, bounded memory), then - once truly
// caught up to the head - move the live subscription's watermark to the head and
// start it. Deferring the real-time listener this way keeps its initial snapshot
// to just the live tail, instead of re-loading the entire backlog we just
// drained (which on a long absence would time out and wedge). Idempotent: the
// subscription starts at most once; later calls just keep the tail drained.
const driveCatchUp = async () => {
	const engine = getSyncEngine();
	if (!engine) {
		return;
	}
	const reachedHead = await engine.catchUp();
	// Only go live once the drain has actually reached the head - otherwise the
	// subscription's initial snapshot would re-load the still-undrained backlog.
	// (A failed fetch returns false, so we don't prematurely go live either.)
	if (reachedHead && !engine.hasChangesSubscription()) {
		currentTransport?.updateSince(engine.getPersistedSeq());
		engine.startChangesSubscription();
	}
	// Catching up may have just unblocked edits.
	pushEditsPaused();
};

// Called from the UI (on the simmer's device) whenever its auto-play schedule
// changes, to broadcast it to the room. No-op if not connected.
export const publishAutoPlayState = async (
	state: import("../../../common/types.ts").SyncedAutoPlay,
) => {
	try {
		await currentTransport?.publishAutoPlay?.(state);
	} catch (error) {
		// The schedule ride-along on the authority doc requires holding the wheel;
		// a stale writer (just lost the wheel) is denied - harmless, since claiming
		// the wheel already cleared the old schedule.
		console.error("publishAutoPlayState failed", error);
	}
};

export const getSyncRequired = () => syncRequired;

// True while we intend to be synced but aren't connected yet (reconnecting or
// offline). The wheel guard uses this to pause simming; the UI shows it.
export const isReconnecting = () =>
	syncRequired && getSyncEngine() === undefined;

// Called by the UI's auto-reconnect the instant it knows this league should be
// synced - before the async connect finishes - so simming is gated during the
// whole reconnect window, not just once it completes.
export const markSyncRequired = () => {
	syncRequired = true;
	if (getSyncEngine() === undefined) {
		void toUI("updateLocal", [{ mpSyncReconnecting: true }]);
	}
};

export const getSyncStatus = () => {
	const engine = getSyncEngine();
	return {
		connected: engine !== undefined,
		reconnecting: isReconnecting(),
		code: currentCode,
		// "host" now means "current wheel-holder", read live from the engine.
		isHost: engine?.isAuthority() ?? false,
		hostName: currentHostName,
	};
};

// Push the current wheel state into reactive UI local state so the Play menu,
// draft, and sync page can reflect who's in control without polling.
const pushAuthorityToUI = (isHost: boolean, hostName: string | undefined) => {
	void toUI("updateLocal", [
		{ mpSyncIsHost: isHost, mpSyncHostName: hostName },
	]);
};

// Take the wheel on this device (become the one allowed to advance the league).
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
	// gameAttributes keys carried by an entry (phase, daysLeft, etc.).
	const attrsOf = (entry: (typeof entries)[number]): string[] =>
		entry.changeset.changes
			.filter((c) => c.store === "gameAttributes")
			.map((c) => String(c.id));

	const byKey = new Map<string, SyncActivityItem>();
	for (const entry of entries) {
		const key = entry.batchId ?? entry.id;
		const records = entry.changeset.changes.length;
		const mine = entry.authorId === clientId;
		const existing = byKey.get(key);
		if (existing) {
			existing.records += records;
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
export const connectSharedLeague = async ({
	code,
	isHost = false,
}: {
	code: string;
	isHost?: boolean;
}) => {
	const trimmed = code.trim();
	if (!trimmed) {
		throw new Error("A league code is required.");
	}

	// Tear down any existing session first.
	disconnectSharedLeague();

	// From here on this device is committed to the session, so simming stays
	// gated through the whole async connect (and if it throws) - never sim
	// offline and diverge.
	syncRequired = true;

	// Authenticate - the uid is our stable, rule-enforceable sync identity.
	const clientId = await ensureAnonymousAuth();

	const lid = g.get("lid");
	connectedLid = typeof lid === "number" ? lid : undefined;
	const watermark = await loadWatermark(lid);

	const transport = new FirebaseTransport(trimmed, clientId, {
		sinceTs: watermark,
	});

	const engine = new SyncEngine(transport, {
		isHost,
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
			void toUI("updateLocal", [{ mpCatchUp: progress }]);
		},
	});
	engine.start();
	setSyncEngine(engine);
	currentCode = trimmed;
	currentHostName = undefined;

	// Register the room so it shows up on the admin page. Best-effort.
	void transport.touchRoom?.();

	// Finish any upload a previous session left unconfirmed (interrupted mid-send),
	// and drop long-dead outbox entries. Best-effort - never blocks the connect.
	void engine.flushOutbox();
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
	healthTimer = setInterval(() => {
		pushHealth();
		pushEditsPaused();
	}, HEALTH_TICK_MS);

	// Watch the shared auto-play schedule so every device shows the same schedule
	// + countdown, and keep a transport handle so the simmer can publish its own.
	currentTransport = transport;
	autoPlayUnsub = transport.subscribeAutoPlay?.((autoPlay) => {
		void toUI("updateLocal", [{ mpAutoPlay: autoPlay }]);
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
		void driveCatchUp();
		// Also drain any upload the retry couldn't land this session (rare); the
		// call is a no-op when the outbox is empty.
		void getSyncEngine()?.flushOutbox();
	}, CATCH_UP_INTERVAL_MS);

	// Turn on change capture so local actions get published to the room.
	changeTracker.enable();
	changeTracker.reset();

	// Let the UI hide single-player-only chrome (e.g. the multi-team switcher),
	// clear the "reconnecting" state, and reset the wheel display until the
	// control-doc subscription reports in.
	void toUI("updateLocal", [{ mpSyncActive: true, mpSyncReconnecting: false }]);
	pushAuthorityToUI(false, undefined);

	return { connected: true, code: trimmed, isHost, clientId };
};

export const disconnectSharedLeague = () => {
	if (catchUpTimer !== undefined) {
		clearInterval(catchUpTimer);
		catchUpTimer = undefined;
	}
	if (healthTimer !== undefined) {
		clearInterval(healthTimer);
		healthTimer = undefined;
	}
	lastHealthPushed = undefined;
	lastEditsPausedPushed = undefined;
	autoPlayUnsub?.();
	autoPlayUnsub = undefined;
	currentTransport = undefined;
	void toUI("updateLocal", [
		{
			mpAutoPlay: undefined,
			mpSyncUpload: undefined,
			mpSyncHealthy: false,
			mpEditsPaused: false,
			mpCatchUp: undefined,
		},
	]);
	const engine = getSyncEngine();
	if (engine) {
		engine.stop();
		setSyncEngine(undefined);
	}
	currentCode = undefined;
	connectedLid = undefined;
	currentHostName = undefined;
	// Explicit disconnect clears the intent, so single-player simming works again.
	syncRequired = false;

	void toUI("updateLocal", [
		{ mpSyncActive: false, mpSyncReconnecting: false },
	]);
	pushAuthorityToUI(false, undefined);

	// Leave the tracker enabled in dev (the console logger uses it); otherwise
	// turn it back off so single-player has zero overhead.
	if (process.env.NODE_ENV !== "development") {
		changeTracker.disable();
	}

	return { connected: false };
};
