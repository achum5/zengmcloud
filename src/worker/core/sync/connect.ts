import { SyncEngine } from "./SyncEngine.ts";
import { FirebaseTransport } from "./FirebaseTransport.ts";
import { ensureAnonymousAuth } from "./auth.ts";
import { getSyncEngine, setSyncEngine } from "./engineHolder.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { idb } from "../../db/index.ts";
import { g, toUI } from "../../util/index.ts";

// Thrown internally when a room's league fingerprint doesn't match this file.
class SyncMismatchError extends Error {}

// This league file's stable fingerprint. Stored as a gameAttribute so it's
// carried IN the file - every device that loads the same exported file shares
// it. Generated once (lazily) if the file doesn't have one yet. This is what
// lets a room refuse a different league than the one it was recorded against.
const ensureLeagueId = async (): Promise<string> => {
	const existing = await idb.cache.gameAttributes.get("syncLeagueId");
	if (existing && typeof existing.value === "string") {
		return existing.value;
	}
	const id =
		typeof crypto !== "undefined" && crypto.randomUUID
			? crypto.randomUUID()
			: `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
	await idb.cache.gameAttributes.put({ key: "syncLeagueId", value: id });
	// @ts-expect-error - syncLeagueId isn't in the GameAttributesLeague type.
	g.setWithoutSavingToDB("syncLeagueId", id);
	return id;
};

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
	const league = await idb.meta.get("leagues", lid);
	if (league && (league.syncWatermark ?? 0) < ts) {
		league.syncWatermark = ts;
		await idb.meta.put("leagues", league);
	}
};

// The room we're currently connected to (if any), so the UI can reflect
// connection state - including after an auto-reconnect it didn't drive.
let currentCode: string | undefined;
// Name of whoever currently holds the wheel (for display), from the shared doc.
let currentHostName: string | undefined;

// Whether this device is *supposed* to be in a sync session. Stays true across
// the async reconnect after a refresh, and even if that reconnect fails - so we
// can gate simming until the connection is actually live, instead of letting
// the device sim while offline and silently diverge from the league.
let syncRequired = false;

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

	const entries = await engine.fetchLog();
	const watermark = engine.getPersistedSeq();
	const clientId = engine.clientId;

	// Collapse chunked bulk batches (which share a batchId) into a single row.
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
		} else {
			byKey.set(key, {
				key,
				action: entry.action,
				ts: entry.seq,
				records,
				mine,
				caughtUp: mine || entry.seq <= watermark,
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
	const watermark = await loadWatermark(lid);

	const transport = new FirebaseTransport(trimmed, clientId, {
		sinceTs: watermark,
	});

	// League-identity guard. A room's change log is a stream of deltas that only
	// makes sense on top of the SAME league file it was recorded against. Replaying
	// it onto a DIFFERENT league corrupts the database (duplicate teamSeasons,
	// etc.). So each room is stamped with a fingerprint of its league, and we
	// refuse to connect a league whose fingerprint doesn't match.
	const localLeagueId = await ensureLeagueId();
	try {
		const roomInfo = await transport.getRoomInfo?.();
		if (roomInfo?.leagueId) {
			if (roomInfo.leagueId !== localLeagueId) {
				syncRequired = false;
				void toUI("updateLocal", [{ mpSyncActive: false }]);
				throw new SyncMismatchError();
			}
		}
		// Fresh room (no fingerprint yet): stamp it with ours below.
	} catch (error) {
		if (error instanceof SyncMismatchError) {
			throw new Error(
				"This code belongs to a different league. Everyone must load the same league file. If you just updated the shared file, re-import the latest copy — otherwise use a new code.",
			);
		}
		// A read failure (e.g. rules not published yet, offline) shouldn't block
		// connecting; the guard simply can't run until the room is reachable.
	}

	const engine = new SyncEngine(transport, {
		isHost,
		initialWatermark: watermark,
		onWatermark: (seq) => {
			void saveWatermark(lid, seq);
		},
		onAuthorityChange: (authority) => {
			currentHostName = authority?.holderName;
			pushAuthorityToUI(
				authority?.holderId === clientId,
				authority?.holderName,
			);
		},
	});
	engine.start();
	setSyncEngine(engine);
	currentCode = trimmed;
	currentHostName = undefined;

	// Register the room (listable on the admin page) and stamp our league
	// fingerprint so a mismatched league can be refused later. Best-effort.
	void transport.touchRoom?.(localLeagueId);

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
	const engine = getSyncEngine();
	if (engine) {
		engine.stop();
		setSyncEngine(undefined);
	}
	currentCode = undefined;
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
