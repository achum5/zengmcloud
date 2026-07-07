import { SyncEngine } from "./SyncEngine.ts";
import { FirebaseTransport } from "./FirebaseTransport.ts";
import { ensureAnonymousAuth } from "./auth.ts";
import { getSyncEngine, setSyncEngine } from "./engineHolder.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { idb } from "../../db/index.ts";
import { g, toUI } from "../../util/index.ts";

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

export const getSyncStatus = () => {
	const engine = getSyncEngine();
	return {
		connected: engine !== undefined,
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

	// Authenticate - the uid is our stable, rule-enforceable sync identity.
	const clientId = await ensureAnonymousAuth();

	const lid = g.get("lid");
	const watermark = await loadWatermark(lid);

	const transport = new FirebaseTransport(trimmed, clientId, {
		sinceTs: watermark,
	});
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

	// Turn on change capture so local actions get published to the room.
	changeTracker.enable();
	changeTracker.reset();

	// Let the UI hide single-player-only chrome (e.g. the multi-team switcher),
	// and reset the wheel display until the control-doc subscription reports in.
	void toUI("updateLocal", [{ mpSyncActive: true }]);
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

	void toUI("updateLocal", [{ mpSyncActive: false }]);
	pushAuthorityToUI(false, undefined);

	// Leave the tracker enabled in dev (the console logger uses it); otherwise
	// turn it back off so single-player has zero overhead.
	if (process.env.NODE_ENV !== "development") {
		changeTracker.disable();
	}

	return { connected: false };
};
