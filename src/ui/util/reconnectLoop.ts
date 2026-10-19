import { ERROR_MESSAGE_SYNC_ROOM_MISMATCH } from "../../common/constants.ts";
import type { StoredSync } from "./autoReconnectSync.ts";

// The auto-reconnect's loop, with everything it touches passed in (see
// autoReconnectSync.ts for the real ones, and the test for why each rule is
// there).

// How long to wait before going round again, by how many tries in a row have
// failed. There is no last try: a league in a room stays in it until the user
// disconnects, and "the auto-reconnect gave up" used to mean "until the page
// is reloaded" - the league sat there looking like it had never been in a
// room, sims paused, and a refresh was the only way back.
export const RECONNECT_RETRY_MS = [1_000, 2_000, 4_000, 8_000, 15_000, 30_000];

export type ReconnectDeps = {
	markSyncRequired: (session: StoredSync) => Promise<unknown>;
	getSyncStatus: () => Promise<{ connected: boolean } | undefined>;
	refreshSyncUIState: () => Promise<unknown>;
	connect: (session: StoredSync) => Promise<{ connected: boolean } | undefined>;
	currentLid: () => number | undefined;
	stored: (lid: number) => StoredSync | undefined;
	setStored: (lid: number, session: StoredSync) => void;
	clearStored: (lid: number) => void;
	wait: (ms: number) => Promise<void>;
};

// Until this league is connected, the user leaves it, or they disconnect it.
export const runReconnect = async (
	lid: number,
	deps: ReconnectDeps,
): Promise<"connected" | "left" | "disconnected" | "mismatch"> => {
	const first = deps.stored(lid);
	if (!first) {
		return "disconnected";
	}

	// Gate simming immediately - before we've (re)connected - so a refresh can't
	// leave a window where this device sims offline and silently diverges from
	// the league. The worker keeps sims paused until the connection is live.
	try {
		await deps.markSyncRequired(first);
	} catch {}

	for (let attempt = 0; ; attempt++) {
		if (deps.currentLid() !== lid) {
			return "left";
		}
		// Read fresh each time round: disconnecting clears it.
		const session = deps.stored(lid);
		if (!session) {
			return "disconnected";
		}
		try {
			const status = await deps.getSyncStatus();
			if (status?.connected) {
				// The worker's engine is already connected (e.g. it outlived this UI
				// reload), so no connect runs to push sync state to the fresh UI. Ask
				// the worker to re-assert it, or the UI would sit showing "nobody
				// simming" with an unlocked Play menu while really following.
				await deps.refreshSyncUIState();
				return "connected";
			}
			const result = await deps.connect(session);
			if (result?.connected) {
				if (session.pendingJoin) {
					deps.setStored(lid, {
						code: session.code,
						isHost: session.isHost,
						firebaseConfig: session.firebaseConfig,
					});
				}
				return "connected";
			}
			// Not connected and nothing thrown - the league was still loading, say.
			// That is no answer; go round again.
		} catch (error) {
			// The room belongs to a different league file: this stored session is
			// stale/wrong (e.g. a recycled lid). Drop it and stop - the worker has
			// already released the sim gate. (Only while this is still the league
			// on screen: a connect that finishes after a switch to another league
			// says nothing about this one.)
			if (
				error instanceof Error &&
				error.message.includes(ERROR_MESSAGE_SYNC_ROOM_MISMATCH)
			) {
				if (deps.currentLid() !== lid) {
					return "left";
				}
				deps.clearStored(lid);
				return "mismatch";
			}
			console.warn(`Auto-reconnect attempt ${attempt + 1} failed`, error);
		}
		await deps.wait(
			RECONNECT_RETRY_MS[Math.min(attempt, RECONNECT_RETRY_MS.length - 1)]!,
		);
	}
};
