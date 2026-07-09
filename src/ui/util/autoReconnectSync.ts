import { toWorker } from "./toWorker.ts";
import { safeLocalStorage } from "./safeLocalStorage.ts";
import { ERROR_MESSAGE_SYNC_ROOM_MISMATCH } from "../../common/constants.ts";

// The intent to be connected to a shared-league room is persisted per-league in
// localStorage, so a full page refresh (which tears down the worker and its
// in-memory sync engine) can transparently reconnect - including the host role.

export type StoredSync = {
	code: string;
	isHost: boolean;
	// One-shot marker set when the user chose this room by name (e.g. at league
	// creation) but the connect happens later, after navigation. It makes that
	// first connect count as an EXPLICIT join (allowed to bind the league file to
	// the room); cleared once the connect succeeds.
	pendingJoin?: boolean;
};

const key = (lid: number) => `syncSession-${lid}`;

export const getStoredSync = (lid: number): StoredSync | undefined => {
	const raw = safeLocalStorage.getItem(key(lid));
	if (!raw) {
		return undefined;
	}
	try {
		const parsed = JSON.parse(raw);
		if (typeof parsed?.code === "string" && parsed.code.trim() !== "") {
			return {
				code: parsed.code,
				isHost: !!parsed.isHost,
				pendingJoin: !!parsed.pendingJoin,
			};
		}
	} catch {}
	return undefined;
};

export const setStoredSync = (lid: number, session: StoredSync) => {
	safeLocalStorage.setItem(
		key(lid),
		JSON.stringify({
			code: session.code.trim(),
			isHost: session.isHost,
			...(session.pendingJoin ? { pendingJoin: true } : {}),
		}),
	);
};

export const clearStoredSync = (lid: number) => {
	safeLocalStorage.removeItem(key(lid));
};

// Avoid firing a reconnect loop repeatedly for the same league within a session.
let reconnectingLid: number | undefined;

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const autoReconnectSync = async (lid: number) => {
	const session = getStoredSync(lid);
	if (!session || reconnectingLid === lid) {
		return;
	}
	reconnectingLid = lid;

	// Gate simming immediately - before we've (re)connected - so a refresh can't
	// leave a window where this device sims offline and silently diverges from
	// the league. The worker keeps sims paused until the connection is live.
	try {
		await toWorker("main", "markSyncRequired", session);
	} catch {}

	// Reconnect with a few backoff retries so a transient failure recovers on its
	// own (otherwise the sim gate would strand the user offline).
	for (let attempt = 0; attempt < 5; attempt++) {
		try {
			const status = await toWorker("main", "getSyncStatus", undefined);
			if (status.connected) {
				// The worker's engine is already connected (e.g. it outlived this UI
				// reload), so no connect runs to push sync state to the fresh UI. Ask
				// the worker to re-assert it, or the UI would sit showing "nobody
				// simming" with an unlocked Play menu while really following.
				await toWorker("main", "refreshSyncUIState", undefined);
				return;
			}
			await toWorker("main", "connectSharedLeague", {
				code: session.code,
				isHost: session.isHost,
				// A join the user chose at league creation runs its actual connect
				// here, post-navigation - it still counts as explicit, once.
				explicit: !!session.pendingJoin,
			});
			if (session.pendingJoin) {
				setStoredSync(lid, { code: session.code, isHost: session.isHost });
			}
			return;
		} catch (error) {
			// The room belongs to a different league file: this stored session is
			// stale/wrong (e.g. a recycled lid). Drop it and stop retrying - the
			// worker has already released the sim gate.
			if (
				error instanceof Error &&
				error.message.includes(ERROR_MESSAGE_SYNC_ROOM_MISMATCH)
			) {
				clearStoredSync(lid);
				reconnectingLid = undefined;
				return;
			}
			console.warn(`Auto-reconnect attempt ${attempt + 1} failed`, error);
			await delay(1000 * (attempt + 1));
		}
	}

	// Still offline after retries: try again the moment the browser is back
	// online, and allow future manual retries (e.g. revisiting the sync page).
	reconnectingLid = undefined;
	const onOnline = () => {
		window.removeEventListener("online", onOnline);
		void autoReconnectSync(lid);
	};
	window.addEventListener("online", onOnline);
};
