import { toWorker } from "./toWorker.ts";
import { safeLocalStorage } from "./safeLocalStorage.ts";

// The intent to be connected to a shared-league room is persisted per-league in
// localStorage, so a full page refresh (which tears down the worker and its
// in-memory sync engine) can transparently reconnect - including the host role.

export type StoredSync = {
	code: string;
	isHost: boolean;
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
			return { code: parsed.code, isHost: !!parsed.isHost };
		}
	} catch {}
	return undefined;
};

export const setStoredSync = (lid: number, session: StoredSync) => {
	safeLocalStorage.setItem(
		key(lid),
		JSON.stringify({ code: session.code.trim(), isHost: session.isHost }),
	);
};

export const clearStoredSync = (lid: number) => {
	safeLocalStorage.removeItem(key(lid));
};

// Avoid firing a reconnect repeatedly for the same league within a session.
let reconnectingLid: number | undefined;

export const autoReconnectSync = async (lid: number) => {
	const session = getStoredSync(lid);
	if (!session || reconnectingLid === lid) {
		return;
	}
	reconnectingLid = lid;

	try {
		const status = await toWorker("main", "getSyncStatus", undefined);
		if (status.connected) {
			return;
		}
		await toWorker("main", "connectSharedLeague", {
			code: session.code,
			isHost: session.isHost,
		});
	} catch (error) {
		console.warn("Auto-reconnect to shared league failed", error);
		// Allow a later retry (e.g. user revisits the sync page).
		reconnectingLid = undefined;
	}
};
