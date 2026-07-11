import { createNanoEvents } from "nanoevents";
import { toWorker } from "./toWorker.ts";

// A small in-app buffer of the sync debug logs, so they can be viewed on a
// device with no reachable console (a phone). The worker only mirrors logs here
// when sync debug logging is enabled; this collects them into a ring buffer and
// notifies an on-screen overlay (see SyncDebugOverlay.tsx).

export type SyncDebugEntry = {
	seq: number;
	at: string;
	event: string;
	payload: Record<string, unknown>;
};

const MAX_ENTRIES = 500;
const STORAGE_KEY = "syncDebugLog";

let entries: SyncDebugEntry[] = [];
let seq = 0;

const emitter = createNanoEvents<{
	change: (entries: SyncDebugEntry[]) => void;
}>();

export const syncDebugEnabled = (): boolean => {
	try {
		return localStorage.getItem(STORAGE_KEY) === "1";
	} catch {
		return false;
	}
};

// Turn logging on/off from the UI: persist the flag (read on startup), flip the
// worker flag now, and notify the overlay so it appears/disappears immediately.
export const setSyncDebugEnabled = (enabled: boolean) => {
	try {
		if (enabled) {
			localStorage.setItem(STORAGE_KEY, "1");
		} else {
			localStorage.removeItem(STORAGE_KEY);
		}
	} catch {}
	void toWorker("main", "setSyncDebugLogging", enabled);
	emitter.emit("change", entries);
};

export const pushSyncDebugEntry = (payload: Record<string, unknown>) => {
	const event = typeof payload.event === "string" ? payload.event : "event";
	const at =
		typeof payload.at === "string" ? payload.at : new Date().toISOString();
	seq += 1;
	entries = [...entries.slice(-(MAX_ENTRIES - 1)), { seq, at, event, payload }];
	emitter.emit("change", entries);
};

export const getSyncDebugEntries = (): SyncDebugEntry[] => entries;

export const clearSyncDebugEntries = () => {
	entries = [];
	emitter.emit("change", entries);
};

export const subscribeSyncDebug = (
	cb: (entries: SyncDebugEntry[]) => void,
): (() => void) => emitter.on("change", cb);
