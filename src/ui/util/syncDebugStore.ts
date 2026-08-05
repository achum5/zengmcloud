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

// Rare one-shot events that are usually the whole point of a capture (they
// fire once at import/connect time, then a busy catch-up floods the buffer and
// evicts them before anyone can copy). Never evicted.
const PINNED_EVENTS = new Set([
	"export:checkpoint",
	"export:full-check",
	"import:checkpoint",
	"connect:initial-watermark",
	"connect:duplicate-skipped",
	"engine:batch-abandoned",
	"engine:batch-permanently-incomplete",
	// V2: the rare events that ARE the diagnosis when something goes wrong.
	"v2:cas-lost",
	"v2:delta-missing",
	"v2:catchup-failed",
	"v2:checkpoint-blocked-history",
	"v2:checkpoint-blocked-integrity",
	"v2:checkpoint-failed",
	"v2:stale-advance-discarded",
	"v2:publish-retries-exhausted",
	"v2:recovered-from-checkpoint",
	"v2:recovery-no-checkpoint",
	"v2:drain-failed",
	"v2:apply-declined",
]);

export const pushSyncDebugEntry = (payload: Record<string, unknown>) => {
	const event = typeof payload.event === "string" ? payload.event : "event";
	const at =
		typeof payload.at === "string" ? payload.at : new Date().toISOString();
	seq += 1;
	const next = [...entries, { seq, at, event, payload }];
	// Evict oldest NON-pinned lines first, so the one-shot connect/import lines
	// survive an arbitrarily long catch-up flood.
	while (next.length > MAX_ENTRIES) {
		const i = next.findIndex((e) => !PINNED_EVENTS.has(e.event));
		if (i === -1) {
			next.shift();
		} else {
			next.splice(i, 1);
		}
	}
	entries = next;
	emitter.emit("change", entries);
};

export const getSyncDebugEntries = (): SyncDebugEntry[] => entries;

export const clearSyncDebugEntries = () => {
	entries = [];
	emitter.emit("change", entries);
};

const asText = (list: SyncDebugEntry[]): string =>
	list.map((e) => `${e.at} ${e.event} ${JSON.stringify(e.payload)}`).join("\n");

// The full copy-paste capture: worker state snapshot + buffered log lines.
// Shared by the debug overlay and the sync page's Copy button, so a tester
// gets the identical, self-describing paste from either place - including
// with the debug flag OFF, since v2 events mirror unconditionally.
export const buildSyncLogCapture = async (
	list: SyncDebugEntry[] = entries,
): Promise<string> => {
	let header = `=== SYNC LOG CAPTURE (ui v${(window as any).bbgmVersion}) ===\n`;
	const unavailable =
		"(worker snapshot unavailable - the app may still be running an older version; fully close the app and reopen it twice)\n";
	try {
		const snap = await toWorker("main", "getSyncDebugSnapshot", undefined);
		header += typeof snap === "string" ? `${snap}\n` : unavailable;
	} catch {
		header += unavailable;
	}
	return `${header}${asText(list)}`;
};

export const subscribeSyncDebug = (
	cb: (entries: SyncDebugEntry[]) => void,
): (() => void) => emitter.on("change", cb);
