import { toUI } from "../../util/index.ts";

// Sync debug logging is expensive at scale - every event is a console.log plus
// a cross-thread message to mirror it into the page console - so it's OFF by
// default and gated at runtime. To enable, run this in the browser console and
// refresh:
//   localStorage.setItem("syncDebugLog", "1")
// (The UI reads that key on startup and flips this flag in the worker; setting
// it to anything else, or removing it, turns logging back off.)
let loggingEnabled = false;

export const setSyncDebugLogging = (enabled: boolean) => {
	loggingEnabled = enabled;
	if (enabled) {
		console.log("[sync-debug] logging enabled");
	}
};

const TRACE_LABEL_PREFIXES = ["playMenu.", "actions.", "main.reorder"];

const TRACE_LABELS = new Set([
	"playMenu.sim",
	"playMenu.day",
	"actions.liveGame",
	"actions.simGame",
	"main.reorderRosterDrag",
	"main.reorderDepthDrag",
]);

// Callers use this to skip building trace payloads entirely when logging is
// off, so disabled logging costs a single boolean check.
export const shouldTraceSyncLabel = (label: string): boolean =>
	loggingEnabled &&
	(TRACE_LABELS.has(label) ||
		TRACE_LABEL_PREFIXES.some((prefix) => label.startsWith(prefix)));

export const syncDebugLog = (
	event: string,
	data: Record<string, unknown> = {},
) => {
	if (!loggingEnabled) {
		return;
	}
	const payload = {
		at: new Date().toISOString(),
		event,
		...data,
	};
	console.log(`[sync-debug] ${event}`, payload);

	// SharedWorker console output is easy to miss in DevTools. Mirror these logs
	// into the page console so production debugging only needs the normal console.
	void toUI("syncDebugLog", [payload]).catch(() => {});
};
