import { toUI } from "../../util/index.ts";

// Sync debug logging is OFF by default: every event is a console.log PLUS a
// cross-thread message to mirror it into the page console, and during a big
// catch-up that firehose (hundreds/sec) noticeably lags the game. The logging
// stays in the code so it can be switched back on the instant a sync issue
// needs diagnosing - opt in from the browser console and refresh:
//   localStorage.setItem("syncDebugLog", "1")
// (The UI reads that key on startup and flips this flag on in the worker;
// remove the key or set it to "0" to turn logging back off.)
let loggingEnabled = false;

export const setSyncDebugLogging = (enabled: boolean) => {
	loggingEnabled = enabled;
	console.log(`[sync-debug] logging ${enabled ? "enabled" : "disabled"}`);
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
	// V2 protocol events are ALWAYS logged and mirrored, opt-in flag or not.
	// The flag exists because v1 catch-up produces hundreds of lines a second;
	// v2 produces one line per version - a day of heavy play fits in the ring
	// buffer - and always-on means a tester's copy-paste capture just works,
	// with zero setup, on the exact session where something misbehaved.
	if (!loggingEnabled && !event.startsWith("v2:")) {
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
