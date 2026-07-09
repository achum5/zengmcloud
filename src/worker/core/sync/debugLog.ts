import { toUI } from "../../util/index.ts";

const TRACE_LABEL_PREFIXES = ["playMenu.", "actions.", "main.reorder"];

const TRACE_LABELS = new Set([
	"playMenu.sim",
	"playMenu.day",
	"actions.liveGame",
	"actions.simGame",
	"main.reorderRosterDrag",
	"main.reorderDepthDrag",
]);

export const shouldTraceSyncLabel = (label: string): boolean =>
	TRACE_LABELS.has(label) ||
	TRACE_LABEL_PREFIXES.some((prefix) => label.startsWith(prefix));

export const syncDebugLog = (
	event: string,
	data: Record<string, unknown> = {},
) => {
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
