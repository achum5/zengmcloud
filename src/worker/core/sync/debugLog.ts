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
	console.log(`[sync-debug] ${event}`, {
		at: new Date().toISOString(),
		...data,
	});
};
