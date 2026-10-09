// Whether a run of live-sim events belongs to the game whose roster the court
// is holding. Every player an event names has to be on one of the two teams;
// a single stranger means the events are some other game's.
export const eventsMatchRoster = (
	events: readonly unknown[],
	roster: readonly { pid: number }[],
): boolean => {
	if (roster.length === 0) {
		return false;
	}
	const pids = new Set(roster.map((p) => p.pid));
	for (const e of events) {
		if (e && typeof e === "object") {
			const pid = (e as { pid?: unknown }).pid;
			if (typeof pid === "number" && !pids.has(pid)) {
				return false;
			}
		}
	}
	return true;
};
