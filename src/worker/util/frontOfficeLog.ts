// A record of what the AI front offices decided and why.
//
// The behavior this traces - a team sitting on cap space, dumping salary to
// clear more, then landing a free agent - is invisible in the final box score:
// all you see months later is that a team has a new star. When it goes wrong it
// is equally invisible, so the only way to trust it is to be able to read back
// every decision from a long sim.
//
// Off by default and free when off (one boolean check). Turn it on from the
// browser console and refresh:
//   localStorage.setItem("frontOfficeLog", "1")
// Sim harnesses call captureFrontOfficeLog() instead, which buffers entries in
// memory rather than printing them.

export type FrontOfficeEntry = {
	season: number;
	tid: number;
	event: string;
	data: Record<string, unknown>;
};

let consoleEnabled = false;
let buffer: FrontOfficeEntry[] | undefined;

export const setFrontOfficeLogging = (enabled: boolean) => {
	consoleEnabled = enabled;
};

// Start buffering entries in memory and return a handle to read/stop. Used by
// the offseason harness to audit thousands of decisions without printing them.
export const captureFrontOfficeLog = () => {
	buffer = [];
	return {
		entries: () => buffer ?? [],
		stop: () => {
			const out = buffer ?? [];
			buffer = undefined;
			return out;
		},
	};
};

export const frontOfficeLoggingActive = () =>
	consoleEnabled || buffer !== undefined;

export const frontOfficeLog = (
	season: number,
	tid: number,
	event: string,
	data: Record<string, unknown> = {},
) => {
	if (!consoleEnabled && buffer === undefined) {
		return;
	}
	const entry: FrontOfficeEntry = { season, tid, event, data };
	buffer?.push(entry);
	if (consoleEnabled) {
		console.log(`[front-office] ${season} tid=${tid} ${event}`, data);
	}
};
