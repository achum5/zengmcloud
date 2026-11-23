// Is this device currently WATCHING a live playback (following a league-mate's
// live-sim broadcast)? Registered by connect.ts, read by the apply layer so
// remote changes landing mid-playback don't repaint spoilers (final scores,
// a phase flip that gives away the finals). Its own tiny module, like
// engineHolder, to avoid a changeset.ts <-> connect.ts import cycle.

let gate: (() => boolean) | undefined;

export const setLiveWatchGate = (fn: (() => boolean) | undefined) => {
	gate = fn;
};

export const isWatchingLiveBroadcast = (): boolean => gate?.() ?? false;

// A league-mate's live-simmed result is landing on this device (see
// noticeLiveResults): connect.ts decides whether to hold the paint for it the
// way a watcher would. Wired the same way, for the same import-cycle reason.
let liveResultNotice: ((gid: number, liveAt: number) => void) | undefined;

export const setLiveResultNotice = (
	fn: ((gid: number, liveAt: number) => void) | undefined,
) => {
	liveResultNotice = fn;
};

export const noticeLiveResult = (gid: number, liveAt: number) => {
	liveResultNotice?.(gid, liveAt);
};

// A phase change to hold back on screen: the playoffs ended (or moved on) at
// the tip-off of a playoff game a league mate is still playing live, watched
// here or not. The data is in; the header, the Play menu and the phase's page
// wait for that game's final (see util/liveGameGate.ts). Wired by connect.ts.
let phasePaintHold: (() => boolean) | undefined;

export const setPhasePaintHold = (fn: (() => boolean) | undefined) => {
	phasePaintHold = fn;
};

export const holdsPhasePaint = (): boolean => {
	try {
		return phasePaintHold?.() ?? false;
	} catch {
		return false;
	}
};
