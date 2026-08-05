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
