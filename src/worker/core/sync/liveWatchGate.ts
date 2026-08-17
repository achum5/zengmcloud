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

// Is a live sim running ANYWHERE in the room - whether or not this device is
// watching it? A separate question from the one above, and the one the sim
// fence has to ask: since viewers can walk out of any broadcast, "I am not
// watching" stopped being the same thing as "nothing is being simmed", and a
// device that left must not be allowed to start a second sim on top of the
// one it just walked away from.

let roomGate: (() => boolean) | undefined;

export const setRoomBroadcastGate = (fn: (() => boolean) | undefined) => {
	roomGate = fn;
};

export const isLiveBroadcastActiveInRoom = (): boolean => roomGate?.() ?? false;
