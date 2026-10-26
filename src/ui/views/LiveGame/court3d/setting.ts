// Which live-game picture a device shows for basketball (or football): the
// 3D one, always - the classic 2D court (or field) only when the 3D one can't
// play a game (see LiveGame's onFail).
// (In a multiplayer broadcast, everyone watching follows the device in charge
// of simming instead - see LiveGame.)

export type LiveGameView = "classic" | "3d";

// "2.5d" and "retro" are what the 3D court used to be called - still what
// a device on an older version that's in charge of simming may send.
export const parseLiveGameView = (value: unknown): LiveGameView =>
	value === "3d" || value === "2.5d" || value === "retro" ? "3d" : "classic";

export const getLiveGameView = (): LiveGameView => "3d";

// How fast the 3D court plays: 1x is real time - a second of the game in
// a second (bar the dead time it runs through fast) - and quicker from there.
export const COURT3D_SPEEDS = [1, 2, 4, 8] as const;
export type Court3DSpeed = (typeof COURT3D_SPEEDS)[number];

export const parseCourt3DSpeed = (value: unknown): Court3DSpeed =>
	COURT3D_SPEEDS.find((s) => String(s) === String(value)) ?? 1;
