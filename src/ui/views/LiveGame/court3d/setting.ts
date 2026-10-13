import { safeLocalStorage } from "../../../util/safeLocalStorage.ts";

// Which live-game picture this device shows for basketball: the classic 2D
// court or the 3D one. Saved in this browser only, like the color scheme.
// (In a multiplayer broadcast, everyone watching follows the device in charge
// of simming instead - see LiveGame.)

export type LiveGameView = "classic" | "3d";

const KEY = "bbgmLiveGameView";

// "2.5d" and "retro" are what the 3D court used to be called - still what
// it reads back from a browser that chose it then, or from a device on an
// older version that's in charge of simming.
export const parseLiveGameView = (value: unknown): LiveGameView =>
	value === "3d" || value === "2.5d" || value === "retro" ? "3d" : "classic";

export const getLiveGameView = (): LiveGameView =>
	parseLiveGameView(safeLocalStorage.getItem(KEY));

export const setLiveGameView = (view: LiveGameView) => {
	if (view === "3d") {
		safeLocalStorage.setItem(KEY, view);
	} else {
		safeLocalStorage.removeItem(KEY);
	}
};

// How fast the 3D court plays: 1x is real time - a second of the game in
// a second (bar the dead time it runs through fast) - and quicker from there.
export const COURT3D_SPEEDS = [1, 2, 4, 8] as const;
export type Court3DSpeed = (typeof COURT3D_SPEEDS)[number];

export const parseCourt3DSpeed = (value: unknown): Court3DSpeed =>
	COURT3D_SPEEDS.find((s) => String(s) === String(value)) ?? 1;
