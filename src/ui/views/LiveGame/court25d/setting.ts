import { safeLocalStorage } from "../../../util/safeLocalStorage.ts";

// Which live-game picture this device shows for basketball: the classic 2D
// court or the 2.5D one. Saved in this browser only, like the color scheme.
// (In a multiplayer broadcast, everyone watching follows the device in charge
// of simming instead - see LiveGame.)

export type LiveGameView = "classic" | "2.5d";

const KEY = "bbgmLiveGameView";

// "retro" is what the first version of the 2.5D court was called.
export const parseLiveGameView = (value: unknown): LiveGameView =>
	value === "2.5d" || value === "retro" ? "2.5d" : "classic";

export const getLiveGameView = (): LiveGameView =>
	parseLiveGameView(safeLocalStorage.getItem(KEY));

export const setLiveGameView = (view: LiveGameView) => {
	if (view === "2.5d") {
		safeLocalStorage.setItem(KEY, view);
	} else {
		safeLocalStorage.removeItem(KEY);
	}
};
