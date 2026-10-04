import { safeLocalStorage } from "../../../util/safeLocalStorage.ts";

// Which live-game picture this device shows: the classic 2D court or the
// retro one. Saved in this browser only, like the color scheme, so each person
// picks on each device and a multiplayer room never has to agree.

export type LiveGameView = "classic" | "retro";

const KEY = "bbgmLiveGameView";

export const getLiveGameView = (): LiveGameView =>
	safeLocalStorage.getItem(KEY) === "retro" ? "retro" : "classic";

export const setLiveGameView = (view: LiveGameView) => {
	if (view === "retro") {
		safeLocalStorage.setItem(KEY, view);
	} else {
		safeLocalStorage.removeItem(KEY);
	}
};
