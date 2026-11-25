import { useSyncExternalStore } from "react";
import { safeLocalStorage } from "../../../../util/safeLocalStorage.ts";

// GAME SOUND ON THIS DEVICE OR NOT (Global Settings, "Game Sound", and the
// speaker among the live game's controls): off until somebody turns it on.
// Off, nothing of it is fetched or run at all.

const KEY = "gameSound";

// Shown (the speaker, and Global Settings' "Game Sound") once the real
// recordings are in (see bank.ts): till then none of it is heard anywhere.
export const SOUND_SHOWN: boolean = false;

// (Read once and kept: the court asks every frame.)
let now: boolean | undefined;
export const soundHere = (): boolean => {
	if (!SOUND_SHOWN) {
		return false;
	}
	if (now === undefined) {
		try {
			now = safeLocalStorage.getItem(KEY) === "1";
		} catch {
			now = false;
		}
	}
	return now;
};

const told = new Set<() => void>();
export const setSound = (on: boolean) => {
	now = on;
	try {
		if (on) {
			safeLocalStorage.setItem(KEY, "1");
		} else {
			safeLocalStorage.removeItem(KEY);
		}
	} catch {
		// (Nowhere to keep it: on or off for this visit all the same.)
	}
	for (const tell of told) {
		tell();
	}
};
const listen = (tell: () => void) => {
	told.add(tell);
	return () => {
		told.delete(tell);
	};
};
// (Switched in the settings or at the speaker, the court has it at once.)
export const useSound = (): boolean => useSyncExternalStore(listen, soundHere);
