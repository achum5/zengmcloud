import { useSyncExternalStore } from "react";
import { safeLocalStorage } from "../../../util/safeLocalStorage.ts";

// HOW MUCH OF A POSSESSION PLAYS AT REAL SPEED, as whoever is watching likes
// it - kept on this device (see the settings beside the court's controls).
//
// By default the picture runs through the ball brought up and the set got
// going, and plays the last seconds before each shot, turnover or foul at
// real speed. `whole`: every possession played whole, only the dead ball
// run through fast. The game staged is the same either way (see the
// timeline's calm, and showWhole in evaluate.ts), so a device watching
// along with another shows the same game whichever it is.
export type Pacing = { whole: boolean };
export const PACING_DEFAULTS: Pacing = { whole: false };

const KEY = "court3dPacing";

export const parsePacing = (value: unknown): Pacing => {
	const v = (value ?? {}) as Partial<Record<keyof Pacing, unknown>>;
	return { whole: v.whole === true };
};

// (Read once and kept: the court asks every frame.)
let now: Pacing | undefined;
export const getPacing = (): Pacing => {
	if (!now) {
		try {
			const raw = safeLocalStorage.getItem(KEY);
			now = parsePacing(raw ? JSON.parse(raw) : undefined);
		} catch {
			now = PACING_DEFAULTS;
		}
	}
	return now;
};
const told = new Set<() => void>();
export const setPacing = (pacing: Pacing) => {
	now = parsePacing(pacing);
	try {
		safeLocalStorage.setItem(KEY, JSON.stringify(now));
	} catch {
		// (Kept for this visit all the same.)
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
// (Switched over in the settings, the court has it at once.)
export const usePacing = (): Pacing => useSyncExternalStore(listen, getPacing);
