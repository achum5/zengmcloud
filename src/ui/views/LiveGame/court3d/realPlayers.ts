import { safeLocalStorage } from "../../../util/safeLocalStorage.ts";

// THE PLAYERS DRAWN FROM THE BLENDER BODY (see meshBody.ts), not sculpted -
// as whoever is watching likes it, kept on this device (see Global
// Settings: 3D Players), while it is tried out. Only the picture: the game,
// and where everybody is in it, is the same either way.
//
// (Its own key: a device that had the old Realistic players on - the same
// switch, scrapped - starts on Classic.)
const KEY = "court3dBlenderPlayers";

let now: boolean | undefined;
const watching = new Set<() => void>();

export const realPlayers = (): boolean => {
	if (now === undefined) {
		try {
			now = safeLocalStorage.getItem(KEY) === "1";
		} catch {
			now = false;
		}
	}
	return now;
};

export const setRealPlayers = (on: boolean) => {
	now = on;
	try {
		if (on) {
			safeLocalStorage.setItem(KEY, "1");
		} else {
			safeLocalStorage.removeItem(KEY);
		}
	} catch {
		// (Kept for this visit all the same.)
	}
	for (const f of watching) {
		f();
	}
};

// (For useSyncExternalStore: the court changes over as soon as it is set.)
export const watchRealPlayers = (f: () => void) => {
	watching.add(f);
	return () => {
		watching.delete(f);
	};
};
