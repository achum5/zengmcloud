// THE BLENDER BOWL ON THIS DEVICE OR NOT (Global Settings, "3D Arena"): off
// until it is ready, today's stands as they were (see bowlGl.ts).

const KEY = "court3dBowl";

export const bowlHere = (): boolean => {
	try {
		return localStorage.getItem(KEY) === "1";
	} catch {
		return false;
	}
};

export const setBowl = (on: boolean) => {
	try {
		if (on) {
			localStorage.setItem(KEY, "1");
		} else {
			localStorage.removeItem(KEY);
		}
	} catch {
		// (Nowhere to keep it: today's stands, as ever.)
	}
};
