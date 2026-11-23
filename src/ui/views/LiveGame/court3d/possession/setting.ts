// THE NEW POSSESSION SIM, ON THIS DEVICE OR NOT (Global Settings, "3D Game
// Sim"): off until it is ready, today's 3D game playing exactly as it did.
// Staged games go by it (see stageCourt.ts).

const KEY = "court3dPossessionSim";

export const possessionSimHere = (): boolean => {
	try {
		return localStorage.getItem(KEY) === "1";
	} catch {
		return false;
	}
};

export const setPossessionSim = (on: boolean) => {
	try {
		if (on) {
			localStorage.setItem(KEY, "1");
		} else {
			localStorage.removeItem(KEY);
		}
	} catch {
		// (Nowhere to keep it: today's, as ever.)
	}
};
