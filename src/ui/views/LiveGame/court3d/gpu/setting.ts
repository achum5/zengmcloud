// HOW THE 3D GAME IS DRAWN ON THIS DEVICE (Global Settings, "3D Drawing"):
// as today, as today with its frame rate shown over it, or on the graphics
// card all in one picture (a test of that - see scene.ts), its frame rate
// shown the same.

export type GpuMode = "off" | "fps" | "test";
const KEY = "court3dGpu";

export const gpuMode = (): GpuMode => {
	try {
		const v = localStorage.getItem(KEY);
		return v === "fps" || v === "test" ? v : "off";
	} catch {
		return "off";
	}
};

export const setGpuMode = (mode: GpuMode) => {
	try {
		if (mode === "off") {
			localStorage.removeItem(KEY);
		} else {
			localStorage.setItem(KEY, mode);
		}
	} catch {
		// (Nowhere to keep it: drawn as today.)
	}
};
