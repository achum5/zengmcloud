import { safeLocalStorage } from "../../../util/safeLocalStorage.ts";

// HOW THE CAMERA SHOWS THE GAME, as whoever is watching likes it - kept on
// this device (see the settings beside the court's controls).
//
// `zoom`: how far in it is on the play, to the camera as it always was (1:
// the whole floor's depth in the picture, the players small on it, the way a
// cartoon game shows them). In closer it can't show the whole floor at
// once, so it follows the play across the floor as well as along it.
export type CameraSettings = { zoom: number };
export const CAMERA_DEFAULTS: CameraSettings = { zoom: 1 };
export const CAMERA_ZOOM = { min: 1, max: 2.5, step: 0.05 } as const;

const KEY = "court3dCamera";

export const parseCamera = (value: unknown): CameraSettings => {
	const v = (value ?? {}) as Partial<Record<keyof CameraSettings, unknown>>;
	const zoom =
		typeof v.zoom === "number" && Number.isFinite(v.zoom) ? v.zoom : 1;
	return {
		zoom: Math.min(CAMERA_ZOOM.max, Math.max(CAMERA_ZOOM.min, zoom)),
	};
};

// (Read once and kept: the court asks every frame.)
let now: CameraSettings | undefined;
export const getCamera = (): CameraSettings => {
	if (!now) {
		try {
			const raw = safeLocalStorage.getItem(KEY);
			now = parseCamera(raw ? JSON.parse(raw) : undefined);
		} catch {
			now = CAMERA_DEFAULTS;
		}
	}
	return now;
};
export const setCamera = (settings: CameraSettings) => {
	now = parseCamera(settings);
	try {
		safeLocalStorage.setItem(KEY, JSON.stringify(now));
	} catch {
		// (Kept for this visit all the same.)
	}
};
