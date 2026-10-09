import { assert, describe, test } from "vitest";
import { courtFit, makeCamera, MAIN_RIG, project } from "./camera.ts";
import { COURT_H, COURT_W } from "./geometry.ts";

describe("3D camera", () => {
	// However tight the play wants it, both sidelines stay in the picture:
	// the near one above the bottom edge, the far one's players below the top.
	test("the whole floor stays in the picture, sideline to sideline", () => {
		for (const [w, h] of [
			[1280, 720],
			[640, 480],
			[1920, 1080],
		] as const) {
			const fit = courtFit(w / h);
			for (const width of [fit.min, fit.min + 6, fit.min + 20]) {
				for (const x of [20, COURT_W / 2, COURT_W - 20]) {
					const cam = makeCamera({ x, width, y: fit.y(width) }, w, h, MAIN_RIG);
					for (const across of [x - width / 3, x, x + width / 3]) {
						const near = project(cam, { x: across, y: COURT_H, z: 0 });
						const far = project(cam, { x: across, y: 0, z: 7 });
						assert.isBelow(near.y, h, `${w}x${h} ${width}`);
						assert.isAbove(far.y, 0, `${w}x${h} ${width}`);
					}
				}
			}
			// And not zoomed out further than it has to be.
			const tight = makeCamera(
				{ x: COURT_W / 2, width: fit.min, y: fit.y(fit.min) },
				w,
				h,
				MAIN_RIG,
			);
			const near = project(tight, { x: COURT_W / 2, y: COURT_H, z: 0 });
			assert.isAbove(near.y, h * 0.9);
		}
	});
});
