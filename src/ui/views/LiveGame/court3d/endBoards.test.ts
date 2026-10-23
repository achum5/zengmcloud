import { assert, test } from "vitest";
import { END_STANDS, END_WALL, LED_WALL } from "./arena.ts";
import { courtFit, makeCamera, MAIN_RIG, project } from "./camera.ts";
import { COURT_W } from "./geometry.ts";
import type { TexturedPlane } from "./planes.ts";

// The boards behind the baskets read the right way round from the
// broadcast camera, like the ones along the far side - not mirrored, as if
// seen from behind the wall.

// How the plane's picture lies on the screen: + its right and its down the
// way round they are on the picture; - mirrored.
const facing = (plane: TexturedPlane, x: number) => {
	const fit = courtFit(16 / 9);
	const cam = makeCamera({ x, width: 60, y: fit.y(60) }, 1200, 675, MAIN_RIG);
	const at = (u: number, v: number) =>
		project(cam, {
			x: plane.origin.x + plane.alongX.x * u + plane.alongY.x * v,
			y: plane.origin.y + plane.alongX.y * u + plane.alongY.y * v,
			z: plane.origin.z + plane.alongX.z * u + plane.alongY.z * v,
		});
	// Round the middle of it.
	const u = plane.w / 2;
	const v = plane.h / 2;
	const o = at(u, v);
	const r = at(u + 1, v);
	const d = at(u, v + 1);
	return Math.sign((r.x - o.x) * (d.y - o.y) - (r.y - o.y) * (d.x - o.x));
};

test("the boards behind both baskets read the right way round", () => {
	// (As the boards along the far side do.)
	assert.strictEqual(facing(LED_WALL, COURT_W / 2), 1);
	for (const x of [8, 20, COURT_W / 2]) {
		assert.strictEqual(facing(END_WALL[0], x), 1, `left end, camera at ${x}`);
	}
	for (const x of [COURT_W - 8, COURT_W - 20, COURT_W / 2]) {
		assert.strictEqual(facing(END_WALL[1], x), 1, `right end, camera at ${x}`);
	}
});

test("the stands behind them too", () => {
	assert.strictEqual(facing(END_STANDS[0], 14), 1);
	assert.strictEqual(facing(END_STANDS[1], COURT_W - 14), 1);
});
