import { assert, test } from "vitest";
import { drawHoop, shotClockFace } from "./arena.ts";
import {
	makeCamera,
	MAIN_RIG,
	project,
	type Camera,
	type Rig,
} from "./camera.ts";
import { COURT_W, type Pt3, type Side } from "./geometry.ts";

// The shot clock on top of the board is the NBA's round one: the game
// clock over the shot clock on both its faces. Whichever face the camera
// sees reads the right way round - not mirrored, not upside down - at
// either end, from the court and from behind the baseline; and from the
// sideline with the play at that end it is seen all but edge on, as the
// real one is.

const at = (p: Pt3, q: Pt3, k: number): Pt3 => ({
	x: p.x + q.x * k,
	y: p.y + q.y * k,
	z: p.z + q.z * k,
});
// The face's across and down on the picture.
const axes = (cam: Camera, side: Side) => {
	const f = shotClockFace(cam, side);
	assert.isDefined(f);
	const o = project(cam, f!.origin);
	const across = project(cam, at(f!.origin, f!.alongX, f!.w));
	const down = project(cam, at(f!.origin, f!.alongY, f!.h));
	return {
		ax: across.x - o.x,
		ay: across.y - o.y,
		dx: down.x - o.x,
		dy: down.y - o.y,
	};
};
const standing = (x: number, y: number, z: number): Rig => ({
	back: 0,
	high: 0,
	slide: 0,
	upright: false,
	at: { x, y, z },
});

test("its face reads the right way round from the court and from behind the baseline", () => {
	for (const side of [0, 1] as const) {
		const X = (d: number) => (side === 0 ? d : COURT_W - d);
		const looks = [
			// Up the floor at the basket, off to either side of it.
			makeCamera(
				{ x: X(4), y: 25, z: 15, width: 16 },
				1200,
				675,
				standing(X(24), 18, 11),
			),
			makeCamera(
				{ x: X(4), y: 25, z: 15, width: 16 },
				1200,
				675,
				standing(X(20), 36, 9),
			),
			// From behind the baseline, up in the corner.
			makeCamera(
				{ x: X(4), y: 25, z: 15, width: 16 },
				1200,
				675,
				standing(X(-12), 34, 14),
			),
			// The broadcast's, the play at the other end.
			makeCamera({ x: X(70), width: 46, y: 24 }, 1200, 675, MAIN_RIG),
		];
		for (const cam of looks) {
			const a = axes(cam, side);
			const msg = JSON.stringify({ side, a });
			assert.isAbove(a.ax, 0, msg);
			assert.isAbove(a.dy, 0, msg);
			assert.isAbove(a.ax * a.dy - a.ay * a.dx, 0, msg);
		}
	}
});

test("from the sideline with the play at that end it is all but edge on", () => {
	for (const side of [0, 1] as const) {
		const x = side === 0 ? 12 : COURT_W - 12;
		const a = axes(
			makeCamera({ x, width: 46, y: 24 }, 1200, 675, MAIN_RIG),
			side,
		);
		assert.isBelow(Math.abs(a.ax), 0.25 * a.dy, JSON.stringify({ side, a }));
	}
});

test("drawn without a page to paint its face on (as here), the basket still draws", () => {
	const ctx = new Proxy(
		{},
		{
			get: () => () => ({ addColorStop: () => undefined }),
			set: () => true,
		},
	) as unknown as CanvasRenderingContext2D;
	const cam = makeCamera(
		{ x: 8, width: 22, y: 25, z: 9 },
		1200,
		675,
		standing(24, 18, 11),
	);
	drawHoop(
		ctx,
		cam,
		0,
		"#8c1d40",
		"14",
		{ swish: 0, clank: 0, dunk: 0, t: 0 },
		undefined,
		undefined,
		"5:57",
	);
});
