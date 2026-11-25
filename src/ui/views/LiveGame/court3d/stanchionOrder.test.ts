import { assert, test } from "vitest";
import { drawOrder, eyeOf, splits, type ModelPlace } from "./arenaModel.ts";
import {
	depthOf,
	makeCamera,
	MAIN_RIG,
	project,
	type Camera,
	type Rig,
} from "./camera.ts";
import { COURT_W, type Side } from "./geometry.ts";
import { STANCHION } from "./stanchionModel.ts";

// The stanchion's parts go over one another the way they stand - the pad on
// the boom over the boom, the column over the post from the court - and
// never swap from one frame to the next as the camera slides with the play
// (they used to: the main camera had most of them the same distance away,
// and which went over which flickered with the rounding).

const placeOf = (side: Side): ModelPlace => ({
	at: (p) => ({
		x: side === 0 ? p[0]! : COURT_W - p[0]!,
		y: p[1]!,
		z: p[2]!,
	}),
	mirrored: side === 1,
});
const orderOf = (cam: Camera, side: Side) => {
	const place = placeOf(side);
	const depths = STANCHION.parts.map((part) => {
		let d = 0;
		for (const p of part.pts) {
			d += depthOf(cam, place.at(p, 0));
		}
		return d / part.pts.length;
	});
	return drawOrder(cam, STANCHION, place, depths).map(
		(i) => STANCHION.parts[i]!.name,
	);
};
const over = (order: string[], top: string, under: string, msg: string) =>
	assert.isAbove(order.indexOf(top), order.indexOf(under), msg);
const standing = (x: number, y: number, z: number): Rig => ({
	back: 0,
	high: 0,
	slide: 0,
	upright: false,
	at: { x, y, z },
});

test("every two parts have a wall between them", () => {
	const n = STANCHION.parts.length;
	assert.strictEqual(splits(STANCHION).length, (n * (n - 1)) / 2);
});

test("the upright camera's lines of sight all run back to its eye", () => {
	for (const x of [6, 47, 88]) {
		const cam = makeCamera({ x, width: 46, y: 24 }, 1200, 675, MAIN_RIG);
		const eye = eyeOf(cam);
		for (const p of [
			{ x: 0, y: 25, z: 11 },
			{ x: COURT_W + 8, y: 23, z: 2 },
			{ x: x + 10, y: 40, z: 6 },
		]) {
			const a = project(cam, p);
			for (const k of [0.5, 1.7]) {
				const b = project(cam, {
					x: eye.x + (p.x - eye.x) * k,
					y: eye.y + (p.y - eye.y) * k,
					z: eye.z + (p.z - eye.z) * k,
				});
				assert.closeTo(b.x, a.x, 1e-6);
				assert.closeTo(b.y, a.y, 1e-6);
			}
		}
	}
});

test("the main camera sliding with the play never swaps two parts", () => {
	for (const side of [0, 1] as const) {
		let last: string | undefined;
		let swaps = 0;
		for (let i = 0; i <= 2400; i++) {
			// Down the whole floor, a few hundredths of a foot a frame - in and
			// out, and up and down the floor a little, as it follows the play.
			const x = (i / 2400) * COURT_W;
			const cam = makeCamera(
				{
					x,
					width: 30 + 16 * Math.abs(Math.sin(i / 97)),
					y: 24 + 3 * Math.sin(i / 53),
				},
				390,
				600,
				MAIN_RIG,
			);
			const order = orderOf(cam, side);
			const msg = JSON.stringify({ side, x, order });
			over(order, "boomPad", "boom", msg);
			over(order, "column", "post", msg);
			over(order, "column", "base", msg);
			over(order, "earNear", "column", msg);
			over(order, "adBox", "boom", msg);
			const key = order.join();
			if (last !== undefined && key !== last) {
				swaps += 1;
			}
			last = key;
		}
		// (Only as the camera goes past the end of a wall between two parts
		// that are side by side on the screen then.)
		assert.isAtMost(swaps, 6, `side ${side}`);
	}
});

test("from behind the baseline the post and the boom go over the column and the pad", () => {
	for (const side of [0, 1] as const) {
		const X = (d: number) => (side === 0 ? d : COURT_W - d);
		const cam = makeCamera(
			{ x: X(4), y: 25, z: 12, width: 16 },
			1200,
			675,
			standing(X(-14), 31, 16),
		);
		const order = orderOf(cam, side);
		const msg = JSON.stringify({ side, order });
		over(order, "post", "column", msg);
		over(order, "boom", "boomPad", msg);
		over(order, "boomPad", "base", msg);
	}
});
