import { assert, describe, test } from "vitest";
import { adjust, artFor, makeResolution } from "./resolution.ts";

// Frames for `ms` of screen time, each drawn in `drawMs` and `frameMs` apart.
const run = (
	res: ReturnType<typeof makeResolution>,
	rows: number,
	from: number,
	ms: number,
	frameMs: number,
	drawMs: number,
) => {
	let t = from;
	for (; t < from + ms; t += frameMs) {
		adjust(res, rows, frameMs, drawMs, t);
	}
	return t;
};

describe("3D picture resolution", () => {
	test("as fine as a whole number of screen pixels allows", () => {
		const res = makeResolution(0);
		// A 720-pixel-tall picture: one screen pixel each.
		assert.strictEqual(artFor(720, res), 1);
		// A phone's 4:3 picture at 3x: two screen pixels each.
		assert.strictEqual(artFor(877, res), 2);
		// A big 2x screen: still about as fine.
		assert.strictEqual(artFor(1620, res), 3);
		assert.strictEqual(artFor(300, res), 1);
	});

	test("a slow device steps coarser, never past the old pixel art, and back when it can", () => {
		const res = makeResolution(0);
		// Struggling (under 40 frames a second): coarser, a step at a time.
		let t = run(res, 1620, 0, 20_000, 45, 30);
		assert.isAbove(artFor(1620, res), 3);
		assert.isAtMost(artFor(1620, res), Math.ceil(1620 / 300));
		const coarse = artFor(1620, res);
		// Now with time to spare: finer again.
		t = run(res, 1620, t, 20_000, 16.7, 2);
		assert.isBelow(artFor(1620, res), coarse);
		// But never a flicker back and forth for good.
		run(res, 1620, t, 60_000, 45, 30);
		assert.isAtMost(res.changes, 6);
	});

	test("slow frames that are not the drawing's doing leave it as fine as it is", () => {
		const res = makeResolution(0);
		run(res, 1620, 0, 30_000, 45, 5);
		assert.strictEqual(artFor(1620, res), 3);
		assert.strictEqual(res.changes, 0);
	});

	test("a device keeping up stays as fine as it gets", () => {
		const res = makeResolution(0);
		run(res, 720, 0, 30_000, 16.7, 9);
		assert.strictEqual(artFor(720, res), 1);
		assert.strictEqual(res.changes, 0);
	});
});
