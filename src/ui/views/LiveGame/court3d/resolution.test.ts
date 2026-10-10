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
	test("the screen's own resolution, sharp, unless it is huge", () => {
		const res = makeResolution(0);
		// A 720-pixel-tall picture: one screen pixel each.
		assert.strictEqual(artFor(720, res), 1);
		// A phone's 4:3 picture at 3x: one screen pixel each, too.
		assert.strictEqual(artFor(877, res), 1);
		assert.strictEqual(artFor(1300, res), 1);
		// A big picture on a 2x screen: two screen pixels each.
		assert.strictEqual(artFor(1620, res), 2);
		assert.strictEqual(artFor(300, res), 1);
	});

	test("a slow device steps coarser, never past the old pixel art, and back when it can", () => {
		const res = makeResolution(0);
		// Struggling (under 40 frames a second): coarser, a step at a time.
		let t = run(res, 1620, 0, 20_000, 45, 30);
		assert.isAbove(artFor(1620, res), 2);
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
		assert.strictEqual(artFor(1620, res), 2);
		assert.strictEqual(res.changes, 0);
	});

	test("judged from the first frame drawn, after its first hitches", () => {
		// Mounted at 0; the game staged and the first frame drawn at 8s, its
		// first frames slow while every player is sculpted for the first time.
		const res = makeResolution(1500);
		const t = run(res, 1300, 8000, 500, 60, 80);
		run(res, 1300, t, 20_000, 16.7, 6);
		assert.strictEqual(artFor(1300, res), 1);
		assert.strictEqual(res.changes, 0);
	});

	test("back to sharp once it can be, but not back and forth", () => {
		const res = makeResolution(0);
		let t = run(res, 1300, 0, 4000, 30, 14);
		assert.strictEqual(artFor(1300, res), 2);
		t = run(res, 1300, t, 6000, 16.7, 4.5);
		assert.strictEqual(artFor(1300, res), 1);
		// Too slow there again: coarser, and there it stays.
		t = run(res, 1300, t, 2000, 30, 14);
		assert.strictEqual(artFor(1300, res), 2);
		run(res, 1300, t, 20_000, 16.7, 4.5);
		assert.strictEqual(artFor(1300, res), 2);
		assert.strictEqual(res.changes, 3);
	});

	test("a device keeping up stays as fine as it gets", () => {
		const res = makeResolution(0);
		run(res, 720, 0, 30_000, 16.7, 9);
		assert.strictEqual(artFor(720, res), 1);
		assert.strictEqual(res.changes, 0);
	});
});
