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

// The same, but the frames as far apart as a device takes for a picture of
// the size it is now.
const runAt = (
	res: ReturnType<typeof makeResolution>,
	rows: number,
	from: number,
	ms: number,
	frameAt: (art: number) => number,
	drawMs = 3,
) => {
	let t = from;
	while (t < from + ms) {
		const f = frameAt(artFor(rows, res));
		adjust(res, rows, f, drawMs, t);
		t += f;
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

	// A phone: the drawing itself quick (the players sculpted elsewhere), but
	// a sharp picture too much to put on its screen in time.
	test("too slow for the sharp picture: coarser, as far as it takes", () => {
		const res = makeResolution(0);
		runAt(res, 1300, 0, 30_000, (art) => 48 / art);
		assert.strictEqual(artFor(1300, res), 2);
		// Never coarser than the old pixel art, however slow.
		const slow = makeResolution(0);
		runAt(slow, 1300, 0, 60_000, (art) => 400 / art);
		assert.isAtMost(artFor(1300, slow), Math.ceil(1300 / 300));
		assert.isAbove(artFor(1300, slow), 2);
	});

	// The frames held back by something else - the screen at 30 a second to
	// save the battery: a coarser picture is tried, no faster, and it goes
	// back to sharp for good.
	test("a coarser picture that is no faster is not kept", () => {
		const res = makeResolution(0);
		runAt(res, 1300, 0, 60_000, () => 33.3);
		assert.strictEqual(artFor(1300, res), 1);
		assert.strictEqual(res.changes, 2);
		const big = makeResolution(0);
		runAt(big, 1620, 0, 60_000, () => 45);
		assert.strictEqual(artFor(1620, big), 2);
		assert.isAtMost(big.changes, 2);
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
