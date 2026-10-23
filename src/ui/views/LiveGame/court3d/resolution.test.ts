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
	// Of the drawing, making new pictures of the players.
	spriteMs = 0,
) => {
	let t = from;
	for (; t < from + ms; t += frameMs) {
		adjust(res, rows, frameMs, drawMs, t, spriteMs);
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

	test("a hitch every few seconds - new poses at each cut - leaves it as fine as it is", () => {
		const res = makeResolution(0);
		// A Retina picture drawn in 7 ms at 60 frames a second - and every
		// 3 s, at the jump to the next key play, two frames of 100 ms.
		let t = 0;
		for (let k = 0; k < 40; k++) {
			t = run(res, 1350, t, 3000, 16.7, 7);
			for (let i = 0; i < 2; i++) {
				t += 100;
				adjust(res, 1350, 100, 95, t);
			}
		}
		assert.strictEqual(artFor(1350, res), 2);
		assert.strictEqual(res.changes, 0);
		// The page's own first frames of a game, longer still.
		for (const ms of [420, 250, 180, 100]) {
			t += ms;
			adjust(res, 1350, ms, ms - 5, t);
		}
		t = run(res, 1350, t, 5000, 16.7, 7);
		assert.strictEqual(res.changes, 0);
	});

	test("a few seconds of new poses for everybody - the teams coming out - leave it as fine as it is", () => {
		const res = makeResolution(0);
		let t = run(res, 1350, 0, 4000, 16.7, 7, 3);
		t = run(res, 1350, t, 4000, 40, 30, 25);
		run(res, 1350, t, 30_000, 16.7, 7, 3);
		assert.strictEqual(artFor(1350, res), 2);
		assert.strictEqual(res.changes, 0);
	});

	test("coarser while the players' new pictures took too long, back finer once they don't", () => {
		const res = makeResolution(0);
		// Slow for a good while: a size coarser.
		let t = run(res, 1350, 0, 12_000, 40, 30, 25);
		assert.strictEqual(artFor(1350, res), 3);
		// Then a frame in 5.5 ms, 4 of it the players' new pictures: a size
		// finer would take about 10.5 ms (the pictures 2.25 times the
		// pixels, the rest as it is) - room for it.
		run(res, 1350, t, 15_000, 16.7, 5.5, 4);
		assert.strictEqual(artFor(1350, res), 2);
		// But not if nearly all of it was the players' pictures (about
		// 12 ms a size finer).
		const busy = makeResolution(0);
		t = run(busy, 1350, 0, 12_000, 40, 30, 25);
		run(busy, 1350, t, 15_000, 16.7, 5.5, 5);
		assert.strictEqual(artFor(1350, busy), 3);
	});

	test("a device keeping up stays as fine as it gets", () => {
		const res = makeResolution(0);
		run(res, 720, 0, 30_000, 16.7, 9);
		assert.strictEqual(artFor(720, res), 1);
		assert.strictEqual(res.changes, 0);
	});
});
