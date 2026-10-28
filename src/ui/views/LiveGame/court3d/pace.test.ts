import { assert, describe, test } from "vitest";
import { drawn, due, FAST, makePace, rateOf, SLOW } from "./pace.ts";

// Frames on a screen refreshing every `refreshMs`, for `ms`: each drawn when
// due, taking `workMs` - the screen's next frame after one that took longer
// than its refresh is the first one after it is done.
const run = (
	pace: ReturnType<typeof makePace>,
	from: number,
	ms: number,
	refreshMs: number,
	workMs: number,
	paused = false,
) => {
	let frames = 0;
	let t = from;
	while (t < from + ms) {
		let next = t + refreshMs;
		if (due(pace, t, paused)) {
			drawn(pace, t, workMs, paused);
			frames += 1;
			while (next < t + workMs) {
				next += refreshMs;
			}
		}
		t = next;
	}
	return { t, frames };
};

describe("3D frame pacing", () => {
	test("a 120 Hz screen gets 60 frames a second, a 60 Hz one every frame", () => {
		const fast = makePace(0);
		assert.closeTo(run(fast, 0, 10_000, 1000 / 120, 4).frames / 10, 60, 2);
		const sixty = makePace(0);
		assert.closeTo(run(sixty, 0, 10_000, 1000 / 60, 4).frames / 10, 60, 2);
		assert.strictEqual(sixty.fps, FAST);
	});

	test("a device that can't keep up draws a steady 30", () => {
		const pace = makePace(0);
		// Frames taking 24 ms: about 40 a second, unevenly.
		let { t } = run(pace, 0, 15_000, 1000 / 60, 24);
		assert.strictEqual(pace.fps, SLOW);
		const { frames } = run(pace, t, 10_000, 1000 / 60, 24);
		assert.closeTo(frames / 10, 30, 2);
		// And back to 60 once it has time to spare - not straight away.
		t += 10_000;
		({ t } = run(pace, t, 5_000, 1000 / 60, 3));
		assert.strictEqual(pace.fps, SLOW);
		run(pace, t, 30_000, 1000 / 60, 3);
		assert.strictEqual(pace.fps, FAST);
	});

	test("a phone drawing 60 only by doing little else draws 30; a computer keeps 60", () => {
		const phone = makePace(0, true);
		run(phone, 0, 15_000, 1000 / 60, 13);
		assert.strictEqual(phone.fps, SLOW);
		const computer = makePace(0);
		run(computer, 0, 15_000, 1000 / 60, 13);
		assert.strictEqual(computer.fps, FAST);
		// Plenty of room: a phone stays at 60 too.
		const easy = makePace(0, true);
		run(easy, 0, 15_000, 1000 / 60, 5);
		assert.strictEqual(easy.fps, FAST);
	});

	test("paused, 30 a second - and the pause says nothing about the device", () => {
		const pace = makePace(0);
		const { frames } = run(pace, 0, 15_000, 1000 / 60, 12, true);
		assert.closeTo(frames / 15, 30, 2);
		assert.strictEqual(pace.fps, FAST);
		assert.strictEqual(rateOf(pace, true), SLOW);
		assert.strictEqual(rateOf(pace), FAST);
	});

	test("a long break in drawing (off the screen) is not a slow frame", () => {
		const pace = makePace(0);
		let { t } = run(pace, 0, 5_000, 1000 / 60, 4);
		for (let k = 0; k < 10; k++) {
			t += 2000;
			({ t } = run(pace, t, 100, 1000 / 60, 4));
		}
		assert.strictEqual(pace.fps, FAST);
	});
});
