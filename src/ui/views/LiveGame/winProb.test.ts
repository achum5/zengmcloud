import { assert, describe, test } from "vitest";
import { momentsOf, winProb } from "./winProb.ts";

describe("win probability", () => {
	test("level is a coin flip at the end, and the team ahead has won", () => {
		assert.strictEqual(winProb(0, 0, true, 0), 0.5);
		assert.strictEqual(winProb(1, 0), 1);
		assert.strictEqual(winProb(-1, 0), 0);
	});

	test("more ahead, or less time left, is more likely", () => {
		const t = 20 * 60;
		assert.isAbove(winProb(10, t), winProb(5, t));
		assert.isAbove(winProb(5, 60), winProb(5, t));
		// Symmetric on a neutral floor.
		assert.closeTo(winProb(7, t, true, 0) + winProb(-7, t, true, 0), 1, 1e-9);
		// About right: ten up at the half is about 80%.
		assert.closeTo(winProb(10, 24 * 60, true, 0), 0.86, 0.06);
		// The home edge: a bit better than even at the tip.
		assert.isAbove(winProb(0, 48 * 60, true), 0.5);
		assert.isBelow(winProb(0, 48 * 60, true), 0.6);
	});

	test("through a game: a moment before each event, the result after", () => {
		const events = [
			{ type: "jumpBall", t: 0, clock: 600 },
			{ type: "tp", t: 0, clock: 590 },
			{ type: "stat", t: 0, s: "pts", amt: 3 },
			{ type: "period" },
			{ type: "fgAtRim", t: 1, clock: 1 },
			{ type: "stat", t: 1, s: "pts", amt: 2 },
			{ type: "gameOver" },
		];
		const m = momentsOf(events, { numPeriods: 2 });
		assert.strictEqual(m.length, events.length + 1);
		assert.deepEqual(m[2]!.score, [0, 0]);
		assert.deepEqual(m[3]!.score, [3, 0]);
		// (Ten-minute halves, read off the clock.)
		assert.isAbove(m[3]!.p0, 0.5);
		assert.strictEqual(m.at(-1)!.p0, 1);
	});
});
