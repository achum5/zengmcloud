import { assert, describe, test } from "vitest";
import { momentsOf, winProb } from "./winProb.ts";

describe("win probability", () => {
	test("level is a coin flip at the end, and the team ahead has won", () => {
		assert.strictEqual(winProb({ margin: 0, left: 0 }), 0.5);
		assert.strictEqual(winProb({ margin: 1, left: 0 }), 1);
		assert.strictEqual(winProb({ margin: -1, left: 0 }), 0);
	});

	test("more ahead, or less time left, is more likely", () => {
		const half = 0.5;
		assert.isAbove(
			winProb({ margin: 10, left: half }),
			winProb({ margin: 5, left: half }),
		);
		assert.isAbove(
			winProb({ margin: 5, left: 0.02 }),
			winProb({ margin: 5, left: half }),
		);
		// Even teams: either side of the same margin adds up to a certainty.
		assert.closeTo(
			winProb({ margin: 7, left: half }) + winProb({ margin: -7, left: half }),
			1,
			1e-9,
		);
		// Ten up at the half, even teams: about 86%.
		assert.closeTo(winProb({ margin: 10, left: half }), 0.86, 0.03);
	});

	test("the first few possessions barely move it", () => {
		// A basket each way in the first minute, even teams.
		const early = winProb({ margin: 2, left: 47 / 48 });
		assert.isAbove(early, 0.5);
		assert.isBelow(early, 0.58);
		// The favorite by 8 is still the favorite, down 5-0 two minutes in...
		assert.isAbove(winProb({ margin: -5, left: 46 / 48, spread: 8 }), 0.55);
		// ...but not down 5 with two minutes left.
		assert.isBelow(winProb({ margin: -5, left: 2 / 48, spread: 8 }), 0.2);
		// At the tip, it is the spread's.
		assert.closeTo(winProb({ margin: 0, left: 1, spread: 6 }), 0.68, 0.02);
	});

	test("the ball counts for little early and a lot at the end", () => {
		const late = 10 / 2880;
		const tiedWith = winProb({ margin: 0, left: late, ball: 1 });
		const tiedWithout = winProb({ margin: 0, left: late, ball: -1 });
		assert.isAbove(tiedWith, 0.65);
		assert.closeTo(tiedWith + tiedWithout, 1, 1e-9);
		assert.isBelow(winProb({ margin: 0, left: 0.9, ball: 1 }), 0.52);
	});

	test("a shorter game is less settled by the same lead at the same share of it", () => {
		// (Fewer possessions in it: the same share of it is less to come back
		// in, so the same lead counts for more.)
		assert.isAbove(
			winProb({ margin: 6, left: 0.25, length: 40 / 48 }),
			winProb({ margin: 6, left: 0.25 }),
		);
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
		const m = momentsOf(events, { numPeriods: 2, edge: 0 });
		assert.strictEqual(m.length, events.length + 1);
		assert.deepEqual(m[2]!.score, [0, 0]);
		assert.deepEqual(m[3]!.score, [3, 0]);
		// (Ten-minute halves, read off the clock.)
		assert.isAbove(m[3]!.p0, 0.5);
		assert.strictEqual(m.at(-1)!.p0, 1);
		// The jump won: team 0's ball, a little better than even.
		assert.isAbove(m[1]!.p0, 0.5);
		assert.isBelow(m[1]!.p0, 0.55);
	});

	test("the pregame favorite starts ahead, and home court counts without one", () => {
		const events = [
			{ type: "jumpBall", t: 1, clock: 720 },
			{ type: "gameOver" },
		];
		assert.isBelow(momentsOf(events, { spread: -7 })[0]!.p0, 0.32);
		assert.isAbove(momentsOf(events)[0]!.p0, 0.5);
		assert.closeTo(momentsOf(events, { edge: 0 })[0]!.p0, 0.5, 1e-6);
	});
});
