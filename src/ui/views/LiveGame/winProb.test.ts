import { assert, describe, test } from "vitest";
import { flowChance, GAME_S, momentsOf, winProb } from "./winProb.ts";

// What the model says was measured against the sim (see
// worker/core/game/winProbCalibration.test.ts); these pin what it measured.

describe("win probability", () => {
	test("over: the team ahead has won; level, it is an overtime", () => {
		assert.strictEqual(winProb({ margin: 1, secs: 0 }), 1);
		assert.strictEqual(winProb({ margin: -1, secs: 0 }), 0);
		assert.closeTo(winProb({ margin: 0, secs: 0 }), 0.5, 1e-9);
		// (The better team is the better bet in overtime, a little.)
		const ot = winProb({ margin: 0, secs: 0, spread: 8 });
		assert.isAbove(ot, 0.52);
		assert.isBelow(ot, 0.62);
	});

	test("never certain before the end", () => {
		for (const secs of [1, 5, 30, 600]) {
			for (const margin of [-30, -4, 4, 30]) {
				const p = winProb({ margin, secs });
				assert.isAbove(p, 0);
				assert.isBelow(p, 1);
			}
		}
	});

	test("at the tip: the spread's, or a coin flip", () => {
		assert.closeTo(winProb({ margin: 0, secs: GAME_S }), 0.5, 1e-9);
		const fav = winProb({ margin: 0, secs: GAME_S, spread: 6 });
		assert.isAbove(fav, 0.63);
		assert.isBelow(fav, 0.7);
		// Even teams: either side of the same margin adds up to a certainty.
		assert.closeTo(
			winProb({ margin: 7, secs: GAME_S / 2 }) +
				winProb({ margin: -7, secs: GAME_S / 2 }),
			1,
			1e-9,
		);
	});

	test("more ahead, or less time left, is more likely", () => {
		const half = GAME_S / 2;
		assert.isAbove(
			winProb({ margin: 10, secs: half }),
			winProb({ margin: 5, secs: half }),
		);
		assert.isAbove(
			winProb({ margin: 5, secs: 120 }),
			winProb({ margin: 5, secs: half }),
		);
	});

	test("leads fade: ten up at the half is about four in five, not nine in ten", () => {
		const p = winProb({ margin: 10, secs: GAME_S / 2 });
		assert.isAbove(p, 0.76);
		assert.isBelow(p, 0.84);
		// Twenty up after a quarter: a long way from over (the sim's teams win
		// from there 84-91% of the time).
		const q1 = winProb({ margin: 20, secs: (GAME_S * 3) / 4 });
		assert.isAbove(q1, 0.82);
		assert.isBelow(q1, 0.93);
	});

	test("the first few possessions barely move it", () => {
		const early = winProb({ margin: 2, secs: GAME_S - 60 });
		assert.isAbove(early, 0.5);
		assert.isBelow(early, 0.56);
		// The favorite by 8 is still the favorite, down 5-0 two minutes in...
		assert.isAbove(
			winProb({ margin: -5, secs: GAME_S - 120, spread: 8 }),
			0.55,
		);
		// ...but not down 5 with two minutes left.
		assert.isBelow(winProb({ margin: -5, secs: 120, spread: 8 }), 0.2);
	});

	test("down 2 with 9 seconds left: very much alive, more so with the ball", () => {
		const unknown = winProb({ margin: -2, secs: 9 });
		const withBall = winProb({ margin: -2, secs: 9, ball: 1 });
		const without = winProb({ margin: -2, secs: 9, ball: 0 });
		assert.isAbove(unknown, 0.05);
		assert.isBelow(unknown, 0.45);
		assert.isAbove(withBall, 0.2);
		assert.isBelow(withBall, 0.35);
		assert.isAbove(without, 0.03);
		assert.isBelow(without, 0.15);
		// The same in overtime.
		assert.closeTo(
			winProb({ margin: -2, secs: 9, ball: 1, overtime: true }),
			withBall,
			1e-9,
		);
	});

	test("a possession is everything at the end: down 3 with the ball is a chance, down 4 is not", () => {
		const three = winProb({ margin: -3, secs: 5, ball: 1 });
		const four = winProb({ margin: -4, secs: 5, ball: 1 });
		assert.isAbove(three, 0.08);
		assert.isBelow(four, 0.03);
		// Up 1 with the ball and a few seconds left: nearly - not quite - over.
		const up1 = winProb({ margin: 1, secs: 3, ball: 1 });
		assert.isAbove(up1, 0.9);
		assert.isBelow(up1, 1);
	});

	test("the ball counts for little early and a lot at the end", () => {
		assert.isBelow(
			winProb({ margin: 0, secs: GAME_S * 0.9, ball: 1 }) -
				winProb({ margin: 0, secs: GAME_S * 0.9, ball: 0 }),
			0.03,
		);
		assert.isAbove(winProb({ margin: 0, secs: 10, ball: 1 }), 0.6);
		assert.isBelow(winProb({ margin: 0, secs: 10, ball: 0 }), 0.4);
	});

	test("smooth into the last minute", () => {
		for (const margin of [-6, -3, -1, 0, 2, 5]) {
			for (const ball of [0, 0.5, 1]) {
				let last = winProb({ margin, secs: 80, ball });
				for (let secs = 79; secs >= 30; secs -= 1) {
					const p = winProb({ margin, secs, ball });
					assert.isBelow(Math.abs(p - last), 0.02, `${margin} ${ball} ${secs}`);
					last = p;
				}
			}
		}
	});

	test("a shorter game is less settled by the same lead at the same share of it", () => {
		// (Less time to come back in.)
		const short = 40 * 60;
		assert.isAbove(
			winProb({ margin: 6, secs: short / 4, regS: short }),
			winProb({ margin: 6, secs: GAME_S / 4 }),
		);
	});
});

describe("over a game drawn from its scores", () => {
	// Ten-minute halves, an overtime of five; team 1 scores last in
	// regulation to tie it, team 0 wins it in overtime.
	const flow = {
		scores: [
			[0, 0, 0],
			[30, 2, 0],
			[1190, 2, 2],
			[1400, 5, 2],
		] as [number, number, number][],
		periodEnds: [600, 1200, 1500],
		total: 1500,
	};
	const chance = flowChance(flow, { numPeriods: 2, spread: 0, over: true });

	test("over, the result; level at the end of regulation, overtime's coin flip", () => {
		assert.strictEqual(chance(1500), 1);
		assert.closeTo(chance(1200), 0.5, 0.01);
	});

	test("just scored on, a team has the ball", () => {
		// Team 1 ties it with 10 seconds left: team 0's ball, and more than
		// even - then, as the seconds go, less so.
		const now = chance(1190);
		assert.isAbove(now, 0.55);
		assert.isBelow(chance(1199), now);
		// Down 2 with 10 seconds left, the ball whoever's: alive.
		assert.isAbove(1 - chance(1189.999), 0.1);
	});
});

describe("over a game's events", () => {
	test("a moment before each event, the result after", () => {
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
		assert.isAbove(m[3]!.p0, 0.5);
		assert.strictEqual(m.at(-1)!.p0, 1);
		// The jump won: team 0's ball, a little better than even.
		assert.isAbove(m[1]!.p0, 0.5);
		assert.isBelow(m[1]!.p0, 0.55);
	});

	test("overtime is its own period, its clock its own", () => {
		const events = [
			{ type: "jumpBall", t: 0, clock: 720 },
			{ type: "period" },
			{ type: "period" },
			{ type: "period" },
			{ type: "overtime" },
			{ type: "jumpBall", t: 1, clock: 300 },
			{ type: "tp", t: 1, clock: 4 },
			{ type: "stat", t: 1, s: "pts", amt: 3 },
			{ type: "gameOver" },
		];
		const m = momentsOf(events, { edge: 0 });
		assert.strictEqual(m[5]!.period, 5);
		// Level at the overtime's tip.
		assert.closeTo(m[5]!.p0, 0.5, 0.01);
		// Team 1 up 3 with 4 seconds left, the ball team 0's: a long shot.
		assert.isAbove(m[8]!.p0, 0.05);
		assert.isBelow(m[8]!.p0, 0.2);
		assert.strictEqual(m.at(-1)!.p0, 0);
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
