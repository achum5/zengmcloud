import { assert, describe, test } from "vitest";
import {
	awardProbsFromScores,
	expectedGameTotal,
	marginToWinProb,
	normalCdf,
	overProb,
	strengthProbs,
	toHalfPointLine,
	winTotalOverProb,
} from "./sportsbookOdds.ts";

const sum = (xs: number[]) => xs.reduce((a, b) => a + b, 0);

describe("normalCdf", () => {
	test("0 is 0.5, symmetric, monotonic", () => {
		assert.ok(Math.abs(normalCdf(0) - 0.5) < 1e-6);
		assert.ok(Math.abs(normalCdf(-1) + normalCdf(1) - 1) < 1e-6);
		assert.ok(normalCdf(1) > normalCdf(0));
		assert.ok(normalCdf(2) > 0.97);
	});
});

describe("marginToWinProb", () => {
	test("a pick'em is 50/50; favorites above, dogs below", () => {
		assert.ok(Math.abs(marginToWinProb(0) - 0.5) < 1e-6);
		assert.ok(marginToWinProb(6) > 0.5);
		assert.ok(marginToWinProb(-6) < 0.5);
		// Home + away win probs of a single game complement.
		assert.ok(Math.abs(marginToWinProb(6) + marginToWinProb(-6) - 1) < 1e-6);
	});
	test("a big favorite is a heavy favorite but never certain", () => {
		const p = marginToWinProb(20);
		assert.ok(p > 0.85 && p < 0.995);
	});
});

describe("expectedGameTotal", () => {
	test("blends each team's scoring for and against", () => {
		const t = expectedGameTotal({
			homeFor: 115,
			homeAgainst: 110,
			awayFor: 105,
			awayAgainst: 108,
			leagueAvgTotal: 220,
		});
		// homePts = (115 + 108)/2 = 111.5; awayPts = (105 + 110)/2 = 107.5 → 219.
		assert.ok(Math.abs(t - 219) < 1e-6);
	});
	test("falls back to league average when a team has no data", () => {
		const t = expectedGameTotal({ leagueAvgTotal: 220 });
		assert.ok(Math.abs(t - 220) < 1e-6);
	});
});

describe("overProb", () => {
	test("at the projection it's a coin flip; scales with the line", () => {
		assert.ok(Math.abs(overProb(220, 220) - 0.5) < 1e-6);
		assert.ok(overProb(220, 210) > 0.5); // low line → likely over
		assert.ok(overProb(220, 230) < 0.5);
	});
});

describe("toHalfPointLine", () => {
	test("lands on a half point", () => {
		assert.strictEqual(toHalfPointLine(47.3), 47.5);
		assert.strictEqual(toHalfPointLine(47.8), 47.5);
		assert.strictEqual(toHalfPointLine(48), 48.5);
		// Always a half point, never an integer.
		for (const x of [10, 10.2, 10.9, 55.5]) {
			assert.notStrictEqual(toHalfPointLine(x) % 1, 0);
		}
	});
});

describe("strengthProbs", () => {
	test("sums to 1 and favors the strongest", () => {
		const probs = strengthProbs([70, 60, 55, 50], 1.2);
		assert.ok(Math.abs(sum(probs) - 1) < 1e-9);
		assert.ok(probs[0]! > probs[1]!);
		assert.ok(probs[1]! > probs[2]!);
	});
	test("higher power concentrates on the favorite", () => {
		const soft = strengthProbs([70, 60, 55, 50], 0.5);
		const sharp = strengthProbs([70, 60, 55, 50], 2.5);
		assert.ok(sharp[0]! > soft[0]!);
	});
	test("equal strengths split evenly", () => {
		const probs = strengthProbs([50, 50, 50, 50], 1.5);
		for (const p of probs) {
			assert.ok(Math.abs(p - 0.25) < 1e-9);
		}
	});
});

describe("winTotalOverProb", () => {
	test("line at the projection is ~50/50", () => {
		const p = winTotalOverProb({
			projectedWins: 41,
			line: 41,
			gamesTotal: 82,
			winProb: 0.5,
		});
		assert.ok(Math.abs(p - 0.5) < 1e-6);
	});
	test("a line below the projection is more likely to go over", () => {
		const p = winTotalOverProb({
			projectedWins: 55,
			line: 48.5,
			gamesTotal: 82,
			winProb: 0.67,
		});
		assert.ok(p > 0.5);
	});
});

describe("awardProbsFromScores", () => {
	test("sums to 1, favorite leads, long shots non-zero", () => {
		const probs = awardProbsFromScores([100, 80, 70, 40, 20]);
		assert.ok(Math.abs(sum(probs) - 1) < 1e-9);
		assert.ok(probs[0]! > probs[1]!);
		assert.ok(probs.at(-1)! > 0);
	});
	test("works as a pure rank model with a descending series", () => {
		const probs = awardProbsFromScores([5, 4, 3, 2, 1]);
		assert.ok(probs[0]! > probs[4]!);
		assert.ok(Math.abs(sum(probs) - 1) < 1e-9);
	});
});
