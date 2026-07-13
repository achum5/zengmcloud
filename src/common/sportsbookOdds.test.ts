import { assert, describe, test } from "vitest";
import {
	awardProbsFromScores,
	expectedGameTotal,
	marginToWinProb,
	normalCdf,
	overProb,
	seriesWinProb,
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
	test("additive matchup model from real points for/against", () => {
		const t = expectedGameTotal({
			homeFor: 115,
			homeAgainst: 110,
			awayFor: 105,
			awayAgainst: 108,
			leagueAvgTotal: 220,
		});
		// homePts = 115 + 108 - 110 = 113; awayPts = 105 + 110 - 110 = 105 → 218.
		assert.ok(Math.abs(t - 218) < 1e-6, `${t}`);
	});

	test("great offense vs bad defense projects ABOVE both season averages", () => {
		// Home scores 118/gm; away allows 120/gm in a 110-average league. A naive
		// blend would pull toward the middle; the additive model goes up.
		const t = expectedGameTotal({
			homeFor: 118,
			homeAgainst: 110,
			awayFor: 110,
			awayAgainst: 120,
			leagueAvgTotal: 220,
		});
		// homePts = 118 + 120 - 110 = 128 → total 238.
		assert.ok(t > 230, `${t}`);
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

describe("seriesWinProb", () => {
	test("a coin flip is 50/50 over any series", () => {
		assert.ok(Math.abs(seriesWinProb(0.5, 7) - 0.5) < 1e-9);
		assert.ok(Math.abs(seriesWinProb(0.5, 1) - 0.5) < 1e-9);
	});
	test("a best-of-1 equals the single-game probability", () => {
		assert.ok(Math.abs(seriesWinProb(0.62, 1) - 0.62) < 1e-9);
	});
	test("a series amplifies the favorite", () => {
		assert.ok(seriesWinProb(0.6, 7) > 0.6);
		assert.ok(seriesWinProb(0.7, 7) > 0.7);
		// Known value: a 60% team wins a best-of-7 about 71% of the time.
		assert.ok(Math.abs(seriesWinProb(0.6, 7) - 0.71) < 0.02);
	});
	test("stays a probability and is monotonic", () => {
		let prev = 0;
		for (let p = 0.05; p <= 0.95; p += 0.05) {
			const s = seriesWinProb(p, 7);
			assert.ok(s >= 0 && s <= 1);
			assert.ok(s >= prev);
			prev = s;
		}
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
