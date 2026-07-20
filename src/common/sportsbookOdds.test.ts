import { assert, describe, test } from "vitest";
import {
	awardProbsFromScores,
	combineIndependentSigmas,
	expectedGameTotal,
	marginToWinProb,
	milestoneProb,
	mulberry32,
	normalCdf,
	normalSample,
	overProb,
	overProbFromSigma,
	probNear,
	seriesWinProb,
	strengthProbs,
	tierMembershipProbs,
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

describe("mulberry32 / normalSample", () => {
	test("mulberry32 is deterministic per seed and stays in [0,1)", () => {
		const a = mulberry32(42);
		const b = mulberry32(42);
		for (let i = 0; i < 20; i++) {
			const x = a();
			const y = b();
			assert.strictEqual(x, y);
			assert.ok(x >= 0 && x < 1);
		}
	});
	test("different seeds diverge", () => {
		const a = mulberry32(1);
		const b = mulberry32(2);
		const seqA = Array.from({ length: 5 }, () => a());
		const seqB = Array.from({ length: 5 }, () => b());
		assert.notDeepEqual(seqA, seqB);
	});
	test("normalSample is roughly standard-normal over many draws", () => {
		const rand = mulberry32(7);
		const n = 20000;
		let sum = 0;
		let sumSq = 0;
		for (let i = 0; i < n; i++) {
			const x = normalSample(rand);
			sum += x;
			sumSq += x * x;
		}
		const mean = sum / n;
		const variance = sumSq / n - mean * mean;
		assert.ok(Math.abs(mean) < 0.05, `mean=${mean}`);
		assert.ok(Math.abs(variance - 1) < 0.1, `variance=${variance}`);
	});
});

describe("tierMembershipProbs", () => {
	test("each candidate's tier probabilities sum to at most 1 (rest is 'misses every tier')", () => {
		const probs = tierMembershipProbs([90, 80, 70, 60, 50, 40], [2, 2], {
			seed: 1,
		});
		for (const row of probs) {
			const total = row.reduce((a, b) => a + b, 0);
			assert.ok(total >= 0 && total <= 1 + 1e-9);
		}
	});
	test("every simulated world assigns each tier slot to exactly one candidate (probabilities sum across the field to the tier size)", () => {
		const scores = [90, 80, 70, 60, 50, 40];
		const tierSizes = [2, 2];
		const probs = tierMembershipProbs(scores, tierSizes, { seed: 3 });
		for (let tier = 0; tier < tierSizes.length; tier++) {
			const total = probs.reduce((sum, row) => sum + row[tier]!, 0);
			assert.ok(
				Math.abs(total - tierSizes[tier]!) < 1e-9,
				`tier ${tier} total=${total}`,
			);
		}
	});
	test("a dominant leader is heavily favored for the top tier, a bottom-dweller is not", () => {
		const probs = tierMembershipProbs([1000, 10, 9, 8, 7, 6], [1, 1, 1], {
			seed: 5,
			iterations: 4000,
		});
		assert.ok(probs[0]![0]! > 0.9, `leader P(tier1)=${probs[0]![0]}`);
		assert.ok(probs.at(-1)![0]! < 0.1, `last P(tier1)=${probs.at(-1)![0]}`);
	});
	test("a tight, bunched field is genuinely competitive (no one is a lock)", () => {
		const probs = tierMembershipProbs([61, 60, 59, 58], [1], {
			seed: 9,
			iterations: 4000,
		});
		// The nominal #1 should still lose the single spot a meaningful share of
		// the time in a near-tied field - noise is scaled to the field's own
		// spread, so tiny real gaps never collapse to a near-certain outcome.
		assert.ok(probs[0]![0]! < 0.95, `nominal leader P(tier1)=${probs[0]![0]}`);
	});
	test("deterministic per seed", () => {
		const a = tierMembershipProbs([50, 40, 30], [1, 1], { seed: 11 });
		const b = tierMembershipProbs([50, 40, 30], [1, 1], { seed: 11 });
		assert.deepStrictEqual(a, b);
	});
	test("a bigger noiseFactor makes the field more competitive (leader less certain)", () => {
		const scores = [100, 80, 60, 40];
		const low = tierMembershipProbs(scores, [1], {
			seed: 7,
			iterations: 4000,
			noiseFactor: 0.6,
		});
		const high = tierMembershipProbs(scores, [1], {
			seed: 7,
			iterations: 4000,
			noiseFactor: 1.4,
		});
		// More noise → the nominal leader wins the single spot less often.
		assert.ok(
			high[0]![0]! < low[0]![0]!,
			`high=${high[0]![0]} should be < low=${low[0]![0]}`,
		);
	});
	test("omitting noiseFactor matches the historical 0.6 default", () => {
		const scores = [90, 70, 55, 30];
		const withDefault = tierMembershipProbs(scores, [1, 1], { seed: 4 });
		const explicit = tierMembershipProbs(scores, [1, 1], {
			seed: 4,
			noiseFactor: 0.6,
		});
		assert.deepStrictEqual(withDefault, explicit);
	});
	test("empty field or no tiers returns all zeros, one row per candidate", () => {
		assert.deepStrictEqual(tierMembershipProbs([], [1, 1]), []);
		const probs = tierMembershipProbs([10, 20], []);
		assert.deepStrictEqual(probs, [[], []]);
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

describe("overProbFromSigma", () => {
	test("at the mean it's a coin flip", () => {
		assert.ok(Math.abs(overProbFromSigma(20, 20, 4) - 0.5) < 1e-6);
	});
	test("a lower line is more likely to go over; a higher line less likely", () => {
		assert.ok(overProbFromSigma(20, 15, 4) > 0.5);
		assert.ok(overProbFromSigma(20, 25, 4) < 0.5);
	});
	test("a tighter sigma makes the same line's probability more extreme", () => {
		const wide = overProbFromSigma(20, 25, 8);
		const tight = overProbFromSigma(20, 25, 2);
		assert.ok(tight < wide, `tight=${tight} wide=${wide}`);
	});
	test("overProb(expectedTotal, line) matches its own 9%-of-mean sigma", () => {
		const direct = overProb(220, 220);
		const viaSigma = overProbFromSigma(220, 220, 220 * 0.09);
		assert.ok(Math.abs(direct - viaSigma) < 1e-9);
	});
});

describe("combineIndependentSigmas", () => {
	test("combines via sqrt of sum of squares (never exceeds the naive sum)", () => {
		const combined = combineIndependentSigmas([3, 4]);
		assert.ok(Math.abs(combined - 5) < 1e-9); // 3-4-5 triangle
		assert.ok(combined < 3 + 4);
	});
	test("a single sigma is unchanged", () => {
		assert.ok(Math.abs(combineIndependentSigmas([7]) - 7) < 1e-9);
	});
	test("empty input is 0", () => {
		assert.strictEqual(combineIndependentSigmas([]), 0);
	});
});

describe("probNear", () => {
	test("an even matchup (mean 0) has the highest near-zero density", () => {
		const even = probNear(0, 13, 2);
		const lopsided = probNear(20, 13, 2);
		assert.ok(even > lopsided, `even=${even} lopsided=${lopsided}`);
	});
	test("a wider band always contains at least as much probability", () => {
		const narrow = probNear(0, 13, 1);
		const wide = probNear(0, 13, 5);
		assert.ok(wide > narrow);
	});
	test("stays a valid probability", () => {
		for (const mean of [-30, -5, 0, 5, 30]) {
			const p = probNear(mean, 13, 2);
			assert.ok(p >= 0 && p <= 1, `mean=${mean} p=${p}`);
		}
	});
});

describe("milestoneProb", () => {
	test("a player projected well above the threshold in every category is a near-lock for the double-double", () => {
		const cats = [
			{ mean: 25, sigma: 5 },
			{ mean: 12, sigma: 3 },
			{ mean: 11, sigma: 3 },
			{ mean: 1.5, sigma: 1 },
			{ mean: 0.8, sigma: 0.6 },
		];
		const p = milestoneProb(cats, 10, 2, { seed: 1 });
		assert.ok(p > 0.85, `p=${p}`);
	});
	test("a player nowhere near the threshold in any category is a near-zero for the double-double", () => {
		const cats = [
			{ mean: 8, sigma: 3 },
			{ mean: 2, sigma: 1.2 },
			{ mean: 3, sigma: 1.2 },
			{ mean: 0.5, sigma: 0.6 },
			{ mean: 0.3, sigma: 0.6 },
		];
		const p = milestoneProb(cats, 10, 2, { seed: 1 });
		assert.ok(p < 0.1, `p=${p}`);
	});
	test("triple-double (need 3) is always <= double-double (need 2) probability for the same player", () => {
		const cats = [
			{ mean: 18, sigma: 5 },
			{ mean: 10, sigma: 3 },
			{ mean: 9, sigma: 3 },
			{ mean: 1.5, sigma: 1 },
			{ mean: 1, sigma: 0.8 },
		];
		const dd = milestoneProb(cats, 10, 2, { seed: 2 });
		const td = milestoneProb(cats, 10, 3, { seed: 2 });
		assert.ok(td <= dd, `dd=${dd} td=${td}`);
	});
	test("needing more categories than exist is impossible", () => {
		const cats = [{ mean: 20, sigma: 3 }];
		assert.strictEqual(milestoneProb(cats, 10, 2, { seed: 1 }), 0);
	});
	test("deterministic per seed", () => {
		const cats = [
			{ mean: 15, sigma: 4 },
			{ mean: 8, sigma: 2 },
		];
		const a = milestoneProb(cats, 10, 2, { seed: 9 });
		const b = milestoneProb(cats, 10, 2, { seed: 9 });
		assert.strictEqual(a, b);
	});
});
