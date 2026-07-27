import { assert, describe, test } from "vitest";
import {
	awardWinProbs,
	projectedGamesPlayed,
	talentWeight,
} from "./awardOdds.ts";

describe("projectedGamesPlayed", () => {
	test("a healthy player who has played every game projects to the full season", () => {
		assert.strictEqual(
			projectedGamesPlayed({ gp: 10, teamGp: 10, numGames: 82 }),
			82,
		);
	});

	test("missing four games early costs four games, not 40% of the year", () => {
		// This is the whole bug: at game 10 a player who missed 4 has 60% of a
		// peer's cumulative production, and the old odds treated that as a settled
		// fact about the season.
		const projected = projectedGamesPlayed({
			gp: 6,
			teamGp: 10,
			numGames: 82,
		});
		// He keeps missing at the same rate, so he lands well above 60% of 82.
		assert.ok(projected > 50, `projected ${projected}`);
		assert.ok(projected < 82);
	});

	test("a current injury is subtracted from what's left", () => {
		const healthy = projectedGamesPlayed({ gp: 40, teamGp: 40, numGames: 82 });
		const hurt = projectedGamesPlayed({
			gp: 40,
			teamGp: 40,
			numGames: 82,
			injuryGamesRemaining: 20,
		});
		assert.strictEqual(healthy, 82);
		assert.strictEqual(hurt, 62);
	});

	test("an injury longer than the season left doesn't project negative games", () => {
		const projected = projectedGamesPlayed({
			gp: 70,
			teamGp: 78,
			numGames: 82,
			injuryGamesRemaining: 200,
		});
		assert.strictEqual(projected, 70);
	});

	test("nobody has played yet", () => {
		assert.strictEqual(
			projectedGamesPlayed({ gp: 0, teamGp: 0, numGames: 82 }),
			82,
		);
	});

	test("a finished season projects exactly what was played", () => {
		assert.strictEqual(
			projectedGamesPlayed({ gp: 74, teamGp: 82, numGames: 82 }),
			74,
		);
	});
});

describe("talentWeight", () => {
	test("talent carries the ranking before anything is played and is gone by the end", () => {
		assert.ok(talentWeight(0) > 0.5);
		assert.strictEqual(talentWeight(1), 0);
	});

	test("it falls off fast, not linearly", () => {
		// Gone well before the halfway mark rather than propping a name up all year.
		assert.ok(talentWeight(0.5) < 0.25);
		assert.ok(talentWeight(0.25) < talentWeight(0.1));
	});
});

describe("awardWinProbs", () => {
	const opts = (fractionComplete: number) => ({
		fractionComplete,
		seed: "test",
	});

	test("probabilities sum to one", () => {
		const probs = awardWinProbs(
			[
				{ score: 10, talent: 60 },
				{ score: 8, talent: 55 },
				{ score: 6, talent: 50 },
			],
			opts(0.5),
		);
		assert.ok(Math.abs(probs.reduce((a, b) => a + b, 0) - 1) < 1e-9);
	});

	test("being the most talented in the field is worth something early", () => {
		// Scoring is held fixed and only the talent is moved, so this isolates the
		// prior. The projection is what actually rescues a player who missed games;
		// talent is the nudge on top of it while the sample is still small.
		const scores = [5, 4.9, 4.8, 4.6];
		const asWorst = awardWinProbs(
			scores.map((score, i) => ({ score, talent: [61, 57, 55, 40][i]! })),
			opts(0.1),
		);
		const asBest = awardWinProbs(
			scores.map((score, i) => ({ score, talent: [61, 57, 55, 90][i]! })),
			opts(0.1),
		);
		assert.ok(asBest[3]! > asWorst[3]!, `${asWorst[3]} -> ${asBest[3]}`);
	});

	test("talent stops mattering once the season is mostly played", () => {
		const scores = [5, 4.9, 4.8, 4.6];
		const late = (talent: number) =>
			awardWinProbs(
				scores.map((score, i) => ({ score, talent: [61, 57, 55, talent][i]! })),
				opts(0.85),
			)[3]!;
		// Same swing that moved the odds at 10% barely registers at 85%.
		assert.ok(late(90) - late(40) < 0.05);
	});

	test("early odds are close together, late odds are not", () => {
		const field = [
			{ score: 10, talent: 60 },
			{ score: 8, talent: 58 },
			{ score: 6, talent: 55 },
		];
		const early = awardWinProbs(field, opts(0.05));
		const late = awardWinProbs(field, opts(0.95));
		// The leader is far more certain in April than in October.
		assert.ok(late[0]! > early[0]!, `${early[0]} -> ${late[0]}`);
		assert.ok(early[0]! < 0.85);
		assert.ok(late[0]! > 0.9);
	});

	test("the back of the field is never priced as impossible mid-season", () => {
		// Betting the whole back half of every race used to be close to free money,
		// because a candidate who never won a sample was priced at the +30000 cap.
		const field = [10, 9.4, 8.6, 7.2, 6.5, 5.9, 5.1, 4.4].map((score, i) => ({
			score,
			talent: 60 - i,
		}));
		const probs = awardWinProbs(field, opts(0.4));
		for (const [i, prob] of probs.entries()) {
			assert.ok(prob > 0.01, `candidate ${i} priced at ${prob}`);
		}
	});

	test("but by the end of the season a runaway leader is a lock", () => {
		// The floor exists because the season can still turn. Once it can't, it has
		// to get out of the way.
		const probs = awardWinProbs(
			[
				{ score: 20, talent: 60 },
				{ score: 5, talent: 55 },
				{ score: 4, talent: 50 },
			],
			opts(0.99),
		);
		assert.ok(probs[0]! > 0.95, `leader at ${probs[0]}`);
	});

	test("a finished season gives the leader the award outright", () => {
		const probs = awardWinProbs(
			[
				{ score: 10, talent: 50 },
				{ score: 11, talent: 90 },
			],
			opts(1),
		);
		assert.deepStrictEqual(probs, [0, 1]);
	});

	test("stats win out once enough of the season is played", () => {
		// Same field as the talent case, but late: the talented player who never
		// produced no longer gets carried.
		const probs = awardWinProbs(
			[
				{ score: 10, talent: 50 },
				{ score: 4, talent: 90 },
			],
			opts(0.9),
		);
		assert.ok(probs[0]! > probs[1]!);
	});

	test("identical candidates split evenly", () => {
		const probs = awardWinProbs(
			[
				{ score: 5, talent: 60 },
				{ score: 5, talent: 60 },
			],
			opts(0.5),
		);
		assert.ok(Math.abs(probs[0]! - 0.5) < 0.05, probs.join(", "));
	});

	test("the same inputs always give the same odds", () => {
		// Odds that flicker on every re-render read as broken.
		const field = [
			{ score: 9, talent: 60 },
			{ score: 7, talent: 65 },
		];
		assert.deepStrictEqual(
			awardWinProbs(field, opts(0.3)),
			awardWinProbs(field, opts(0.3)),
		);
	});

	test("degenerate fields don't crash", () => {
		assert.deepStrictEqual(awardWinProbs([], opts(0.5)), []);
		assert.deepStrictEqual(
			awardWinProbs([{ score: 3, talent: 1 }], opts(0)),
			[1],
		);
		const zeros = awardWinProbs(
			[
				{ score: 0, talent: 0 },
				{ score: 0, talent: 0 },
			],
			opts(0),
		);
		assert.ok(Math.abs(zeros.reduce((a, b) => a + b, 0) - 1) < 1e-9);
	});
});
