import { assert, test } from "vitest";
import {
	simulateSeason,
	winProbability,
	type SeasonSimTeam,
} from "./seasonSim.ts";

// Thirty teams in two conferences, strengths spread evenly from -8 to +8.
const league = (overrides: Partial<SeasonSimTeam>[] = []): SeasonSimTeam[] =>
	Array.from({ length: 30 }, (_, tid) => ({
		tid,
		cid: tid % 2,
		strength: -8 + (16 * tid) / 29,
		won: 0,
		lost: 0,
		gamesRemaining: 82,
		...overrides[tid],
	}));

const run = (teams: SeasonSimTeam[], seed = 1, playIn = true) =>
	simulateSeason({
		teams,
		numGamesPlayoffSeries: [7, 7, 7, 7],
		playIn,
		byConference: true,
		uncertainty: 2,
		iterations: 2000,
		seed,
	});

const sum = (
	results: ReturnType<typeof run>,
	key: "title" | "finals" | "playoffs",
) => [...results.values()].reduce((a, r) => a + r[key], 0);

test("an even game is a coin flip, and a better team is favored", () => {
	assert.closeTo(winProbability(0), 0.5, 1e-7);
	assert.closeTo(winProbability(5) + winProbability(-5), 1, 1e-7);
	assert.isAbove(winProbability(5), 0.6);
	assert.isBelow(winProbability(5), 0.7);
});

test("one champion, two finalists and sixteen playoff teams a season", () => {
	for (const playIn of [true, false]) {
		const results = run(league(), 1, playIn);
		assert.closeTo(sum(results, "title"), 1, 1e-9);
		assert.closeTo(sum(results, "finals"), 2, 1e-9);
		assert.closeTo(sum(results, "playoffs"), 16, 1e-9);
	}
});

test("the same seed gives the same odds", () => {
	const a = run(league(), 5);
	const b = run(league(), 5);
	for (const [tid, r] of a) {
		assert.deepEqual(b.get(tid), r);
	}
});

test("better teams have better chances", () => {
	const results = run(league());
	const best = results.get(29)!;
	const worst = results.get(0)!;
	const middle = results.get(15)!;
	assert.isAbove(best.title, middle.title);
	assert.isAbove(middle.playoffs, worst.playoffs);
	assert.isAbove(best.title, 0.15);
	assert.isBelow(worst.playoffs, 0.05);
});

test("games already won count", () => {
	// Equal teams, but one has started 40-0 and the season is half over.
	const teams = league().map((t) => ({
		...t,
		strength: 0,
		won: t.tid === 3 ? 41 : 20,
		lost: t.tid === 3 ? 0 : 21,
		gamesRemaining: 41,
	}));
	const results = run(teams);
	assert.isAbove(results.get(3)!.playoffs, 0.99);
	assert.isBelow(results.get(4)!.playoffs, 0.75);
});

test("a team stronger by the playoffs wins more titles, not more seeds", () => {
	const base = run(league());
	const boosted = run(
		league().map((t) =>
			t.tid === 25 ? { ...t, playoffStrength: t.strength + 4 } : t,
		),
	);
	assert.isAbove(boosted.get(25)!.title, base.get(25)!.title + 0.03);
	assert.closeTo(boosted.get(25)!.playoffs, base.get(25)!.playoffs, 0.02);
});
