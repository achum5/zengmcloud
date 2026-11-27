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

test(
	"one champion, two finalists and sixteen playoff teams a season",
	{ timeout: 60_000 },
	() => {
		for (const playIn of [true, false]) {
			const results = run(league(), 1, playIn);
			assert.closeTo(sum(results, "title"), 1, 1e-9);
			assert.closeTo(sum(results, "finals"), 2, 1e-9);
			assert.closeTo(sum(results, "playoffs"), 16, 1e-9);
		}
	},
);

test("the same seed gives the same odds", { timeout: 60_000 }, () => {
	const a = run(league(), 5);
	const b = run(league(), 5);
	for (const [tid, r] of a) {
		assert.deepEqual(b.get(tid), r);
	}
});

test("better teams have better chances", { timeout: 60_000 }, () => {
	const results = run(league());
	const best = results.get(29)!;
	const worst = results.get(0)!;
	const middle = results.get(15)!;
	assert.isAbove(best.title, middle.title);
	assert.isAbove(middle.playoffs, worst.playoffs);
	assert.isAbove(best.title, 0.15);
	assert.isBelow(worst.playoffs, 0.05);
});

test("games already won count", { timeout: 60_000 }, () => {
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

test(
	"a team stronger by the playoffs wins more titles, not more seeds",
	{ timeout: 60_000 },
	() => {
		const base = run(league());
		const boosted = run(
			league().map((t) =>
				t.tid === 25 ? { ...t, playoffStrength: t.strength + 4 } : t,
			),
		);
		assert.isAbove(boosted.get(25)!.title, base.get(25)!.title + 0.03);
		assert.closeTo(boosted.get(25)!.playoffs, base.get(25)!.playoffs, 0.02);
	},
);

test(
	"a team's chances with a shift, read off the same seasons, are its chances played again that much stronger",
	{ timeout: 120_000 },
	() => {
		const shifts = [-1, 1];
		const teams = league();
		const read = simulateSeason({
			teams,
			numGamesPlayoffSeries: [7, 7, 7, 7],
			playIn: true,
			byConference: true,
			uncertainty: 2,
			iterations: 4000,
			seed: 3,
			shifts,
		});
		// The best team and one in the middle.
		for (const tid of [29, 20]) {
			for (const [k, d] of shifts.entries()) {
				const played = simulateSeason({
					teams: league(
						Object.assign([], {
							[tid]: { strength: teams[tid]!.strength + d },
						}),
					),
					numGamesPlayoffSeries: [7, 7, 7, 7],
					playIn: true,
					byConference: true,
					uncertainty: 2,
					iterations: 20000,
					seed: 4,
				}).get(tid)!;
				const shifted = read.get(tid)!.shifted![k]!;
				assert.closeTo(shifted.at(-1)!, played.title, 0.03);
				assert.closeTo(1 - shifted[0]!, played.playoffs, 0.03);
			}
		}
		// Without shifts, the same seasons as before.
		const plain = simulateSeason({
			teams,
			numGamesPlayoffSeries: [7, 7, 7, 7],
			playIn: true,
			byConference: true,
			uncertainty: 2,
			iterations: 4000,
			seed: 3,
		});
		for (const [tid, r] of plain) {
			assert.strictEqual(r.title, read.get(tid)!.title);
			assert.isUndefined(r.shifted);
		}
	},
);

test(
	"with playoff games more decisive, the best teams win more titles and who makes the playoffs barely moves",
	{ timeout: 60_000 },
	() => {
		const teams = league();
		const base = run(teams, 5);
		const decisive = simulateSeason({
			teams,
			numGamesPlayoffSeries: [7, 7, 7, 7],
			playIn: true,
			byConference: true,
			uncertainty: 2,
			iterations: 2000,
			seed: 5,
			playoffScale: 1.58,
		});
		assert.isAbove(decisive.get(29)!.title, base.get(29)!.title + 0.03);
		assert.isBelow(decisive.get(20)!.title, base.get(20)!.title);
		assert.closeTo(sum(decisive, "title"), 1, 1e-9);
		for (const tid of [29, 20, 10]) {
			assert.closeTo(
				decisive.get(tid)!.playoffs,
				base.get(tid)!.playoffs,
				0.03,
			);
		}
	},
);
