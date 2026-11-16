import { assert, describe, test } from "vitest";
import {
	franchiseStreak,
	rankBy,
	seasonShape,
	type TeamGameResult,
} from "./seasonRecapStory.ts";

const game = (won: boolean, pts: number, oppPts: number, opp = "BOS") =>
	({ won, pts, oppPts, opp }) satisfies TeamGameResult;

describe("seasonShape", () => {
	// 3-6, then 9-0, then 3-6: a season that was decided in the middle.
	const games: TeamGameResult[] = [
		...Array.from({ length: 9 }, (_, i) =>
			i < 3 ? game(true, 110, 100) : game(false, 95, 105),
		),
		...Array.from({ length: 9 }, () => game(true, 120, 101)),
		...Array.from({ length: 9 }, (_, i) =>
			i < 3 ? game(true, 101, 99) : game(false, 99, 101),
		),
	];

	test("cuts the season into thirds", () => {
		const shape = seasonShape(games)!;
		assert.deepStrictEqual(
			shape.stretches.map((x) => `${x.label} ${x.won}-${x.lost}`),
			["games 1-9 3-6", "games 10-18 9-0", "games 19-27 3-6"],
		);
	});

	test("finds the streaks, close games, extremes and last ten", () => {
		const shape = seasonShape(games)!;
		// 9 straight in the middle, plus the 3 wins that open the last third.
		assert.strictEqual(shape.longestWinStreak, 12);
		assert.strictEqual(shape.longestLosingStreak, 6);
		assert.deepStrictEqual(shape.close, { won: 3, lost: 6 });
		assert.strictEqual(shape.biggestWin, "120-101 vs BOS");
		assert.strictEqual(shape.worstLoss, "95-105 vs BOS");
		assert.deepStrictEqual(shape.lastTen, { won: 4, lost: 6 });
	});

	test("says nothing about a season too short to have a shape", () => {
		assert.strictEqual(seasonShape(games.slice(0, 5)), undefined);
	});
});

describe("franchiseStreak", () => {
	const history = (results: number[], from = 2000) =>
		results.map((playoffRoundsWon, i) => ({
			season: from + i,
			playoffRoundsWon,
		}));

	test("a drought ended", () => {
		assert.deepStrictEqual(
			franchiseStreak(history([1, -1, -1, -1, -1, 0]), 2005, 4),
			["first playoff appearance since 2000"],
		);
	});

	test("a streak of appearances and a streak of misses", () => {
		assert.deepStrictEqual(franchiseStreak(history([0, 1, 2, 0]), 2003, 4), [
			"4th straight playoff appearance",
		]);
		assert.deepStrictEqual(franchiseStreak(history([0, -1, -1, -1]), 2003, 4), [
			"missed the playoffs for the 3rd straight season",
		]);
	});

	test("titles: a first, a repeat, and one after a long wait", () => {
		assert.deepStrictEqual(franchiseStreak(history([-1, 0, 4]), 2002, 4), [
			"first championship in franchise history",
		]);
		assert.deepStrictEqual(franchiseStreak(history([1, 4, 4]), 2002, 4), [
			"3rd straight playoff appearance",
			"2nd straight championship",
		]);
		assert.deepStrictEqual(
			franchiseStreak(history([4, -1, -1, -1, -1, -1, 4]), 2006, 4),
			["first playoff appearance since 2000", "first championship since 2000"],
		);
	});

	// The owner's league, Cleveland 2023: eight straight trips to at least the
	// conference finals and five titles in ten seasons, counted over the whole
	// history - shown only the last few seasons, the AI said "seven" and "a
	// third title in seven seasons".
	test("deep runs and a title count over the whole history", () => {
		assert.deepStrictEqual(
			franchiseStreak(
				history([4, 1, 4, 4, 2, 2, 3, 2, 4, 4], 2014),
				2023,
				4,
				true,
			),
			[
				"10th straight playoff appearance",
				"8th straight season reaching at least the conference finals (every season since 2016)",
				"5th championship in 10 seasons (2014, 2016, 2017, 2022, 2023)",
				"2nd straight championship",
			],
		);
		// Without conferences, the round is the semifinals; a finals run
		// longer than the title run is named too.
		assert.deepStrictEqual(
			franchiseStreak(history([2, 3, 3, 4], 2000), 2003, 4),
			[
				"4th straight playoff appearance",
				"4th straight season reaching at least the semifinals (every season since 2000)",
				"3rd straight trip to the finals (every season since 2001)",
				"first championship in franchise history",
			],
		);
	});

	test("nothing to say is said as nothing", () => {
		assert.deepStrictEqual(franchiseStreak(history([0, -1, 1]), 2002, 4), []);
		// A season that isn't in the history at all.
		assert.deepStrictEqual(franchiseStreak(history([0, 1]), 2005, 4), []);
	});
});

describe("rankBy", () => {
	test("best first, ties share a rank, and lower-is-better works", () => {
		const items = [{ v: 10 }, { v: 30 }, { v: 30 }, { v: 5 }];
		const ranks = rankBy(items, (x) => x.v);
		assert.deepStrictEqual(
			items.map((x) => ranks.get(x)),
			[3, 1, 1, 4],
		);
		const low = rankBy(items, (x) => x.v, false);
		assert.deepStrictEqual(
			items.map((x) => low.get(x)),
			[2, 3, 3, 1],
		);
	});
});
