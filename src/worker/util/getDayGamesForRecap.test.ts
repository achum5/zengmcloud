import { assert, describe, test } from "vitest";
import {
	regularSeasonRecordAsOf,
	seriesWinsBefore,
} from "./getDayGamesForRecap.ts";

// gid, day, playoffs, home tid+result. Winner is whoever has more pts.
const game = (
	gid: number,
	day: number,
	homeTid: number,
	awayTid: number,
	homePts: number,
	awayPts: number,
	playoffs = false,
) => {
	const [won, lost] =
		homePts >= awayPts
			? [{ tid: homeTid }, { tid: awayTid }]
			: [{ tid: awayTid }, { tid: homeTid }];
	return {
		gid,
		day,
		playoffs,
		teams: [{ tid: homeTid }, { tid: awayTid }],
		won,
		lost,
	};
};

describe("seriesWinsBefore", () => {
	// A best-of-7 between tid 0 (home) and tid 1, games on consecutive gids.
	// tid 0 won gid 10, tid 1 won gid 11, tid 0 won gid 12.
	const series = [
		game(10, 100, 0, 1, 110, 100, true),
		game(11, 101, 0, 1, 95, 105, true),
		game(12, 102, 0, 1, 120, 118, true),
	];
	const gids = [10, 11, 12];

	test("entering game 1 the series is 0-0 (no games counted before it)", () => {
		const { homeWon, awayWon } = seriesWinsBefore(10, 0, 1, gids, series);
		assert.strictEqual(homeWon, 0);
		assert.strictEqual(awayWon, 0);
	});

	test("entering game 2, only game 1's result counts (home leads 1-0)", () => {
		const { homeWon, awayWon } = seriesWinsBefore(11, 0, 1, gids, series);
		assert.strictEqual(homeWon, 1);
		assert.strictEqual(awayWon, 0);
	});

	test("entering game 3, both earlier games count (tied 1-1)", () => {
		const { homeWon, awayWon } = seriesWinsBefore(12, 0, 1, gids, series);
		assert.strictEqual(homeWon, 1);
		assert.strictEqual(awayWon, 1);
	});

	test("later games in the series are never counted toward an earlier game", () => {
		// Even though game 12 exists (and home won it), it must not inflate the
		// pre-game-1 record.
		const { homeWon, awayWon } = seriesWinsBefore(10, 0, 1, gids, series);
		assert.strictEqual(homeWon, 0);
		assert.strictEqual(awayWon, 0);
	});

	test("falls back to head-to-head playoff games when gids are absent", () => {
		const { homeWon, awayWon } = seriesWinsBefore(12, 0, 1, undefined, series);
		assert.strictEqual(homeWon, 1);
		assert.strictEqual(awayWon, 1);
	});

	test("head-to-head fallback ignores regular-season meetings between the teams", () => {
		const withRegularSeason = [
			game(5, 10, 0, 1, 100, 90, false), // regular-season win for tid 0
			...series,
		];
		const { homeWon, awayWon } = seriesWinsBefore(
			12,
			0,
			1,
			undefined,
			withRegularSeason,
		);
		// The regular-season game (gid 5) must NOT be counted in the series score.
		assert.strictEqual(homeWon, 1);
		assert.strictEqual(awayWon, 1);
	});
});

describe("regularSeasonRecordAsOf", () => {
	const games = [
		game(1, 1, 0, 1, 100, 90), // tid 0 W
		game(2, 2, 0, 2, 80, 95), // tid 0 L
		game(3, 3, 3, 0, 88, 99), // tid 0 W (away)
		game(4, 4, 0, 1, 101, 100, true), // playoff game - excluded
	];

	test("counts wins and losses through the given day (inclusive)", () => {
		assert.deepEqual(regularSeasonRecordAsOf(0, 2, games), { won: 1, lost: 1 });
	});

	test("includes a game played ON the cutoff day", () => {
		assert.deepEqual(regularSeasonRecordAsOf(0, 3, games), { won: 2, lost: 1 });
	});

	test("excludes future days", () => {
		assert.deepEqual(regularSeasonRecordAsOf(0, 1, games), { won: 1, lost: 0 });
	});

	test("never counts playoff games in the regular-season record", () => {
		// Day 4 includes the playoff game, but the record stays at the day-3 total.
		assert.deepEqual(regularSeasonRecordAsOf(0, 99, games), { won: 2, lost: 1 });
	});
});
