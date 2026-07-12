import { assert, describe, test } from "vitest";
import {
	enteringAverages,
	regularSeasonRecordAsOf,
	selectRecapGames,
	seriesWinsBefore,
	type PlayerGameLine,
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
		assert.deepEqual(regularSeasonRecordAsOf(0, 99, games), {
			won: 2,
			lost: 1,
		});
	});
});

describe("selectRecapGames", () => {
	const g = (gid: number, day: number, note?: string) => ({ gid, day, note });

	test("sweeps in unrecapped games from missed days alongside the viewed day", () => {
		const completed = [
			g(1, 1, "recapped"),
			g(2, 2), // missed
			g(3, 3), // missed
			g(4, 4),
			g(5, 4),
		];
		const picked = selectRecapGames(completed, 4, 45);
		assert.deepEqual(
			picked.map((x) => x.gid),
			[2, 3, 4, 5],
		);
	});

	test("games that already have a note are not re-copied (except the viewed day)", () => {
		const completed = [g(1, 1, "done"), g(2, 2, "done"), g(3, 3)];
		const picked = selectRecapGames(completed, 3, 45);
		assert.deepEqual(
			picked.map((x) => x.gid),
			[3],
		);
	});

	test("the viewed day is always included in full, even if already recapped", () => {
		const completed = [g(1, 2, "done"), g(2, 2)];
		const picked = selectRecapGames(completed, 2, 45);
		assert.deepEqual(
			picked.map((x) => x.gid),
			[1, 2],
		);
	});

	test("the cap drops swept games, never the viewed day, and output stays chronological", () => {
		const completed = [g(1, 1), g(2, 2), g(3, 3), g(4, 4), g(5, 4)];
		const picked = selectRecapGames(completed, 4, 3);
		// Day 4's two games survive; only one older game fits under the cap.
		assert.deepEqual(
			picked.map((x) => x.gid),
			[1, 4, 5],
		);
	});

	test("unrecapped games from days AFTER the viewed day are swept too", () => {
		const completed = [g(1, 2), g(2, 3)];
		const picked = selectRecapGames(completed, 2, 45);
		assert.deepEqual(
			picked.map((x) => x.gid),
			[1, 2],
		);
	});
});

describe("enteringAverages", () => {
	const line = (
		day: number,
		gid: number,
		pts: number,
		playoffs = false,
	): PlayerGameLine => ({
		day,
		gid,
		playoffs,
		row: { gp: 1, min: 30, pts, fg: 5, fga: 10, orb: 1, drb: 4, ast: 3 },
	});

	test("excludes the game being recapped: a 16 ppg scorer who drops 46 still shows 16", () => {
		// Two games at 16, then this game (gid 3) is a 46-point eruption. The live
		// season average is 26 - but he came IN averaging 16, and that's what the
		// recap data must say.
		const lines = [line(1, 1, 16), line(2, 2, 16), line(3, 3, 46)];
		const before = enteringAverages(lines, 3, 3, false);
		assert.strictEqual(before?.pts, 16);
		assert.strictEqual(before?.gp, 2);
	});

	test("excludes games AFTER the recapped game (recapping a past day)", () => {
		const lines = [line(1, 1, 10), line(2, 2, 20), line(3, 3, 30)];
		// Recapping game 2: only game 1 counts.
		const before = enteringAverages(lines, 2, 2, false);
		assert.strictEqual(before?.pts, 10);
		assert.strictEqual(before?.gp, 1);
	});

	test("a player's first game has no entering averages", () => {
		assert.strictEqual(enteringAverages([line(1, 1, 25)], 1, 1, false), undefined);
	});

	test("playoff and regular-season lines don't mix", () => {
		const lines = [line(1, 1, 10), line(2, 2, 12), line(50, 9, 30, true)];
		const playoffBefore = enteringAverages(lines, 10, 51, true);
		assert.strictEqual(playoffBefore?.pts, 30);
		assert.strictEqual(playoffBefore?.gp, 1);
		const regularEntering = enteringAverages(lines, 10, 51, false);
		assert.strictEqual(regularEntering?.gp, 2);
	});

	test("same-day games are ordered by gid", () => {
		const lines = [line(1, 1, 10), line(1, 2, 20)];
		const before = enteringAverages(lines, 2, 1, false);
		assert.strictEqual(before?.pts, 10);
		assert.strictEqual(before?.gp, 1);
	});
});
