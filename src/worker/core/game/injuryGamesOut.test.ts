import { assert, describe, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { daysAndGamesText } from "../../../common/gamesOut.ts";
import type { Game, PlayoffSeries } from "../../../common/types.ts";
import { calendarClock, daysOut, ticksBetween } from "./calendarTicks.ts";
import {
	countGamesOut,
	estimateGamesOut,
	playoffGamesByTid,
	regularSeason,
} from "./injuryGamesOut.ts";
import { countFrom, daysLeft } from "./refreshCalendarDays.ts";

const row = (homeTid: number, awayTid: number, date: string) => ({
	gid: 0,
	day: 0,
	homeTid,
	awayTid,
	date,
});

describe("countGamesOut", () => {
	// Out 9 days after Nov 13: through Nov 22.
	const team = ["2025-11-14", "2025-11-16", "2025-11-18", "2025-11-21"];

	test("the games on his team's dates through his last day out", () => {
		assert.deepStrictEqual(
			countGamesOut("2025-11-13", 9, {
				sure: [...team, "2025-11-22", "2025-11-23"],
				openAfter: "2026-04-12",
			}),
			{ n: 5 },
		);
	});

	test("none when he's back before his team plays again", () => {
		assert.deepStrictEqual(
			countGamesOut("2025-11-13", 1, {
				sure: ["2025-11-16"],
				openAfter: "2026-04-12",
			}),
			{ n: 0 },
		);
	});

	test("past the last night, more may come", () => {
		assert.deepStrictEqual(
			countGamesOut("2026-04-08", 9, {
				sure: ["2026-04-09", "2026-04-11"],
				openAfter: "2026-04-12",
			}),
			{ n: 2, more: true },
		);
		// Nothing known to count.
		assert.isUndefined(
			countGamesOut("2026-04-12", 9, { sure: [], openAfter: "2026-04-12" }),
		);
	});

	test("an if-necessary game in his days", () => {
		assert.deepStrictEqual(
			countGamesOut("2026-04-25", 4, {
				sure: ["2026-04-26"],
				maybe: ["2026-04-28", "2026-04-30"],
				openAfter: "2026-05-02",
			}),
			{ n: 1, more: true },
		);
	});

	test("reads as days, then games", () => {
		assert.strictEqual(daysAndGamesText(9, { n: 5 }), "9 days (5 games)");
		assert.strictEqual(daysAndGamesText(1, { n: 1 }), "1 day (1 game)");
		assert.strictEqual(
			daysAndGamesText(30, { n: 12, more: true }),
			"30 days (12+ games)",
		);
		assert.strictEqual(
			daysAndGamesText(40, { n: 19, about: true }),
			"40 days (~19 games)",
		);
		assert.strictEqual(daysAndGamesText(4), "4 days");
	});
});

describe("regularSeason", () => {
	test("counts each team's own games after the night played", () => {
		const count = regularSeason(
			[
				row(0, 1, "2025-11-13"),
				row(0, 2, "2025-11-14"),
				row(-4, -4, "2025-11-15"),
				row(3, 0, "2025-11-16"),
				row(1, 2, "2025-11-16"),
				row(0, 1, "2025-11-20"),
			],
			"2025-11-13",
		);
		assert.deepStrictEqual(count(0, 3), { n: 2 });
		assert.deepStrictEqual(count(1, 3), { n: 1 });
		assert.deepStrictEqual(count(3, 7), { n: 1 });
		assert.deepStrictEqual(count(0, 30), { n: 3, more: true });
		// A free agent, and a healthy player.
		assert.isUndefined(count(-1, 3));
		assert.isUndefined(count(0, undefined));
	});
});

describe("playoffGamesByTid", () => {
	const team = (tid: number, won: number) => ({
		tid,
		won,
		seed: tid,
		cid: 0,
		pts: 0,
	});
	const dates = [
		"2026-04-19",
		"2026-04-21",
		"2026-04-24",
		"2026-04-26",
		"2026-04-28",
		"2026-04-30",
		"2026-05-02",
	];

	test("a series' sure games and if-necessary ones", () => {
		const byTid = playoffGamesByTid(
			{
				currentRound: 0,
				series: [
					[
						{ home: team(0, 2), away: team(1, 1), dates },
						{ home: team(2, 4), away: team(3, 0), dates },
					],
				],
			} as unknown as PlayoffSeries,
			() => 4,
		);
		// 2-1: Games 4 and 5 will be played, 6 and 7 maybe.
		assert.deepStrictEqual(byTid.get(1), {
			sure: ["2026-04-26", "2026-04-28"],
			maybe: ["2026-04-30", "2026-05-02"],
			openAfter: "2026-05-02",
		});
		// Out through Game 4: 1. Through Game 6: 2, and more maybe.
		assert.deepStrictEqual(countGamesOut("2026-04-24", 2, byTid.get(1)!), {
			n: 1,
		});
		assert.deepStrictEqual(countGamesOut("2026-04-24", 6, byTid.get(1)!), {
			n: 2,
			more: true,
		});
		// A swept team is done; the winner waits on the next round.
		assert.isFalse(byTid.has(2));
		assert.isFalse(byTid.has(3));
	});
});

describe("estimateGamesOut", () => {
	test("at the season's pace, before its schedule is made", () => {
		assert.deepStrictEqual(estimateGamesOut(40, 82, 170), {
			n: 19,
			about: true,
		});
		assert.deepStrictEqual(estimateGamesOut(200, 82, 170), {
			n: 82,
			more: true,
		});
	});
});

describe("days left after a skipped date", () => {
	const game = (gid: number, date: string) =>
		({
			gid,
			day: gid,
			season: 2026,
			date,
			teams: [{ tid: 0 }, { tid: 1 }],
		}) as unknown as Game;
	const clock = calendarClock({
		games: [game(1, "2025-10-21"), game(2, "2026-04-12")],
		schedule: [],
		season: 2026,
		phase: PHASE.REGULAR_SEASON,
		classicDays: 89,
	})!;

	test("count from the day before the next night, the day shown as today", () => {
		// No games on Christmas Eve in a season that skips it: after the 23rd,
		// the next night is the 25th.
		assert.strictEqual(
			countFrom("2025-12-23", [row(0, 1, "2025-12-25")]),
			"2025-12-24",
		);
		assert.strictEqual(
			countFrom("2025-12-23", [row(-4, -4, "2025-12-24")]),
			"2025-12-23",
		);
		assert.strictEqual(countFrom("", [row(0, 1, "2025-10-21")]), "");
	});

	test("the shown days go down by one a day across the skipped date", () => {
		const count = (tonight: string, from: string) => ({
			clock,
			tonight,
			from,
			gamesOut: () => undefined,
		});
		// After the 22nd (the 23rd is today).
		const gamesRemaining = 5;
		const days = daysLeft(gamesRemaining, count("2025-12-22", "2025-12-22"));
		assert.strictEqual(days, daysOut(gamesRemaining, clock, "2025-12-22"));
		const after23 =
			gamesRemaining - ticksBetween(clock, "2025-12-22", "2025-12-23");
		const after25 = after23 - ticksBetween(clock, "2025-12-23", "2025-12-25");

		// After the 23rd, the 25th is today: two days gone, not one.
		assert.strictEqual(
			daysLeft(after23, count("2025-12-23", "2025-12-24")),
			days - 2,
		);
		assert.strictEqual(
			daysLeft(after25, count("2025-12-25", "2025-12-25")),
			days - 3,
		);
	});
});
