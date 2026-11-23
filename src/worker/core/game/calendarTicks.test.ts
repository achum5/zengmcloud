import { assert, describe, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import type { Game } from "../../../common/types.ts";
import {
	calendarClock,
	countClassicDays,
	daysOut,
	daysOutHurtTonight,
	injuryTicksUntil,
	nightTick,
	ticksBetween,
} from "./calendarTicks.ts";
import { addDays } from "../../../common/calendarDates.ts";
import { resetG } from "../../../test/helpers.ts";
import { g, helpers } from "../../util/index.ts";
import newSchedule from "../season/newSchedule.ts";
import addDaysToSchedule from "../season/addDaysToSchedule.ts";

const game = (
	gid: number,
	day: number,
	tids: [number, number],
	date?: string,
): Game =>
	({
		gid,
		day,
		season: 2027,
		...(date ? { date } : {}),
		teams: [{ tid: tids[0] }, { tid: tids[1] }],
	}) as unknown as Game;

const tick = (
	games: Game[],
	day: number,
	{
		phase = PHASE.REGULAR_SEASON,
		schedule = [],
		classicDays,
	}: {
		phase?: number;
		schedule?: { homeTid: number; awayTid: number; date?: string }[];
		classicDays?: number;
	} = {},
) =>
	nightTick({
		games,
		schedule,
		season: 2027,
		day,
		phase,
		classicDays,
	});

describe("nightTick", () => {
	test("classic days tick everyone once", () => {
		const games = [game(1, 1, [0, 1]), game(2, 1, [2, 3])];
		const result = tick(games, 1);
		assert.isUndefined(result.ticksByTid);
		assert.strictEqual(result.leagueRuns, 1);
		assert.isUndefined(result.clock);
	});

	test("a calendar night ticks everyone the classic days its clock passed", () => {
		// Opening night Oct 20 to the last night Oct 29: 10 days, 5 classic
		// days, so one classic day every two days, off days included.
		const games = [
			game(1, 1, [0, 1], "2026-10-20"),
			game(2, 2, [2, 3], "2026-10-21"),
			game(3, 3, [0, 2], "2026-10-24"),
			game(4, 4, [1, 3], "2026-10-29"),
		];
		const ticks = [1, 2, 3, 4].map((day) => {
			const result = tick(
				games.filter((row) => row.day! <= day),
				day,
				{
					schedule: games
						.filter((row) => row.day! > day)
						.map((row) => ({
							homeTid: row.teams[0].tid,
							awayTid: row.teams[1].tid,
							date: row.date,
						})),
					classicDays: 5,
				},
			);
			// Everyone alike: teams that didn't play tick too.
			assert.strictEqual(result.ticksByTid!.size, 0);
			assert.strictEqual(result.leagueRuns, result.restTicks);
			return result.restTicks;
		});
		assert.deepStrictEqual(ticks, [0, 1, 1, 3]);
	});

	test("the All-Star Game counts one more classic day", () => {
		const games = [
			game(1, 1, [0, 1], "2027-02-01"),
			game(2, 2, [-1, -2], "2027-02-05"),
			game(3, 3, [0, 1], "2027-02-10"),
		];
		const clock = calendarClock({
			games,
			schedule: [],
			season: 2027,
			phase: PHASE.REGULAR_SEASON,
			classicDays: 4,
		})!;
		assert.strictEqual(clock.rate * clock.at("2027-02-10"), 5);
	});

	test("the playoffs' clock starts at the regular season's last night", () => {
		const games = [
			game(1, 1, [0, 1], "2027-04-11"),
			{ ...game(2, 2, [0, 1], "2027-04-14"), playoffs: true },
			{ ...game(3, 3, [0, 1], "2027-04-16"), playoffs: true },
			{ ...game(4, 4, [0, 1], "2027-04-18"), playoffs: true },
		] as Game[];
		const ticks = [2, 3, 4].map(
			(day) =>
				tick(
					games.filter((row) => row.day! <= day),
					day,
					{ phase: PHASE.PLAYOFFS },
				).restTicks,
		);
		// 0.39 a day: 1.17, 1.95, 2.73.
		assert.deepStrictEqual(ticks, [1, 0, 1]);
	});

	// The whole point: a calendar season heals injuries, counts down trade
	// cooldowns and runs the CPU's trades and signings exactly as many times
	// as the same league's classic season.
	test("a calendar season ticks exactly as often as the classic one", () => {
		resetG();
		g.setWithoutSavingToDB("season", 2027);
		const teams = helpers.getTeamsDefault().map((t) => ({
			tid: t.tid,
			seasonAttrs: { cid: t.cid, did: t.did },
		}));
		const tids = newSchedule(teams);
		const classicDays = countClassicDays(tids);
		// BBGM packs 82 games into about 89 days.
		assert.isAbove(classicDays, 84);
		assert.isBelow(classicDays, 95);

		// A calendar of the same games: each classic day split into two
		// nights, with an off night now and then.
		const byDay = new Map<number, [number, number][]>();
		for (const row of addDaysToSchedule(
			tids.map(([homeTid, awayTid]) => ({ homeTid, awayTid })),
		)) {
			if (row.homeTid >= 0 && row.awayTid >= 0) {
				const day = byDay.get(row.day) ?? [];
				day.push([row.homeTid, row.awayTid]);
				byDay.set(row.day, day);
			}
		}
		const nights: { date: string; matchups: [number, number][] }[] = [];
		let date = "2026-10-21";
		for (const day of byDay.values()) {
			const half = Math.ceil(day.length / 2);
			nights.push({ date, matchups: day.slice(0, half) });
			date = addDays(date, 1);
			if (day.length > half) {
				nights.push({ date, matchups: day.slice(half) });
				date = addDays(date, nights.length % 5 === 0 ? 2 : 1);
			}
		}

		const games: Game[] = [];
		let ticks = 0;
		let runs = 0;
		let gid = 0;
		nights.forEach((night, i) => {
			for (const matchup of night.matchups) {
				games.push(game(gid++, i + 1, matchup, night.date));
			}
			const schedule = nights.slice(i + 1).flatMap((later) =>
				later.matchups.map(([homeTid, awayTid]) => ({
					homeTid,
					awayTid,
					date: later.date,
				})),
			);
			const result = tick(games, i + 1, { schedule, classicDays });
			assert.isAtMost(result.restTicks!, 2);
			ticks += result.restTicks!;
			runs += result.leagueRuns;
		});
		assert.strictEqual(ticks, classicDays);
		assert.strictEqual(runs, classicDays);
	});
});

describe("daysOut", () => {
	// Opening night Oct 21, the last night Apr 8 (170 days), 89 classic days;
	// then the playoffs.
	const first = "2026-10-21";
	const last = addDays(first, 169);
	const regular = calendarClock({
		games: [game(1, 1, [0, 1], first), game(2, 2, [0, 1], last)],
		schedule: [],
		season: 2027,
		phase: PHASE.REGULAR_SEASON,
		classicDays: 89,
	})!;
	const playoffs = calendarClock({
		games: [game(2, 2, [0, 1], last)],
		schedule: [],
		season: 2027,
		phase: PHASE.PLAYOFFS,
		classicDays: undefined,
	})!;

	// A player with `gamesRemaining` after the night `from`, counted down
	// night by night: the days shown each night, through the playoffs.
	const countdown = (gamesRemaining: number, from: string) => {
		const shown: number[] = [];
		let left = gamesRemaining;
		let night = from;
		while (left > 0) {
			const clock = night > last ? playoffs : regular;
			shown.push(daysOut(left, clock, night));
			const next = addDays(night, 1);
			const nextClock = next > last ? playoffs : regular;
			left = Math.max(
				0,
				left -
					(nextClock === clock
						? ticksBetween(clock, night, next)
						: ticksBetween(nextClock, undefined, next)),
			);
			night = next;
		}
		return shown;
	};

	test("goes down by one a day, and he's back the day it would hit zero", () => {
		for (const [gamesRemaining, from] of [
			[1, first],
			[10, first],
			[14, addDays(first, 50)],
			[3, addDays(first, 101)],
		] as const) {
			const shown = countdown(gamesRemaining, from);
			assert.deepStrictEqual(
				shown,
				shown.map((_, i) => shown[0]! - i),
				`${gamesRemaining} from ${from}`,
			);
			assert.strictEqual(shown.at(-1), 1);
		}
	});

	test("about 1.9 days a classic day in the season, 2.6 in the playoffs", () => {
		assert.strictEqual(daysOut(10, regular, first), 19);
		assert.strictEqual(daysOut(10, playoffs, last), 26);
	});

	test("an injury running into the playoffs counts their days", () => {
		// 3 days before the last night, 10 classic days left: the season's
		// last days, then the playoffs' slower ones - with no jump between.
		const from = addDays(last, -3);
		const shown = countdown(10, from);
		assert.isAbove(shown[0]!, 19);
		assert.deepStrictEqual(
			shown,
			shown.map((_, i) => shown[0]! - i),
		);
	});

	test("hurt tonight: after tonight's countdown", () => {
		const games = [game(1, 1, [0, 1], first), game(2, 2, [0, 1], "2026-10-23")];
		// Oct 23 ticks one classic day (3 days in), so 5 left reads as 4.
		assert.strictEqual(ticksBetween(regular, first, "2026-10-23"), 1);
		assert.strictEqual(
			daysOutHurtTonight({
				gamesRemaining: 5,
				clock: regular,
				games,
				season: 2027,
				tonight: "2026-10-23",
			}),
			daysOut(4, regular, "2026-10-23"),
		);
	});
});

describe("injuryTicksUntil", () => {
	const row = (
		gid: number,
		day: number,
		homeTid: number,
		awayTid: number,
		date?: string,
	) => ({ gid, day, homeTid, awayTid, ...(date ? { date } : {}) });

	test("classic days count days", () => {
		const until = injuryTicksUntil([row(1, 5, 0, 1), row(2, 7, 0, 2)]);
		assert.strictEqual(until(0, 2, 7), 2);
	});

	test("on a calendar, the clock's classic days through the night before", () => {
		// One classic day every two days from Feb 1; Feb 10 played (5 ticks).
		const clock = { rate: 0.5, at: (date: string) => Number(date.slice(8)) };
		const until = injuryTicksUntil(
			[
				row(1, 5, 0, 1, "2027-02-11"),
				row(2, 6, 0, 2, "2027-02-12"),
				row(3, 7, 2, 3, "2027-02-14"),
			],
			{ clock, lastNight: "2027-02-10" },
		);
		assert.strictEqual(until(0, 1, 5), 0);
		// Through Feb 11: still 5.
		assert.strictEqual(until(0, 2, 6), 0);
		// Through Feb 12: 6.
		assert.strictEqual(until(2, 3, 7), 1);
	});
});
