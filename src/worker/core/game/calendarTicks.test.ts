import { assert, describe, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import type { Game } from "../../../common/types.ts";
import {
	countClassicDays,
	dayTickFromGames,
	injuryTicksUntil,
} from "./calendarTicks.ts";
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
	phase: number = PHASE.REGULAR_SEASON,
) =>
	dayTickFromGames({
		games,
		schedule: [],
		season: 2027,
		day,
		phase,
		numActiveTeams: 4,
		classicDays: undefined,
	});

describe("dayTickFromGames", () => {
	test("classic days tick everyone once", () => {
		const games = [game(1, 1, [0, 1]), game(2, 1, [2, 3])];
		const result = tick(games, 1);
		assert.isUndefined(result.ticksByTid);
		assert.strictEqual(result.leagueRuns, 1);
	});

	test("a calendar night ticks only the teams that played", () => {
		const games = [game(1, 1, [0, 1], "2026-10-20")];
		const result = tick(games, 1);
		assert.deepStrictEqual(Array.from(result.ticksByTid!.entries()).sort(), [
			[0, 1],
			[1, 1],
		]);
	});

	test("league-wide rules run once per round of games", () => {
		// Four teams: a round is two games.
		const games = [
			game(1, 1, [0, 1], "2026-10-20"),
			game(2, 2, [2, 3], "2026-10-21"),
			game(3, 3, [0, 2], "2026-10-22"),
			game(4, 4, [1, 3], "2026-10-23"),
			game(5, 4, [0, 1], "2026-10-23"),
		];
		const runs = [1, 2, 3, 4].map(
			(day) =>
				tick(
					games.filter((g) => g.day! <= day),
					day,
				).leagueRuns,
		);
		// 1 game, 2, 3, 5: rounds complete after the 2nd and 4th games.
		assert.deepStrictEqual(runs, [0, 1, 0, 1]);
	});

	test("the All-Star Game and the playoffs count as classic days", () => {
		const allStar = [game(1, 5, [-1, -2], "2027-02-14")];
		assert.isUndefined(tick(allStar, 5).ticksByTid);
		assert.strictEqual(tick(allStar, 5).leagueRuns, 1);

		const playoffs = [game(1, 1, [0, 1], "2027-04-20")];
		assert.isUndefined(tick(playoffs, 1, PHASE.PLAYOFFS).ticksByTid);
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
		// nights, a team playing at most once a night.
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
		const nights: [number, number][][] = [];
		for (const day of byDay.values()) {
			const half = Math.ceil(day.length / 2);
			nights.push(day.slice(0, half));
			if (day.length > half) {
				nights.push(day.slice(half));
			}
		}

		const games: Game[] = [];
		const healed = new Map<number, number>();
		let runs = 0;
		let gid = 0;
		nights.forEach((night, i) => {
			for (const matchup of night) {
				games.push(game(gid++, i + 1, matchup, "2026-11-01"));
			}
			const schedule = nights
				.slice(i + 1)
				.flat()
				.map(([homeTid, awayTid]) => ({ homeTid, awayTid }));
			const result = dayTickFromGames({
				games,
				schedule,
				season: 2027,
				day: i + 1,
				phase: PHASE.REGULAR_SEASON,
				numActiveTeams: 30,
				classicDays,
			});
			for (const [tid, ticks] of result.ticksByTid!) {
				assert.isAtLeast(ticks, 1, "every game night ticks");
				assert.isAtMost(ticks, 2);
				healed.set(tid, (healed.get(tid) ?? 0) + ticks);
			}
			runs += result.leagueRuns;
		});
		assert.strictEqual(healed.size, 30);
		for (const ticks of healed.values()) {
			assert.strictEqual(ticks, classicDays);
		}
		assert.strictEqual(runs, classicDays);
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

	test("a calendar counts the team's own games, and the All-Star night", () => {
		const until = injuryTicksUntil([
			row(1, 5, 0, 1, "2027-02-10"),
			row(2, 6, 2, 3, "2027-02-11"),
			row(3, 7, -3, -3, "2027-02-11"),
			row(4, 8, -1, -2, "2027-02-15"),
			row(5, 9, 0, 2, "2027-02-19"),
		]);
		assert.strictEqual(until(0, 1, 5), 0);
		assert.strictEqual(until(2, 2, 6), 0);
		// Team 0: one game, then the All-Star night.
		assert.strictEqual(until(0, 5, 9), 2);
		assert.strictEqual(until(2, 5, 9), 2);
	});

	test("on a calendar, at the classic pace", () => {
		// Team 0 has played 3 of 6 games, in a season of 12 classic days: two
		// ticks a game.
		const schedule = [
			row(1, 5, 0, 1, "2027-02-10"),
			row(2, 6, 0, 2, "2027-02-11"),
			row(3, 7, 0, 3, "2027-02-12"),
		];
		const until = injuryTicksUntil(schedule, {
			classicDays: 12,
			played: new Map([[0, 3]]),
		});
		assert.strictEqual(until(0, 1, 5), 0);
		assert.strictEqual(until(0, 2, 6), 2);
		assert.strictEqual(until(0, 3, 7), 4);
	});
});
