import { assert, describe, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import type { Game } from "../../../common/types.ts";
import { dayTickFromGames, injuryTicksUntil } from "./calendarTicks.ts";

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
) => dayTickFromGames({ games, season: 2027, day, phase, numActiveTeams: 4 });

describe("dayTickFromGames", () => {
	test("classic days tick everyone once", () => {
		const games = [game(1, 1, [0, 1]), game(2, 1, [2, 3])];
		const result = tick(games, 1);
		assert.isUndefined(result.tidsPlayed);
		assert.strictEqual(result.leagueRuns, 1);
	});

	test("a calendar night ticks only the teams that played", () => {
		const games = [game(1, 1, [0, 1], "2026-10-20")];
		const result = tick(games, 1);
		assert.deepStrictEqual(Array.from(result.tidsPlayed!).sort(), [0, 1]);
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
		assert.isUndefined(tick(allStar, 5).tidsPlayed);
		assert.strictEqual(tick(allStar, 5).leagueRuns, 1);

		const playoffs = [game(1, 1, [0, 1], "2027-04-20")];
		assert.isUndefined(tick(playoffs, 1, PHASE.PLAYOFFS).tidsPlayed);
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
});
