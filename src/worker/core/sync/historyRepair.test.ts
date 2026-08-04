import { assert, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import {
	repairLeagueHistory,
	repairSeasonHistory,
	roundsWonFromSeries,
} from "./historyRepair.ts";

// ---------------------------------------------------------------------------
// The "??? champion" incident: after a rollback-and-replay recovery, one 2005
// teamSeasons row came back at an old vintage, its playoffRoundsWon stale.
// Every champion display derives from that field, so a finished season showed
// no champion at all - while the bracket, sitting right there in
// playoffSeries, still said exactly who won what. These tests pin the repair:
// recompute the derived field from the bracket, fix only what disagrees, and
// refuse to vouch for a season whose bracket cannot name a champion.
// ---------------------------------------------------------------------------

// A finished two-round bracket: numGamesPlayoffSeries [5, 7], so 3 wins take
// round one and 4 take the finals. A beats B, C beats D; A beats C for the
// title. Truth: A=2, C=1, B=D=0.
const FINISHED_SERIES = [
	[
		{
			home: { tid: 0, won: 3 },
			away: { tid: 1, won: 1 },
		},
		{
			home: { tid: 2, won: 3 },
			away: { tid: 3, won: 2 },
		},
	],
	[
		{
			home: { tid: 0, won: 4 },
			away: { tid: 2, won: 2 },
		},
	],
];

const NUM_GAMES = [5, 7];

describe("roundsWonFromSeries", () => {
	test("derives every bracket team's rounds won, champion included", () => {
		const rounds = roundsWonFromSeries(FINISHED_SERIES as any, NUM_GAMES);
		assert.deepStrictEqual(
			[...rounds.entries()].sort((a, b) => a[0] - b[0]),
			[
				[0, 2],
				[1, 0],
				[2, 1],
				[3, 0],
			],
		);
	});

	test("an undecided finals leaves both teams at their appearances", () => {
		const torn = [
			FINISHED_SERIES[0],
			[{ home: { tid: 0, won: 2 }, away: { tid: 2, won: 1 } }],
		];
		const rounds = roundsWonFromSeries(torn as any, NUM_GAMES);
		assert.strictEqual(rounds.get(0), 1);
		assert.strictEqual(rounds.get(2), 1);
	});

	test("a bye advances without claiming a fake series win for the opponent", () => {
		const withBye = [
			[{ home: { tid: 5, won: 0 } }],
			[{ home: { tid: 5, won: 4 }, away: { tid: 6, won: 1 } }],
		];
		const rounds = roundsWonFromSeries(withBye as any, NUM_GAMES);
		assert.strictEqual(rounds.get(5), 2);
		assert.strictEqual(rounds.get(6), 1);
	});

	test("a pending play-in slot is a placeholder, not a playoff team", () => {
		const pending = [
			[
				{
					home: { tid: 0, won: 0 },
					away: { tid: 99, won: 0, pendingPlayIn: true },
				},
			],
		];
		const rounds = roundsWonFromSeries(pending as any, NUM_GAMES);
		assert.strictEqual(rounds.has(99), false);
		assert.strictEqual(rounds.get(0), 0);
	});
});

describe("repairing a completed season", () => {
	// The device's rows: the CHAMPION's is stale (says 1, the bracket says 2),
	// everyone else is right, and a lottery team sits at -1 outside the bracket.
	const setup = async ({
		series = FINISHED_SERIES,
		dropChampionRow = false,
	}: { series?: any; dropChampionRow?: boolean } = {}) => {
		resetG();
		g.setWithoutSavingToDB("season", 2005);
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		g.setWithoutSavingToDB("startingSeason", 2005);
		g.setWithoutSavingToDB("numGamesPlayoffSeries", [
			{ start: -Infinity, value: NUM_GAMES },
		] as any);

		const teamSeasons = [
			{ tid: 0, season: 2005, playoffRoundsWon: 1 }, // stale champion
			{ tid: 1, season: 2005, playoffRoundsWon: 0 },
			{ tid: 2, season: 2005, playoffRoundsWon: 1 },
			{ tid: 3, season: 2005, playoffRoundsWon: 0 },
			{ tid: 4, season: 2005, playoffRoundsWon: -1 }, // missed playoffs
		].filter((row) => !(dropChampionRow && row.tid === 0));

		await resetCache({ teamSeasons: teamSeasons as any });
		await idb.cache.playoffSeries.add({
			season: 2005,
			currentRound: 1,
			series,
		} as any);
	};

	const rowFor = async (tid: number) =>
		idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [2005, tid]);

	test("the stale champion row is put back to what the bracket proves", async () => {
		await setup();
		const result = await repairSeasonHistory(2005);

		assert.strictEqual(result.repaired, 1);
		assert.deepStrictEqual(result.problems, []);
		assert.strictEqual((await rowFor(0))!.playoffRoundsWon, 2);
		// Rows that already agreed are untouched, including the lottery team the
		// bracket says nothing about.
		assert.strictEqual((await rowFor(2))!.playoffRoundsWon, 1);
		assert.strictEqual((await rowFor(4))!.playoffRoundsWon, -1);
	});

	test("running again finds nothing - the repair converges", async () => {
		await setup();
		await repairSeasonHistory(2005);
		const second = await repairSeasonHistory(2005);
		assert.strictEqual(second.repaired, 0);
		assert.deepStrictEqual(second.problems, []);
	});

	test("a missing champion row is a problem, not a crash - and blocks vouching", async () => {
		await setup({ dropChampionRow: true });
		const result = await repairSeasonHistory(2005);
		assert.strictEqual(result.repaired, 0);
		assert.strictEqual(result.problems.length, 1);
		assert.ok(result.problems[0]!.includes("missing"));
	});

	test("a torn finals in a finished season cannot name a champion", async () => {
		const torn = [
			FINISHED_SERIES[0],
			[{ home: { tid: 0, won: 2 }, away: { tid: 2, won: 1 } }],
		];
		await setup({ series: torn });
		const result = await repairSeasonHistory(2005);
		assert.ok(result.problems.some((p) => p.includes("champions")));
	});

	test("repairLeagueHistory walks the completed seasons and reports the total", async () => {
		await setup();
		const result = await repairLeagueHistory("test");
		assert.strictEqual(result.repaired, 1);
		assert.deepStrictEqual(result.problems, []);
		// The champion is derivable again - this is the line every champion
		// display in the app effectively runs.
		assert.strictEqual((await rowFor(0))!.playoffRoundsWon, NUM_GAMES.length);
	});

	test("a season with no bracket at all is left alone", async () => {
		await setup();
		await idb.cache.playoffSeries.delete(2005);
		const result = await repairSeasonHistory(2005);
		assert.strictEqual(result.repaired, 0);
		assert.deepStrictEqual(result.problems, []);
		assert.strictEqual((await rowFor(0))!.playoffRoundsWon, 1);
	});
});
