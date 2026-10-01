import { assert, beforeEach, describe, test } from "vitest";
import {
	describeTransaction,
	draftIsComplete,
	storyHooks,
} from "./getPlayerRecapData.ts";
import { PHASE } from "../../common/constants.ts";
import { g } from "./index.ts";
import { resetG } from "../../test/helpers.ts";

const abbrevs = new Map([
	[0, "LAL"],
	[1, "BOS"],
]);

describe("describeTransaction", () => {
	// The offseason phases are dated to the season that just FINISHED, so a
	// signing in "2002 free agency" is a player who was somewhere else all
	// through 2002 and debuts for his new team in 2003. Rendered bare, that reads
	// as a move made during 2002 - which leaves the AI unable to say how a player
	// got to the team he's playing for, or dating the move a year early.
	test("an offseason move says which season it takes effect for", () => {
		for (const phase of [
			PHASE.DRAFT_LOTTERY,
			PHASE.DRAFT,
			PHASE.AFTER_DRAFT,
			PHASE.RESIGN_PLAYERS,
			PHASE.FREE_AGENCY,
		]) {
			const text = describeTransaction(
				{ season: 2002, phase, tid: 0, type: "freeAgent" },
				abbrevs,
			);
			assert.ok(text.includes("(for 2003)"), `phase ${phase}: ${text}`);
		}
	});

	test("a move made during the season is not relabelled", () => {
		for (const phase of [
			PHASE.PRESEASON,
			PHASE.REGULAR_SEASON,
			PHASE.AFTER_TRADE_DEADLINE,
			PHASE.PLAYOFFS,
		]) {
			const text = describeTransaction(
				{ season: 2002, phase, tid: 1, type: "trade", fromTid: 0 },
				abbrevs,
			);
			assert.ok(!text.includes("(for"), `phase ${phase}: ${text}`);
		}
	});

	test("free agency reads as the move that put him on next year's team", () => {
		assert.strictEqual(
			describeTransaction(
				{
					season: 2002,
					phase: PHASE.FREE_AGENCY,
					tid: 0,
					type: "freeAgent",
				},
				abbrevs,
			),
			"2002 free agency (for 2003): signed with LAL",
		);
	});

	test("a deadline trade still reads as mid-season", () => {
		assert.strictEqual(
			describeTransaction(
				{
					season: 2002,
					phase: PHASE.AFTER_TRADE_DEADLINE,
					tid: 1,
					type: "trade",
					fromTid: 0,
				},
				abbrevs,
			),
			"2002 regular season: traded to BOS from LAL",
		);
	});

	test("the draft is an offseason move too, matching the DRAFTED block", () => {
		assert.strictEqual(
			describeTransaction(
				{
					season: 2001,
					phase: PHASE.DRAFT,
					tid: 1,
					type: "draft",
					pickNum: 5,
				},
				abbrevs,
			),
			"2001 draft (for 2002): drafted by BOS (pick 5)",
		);
	});
});

// A draft class written up before its draft produces a writeup about being
// picked by nobody - and because the draft-year section of a note is shown on
// the player's page off his draft line, it then sits there on every prospect in
// the class as a report on a draft that has not happened. The pass has to stay
// away until the picks are real.
describe("draftIsComplete", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("season", 2005);
	});

	test("the current class is off limits right up to the last pick", () => {
		for (const phase of [
			PHASE.PRESEASON,
			PHASE.REGULAR_SEASON,
			PHASE.AFTER_TRADE_DEADLINE,
			PHASE.PLAYOFFS,
			PHASE.DRAFT_LOTTERY,
			// Mid-draft counts as incomplete: half the class is still unpicked.
			PHASE.DRAFT,
		]) {
			g.setWithoutSavingToDB("phase", phase);
			assert.strictEqual(draftIsComplete(2005), false, `phase ${phase}`);
		}
	});

	test("it opens up the moment the draft is over, and stays open", () => {
		for (const phase of [
			PHASE.AFTER_DRAFT,
			PHASE.RESIGN_PLAYERS,
			PHASE.FREE_AGENCY,
		]) {
			g.setWithoutSavingToDB("phase", phase);
			assert.strictEqual(draftIsComplete(2005), true, `phase ${phase}`);
		}
	});

	test("past classes are always complete, whatever the phase", () => {
		for (const phase of [PHASE.PRESEASON, PHASE.PLAYOFFS, PHASE.DRAFT]) {
			g.setWithoutSavingToDB("phase", phase);
			assert.strictEqual(draftIsComplete(2004), true, `phase ${phase}`);
		}
	});

	test("a class from a season that hasn't happened is never complete", () => {
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		assert.strictEqual(draftIsComplete(2006), false);
	});
});

describe("storyHooks", () => {
	// Totals, like the stored stat rows: per game is total / gp.
	const row = (
		season: number,
		abbrev: string,
		gp: number,
		perGame: { pts: number; min: number; gs?: number },
	) => ({
		season,
		age: 25,
		abbrev,
		playoffs: false,
		gp,
		gs: perGame.gs,
		min: perGame.min * gp,
		pts: perGame.pts * gp,
		trb: 4 * gp,
		ast: 2 * gp,
		stl: 0,
		blk: 0,
		tov: 0,
		fg: 0,
		fga: 0,
		tp: 0,
		tpa: 0,
		ft: 0,
		fta: 0,
	});
	const roster = [
		{ name: "Me", pos: "G", age: 25, gp: 80, min: 34, pts: 24, trb: 4, ast: 2 },
		{
			name: "Other",
			pos: "F",
			age: 28,
			gp: 80,
			min: 36,
			pts: 20,
			trb: 8,
			ast: 3,
		},
	];

	test("a breakout on a new team says so, with the numbers", () => {
		const hooks = storyHooks({
			statRows: [
				row(2024, "BOS", 70, { pts: 9, min: 18, gs: 2 }),
				row(2025, "BOS", 70, { pts: 12, min: 22, gs: 10 }),
				row(2026, "LAL", 80, { pts: 24, min: 34, gs: 80 }),
			],
			season: 2026,
			teamAbbrevs: ["LAL"],
			teamRoster: () => roster,
			name: "Me",
			leagueRanks: ["5th in points (24)"],
			contractExp: 2026,
		});
		assert.deepStrictEqual(hooks, [
			"vs last season: 12 to 24 points and 22 to 34 minutes per game, 10 to 80 starts",
			"first season with LAL",
			"career high in points per game (previous best 12 in 2025)",
			"led LAL in scoring",
			"2nd on LAL in minutes",
			"league: 5th in points (24)",
			"contract year: his deal runs out after this season",
		]);
	});

	test("a rookie is a rookie, and a player who didn't play gets nothing", () => {
		const rookie = storyHooks({
			statRows: [row(2026, "LAL", 40, { pts: 5, min: 12 })],
			season: 2026,
			teamAbbrevs: ["LAL"],
			teamRoster: () => [],
			name: "Me",
			leagueRanks: [],
			draftYear: 2025,
		});
		assert.deepStrictEqual(rookie, ["rookie season"]);

		// A league's first season: no earlier stats for anyone, but a veteran
		// drafted years ago is not a rookie.
		const veteran = storyHooks({
			statRows: [row(2026, "LAL", 40, { pts: 5, min: 12 })],
			season: 2026,
			teamAbbrevs: ["LAL"],
			teamRoster: () => [],
			name: "Me",
			leagueRanks: [],
			draftYear: 2019,
		});
		assert.deepStrictEqual(veteran, []);

		const none = storyHooks({
			statRows: [row(2025, "LAL", 40, { pts: 5, min: 12 })],
			season: 2026,
			teamAbbrevs: [],
			teamRoster: () => [],
			name: "Me",
			leagueRanks: [],
		});
		assert.deepStrictEqual(none, []);
	});
});
