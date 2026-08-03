import { assert, beforeEach, describe, test } from "vitest";
import { describeTransaction, draftIsComplete } from "./getPlayerRecapData.ts";
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
