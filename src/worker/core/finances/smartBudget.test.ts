import { assert, describe, test } from "vitest";
import { MAX_LEVEL } from "../../../common/budgetLevels.ts";
import { BUDGET_TILT, smartBudgetLevels } from "./smartBudget.ts";
import type { TradeTier } from "../trade/tradePosture.ts";

const TIERS: TradeTier[] = ["teardown", "seller", "fringe", "buyer", "allIn"];

describe("how a plan spends its budget", () => {
	// The tilt reallocates the money a team was going to spend anyway - it
	// must never be a way for the AI to conjure extra budget.
	test("every tilt row sums to zero", () => {
		for (const tier of TIERS) {
			const row = BUDGET_TILT[tier];
			const sum = Object.values(row).reduce((s, x) => s + x, 0);
			assert.strictEqual(sum, 0, tier);
		}
	});

	test("a rebuild funds development, a contender funds the training room", () => {
		const rebuild = smartBudgetLevels({ tier: "teardown", baseLevel: 50 });
		const contender = smartBudgetLevels({ tier: "allIn", baseLevel: 50 });
		assert.isAbove(rebuild.coaching, rebuild.health);
		assert.isAbove(rebuild.scouting, contender.scouting);
		assert.isAbove(contender.health, rebuild.health);
		assert.isAbove(contender.facilities, rebuild.facilities);
	});

	test("a fringe team spends exactly like vanilla", () => {
		const levels = smartBudgetLevels({ tier: "fringe", baseLevel: 42 });
		for (const v of Object.values(levels)) {
			assert.strictEqual(v, 42);
		}
	});

	// The row summing to zero is only half of it: the clamp can eat one side of
	// the trade and leave the other, which is how a small-market rebuild ended
	// up spending nearly three times its base out of nowhere.
	test("what a plan spends adds up to what it was going to spend", () => {
		for (const tier of TIERS) {
			for (const base of [1, 2, 5, 12, 34, 50, 88, 96, MAX_LEVEL]) {
				const levels = smartBudgetLevels({ tier, baseLevel: base });
				const spent = Object.values(levels).reduce((s, x) => s + x, 0);
				assert.strictEqual(
					spent,
					4 * base,
					`${tier} at base ${base} spent ${spent} against ${4 * base}`,
				);
			}
		}
	});

	test("a plan the scale cannot fund keeps as much of its shape as it can", () => {
		// At the very bottom there is nowhere to take money FROM, so a rebuild
		// still leans toward coaching over health - it just cannot lean as far.
		const rebuild = smartBudgetLevels({ tier: "teardown", baseLevel: 2 });
		assert.isAtLeast(rebuild.coaching, rebuild.health);
		assert.isAtLeast(rebuild.scouting, rebuild.facilities);
	});

	test("levels stay on the 1..MAX_LEVEL scale at the extremes", () => {
		for (const tier of TIERS) {
			for (const base of [1, MAX_LEVEL]) {
				for (const v of Object.values(
					smartBudgetLevels({ tier, baseLevel: base }),
				)) {
					assert.ok(v >= 1 && v <= MAX_LEVEL);
				}
			}
		}
	});
});
