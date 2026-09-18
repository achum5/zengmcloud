import { assert, describe, test } from "vitest";
import { AWARD_STATS_ALL } from "./getPlayers.ts";
import { FormulaEvaluator } from "../../util/FormulaEvaluator.ts";

// An award for the best player over 35. There is no age filter, so the formula
// gates on age itself - max(0, 35 - age) is 0 once a player is old enough and
// grows the further under the cutoff he is.
const UNC = "ewa / 2.1 + vorp + gp / 82 * pts / 2 - 100 * max(0, 35 - age)";

const player = (age: number, rest: Record<string, number>) => ({
	age,
	ewa: 10,
	vorp: 2,
	gp: 70,
	pts: 18,
	...rest,
});

describe("age in an award formula", () => {
	test("is a variable an award formula is allowed to use", () => {
		assert.include(AWARD_STATS_ALL, "age");

		// Throws InvalidVariableError if age isn't in scope, which is what the
		// Award Settings page reports when you try to save
		assert.doesNotThrow(() => new FormulaEvaluator(UNC, AWARD_STATS_ALL, []));
	});

	test("gates an award to old players without a filter", () => {
		const evaluator = new FormulaEvaluator(UNC, AWARD_STATS_ALL, []);

		const unc = evaluator.evaluate(player(36, {}) as any);
		const prime = evaluator.evaluate(
			player(27, { ewa: 22, vorp: 9, pts: 28 }) as any,
		);

		assert.isAbove(
			unc,
			prime,
			"a much better player under the cutoff should still lose",
		);
	});

	test("ranks the old players against each other normally", () => {
		const evaluator = new FormulaEvaluator(UNC, AWARD_STATS_ALL, []);

		const better = evaluator.evaluate(player(36, { ewa: 14, vorp: 4 }) as any);
		const worse = evaluator.evaluate(player(39, { ewa: 4, vorp: 0.5 }) as any);

		assert.isAbove(better, worse);
	});
});
