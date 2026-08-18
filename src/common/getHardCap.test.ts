import { assert, describe, test } from "vitest";
import { hardCapAmountProblem, hardCapForTid } from "./getHardCap.ts";

describe("hardCapForTid", () => {
	test("off (Infinity) when the amount is 0", () => {
		assert.strictEqual(
			hardCapForTid(0, { hardCapAmount: 0, hardCapTids: [] }),
			Infinity,
		);
		assert.strictEqual(
			hardCapForTid(3, { hardCapAmount: 0, hardCapTids: [3] }),
			Infinity,
		);
	});

	test("applies to every team when the tid list is empty", () => {
		assert.strictEqual(
			hardCapForTid(0, { hardCapAmount: 200000, hardCapTids: [] }),
			200000,
		);
		assert.strictEqual(
			hardCapForTid(29, { hardCapAmount: 200000, hardCapTids: [] }),
			200000,
		);
	});

	test("applies only to listed teams when the tid list is set", () => {
		assert.strictEqual(
			hardCapForTid(5, { hardCapAmount: 200000, hardCapTids: [5, 7] }),
			200000,
		);
		assert.strictEqual(
			hardCapForTid(7, { hardCapAmount: 200000, hardCapTids: [5, 7] }),
			200000,
		);
		assert.strictEqual(
			hardCapForTid(3, { hardCapAmount: 200000, hardCapTids: [5, 7] }),
			Infinity,
		);
	});

	test("a negative amount is treated as off", () => {
		assert.strictEqual(
			hardCapForTid(0, { hardCapAmount: -1, hardCapTids: [] }),
			Infinity,
		);
	});

	test("tracks the luxury tax line when hardCapUseLuxuryTax is set", () => {
		// Uses luxuryPayroll, ignoring the fixed hardCapAmount.
		assert.strictEqual(
			hardCapForTid(5, {
				hardCapAmount: 999999,
				hardCapTids: [5],
				hardCapUseLuxuryTax: true,
				luxuryPayroll: 44650,
			}),
			44650,
		);
		// Still respects the team list.
		assert.strictEqual(
			hardCapForTid(3, {
				hardCapAmount: 0,
				hardCapTids: [5],
				hardCapUseLuxuryTax: true,
				luxuryPayroll: 44650,
			}),
			Infinity,
		);
	});
});

describe("hardCapAmountProblem", () => {
	// The reported state, exactly: hard cap tracking the luxury tax line, with
	// a leftover fixed amount (44.65M) below the salary cap (58.7M). Saving any
	// unrelated setting was refused, naming a field the user had not touched
	// and that had no effect on their league.
	const REPORTED = {
		hardCapAmount: 44650,
		salaryCap: 58700,
	};

	test("the reported config saves cleanly, because the amount is inert", () => {
		assert.isUndefined(
			hardCapAmountProblem({ ...REPORTED, hardCapUseLuxuryTax: true }),
		);
	});

	test("the same numbers are still rejected when the amount is what binds", () => {
		// Turn the toggle off and the value matters again, so the check must
		// come back rather than being deleted.
		assert.strictEqual(
			hardCapAmountProblem({ ...REPORTED, hardCapUseLuxuryTax: false }),
			"Hard cap must be at least the salary cap",
		);
		assert.strictEqual(
			hardCapAmountProblem(REPORTED),
			"Hard cap must be at least the salary cap",
		);
	});

	test("a negative amount is always wrong, toggle or not", () => {
		// This one is about the field itself, not about what reads it.
		for (const hardCapUseLuxuryTax of [true, false]) {
			assert.strictEqual(
				hardCapAmountProblem({
					hardCapAmount: -1,
					salaryCap: 58700,
					hardCapUseLuxuryTax,
				}),
				"Must be 0 (off) or a positive number",
			);
		}
	});

	test("0 means off and is always fine", () => {
		assert.isUndefined(
			hardCapAmountProblem({ hardCapAmount: 0, salaryCap: 58700 }),
		);
	});

	test("an amount at or above the salary cap is fine", () => {
		assert.isUndefined(
			hardCapAmountProblem({ hardCapAmount: 58700, salaryCap: 58700 }),
		);
		assert.isUndefined(
			hardCapAmountProblem({ hardCapAmount: 90000, salaryCap: 58700 }),
		);
	});

	test("no salary cap to compare against means nothing to complain about", () => {
		assert.isUndefined(
			hardCapAmountProblem({ hardCapAmount: 1000, salaryCap: undefined }),
		);
	});

	test("agrees with the resolver about when the amount is read", () => {
		// The rule only makes sense if it matches what actually binds teams: an
		// amount the resolver ignores must never block a save.
		const ignored = hardCapForTid(0, {
			hardCapAmount: REPORTED.hardCapAmount,
			hardCapTids: [],
			hardCapUseLuxuryTax: true,
			luxuryPayroll: 71150,
		});
		assert.strictEqual(ignored, 71150);
		assert.isUndefined(
			hardCapAmountProblem({ ...REPORTED, hardCapUseLuxuryTax: true }),
		);
	});
});
