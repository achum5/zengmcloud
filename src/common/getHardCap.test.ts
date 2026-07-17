import { assert, describe, test } from "vitest";
import { hardCapForTid } from "./getHardCap.ts";

describe("hardCapForTid", () => {
	test("off (Infinity) when the amount is 0", () => {
		assert.strictEqual(hardCapForTid(0, { hardCapAmount: 0, hardCapTids: [] }), Infinity);
		assert.strictEqual(hardCapForTid(3, { hardCapAmount: 0, hardCapTids: [3] }), Infinity);
	});

	test("applies to every team when the tid list is empty", () => {
		assert.strictEqual(hardCapForTid(0, { hardCapAmount: 200000, hardCapTids: [] }), 200000);
		assert.strictEqual(hardCapForTid(29, { hardCapAmount: 200000, hardCapTids: [] }), 200000);
	});

	test("applies only to listed teams when the tid list is set", () => {
		assert.strictEqual(hardCapForTid(5, { hardCapAmount: 200000, hardCapTids: [5, 7] }), 200000);
		assert.strictEqual(hardCapForTid(7, { hardCapAmount: 200000, hardCapTids: [5, 7] }), 200000);
		assert.strictEqual(hardCapForTid(3, { hardCapAmount: 200000, hardCapTids: [5, 7] }), Infinity);
	});

	test("a negative amount is treated as off", () => {
		assert.strictEqual(hardCapForTid(0, { hardCapAmount: -1, hardCapTids: [] }), Infinity);
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
