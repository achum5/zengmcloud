import { assert, describe, test } from "vitest";
import { hardCapForTid } from "./getHardCap.ts";

describe("hardCapForTid", () => {
	test("off (Infinity) when the amount is 0", () => {
		assert.strictEqual(hardCapForTid(0, 0, []), Infinity);
		assert.strictEqual(hardCapForTid(3, 0, [3]), Infinity);
	});

	test("applies to every team when the tid list is empty", () => {
		assert.strictEqual(hardCapForTid(0, 200000, []), 200000);
		assert.strictEqual(hardCapForTid(29, 200000, []), 200000);
	});

	test("applies only to listed teams when the tid list is set", () => {
		assert.strictEqual(hardCapForTid(5, 200000, [5, 7]), 200000);
		assert.strictEqual(hardCapForTid(7, 200000, [5, 7]), 200000);
		assert.strictEqual(hardCapForTid(3, 200000, [5, 7]), Infinity);
	});

	test("a negative amount is treated as off", () => {
		assert.strictEqual(hardCapForTid(0, -1, []), Infinity);
	});
});
