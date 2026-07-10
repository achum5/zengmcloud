import { assert, describe, test } from "vitest";
import { oddsBands } from "./faBoard.ts";

describe("free-agency board odds bands", () => {
	test("mood +3 vs +2 at base 1.5 -> 60/40", () => {
		const bands = oddsBands([1.5 ** 3, 1.5 ** 2]);
		assert.strictEqual(bands[0]!.lo, 1);
		assert.strictEqual(bands[0]!.hi, 60);
		assert.strictEqual(bands[1]!.lo, 61);
		assert.strictEqual(bands[1]!.hi, 100);
		assert.strictEqual(bands[0]!.pct, 60);
		assert.strictEqual(bands[1]!.pct, 40);
	});

	test("equal moods -> even split", () => {
		const bands = oddsBands([1, 1]);
		assert.strictEqual(bands[0]!.hi, 50);
		assert.strictEqual(bands[1]!.lo, 51);
		assert.strictEqual(bands[1]!.hi, 100);
	});

	test("bands are contiguous, start at 1, end at 100", () => {
		for (const weights of [
			[1, 1, 1],
			[8, 4, 2, 1],
			[1.5 ** 5, 1.5 ** -3, 1],
			[100, 1],
		]) {
			const bands = oddsBands(weights);
			assert.strictEqual(bands[0]!.lo, 1);
			assert.strictEqual(bands.at(-1)!.hi, 100);
			for (let i = 1; i < bands.length; i++) {
				assert.strictEqual(bands[i]!.lo, bands[i - 1]!.hi + 1);
			}
			for (const b of bands) {
				assert.isAtLeast(b.hi, b.lo);
			}
		}
	});

	test("a hugely outweighed team still gets a 1-wide band", () => {
		const bands = oddsBands([1000, 0.001]);
		assert.strictEqual(bands[1]!.lo, 100);
		assert.strictEqual(bands[1]!.hi, 100);
	});

	test("negative moods work (weights below 1)", () => {
		const bands = oddsBands([1.5 ** -2, 1.5 ** -2]);
		assert.strictEqual(bands[0]!.hi, 50);
		assert.strictEqual(bands[1]!.hi, 100);
	});
});
