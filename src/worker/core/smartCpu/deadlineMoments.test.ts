import { assert, describe, test } from "vitest";
import { deadlineMomentDay } from "./market.ts";

describe("deadlineMomentDay", () => {
	const prefix = "2027:1:";
	test("a moment for each day as the days left count down", () => {
		assert.strictEqual(deadlineMomentDay(4, "2027:1:games:30", prefix), 4);
		assert.strictEqual(deadlineMomentDay(3, "2027:1:deadline:4", prefix), 3);
		// A night that is no classic day: the same day, no new moment.
		assert.strictEqual(deadlineMomentDay(3, "2027:1:deadline:3", prefix), 3);
	});

	test("a night that is two classic days meets for each", () => {
		// Asked twice that night (its league rules run twice).
		const first = deadlineMomentDay(1, "2027:1:deadline:3", prefix);
		assert.strictEqual(first, 2);
		assert.strictEqual(
			deadlineMomentDay(1, `${prefix}deadline:${first}`, prefix),
			1,
		);
	});

	test("another season's or phase's moment is not this one's", () => {
		assert.strictEqual(deadlineMomentDay(1, "2026:1:deadline:4", prefix), 1);
		assert.strictEqual(deadlineMomentDay(1, undefined, prefix), 1);
	});
});
