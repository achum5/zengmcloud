import { assert, describe, test } from "vitest";
import { calendarMomentKey, deadlineMomentDay } from "./market.ts";

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

describe("calendarMomentKey", () => {
	// 2024-25: opening night Tuesday October 22, the deadline February 6.
	const deadline = "2025-02-06";
	const key = (tonight: string | undefined, next: string | undefined) =>
		calendarMomentKey({ tonight, next, deadline });

	test("going into each Sunday and Thursday, not as the season opens", () => {
		assert.strictEqual(key(undefined, "2024-10-22"), undefined);
		assert.strictEqual(key("2024-10-22", "2024-10-23"), undefined);
		assert.strictEqual(key("2024-10-23", "2024-10-24"), "week:2024-10-24");
		assert.strictEqual(key("2024-10-24", "2024-10-25"), undefined);
		assert.strictEqual(key("2024-10-26", "2024-10-27"), "week:2024-10-27");
		// No games on the Sunday: going into the Monday instead.
		assert.strictEqual(key("2024-10-25", "2024-10-28"), "week:2024-10-27");
	});

	test("every night of the last ten days before the deadline", () => {
		assert.strictEqual(key("2025-01-25", "2025-01-26"), "week:2025-01-26");
		assert.strictEqual(key("2025-01-26", "2025-01-27"), "deadline:2025-01-27");
		assert.strictEqual(key("2025-01-27", "2025-01-28"), "deadline:2025-01-28");
		assert.strictEqual(key("2025-02-05", "2025-02-06"), "deadline:2025-02-06");
	});
});
