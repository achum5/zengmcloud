import { assert, test } from "vitest";
import {
	formatGameDate,
	formatMonth,
	formatSeasonSpan,
} from "./calendarDates.ts";

test("weekdays are the real ones for the year", () => {
	assert.strictEqual(
		formatGameDate("2026-11-22", "full"),
		"Sunday, November 22, 2026",
	);
	assert.strictEqual(
		formatGameDate("2025-11-22", "shortWithYear"),
		"Sat, Nov 22, 2025",
	);
	assert.strictEqual(formatGameDate("2022-11-22", "short"), "Tue, Nov 22");
	assert.strictEqual(
		formatGameDate("2028-02-29", "full"),
		"Tuesday, February 29, 2028",
	);
});

test("months and seasons", () => {
	assert.strictEqual(formatMonth("2025-11-22"), "November 2025");
	assert.strictEqual(formatMonth("2025-11-22", true), "Nov");
	assert.strictEqual(formatSeasonSpan(2026), "2025-26");
	assert.strictEqual(formatSeasonSpan(2000), "1999-00");
});
