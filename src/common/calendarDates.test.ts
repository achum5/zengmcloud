import { assert, test } from "vitest";
import {
	formatGameDate,
	formatMonth,
	formatNumericDate,
	formatNumericDateRange,
	formatSeasonSpan,
} from "./calendarDates.ts";

test("mm/dd/yy", () => {
	assert.strictEqual(formatNumericDate("2025-11-12"), "11/12/25");
	assert.strictEqual(formatNumericDate("2027-01-05"), "01/05/27");
	assert.strictEqual(formatNumericDate("2000-03-09"), "03/09/00");
	assert.strictEqual(
		formatNumericDateRange("2025-11-28", "2025-12-02"),
		"11/28–12/02/25",
	);
	assert.strictEqual(
		formatNumericDateRange("2025-12-29", "2026-01-02"),
		"12/29/25–01/02/26",
	);
	assert.strictEqual(
		formatNumericDateRange("2025-11-12", "2025-11-12"),
		"11/12/25",
	);
});

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
