import { assert, describe, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import {
	classicDaysTo,
	signedTradableOn,
	threeMonthsAfter,
} from "./tradableOn.ts";
import type { CalendarClock } from "./calendarTicks.ts";

describe("the NBA's trade dates for a signing", () => {
	test("three months on, the month's last day if it's shorter", () => {
		assert.strictEqual(threeMonthsAfter("2024-10-22"), "2025-01-22");
		assert.strictEqual(threeMonthsAfter("2024-11-06"), "2025-02-06");
		assert.strictEqual(threeMonthsAfter("2024-11-30"), "2025-02-28");
		assert.strictEqual(threeMonthsAfter("2023-11-30"), "2024-02-29");
		assert.strictEqual(threeMonthsAfter("2024-09-15"), "2024-12-15");
	});

	test("the summer's signings Dec 15, re-signings Jan 15, the preseason's Jan 1", () => {
		resetG();
		// The 2024 offseason: the 2025 season, starting in October 2024, next.
		g.setWithoutSavingToDB("season", 2024);
		for (const phase of [
			PHASE.DRAFT,
			PHASE.RESIGN_PLAYERS,
			PHASE.FREE_AGENCY,
		]) {
			assert.strictEqual(
				signedTradableOn({ phase, resigned: false }),
				"2024-12-15",
			);
		}
		assert.strictEqual(
			signedTradableOn({ phase: PHASE.RESIGN_PLAYERS, resigned: true }),
			"2025-01-15",
		);
		g.setWithoutSavingToDB("season", 2025);
		assert.strictEqual(
			signedTradableOn({ phase: PHASE.PRESEASON, resigned: false }),
			"2025-01-01",
		);
	});

	test("in season, three months after the night he signed before", () => {
		resetG();
		g.setWithoutSavingToDB("season", 2025);
		assert.strictEqual(
			signedTradableOn({
				phase: PHASE.REGULAR_SEASON,
				signedOn: "2024-11-06",
				resigned: false,
			}),
			"2025-02-06",
		);
		assert.strictEqual(
			signedTradableOn({
				phase: PHASE.AFTER_TRADE_DEADLINE,
				signedOn: "2025-03-01",
				resigned: true,
			}),
			"2025-06-01",
		);
	});
});

describe("a wait to a date, night by night", () => {
	// Opening night October 22, about half a classic day a night.
	const from = "2024-10-22";
	const clock: CalendarClock = {
		rate: 0.53,
		at: (date) =>
			Math.max(
				0,
				Math.round(
					(Date.parse(`${date}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
						86_400_000,
				) + 1,
			),
	};

	test("over with the night before its date, never a night early", () => {
		for (const tonight of ["2024-12-10", "2024-12-12", "2024-12-13"]) {
			assert.isAtLeast(classicDaysTo("2024-12-15", clock, tonight), 1);
		}
		assert.strictEqual(classicDaysTo("2024-12-15", clock, "2024-12-14"), 0);
		assert.strictEqual(classicDaysTo("2024-12-15", clock, "2024-12-20"), 0);
		// No games until the 16th (a break): over after the 12th's.
		assert.strictEqual(
			classicDaysTo("2024-12-15", clock, "2024-12-12", "2024-12-16"),
			0,
		);
		assert.strictEqual(
			classicDaysTo("2024-12-15", clock, "2024-12-12", "2024-12-14"),
			1,
		);
		// Weeks out, the classic days to it.
		assert.strictEqual(classicDaysTo("2024-12-15", clock, "2024-10-22"), 28);
	});
});
