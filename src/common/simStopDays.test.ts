import { assert, describe, test } from "vitest";
import {
	formatSimStopDays,
	invalidSimStopDayToken,
	parseSimStopDays,
	stopDateIn,
	stopsOnDay,
	stopsOnNight,
} from "./simStopDays.ts";

describe("parseSimStopDays", () => {
	test("a blank setting stops nothing", () => {
		for (const raw of ["", "   ", undefined]) {
			assert.deepStrictEqual(parseSimStopDays(raw), {
				deadline: false,
				days: [],
				dates: [],
			});
		}
	});

	// The whole point of the change: the deadline is an entry in the list, not a
	// thing that happens whether the league wants it or not.
	test("the deadline is just another entry", () => {
		assert.deepStrictEqual(parseSimStopDays("deadline"), {
			deadline: true,
			days: [],
			dates: [],
		});
		assert.deepStrictEqual(parseSimStopDays("15, deadline"), {
			deadline: true,
			days: [15],
			dates: [],
		});
		assert.strictEqual(parseSimStopDays("15").deadline, false);
	});

	test("people type it however they like", () => {
		for (const raw of [
			"15,41",
			" 15 , 41 ",
			"Day 15, day 41",
			"41, 15",
			"15, 41, 15",
			"15,,41,",
		]) {
			assert.deepStrictEqual(parseSimStopDays(raw).days, [15, 41], raw);
		}
		assert.strictEqual(parseSimStopDays("DEADLINE").deadline, true);
	});

	// A typo that silently became day 0 would stop the league somewhere it could
	// never leave, so nothing that isn't a real day survives parsing.
	test("nonsense is dropped rather than becoming a day", () => {
		assert.deepStrictEqual(parseSimStopDays("0, -3, 1.5, abc").days, []);
	});

	test("it round-trips through the settings field", () => {
		for (const raw of [
			"",
			"15",
			"deadline",
			"15, 41, deadline",
			"Jan 15, Dec 15, deadline",
		]) {
			const parsed = parseSimStopDays(raw);
			assert.deepStrictEqual(
				parseSimStopDays(formatSimStopDays(parsed)),
				parsed,
			);
		}
	});
});

describe("invalidSimStopDayToken", () => {
	// The settings form rejects a typo rather than quietly dropping it, since a
	// dropped entry looks exactly like a stop that never fires.
	test("names the token that is wrong", () => {
		assert.strictEqual(invalidSimStopDayToken("15, deadline"), undefined);
		assert.strictEqual(invalidSimStopDayToken(""), undefined);
		assert.strictEqual(invalidSimStopDayToken("15, deadlien"), "deadlien");
		assert.strictEqual(invalidSimStopDayToken("15, 0"), "0");
		assert.strictEqual(invalidSimStopDayToken("1.5"), "1.5");
		assert.strictEqual(invalidSimStopDayToken("Dec 15, Feb 29"), undefined);
		assert.strictEqual(invalidSimStopDayToken("Dec 15, Feb 30"), "Feb 30");
		assert.strictEqual(invalidSimStopDayToken("Dex 15"), "Dex 15");
	});
});

describe("stopsOnDay", () => {
	test("only the configured days", () => {
		const stops = parseSimStopDays("15, 41");
		assert.isTrue(stopsOnDay(stops, 15));
		assert.isTrue(stopsOnDay(stops, 41));
		assert.isFalse(stopsOnDay(stops, 14));
		assert.isFalse(stopsOnDay(stops, undefined));
	});

	test("asking for the deadline does not stop every day", () => {
		const stops = parseSimStopDays("deadline");
		for (const day of [1, 15, 41, 82]) {
			assert.isFalse(stopsOnDay(stops, day));
		}
	});
});

describe("dates, on the NBA calendar", () => {
	test("read however they're typed, kept in season order", () => {
		assert.deepStrictEqual(
			parseSimStopDays("January 15, dec 15, Dec. 15, 15").dates,
			[
				{ month: 12, day: 15 },
				{ month: 1, day: 15 },
			],
		);
		assert.deepStrictEqual(parseSimStopDays("Jan 15, Dec 15").days, []);
		assert.strictEqual(
			formatSimStopDays(parseSimStopDays("jan 15, december 15")),
			"Dec 15, Jan 15",
		);
	});

	// The 2025 season: opening night October 22, 2024.
	test("before the first night on or after the date", () => {
		const stops = parseSimStopDays("Dec 15");
		assert.strictEqual(stopDateIn(stops.dates[0]!, 2025), "2024-12-15");
		// Nights with games on the 14th and 16th, none on the 15th.
		assert.isFalse(stopsOnNight(stops, "2024-12-14", "2024-12-13", 2025));
		assert.isTrue(stopsOnNight(stops, "2024-12-16", "2024-12-14", 2025));
		// Games on the 15th: that night.
		assert.isTrue(stopsOnNight(stops, "2024-12-15", "2024-12-14", 2025));
		// Once past it, never again.
		assert.isFalse(stopsOnNight(stops, "2024-12-16", "2024-12-15", 2025));
		// Before opening night: there.
		assert.isTrue(
			stopsOnNight(parseSimStopDays("Oct 22"), "2024-10-22", undefined, 2025),
		);
		assert.isFalse(stopsOnNight(stops, undefined, "2024-12-14", 2025));
	});
});
