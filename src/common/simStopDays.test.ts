import { assert, describe, test } from "vitest";
import {
	formatSimStopDays,
	invalidSimStopDayToken,
	parseSimStopDays,
	stopsOnDay,
} from "./simStopDays.ts";

describe("parseSimStopDays", () => {
	test("a blank setting stops nothing", () => {
		for (const raw of ["", "   ", undefined]) {
			assert.deepStrictEqual(parseSimStopDays(raw), {
				deadline: false,
				days: [],
			});
		}
	});

	// The whole point of the change: the deadline is an entry in the list, not a
	// thing that happens whether the league wants it or not.
	test("the deadline is just another entry", () => {
		assert.deepStrictEqual(parseSimStopDays("deadline"), {
			deadline: true,
			days: [],
		});
		assert.deepStrictEqual(parseSimStopDays("15, deadline"), {
			deadline: true,
			days: [15],
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
		for (const raw of ["", "15", "deadline", "15, 41, deadline"]) {
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
