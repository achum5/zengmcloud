import { assert, describe, test } from "vitest";
import {
	crossesMidnight,
	newRule,
	nextFireForRule,
	summarizeRule,
	type ScheduleRule,
} from "./scheduleTime.ts";

const rule = (patch: Partial<ScheduleRule>): ScheduleRule => ({
	...newRule(),
	...patch,
});

// Jan 2026: 1st = Thu, 5th = Mon, 10th = Sat.
const at = (day: number, h: number, m: number) =>
	new Date(2026, 0, day, h, m, 0, 0);

describe("nextFireForRule — at times", () => {
	const r = rule({ mode: "at", times: ["08:00", "20:00"], days: [] });

	test("later same-day time", () => {
		assert.strictEqual(nextFireForRule(r, at(5, 9, 0)), at(5, 20, 0).getTime());
	});

	test("rolls to next day after the last time", () => {
		assert.strictEqual(nextFireForRule(r, at(5, 21, 0)), at(6, 8, 0).getTime());
	});

	test("respects days-of-week (Saturdays only)", () => {
		const sat = rule({ mode: "at", times: ["10:00"], days: [6] });
		// From Monday the 5th, the next Saturday is the 10th.
		assert.strictEqual(
			nextFireForRule(sat, at(5, 9, 0)),
			at(10, 10, 0).getTime(),
		);
	});

	test("no valid times → undefined", () => {
		assert.strictEqual(
			nextFireForRule(rule({ mode: "at", times: [] }), at(5, 9, 0)),
			undefined,
		);
	});
});

describe("nextFireForRule — every N within a window", () => {
	const r = rule({
		mode: "every",
		start: "09:00",
		end: "17:00",
		everyMinutes: 30,
		days: [],
	});

	test("next slot inside the window", () => {
		assert.strictEqual(
			nextFireForRule(r, at(5, 9, 15)),
			at(5, 9, 30).getTime(),
		);
	});

	test("before the window opens → fires at start", () => {
		assert.strictEqual(nextFireForRule(r, at(5, 7, 0)), at(5, 9, 0).getTime());
	});

	test("after the window closes → next day's start", () => {
		assert.strictEqual(nextFireForRule(r, at(5, 18, 0)), at(6, 9, 0).getTime());
	});

	test("end time is an inclusive slot", () => {
		// 16:45 → 17:00 (the last slot; end is inclusive).
		assert.strictEqual(
			nextFireForRule(r, at(5, 16, 45)),
			at(5, 17, 0).getTime(),
		);
	});

	test("at the final slot, rolls to the next day", () => {
		// From 17:00 the next slot (17:30) exceeds the window → tomorrow 09:00.
		assert.strictEqual(nextFireForRule(r, at(5, 17, 0)), at(6, 9, 0).getTime());
	});
});

test("disabled rule never fires", () => {
	assert.strictEqual(
		nextFireForRule(rule({ enabled: false }), at(5, 9, 0)),
		undefined,
	);
});

describe("nextFireForRule — overnight windows", () => {
	// The natural shape for an unattended sim: start in the evening, run until
	// morning. An end earlier than the start used to be skipped entirely, so a
	// rule like this silently never fired.
	const r = rule({
		mode: "every",
		start: "22:00",
		end: "06:00",
		everyMinutes: 60,
		days: [],
	});

	test("fires inside the evening half of the window", () => {
		assert.strictEqual(
			nextFireForRule(r, at(5, 22, 30)),
			at(5, 23, 0).getTime(),
		);
	});

	test("keeps firing after midnight, inside the same window", () => {
		assert.strictEqual(nextFireForRule(r, at(6, 1, 30)), at(6, 2, 0).getTime());
	});

	test("the last slot before the end is honored", () => {
		assert.strictEqual(nextFireForRule(r, at(6, 5, 30)), at(6, 6, 0).getTime());
	});

	test("past the end, waits for the evening reopen", () => {
		assert.strictEqual(nextFireForRule(r, at(6, 7, 0)), at(6, 22, 0).getTime());
	});

	test("day-of-week is judged by the day the window OPENS", () => {
		// Friday nights only. Jan 2 2026 is a Friday, so the window runs from Fri
		// 10pm into Saturday morning - and 2am Saturday still belongs to it.
		const friNight = rule({ ...r, days: [5] });
		assert.strictEqual(
			nextFireForRule(friNight, at(3, 1, 30)),
			at(3, 2, 0).getTime(),
		);
		// Saturday evening is NOT a Friday window, so it waits a week.
		assert.strictEqual(
			nextFireForRule(friNight, at(3, 23, 0)),
			at(9, 22, 0).getTime(),
		);
	});

	test("crossesMidnight identifies the shape", () => {
		assert.strictEqual(crossesMidnight("22:00", "06:00"), true);
		assert.strictEqual(crossesMidnight("09:00", "17:00"), false);
		assert.strictEqual(crossesMidnight("09:00", "09:00"), false);
	});
});

describe("summarizeRule", () => {
	test("describes a custom day count rather than the preset name", () => {
		const r = rule({
			mode: "at",
			times: ["20:00"],
			amount: "days",
			numDays: 3,
			days: [1, 3],
		});
		assert.strictEqual(summarizeRule(r), "Mo,We at 8:00 PM — sim 3 days");
	});

	test("flags an overnight window so it doesn't read as a mistake", () => {
		const r = rule({
			mode: "every",
			start: "22:00",
			end: "06:00",
			everyMinutes: 15,
			amount: "day",
			days: [],
		});
		assert.ok(summarizeRule(r).includes("(overnight)"));
		assert.ok(summarizeRule(r).includes("sim day"));
	});
});
