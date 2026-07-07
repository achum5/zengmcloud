import { assert, describe, test } from "vitest";
import {
	newRule,
	nextFireForRule,
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
		assert.strictEqual(
			nextFireForRule(r, at(5, 9, 0)),
			at(5, 20, 0).getTime(),
		);
	});

	test("rolls to next day after the last time", () => {
		assert.strictEqual(
			nextFireForRule(r, at(5, 21, 0)),
			at(6, 8, 0).getTime(),
		);
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
		assert.strictEqual(
			nextFireForRule(r, at(5, 7, 0)),
			at(5, 9, 0).getTime(),
		);
	});

	test("after the window closes → next day's start", () => {
		assert.strictEqual(
			nextFireForRule(r, at(5, 18, 0)),
			at(6, 9, 0).getTime(),
		);
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
		assert.strictEqual(
			nextFireForRule(r, at(5, 17, 0)),
			at(6, 9, 0).getTime(),
		);
	});
});

test("disabled rule never fires", () => {
	assert.strictEqual(
		nextFireForRule(rule({ enabled: false }), at(5, 9, 0)),
		undefined,
	);
});
