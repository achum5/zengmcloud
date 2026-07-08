import { assert, describe, test } from "vitest";
import { nextFires, projectFires, type PreviewDay } from "./autoPlayPreview.ts";
import { newRule, type ScheduleRule } from "./scheduleTime.ts";

const rule = (patch: Partial<ScheduleRule>): ScheduleRule => ({
	...newRule(),
	...patch,
});

const amountDays = { day: 1, week: 7, month: 30 };

describe("nextFires", () => {
	test("returns fires in chronological order across rules", () => {
		// Every 60 min all day, sim day.
		const r = rule({ mode: "every", everyMinutes: 60, start: "00:00", end: "23:59", amount: "day" });
		const from = new Date("2026-01-01T00:10:00");
		const fires = nextFires([r], from, 3);
		assert.strictEqual(fires.length, 3);
		assert.ok(fires[0]!.at < fires[1]!.at);
		assert.ok(fires[1]!.at < fires[2]!.at);
		// ~1 hour apart.
		assert.ok(fires[1]!.at - fires[0]!.at >= 59 * 60_000);
	});

	test("interleaves two rules and carries each rule's amount", () => {
		const daily = rule({ mode: "at", times: ["09:00"], amount: "week" });
		const nightly = rule({ mode: "at", times: ["21:00"], amount: "day" });
		const from = new Date("2026-01-01T06:00:00");
		const fires = nextFires([daily, nightly], from, 2);
		assert.strictEqual(fires[0]!.amount, "week"); // 9am comes first
		assert.strictEqual(fires[1]!.amount, "day"); // then 9pm
	});

	test("ignores disabled rules", () => {
		const off = rule({ enabled: false, mode: "at", times: ["09:00"] });
		assert.strictEqual(nextFires([off], new Date("2026-01-01T00:00:00"), 5).length, 0);
	});
});

describe("projectFires", () => {
	const fire = (amount: "day" | "week" | "month", at = 0) => ({ at, amount });

	const days = (n: number, from = 1): PreviewDay[] =>
		Array.from({ length: n }, (_, i) => ({ day: from + i, numGames: 5 }));

	test("a day sim covers exactly one league day", () => {
		const p = projectFires([fire("day")], days(10), amountDays, undefined);
		assert.strictEqual(p.length, 1);
		assert.strictEqual(p[0]!.fromDay, 1);
		assert.strictEqual(p[0]!.toDay, 1);
		assert.strictEqual(p[0]!.numDays, 1);
		assert.strictEqual(p[0]!.numGames, 5);
	});

	test("a week sim covers seven days and sums their games", () => {
		const p = projectFires([fire("week")], days(10), amountDays, undefined);
		assert.strictEqual(p[0]!.fromDay, 1);
		assert.strictEqual(p[0]!.toDay, 7);
		assert.strictEqual(p[0]!.numDays, 7);
		assert.strictEqual(p[0]!.numGames, 35);
	});

	test("consecutive fires advance the cursor without overlap", () => {
		const p = projectFires([fire("day"), fire("day"), fire("week")], days(20), amountDays, undefined);
		assert.deepEqual(
			p.map((f) => [f.fromDay, f.toDay]),
			[
				[1, 1],
				[2, 2],
				[3, 9],
			],
		);
	});

	test("caps the last sim at the end of the schedule and flags the phase end", () => {
		// 3 days left, a week sim should only cover those 3 and note the transition.
		const p = projectFires([fire("week")], days(3), amountDays, "Regular season ends, playoffs begin");
		assert.strictEqual(p.length, 1);
		assert.strictEqual(p[0]!.numDays, 3);
		assert.ok(p[0]!.endsPhase);
		assert.ok(p[0]!.events.includes("Regular season ends, playoffs begin"));
	});

	test("stops projecting once the schedule is exhausted", () => {
		// Two day-sims but only one day left → only one projected fire.
		const p = projectFires([fire("day"), fire("day")], days(1), amountDays, undefined);
		assert.strictEqual(p.length, 1);
	});

	test("surfaces trade-deadline and All-Star days within a sim's range", () => {
		const calendar: PreviewDay[] = [
			{ day: 40, numGames: 6 },
			{ day: 41, numGames: 0, tradeDeadline: true },
			{ day: 42, numGames: 4, allStar: true },
		];
		const p = projectFires([fire("week")], calendar, amountDays, undefined);
		assert.ok(p[0]!.events.includes("Trade deadline"));
		assert.ok(p[0]!.events.includes("All-Star Game"));
		assert.strictEqual(p[0]!.numGames, 10);
	});
});
