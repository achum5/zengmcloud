import { assert, describe, test } from "vitest";
import type { AutoPlayPreviewData, PreviewDay } from "./autoPlayPreview.ts";
import { newRule, prevFireForRule, type ScheduleRule } from "./scheduleTime.ts";
import { formatSimWindow, simWindows, simWindowsByDate } from "./simWindows.ts";

const rule = (patch: Partial<ScheduleRule>): ScheduleRule => ({
	...newRule(),
	...patch,
});

const every15 = rule({
	mode: "every",
	everyMinutes: 15,
	start: "00:00",
	end: "23:59",
	amount: "day",
});

const at = (s: string) => new Date(s).getTime();

const preview = (
	days: Partial<PreviewDay>[],
	season = 2026,
): AutoPlayPreviewData => ({
	phase: 1,
	season,
	upcomingDays: days.map((day, i) => ({
		day: 40 + i,
		date: `2026-12-${String(10 + i).padStart(2, "0")}`,
		numGames: 5,
		...day,
	})),
	amountDays: { day: 1, week: 7, month: 30 },
});

// Plain spaces, whatever the device's formatter puts around the dash.
const plain = (s: string) => s.replace(/[\u00a0\u2009\u202f]/gu, " ");

describe("prevFireForRule", () => {
	test("every N minutes: the slot at or before now", () => {
		const now = new Date("2026-01-07T13:20:00");
		assert.strictEqual(
			prevFireForRule(every15, now),
			at("2026-01-07T13:15:00"),
		);
		assert.strictEqual(
			prevFireForRule(every15, new Date("2026-01-07T13:15:00")),
			at("2026-01-07T13:15:00"),
		);
	});

	test("an overnight window opened yesterday", () => {
		const r = rule({
			mode: "every",
			everyMinutes: 30,
			start: "22:00",
			end: "02:00",
		});
		assert.strictEqual(
			prevFireForRule(r, new Date("2026-01-07T01:10:00")),
			at("2026-01-07T01:00:00"),
		);
		// Outside the window: its last slot last night.
		assert.strictEqual(
			prevFireForRule(r, new Date("2026-01-07T15:00:00")),
			at("2026-01-07T02:00:00"),
		);
	});

	test("fixed times, on chosen days of the week", () => {
		// 2026-01-07 is a Wednesday; Mondays only.
		const r = rule({ mode: "at", times: ["20:00"], days: [1] });
		assert.strictEqual(
			prevFireForRule(r, new Date("2026-01-07T21:00:00")),
			at("2026-01-05T20:00:00"),
		);
	});

	test("agrees with the next fire: the one before it", () => {
		const r = rule({
			mode: "every",
			everyMinutes: 7,
			start: "09:05",
			end: "17:00",
		});
		const now = new Date("2026-01-07T12:00:00");
		const prev = prevFireForRule(r, now)!;
		assert.ok(prev <= now.getTime());
		assert.ok(now.getTime() - prev < 7 * 60_000);
	});
});

describe("simWindows", () => {
	test("each day runs from the sim before it to the sim that plays it", () => {
		const windows = simWindows({
			now: new Date("2026-01-07T13:20:00"),
			preview: preview([{}, {}, {}]),
			rules: [every15],
		});
		assert.deepStrictEqual(windows.get(40), {
			start: at("2026-01-07T13:15:00"),
			end: at("2026-01-07T13:30:00"),
		});
		assert.deepStrictEqual(windows.get(41), {
			start: at("2026-01-07T13:30:00"),
			end: at("2026-01-07T13:45:00"),
		});
		assert.deepStrictEqual(windows.get(42), {
			start: at("2026-01-07T13:45:00"),
			end: at("2026-01-07T14:00:00"),
		});
	});

	test("nothing from a stop the room still has to ready up for", () => {
		const windows = simWindows({
			now: new Date("2026-01-07T13:20:00"),
			preview: preview([{}, { simStop: true }, {}]),
			rules: [every15],
		});
		assert.deepStrictEqual([...windows.keys()], [40]);
	});

	test("a sim that plays a week gives its days one window", () => {
		const windows = simWindows({
			now: new Date("2026-01-07T13:20:00"),
			preview: preview(Array.from({ length: 9 }, () => ({}))),
			rules: [{ ...every15, amount: "week" }],
		});
		for (let day = 40; day <= 46; day++) {
			assert.strictEqual(windows.get(day)!.end, at("2026-01-07T13:30:00"));
		}
		assert.strictEqual(windows.get(47)!.start, at("2026-01-07T13:30:00"));
		assert.strictEqual(windows.get(47)!.end, at("2026-01-07T13:45:00"));
	});

	test("the simmer's own stops", () => {
		const base = {
			now: new Date("2026-01-07T13:20:00"),
			preview: preview(Array.from({ length: 6 }, () => ({}))),
			rules: [every15],
		};
		assert.deepStrictEqual(
			[...simWindows({ ...base, stops: { stopAfterSims: 2 } }).keys()],
			[40, 41],
		);
		assert.deepStrictEqual(
			[
				...simWindows({
					...base,
					stops: { stopAt: at("2026-01-07T14:00:00") },
				}).keys(),
			],
			[40, 41],
		);
		assert.deepStrictEqual(
			[
				...simWindows({
					...base,
					stops: { stopAfter: { season: 2026, day: 42 } },
				}).keys(),
			],
			[40, 41, 42],
		);
		// Already past it (an older season's stop): it stops at the next sim.
		assert.strictEqual(
			simWindows({
				...base,
				stops: { stopAfter: { season: 2025, day: 80 } },
			}).size,
			0,
		);
	});

	test("no rules, no times", () => {
		const windows = simWindows({
			now: new Date("2026-01-07T13:20:00"),
			preview: preview([{}, {}]),
			rules: [{ ...every15, enabled: false }],
		});
		assert.strictEqual(windows.size, 0);
	});

	test("by night: the trade deadline shares its night with the games", () => {
		const p = preview([{}, { date: "2026-12-11" }, { date: "2026-12-11" }]);
		const byDate = simWindowsByDate(
			simWindows({
				now: new Date("2026-01-07T13:20:00"),
				preview: p,
				rules: [every15],
			}),
			p,
		);
		assert.deepStrictEqual(byDate.get("2026-12-11"), {
			start: at("2026-01-07T13:30:00"),
			end: at("2026-01-07T14:00:00"),
		});
	});
});

describe("formatSimWindow", () => {
	const now = at("2026-01-07T13:20:00");

	test("today: just the times", () => {
		const text = plain(
			formatSimWindow(
				{ start: at("2026-01-07T13:15:00"), end: at("2026-01-07T13:30:00") },
				now,
			),
		);
		assert.match(text, /^1:15 (PM )?– 1:30 PM$/);
	});

	test("another day says which", () => {
		const text = plain(
			formatSimWindow(
				{ start: at("2026-01-08T13:15:00"), end: at("2026-01-08T13:30:00") },
				now,
			),
		);
		assert.match(text, /^Tomorrow 1:15 (PM )?– 1:30 PM$/);
		assert.match(
			plain(
				formatSimWindow(
					{
						start: at("2026-01-07T23:00:00"),
						end: at("2026-01-08T09:00:00"),
					},
					now,
				),
			),
			/^Today 11:00 PM – Tomorrow 9:00 AM$/,
		);
		// (Wed Jan 7: a Wednesday a week on has its date, so it's not today.)
		assert.match(
			plain(
				formatSimWindow(
					{
						start: at("2026-01-13T20:00:00"),
						end: at("2026-01-14T20:00:00"),
					},
					now,
				),
			),
			/^Tue 8:00 PM – Wed 1\/14 8:00 PM$/,
		);
	});
});
