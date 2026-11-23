import { assert, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g, helpers } from "../../util/index.ts";
import addDaysToSchedule from "../season/addDaysToSchedule.ts";
import newSchedule from "../season/newSchedule.ts";
import { countClassicDays } from "./calendarTicks.ts";
import { classicDaysLeftFrom } from "./classicDaysLeft.ts";

type Row = { homeTid: number; awayTid: number; day: number };

describe("classicDaysLeftFrom", () => {
	test("off a calendar, the schedule's own days", () => {
		const schedule: Row[] = [
			{ homeTid: 0, awayTid: 1, day: 10 },
			{ homeTid: 2, awayTid: 3, day: 10 },
			{ homeTid: 0, awayTid: 2, day: 11 },
			{ homeTid: -3, awayTid: -3, day: 12 },
			{ homeTid: 1, awayTid: 3, day: 13 },
		];
		const at = (target?: "tradeDeadline") =>
			classicDaysLeftFrom({
				schedule,
				played: 40,
				classicDays: undefined,
				target,
			});
		assert.strictEqual(at("tradeDeadline"), 2);
		assert.strictEqual(at(), 4);
	});

	test("no deadline left throws, as getDaysLeftSchedule does", () => {
		assert.throws(() =>
			classicDaysLeftFrom({
				schedule: [{ homeTid: 0, awayTid: 1, day: 3 }],
				played: 0,
				classicDays: 10,
				target: "tradeDeadline",
			}),
		);
	});

	test("on a calendar, the classic days the games before it make", () => {
		// 20 games over 10 classic days: two games a day. 10 played, 4 of
		// the 10 left before the deadline (one a night), then the All-Star
		// Game, a day of its own.
		const schedule: Row[] = [
			{ homeTid: 0, awayTid: 1, day: 21 },
			{ homeTid: 2, awayTid: 3, day: 22 },
			{ homeTid: 0, awayTid: 2, day: 23 },
			{ homeTid: 1, awayTid: 3, day: 24 },
			{ homeTid: -3, awayTid: -3, day: 25 },
			{ homeTid: -1, awayTid: -2, day: 26 },
			...[27, 28, 29, 30, 31, 32].map((day) => ({
				homeTid: 0,
				awayTid: 1,
				day,
			})),
		];
		const at = (target?: "tradeDeadline") =>
			classicDaysLeftFrom({ schedule, played: 10, classicDays: 10, target });
		// Four nights, two classic days.
		assert.strictEqual(at("tradeDeadline"), 2);
		// The rest of the season: five game days, the deadline's and the
		// All-Star Game's.
		assert.strictEqual(at(), 7);
	});

	// The point: the deadline's last days come as often as in the same
	// league's classic season, each about two nights.
	test("a calendar counts down to the deadline as the classic season does", () => {
		resetG();
		g.setWithoutSavingToDB("season", 2027);
		const teams = helpers.getTeamsDefault().map((t) => ({
			tid: t.tid,
			seasonAttrs: { cid: t.cid, did: t.did },
		}));
		const tids = newSchedule(teams);
		const classicDays = countClassicDays(tids);
		const classic = addDaysToSchedule(
			tids.map(([homeTid, awayTid]) => ({ homeTid, awayTid })),
		);
		// The same games on a calendar: each classic day two nights.
		const byDay = new Map<number, Row[]>();
		for (const row of classic) {
			byDay.set(row.day, [...(byDay.get(row.day) ?? []), row]);
		}
		const calendar: Row[] = [];
		let night = 0;
		for (const rows of byDay.values()) {
			const half = Math.ceil(rows.length / 2);
			night += 1;
			for (const row of rows.slice(0, half)) {
				calendar.push({ ...row, day: night });
			}
			if (rows.length > half) {
				night += 1;
				for (const row of rows.slice(half)) {
					calendar.push({ ...row, day: night });
				}
			}
		}
		// Days left to the deadline after each day (or night) before it, in a
		// window of the last five.
		const window = (schedule: Row[], days: number | undefined) => {
			const seen: number[] = [];
			let played = 0;
			for (let i = 0; schedule[i]!.homeTid !== -3; i++) {
				played += schedule[i]!.homeTid >= 0 ? 1 : 0;
				if (schedule[i + 1]!.day === schedule[i]!.day) {
					continue;
				}
				const left = classicDaysLeftFrom({
					schedule: schedule.slice(i + 1),
					played,
					classicDays: days,
					target: "tradeDeadline",
				});
				if (left < 5) {
					seen.push(left);
				}
			}
			return seen;
		};
		const days = window(classic, undefined);
		const nights = window(calendar, classicDays);
		assert.deepStrictEqual(days, [4, 3, 2, 1, 0]);
		// Every one of the five days, in order, over about twice the nights.
		assert.deepStrictEqual([...new Set(nights)], [4, 3, 2, 1, 0]);
		assert.isAtLeast(nights.length, 8);
		assert.isAtMost(nights.length, 12);
	});
});
