import { assert, beforeEach, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g, helpers } from "../../util/index.ts";
import newCalendarSchedule, {
	assembleDatedSchedule,
} from "./newCalendarSchedule.ts";
import addDaysToSchedule from "./addDaysToSchedule.ts";
import {
	isAllStarGame,
	isTradeDeadline,
} from "../../../common/specialGames.ts";
import { weekdayOf } from "../../../common/calendarDates.ts";

const teams = () =>
	helpers.getTeamsDefault().map((t) => ({
		tid: t.tid,
		seasonAttrs: {
			cid: t.cid,
			did: t.did,
			abbrev: t.abbrev,
			region: t.region,
		},
	}));

const check = (tids: [number, number][], dates: string[]) => {
	assert.strictEqual(tids.length, dates.length);
	for (let i = 1; i < dates.length; i++) {
		assert.isTrue(dates[i - 1]! <= dates[i]!, "in date order");
	}
	const schedule = addDaysToSchedule(
		tids.map(([homeTid, awayTid], i) => ({
			homeTid,
			awayTid,
			date: dates[i],
		})),
	);
	// One night, one day - the All-Star Game and the trade deadline each get
	// a day of their own.
	const dateByDay = new Map<number, string>();
	const dayByDate = new Map<string, Set<number>>();
	for (const game of schedule) {
		const special =
			isAllStarGame(game.homeTid, game.awayTid) ||
			isTradeDeadline(game.homeTid, game.awayTid);
		if (dateByDay.has(game.day)) {
			assert.strictEqual(dateByDay.get(game.day), game.date);
		}
		dateByDay.set(game.day, game.date!);
		if (!special) {
			const days = dayByDate.get(game.date!) ?? new Set();
			days.add(game.day);
			dayByDate.set(game.date!, days);
		}
	}
	for (const days of dayByDate.values()) {
		assert.strictEqual(days.size, 1);
	}
	return schedule;
};

describe("newCalendarSchedule", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("season", 2027);
	});

	test("an NBA-shaped league plays a real season", () => {
		const { tids, dates } = newCalendarSchedule(teams());
		assert.isDefined(dates);
		const schedule = check(tids, dates!);
		const regular = schedule.filter(
			(game) =>
				!isAllStarGame(game.homeTid, game.awayTid) &&
				!isTradeDeadline(game.homeTid, game.awayTid),
		);
		assert.strictEqual(regular.length, 1230);

		const allStar = schedule.find((game) =>
			isAllStarGame(game.homeTid, game.awayTid),
		)!;
		assert.strictEqual(weekdayOf(allStar.date!), 0);
		assert.isFalse(
			schedule.some((game) => game.date === allStar.date && game !== allStar),
		);

		const deadline = schedule.find((game) =>
			isTradeDeadline(game.homeTid, game.awayTid),
		)!;
		assert.isTrue(deadline.date! < allStar.date!);
		const next = schedule[schedule.indexOf(deadline) + 1]!;
		assert.isAbove(next.day, deadline.day);
	});

	test("any other league gets a generated calendar", () => {
		g.setWithoutSavingToDB("numGames", 60);
		const { tids, dates } = newCalendarSchedule(teams());
		assert.isDefined(dates);
		const schedule = check(tids, dates!);
		const counts = new Map<number, number>();
		for (const game of schedule) {
			if (
				!isAllStarGame(game.homeTid, game.awayTid) &&
				!isTradeDeadline(game.homeTid, game.awayTid)
			) {
				counts.set(game.homeTid, (counts.get(game.homeTid) ?? 0) + 1);
				counts.set(game.awayTid, (counts.get(game.awayTid) ?? 0) + 1);
			}
		}
		for (const count of counts.values()) {
			assert.strictEqual(count, 60);
		}
	});

	test("with the setting off, classic numbered days", () => {
		g.setWithoutSavingToDB("nbaCalendar", false);
		const { dates } = newCalendarSchedule(teams());
		assert.isUndefined(dates);
	});

	test("the specials sit before that night's games", () => {
		const { tids, dates } = assembleDatedSchedule(
			[
				{ homeTid: 0, awayTid: 1, date: "2027-02-03" },
				{ homeTid: 2, awayTid: 3, date: "2027-02-04" },
				{ homeTid: 0, awayTid: 2, date: "2027-02-05" },
				{ homeTid: 1, awayTid: 3, date: "2027-02-18" },
			],
			"2027-02-14",
			0.4,
		);
		assert.deepStrictEqual(dates, [
			"2027-02-03",
			"2027-02-04",
			"2027-02-04",
			"2027-02-05",
			"2027-02-14",
			"2027-02-18",
		]);
		assert.isTrue(isTradeDeadline(...tids[1]!));
		assert.isTrue(isAllStarGame(...tids[4]!));
	});
});
