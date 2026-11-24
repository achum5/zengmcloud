import { assert, describe, test } from "vitest";
import { OFF_DAY_TIDS } from "../../../common/specialGames.ts";
import addDaysToSchedule from "./addDaysToSchedule.ts";
import { playoffOffDates, withOffDays } from "./offDays.ts";

describe("withOffDays", () => {
	test("a day for each date with no games, between the first night and the last", () => {
		const { tids, dates } = withOffDays(
			[
				[0, 1],
				[2, 3],
				[0, 2],
				[-3, -3],
				[1, 3],
			],
			["2026-11-04", "2026-11-04", "2026-11-06", "2026-11-09", "2026-11-09"],
		);
		assert.deepStrictEqual(dates, [
			"2026-11-04",
			"2026-11-04",
			"2026-11-05",
			"2026-11-06",
			"2026-11-07",
			"2026-11-08",
			"2026-11-09",
			"2026-11-09",
		]);
		assert.deepStrictEqual(tids, [
			[0, 1],
			[2, 3],
			[...OFF_DAY_TIDS],
			[0, 2],
			[...OFF_DAY_TIDS],
			[...OFF_DAY_TIDS],
			[-3, -3],
			[1, 3],
		]);
	});

	test("each date is a day of its own, the deadline still apart from its night", () => {
		const { tids, dates } = withOffDays(
			[
				[0, 1],
				[-3, -3],
				[2, 3],
				[0, 2],
			],
			["2027-02-03", "2027-02-05", "2027-02-05", "2027-02-07"],
		);
		const rows = addDaysToSchedule(
			tids.map(([homeTid, awayTid], i) => ({
				homeTid,
				awayTid,
				date: dates[i],
			})),
		);
		assert.deepStrictEqual(
			rows.map((row) => [row.date, row.day]),
			[
				["2027-02-03", 1],
				["2027-02-04", 2],
				["2027-02-05", 3],
				["2027-02-05", 4],
				["2027-02-06", 5],
				["2027-02-07", 6],
			],
		);
	});

	test("nothing to add to back-to-back nights", () => {
		const out = withOffDays(
			[
				[0, 1],
				[2, 3],
			],
			["2026-10-20", "2026-10-21"],
		);
		assert.deepStrictEqual(out.dates, ["2026-10-20", "2026-10-21"]);
	});
});

describe("playoffOffDates", () => {
	test("from the day after the last game to the night before the next", () => {
		assert.deepStrictEqual(
			playoffOffDates({
				lastPlayed: "2027-04-13",
				firstLeft: undefined,
				firstNew: "2027-04-16",
			}),
			["2027-04-14", "2027-04-15"],
		);
	});

	test("not the days already lived", () => {
		assert.deepStrictEqual(
			playoffOffDates({
				lastPlayed: "2027-04-13",
				firstLeft: "2027-04-15",
				firstNew: "2027-04-16",
			}),
			["2027-04-15"],
		);
	});

	test("none before the rest of tonight's games", () => {
		assert.deepStrictEqual(
			playoffOffDates({
				lastPlayed: "2027-04-16",
				firstLeft: "2027-04-16",
				firstNew: "2027-04-16",
			}),
			[],
		);
	});
});
