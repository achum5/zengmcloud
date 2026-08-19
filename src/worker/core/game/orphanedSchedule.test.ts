import { assert, describe, test } from "vitest";
import { orphanedScheduleGids } from "./orphanedSchedule.ts";

describe("orphanedScheduleGids", () => {
	const day = (...gids: number[]) => gids.map((gid) => ({ gid }));

	test("a healthy day has no orphans", () => {
		assert.deepStrictEqual(
			orphanedScheduleGids(day(1, 2, 3), () => false),
			[],
		);
	});

	// The field incident: day 95 of the playoffs held eight games, three of
	// them already carrying final box scores (their results synced to every
	// device; two of the three schedule-row deletions did not). The sim-day
	// fence rightly refused to re-sim those gids, so the day was wedged until
	// the orphans were swept.
	test("scheduled games with saved results are the orphans", () => {
		const played = new Set([12823, 12820]);
		assert.deepStrictEqual(
			orphanedScheduleGids(day(12820, 12821, 12823, 12824, 12825), (gid) =>
				played.has(gid),
			),
			[12820, 12823],
		);
	});

	test("a fully played day is entirely orphans", () => {
		assert.deepStrictEqual(
			orphanedScheduleGids(day(7, 8), () => true),
			[7, 8],
		);
	});

	test("an empty day asks nothing", () => {
		let called = false;
		assert.deepStrictEqual(
			orphanedScheduleGids([], () => {
				called = true;
				return true;
			}),
			[],
		);
		assert.strictEqual(called, false);
	});
});
