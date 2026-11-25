import { assert, describe, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { PLAYOFF_CLASSIC_DAYS_PER_DAY } from "./calendarTicks.ts";
import { playThroughDays, playThroughRate } from "./classicDays.ts";

describe("Play Through Injuries in days", () => {
	test("the user's setting, the same days every day", () => {
		resetG();
		g.setWithoutSavingToDB("userTids", [0]);
		g.setWithoutSavingToDB("spectator", false);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		const rate = 89 / 170;
		const clock = { rate, at: () => 0 };
		assert.strictEqual(playThroughRate(false, clock), rate);
		assert.strictEqual(
			playThroughRate(true, clock),
			PLAYOFF_CLASSIC_DAYS_PER_DAY,
		);

		const cavs = {
			tid: 0,
			playThroughInjuries: [1, 4] as [number, number],
		};
		// Set in days: those days.
		assert.deepStrictEqual(
			playThroughDays({ ...cavs, playThroughInjuryDays: [2, 9] }, false, rate),
			{ days: 2, rate },
		);
		// Never set in days: the classic setting as days at the stretch's pace,
		// the same whatever the date.
		assert.deepStrictEqual(playThroughDays(cavs, false, rate), {
			days: 2,
			rate,
		});
		assert.strictEqual(
			playThroughDays(cavs, true, PLAYOFF_CLASSIC_DAYS_PER_DAY)?.days,
			10,
		);
		assert.strictEqual(
			playThroughDays({ tid: 0, playThroughInjuries: [0, 0] }, false, rate)
				?.days,
			0,
		);
		// A CPU team keeps its classic setting.
		assert.isUndefined(playThroughDays({ ...cavs, tid: 1 }, false, rate));
		// Off the calendar.
		assert.isUndefined(playThroughDays(cavs, false, undefined));
	});
});
