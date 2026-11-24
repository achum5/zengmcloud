import { assert, describe, test } from "vitest";
import playThroughInjuriesFactor, {
	playThroughInjuriesDayFactor,
	playingThroughInjury,
} from "./playThroughInjuriesFactor.ts";

describe("playing through injuries", () => {
	test("classic: by games left", () => {
		assert.deepStrictEqual(
			playingThroughInjury({ gamesRemaining: 2 }, 2, undefined),
			{ playingThrough: true, factor: playThroughInjuriesFactor(2) },
		);
		assert.isFalse(
			playingThroughInjury({ gamesRemaining: 3 }, 2, undefined).playingThrough,
		);
		// Healthy isn't playing through anything.
		assert.deepStrictEqual(
			playingThroughInjury({ gamesRemaining: 0 }, 2, undefined),
			{ playingThrough: false, factor: 1 },
		);
	});

	test("on the calendar in days: every day, the performance prorated", () => {
		const byDays = { days: 1, rate: 0.5 };
		// One day left at half a classic day a day: as a classic half game.
		const oneDay = playingThroughInjury(
			{ gamesRemaining: 1, daysRemaining: 1 },
			0,
			byDays,
		);
		assert.isTrue(oneDay.playingThrough);
		assert.closeTo(oneDay.factor, 0.9875, 1e-9);
		assert.isFalse(
			playingThroughInjury({ gamesRemaining: 1, daysRemaining: 2 }, 5, byDays)
				.playingThrough,
		);
		// Two days at that rate play like one classic game.
		assert.closeTo(
			playThroughInjuriesDayFactor(2, 0.5),
			playThroughInjuriesFactor(1),
			1e-9,
		);
		// No days known (a season off the calendar): the classic setting.
		assert.isTrue(
			playingThroughInjury({ gamesRemaining: 3 }, 3, byDays).playingThrough,
		);
	});
});
