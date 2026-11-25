import { assert, describe, test } from "vitest";
import playThroughInjuriesFactor, {
	injuryAtGame,
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

	test("a game ahead: by the days he'll have left that night", () => {
		const byDays = { days: 2, rate: 89 / 170 };
		// 3 days left today, his team's next game the day after tomorrow:
		// he'll be at 2 days then, so a 2-day setting plays him - however his
		// classic days left round.
		const knox = { gamesRemaining: 2, daysRemaining: 3 };
		assert.isFalse(injuryAtGame(knox, 1, byDays).plays);
		const thatNight = injuryAtGame(knox, 1, byDays, { games: 0, days: 1 });
		assert.isTrue(thatNight.plays);
		assert.closeTo(
			thatNight.factor,
			playThroughInjuriesDayFactor(2, byDays.rate),
			1e-9,
		);
		// Healed by then, by his classic days, as the sim heals him.
		assert.deepStrictEqual(
			injuryAtGame(knox, 0, byDays, { games: 2, days: 1 }),
			{ healthy: true, plays: true, factor: 1 },
		);
		// Still hurt that night: never under a day.
		assert.closeTo(
			injuryAtGame(knox, 0, { days: 1, rate: 0.5 }, { games: 1, days: 5 })
				.factor,
			playThroughInjuriesDayFactor(1, 0.5),
			1e-9,
		);
		// Off the calendar: classic days ahead against the classic setting.
		assert.isTrue(
			injuryAtGame({ gamesRemaining: 3 }, 1, undefined, { games: 2, days: 0 })
				.plays,
		);
	});
});
