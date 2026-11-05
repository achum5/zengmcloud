import { assert, describe, test } from "vitest";
import { seriesOf, streakBefore } from "./liveGameInfo.ts";

const game = (
	gid: number,
	day: number,
	home: [number, number],
	away: [number, number],
	playoffs = false,
) =>
	({
		gid,
		day,
		playoffs,
		teams: [
			{ tid: home[0], pts: home[1] },
			{ tid: away[0], pts: away[1] },
		],
	}) as any;

describe("the gamecast's season series and streaks", () => {
	test("this game and later meetings show no score; earlier ones do", () => {
		const watched = game(20, 10, [1, 99], [2, 90]);
		const played = [
			game(5, 3, [2, 100], [1, 95]),
			watched,
			game(40, 30, [1, 80], [2, 70]),
			game(6, 3, [3, 100], [4, 95]),
		];
		const scheduled = [{ gid: 60, day: 50, homeTid: 2, awayTid: 1 }];
		const series = seriesOf(watched, played, scheduled);
		assert.deepEqual(
			series.map((s) => s.gid),
			[5, 20, 40, 60],
		);
		assert.deepEqual(series[0]!.pts, [100, 95]);
		assert.isTrue(series[1]!.now);
		assert.isUndefined(series[1]!.pts);
		assert.isUndefined(series[2]!.pts);
		assert.isUndefined(series[3]!.pts);
	});

	test("a game not in the store yet is still listed as the one on now", () => {
		const watched = game(20, 10, [1, 0], [2, 0]);
		const series = seriesOf(watched, [], []);
		assert.strictEqual(series.length, 1);
		assert.isTrue(series[0]!.now);
	});

	test("a streak counts only the games before this one", () => {
		const watched = game(20, 10, [1, 99], [2, 90]);
		const played = [
			game(1, 1, [1, 90], [3, 80]), // won
			game(2, 2, [4, 90], [1, 80]), // lost
			game(3, 3, [1, 90], [5, 80]), // won
			game(4, 4, [6, 70], [1, 80]), // won
			watched, // won, but not to be counted
		];
		assert.strictEqual(streakBefore(1, watched, played), 2);
		assert.strictEqual(streakBefore(2, watched, played), 0);
	});

	test("a finished game's page: every meeting played so far with its score, this one too", () => {
		const watched = game(20, 10, [1, 99], [2, 90]);
		const played = [
			game(5, 3, [2, 100], [1, 95]),
			watched,
			game(40, 30, [1, 80], [2, 70]),
		];
		const series = seriesOf(watched, played, [], true);
		assert.deepEqual(series[0]!.pts, [100, 95]);
		assert.isTrue(series[1]!.now);
		assert.deepEqual(series[1]!.pts, [99, 90]);
		assert.deepEqual(series[2]!.pts, [80, 70]);
	});
});
