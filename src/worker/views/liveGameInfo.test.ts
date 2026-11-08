import { assert, describe, test } from "vitest";
import {
	bracketAsOf,
	recentOf,
	seriesOf,
	standingsAsOf,
	streakBefore,
} from "./liveGameInfo.ts";

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

	test("the standings as they stood: at the tip, the day's games not in; that night, all of them", () => {
		const watched = game(20, 10, [1, 99], [2, 90]);
		const played = [
			game(5, 3, [2, 100], [1, 95]),
			game(18, 10, [3, 90], [4, 80]), // the same day
			watched,
			game(40, 30, [1, 80], [2, 70]), // later
		];
		const tip = standingsAsOf(watched, played, false);
		assert.deepEqual(tip.get(1), { won: 0, lost: 1, tied: 0, streak: -1 });
		assert.isUndefined(tip.get(3));
		const night = standingsAsOf(watched, played, true);
		assert.deepEqual(night.get(1), { won: 1, lost: 1, tied: 0, streak: 1 });
		assert.deepEqual(night.get(3), { won: 1, lost: 0, tied: 0, streak: 1 });
		// (Nothing from after it.)
		assert.strictEqual(night.get(2)!.won + night.get(2)!.lost, 2);
	});

	test("each team's last games, latest first", () => {
		const watched = game(20, 10, [1, 99], [2, 90]);
		const played = [
			game(5, 3, [2, 100], [1, 95]),
			game(9, 5, [1, 88], [3, 80]),
			watched,
			game(40, 30, [1, 80], [2, 70]),
		];
		const atTip = recentOf(1, watched, played, false);
		assert.deepEqual(
			atTip.map((x) => x.gid),
			[9, 5],
		);
		assert.deepEqual(atTip[1]!.pts, [95, 100]);
		assert.isFalse(atTip[1]!.home);
		const final = recentOf(1, watched, played, true);
		assert.deepEqual(
			final.map((x) => x.gid),
			[20, 9, 5],
		);
		assert.isTrue(final[0]!.now);
	});
});

describe("a playoff game's bracket, as it stood", () => {
	// A four-team bracket: 1 v 4 and 2 v 3, the winners in the final;
	// best of three (two wins take a series).
	const ps = {
		series: [
			[
				{
					home: { tid: 1, seed: 1, cid: 0, won: 2 },
					away: { tid: 4, seed: 4, cid: 0, won: 0 },
				},
				{
					home: { tid: 2, seed: 2, cid: 0, won: 1 },
					away: { tid: 3, seed: 3, cid: 0, won: 2 },
				},
			],
			[
				{
					home: { tid: 1, seed: 1, cid: 0, won: 0 },
					away: { tid: 3, seed: 3, cid: 0, won: 0 },
				},
			],
		],
	} as any;
	const info = {
		toWin: () => 2,
		roundName: (r: number) => (r === 0 ? "1st round" : "Finals"),
		team: (tid: number) => ({ tid, abbrev: `T${tid}` }),
	};
	const played = [
		game(1, 1, [1, 100], [4, 90], true),
		game(2, 1, [2, 100], [3, 90], true),
		game(3, 2, [4, 90], [1, 100], true),
		game(4, 2, [3, 100], [2, 90], true),
		game(5, 3, [2, 90], [3, 100], true),
		game(6, 4, [1, 100], [3, 90], true),
	];

	test("at the tip: this game and every one after it left out", () => {
		// Game 5 (2 v 3, the deciding game), watched live: 1-1 going in, and
		// the final has only the team that had won its way there.
		const b = bracketAsOf(played[4], played, ps, info, false)!;
		const [r0, r1] = b;
		assert.deepEqual(
			r0!.series.map((s) => s.teams.map((t) => t?.won)),
			[
				[2, 0],
				[1, 1],
			],
		);
		assert.isTrue(r0!.series[1]!.now);
		assert.deepEqual(
			r1!.series[0]!.teams.map((t) => t?.tid),
			[1, undefined],
		);
	});

	test("on its box score: this game counted, its winner on through", () => {
		const b = bracketAsOf(played[4], played, ps, info, true)!;
		assert.deepEqual(
			b[0]!.series[1]!.teams.map((t) => t?.won),
			[1, 2],
		);
		assert.deepEqual(
			b[1]!.series[0]!.teams.map((t) => t?.tid),
			[1, 3],
		);
		// (And not game 6, played after it.)
		assert.deepEqual(
			b[1]!.series[0]!.teams.map((t) => t?.won),
			[0, 0],
		);
	});
});
