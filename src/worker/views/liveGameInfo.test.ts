import { assert, describe, test } from "vitest";
import {
	bracketAsOf,
	leadersOf,
	playInOf,
	recentOf,
	seriesOf,
	standingsAsOf,
	streakBefore,
	totalAsOf,
} from "./liveGameInfo.ts";
import {
	expectedTotalFromRates,
	leagueAvgGameTotal,
	toHalfPointLine,
} from "../../common/sportsbookOdds.ts";

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

describe("a play-in game", () => {
	const team = (tid: number, seed: number) => ({ tid, seed, cid: 0 });
	const playIns = [
		[
			{ home: team(7, 7), away: team(8, 8), gids: [100] },
			{ home: team(9, 9), away: team(10, 10), gids: [101] },
			{ home: team(7, 7), away: team(9, 9) },
		],
	] as any;
	test("each team's seed, and which of the play-in's games it is", () => {
		assert.deepEqual(playInOf(game(101, 1, [9, 0], [10, 0], true), playIns), {
			seeds: { 9: 9, 10: 10 },
			stage: 1,
		});
		// (The last game, for the final spot: its game not known yet.)
		assert.deepEqual(playInOf(game(102, 2, [7, 0], [9, 0], true), playIns), {
			seeds: { 7: 7, 9: 9 },
			stage: 2,
		});
	});
	test("not a play-in game: a series between two other teams, or none", () => {
		assert.isUndefined(playInOf(game(103, 3, [1, 0], [8, 0], true), playIns));
		assert.isUndefined(
			playInOf(game(101, 1, [9, 0], [10, 0], true), undefined),
		);
	});
});

describe("the over/under at the tip", () => {
	// Teams 1 and 2 meet on day 10; 3 and 4 fill out the league.
	const watched = game(20, 10, [1, 0], [2, 0]);
	const played = [
		game(1, 1, [1, 110], [3, 100]),
		game(2, 1, [2, 95], [4, 105]),
		game(3, 2, [3, 120], [2, 118]),
		game(4, 2, [4, 99], [1, 101]),
		// The game itself, the same night's others and anything after: not
		// counted.
		watched,
		game(21, 10, [3, 140], [4, 140]),
		game(30, 12, [1, 150], [2, 150]),
		// Nor the playoffs.
		game(40, 3, [1, 200], [2, 200], true),
	];

	test("is the sportsbook's line from the season's scoring before the night", () => {
		const rate = (pts: number[], opp: number[]) => ({
			gp: pts.length,
			pts: pts.reduce((a, b) => a + b, 0) / pts.length,
			oppPts: opp.reduce((a, b) => a + b, 0) / opp.length,
		});
		const rates = [
			rate([110, 101], [100, 99]), // 1
			rate([95, 118], [105, 120]), // 2
			rate([100, 120], [110, 118]), // 3
			rate([105, 99], [95, 101]), // 4
		];
		const prior = { total: 212, games: 150 };
		const expected = toHalfPointLine(
			expectedTotalFromRates({
				home: rates[0]!,
				away: rates[1]!,
				leagueAvgTotal: leagueAvgGameTotal(rates, prior),
			}),
		);
		assert.strictEqual(totalAsOf(watched, played, [2, 2], prior), expected);
		assert.strictEqual(expected % 1, 0.5);
	});

	test("no line when the games found don't add up to a team's record", () => {
		// (An old season's box scores deleted.)
		assert.isUndefined(
			totalAsOf(watched, played, [5, 2], { total: 212, games: 150 }),
		);
	});

	test("the first night of the season: the season's starting guess", () => {
		const first = game(1, 1, [1, 0], [2, 0]);
		assert.strictEqual(
			totalAsOf(first, [first], [0, 0], { total: 212.2, games: 150 }),
			212.5,
		);
	});
});

describe("each team's leading scorer going in", () => {
	// A game with box lines: [pid, pts, min] for each man.
	const box = (
		gid: number,
		day: number,
		home: [number, [number, number, number][]],
		away: [number, [number, number, number][]],
		playoffs = false,
	) =>
		({
			gid,
			day,
			playoffs,
			teams: [home, away].map(([tid, men]) => ({
				tid,
				pts: 0,
				players: men.map(([pid, pts, min]) => ({
					pid,
					name: `P${pid}`,
					pts,
					min,
					orb: 1,
					drb: 4,
					ast: 3,
				})),
			})),
		}) as any;

	test("the season before the tip only: not this game, not later ones", () => {
		const watched = box(
			30,
			10,
			[
				1,
				[
					[11, 50, 30],
					[12, 0, 30],
				],
			],
			[2, [[21, 0, 30]]],
		);
		const played = [
			box(
				5,
				2,
				[
					1,
					[
						[11, 10, 30],
						[12, 20, 30],
					],
				],
				[3, [[31, 99, 30]]],
			),
			box(
				6,
				3,
				[2, [[21, 30, 30]]],
				[
					1,
					[
						[11, 12, 30],
						[12, 22, 30],
					],
				],
			),
			watched,
			box(
				40,
				12,
				[
					1,
					[
						[11, 90, 30],
						[12, 0, 30],
					],
				],
				[2, [[21, 0, 30]]],
			),
		];
		const leaders = leadersOf(watched, played)!;
		assert.strictEqual(leaders[0].pid, 12);
		assert.strictEqual(leaders[0].tid, 1);
		assert.strictEqual(leaders[0].pts, 21);
		assert.strictEqual(leaders[0].trb, 5);
		assert.strictEqual(leaders[0].gp, 2);
		assert.strictEqual(leaders[1].pid, 21);
		assert.strictEqual(leaders[1].pts, 30);
	});

	test("one big night in a few games doesn't make the leader", () => {
		const watched = box(
			30,
			10,
			[
				1,
				[
					[11, 0, 30],
					[12, 0, 30],
				],
			],
			[2, [[21, 0, 30]]],
		);
		const played = [
			box(
				1,
				1,
				[
					1,
					[
						[11, 15, 30],
						[12, 40, 30],
					],
				],
				[2, [[21, 9, 30]]],
			),
			box(
				2,
				2,
				[
					1,
					[
						[11, 15, 30],
						[12, 0, 0],
					],
				],
				[2, [[21, 9, 30]]],
			),
			box(
				3,
				3,
				[
					1,
					[
						[11, 15, 30],
						[12, 0, 0],
					],
				],
				[2, [[21, 9, 30]]],
			),
		];
		assert.strictEqual(leadersOf(watched, played)![0].pid, 11);
	});

	test("none on the first night; a playoff opener goes by the season", () => {
		const opener = box(1, 1, [1, [[11, 0, 30]]], [2, [[21, 0, 30]]]);
		assert.isUndefined(leadersOf(opener, [opener]));
		const regular = box(1, 1, [1, [[11, 20, 30]]], [2, [[21, 10, 30]]]);
		const g1 = box(50, 90, [1, [[11, 0, 30]]], [2, [[21, 0, 30]]], true);
		assert.strictEqual(leadersOf(g1, [regular, g1])![0].pts, 20);
		const p1 = box(50, 90, [1, [[11, 40, 30]]], [2, [[21, 2, 30]]], true);
		const g2 = box(51, 92, [1, [[11, 0, 30]]], [2, [[21, 0, 30]]], true);
		const l = leadersOf(g2, [regular, p1, g2])!;
		assert.strictEqual(l[0].pts, 40);
		assert.strictEqual(l[1].pts, 2);
	});
});
