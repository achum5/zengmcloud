import { assert, describe, test } from "vitest";
import {
	decideLiveGate,
	eventGivesAwayLiveGame,
	nightShown,
	recordDeltas,
	streakAndLastTen,
	teamGoingIn,
	unplaySeries,
} from "./liveGameGate.ts";

const rows = (entries: [number, { day: number; playoffs?: boolean }][]) =>
	new Map(entries);

describe("decideLiveGate", () => {
	test("this device's own live game stays hidden whatever was played since", () => {
		const { hidden } = decideLiveGate({
			watching: new Set([10]),
			room: new Set(),
			rows: rows([[10, { day: 5 }]]),
			maxPlayedDay: () => 9,
		});
		assert.deepStrictEqual([...hidden], [10]);
	});

	test("a league mate's regular season game shows once a later day is played", () => {
		const args = {
			watching: new Set<number>(),
			room: new Set([10]),
			rows: rows([[10, { day: 5 }]]),
		};
		assert.isTrue(
			decideLiveGate({ ...args, maxPlayedDay: () => 5 }).hidden.has(10),
		);
		assert.isFalse(
			decideLiveGate({ ...args, maxPlayedDay: () => 6 }).hidden.has(10),
		);
	});

	test("a playoff game stays hidden, and so does every day after it", () => {
		const { hidden, dayCutoff } = decideLiveGate({
			watching: new Set(),
			room: new Set([10, 11]),
			rows: rows([
				[10, { day: 40, playoffs: true }],
				[11, { day: 41, playoffs: true }],
			]),
			maxPlayedDay: () => 41,
		});
		assert.deepStrictEqual([...hidden].sort(), [10, 11]);
		assert.strictEqual(dayCutoff, 40);
	});

	test("a game not here yet stays hidden, with no cutoff", () => {
		const { hidden, dayCutoff } = decideLiveGate({
			watching: new Set(),
			room: new Set([10]),
			rows: new Map(),
			maxPlayedDay: () => 99,
		});
		assert.isTrue(hidden.has(10));
		assert.isUndefined(dayCutoff);
	});
});

const playedGame = (
	pts: [number, number],
	extra: { overtimes?: number; playoffs?: boolean } = {},
) => ({
	gid: 7,
	overtimes: extra.overtimes ?? 0,
	playoffs: extra.playoffs,
	neutralSite: false,
	teams: [
		{
			tid: 0,
			pts: pts[0],
			won: 10,
			lost: 5,
			tied: 0,
			otl: 2,
			playoffs: { seed: 1, won: 3, lost: 1 },
		},
		{
			tid: 1,
			pts: pts[1],
			won: 6,
			lost: 9,
			tied: 0,
			otl: 0,
			playoffs: { seed: 8, won: 1, lost: 3 },
		},
	],
});

describe("teamGoingIn", () => {
	test("takes the game's result back out of the records it stores", () => {
		const game = playedGame([100, 90]) as any;
		const home = teamGoingIn(game, 0, game.teams[0], true);
		const away = teamGoingIn(game, 1, game.teams[1], true);
		assert.strictEqual(home.won, 9);
		assert.strictEqual(home.lost, 5);
		assert.deepStrictEqual(home.playoffs, { seed: 1, won: 2, lost: 1 });
		assert.strictEqual(away.lost, 8);
		assert.deepStrictEqual(away.playoffs, { seed: 8, won: 1, lost: 2 });
	});

	test("an overtime loss is an OTL where the league counts them", () => {
		const game = playedGame([100, 101], { overtimes: 1 }) as any;
		assert.strictEqual(teamGoingIn(game, 0, game.teams[0], true).otl, 1);
		assert.strictEqual(teamGoingIn(game, 0, game.teams[0], false).lost, 4);
	});
});

describe("recordDeltas", () => {
	test("each team's result, where, and against whom", () => {
		const deltas = recordDeltas([playedGame([100, 90]) as any], {
			otlSetting: false,
			divByTid: new Map([
				[0, 1],
				[1, 1],
			]),
			confByTid: new Map([
				[0, 0],
				[1, 0],
			]),
		});
		const home = deltas.get(0)!;
		const away = deltas.get(1)!;
		assert.strictEqual(home.won, 1);
		assert.strictEqual(home.wonHome, 1);
		assert.strictEqual(home.wonDiv, 1);
		assert.strictEqual(home.wonConf, 1);
		assert.strictEqual(home.pts, 100);
		assert.strictEqual(home.oppPts, 90);
		assert.strictEqual(away.lost, 1);
		assert.strictEqual(away.lostAway, 1);
	});

	test("playoff games aren't in the standings", () => {
		const deltas = recordDeltas(
			[playedGame([100, 90], { playoffs: true }) as any],
			{ otlSetting: false, divByTid: new Map(), confByTid: new Map() },
		);
		assert.strictEqual(deltas.size, 0);
	});
});

describe("streakAndLastTen", () => {
	test("reads like the standings do", () => {
		assert.deepStrictEqual(streakAndLastTen([1, 1, -1, "OTL", 1]), {
			streak: "Won 2",
			lastTen: "3-1-1",
		});
		assert.deepStrictEqual(streakAndLastTen(["OTL", -1, 1]), {
			streak: "Lost 2",
			lastTen: "1-1-1",
		});
		assert.deepStrictEqual(streakAndLastTen([0, 1]), {
			streak: "None",
			lastTen: "1-0-1",
		});
	});
});

type Side = { tid: number; won: number; pts?: number; sPts?: number };
type Series = { home: Side; away?: Side; gids?: number[] };

describe("unplaySeries", () => {
	const bracket = (): Series[][] => [
		[
			{
				home: { tid: 0, won: 4, pts: 400 },
				away: { tid: 1, won: 2, pts: 380 },
				gids: [1, 2, 3, 4, 5, 6, 7],
			},
			{
				home: { tid: 2, won: 4, pts: 300 },
				away: { tid: 3, won: 0, pts: 250 },
				gids: [8, 9, 10, 11],
			},
		],
		[
			{
				home: { tid: 0, won: 0 },
				away: { tid: 2, won: 0 },
			},
		],
	];

	test("the series-ending game comes back out, and the next round with it", () => {
		const series = bracket();
		const out = unplaySeries(series, [
			{
				gid: 7,
				teams: [
					{ tid: 1, pts: 95 },
					{ tid: 0, pts: 110 },
				] as any,
			},
		]);
		assert.strictEqual(out[0]![0]!.home.won, 3);
		assert.strictEqual(out[0]![0]!.away!.won, 2);
		assert.strictEqual(out[0]![0]!.home.pts, 290);
		assert.strictEqual(out[0]![0]!.away!.pts, 285);
		assert.deepStrictEqual(out[0]![0]!.gids, [1, 2, 3, 4, 5, 6]);
		assert.deepStrictEqual(out[1], []);
		// (A copy: the stored bracket is left alone.)
		assert.strictEqual(series[0]![0]!.home.won, 4);
		assert.strictEqual(series[1]!.length, 1);
	});

	test("nothing to take out: the same bracket", () => {
		const series = bracket();
		assert.strictEqual(unplaySeries(series, []), series);
	});
});

describe("eventGivesAwayLiveGame", () => {
	const gate = {
		hidden: new Set([7]),
		games: new Map([
			[
				7,
				{
					season: 2026,
					playoffs: true,
					teams: [{ tid: 0 }, { tid: 1 }],
				} as any,
			],
		]),
	};

	test("a story about the game, or the series it ended", () => {
		assert.isTrue(
			eventGivesAwayLiveGame(
				{
					type: "playerFeat",
					text: '<a href="/l/1/game_log/BOS_0/2026/7">a big game</a>',
				},
				gate,
			),
		);
		assert.isTrue(
			eventGivesAwayLiveGame(
				{ type: "playoffs", tids: [1, 0], season: 2026, text: "won" },
				gate,
			),
		);
		assert.isFalse(
			eventGivesAwayLiveGame(
				{ type: "playoffs", tids: [2, 3], season: 2026, text: "won" },
				gate,
			),
		);
	});
});

describe("nightShown", () => {
	const gate = (
		dayCutoff: number | undefined,
		games: [number, { day: number; date?: string }][],
	) => ({ dayCutoff, games: new Map(games) as any });

	test("no playoff game being played live: today as it is", () => {
		assert.strictEqual(
			nightShown("2026-04-20", gate(undefined, [])),
			"2026-04-20",
		);
		assert.strictEqual(nightShown(undefined, gate(undefined, [])), undefined);
	});

	test("a playoff game still being played live keeps its night today", () => {
		// (Its tip-off already worked out the next night - or, a series or the
		// playoffs over, none at all.)
		const live = gate(40, [[900, { day: 40, date: "2026-04-19" }]]);
		assert.strictEqual(nightShown("2026-04-21", live), "2026-04-19");
		assert.strictEqual(nightShown(undefined, live), "2026-04-19");
	});

	test("an undated league stays undated", () => {
		assert.strictEqual(
			nightShown(undefined, gate(40, [[900, { day: 40 }]])),
			undefined,
		);
	});
});
