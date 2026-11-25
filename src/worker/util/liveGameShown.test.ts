import { assert, describe, test } from "vitest";
import { buildLiveShown, playoffPlaceOf } from "./liveGameShown.ts";
import type { Game } from "../../common/types.ts";

const SEASON = 2025;

const box = (pid: number, stats: Record<string, unknown> = {}) => ({
	pid,
	name: `P${pid}`,
	pos: "G",
	skills: [],
	injury: { type: "Healthy", gamesRemaining: 0 },
	jerseyNumber: "1",
	gp: 1,
	gs: 1,
	min: 30,
	fg: 5,
	fga: 10,
	tp: 1,
	tpa: 3,
	ft: 2,
	fta: 2,
	orb: 1,
	drb: 4,
	ast: 3,
	tov: 2,
	stl: 1,
	blk: 0,
	ba: 0,
	pf: 2,
	pts: 13,
	pm: 4,
	...stats,
});

const team = (
	tid: number,
	pts: number,
	players: ReturnType<typeof box>[],
	extra: Record<string, unknown> = {},
) => ({
	tid,
	pts,
	min: 240,
	fg: 40,
	fga: 85,
	tp: 10,
	tpa: 30,
	ft: 10,
	fta: 12,
	orb: 10,
	drb: 30,
	ast: 22,
	tov: 12,
	stl: 7,
	blk: 4,
	pf: 18,
	dd: 0,
	td: 0,
	qd: 0,
	fxf: 0,
	players,
	...extra,
});

const game = (
	gid: number,
	day: number,
	home: ReturnType<typeof team>,
	away: ReturnType<typeof team>,
	extra: Partial<Game> = {},
) =>
	({
		gid,
		day,
		season: SEASON,
		overtimes: 0,
		att: 18000,
		teams: [home, away],
		won: {},
		lost: {},
		...extra,
	}) as unknown as Game;

// CLE (0) at home to SAC (1): CLE wins 110-100. Player 5 (CLE) scores 40 and
// hurts his ankle.
const liveGame = game(
	100,
	20,
	team(0, 110, [
		box(5, {
			pts: 40,
			injury: {
				type: "Sprained Ankle",
				gamesRemaining: 4,
				newThisGame: true,
			},
		}),
		box(6),
	]),
	team(1, 100, [box(7, { gp: 0, min: 0 })]),
);

// Earlier games, already shown.
const earlier = [
	game(90, 18, team(0, 99, [box(5, { pts: 31 })]), team(2, 101, [])),
	game(91, 19, team(3, 90, []), team(0, 95, [box(5, { pts: 22 })])),
];

const shown = (
	games: Game[] = [liveGame],
	seasonGames = [...earlier, liveGame],
) =>
	buildLiveShown({
		games,
		season: SEASON,
		otlSetting: false,
		divByTid: new Map([
			[0, 0],
			[1, 1],
			[2, 0],
			[3, 1],
		]),
		confByTid: new Map([
			[0, 0],
			[1, 0],
			[2, 0],
			[3, 0],
		]),
		seasonGames: () => seasonGames,
		playoffSeries: undefined,
	});

const teamSeason = (tid: number, extra: Record<string, unknown> = {}) => ({
	tid,
	season: SEASON,
	won: 15,
	lost: 6,
	tied: 0,
	otl: 0,
	wonHome: 8,
	lostHome: 2,
	tiedHome: 0,
	otlHome: 0,
	wonAway: 7,
	lostAway: 4,
	tiedAway: 0,
	otlAway: 0,
	wonDiv: 3,
	lostDiv: 1,
	tiedDiv: 0,
	otlDiv: 0,
	wonConf: 10,
	lostConf: 3,
	tiedConf: 0,
	otlConf: 0,
	gpHome: 10,
	att: 180000,
	streak: 1,
	lastTen: [1, 1, 0],
	playoffRoundsWon: -1,
	...extra,
});

describe("teamSeason", () => {
	test("the record as it stood going in", () => {
		const out = shown().teamSeason(teamSeason(0)) as any;
		assert.strictEqual(out.won, 14);
		assert.strictEqual(out.lost, 6);
		assert.strictEqual(out.wonHome, 7);
		assert.strictEqual(out.wonAway, 7);
		assert.strictEqual(out.wonConf, 9);
		// Not a division game.
		assert.strictEqual(out.wonDiv, 3);
		assert.strictEqual(out.gpHome, 9);
		assert.strictEqual(out.att, 162000);
	});

	test("the loser's too, away", () => {
		const out = shown().teamSeason(teamSeason(1)) as any;
		assert.strictEqual(out.won, 15);
		assert.strictEqual(out.lost, 5);
		assert.strictEqual(out.lostAway, 3);
		assert.strictEqual(out.lostHome, 2);
		assert.strictEqual(out.gpHome, 10);
	});

	test("streak and last ten from the games before it", () => {
		const out = shown().teamSeason(teamSeason(0)) as any;
		// Lost on day 18, won on day 19; day 20 is the live game.
		assert.strictEqual(out.streak, 1);
		assert.deepStrictEqual(out.lastTen, [1, 0]);
	});

	test("other teams and seasons are the very same row", () => {
		const s = shown();
		const other = teamSeason(2);
		assert.strictEqual(s.teamSeason(other), other);
		const past = teamSeason(0, { season: SEASON - 1 });
		assert.strictEqual(s.teamSeason(past), past);
	});

	test("never touches the stored row", () => {
		const row = teamSeason(0);
		shown().teamSeason(row);
		assert.strictEqual(row.won, 15);
		assert.deepStrictEqual(row.lastTen, [1, 1, 0]);
	});
});

describe("teamStats", () => {
	test("the game's own and opponent totals out", () => {
		const row = {
			tid: 0,
			season: SEASON,
			playoffs: false,
			gp: 21,
			min: 5040,
			pts: 2300,
			oppPts: 2150,
			fg: 840,
			oppFg: 800,
		};
		const out = shown().teamStats(row) as any;
		assert.strictEqual(out.gp, 20);
		assert.strictEqual(out.min, 4800);
		assert.strictEqual(out.pts, 2190);
		assert.strictEqual(out.oppPts, 2050);
		assert.strictEqual(out.fg, 800);
		assert.strictEqual(out.oppFg, 760);
		assert.strictEqual(row.gp, 21);
	});

	test("the playoff row is left alone for a regular season game", () => {
		const row = { tid: 0, season: SEASON, playoffs: true, gp: 3, pts: 300 };
		assert.strictEqual(shown().teamStats(row), row);
	});
});

const player = (extra: Record<string, unknown> = {}) => ({
	pid: 5,
	tid: 0,
	injury: { type: "Sprained Ankle", gamesRemaining: 4 },
	injuries: [
		{ season: SEASON - 1, games: 3, type: "Bruised Knee" },
		{ season: SEASON, games: 4, type: "Sprained Ankle" },
	],
	ratings: [{ season: SEASON, ovr: 60 }],
	stats: [
		{ pid: 5, season: SEASON - 1, tid: 0, playoffs: false, gp: 70, pts: 900 },
		{
			pid: 5,
			season: SEASON,
			tid: 0,
			playoffs: false,
			gp: 21,
			gs: 21,
			min: 630,
			pts: 400,
			fg: 150,
			ptsMax: [40, 100],
			fgMax: [5, 90],
		},
	],
	...extra,
});

describe("player", () => {
	test("his line as it stood going in", () => {
		const p = player();
		const out = shown().player(p as any) as any;
		const row = out.stats[1];
		assert.strictEqual(row.gp, 20);
		assert.strictEqual(row.gs, 20);
		assert.strictEqual(row.min, 600);
		assert.strictEqual(row.pts, 360);
		assert.strictEqual(row.fg, 145);
		// Last season untouched.
		assert.strictEqual(out.stats[0], p.stats[0]);
		// The stored player untouched.
		assert.strictEqual(p.stats[1]!.gp, 21);
	});

	test("a season high set in it goes back to the best of the rest", () => {
		const out = shown().player(player() as any) as any;
		assert.deepStrictEqual(out.stats[1].ptsMax, [31, 90]);
		// Set in another game: kept.
		assert.deepStrictEqual(out.stats[1].fgMax, [5, 90]);
	});

	test("not hurt yet", () => {
		const out = shown().player(player() as any) as any;
		assert.deepStrictEqual(out.injury, { type: "Healthy", gamesRemaining: 0 });
		assert.strictEqual(out.injuries.length, 1);
		assert.strictEqual(out.injuries[0].type, "Bruised Knee");
	});

	test("still playing through what he had at tip-off", () => {
		const g = structuredClone(liveGame) as any;
		g.teams[0].players[0].injuryAtStart = {
			type: "Sore Back",
			gamesRemaining: 2,
		};
		const out = shown([g]).player(player() as any) as any;
		assert.deepStrictEqual(out.injury, {
			type: "Sore Back",
			gamesRemaining: 2,
		});
	});

	test("the ratings the injury cost him come back", () => {
		const p = player({
			ratings: [
				{ season: SEASON, ovr: 60 },
				{ season: SEASON, ovr: 55, injuryIndex: 1 },
			],
		});
		const out = shown().player(p as any) as any;
		assert.strictEqual(out.ratings.length, 1);
		assert.strictEqual(out.ratings[0].ovr, 60);
	});

	test("someone who sat out, or wasn't in it, is the same player", () => {
		const s = shown();
		const dnp = {
			pid: 7,
			injury: { type: "Healthy", gamesRemaining: 0 },
			stats: [{ season: SEASON, tid: 1, playoffs: false, gp: 10, pts: 50 }],
		};
		assert.strictEqual((s.player(dnp as any) as any).stats[0].gp, 10);
		const elsewhere = { ...dnp, pid: 99 };
		assert.strictEqual(s.player(elsewhere as any), elsewhere);
	});

	test("minutes available: out for whoever was healthy, played or not", () => {
		const s = shown();
		// Sat out healthy: SAC's 240 minutes / 5.
		const dnp = {
			pid: 7,
			injury: { type: "Healthy", gamesRemaining: 0 },
			stats: [
				{ season: SEASON, tid: 1, playoffs: false, gp: 10, minAvailable: 500 },
			],
		};
		assert.strictEqual(
			(s.player(dnp as any) as any).stats[0].minAvailable,
			452,
		);
		// Hurt in it: never counted.
		const hurt = player({
			stats: [
				{ season: SEASON, tid: 0, playoffs: false, gp: 21, minAvailable: 500 },
			],
		});
		assert.strictEqual(
			(s.player(hurt as any) as any).stats[0].minAvailable,
			500,
		);
		// Played healthy.
		const healthy = { ...dnp, pid: 6, stats: [{ ...dnp.stats[0], tid: 0 }] };
		assert.strictEqual(
			(s.player(healthy as any) as any).stats[0].minAvailable,
			452,
		);
	});

	test("his first playoff game's row didn't exist going in", () => {
		const playoffGame = game(
			200,
			90,
			team(0, 110, [box(5)]),
			team(1, 100, []),
			{ playoffs: true },
		);
		const p = player({
			stats: [
				...player().stats,
				{ season: SEASON, tid: 0, playoffs: true, gp: 1, pts: 13 },
			],
		});
		const out = shown([playoffGame], [playoffGame]).player(p as any) as any;
		assert.strictEqual(out.stats.length, 2);
		assert.isFalse(out.stats.some((row: any) => row.playoffs));
	});
});

describe("headToHead", () => {
	test("the game out of the pair's record", () => {
		const row = {
			season: SEASON,
			regularSeason: {
				0: {
					1: {
						won: 2,
						lost: 0,
						tied: 0,
						otl: 0,
						otw: 0,
						pts: 220,
						oppPts: 200,
					},
				},
			},
			playoffs: {},
		};
		const out = shown().headToHead(row as any) as any;
		assert.strictEqual(out.regularSeason[0][1].won, 1);
		assert.strictEqual(out.regularSeason[0][1].pts, 110);
		assert.strictEqual(row.regularSeason[0][1].won, 2);
	});

	test("their first meeting: no record yet", () => {
		const row = {
			season: SEASON,
			regularSeason: {
				0: {
					1: {
						won: 1,
						lost: 0,
						tied: 0,
						otl: 0,
						otw: 0,
						pts: 110,
						oppPts: 100,
					},
				},
			},
			playoffs: {},
		};
		const out = shown().headToHead(row as any) as any;
		assert.deepEqual(out.regularSeason, {});
	});
});

describe("events", () => {
	test("his injury story and the game's own story go; the rest stay", () => {
		const events = [
			{
				eid: 1,
				type: "injured",
				season: SEASON,
				pids: [5],
				text: "was injured! (Sprained Ankle, out for 9 days)",
			},
			{
				eid: 2,
				type: "injured",
				season: SEASON,
				pids: [5],
				text: "was injured! (Sprained Ankle, out for 4 days)",
			},
			{
				eid: 3,
				type: "playerFeat",
				season: SEASON,
				pids: [5],
				text: '<a href="/l/1/game_log/CLE/2025/100">40 points</a>',
			},
			{ eid: 4, type: "trade", season: SEASON, pids: [6], text: "traded" },
		];
		const out = shown().events(events);
		assert.deepStrictEqual(
			out.map((event) => event.eid),
			[1, 4],
		);
	});
});

describe("playoffPlaceOf", () => {
	test("a series round, or a play-in game", () => {
		const playoffSeries = {
			series: [
				[{ home: { tid: 0, won: 1 }, away: { tid: 1, won: 0 }, gids: [5] }],
				[{ home: { tid: 0, won: 0 }, away: { tid: 2, won: 0 }, gids: [9] }],
			],
			playIns: [[]],
		} as any;
		assert.strictEqual(playoffPlaceOf(9, playoffSeries), 1);
		assert.strictEqual(playoffPlaceOf(3, playoffSeries), "playIn");
		assert.isUndefined(playoffPlaceOf(3, { ...playoffSeries, playIns: [] }));
	});

	test("a series won in it isn't won yet", () => {
		const finalGame = game(300, 100, team(0, 110, []), team(1, 100, []), {
			playoffs: true,
		});
		const s = buildLiveShown({
			games: [finalGame],
			season: SEASON,
			otlSetting: false,
			divByTid: new Map(),
			confByTid: new Map(),
			seasonGames: () => [finalGame],
			playoffSeries: {
				series: [
					[
						{
							home: { tid: 0, won: 4 } as any,
							away: { tid: 1, won: 2 } as any,
							gids: [300],
						},
					],
				],
			},
		});
		const out = s.teamSeason(teamSeason(0, { playoffRoundsWon: 1 })) as any;
		assert.strictEqual(out.playoffRoundsWon, 0);
		// And the regular season record is not touched by a playoff game.
		assert.strictEqual(out.won, 15);
	});
});
