import "fake-indexeddb/auto";
import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import {
	describeRebuild,
	inPlayedOrder,
	rebuildRecord,
	rebuildSeasonAggregates,
	rebuildTeamStatsRow,
	recordDiffers,
	statsRowDiffers,
	type GameForStats,
} from "./rebuildSeasonAggregates.ts";

// Straight from the field: a team went from 39-22 to 38-23 in a single game.
// The loss was real; the win it took away was the previous game's, which the
// device that simmed the loss had never counted. The games say otherwise, and
// the games are what these tests replay.

const SEASON = 2016;

// Two divisions in one conference, a third division in the other.
const divisionOf = (tid: number) => [0, 0, 1, 2][tid];
const conferenceOf = (tid: number) => [0, 0, 0, 1][tid];

const game = ({
	gid,
	day,
	home,
	away,
	homePts,
	awayPts,
	playoffs,
	overtimes = 0,
	players,
}: {
	gid: number;
	day: number;
	home: number;
	away: number;
	homePts: number;
	awayPts: number;
	playoffs?: boolean;
	overtimes?: number;
	players?: [any[], any[]];
}): GameForStats => {
	const homeWon = homePts >= awayPts;
	return {
		gid,
		day,
		season: SEASON,
		playoffs,
		overtimes,
		won: homeWon ? { tid: home, pts: homePts } : { tid: away, pts: awayPts },
		lost: homeWon ? { tid: away, pts: awayPts } : { tid: home, pts: homePts },
		teams: [
			{
				tid: home,
				pts: homePts,
				fg: Math.floor(homePts / 2),
				min: 240 + overtimes * 25,
				players: players?.[0] ?? [],
			},
			{
				tid: away,
				pts: awayPts,
				fg: Math.floor(awayPts / 2),
				min: 240 + overtimes * 25,
				players: players?.[1] ?? [],
			},
		],
	};
};

const record = (games: GameForStats[], tid = 0, otl = false) =>
	rebuildRecord({ tid, season: SEASON, games, divisionOf, conferenceOf, otl });

describe("rebuildRecord", () => {
	test("gives back the win the stale row lost", () => {
		// Three wins, then the win over ATL (gid 10) and the loss to OKC (gid 11).
		const games = [
			game({ gid: 1, day: 1, home: 0, away: 1, homePts: 100, awayPts: 90 }),
			game({ gid: 2, day: 2, home: 2, away: 0, homePts: 90, awayPts: 100 }),
			game({ gid: 3, day: 3, home: 0, away: 3, homePts: 100, awayPts: 90 }),
			game({ gid: 10, day: 4, home: 0, away: 2, homePts: 100, awayPts: 97 }),
			game({ gid: 11, day: 5, home: 3, away: 0, homePts: 94, awayPts: 92 }),
		];
		const rebuilt = record(games);
		assert.strictEqual(rebuilt.won, 4);
		assert.strictEqual(rebuilt.lost, 1);

		// The row that lost the update: 3-1 where the games say 4-1.
		const staleRow = { ...rebuilt, won: 3, lost: 1 };
		assert.isTrue(recordDiffers(staleRow, rebuilt));
		assert.isFalse(recordDiffers({ ...rebuilt }, rebuilt));
	});

	test("home/away, division and conference splits, streak and last ten", () => {
		const games = [
			// Home win over a division rival.
			game({ gid: 1, day: 1, home: 0, away: 1, homePts: 100, awayPts: 90 }),
			// Road loss in-conference, out of division.
			game({ gid: 2, day: 2, home: 2, away: 0, homePts: 100, awayPts: 90 }),
			// Road win out of conference.
			game({ gid: 3, day: 3, home: 3, away: 0, homePts: 90, awayPts: 100 }),
			// Home win in-conference.
			game({ gid: 4, day: 4, home: 0, away: 2, homePts: 100, awayPts: 90 }),
		];
		const rebuilt = record(games);
		assert.deepStrictEqual(rebuilt, {
			won: 3,
			lost: 1,
			tied: 0,
			otl: 0,
			wonHome: 2,
			lostHome: 0,
			tiedHome: 0,
			otlHome: 0,
			wonAway: 1,
			lostAway: 1,
			tiedAway: 0,
			otlAway: 0,
			wonDiv: 1,
			lostDiv: 0,
			tiedDiv: 0,
			otlDiv: 0,
			wonConf: 2,
			lostConf: 1,
			tiedConf: 0,
			otlConf: 0,
			lastTen: [1, 1, 0, 1],
			streak: 2,
			gpHome: 2,
		});
	});

	test("playoff games count for gpHome but not the record; the All-Star game counts for nothing", () => {
		const games = [
			game({ gid: 1, day: 1, home: 0, away: 1, homePts: 100, awayPts: 90 }),
			game({
				gid: 2,
				day: 2,
				home: 0,
				away: 1,
				homePts: 80,
				awayPts: 90,
				playoffs: true,
			}),
			game({ gid: 3, day: 3, home: -1, away: -2, homePts: 150, awayPts: 140 }),
		];
		const rebuilt = record(games);
		assert.strictEqual(rebuilt.won, 1);
		assert.strictEqual(rebuilt.lost, 0);
		assert.strictEqual(rebuilt.gpHome, 2);
		assert.deepStrictEqual(rebuilt.lastTen, [1]);
	});

	test("an overtime loss is an OTL only when the league counts them", () => {
		const games = [
			game({
				gid: 1,
				day: 1,
				home: 1,
				away: 0,
				homePts: 3,
				awayPts: 2,
				overtimes: 1,
			}),
		];
		const withOtl = record(games, 0, true);
		assert.strictEqual(withOtl.otl, 1);
		assert.strictEqual(withOtl.otlAway, 1);
		assert.strictEqual(withOtl.otlDiv, 1);
		assert.strictEqual(withOtl.lost, 0);
		assert.deepStrictEqual(withOtl.lastTen, ["OTL"]);
		assert.strictEqual(withOtl.streak, -1);

		const without = record(games, 0, false);
		assert.strictEqual(without.otl, 0);
		assert.strictEqual(without.lost, 1);
		assert.deepStrictEqual(without.lastTen, [0]);
	});

	test("a tie ends the streak", () => {
		const games = [
			game({ gid: 1, day: 1, home: 0, away: 1, homePts: 100, awayPts: 90 }),
			game({ gid: 2, day: 2, home: 0, away: 1, homePts: 90, awayPts: 90 }),
		];
		const rebuilt = record(games);
		assert.strictEqual(rebuilt.tied, 1);
		assert.strictEqual(rebuilt.tiedHome, 1);
		assert.strictEqual(rebuilt.streak, 0);
		assert.deepStrictEqual(rebuilt.lastTen, [-1, 1]);
	});

	test("last ten keeps the newest ten, newest first, in the order the games were played", () => {
		// Twelve games; the gids run BACKWARDS against the days, so anything
		// sorting by gid would replay them in the wrong order. Wins through day
		// 9, losses on days 10-12.
		const games = Array.from({ length: 12 }, (_, i) => {
			const day = i + 1;
			const win = day <= 9;
			return game({
				gid: 100 - i,
				day,
				home: 0,
				away: 1,
				homePts: win ? 100 : 90,
				awayPts: win ? 90 : 100,
			});
		});
		assert.deepStrictEqual(
			inPlayedOrder(games).map((g) => g.day),
			Array.from({ length: 12 }, (_, i) => i + 1),
		);
		const rebuilt = record(games);
		assert.strictEqual(rebuilt.won, 9);
		assert.strictEqual(rebuilt.lost, 3);
		assert.deepStrictEqual(rebuilt.lastTen, [0, 0, 0, 1, 1, 1, 1, 1, 1, 1]);
		assert.strictEqual(rebuilt.streak, -3);
	});
});

describe("rebuildTeamStatsRow", () => {
	test("re-sums the additive keys, opponent keys from the other side, and leaves the rest", () => {
		const games = [
			game({ gid: 1, day: 1, home: 0, away: 1, homePts: 100, awayPts: 90 }),
			game({ gid: 2, day: 2, home: 1, away: 0, homePts: 80, awayPts: 70 }),
			// Playoffs: a different row.
			game({
				gid: 3,
				day: 3,
				home: 0,
				away: 1,
				homePts: 120,
				awayPts: 110,
				playoffs: true,
			}),
		];
		const row = {
			rid: 7,
			tid: 0,
			season: SEASON,
			playoffs: false,
			gp: 9,
			min: 9999,
			pts: 9999,
			fg: 9999,
			oppPts: 9999,
			oppFg: 9999,
			ptsQtrs: [25, 25, 25, 25],
			abbrev: "CLE",
		};
		const rebuilt = rebuildTeamStatsRow({
			row,
			tid: 0,
			season: SEASON,
			playoffs: false,
			games,
		});
		assert.deepStrictEqual(rebuilt, {
			rid: 7,
			tid: 0,
			season: SEASON,
			playoffs: false,
			gp: 2,
			min: 480,
			pts: 170,
			fg: 85,
			oppPts: 170,
			oppFg: 85,
			ptsQtrs: [25, 25, 25, 25],
			abbrev: "CLE",
		});
		assert.isTrue(statsRowDiffers(row, rebuilt));
		assert.isFalse(statsRowDiffers(rebuilt, { ...rebuilt, rid: 99 }));

		const playoffRow = rebuildTeamStatsRow({
			row: { ...row, playoffs: true },
			tid: 0,
			season: SEASON,
			playoffs: true,
			games,
		});
		assert.strictEqual(playoffRow.gp, 1);
		assert.strictEqual(playoffRow.pts, 120);
		assert.strictEqual(playoffRow.oppPts, 110);
	});
});

describe("rebuildSeasonAggregates", () => {
	const seasonRow = (tid: number, extra: Record<string, unknown> = {}) => ({
		tid,
		season: SEASON,
		did: divisionOf(tid),
		cid: conferenceOf(tid),
		won: 0,
		lost: 0,
		tied: 0,
		otl: 0,
		wonHome: 0,
		lostHome: 0,
		tiedHome: 0,
		otlHome: 0,
		wonAway: 0,
		lostAway: 0,
		tiedAway: 0,
		otlAway: 0,
		wonDiv: 0,
		lostDiv: 0,
		tiedDiv: 0,
		otlDiv: 0,
		wonConf: 0,
		lostConf: 0,
		tiedConf: 0,
		otlConf: 0,
		lastTen: [],
		streak: 0,
		gpHome: 0,
		hype: 0.5,
		...extra,
	});

	const statsRow = (tid: number, extra: Record<string, unknown> = {}) => ({
		tid,
		season: SEASON,
		playoffs: false,
		gp: 0,
		min: 0,
		pts: 0,
		fg: 0,
		oppPts: 0,
		oppFg: 0,
		...extra,
	});

	// The five games of the first test, replayed from the cache.
	const fieldGames = () => [
		game({ gid: 1, day: 1, home: 0, away: 1, homePts: 100, awayPts: 90 }),
		game({ gid: 2, day: 2, home: 2, away: 0, homePts: 90, awayPts: 100 }),
		game({ gid: 3, day: 3, home: 0, away: 3, homePts: 100, awayPts: 90 }),
		game({ gid: 10, day: 4, home: 0, away: 2, homePts: 100, awayPts: 97 }),
		game({ gid: 11, day: 5, home: 3, away: 0, homePts: 94, awayPts: 92 }),
	];

	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", SEASON);
		g.setWithoutSavingToDB("otl", false);
	});

	const seed = async ({
		teamSeasons,
		teamStats,
		games,
		players = [],
	}: {
		teamSeasons: Record<string, unknown>[];
		teamStats: Record<string, unknown>[];
		games: GameForStats[];
		players?: any[];
	}) => {
		await resetCache({ teamSeasons, teamStats, players });
		for (const g2 of games) {
			await idb.cache.games.add(g2 as any);
		}
	};

	test("writes back the record the games prove, and only that row", async () => {
		const truth = record(fieldGames());
		await seed({
			teamSeasons: [
				// Stale: a game behind on the win.
				seasonRow(0, { ...truth, won: 3, lastTen: [0, 1, 1, 1] }),
				// Right already.
				seasonRow(1, record(fieldGames(), 1)),
				seasonRow(2, record(fieldGames(), 2)),
				seasonRow(3, record(fieldGames(), 3)),
			],
			teamStats: [statsRow(0), statsRow(1), statsRow(2), statsRow(3)],
			games: fieldGames(),
		});

		const report = await rebuildSeasonAggregates();
		assert.deepStrictEqual(report.recordsFixed, [
			{ tid: 0, before: "3-1", after: "4-1" },
		]);
		assert.deepStrictEqual(report.recordsHeld, []);
		assert.strictEqual(report.teamsChecked, 4);

		const fixed: any = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[SEASON, 0],
		);
		assert.strictEqual(fixed.won, 4);
		assert.strictEqual(fixed.lost, 1);
		assert.deepStrictEqual(fixed.lastTen, [0, 1, 1, 1, 1]);
		// Not derivable, not touched.
		assert.strictEqual(fixed.hype, 0.5);

		// The empty stats rows all get their totals; the report says so.
		assert.strictEqual(report.statsRowsFixed, 4);
		const stats: any = await idb.cache.teamStats.indexGet(
			"teamStatsByPlayoffsTid",
			[false, 0],
		);
		assert.strictEqual(stats.gp, 5);
		assert.strictEqual(stats.pts, 492);
		assert.strictEqual(stats.oppPts, 461);

		assert.strictEqual(
			describeRebuild(report),
			"[sync] Rebuilt from games (2016, 4 teams): fixed records tid 0 3-1 -> 4-1; fixed 4 team stat row(s)",
		);
	});

	test("a healthy league writes nothing and says nothing", async () => {
		const games = fieldGames();
		const rows = [0, 1, 2, 3].map((tid) => seasonRow(tid, record(games, tid)));
		const stats = [0, 1, 2, 3].map((tid) =>
			rebuildTeamStatsRow({
				row: statsRow(tid),
				tid,
				season: SEASON,
				playoffs: false,
				games,
			}),
		);
		await seed({ teamSeasons: rows, teamStats: stats, games });

		const report = await rebuildSeasonAggregates();
		assert.deepStrictEqual(report.recordsFixed, []);
		assert.strictEqual(report.statsRowsFixed, 0);
		assert.strictEqual(describeRebuild(report), undefined);
	});

	test("never counts a row down: a row with more games than this device has is held", async () => {
		const games = fieldGames();
		const truth = record(games);
		await seed({
			teamSeasons: [
				// Claims a sixth game the cache does not have.
				seasonRow(0, { ...truth, won: 5 }),
				seasonRow(1, record(games, 1)),
				seasonRow(2, record(games, 2)),
				seasonRow(3, record(games, 3)),
			],
			teamStats: [statsRow(0, { gp: 6, pts: 600 })],
			games,
		});

		const report = await rebuildSeasonAggregates();
		assert.deepStrictEqual(report.recordsFixed, []);
		assert.deepStrictEqual(report.recordsHeld, [
			{ tid: 0, before: "5-1", after: "4-1" },
		]);
		assert.strictEqual(report.statsRowsFixed, 0);
		assert.strictEqual(report.statsRowsHeld, 1);

		const untouched: any = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[SEASON, 0],
		);
		assert.strictEqual(untouched.won, 5);
		assert.match(describeRebuild(report)!, /held records .*tid 0 5-1 -> 4-1/);
	});

	test("limits itself to the teams asked for", async () => {
		const games = fieldGames();
		await seed({
			teamSeasons: [0, 1, 2, 3].map((tid) => seasonRow(tid)),
			teamStats: [],
			games,
		});
		const report = await rebuildSeasonAggregates({ tids: [2] });
		assert.strictEqual(report.teamsChecked, 1);
		assert.deepStrictEqual(report.recordsFixed, [
			{ tid: 2, before: "0-0", after: "0-2" },
		]);
	});

	test("counts player rows that disagree with the box scores, without writing them", async () => {
		const box = (pid: number, min: number, pts: number) => ({
			pid,
			gp: 1,
			min,
			pts,
		});
		const games = [
			game({
				gid: 1,
				day: 1,
				home: 0,
				away: 1,
				homePts: 100,
				awayPts: 90,
				players: [[box(5, 30, 20)], [box(6, 30, 10)]],
			}),
			game({
				gid: 2,
				day: 2,
				home: 0,
				away: 1,
				homePts: 100,
				awayPts: 90,
				players: [[box(5, 32, 24)], [box(6, 28, 12)]],
			}),
		];
		const playerRow = (tid: number, gp: number, min: number, pts: number) => ({
			season: SEASON,
			tid,
			playoffs: false,
			gp,
			min,
			pts,
		});
		await seed({
			teamSeasons: [
				seasonRow(0, record(games, 0)),
				seasonRow(1, record(games, 1)),
			],
			teamStats: [],
			games,
			players: [
				// A game behind, like the team row was.
				{ pid: 5, tid: 0, stats: [playerRow(0, 1, 30, 20)] },
				{ pid: 6, tid: 1, stats: [playerRow(1, 2, 58, 22)] },
			],
		});

		const report = await rebuildSeasonAggregates();
		assert.strictEqual(report.playerRowsSuspect, 1);
		const p: any = await idb.cache.players.get(5);
		assert.strictEqual(p.stats[0].gp, 1);
		assert.match(describeRebuild(report)!, /1 player row\(s\) disagree/);
	});
});
