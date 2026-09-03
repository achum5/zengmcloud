import "fake-indexeddb/auto";
import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import {
	applyStamp,
	describeRebuild,
	gameRecordStamps,
	inPlayedOrder,
	rebuildRecord,
	rebuildSeasonAggregates,
	rebuildTeamStatsRow,
	recordDiffers,
	statsRowDiffers,
	type GameForStats,
} from "./rebuildSeasonAggregates.ts";
import { repairAggregatesFromGames } from "./changeset.ts";
import { changeTracker } from "../../db/changeTracker.ts";

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
});

// THE REPAIR PASS, which is what actually heals a league that is already
// wrong. It runs after an apply that brought games, and once when a synced
// league connects - the second being the one that matters for a record that
// went wrong days ago, because the first only fires on a device RECEIVING a
// sim.
describe("repairAggregatesFromGames", () => {
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
		...extra,
	});

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

	const seed = async (
		staleWon: number,
		schedule: { gid: number; day: number }[] = [],
	) => {
		const games = fieldGames();
		const truth = record(games);
		await resetCache({
			teamSeasons: [
				seasonRow(0, { ...truth, won: staleWon }),
				seasonRow(1, record(games, 1)),
				seasonRow(2, record(games, 2)),
				seasonRow(3, record(games, 3)),
			],
			teamStats: [],
		});
		for (const g2 of games) {
			await idb.cache.games.add(g2 as any);
		}
		for (const row of schedule) {
			await idb.cache.schedule.add({
				...row,
				season: SEASON,
				homeTid: 0,
				awayTid: 1,
			} as any);
		}
	};

	const wonOf = async () =>
		(
			(await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
				SEASON,
				0,
			])) as any
		).won;

	test("gives the lost win back", async () => {
		await seed(3);
		await repairAggregatesFromGames("connect");
		assert.strictEqual(await wonOf(), 4);
	});

	// THE LOOP. The first version of this published its corrections, and two
	// devices that disagreed about one game published corrected rows at each
	// other once a second for four minutes - 139 versions, the room never
	// settling, auto-play stuck behind it. Nothing the repair writes may reach
	// the change tracker, so nothing it writes can be broadcast.
	test("nothing it writes is left pending for the room", async () => {
		await seed(3);
		await changeTracker.runCaptured(async () => {
			await repairAggregatesFromGames("connect");
		});
		assert.strictEqual(await wonOf(), 4);
		assert.deepStrictEqual(
			changeTracker.drain().map((change) => change.store),
			[],
		);
	});

	test("a day missing from this device stops it dead", async () => {
		// An unplayed row on day 2 while the league has played day 5: this
		// device never got a whole day, so its games cannot be trusted and
		// nothing may be rebuilt from them - let alone published.
		await seed(3, [{ gid: 99, day: 2 }]);
		await repairAggregatesFromGames("connect");
		assert.strictEqual(await wonOf(), 3);
	});

	test("a healthy league is left exactly as it is", async () => {
		await seed(4);
		await repairAggregatesFromGames("apply");
		assert.strictEqual(await wonOf(), 4);
	});

	test("a row claiming more than the games show is held, not lowered", async () => {
		await seed(6);
		await repairAggregatesFromGames("connect");
		assert.strictEqual(await wonOf(), 6);
	});
});

// THE NUMBER ON THE BOX SCORE.
//
// Each game row stores the record both sides carried INTO it, and that is what
// the box score and the game log print. A device simming off a stale season
// row stamps the stale record onto the game permanently - which is the "39
// wins, then 38 wins" a league-mate actually reads.
describe("the record stamped on each game", () => {
	const stampGame = (
		gid: number,
		day: number,
		home: number,
		away: number,
		homePts: number,
		awayPts: number,
		stored: [Record<string, unknown>, Record<string, unknown>],
		extra: { playoffs?: boolean; overtimes?: number } = {},
	) => {
		const homeWon = homePts > awayPts;
		return {
			gid,
			day,
			season: SEASON,
			overtimes: extra.overtimes ?? 0,
			playoffs: extra.playoffs,
			won: homeWon ? { tid: home, pts: homePts } : { tid: away, pts: awayPts },
			lost: homeWon ? { tid: away, pts: awayPts } : { tid: home, pts: homePts },
			teams: [
				{ tid: home, pts: homePts, ...stored[0] },
				{ tid: away, pts: awayPts, ...stored[1] },
			],
		} as any;
	};

	const rec = (won: number, lost: number) => ({ won, lost });

	test("the stamp is the record INCLUDING the game, and a tie and an OTL count", () => {
		const games = [
			stampGame(1, 1, 0, 1, 100, 90, [rec(0, 0), rec(0, 0)]),
			stampGame(2, 2, 0, 1, 90, 90, [rec(1, 0), rec(0, 1)]),
			stampGame(3, 3, 1, 0, 101, 99, [rec(0, 0), rec(0, 0)], {
				overtimes: 1,
			}),
		];
		const withOtl = gameRecordStamps(games, true);
		// The sim adds the result before storing the game: a first win says 1-0.
		assert.deepStrictEqual(withOtl.get(1), [
			{ won: 1, lost: 0, tied: 0, otl: 0 },
			{ won: 0, lost: 1, tied: 0, otl: 0 },
		]);
		// Game 2 was a tie, and its stamp carries it.
		assert.deepStrictEqual(withOtl.get(2)![0], {
			won: 1,
			lost: 0,
			tied: 1,
			otl: 0,
		});
		// Game 3 went to overtime, so with OTL on it lands as an overtime loss
		// on the loser's stamp; tid 0 is away here.
		assert.deepStrictEqual(withOtl.get(3)![1], {
			won: 1,
			lost: 0,
			tied: 1,
			otl: 1,
		});
		const after = gameRecordStamps(
			[...games, stampGame(4, 4, 0, 1, 100, 90, [rec(0, 0), rec(0, 0)])],
			true,
		);
		assert.deepStrictEqual(after.get(4)![0], {
			won: 2,
			lost: 0,
			tied: 1,
			otl: 1,
		});
		// Without OTL the overtime loss is a plain loss.
		const noOtl = gameRecordStamps(
			[...games, stampGame(4, 4, 0, 1, 100, 90, [rec(0, 0), rec(0, 0)])],
			false,
		);
		assert.deepStrictEqual(noOtl.get(4)![0], {
			won: 2,
			lost: 1,
			tied: 1,
			otl: 0,
		});
	});

	test("a playoff game advances nothing, and the All-Star game is not stamped", () => {
		const games = [
			stampGame(1, 1, 0, 1, 100, 90, [rec(0, 0), rec(0, 0)]),
			stampGame(2, 2, -1, -2, 150, 140, [rec(0, 0), rec(0, 0)]),
			stampGame(3, 3, 0, 1, 100, 90, [rec(0, 0), rec(0, 0)], {
				playoffs: true,
			}),
			stampGame(4, 4, 0, 1, 100, 90, [rec(0, 0), rec(0, 0)], {
				playoffs: true,
			}),
		];
		const stamps = gameRecordStamps(games, false);
		assert.strictEqual(stamps.has(2), false);
		// Both playoff games carry the final regular-season record.
		assert.deepStrictEqual(stamps.get(3)![0], stamps.get(4)![0]);
		assert.strictEqual(stamps.get(4)![0]!.won, 1);
	});

	test("only the fields the row already has are written", () => {
		const stamp = { won: 4, lost: 1, tied: 2, otl: 3 };
		// A league with no ties and no overtime losses stores neither.
		const plain: Record<string, unknown> = { tid: 0, won: 3, lost: 1 };
		assert.strictEqual(applyStamp(plain, stamp), true);
		assert.deepStrictEqual(plain, { tid: 0, won: 4, lost: 1 });
		// A legacy row with no record at all is left alone.
		const legacy: Record<string, unknown> = { tid: 0 };
		assert.strictEqual(applyStamp(legacy, stamp), false);
		assert.deepStrictEqual(legacy, { tid: 0 });
		// Nothing to do is reported as nothing done.
		const right: Record<string, unknown> = { tid: 0, won: 4, lost: 1 };
		assert.strictEqual(applyStamp(right, stamp), false);
	});

	test("the field case: a stale stamp is corrected on the game itself", async () => {
		resetG();
		g.setWithoutSavingToDB("season", SEASON);
		g.setWithoutSavingToDB("otl", false);
		// Four wins for tid 0, then the loss to tid 3 - which a stale device
		// stamped as though the fourth win had never happened.
		const games = [
			stampGame(1, 1, 0, 1, 100, 90, [rec(1, 0), rec(0, 1)]),
			stampGame(2, 2, 2, 0, 90, 100, [rec(0, 1), rec(2, 0)]),
			stampGame(3, 3, 0, 3, 100, 90, [rec(3, 0), rec(0, 1)]),
			stampGame(10, 4, 0, 2, 100, 97, [rec(4, 0), rec(0, 2)]),
			// Stamped 3-1 by a device a game behind; the games say 4-1.
			stampGame(11, 5, 3, 0, 94, 92, [rec(1, 1), rec(3, 1)]),
		];
		await resetCache({
			teamSeasons: [0, 1, 2, 3].map((tid) => ({
				tid,
				season: SEASON,
				did: divisionOf(tid),
				cid: conferenceOf(tid),
				...record(games as any, tid),
			})),
			teamStats: [],
		});
		for (const game2 of games) {
			await idb.cache.games.add(game2);
		}

		const report = await rebuildSeasonAggregates();
		assert.strictEqual(report.gameStampsFixed, 1);
		const fixed: any = await idb.cache.games.get(11);
		assert.strictEqual(fixed.teams[1].won, 4);
		assert.strictEqual(fixed.teams[1].lost, 1);
		assert.match(
			describeRebuild(report)!,
			/restamped the record on 1 box score/,
		);

		// Idempotent: a second pass finds nothing left to do.
		const again = await rebuildSeasonAggregates();
		assert.strictEqual(again.gameStampsFixed, 0);
	});

	test("a season row that had to be held stops the restamping too", async () => {
		resetG();
		g.setWithoutSavingToDB("season", SEASON);
		g.setWithoutSavingToDB("otl", false);
		const games = [
			stampGame(1, 1, 0, 1, 100, 90, [rec(0, 0), rec(0, 0)]),
			// Stamped wrong, but the season row below claims games this device
			// does not have - so the games are the suspect party, not the row.
			stampGame(2, 2, 0, 1, 100, 90, [rec(9, 9), rec(0, 1)]),
		];
		await resetCache({
			teamSeasons: [
				{
					tid: 0,
					season: SEASON,
					did: divisionOf(0),
					cid: conferenceOf(0),
					...record(games as any, 0),
					won: 20,
				},
				{
					tid: 1,
					season: SEASON,
					did: divisionOf(1),
					cid: conferenceOf(1),
					...record(games as any, 1),
				},
			],
			teamStats: [],
		});
		for (const game2 of games) {
			await idb.cache.games.add(game2);
		}
		const report = await rebuildSeasonAggregates();
		assert.strictEqual(report.recordsHeld.length, 1);
		assert.strictEqual(report.gameStampsFixed, 0);
		const untouched: any = await idb.cache.games.get(2);
		assert.strictEqual(untouched.teams[0].won, 9);
	});
});

// THE PLAYOFF FLAG.
//
// The playoffs phase writes playoffRoundsWon = 0 for every team in the
// bracket, and a stale season row published whole drags it back to -1 - a team
// up 2-0 in round 1 whose page says they missed the playoffs. The record
// rebuild cannot see that field, but the bracket is shared state every device
// agrees on, so it is the authority the repair raises from.
describe("playoffRoundsWon against the bracket", () => {
	const side = (tid: number, won = 0) => ({ tid, won, seed: 1 });

	const seedBracket = async (
		playoffRoundsWon: number[],
		playoffSeries: Record<string, unknown>,
	) => {
		resetG();
		g.setWithoutSavingToDB("season", SEASON);
		g.setWithoutSavingToDB("otl", false);
		const games = [
			game({ gid: 1, day: 1, home: 0, away: 1, homePts: 100, awayPts: 90 }),
		];
		await resetCache({
			teamSeasons: [0, 1, 2, 3].map((tid) => ({
				tid,
				season: SEASON,
				did: divisionOf(tid),
				cid: conferenceOf(tid),
				...record(games, tid),
				playoffRoundsWon: playoffRoundsWon[tid],
			})),
			teamStats: [],
		});
		await idb.cache.playoffSeries.put({
			season: SEASON,
			...playoffSeries,
		} as any);
		for (const g2 of games) {
			await idb.cache.games.add(g2 as any);
		}
	};

	const flagOf = async (tid: number) => {
		const row: any = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[SEASON, tid],
		);
		return row.playoffRoundsWon;
	};

	test("a bracket team dragged to -1 is raised to what the bracket proves", async () => {
		await seedBracket([-1, 0, 1, -1], {
			currentRound: 1,
			series: [
				[
					{ home: side(0, 4), away: side(1, 2) },
					{ home: side(2, 4), away: side(3, 0) },
				],
				[{ home: side(0), away: side(2) }],
			],
		});
		const report = await rebuildSeasonAggregates();
		// tid 0 stands in round 1, so it has won round 0; tid 3 stands in
		// round 0 and nothing further; tid 2 already says what the bracket says.
		assert.deepStrictEqual(report.playoffFlagsFixed, [
			{ tid: 0, before: -1, after: 1 },
			{ tid: 3, before: -1, after: 0 },
		]);
		assert.strictEqual(await flagOf(0), 1);
		assert.strictEqual(await flagOf(1), 0);
		assert.strictEqual(await flagOf(2), 1);
		assert.strictEqual(await flagOf(3), 0);
		assert.match(describeRebuild(report)!, /raised playoffRoundsWon/);
		// Off the wire, like every repair write.
		assert.deepStrictEqual(changeTracker.drain(), []);

		// Idempotent: a second pass finds nothing left to raise.
		const again = await rebuildSeasonAggregates();
		assert.deepStrictEqual(again.playoffFlagsFixed, []);
	});

	test("the champion is raised to every round, and a row is never lowered", async () => {
		await seedBracket([-1, 3, 0, 0], {
			currentRound: 1,
			series: [
				[
					{ home: side(0, 4), away: side(1, 1) },
					{ home: side(2, 4), away: side(3, 2) },
				],
				[{ home: side(0, 4), away: side(2, 0) }],
			],
		});
		const report = await rebuildSeasonAggregates();
		// tid 0 won the final: raised past the bracket's last round index. The
		// beaten finalist still stands in round 1, so its stale 0 rises to 1.
		// And tid 1's 3, whatever wrote it, is above the floor - not touched:
		// the bracket this device holds may itself be behind.
		assert.deepStrictEqual(report.playoffFlagsFixed, [
			{ tid: 0, before: -1, after: 2 },
			{ tid: 2, before: 0, after: 1 },
		]);
		assert.strictEqual(await flagOf(1), 3);
	});

	test("an unresolved play-in proves nothing", async () => {
		// Round 0 holds provisional tids while currentRound is -1; a
		// provisional team has not made the playoffs.
		await seedBracket([-1, -1, -1, -1], {
			currentRound: -1,
			series: [
				[
					{ home: side(0), away: side(1) },
					{ home: side(2), away: side(3) },
				],
			],
			playIns: [[{ home: side(1), away: side(3) }]],
		});
		const report = await rebuildSeasonAggregates();
		assert.deepStrictEqual(report.playoffFlagsFixed, []);
		assert.strictEqual(await flagOf(0), -1);
	});
});
