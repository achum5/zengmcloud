import { assert, describe, onTestFinished, test } from "vitest";
import { roundFromSeriesKey, seriesKey } from "./playoffSeriesStats.ts";
import { resetCache, resetG } from "../../test/helpers.ts";
import { g } from "../util/index.ts";

describe("series keys", () => {
	test("a round and its key go both ways", () => {
		for (const round of [0, 1, 2, 3, 6]) {
			assert.strictEqual(roundFromSeriesKey(seriesKey(round)), round);
		}
		assert.strictEqual(seriesKey(0), "r1");
	});

	test("anything else is not a series", () => {
		for (const key of [
			undefined,
			"playoffs",
			"regularSeason",
			"combined",
			"r",
			"rx",
			"r1a",
		]) {
			assert.strictEqual(roundFromSeriesKey(key), undefined);
		}
	});
});

// A series' advanced stats are measured against the whole playoffs - every
// other series' games added up too - not just its two teams: so with no
// other round played, its numbers are the playoffs' (see seriesStatsRows).
describe("a series' advanced stats", () => {
	const line = (pid: number, k: number) => ({
		pid,
		gp: 1,
		gs: 1,
		min: 30 + k,
		fg: 8 + (k % 3),
		fga: 17 + k,
		tp: 2 + (k % 2),
		tpa: 6,
		ft: 3,
		fta: 4,
		orb: 1 + (k % 2),
		drb: 5 + k,
		ast: 4 + k,
		tov: 2,
		stl: 1,
		blk: k % 2,
		ba: 0,
		pf: 2 + (k % 3),
		pts: 21 + k,
		pm: 3 - k,
		jerseyNumber: String(pid),
	});
	const side = (tid: number, pts: number, pids: number[], k: number) => ({
		tid,
		pts,
		min: 240,
		fg: 40 + k,
		fga: 85 + k,
		tp: 10,
		tpa: 30,
		ft: 15 + k,
		fta: 20,
		orb: 9 + k,
		drb: 33 - k,
		ast: 22 + k,
		tov: 13 - k,
		stl: 7,
		blk: 4 + k,
		ba: 3,
		pf: 18 + k,
		players: pids.map((pid, i) => line(pid, (i + k) % 5)),
	});
	const game = (gid: number, a: number, b: number, k: number) => ({
		gid,
		season: 2025,
		day: gid,
		playoffs: true,
		numPeriods: 4,
		overtimes: 0,
		teams: [
			side(
				a,
				100 + k,
				[a * 10 + 1, a * 10 + 2, a * 10 + 3, a * 10 + 4, a * 10 + 5],
				k,
			),
			side(
				b,
				95 - k,
				[b * 10 + 1, b * 10 + 2, b * 10 + 3, b * 10 + 4, b * 10 + 5],
				k + 1,
			),
		],
	});
	const players = [0, 1, 2, 3].flatMap((tid) =>
		[1, 2, 3, 4, 5].map((n) => ({
			pid: tid * 10 + n,
			tid,
			ratings: [{ season: 2025, pos: n < 3 ? "G" : "F" }],
			stats: [],
		})),
	);
	const series = { key: "r1", round: 0, oppTid: 1, gids: undefined };

	test("measured against every series played, its own totals its own", async () => {
		const { seriesStatsRows } = await import("./playoffSeriesStats.ts");
		const { idb } = await import("../db/index.ts");
		// (Nothing in the league's database: it is all in the cache.)
		const league = idb.league;
		idb.league = { get: async () => undefined } as any;
		onTestFinished(() => {
			idb.league = league;
		});
		resetG();
		g.setWithoutSavingToDB("season", 2025);
		// Just the series.
		await resetCache({ players });
		for (const gm of [game(1, 0, 1, 0), game(2, 1, 0, 1), game(3, 0, 1, 2)]) {
			await idb.cache.games.put(gm as any);
		}
		const alone = await seriesStatsRows({ tid: 0, series, seasonNum: 2025 });
		// And the other first-round series, played too.
		await resetCache({ players });
		for (const gm of [
			game(1, 0, 1, 0),
			game(2, 1, 0, 1),
			game(3, 0, 1, 2),
			game(4, 2, 3, 3),
			game(5, 3, 2, 4),
			game(6, 2, 3, 0),
		]) {
			await idb.cache.games.put(gm as any);
		}
		const withRest = await seriesStatsRows({
			tid: 0,
			series,
			seasonNum: 2025,
		});
		assert.deepEqual([...withRest.keys()].sort(), [1, 2, 3, 4, 5]);
		const a = alone.get(1)!;
		const b = withRest.get(1)!;
		// (Its own games only: the totals the same.)
		assert.strictEqual(a.gp, b.gp);
		assert.strictEqual(a.pts, b.pts);
		// (Against more than its two teams: the league moved them.)
		assert.notStrictEqual(a.dbpm, b.dbpm);
		assert.isTrue(Number.isFinite(b.dbpm) && Number.isFinite(b.obpm));
	});
});
