import { assert, describe, test } from "vitest";
import {
	availableDecades,
	careerStatPasses,
	debutedInDecade,
	decadeLabel,
	MIN_SEASON_GP,
	seasonsInDecade,
	statLabel,
	statSeasonsFor,
	statSpecById,
} from "./dynamicCriteria.ts";
import type { TriviaPlayer, TriviaPool } from "./pool.ts";

const row = (over: Partial<TriviaPlayer["rows"][number]>) => ({
	season: 2000,
	tid: 0,
	gp: 82,
	min: 2000,
	pts: 1000,
	trb: 500,
	ast: 300,
	stl: 100,
	blk: 50,
	tp: 100,
	tpa: 300,
	fg: 400,
	fga: 900,
	ft: 200,
	fta: 250,
	pos: "SF",
	jerseyNumber: undefined,
	...over,
});

const player = (over: Partial<TriviaPlayer> = {}): TriviaPlayer =>
	({
		pid: 1,
		name: "Test Player",
		firstSeason: 2000,
		lastSeason: 2010,
		bornYear: 1980,
		hof: false,
		draft: { round: 1, pick: 1, year: 1999 },
		awards: [],
		teamsPlayed: [0],
		rows: [row({})],
		tot: {
			gp: 800,
			min: 24000,
			pts: 20000,
			trb: 5000,
			ast: 3000,
			stl: 1000,
			blk: 500,
			tp: 1000,
			tpa: 3000,
			fg: 7000,
			fga: 15000,
			ft: 4000,
			fta: 5000,
			seasons: 12,
		},
		gameHigh: { pts: 50, trb: 20, ast: 15 },
		popularity: 100,
		...over,
	}) as TriviaPlayer;

describe("statLabel", () => {
	test("career totals read like the hand-written achievements", () => {
		const spec = statSpecById("career-pts")!;
		assert.strictEqual(statLabel(spec, "gte", 20000), "20,000+ Career Points");
		assert.strictEqual(
			statLabel(spec, "lte", 5000),
			"5,000 or fewer Career Points",
		);
	});

	test("season rates get the (Season) suffix and drop a trailing .0", () => {
		const spec = statSpecById("season-ppg")!;
		assert.strictEqual(statLabel(spec, "gte", 30), "30+ PPG (Season)");
		assert.strictEqual(statLabel(spec, "gte", 1), "1+ PPG (Season)");
		assert.strictEqual(statLabel(spec, "gte", 100), "100+ PPG (Season)");
		assert.strictEqual(statLabel(spec, "lte", 20), "20 or fewer PPG (Season)");
	});

	test("a fractional threshold keeps its decimal", () => {
		const spec = statSpecById("season-bpg")!;
		assert.strictEqual(statLabel(spec, "gte", 2.5), "2.5+ BPG (Season)");
	});
});

describe("careerStatPasses", () => {
	const spec = statSpecById("career-pts")!;
	const p = player();

	test("gte is inclusive at the threshold", () => {
		assert.strictEqual(careerStatPasses(p, spec, "gte", 20000), true);
		assert.strictEqual(careerStatPasses(p, spec, "gte", 20001), false);
	});

	test("lte is inclusive at the threshold", () => {
		assert.strictEqual(careerStatPasses(p, spec, "lte", 20000), true);
		assert.strictEqual(careerStatPasses(p, spec, "lte", 19999), false);
	});
});

describe("statSeasonsFor", () => {
	const ppg = statSpecById("season-ppg")!;

	test("returns the seasons that qualified, not just a boolean", () => {
		const p = player({
			rows: [
				row({ season: 2001, gp: 82, pts: 82 * 31 }),
				row({ season: 2002, gp: 82, pts: 82 * 10 }),
				row({ season: 2003, gp: 82, pts: 82 * 33 }),
			],
		});
		const seasons = statSeasonsFor(p, ppg, "gte", 30);
		assert.deepStrictEqual([...seasons].sort(), [2001, 2003]);
	});

	test("a short season can't win a per-game threshold", () => {
		// 40 points a game, but over too few games to count.
		const p = player({
			rows: [row({ season: 2001, gp: MIN_SEASON_GP - 1, pts: 40 * 20 })],
		});
		assert.strictEqual(statSeasonsFor(p, ppg, "gte", 30).size, 0);
	});

	test("the games-played spec is a total, so no rate minimum applies", () => {
		const gp = statSpecById("season-gp")!;
		const p = player({ rows: [row({ season: 2001, gp: 70 })] });
		assert.deepStrictEqual([...statSeasonsFor(p, gp, "gte", 70)], [2001]);
	});

	test("'or fewer' finds the low seasons", () => {
		const p = player({
			rows: [
				row({ season: 2001, gp: 82, pts: 82 * 4 }),
				row({ season: 2002, gp: 82, pts: 82 * 25 }),
			],
		});
		assert.deepStrictEqual([...statSeasonsFor(p, ppg, "lte", 5)], [2001]);
	});
});

describe("decades", () => {
	test("labels", () => {
		assert.strictEqual(decadeLabel("debut", 1990), "Debuted in the 1990s");
		assert.strictEqual(decadeLabel("played", 1990), "Played in the 1990s");
	});

	test("only the decades the league has actually reached", () => {
		const pool = { minSeason: 1994, maxSeason: 2026 } as TriviaPool;
		assert.deepStrictEqual(availableDecades(pool), [1990, 2000, 2010, 2020]);
	});

	test("a one-season league still offers its own decade", () => {
		const pool = { minSeason: 2025, maxSeason: 2025 } as TriviaPool;
		assert.deepStrictEqual(availableDecades(pool), [2020]);
	});

	test("debut is the first season, not merely playing in the decade", () => {
		const p = player({ firstSeason: 1998, lastSeason: 2012 });
		assert.strictEqual(debutedInDecade(p, 1990), true);
		assert.strictEqual(debutedInDecade(p, 2000), false);
	});

	test("played-in returns the seasons, so it can be team-aligned", () => {
		const p = player({
			rows: [
				row({ season: 1998, gp: 82 }),
				row({ season: 2001, gp: 82 }),
				// A season on the roster but never on the floor doesn't count.
				row({ season: 1999, gp: 0 }),
			],
		});
		assert.deepStrictEqual([...seasonsInDecade(p, 1990)], [1998]);
		assert.deepStrictEqual([...seasonsInDecade(p, 2000)], [2001]);
	});
});
