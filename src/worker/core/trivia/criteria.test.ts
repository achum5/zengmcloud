import { assert, beforeEach, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import {
	buildCareerAchievements,
	buildSeasonContext,
	buildSeasonIndex,
	mergedSeasons,
	SEASON_ACHIEVEMENTS,
} from "./criteria.ts";
import type { TriviaPlayer, TriviaPool } from "./pool.ts";

// Synthetic players for exercising the predicates without a real league DB.

const makeRow = (
	season: number,
	tid: number,
	over: Partial<TriviaPlayer["rows"][number]> = {},
) => ({
	season,
	tid,
	gp: 70,
	min: 2400,
	pts: 1500,
	trb: 400,
	ast: 300,
	stl: 80,
	blk: 40,
	tp: 100,
	tpa: 280,
	fg: 550,
	fga: 1150,
	ft: 300,
	fta: 360,
	pos: "SF",
	jerseyNumber: "23",
	...over,
});

const makePlayer = (over: Partial<TriviaPlayer> = {}): TriviaPlayer => ({
	pid: 1,
	name: "Test Player",
	firstSeason: 2000,
	lastSeason: 2010,
	bornYear: 1978,
	hof: false,
	draft: { round: 1, pick: 5, year: 1999 },
	awards: [],
	teamsPlayed: [0],
	rows: [makeRow(2000, 0)],
	tot: {
		gp: 70,
		min: 2400,
		pts: 1500,
		trb: 400,
		ast: 300,
		stl: 80,
		blk: 40,
		tp: 100,
		tpa: 280,
		fg: 550,
		fga: 1150,
		ft: 300,
		fta: 360,
		seasons: 1,
	},
	gameHigh: { pts: 40, trb: 15, ast: 12 },
	popularity: 50,
	...over,
});

const makePool = (players: TriviaPlayer[]): TriviaPool => ({
	players,
	byPid: new Map(players.map((p) => [p.pid, p])),
	minSeason: Math.min(...players.map((p) => p.firstSeason)),
	maxSeason: Math.max(...players.map((p) => p.lastSeason)),
});

const careerAch = (pool: TriviaPool, id: string) => {
	const ach = buildCareerAchievements(pool).find((a) => a.id === id);
	assert.ok(ach, `career achievement ${id} should exist`);
	return ach!;
};

const seasonAch = (id: string) => {
	const ach = SEASON_ACHIEVEMENTS.find((a) => a.id === id);
	assert.ok(ach, `season achievement ${id} should exist`);
	return ach!;
};

beforeEach(() => {
	resetG();
	g.setWithoutSavingToDB("season", 2026);
});

describe("career achievements", () => {
	test("career stat thresholds", () => {
		const star = makePlayer({
			tot: { ...makePlayer().tot, pts: 25000, trb: 11000, ast: 6000 },
		});
		const role = makePlayer({ pid: 2 });
		const pool = makePool([star, role]);
		assert.ok(careerAch(pool, "career20kPoints").test(star));
		assert.ok(!careerAch(pool, "career20kPoints").test(role));
		assert.ok(careerAch(pool, "career10kRebounds").test(star));
		assert.ok(careerAch(pool, "career5kAssists").test(star));
	});

	test("stat thresholds adapt to the league instead of being fixed", () => {
		// A modest league: nobody is anywhere near 20,000 career points, so the
		// hand-written cutoff matches NOTHING and would just be dropped, costing
		// the grid variety. The generated ladder has to find rungs that actually
		// split this league's players.
		const modest = makePool(
			Array.from({ length: 40 }, (_, i) =>
				makePlayer({
					pid: i + 1,
					tot: { ...makePlayer().tot, pts: 2000 + i * 250 },
				}),
			),
		);
		const modestPoints = buildCareerAchievements(modest).filter((a) =>
			a.id.startsWith("adaptive_pts_"),
		);
		assert.ok(
			modestPoints.length > 0,
			"a modest league must still get points criteria",
		);
		// Every generated rung must actually match somebody here.
		for (const ach of modestPoints) {
			const n = modest.players.filter((p) => ach.test(p)).length;
			assert.ok(n > 0, `${ach.label} matched nobody`);
			assert.ok(
				n < modest.players.length,
				`${ach.label} matched everybody, so it constrains nothing`,
			);
		}

		// A high-scoring, long-history league should land on HIGHER cutoffs than
		// the modest one - that is the whole point of adapting.
		const loaded = makePool(
			Array.from({ length: 40 }, (_, i) =>
				makePlayer({
					pid: i + 1,
					tot: { ...makePlayer().tot, pts: 12000 + i * 900 },
				}),
			),
		);
		const loadedPoints = buildCareerAchievements(loaded).filter((a) =>
			a.id.startsWith("adaptive_pts_"),
		);
		assert.ok(loadedPoints.length > 0);

		const topRung = (list: typeof modestPoints) =>
			Math.max(...list.map((a) => Number(a.id.replace("adaptive_pts_", ""))));
		assert.ok(
			topRung(loadedPoints) > topRung(modestPoints),
			`expected a higher cutoff in the high-scoring league (${topRung(
				loadedPoints,
			)} vs ${topRung(modestPoints)})`,
		);
	});

	test("a generated threshold never pairs against its hand-written twin", () => {
		// Both must share a family, or a grid could show "20,000+ Career Points"
		// crossed with "12,500+ Career Points".
		const pool = makePool(
			Array.from({ length: 40 }, (_, i) =>
				makePlayer({
					pid: i + 1,
					tot: { ...makePlayer().tot, pts: 5000 + i * 800 },
				}),
			),
		);
		const all = buildCareerAchievements(pool);
		const fixed = all.find((a) => a.id === "career20kPoints")!;
		for (const generated of all.filter((a) =>
			a.id.startsWith("adaptive_pts_"),
		)) {
			assert.strictEqual(
				generated.family,
				fixed.family,
				`${generated.label} must share a family with the fixed points criterion`,
			);
		}
	});

	test("draft achievements are mutually reasonable", () => {
		const pool = makePool([makePlayer()]);
		const first = makePlayer({ draft: { round: 1, pick: 1, year: 1999 } });
		const second = makePlayer({ draft: { round: 2, pick: 31, year: 1999 } });
		const undrafted = makePlayer({ draft: { round: 0, pick: 0, year: 1999 } });
		assert.ok(careerAch(pool, "isPick1Overall").test(first));
		assert.ok(careerAch(pool, "isFirstRoundPick").test(first));
		assert.ok(!careerAch(pool, "isFirstRoundPick").test(second));
		assert.ok(careerAch(pool, "isSecondRoundPick").test(second));
		assert.ok(careerAch(pool, "isUndrafted").test(undrafted));
		assert.ok(!careerAch(pool, "isUndrafted").test(first));
	});

	test("drafted as a teenager", () => {
		const pool = makePool([makePlayer()]);
		const teen = makePlayer({
			bornYear: 1980,
			draft: { round: 1, pick: 13, year: 1999 },
		});
		const adult = makePlayer({
			bornYear: 1977,
			draft: { round: 1, pick: 13, year: 1999 },
		});
		const undrafted = makePlayer({
			bornYear: 1981,
			draft: { round: 0, pick: 0, year: 1999 },
		});
		assert.ok(careerAch(pool, "draftedTeen").test(teen));
		assert.ok(!careerAch(pool, "draftedTeen").test(adult));
		assert.ok(!careerAch(pool, "draftedTeen").test(undrafted));
	});

	test("ROY who later won MVP requires that order", () => {
		const pool = makePool([makePlayer()]);
		const rightOrder = makePlayer({
			awards: [
				{ season: 2000, type: "Rookie of the Year" },
				{ season: 2004, type: "Most Valuable Player" },
			],
		});
		const wrongOrder = makePlayer({
			awards: [
				{ season: 2004, type: "Rookie of the Year" },
				{ season: 2000, type: "Most Valuable Player" },
			],
		});
		assert.ok(careerAch(pool, "royLaterMVP").test(rightOrder));
		assert.ok(!careerAch(pool, "royLaterMVP").test(wrongOrder));
	});

	test("decade achievements are built from the league's year range", () => {
		const pool = makePool([
			makePlayer({ firstSeason: 1994, lastSeason: 2011 }),
		]);
		const achievements = buildCareerAchievements(pool);
		const ids = achievements.map((a) => a.id);
		assert.ok(ids.includes("playedIn1990s"));
		assert.ok(ids.includes("playedIn2000s"));
		assert.ok(ids.includes("debutedIn2010s"));
		assert.ok(ids.includes("playedInThreeDecades"));

		const nineties = achievements.find((a) => a.id === "playedIn1990s")!;
		const p90s = makePlayer({
			rows: [makeRow(1996, 0)],
		});
		const p00s = makePlayer({
			rows: [makeRow(2004, 0)],
		});
		assert.ok(nineties.test(p90s));
		assert.ok(!nineties.test(p00s));
	});

	test("played at age 40+", () => {
		const pool = makePool([makePlayer()]);
		const old = makePlayer({
			bornYear: 1970,
			rows: [makeRow(2011, 0)], // age 41
		});
		const young = makePlayer({
			bornYear: 1980,
			rows: [makeRow(2011, 0)],
		});
		assert.ok(careerAch(pool, "playedAtAge40Plus").test(old));
		assert.ok(!careerAch(pool, "playedAtAge40Plus").test(young));
	});
});

describe("season achievements", () => {
	test("mergedSeasons combines multi-stint seasons", () => {
		const traded = makePlayer({
			rows: [
				makeRow(2005, 0, { gp: 40, pts: 900 }),
				makeRow(2005, 3, { gp: 30, pts: 700 }),
			],
		});
		const merged = mergedSeasons(traded);
		assert.strictEqual(merged.size, 1);
		const s = merged.get(2005)!;
		assert.strictEqual(s.gp, 70);
		assert.strictEqual(s.pts, 1600);
	});

	test("Season30PPG needs both the rate and the games", () => {
		const qualifies = makePlayer({
			rows: [makeRow(2005, 0, { gp: 60, pts: 1900 })], // 31.7 ppg
		});
		const lowGames = makePlayer({
			rows: [makeRow(2005, 0, { gp: 30, pts: 1000 })], // 33 ppg but 30 gp
		});
		const ctx = { leaders: new Map() };
		assert.strictEqual(
			seasonAch("Season30PPG").seasons(qualifies, ctx).size,
			1,
		);
		assert.strictEqual(seasonAch("Season30PPG").seasons(lowGames, ctx).size, 0);
	});

	test("50/40/90 season requires all three splits at volume", () => {
		const qualifies = makePlayer({
			rows: [
				makeRow(2005, 0, {
					fga: 1000,
					fg: 520,
					tpa: 200,
					tp: 85,
					fta: 300,
					ft: 275,
				}),
			],
		});
		const missesFT = makePlayer({
			rows: [
				makeRow(2005, 0, {
					fga: 1000,
					fg: 520,
					tpa: 200,
					tp: 85,
					fta: 300,
					ft: 260, // 86.7%
				}),
			],
		});
		const ctx = { leaders: new Map() };
		assert.strictEqual(
			seasonAch("Season50_40_90").seasons(qualifies, ctx).size,
			1,
		);
		assert.strictEqual(
			seasonAch("Season50_40_90").seasons(missesFT, ctx).size,
			0,
		);
	});

	test("award seasons match BBGM's exact award strings", () => {
		const p = makePlayer({
			awards: [
				{ season: 2003, type: "Most Valuable Player" },
				{ season: 2003, type: "First Team All-League" },
				{ season: 2004, type: "Won Championship" },
			],
		});
		const ctx = { leaders: new Map() };
		assert.deepStrictEqual([...seasonAch("MVP").seasons(p, ctx)], [2003]);
		assert.deepStrictEqual(
			[...seasonAch("AllLeagueAny").seasons(p, ctx)],
			[2003],
		);
		assert.deepStrictEqual([...seasonAch("Champion").seasons(p, ctx)], [2004]);
		assert.strictEqual(seasonAch("DPOY").seasons(p, ctx).size, 0);
	});
});

describe("season context (league leaders)", () => {
	test("scoring leader needs 70% of games and the best per-game rate", () => {
		const volumeStar = makePlayer({
			pid: 1,
			rows: [makeRow(2005, 0, { gp: 70, pts: 2100 })], // 30 ppg
		});
		const efficientButAbsent = makePlayer({
			pid: 2,
			rows: [makeRow(2005, 1, { gp: 20, pts: 800 })], // 40 ppg, 20 gp
		});
		const pool = makePool([volumeStar, efficientButAbsent]);
		const ctx = buildSeasonContext(pool);
		assert.strictEqual(ctx.leaders.get("PointsLeader")!.get(2005), 1);
	});
});

describe("season index (team attachment)", () => {
	test("an award attaches to the primary (most minutes) team that season", () => {
		const traded = makePlayer({
			pid: 7,
			awards: [{ season: 2005, type: "Most Valuable Player" }],
			rows: [
				makeRow(2005, 0, { min: 900 }),
				makeRow(2005, 3, { min: 1800 }), // primary
			],
		});
		const pool = makePool([traded]);
		const ctx = buildSeasonContext(pool);
		const index = buildSeasonIndex(pool, ctx);
		assert.ok(index.get(3)?.get("MVP")?.has(7), "attaches to primary team");
		assert.ok(!index.get(0)?.get("MVP")?.has(7), "not the other stint");
	});

	test("a stat-line season attaches to every team played for that season", () => {
		const traded = makePlayer({
			pid: 8,
			rows: [
				makeRow(2005, 0, { gp: 40, pts: 1300 }),
				makeRow(2005, 3, { gp: 30, pts: 900 }), // merged: 2200 pts
			],
		});
		const pool = makePool([traded]);
		const ctx = buildSeasonContext(pool);
		const index = buildSeasonIndex(pool, ctx);
		assert.ok(index.get(0)?.get("Season2000Points")?.has(8));
		assert.ok(index.get(3)?.get("Season2000Points")?.has(8));
	});
});
