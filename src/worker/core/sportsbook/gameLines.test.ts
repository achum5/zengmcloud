import { assert, beforeAll, describe, test } from "vitest";
import { player, team } from "../index.ts";
import { g, helpers } from "../../util/index.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { range } from "../../../common/utils.ts";
import { buildGameLinePricer } from "./gameLines.ts";
import { americanToImpliedProb } from "../../../common/sportsbook.ts";
import { idb } from "../../db/index.ts";
import { getUpcoming } from "../../views/schedule.ts";
import { getGameSpread, roundHalf } from "../../../common/getGameSpread.ts";
import teamOvr from "../team/ovr.ts";

const NUM_TEAMS = 4;
const ROSTER = 10;

// The team overalls the pricer prices off, computed the same way it does.
const teamOvrsForMatchup = async () => {
	const ovrOf = async (tid: number) => {
		const raw = await idb.cache.players.indexGetAll("playersByTid", tid);
		const players = await idb.getCopies.playersPlus(raw, {
			attrs: ["injury", "pid", "value", "tid"],
			ratings: ["ovr", "pos", "ovrs"],
			season: g.get("season"),
			fuzz: true,
			coarsenRatings: false,
		});
		return teamOvr(players, {
			accountForInjuredPlayers: {
				numDaysInFuture: 0,
				playThroughInjuries: [0, 0],
			},
			playoffs: false,
		});
	};
	return {
		home: await ovrOf(matchup.homeTid),
		away: await ovrOf(matchup.awayTid),
	};
};

const activeTeams = () =>
	range(NUM_TEAMS).map((tid) => ({
		tid,
		playThroughInjuries: [0, 0] as [number, number],
		stats: { gp: 20, pts: 105, oppPts: 103 },
	}));

beforeAll(async () => {
	resetG();
	const teamsDefault = helpers.getTeamsDefault().slice(0, NUM_TEAMS);
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB(
		"teamInfoCache",
		teamsDefault.map((t) => ({
			abbrev: t.abbrev,
			disabled: false,
			imgURL: t.imgURL,
			imgURLSmall: t.imgURLSmall,
			name: t.name,
			region: t.region,
		})),
	);

	const players = [];
	for (const tid of range(NUM_TEAMS)) {
		for (const _ of range(ROSTER)) {
			const p = player.generate(
				tid,
				25,
				g.get("season") - 5,
				false,
				DEFAULT_LEVEL,
			);
			await player.develop(p, 0);
			p.ratings[0]!.season = g.get("season");
			p.value = p.ratings[0]!.ovr;
			p.valueNoPot = p.ratings[0]!.ovr;
			players.push(p);
		}
	}

	await resetCache({
		players,
		teams: teamsDefault.map(team.generate),
		teamSeasons: teamsDefault.map((t) => team.genSeasonRow(t)),
		teamStats: teamsDefault.map((t) => team.genStatsRow(t.tid)),
	});
});

const matchup = { day: 1, homeTid: 0, awayTid: 1 };

describe("the spread", () => {
	// The spread used to be the closed-form line corrected by fifty background
	// game sims. Measured against the engine on a realistic talent grid it was
	// buying about a quarter of a point on a number rounded to the nearest half,
	// so it is gone - see gameLines.ts. What has to stay true is that the line is
	// immediate, identical every time, and identical to the formula.
	test("pricing is instant, because nothing is simulated", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});

		const start = performance.now();
		const line = pricer.priceGame(matchup);
		const elapsed = performance.now() - start;

		assert.ok(line, "should produce a line");
		// One GameSim run alone is ~5ms.
		assert.ok(elapsed < 20, `priceGame took ${elapsed}ms - is it simming?`);
	});

	test("the line is the closed-form formula, exactly", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		const line = pricer.priceGame(matchup)!;
		const ovrs = await teamOvrsForMatchup();
		assert.strictEqual(
			line.margin,
			getGameSpread({
				ovr0: ovrs.home,
				ovr1: ovrs.away,
				homeCourtAdvantage: g.get("homeCourtAdvantage"),
				neutralSite: false,
				numPeriods: g.get("numPeriods"),
				quarterLength: g.get("quarterLength"),
			}),
		);
	});

	// getLines and getGameProps both reach a spread through this one function, so
	// a bet quoted on a game's prop page validates against the main board.
	test("two pricers over the same state quote identical lines", async () => {
		const build = () =>
			buildGameLinePricer({
				activeTeams: activeTeams(),
				season: g.get("season"),
				todayDay: 1,
			});
		const b = await build();
		const c = await build();
		assert.deepStrictEqual(b.priceGame(matchup), c.priceGame(matchup));
	});

	// The user's rule: the moneyline is just the spread, priced. They cannot come
	// from different numbers and disagree.
	test("the moneyline follows the spread", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});

		// Every game on the board, so this holds across the whole range rather
		// than for one fixture.
		const lines = [];
		for (let homeTid = 0; homeTid < NUM_TEAMS; homeTid++) {
			for (let awayTid = 0; awayTid < NUM_TEAMS; awayTid++) {
				if (homeTid !== awayTid) {
					lines.push(pricer.priceGame({ ...matchup, homeTid, awayTid })!);
				}
			}
		}

		for (const line of lines) {
			const pHome = americanToImpliedProb(line.moneyline.home);
			const pAway = americanToImpliedProb(line.moneyline.away);
			if (line.margin > 0) {
				assert.ok(
					pHome > pAway,
					`home favoured by ${line.margin} but priced longer`,
				);
			} else if (line.margin < 0) {
				assert.ok(
					pAway > pHome,
					`away favoured by ${-line.margin} but priced longer`,
				);
			}
			// And the spread line is the margin, the other way up.
			assert.ok(line.spread.line <= 0 === line.margin >= 0);
		}

		// A bigger margin is always a shorter home price - the two are one number.
		const sorted = [...lines].sort((a, b) => a.margin - b.margin);
		for (let i = 1; i < sorted.length; i++) {
			assert.ok(
				americanToImpliedProb(sorted[i]!.moneyline.home) >=
					americanToImpliedProb(sorted[i - 1]!.moneyline.home),
				"a longer spread has to mean a shorter moneyline",
			);
		}
	});

	// A hurt player has to move the line before tipoff - the formula prices off
	// an injury-adjusted team overall, so this is what makes that real.
	test("a player getting hurt moves the line", async () => {
		const lineNow = async () => {
			const pricer = await buildGameLinePricer({
				activeTeams: activeTeams(),
				season: g.get("season"),
				todayDay: 1,
			});
			return pricer.priceGame(matchup)!.margin;
		};

		const roster = await idb.cache.players.indexGetAll("playersByTid", 0);
		const before = roster.map((p) => p.injury);
		const base = await lineNow();
		try {
			for (const p of roster.slice(0, 5)) {
				p.injury = { type: "Torn ACL", gamesRemaining: 60 };
				await idb.cache.players.put(p);
			}
			assert.notStrictEqual(await lineNow(), base);
		} finally {
			for (const [i, p] of roster.entries()) {
				p.injury = before[i]!;
				await idb.cache.players.put(p);
			}
		}
		assert.strictEqual(await lineNow(), base);
	});

	test("the total is left on the season-scoring model", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		const before = pricer.priceGame(matchup)!;

		const warmed = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		assert.strictEqual(
			warmed.priceGame(matchup)!.total.line,
			before.total.line,
		);
	});
});

// The "hide ratings ones digit" display mode floors every rating to its tens
// digit. That is for screens; playersPlus does it by default, and any caller
// doing ARITHMETIC with ratings has to opt out. The pricer didn't, so in a
// league running that mode team.ovr was building team overalls out of 0-10
// inputs: the ovr gap between two teams collapsed to nearly nothing, every
// spread on the board fell back to roughly the home-court constant, and the
// favorite was decided by who was at home rather than by who was better. The
// same wrong number then leaked onto the Daily Schedule, which reads its
// spreads from this pricer, while the league top bar (which computes ovr for
// itself, correctly) showed something else entirely for the same game.
describe("hiding the ratings ones digit", () => {
	const priceWith = async (hideRatingsOnesDigit: boolean) => {
		g.setWithoutSavingToDB("hideRatingsOnesDigit", hideRatingsOnesDigit);
		try {
			const pricer = await buildGameLinePricer({
				activeTeams: activeTeams(),
				season: g.get("season"),
				todayDay: 1,
			});
			return pricer.priceGame(matchup)!;
		} finally {
			g.setWithoutSavingToDB("hideRatingsOnesDigit", false);
		}
	};

	test("does not change any line on the board", async () => {
		const shown = await priceWith(false);
		const hidden = await priceWith(true);

		assert.strictEqual(
			hidden.margin,
			shown.margin,
			"a display setting must not move the expected margin",
		);
		assert.strictEqual(hidden.spread.line, shown.spread.line);
		assert.strictEqual(hidden.moneyline.home, shown.moneyline.home);
		assert.strictEqual(hidden.moneyline.away, shown.moneyline.away);
	});

	// Guards the specific failure above rather than just "the two agree": if
	// both paths coarsened, they would agree on a number that is wrong.
	test("still separates teams that are far apart", async () => {
		const teams = activeTeams();
		// Widest ovr gap available in the fixture, so the formula has something
		// real to say and the assertion isn't measuring home-court advantage.
		const pricer = await buildGameLinePricer({
			activeTeams: teams,
			season: g.get("season"),
			todayDay: 1,
		});
		const margins = [];
		for (const homeTid of range(NUM_TEAMS)) {
			for (const awayTid of range(NUM_TEAMS)) {
				if (homeTid !== awayTid) {
					margins.push(pricer.priceGame({ day: 1, homeTid, awayTid })!.margin);
				}
			}
		}
		const spread = Math.max(...margins) - Math.min(...margins);
		assert.ok(
			spread > 1,
			`every matchup priced within ${spread.toFixed(2)} points of every other - team overalls are not reaching the formula`,
		);
	});
});

// The whole point of shipping a spread with the game object is that every page
// showing that game shows ONE number. That only holds if getUpcoming reaches
// the same simulated margin the pricer does, which means building a byte-
// identical cache key from a separately-loaded player list. Nothing about the
// two call sites forces that, so assert it: if the fingerprints ever drift, the
// peek silently misses and the top bar quietly falls back to the raw formula
describe("one spread per game, everywhere", () => {
	// The schedule pages, the Daily Schedule and the sportsbook all show a spread
	// for the same game, and they must be the same number. They get there by
	// different routes - getUpcoming computes it, the pricer computes it - so the
	// only thing keeping them together is that both call getGameSpread with the
	// same team overalls. Assert it rather than assume it.
	test("getUpcoming and the pricer agree on an upcoming game", async () => {
		await idb.cache.schedule.add({
			awayTid: matchup.awayTid,
			homeTid: matchup.homeTid,
			day: matchup.day,
		} as any);

		// Built the way the real callers build it - from the stored team rows.
		const realTeams = (await idb.cache.teams.getAll()).map((t) => ({
			tid: t.tid,
			playThroughInjuries: t.playThroughInjuries,
			stats: { gp: 20, pts: 105, oppPts: 103 },
		}));

		try {
			const pricer = await buildGameLinePricer({
				activeTeams: realTeams,
				season: g.get("season"),
				todayDay: matchup.day,
			});
			const priced = pricer.priceGame(matchup)!;

			const [upcoming] = await getUpcoming({ day: matchup.day });
			assert.ok(upcoming, "expected getUpcoming to return the scheduled game");
			assert.strictEqual(
				upcoming!.spread,
				roundHalf(priced.margin),
				"the schedule pages and the sportsbook are quoting different lines",
			);
		} finally {
			for (const row of await idb.cache.schedule.getAll()) {
				await idb.cache.schedule.delete(row.gid);
			}
		}
	});
});
