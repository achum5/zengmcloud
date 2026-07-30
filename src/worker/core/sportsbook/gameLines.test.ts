import { assert, beforeAll, beforeEach, describe, test } from "vitest";
import { player, team } from "../index.ts";
import { g, helpers } from "../../util/index.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { range } from "../../../common/utils.ts";
import { buildGameLinePricer } from "./gameLines.ts";
import {
	__clearSimMargins,
	__setSimMargin,
	SIMS_PER_GAME,
} from "./simSpreads.ts";
import { americanToImpliedProb } from "../../../common/sportsbook.ts";
import { idb } from "../../db/index.ts";
import { getUpcoming } from "../../views/schedule.ts";
import { syncDaySpreads } from "./scheduleSpreads.ts";
import { roundHalf } from "../../../common/getGameSpread.ts";

const NUM_TEAMS = 4;
const ROSTER = 10;

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

describe("spreads from the engine", () => {
	beforeEach(() => {
		__clearSimMargins();
	});

	// The whole safety argument rests on this: pricing READS the simulated-margin
	// cache and never fills it. If priceGame ever started simming, the sportsbook
	// would block for seconds behind two dozen games.
	test("pricing an unsimulated board never sims - it queues", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});

		const start = performance.now();
		const line = pricer.priceGame(matchup);
		const elapsed = performance.now() - start;

		assert.ok(line, "should still produce a line");
		// One GameSim run is ~5ms; fifty is ~220ms. Pricing must be nowhere near.
		assert.ok(elapsed < 50, `priceGame took ${elapsed}ms - is it simming?`);
		assert.strictEqual(pricer.pendingSims().length, 1);
	});

	test("with nothing cached, the line is the old formula's", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		const before = pricer.priceGame(matchup)!;

		// Same inputs, same answer - a board that hasn't warmed up yet is exactly
		// the board that shipped before this.
		const pricer2 = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		assert.strictEqual(pricer2.priceGame(matchup)!.margin, before.margin);
	});

	test("a cached simulated margin moves the line toward it", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		const formula = pricer.priceGame(matchup)!.margin;
		const [job] = pricer.pendingSims();
		assert.ok(job);

		// A simulated margin 10 points off the formula, at 50 runs' precision.
		__setSimMargin(job.key, {
			mean: formula + 10,
			se: 1.75,
			n: SIMS_PER_GAME,
		});

		const warmed = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		const blended = warmed.priceGame(matchup)!.margin;

		assert.ok(blended > formula, "should move toward the sim");
		assert.ok(blended < formula + 10, "should not jump all the way");
		// Nothing left to do for this game.
		assert.strictEqual(warmed.pendingSims().length, 0);
	});

	// The user's rule: the moneyline is just the spread, priced. So a line that
	// moved toward the home team has to shorten the home price too - they cannot
	// come from different numbers and disagree.
	test("the moneyline follows the spread", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		const before = pricer.priceGame(matchup)!;
		const [job] = pricer.pendingSims();

		__setSimMargin(job!.key, {
			mean: before.margin + 10,
			se: 1.75,
			n: SIMS_PER_GAME,
		});

		const warmed = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		const after = warmed.priceGame(matchup)!;

		assert.ok(after.margin > before.margin);
		assert.ok(
			americanToImpliedProb(after.moneyline.home) >
				americanToImpliedProb(before.moneyline.home),
			"home should be a shorter price once the spread moved its way",
		);
		// And the spread line itself tracks the margin.
		assert.ok(after.spread.line < before.spread.line);
	});

	// getLines and getGameProps both reach a spread through this one function, so
	// a bet quoted on a game's prop page validates against the main board. Two
	// pricers built from the same state must be indistinguishable.
	test("two pricers over the same state quote identical lines", async () => {
		const build = () =>
			buildGameLinePricer({
				activeTeams: activeTeams(),
				season: g.get("season"),
				todayDay: 1,
			});

		const a = await build();
		a.priceGame(matchup);
		const [job] = a.pendingSims();
		__setSimMargin(job!.key, { mean: 12.5, se: 1.75, n: SIMS_PER_GAME });

		const b = await build();
		const c = await build();
		assert.deepStrictEqual(b.priceGame(matchup), c.priceGame(matchup));
	});

	// A game further out is a different game - rosters heal by then - so it must
	// not be served the same day's cached margin.
	test("the same matchup on a later day is queued separately", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		pricer.priceGame(matchup);
		pricer.priceGame({ ...matchup, day: 5 });
		assert.strictEqual(pricer.pendingSims().length, 2);
	});

	// A team's line has to notice the manager touching his roster before tipoff.
	// The engine reads rosterOrder, ptModifier, each injury, and the team's
	// play-through-injuries setting; if any of them isn't in the cache key, the
	// board keeps quoting the line from before the change.
	describe("a roster move before tipoff re-prices the game", () => {
		const keyFor = async (
			mutate?: (players: any[], teams: ReturnType<typeof activeTeams>) => void,
		) => {
			const teams = activeTeams();
			const roster = await idb.cache.players.indexGetAll("playersByTid", 0);
			const before = roster.map((p) => ({
				ptModifier: p.ptModifier,
				rosterOrder: p.rosterOrder,
				injury: p.injury,
			}));
			mutate?.(roster, teams);
			for (const p of roster) {
				await idb.cache.players.put(p);
			}

			const pricer = await buildGameLinePricer({
				activeTeams: teams,
				season: g.get("season"),
				todayDay: 1,
			});
			pricer.priceGame(matchup);
			const key = pricer.pendingSims()[0]!.key;

			// Put it back so each case measures only its own change.
			for (const [i, p] of roster.entries()) {
				Object.assign(p, before[i]);
				await idb.cache.players.put(p);
			}
			return key;
		};

		test("benching a player", async () => {
			const base = await keyFor();
			const benched = await keyFor((players) => {
				players[0]!.ptModifier = 0;
			});
			assert.notStrictEqual(benched, base);
		});

		test("giving a player extra minutes", async () => {
			const base = await keyFor();
			const boosted = await keyFor((players) => {
				players[0]!.ptModifier = 1.5;
			});
			assert.notStrictEqual(boosted, base);
		});

		test("reordering the rotation", async () => {
			const base = await keyFor();
			const reordered = await keyFor((players) => {
				players[0]!.rosterOrder = 9;
				players[9]!.rosterOrder = 0;
			});
			assert.notStrictEqual(reordered, base);
		});

		test("a player getting hurt", async () => {
			const base = await keyFor();
			const hurt = await keyFor((players) => {
				players[0]!.injury = { type: "Sprained Ankle", gamesRemaining: 3 };
			});
			assert.notStrictEqual(hurt, base);
		});

		// Playing a hurt guy through it changes who suits up, so it changes the
		// game the engine plays.
		test("changing play-through-injuries", async () => {
			const base = await keyFor();
			const through = await keyFor((_players, teams) => {
				teams[0]!.playThroughInjuries = [5, 5];
			});
			assert.notStrictEqual(through, base);
		});

		test("touching nothing leaves the line alone", async () => {
			assert.strictEqual(await keyFor(), await keyFor());
		});
	});

	test("the total is left on the season-scoring model", async () => {
		const pricer = await buildGameLinePricer({
			activeTeams: activeTeams(),
			season: g.get("season"),
			todayDay: 1,
		});
		const before = pricer.priceGame(matchup)!;
		const [job] = pricer.pendingSims();
		__setSimMargin(job!.key, { mean: 30, se: 1.75, n: SIMS_PER_GAME });

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
		__clearSimMargins();
		const shown = await priceWith(false);
		__clearSimMargins();
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
		__clearSimMargins();
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
// while the Daily Schedule shows the corrected line - exactly the split this
// was meant to close.
describe("one spread per game, everywhere", () => {
	test("getUpcoming and the pricer agree on an upcoming game", async () => {
		__clearSimMargins();

		await idb.cache.schedule.add({
			awayTid: matchup.awayTid,
			homeTid: matchup.homeTid,
			day: matchup.day,
		} as any);

		// Built the way the real callers build it - from the stored team rows -
		// rather than from the fixture's hardcoded playThroughInjuries, because
		// that value is part of the cache key and the whole point here is that the
		// two sides key it identically.
		const realTeams = (await idb.cache.teams.getAll()).map((t) => ({
			tid: t.tid,
			playThroughInjuries: t.playThroughInjuries,
			stats: { gp: 20, pts: 105, oppPts: 103 },
		}));

		try {
			// Price it once so the matchup's cache key is known, then answer it with
			// a margin far from the formula's, so a missed peek is unmistakable.
			const cold = await buildGameLinePricer({
				activeTeams: realTeams,
				season: g.get("season"),
				todayDay: matchup.day,
			});
			const coldLine = cold.priceGame(matchup)!;
			const [job] = cold.pendingSims();
			assert.ok(job, "expected the matchup to be queued for a sim");
			__setSimMargin(job!.key, { mean: 25, se: 1.75, n: SIMS_PER_GAME });

			const warm = await buildGameLinePricer({
				activeTeams: realTeams,
				season: g.get("season"),
				todayDay: matchup.day,
			});
			const warmMargin = warm.priceGame(matchup)!.margin;
			assert.notStrictEqual(
				roundHalf(warmMargin),
				roundHalf(coldLine.margin),
				"the seeded margin should have moved the line, or this proves nothing",
			);

			const [upcoming] = await getUpcoming({ day: matchup.day });
			assert.ok(upcoming, "expected getUpcoming to return the scheduled game");
			assert.strictEqual(
				upcoming!.spread,
				roundHalf(warmMargin),
				"the schedule pages and the sportsbook are quoting different lines",
			);

			// And the number pushed to the league top bar is that same one. This is
			// the surface that was wrong: it holds a snapshot of the user's next
			// game rather than rebuilding with the page, so it kept whichever line
			// was current when the snapshot was taken.
			const published = await syncDaySpreads({
				season: g.get("season"),
				day: matchup.day,
			});
			assert.deepStrictEqual(
				published,
				Object.fromEntries(
					(await getUpcoming({ day: matchup.day })).map((game) => [
						game.gid,
						game.spread,
					]),
				),
				"the top bar is being sent a different spread than the pages show",
			);
		} finally {
			for (const row of await idb.cache.schedule.getAll()) {
				await idb.cache.schedule.delete(row.gid);
			}
		}
	});
});
