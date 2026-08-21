import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { draft, player, team, trade } from "../index.ts";
import autoSign from "./autoSign.ts";
import clearSpaceForSignings from "./clearSpace.ts";
import decreaseDemands from "./decreaseDemands.ts";
import newPhaseResignPlayers from "../phase/newPhaseResignPlayers.ts";
import createRandomPlayers from "../league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
} from "../trade/tradePosture.ts";

// ---------------------------------------------------------------------------
// DECADES OF OFFSEASONS, END TO END
//
// Every other test of the AI front office looks at one decision or one
// offseason. This one runs the whole cycle - draft, re-sign, thirty days of
// free agency, roster cuts, develop, retire - for over a decade, because the
// failures worth catching are the ones that COMPOUND. Two that this harness
// actually caught while it was being built:
//
//   - An additive upside bonus that let a rebuilder sign a prospect over a
//     26-points-better star. One misplaced signing is a shrug; a league of
//     them, every summer, left stars unemployed until they retired.
//   - Sign-and-cut churn: fit-driven shopping past the roster limit released
//     a guaranteed contract every few days, and the dead money grew until no
//     team could afford anyone.
//
// The assertions are deliberately loose canaries. They fire on catastrophe
// (league talent collapsing, stars going unemployed, illegal rosters), not on
// tuning drift.
//
// KNOWN LIMITS: no games are simulated (standings are synthesized from roster
// strength) and no AI-AI trades run, so a teardown here cannot convert its
// veterans into picks - rebuild payoff reads slower than the real game.
//
// Knobs for manual deep runs, none of which are set in CI:
//   SEASONS=40 NUM_TEAMS=30 SEED=123 DECADES_LOG=/tmp/decades.log \
//     npx vitest --run src/worker/core/freeAgents/decadesSim.test.ts
// SMART_AI=0 runs the same league on the vanilla front office for comparison.
// ---------------------------------------------------------------------------

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const NUM_TEAMS = Number(nodeEnv.NUM_TEAMS ?? 16);
const SEASONS = Number(nodeEnv.SEASONS ?? 12);
const FA_DAYS = 30;
// Roughly "the best player on a decent team" - the players a league cannot
// afford to leave sitting in free agency.
const STAR_OVR = 65;

// Deterministic PRNG so a failing run reproduces exactly from its seed.
const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

// Enough of idb.league for code that reaches past the cache (addRelatives
// iterates the players store; various getCopies fall through on misses).
const stubLeagueDb = () => {
	const store = {
		index: () => store,
		getAll: async () => [],
		get: async () => undefined,

		async *iterate() {},
	};
	(idb as any).league = {
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
		getAll: async () => [],
		get: async () => undefined,
	};
};

const build = async () => {
	resetG();
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("userTids", []);
	g.setWithoutSavingToDB("userTid", 0);
	g.setWithoutSavingToDB("realisticFaces", false);

	const teams: any[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push(
			team.generate({
				tid,
				cid: tid % 2,
				did: tid % 2,
				region: `R${tid}`,
				name: `N${tid}`,
				abbrev: `T${tid}`,
				pop: 2,
				imgURL: "",
			} as any),
		);
	}

	// The real random-league generator: twenty years of draft classes,
	// developed forward and distributed to teams, so ratings are internally
	// consistent from the first season.
	const players = await createRandomPlayers({
		activeTids: teams.map((t) => t.tid),
		onlyFreeAgents: false,
		scoutingLevel: DEFAULT_LEVEL,
		teams,
	});

	await resetCache({ players, teams, draftPicks: [] });
	stubLeagueDb();
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const t = (await idb.cache.teams.get(tid))!;
		await idb.cache.teamSeasons.add(team.genSeasonRow(t) as any);
	}
};

// How many future drafts of picks exist to be traded at any moment. The trade
// AI prices future picks (futurePickOutlook), and a seller's whole reward for
// a teardown is collecting them - so picks must SURVIVE from year to year,
// with their traded ownership intact, rather than be wiped and regenerated.
const PICK_HORIZON = 3;

let nextDpid = 0;
const ensureDraftClass = async (season: number) => {
	// The real class generator, so the class is the size and shape the game
	// actually produces for this many teams.
	await draft.genPlayers(season, DEFAULT_LEVEL);

	// Retire picks from drafts already held (runPicks consumed the players but
	// the pick rows remain), keep everything else - including picks that were
	// traded, whose owner is not their original team.
	const existing = new Map<string, number>();
	for (const dp of await idb.cache.draftPicks.getAll()) {
		const dpSeason = dp.season;
		if (typeof dpSeason !== "number") {
			continue;
		}
		if (dpSeason < season) {
			await idb.cache.draftPicks.delete(dp.dpid);
		} else {
			existing.set(`${dpSeason}:${dp.round}:${dp.originalTid}`, dp.dpid);
		}
	}

	// Draft order for THIS season, by reverse standings of the pick's original
	// team - trading for a bad team's pick is supposed to buy its lottery slot.
	const ctx = await getLeagueTradeContext();
	const slotByTid = new Map<number, number>();
	for (const [i, { tid }] of [...ctx.teamOvrsSorted].reverse().entries()) {
		slotByTid.set(tid, i + 1);
	}

	for (let future = season; future <= season + PICK_HORIZON; future++) {
		for (const round of [1, 2]) {
			for (let tid = 0; tid < NUM_TEAMS; tid++) {
				if (!existing.has(`${future}:${round}:${tid}`)) {
					await idb.cache.draftPicks.add({
						dpid: nextDpid++,
						tid,
						originalTid: tid,
						round,
						// Unknown until that season's standings exist.
						pick: 0,
						season: future,
					} as any);
				}
			}
		}
	}

	// This season's slots are known now, whoever owns the picks.
	for (const dp of await idb.cache.draftPicks.getAll()) {
		if (dp.season === season) {
			dp.pick = slotByTid.get(dp.originalTid as number)!;
			await idb.cache.draftPicks.put(dp);
		}
	}
};

// Standings synthesized from roster strength - the harness does not sim games,
// and "the better roster wins more" is the honest stand-in.
const setRecords = async (rng: () => number) => {
	const context = await getLeagueTradeContext();
	const season = g.get("season");
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const rank = context.teamOvrsSorted.findIndex((x) => x.tid === tid);
		const base = 0.75 - (0.5 * rank) / Math.max(1, NUM_TEAMS - 1);
		const winp = Math.max(0.1, Math.min(0.9, base + (rng() - 0.5) * 0.14));
		const existing = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[season, tid],
		);
		const row: any =
			existing ?? team.genSeasonRow((await idb.cache.teams.get(tid))!);
		row.season = season;
		row.tid = tid;
		row.won = Math.round(82 * winp);
		row.lost = 82 - row.won;
		row.gp = 82;
		if (existing) {
			await idb.cache.teamSeasons.put(row);
		} else {
			await idb.cache.teamSeasons.add(row);
		}
	}
};

// End of season, then preseason: what the real game does to a player between
// one season and the next.
const rollForward = async (season: number) => {
	// The annual dead-money purge from newPhaseBeforeDraft. Without it,
	// getPayroll counts expired released contracts forever - the real game
	// deletes them every June - and the simulated league slowly suffocates
	// under payroll that does not exist.
	for (const rp of await idb.cache.releasedPlayers.getAll()) {
		if (rp.contract.exp <= season && typeof rp.rid === "number") {
			await idb.cache.releasedPlayers.delete(rp.rid);
		}
	}

	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		PLAYER.FREE_AGENT,
		Infinity,
	])) {
		if (await player.shouldRetire(p)) {
			await player.retire(p, {});
		} else if (p.tid === PLAYER.FREE_AGENT) {
			p.yearsFreeAgent += 1;
		}
		await idb.cache.players.put(p);
	}

	g.setWithoutSavingToDB("season", season + 1);

	const players = await idb.cache.players.indexGetAll("playersByTid", [
		PLAYER.FREE_AGENT,
		Infinity,
	]);
	for (const p of players) {
		player.addRatingsRow(p, DEFAULT_LEVEL);
		await player.develop(p, 1, false, DEFAULT_LEVEL);
	}
	local.playerOvrMeanStdStale = true;
	for (const p of players) {
		await player.updateValues(p);
		await idb.cache.players.put(p);
	}
};

describe("a league runs for a decade without falling apart", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("the full offseason cycle", async () => {
		const rng = makeRng(Number(nodeEnv.SEED ?? 31337));
		// NOT vi.spyOn: a spy records every call it sees, and a decade of
		// simulation calls Math.random tens of millions of times - the mock's
		// call log alone ran the process out of heap.
		const realRandom = Math.random;
		Math.random = rng;

		const rows: string[] = [];
		// Per-team history, for the questions only a decade can answer: does a
		// rebuild ever pay off, does anyone get stuck at the bottom forever.
		const history: { tid: number; tier: string; winp: number }[][] = [];

		// The canaries.
		let illegalRosterSeasons = 0;
		let unsignedStarTotal = 0;
		let positionlessTotal = 0;
		let worstDeadShare = 0;
		const lastTovrs: number[] = [];
		const tiersSeen = new Set<string>();

		try {
			await build();
			g.setWithoutSavingToDB("smartAiFrontOffice", nodeEnv.SMART_AI !== "0");
			const salaryCap = g.get("salaryCap");

			for (let year = 0; year < SEASONS; year++) {
				const season = g.get("season");
				await ensureDraftClass(season);
				await setRecords(rng);

				// THE DEADLINE WINDOW. Mid-season is where the other half of the
				// trade AI lives - contenders renting expiring veterans, sellers
				// cashing in players who will walk - and none of it is reachable
				// from the offseason phases. A week of in-season ticks opens it.
				// (There is no schedule here, so the deadline frenzy ramp stays at
				// its base rate - what is being exercised is the motivation logic,
				// not the frenzy.)
				g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
				if (nodeEnv.NO_TRADES !== "1") {
					for (let tick = 0; tick < 7; tick++) {
						await trade.betweenAiTeams();
					}
				}

				g.setWithoutSavingToDB("phase", PHASE.DRAFT);
				await draft.runPicks({ type: "untilEnd" }, {} as any);

				g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
				await newPhaseResignPlayers({} as any);

				g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
				for (let day = FA_DAYS; day > 0; day--) {
					g.setWithoutSavingToDB("daysLeft", day);
					await decreaseDemands();
					await clearSpaceForSignings();
					await autoSign();
					// The real FA day ends with AI teams talking to each other
					// (freeAgents/play.ts does exactly this), and it is the channel
					// a rebuild actually runs through - veterans out, picks in.
					// NO_TRADES=1 closes it, for measuring what trading contributes.
					if (nodeEnv.NO_TRADES !== "1") {
						await trade.betweenAiTeams();
					}
				}
				await team.checkRosterSizes("other");

				// Measure the season.
				const ctx = await getLeagueTradeContext();
				const tierCounts: Record<string, number> = {};
				const ovrs: number[] = [];
				let illegal = 0;
				let positionless = 0;
				let payroll = 0;
				for (let tid = 0; tid < NUM_TEAMS; tid++) {
					const posture = await getTradePosture(tid, ctx);
					tierCounts[posture.tier] = (tierCounts[posture.tier] ?? 0) + 1;
					tiersSeen.add(posture.tier);
					const roster = await idb.cache.players.indexGetAll(
						"playersByTid",
						tid,
					);
					if (
						roster.length < g.get("minRosterSize") ||
						roster.length > g.get("maxRosterSize")
					) {
						illegal += 1;
					}
					let bigs = 0;
					let guards = 0;
					for (const p of roster) {
						payroll += p.contract.amount;
						const pos = p.ratings.at(-1)!.pos;
						if (pos === "C" || pos === "PF" || pos === "FC") {
							bigs += 1;
						}
						if (pos === "PG" || pos === "SG" || pos === "G") {
							guards += 1;
						}
					}
					if (bigs === 0 || guards === 0) {
						positionless += 1;
					}
					ovrs.push(
						team.ovr(
							roster.map((p) => ({
								pid: p.pid,
								injury: p.injury,
								value: p.value,
								ratings: {
									ovr: p.ratings.at(-1)!.ovr,
									ovrs: p.ratings.at(-1)!.ovrs,
									pos: p.ratings.at(-1)!.pos,
								},
							})),
						),
					);
				}
				if (illegal > 0) {
					illegalRosterSeasons += 1;
				}
				positionlessTotal += positionless;

				const fa = await idb.cache.players.indexGetAll(
					"playersByTid",
					PLAYER.FREE_AGENT,
				);
				const unsignedStars = fa.filter(
					(p) => p.ratings.at(-1)!.ovr >= STAR_OVR,
				).length;
				unsignedStarTotal += unsignedStars;

				// Trades executed this season and picks living away from home.
				let tradedPicks = 0;
				for (const dp of await idb.cache.draftPicks.getAll()) {
					if (dp.tid !== dp.originalTid) {
						tradedPicks += 1;
					}
				}
				const seasonTrades = (await idb.cache.events.getAll()).filter(
					(e: any) => e.type === "trade" && e.season === season,
				);
				const tradeEvents = seasonTrades.length;
				const draftNightTrades = seasonTrades.filter(
					(e: any) => e.aiTrade?.motivation === "draft-trade-up",
				).length;

				let deadMoney = 0;
				for (const rp of await idb.cache.releasedPlayers.getAll()) {
					deadMoney += rp.contract.amount;
				}
				worstDeadShare = Math.max(
					worstDeadShare,
					deadMoney / (salaryCap * NUM_TEAMS),
				);

				const meanTovr = ovrs.reduce((s, x) => s + x, 0) / ovrs.length;
				if (year >= SEASONS - 3) {
					lastTovrs.push(meanTovr);
				}

				const yearRow: (typeof history)[number] = [];
				for (let tid = 0; tid < NUM_TEAMS; tid++) {
					const posture = await getTradePosture(tid, ctx);
					const ts = await idb.cache.teamSeasons.indexGet(
						"teamSeasonsBySeasonTid",
						[season, tid],
					);
					yearRow.push({
						tid,
						tier: posture.tier,
						winp: ts ? ts.won / Math.max(1, ts.won + ts.lost) : 0.5,
					});
				}
				history.push(yearRow);

				rows.push(
					`y${String(year).padStart(2)} s${season} ` +
						`tovr ${meanTovr.toFixed(1)} ` +
						`pay ${((payroll / (salaryCap * NUM_TEAMS)) * 100).toFixed(0)}% ` +
						`dead ${(deadMoney / 1000).toFixed(0)}M ` +
						`fa ${fa.length} starsUnsigned ${unsignedStars} ` +
						`trades ${tradeEvents}(d${draftNightTrades}) pickAway ${tradedPicks} ` +
						`illegal ${illegal} positionless ${positionless} ` +
						`tiers ${["teardown", "seller", "fringe", "buyer", "allIn"]
							.map(
								(t) =>
									`${t[0]}${t === "teardown" ? "d" : ""}=${tierCounts[t] ?? 0}`,
							)
							.join(" ")}`,
				);

				await rollForward(season);
			}
		} finally {
			Math.random = realRandom;
		}

		// What only a decade can tell you. Diagnostics, not assertions - these go
		// to the log for a human to read after a deep run.
		const avg = (a: number[]) =>
			a.length ? a.reduce((s, x) => s + x, 0) / a.length : Number.NaN;
		{
			// Does entering a teardown ever pay off?
			const at3: number[] = [];
			const at5: number[] = [];
			let entered = 0;
			let stuck = 0;
			for (let y = 0; y < history.length; y++) {
				for (const row of history[y]!) {
					if (row.tier !== "teardown") {
						continue;
					}
					if (history[y - 1]?.[row.tid]?.tier === "teardown") {
						continue;
					}
					entered += 1;
					const later3 = history[y + 3]?.[row.tid];
					const later5 = history[y + 5]?.[row.tid];
					if (later3) {
						at3.push(later3.winp - row.winp);
					}
					if (later5) {
						at5.push(later5.winp - row.winp);
						if (later5.tier === "teardown" || later5.tier === "seller") {
							stuck += 1;
						}
					}
				}
			}
			rows.push(
				"",
				`REBUILDS entered=${entered} winp+3y=${avg(at3).toFixed(3)} winp+5y=${avg(at5).toFixed(3)} stillDown@5=${stuck}/${at5.length}`,
			);

			// How sticky are the standings year over year?
			const pairs: [number, number][] = [];
			for (let y = 1; y < history.length; y++) {
				for (const row of history[y]!) {
					pairs.push([history[y - 1]![row.tid]!.winp, row.winp]);
				}
			}
			const mx = avg(pairs.map((x) => x[0]));
			const my = avg(pairs.map((x) => x[1]));
			const cov = avg(pairs.map((x) => (x[0] - mx) * (x[1] - my)));
			const sx = Math.sqrt(avg(pairs.map((x) => (x[0] - mx) ** 2)));
			const sy = Math.sqrt(avg(pairs.map((x) => (x[1] - my) ** 2)));
			rows.push(
				`BALANCE year-over-year winp correlation=${(cov / (sx * sy)).toFixed(3)}`,
			);

			// Who owns the top and the bottom - a fleeced team pins the floor.
			const tops = new Map<number, number>();
			const bottoms = new Map<number, number>();
			for (const yearRow of history) {
				const best = yearRow.reduce((a, b) => (b.winp > a.winp ? b : a));
				const worst = yearRow.reduce((a, b) => (b.winp < a.winp ? b : a));
				tops.set(best.tid, (tops.get(best.tid) ?? 0) + 1);
				bottoms.set(worst.tid, (bottoms.get(worst.tid) ?? 0) + 1);
			}
			rows.push(
				`CONCENTRATION topSeeds distinct=${tops.size}/${NUM_TEAMS} max=${Math.max(...tops.values())}/${history.length}; ` +
					`bottoms distinct=${bottoms.size}/${NUM_TEAMS} max=${Math.max(...bottoms.values())}/${history.length}`,
			);
		}

		const log = rows.join("\n");
		if (nodeEnv.DECADES_LOG) {
			const fs = await import(("node" + ":fs") as any);
			fs.writeFileSync(nodeEnv.DECADES_LOG, log + "\n");
		}

		// League talent must not collapse. The compounding failures this exists
		// for ended with the average team in the twenties and still falling.
		const lastMean = lastTovrs.reduce((s, x) => s + x, 0) / lastTovrs.length;
		assert.isAbove(
			lastMean,
			35,
			`league talent collapsed - mean team ovr over the final seasons was ${lastMean.toFixed(1)}\n${log}`,
		);

		// The market must clear: a star sitting unsigned at the end of free
		// agency should be rare, not policy.
		assert.isAtMost(
			unsignedStarTotal,
			SEASONS,
			`too many unsigned stars (${unsignedStarTotal} team-seasons)\n${log}`,
		);

		// Every roster legal, every season.
		assert.strictEqual(
			illegalRosterSeasons,
			0,
			`seasons with an illegal roster size\n${log}`,
		);

		// Nobody fields a team with no big men or no guards for long. The bound
		// scales with team-seasons so a deep run is held to the same RATE as the
		// CI config, not the same count.
		assert.isAtMost(
			positionlessTotal,
			Math.max(3, Math.ceil(0.01 * NUM_TEAMS * SEASONS)),
			`too many rosters missing an entire position group\n${log}`,
		);

		// Dead money stays a nuisance, not a famine.
		assert.isBelow(
			worstDeadShare,
			0.15,
			`released contracts ate ${(worstDeadShare * 100).toFixed(1)}% of league cap\n${log}`,
		);

		// The posture system keeps expressing the whole range of plans.
		assert.strictEqual(
			tiersSeen.size,
			5,
			`only saw tiers: ${[...tiersSeen].join(", ")}\n${log}`,
		);
	}, 240000);
});
