import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { draft, player, team, trade } from "../index.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "../game/loadTeams.ts";
import { helpers } from "../../util/index.ts";
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

	// Draft order for THIS season, worst team first - by the real record when
	// games were simmed, by roster strength otherwise. Trading for a bad
	// team's pick is supposed to buy its lottery slot.
	const records: { tid: number; winp: number }[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
			season,
			tid,
		]);
		const gp = (ts?.won ?? 0) + (ts?.lost ?? 0);
		if (gp > 0) {
			records.push({ tid, winp: ts!.won / gp });
		}
	}
	const slotByTid = new Map<number, number>();
	if (records.length === NUM_TEAMS) {
		records.sort((a, b) => a.winp - b.winp);
		for (const [i, { tid }] of records.entries()) {
			slotByTid.set(tid, i + 1);
		}
	} else {
		const ctx = await getLeagueTradeContext();
		for (const [i, { tid }] of [...ctx.teamOvrsSorted].reverse().entries()) {
			slotByTid.set(tid, i + 1);
		}
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
		// The offseason heals most injuries, like newPhaseBeforeDraft.
		if (p.injury.gamesRemaining > 0) {
			p.injury =
				p.injury.gamesRemaining <= 82
					? { type: "Healthy", gamesRemaining: 0 }
					: { ...p.injury, gamesRemaining: p.injury.gamesRemaining - 82 };
		}
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

// A REAL SEASON, game by game. Only in deep runs (REAL_GAMES=1) - CI keeps
// the fast synthetic standings. GameSim is driven directly, the way the
// sportsbook does it: no schedule machinery, no locks, just inputs built from
// the cache and results applied back by hand - standings, and the injuries
// that make the in-season market mean something. The market itself (demands
// falling, signings, trades) runs every few days all season, which is where
// deadline behavior and injury stopgaps actually live.
const simRealSeason = async (rng: () => number) => {
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	const season = g.get("season");
	const NUM_GAME_DAYS = 82;
	let gid = 1;

	// A season row per team, 0-0, for the games to write into.
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const existing = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[season, tid],
		);
		if (!existing) {
			const row: any = team.genSeasonRow((await idb.cache.teams.get(tid))!);
			row.season = season;
			row.tid = tid;
			await idb.cache.teamSeasons.add(row);
		}
	}

	const loadSide = async (tid: number) => {
		const [t, teamSeason, players] = await Promise.all([
			idb.cache.teams.get(tid),
			idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [season, tid]),
			idb.getCopies.players({ tid }, "noCopyCache"),
		]);
		if (!t || !teamSeason) {
			return undefined;
		}
		return processTeam(t, teamSeason, players);
	};

	for (let day = 0; day < NUM_GAME_DAYS; day++) {
		// Everyone injured sits this one out and gets a game closer to healthy.
		for (const p of await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		])) {
			if (p.injury.gamesRemaining > 0) {
				p.injury.gamesRemaining -= 1;
				if (p.injury.gamesRemaining <= 0) {
					p.injury = { type: "Healthy", gamesRemaining: 0 };
				}
				await idb.cache.players.put(p);
			}
		}

		// Random pairings, one game per team per day.
		const tids = Array.from({ length: NUM_TEAMS }, (_, i) => i);
		for (let i = tids.length - 1; i > 0; i--) {
			const j = Math.floor(rng() * (i + 1));
			[tids[i], tids[j]] = [tids[j]!, tids[i]!];
		}
		for (let i = 0; i + 1 < tids.length; i += 2) {
			const home = await loadSide(tids[i]!);
			const away = await loadSide(tids[i + 1]!);
			if (!home || !away) {
				continue;
			}
			// GameSim mutates its inputs, so it gets copies - same as the
			// sportsbook's usage.
			const result: any = new GameSim({
				gid: gid++,
				day,
				teams: helpers.deepCopy([home, away]) as any,
				doPlayByPlay: false,
				homeCourtFactor: 1,
				neutralSite: false,
				allStarGame: false,
				baseInjuryRate: g.get("injuryRate"),
			} as any).run();

			const winner = result.team[0].stat.pts > result.team[1].stat.pts ? 0 : 1;
			for (const j of [0, 1] as const) {
				const ts = await idb.cache.teamSeasons.indexGet(
					"teamSeasonsBySeasonTid",
					[season, result.team[j].id],
				);
				if (ts) {
					if (j === winner) {
						ts.won += 1;
					} else {
						ts.lost += 1;
					}
					(ts as any).gp = ts.won + ts.lost;
					await idb.cache.teamSeasons.put(ts);
				}
				for (const sp of result.team[j].player) {
					if (sp.newInjury) {
						const p = await idb.cache.players.get(sp.id);
						if (p && p.injury.gamesRemaining === 0) {
							p.injury = player.injury(DEFAULT_LEVEL);
							await idb.cache.players.put(p);
						}
					}
				}
			}
		}

		// The market stays open all season.
		if (day % 3 === 0) {
			await decreaseDemands();
			await autoSign();
			if (nodeEnv.NO_TRADES !== "1") {
				await trade.betweenAiTeams();
			}
		}
	}
};

// THE PLAYOFFS, because a championship is the whole point. Two conferences by
// cid, top eight each by record, best-of-seven throughout, home court to the
// better seed. Returns the champion, so a decade of titles can be checked
// against what the contending tiers claim to be doing. Injuries keep
// happening - a star going down in May is part of why going all-in is a bet.
const simPlayoffs = async (): Promise<number | undefined> => {
	const season = g.get("season");

	const loadSide = async (tid: number) => {
		const [t, teamSeason, players] = await Promise.all([
			idb.cache.teams.get(tid),
			idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [season, tid]),
			idb.getCopies.players({ tid }, "noCopyCache"),
		]);
		if (!t || !teamSeason) {
			return undefined;
		}
		return processTeam(t, teamSeason, players);
	};

	const standings: { tid: number; cid: number; winp: number }[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
			season,
			tid,
		]);
		const t = await idb.cache.teams.get(tid);
		const gp = (ts?.won ?? 0) + (ts?.lost ?? 0);
		if (ts && t && gp > 0) {
			standings.push({ tid, cid: t.cid, winp: ts.won / gp });
		}
	}
	if (standings.length < 4) {
		return undefined;
	}

	let gid = 100_000 * (season % 1000);
	const series = async (
		higher: number,
		lower: number,
	): Promise<number | undefined> => {
		let winsHigh = 0;
		let winsLow = 0;
		for (let gameNum = 0; gameNum < 7; gameNum++) {
			// 2-2-1-1-1: games 0,1,4,6 at the higher seed.
			const homeTid = [0, 1, 4, 6].includes(gameNum) ? higher : lower;
			const awayTid = homeTid === higher ? lower : higher;
			const home = await loadSide(homeTid);
			const away = await loadSide(awayTid);
			if (!home || !away) {
				return undefined;
			}
			const result: any = new GameSim({
				gid: gid++,
				day: -1,
				teams: helpers.deepCopy([home, away]) as any,
				doPlayByPlay: false,
				homeCourtFactor: 1,
				neutralSite: false,
				allStarGame: false,
				baseInjuryRate: g.get("injuryRate"),
			} as any).run();
			const homeWon = result.team[0].stat.pts > result.team[1].stat.pts;
			const winnerTid = homeWon ? homeTid : awayTid;
			if (winnerTid === higher) {
				winsHigh += 1;
			} else {
				winsLow += 1;
			}
			for (const j of [0, 1] as const) {
				for (const sp of result.team[j].player) {
					if (sp.newInjury) {
						const p = await idb.cache.players.get(sp.id);
						if (p && p.injury.gamesRemaining === 0) {
							p.injury = player.injury(DEFAULT_LEVEL);
							await idb.cache.players.put(p);
						}
					}
				}
			}
			if (winsHigh === 4 || winsLow === 4) {
				break;
			}
		}
		return winsHigh >= 4 ? higher : lower;
	};

	const bracket = async (seeds: number[]): Promise<number | undefined> => {
		let round = seeds;
		while (round.length > 1) {
			const next: number[] = [];
			for (let i = 0; i < round.length / 2; i++) {
				const winner = await series(round[i]!, round[round.length - 1 - i]!);
				if (winner === undefined) {
					return undefined;
				}
				next.push(winner);
			}
			// next[i] vs next[len-1-i] is already the standard bracket (the 1v8
			// winner meets the 4v5 winner), same as the real one - no re-seed.
			round = next;
		}
		return round[0];
	};

	const champs: number[] = [];
	for (const cid of [0, 1]) {
		const conf = standings
			.filter((x) => x.cid === cid)
			.sort((a, b) => b.winp - a.winp)
			.slice(0, 8)
			.map((x) => x.tid);
		if (conf.length < 2) {
			continue;
		}
		const champ = await bracket(conf);
		if (champ !== undefined) {
			champs.push(champ);
		}
	}
	if (champs.length === 2) {
		return series(champs[0]!, champs[1]!);
	}
	return champs[0];
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
		const champions: {
			year: number;
			season: number;
			tid: number;
			tier: string;
		}[] = [];

		// The canaries.
		let illegalRosterSeasons = 0;
		let unsignedStarTotal = 0;
		let positionlessTotal = 0;
		let worstDeadShare = 0;
		const taxByTier = new Map<string, number>();
		const lastTovrs: number[] = [];
		const tiersSeen = new Set<string>();

		try {
			await build();
			g.setWithoutSavingToDB("smartAiFrontOffice", nodeEnv.SMART_AI !== "0");
			const salaryCap = g.get("salaryCap");

			for (let year = 0; year < SEASONS; year++) {
				const season = g.get("season");
				let champion: { tid: number; tier: string } | undefined;
				if (nodeEnv.REAL_GAMES === "1") {
					// The season is actually played: real standings, real injuries,
					// the market open throughout. Records then also set draft slots.
					await simRealSeason(rng);
					const champTid = await simPlayoffs();
					if (champTid !== undefined) {
						// The tier the champion carried INTO the playoffs.
						const champCtx = await getLeagueTradeContext();
						champion = {
							tid: champTid,
							tier: (await getTradePosture(champTid, champCtx)).tier,
						};
					}
				} else {
					await setRecords(rng);
				}
				await ensureDraftClass(season);

				// THE DEADLINE WINDOW. Mid-season is where the other half of the
				// trade AI lives - contenders renting expiring veterans, sellers
				// cashing in players who will walk - and none of it is reachable
				// from the offseason phases. A week of in-season ticks opens it.
				// (There is no schedule here, so the deadline frenzy ramp stays at
				// its base rate - what is being exercised is the motivation logic,
				// not the frenzy.)
				g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
				if (nodeEnv.NO_TRADES !== "1" && nodeEnv.REAL_GAMES !== "1") {
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

				// A star on a team going nowhere, and a team paying the tax with
				// nothing to show for it - the two states a real front office is
				// judged for tolerating.
				let strandedStars = 0;
				let taxNoContender = 0;
				const luxuryPayroll = g.get("luxuryPayroll");
				for (let tid = 0; tid < NUM_TEAMS; tid++) {
					const posture = await getTradePosture(tid, ctx);
					const roster = await idb.cache.players.indexGetAll(
						"playersByTid",
						tid,
					);
					const selling =
						posture.tier === "teardown" || posture.tier === "seller";
					if (selling) {
						for (const p of roster) {
							// A veteran star: young ones are cornerstones a rebuild
							// SHOULD keep, which is not the same failure.
							if (
								p.ratings.at(-1)!.ovr >= ctx.starOvr &&
								season - p.born.year >= 29
							) {
								strandedStars += 1;
							}
						}
					}
					const payroll = await team.getPayroll(tid);
					if (payroll > luxuryPayroll) {
						taxByTier.set(posture.tier, (taxByTier.get(posture.tier) ?? 0) + 1);
						if (posture.tier !== "allIn" && !posture.elite) {
							taxNoContender += 1;
						}
					}
				}

				let injuredNow = 0;
				for (const p of await idb.cache.players.indexGetAll("playersByTid", [
					0,
					Infinity,
				])) {
					if (p.injury.gamesRemaining > 0) {
						injuredNow += 1;
					}
				}
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

				if (champion) {
					champions.push({ year, season, ...champion });
				}
				rows.push(
					`y${String(year).padStart(2)} s${season} ` +
						(champion ? `champ T${champion.tid}(${champion.tier}) ` : "") +
						`tovr ${meanTovr.toFixed(1)} ` +
						`pay ${((payroll / (salaryCap * NUM_TEAMS)) * 100).toFixed(0)}% ` +
						`dead ${(deadMoney / 1000).toFixed(0)}M ` +
						`fa ${fa.length} starsUnsigned ${unsignedStars} ` +
						`trades ${tradeEvents}(d${draftNightTrades}) pickAway ${tradedPicks} inj ${injuredNow} ` +
						`stranded ${strandedStars} taxNoContend ${taxNoContender} ` +
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

			// Does a franchise hold a direction, or flip-flop? Adjacent-tier
			// moves are ordinary drift; a two-step jump in one offseason is
			// abrupt; three or more is whiplash no real front office shows.
			const TIER_ORDER: Record<string, number> = {
				teardown: 0,
				seller: 1,
				fringe: 2,
				buyer: 3,
				allIn: 4,
			};
			let steps1 = 0;
			let steps2 = 0;
			let steps3 = 0;
			let holds = 0;
			for (let y = 1; y < history.length; y++) {
				for (const row of history[y]!) {
					const d = Math.abs(
						TIER_ORDER[row.tier]! - TIER_ORDER[history[y - 1]![row.tid]!.tier]!,
					);
					if (d === 0) {
						holds += 1;
					} else if (d === 1) {
						steps1 += 1;
					} else if (d === 2) {
						steps2 += 1;
					} else {
						steps3 += 1;
					}
				}
			}
			rows.push(
				`TAXPAYERS byTier ${[...taxByTier]
					.sort((a, b) => b[1] - a[1])
					.map(([k, v]) => `${k}=${v}`)
					.join(" ")}`,
			);
			rows.push(
				`WHIPLASH hold=${holds} step1=${steps1} step2=${steps2} step3+=${steps3}`,
			);

			{
				// What the market was FOR, across the whole run - the census of
				// why AI trades happened.
				const motives = new Map<string, number>();
				for (const e of (await idb.cache.events.getAll()) as any[]) {
					if (e.type === "trade" && e.aiTrade?.motivation) {
						motives.set(
							e.aiTrade.motivation,
							(motives.get(e.aiTrade.motivation) ?? 0) + 1,
						);
					}
				}
				if (motives.size > 0) {
					rows.push(
						`MOTIVES ${[...motives]
							.sort((a, b) => b[1] - a[1])
							.map(([k, v]) => `${k}=${v}`)
							.join(" ")}`,
					);
				}
			}

			if (champions.length > 0) {
				const byTid = new Map<number, number>();
				const byTier = new Map<string, number>();
				// The tier at trophy time is circular (a 60-win team reads as a
				// buyer because of its record); the tier the champion OPERATED
				// under the season before is the honest ledger of whether the
				// contending plans actually lead to titles.
				const byPriorTier = new Map<string, number>();
				for (const c of champions) {
					byTid.set(c.tid, (byTid.get(c.tid) ?? 0) + 1);
					byTier.set(c.tier, (byTier.get(c.tier) ?? 0) + 1);
					const prior = history[c.year - 1]?.[c.tid]?.tier;
					if (prior) {
						byPriorTier.set(prior, (byPriorTier.get(prior) ?? 0) + 1);
					}
				}
				const fmt = (m: Map<string, number>) =>
					[...m]
						.sort((a, b) => b[1] - a[1])
						.map(([k, v]) => `${k}=${v}`)
						.join(" ");
				rows.push(
					`TITLES distinct=${byTid.size}/${champions.length} max=${Math.max(...byTid.values())} ` +
						`byTier ${fmt(byTier)}; priorYearTier ${fmt(byPriorTier)}`,
				);
			}
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

		// The posture system keeps expressing the whole range of plans. Tiny
		// smoke runs legitimately may not produce an all-in team, so this only
		// applies at the default scale and up.
		if (NUM_TEAMS * SEASONS >= 150) {
			assert.strictEqual(
				tiersSeen.size,
				5,
				`only saw tiers: ${[...tiersSeen].join(", ")}\n${log}`,
			);
		}
	}, 240000);
});
