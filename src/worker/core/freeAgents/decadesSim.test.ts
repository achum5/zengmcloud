import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import {
	COLA_NUM_LOTTERY_PICKS,
	PHASE,
	PLAYER,
} from "../../../common/constants.ts";
import { draft, player, team, trade } from "../index.ts";
import {
	classEdge,
	setAiColaOptOuts,
	updateColaAfterPlayoffs,
} from "../draft/cola.ts";
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
import { captureFrontOfficeLog } from "../../util/frontOfficeLog.ts";
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

// DRAFT_TYPE=cola runs the real lottery instead of handing the worst team the
// first pick. Opt-in, because every other measurement in this file was taken
// with slots assigned straight off the standings and should stay comparable.
const COLA = nodeEnv.DRAFT_TYPE === "cola";
const colaRows: {
	season: number;
	max: number;
	total: number;
	nonZero: number;
	firstToWorst: boolean;
	optOuts: number;
	strength: number | undefined;
	edge: number | undefined;
}[] = [];

// How this year's class compares to next year's, by the two measures the
// opt-out rule turns on: overall strength, and the EDGE a lottery pick buys
// over the pick you get anyway. Reported because shouldOptOutOfCola is a
// judgement about those numbers and would quietly stop meaning anything if
// class generation ever changed shape.
const classRatios = async (season: number) => {
	const [a, b] = await Promise.all([
		classEdge(season, COLA_NUM_LOTTERY_PICKS),
		classEdge(season + 1, COLA_NUM_LOTTERY_PICKS),
	]);
	if (a === undefined || b === undefined) {
		return { strength: undefined, edge: undefined };
	}
	const edgeA = a.lottery - a.fallback;
	const edgeB = b.lottery - b.fallback;
	return {
		strength: b.lottery > 0 ? a.lottery / b.lottery : undefined,
		edge: edgeB > 0 ? edgeA / edgeB : undefined,
	};
};

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
	// actually produces for this many teams. Next year's too: the real game has
	// classes three seasons out by this point (newPhaseResignPlayers), and a
	// front office deciding whether THIS draft is worth entering has to be able
	// to see the one behind it.
	await draft.genPlayers(season, DEFAULT_LEVEL);
	await draft.genPlayers(season + 1, DEFAULT_LEVEL);

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

	// Under COLA the standings do not decide the top of the draft - an
	// accumulated stockpile of chances does, and a pick that has changed hands
	// is not even in the draw. So the real lottery has to run, or none of the
	// strategy this mode exists to exercise is reachable.
	if (COLA) {
		const beforePhase = g.get("phase");
		g.setWithoutSavingToDB("phase", PHASE.DRAFT_LOTTERY);
		// Opt-outs are decided before the draw and after both classes exist,
		// which is the order newPhaseBeforeDraft/newPhaseDraft run them in.
		await setAiColaOptOuts();
		const optOuts = (await idb.cache.teams.getAll()).filter(
			(t) => t.draftLottery?.type === "cola" && t.draftLottery.optOut,
		).length;
		await draft.genOrder(false, {} as any);
		g.setWithoutSavingToDB("phase", beforePhase);

		const chances: number[] = [];
		for (const t of await idb.cache.teams.getAll()) {
			chances.push(
				t.draftLottery?.type === "cola" ? t.draftLottery.chances : 0,
			);
		}
		let firstPickTid = -1;
		for (const dp of await idb.cache.draftPicks.getAll()) {
			if (dp.season === season && dp.round === 1 && dp.pick === 1) {
				firstPickTid = dp.originalTid;
			}
		}
		const worstTid = [...slotByTid].find(([, slot]) => slot === 1)?.[0];
		colaRows.push({
			season,
			max: Math.max(0, ...chances),
			total: chances.reduce((a, x) => a + x, 0),
			nonZero: chances.filter((x) => x > 0).length,
			firstToWorst: firstPickTid === worstTid,
			optOuts,
			...(await classRatios(season)),
		});
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
// Healthy players per team, sampled weekly through every simulated season.
const healthyBodies: number[] = [];

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

		// Injured players count toward a roster but cannot play, so a full-looking
		// team can still be short a rotation - and nothing in the roster rules
		// notices. Sampled weekly, which is plenty to see a team stuck
		// short-handed and cheap enough not to slow a deep run. See shortHanded
		// in frontOffice.ts.
		if (day % 7 === 0) {
			for (let tid = 0; tid < NUM_TEAMS; tid++) {
				const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
				healthyBodies.push(
					roster.filter((p) => p.injury.gamesRemaining === 0).length,
				);
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

	// How many series each team won. -1 for anyone who never made it, which is
	// what BBGM's playoffRoundsWon means and what COLA reads to decide who
	// banks a season's chances and whose stockpile gets cut.
	const roundsWon = new Map<number, number>();

	const bracket = async (seeds: number[]): Promise<number | undefined> => {
		for (const tid of seeds) {
			roundsWon.set(tid, 0);
		}
		let round = seeds;
		while (round.length > 1) {
			const next: number[] = [];
			for (let i = 0; i < round.length / 2; i++) {
				const winner = await series(round[i]!, round[round.length - 1 - i]!);
				if (winner === undefined) {
					return undefined;
				}
				roundsWon.set(winner, (roundsWon.get(winner) ?? 0) + 1);
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
	let champion: number | undefined;
	if (champs.length === 2) {
		champion = await series(champs[0]!, champs[1]!);
		if (champion !== undefined) {
			roundsWon.set(champion, (roundsWon.get(champion) ?? 0) + 1);
		}
	} else {
		champion = champs[0];
	}

	// Write it back, because the draft lottery is downstream of it.
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
			season,
			tid,
		]);
		if (ts) {
			(ts as any).playoffRoundsWon = roundsWon.get(tid) ?? -1;
			await idb.cache.teamSeasons.put(ts);
		}
	}

	return champion;
};

describe("a league runs for a decade without falling apart", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("the full offseason cycle", async () => {
		const tovrSpread: number[] = [];
		const tovrBest5: number[] = [];
		const tovrWorst5: number[] = [];
		const rosteredOvrs: number[] = [];
		const bargainsLeftOver: number[] = [];
		const bestBargainLeft: number[] = [];
		const rng = makeRng(Number(nodeEnv.SEED ?? 31337));
		// NOT vi.spyOn: a spy records every call it sees, and a decade of
		// simulation calls Math.random tens of millions of times - the mock's
		// call log alone ran the process out of heap.
		const realRandom = Math.random;
		Math.random = rng;

		const rows: string[] = [];
		// FO_LOG=1 records every reasoned front-office decision, so a deep run
		// can be asked WHY - which teams passed, which gave up on a player they
		// wanted, which dumped salary and for whom.
		const foLog = nodeEnv.FO_LOG === "1" ? captureFrontOfficeLog() : undefined;
		// Per-team history, for the questions only a decade can answer: does a
		// rebuild ever pay off, does anyone get stuck at the bottom forever.
		const history: { tid: number; tier: string; winp: number }[][] = [];
		// Every pick made, so what the AI ASSUMED it was buying can be checked
		// against what the player was actually worth once he had grown into it.
		const picksTaken: {
			season: number;
			slot: number;
			pid: number;
			assumed: number;
			realized?: number;
		}[] = [];
		// Every multi-year commitment an AI team made, so what it thought it was
		// buying can be checked against what the player was actually worth while
		// it was still paying for him. The money-side twin of picksTaken: a front
		// office that systematically buys decline is making plans that do not come
		// true, and nothing in a box score says so.
		const dealsSigned: {
			season: number;
			pid: number;
			// Share of the salary cap, per year.
			share: number;
			years: number;
			valueAtSigning: number;
			ageAtSigning: number;
			realized?: number;
			atExpiry?: number;
		}[] = [];
		const seenDeals = new Set<string>();
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
		// The single most committed payroll any team ever carried, as a share of
		// the cap. Removing the tax-line ceiling on re-signing was checked on
		// league AVERAGES; this is the tail it could not see.
		let worstPayrollShare = 0;
		let worstPayrollDetail = "";
		let worstMaxDealOvr = Infinity;
		let worstMaxDeal = "";
		let maxDeals = 0;
		let maxDealOvrTotal = 0;
		let pickAssumedTotal = 0;
		let pickRealizedTotal = 0;
		let settledPicks = 0;
		let rosterOvrTotal = 0;
		let rosterOvrCount = 0;
		const lastTovrs: number[] = [];
		const tiersSeen = new Set<string>();

		try {
			await build();
			g.setWithoutSavingToDB("smartAiFrontOffice", nodeEnv.SMART_AI !== "0");
			if (COLA) {
				g.setWithoutSavingToDB("draftType", "cola");
				for (const t of await idb.cache.teams.getAll()) {
					t.draftLottery = { type: "cola", chances: 0 };
					await idb.cache.teams.put(t);
				}
			}
			const salaryCap = g.get("salaryCap");

			for (let year = 0; year < SEASONS; year++) {
				const season = g.get("season");
				let champion: { tid: number; tier: string } | undefined;
				// COLA implies REAL_GAMES: stockpiles move on playoff results, and
				// synthesized standings have none. Without a played postseason
				// every team banks nothing and the lottery is a formality.
				if (nodeEnv.REAL_GAMES === "1" || COLA) {
					// The season is actually played: real standings, real injuries,
					// the market open throughout. Records then also set draft slots.
					await simRealSeason(rng);
					const champTid = await simPlayoffs();
					if (COLA) {
						// Banks a season for everyone who missed the deep rounds and
						// cuts the stockpiles of everyone who did not, exactly as
						// newPhaseBeforeDraft does. The opt-out decision that follows
						// it there needs the draft classes, so it runs in
						// ensureDraftClass instead.
						await updateColaAfterPlayoffs();
					}
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
				// WHAT A PICK IS ASSUMED TO BE WORTH, straight off the curve the
				// trade AI prices picks from (trade/getPickValues.ts): the value
				// of the Nth best prospect on the board, right now.
				const boardBefore = (
					await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
				)
					.filter((p) => p.draft.year === season)
					.map((p) => p.value)
					.sort((a, b) => b - a);
				const draftedPids = await draft.runPicks(
					{ type: "untilEnd" },
					{} as any,
				);
				for (const [i, pid] of (draftedPids ?? []).entries()) {
					picksTaken.push({
						season,
						slot: i + 1,
						pid,
						assumed: boardBefore[i] ?? 0,
					});
				}

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

				// Every deal on the books we have not seen before. Recorded after
				// free agency so it catches re-signings and new signings alike -
				// both are the same bet, that this player is worth this money for
				// these years. Year zero is skipped because the fixture arrives
				// with contracts nobody decided on, and rookies because a rookie
				// deal is the draft's bet: PICKS already measures that one.
				for (let tid = 0; year > 0 && tid < NUM_TEAMS; tid++) {
					for (const p of await idb.cache.players.indexGetAll(
						"playersByTid",
						tid,
					)) {
						const years = p.contract.exp - season + 1;
						if (
							years < 2 ||
							p.contract.amount <= g.get("minContract") * 2 ||
							p.draft.year >= season - 1
						) {
							continue;
						}
						const key = `${p.pid}|${p.contract.amount}|${p.contract.exp}`;
						if (seenDeals.has(key)) {
							continue;
						}
						seenDeals.add(key);
						dealsSigned.push({
							season,
							pid: p.pid,
							share: p.contract.amount / g.get("salaryCap"),
							years,
							valueAtSigning: p.value,
							ageAtSigning: season - p.born.year,
						});
					}
				}

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
					// Every rostered player, so the top of the league can be
					// compared without allocation getting a say. If this moves,
					// talent was actually gained or lost rather than shuffled.
					for (const rp of roster) {
						rosteredOvrs.push(rp.ratings.at(-1)!.ovr);
					}
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
					// The most overpaid contract in the league: the weakest player
					// anyone is paying near-maximum money. This is what a bad deal
					// looks like on a roster page, and league aggregates hide it.
					for (const p of roster) {
						rosterOvrTotal += p.ratings.at(-1)!.ovr;
						rosterOvrCount += 1;
						if (p.contract.amount >= 0.8 * g.get("maxContract")) {
							const ovr = p.ratings.at(-1)!.ovr;
							if (ovr < worstMaxDealOvr) {
								worstMaxDealOvr = ovr;
								worstMaxDeal = `s${season} ${ovr}ovr at ${(
									p.contract.amount / 1000
								).toFixed(0)}M`;
							}
							maxDeals += 1;
							maxDealOvrTotal += ovr;
						}
					}

					const payroll = await team.getPayroll(tid);
					if (payroll / salaryCap > worstPayrollShare) {
						worstPayrollShare = payroll / salaryCap;
						// What that payroll is MADE of, so a runaway of live
						// contracts can be told apart from accumulated dead money.
						const live = roster.reduce((a, p) => a + p.contract.amount, 0);
						const released = (
							await idb.cache.releasedPlayers.indexGetAll(
								"releasedPlayersByTid",
								tid,
							)
						).reduce((a, rp) => a + rp.contract.amount, 0);
						const top = [...roster]
							.sort((a, b) => b.contract.amount - a.contract.amount)
							.slice(0, 4)
							.map(
								(p) =>
									`${p.ratings.at(-1)!.ovr}ovr/${(p.contract.amount / 1000).toFixed(0)}M`,
							)
							.join(" ");
						worstPayrollDetail = `s${season} T${tid} ${(payroll / 1000).toFixed(0)}M = live ${(
							live / 1000
						).toFixed(0)}M + dead ${(released / 1000).toFixed(0)}M; n=${
							roster.length
						}; top ${top}`;
					}
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

				// The cheapest quality in the game: healthy, useful, and asking the
				// league minimum, yet still unsigned. This is how the AI's refusal
				// to sign minimum players was caught - every summer a 51-to-57 ovr
				// player sat in the pool for nothing while teams carried an empty
				// roster spot. See findBargain in frontOffice.ts.
				const bargainsLeft = fa.filter(
					(p) =>
						p.injury.gamesRemaining === 0 &&
						p.ratings.at(-1)!.ovr >= 50 &&
						p.contract.amount <= g.get("minContract"),
				);
				bargainsLeftOver.push(bargainsLeft.length);
				bestBargainLeft.push(
					Math.max(0, ...bargainsLeft.map((p) => p.ratings.at(-1)!.ovr)),
				);

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

				// WHY THE MEAN ALONE LIES, and it lied to this harness for a while.
				//
				// team.ovr is concave in talent - the fifth star on a roster adds
				// far less than the first - so for a FIXED pool of players the sum
				// of team ovrs is MAXIMISED by spreading them evenly and depressed
				// by any concentration. A league whose contenders assemble stars
				// and whose rebuilds strip down therefore posts a LOWER mean than
				// one where everybody is mediocre, while being exactly what the
				// front office feature is for.
				//
				// Measured against stock BBGM over eight real seasons: mean team
				// ovr 45.2 against 49.6, which reads as a disaster until you look
				// at the spread. The best five teams were BETTER (71.6 v 70.2) and
				// the worst five far worse (6.9 v 27.7). Nothing was lost; it moved.
				//
				// So the spread is reported next to the mean, and the talent rows
				// below measure the pool itself, which no allocation can move.
				{
					const sd = Math.sqrt(
						ovrs.reduce((a, x) => a + (x - meanTovr) ** 2, 0) / ovrs.length,
					);
					const sorted = [...ovrs].sort((a, b) => b - a);
					const mean5 = (xs: number[]) =>
						xs.reduce((a, x) => a + x, 0) / Math.max(1, xs.length);
					tovrSpread.push(sd);
					tovrBest5.push(mean5(sorted.slice(0, 5)));
					tovrWorst5.push(mean5(sorted.slice(-5)));
				}
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

				// Settle up any deal signed two seasons ago, and again in its LAST
				// season - the year the team is still paying full price for
				// whatever is left of him.
				for (const rec of dealsSigned) {
					const elapsed = season - rec.season;
					if (rec.realized === undefined && elapsed === 2 && rec.years > 2) {
						const p = await idb.cache.players.get(rec.pid);
						rec.realized = p && p.tid !== PLAYER.RETIRED ? p.value : 0;
					}
					if (rec.atExpiry === undefined && elapsed === rec.years - 1) {
						const p = await idb.cache.players.get(rec.pid);
						rec.atExpiry = p && p.tid !== PLAYER.RETIRED ? p.value : 0;
					}
				}

				// Settle up any pick made three seasons ago: what is he worth now?
				for (const rec of picksTaken) {
					if (rec.realized === undefined && season - rec.season === 3) {
						const p = await idb.cache.players.get(rec.pid);
						// Out of the league counts as zero, which is the honest
						// answer for a pick that produced nothing.
						rec.realized = p && p.tid !== PLAYER.RETIRED ? p.value : 0;
					}
				}

				await rollForward(season);
			}
		} finally {
			Math.random = realRandom;
		}

		if (foLog) {
			const counts = new Map<string, number>();
			for (const e of foLog.stop()) {
				counts.set(e.event, (counts.get(e.event) ?? 0) + 1);
			}
			rows.push(
				`FRONT OFFICE ${[...counts]
					.sort((a, b) => b[1] - a[1])
					.map(([k, v]) => `${k}=${v}`)
					.join(" ")}`,
			);
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
				`MAX DEALS n=${maxDeals} meanOvr=${(
					maxDealOvrTotal / Math.max(1, maxDeals)
				).toFixed(1)} worst ${worstMaxDeal}`,
				`WORST PAYROLL ${(worstPayrollShare * 100).toFixed(0)}% of cap - ${worstPayrollDetail}`,
				`TAXPAYERS byTier ${[...taxByTier]
					.sort((a, b) => b[1] - a[1])
					.map(([k, v]) => `${k}=${v}`)
					.join(" ")}`,
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

			{
				const settled = picksTaken.filter((r) => r.realized !== undefined);
				if (settled.length > 0) {
					pickAssumedTotal = settled.reduce((a, r) => a + r.assumed, 0);
					pickRealizedTotal = settled.reduce((a, r) => a + r.realized!, 0);
					settledPicks = settled.length;
				}
				if (settled.length > 0) {
					const buckets: [string, number, number][] = [
						["1-3", 1, 3],
						["4-10", 4, 10],
						["11-20", 11, 20],
						["21-30", 21, 30],
						["31+", 31, Infinity],
					];
					const parts: string[] = [];
					for (const [label, lo, hi] of buckets) {
						const rows = settled.filter((r) => r.slot >= lo && r.slot <= hi);
						if (rows.length === 0) {
							continue;
						}
						const mean = (xs: number[]) =>
							xs.reduce((a, x) => a + x, 0) / xs.length;
						const assumed = mean(rows.map((r) => r.assumed));
						const realized = mean(rows.map((r) => r.realized!));
						parts.push(
							`${label}:${assumed.toFixed(0)}->${realized.toFixed(0)}(${(
								realized / Math.max(1, assumed)
							).toFixed(2)}x,n=${rows.length})`,
						);
					}
					rows.push(`PICKS assumed->worth3y ${parts.join(" ")}`);
				}
			}

			{
				// The same question asked of money instead of picks: did the players
				// AI teams committed years to hold their value while the team was
				// still paying? Bucketed by share of the cap, because what makes a
				// deal risky is what it costs. Diagnostic, not a canary - measured
				// at 0.94-0.97x two years in and 0.88-0.93x at expiry, which is
				// ordinary aging rather than a front office buying decline.
				const mean = (xs: number[]) =>
					xs.reduce((a, x) => a + x, 0) / Math.max(1, xs.length);
				const buckets: [string, number, number][] = [
					["max", 0.25, Infinity],
					["big", 0.15, 0.25],
					["mid", 0.07, 0.15],
					["small", 0, 0.07],
				];
				const line = (
					label: string,
					rs: typeof dealsSigned,
					got: (r: (typeof dealsSigned)[number]) => number,
				) => {
					const parts: string[] = [];
					for (const [bl, lo, hi] of buckets) {
						const inB = rs.filter((r) => r.share >= lo && r.share < hi);
						if (inB.length === 0) {
							continue;
						}
						const signed = mean(inB.map((r) => r.valueAtSigning));
						const later = mean(inB.map(got));
						parts.push(
							`${bl}:${signed.toFixed(0)}->${later.toFixed(0)}(${(
								later / Math.max(1, signed)
							).toFixed(2)}x,n=${inB.length},age${mean(
								inB.map((r) => r.ageAtSigning),
							).toFixed(0)},yrs${mean(inB.map((r) => r.years)).toFixed(1)})`,
						);
					}
					if (parts.length > 0) {
						rows.push(`DEALS ${label} ${parts.join(" ")}`);
					}
				};
				line(
					"signed->worth2y",
					dealsSigned.filter((r) => r.realized !== undefined),
					(r) => r.realized!,
				);
				line(
					"signed->worthAtExpiry",
					dealsSigned.filter((r) => r.atExpiry !== undefined),
					(r) => r.atExpiry!,
				);
			}

			if (healthyBodies.length > 0) {
				const bands = new Map<string, number>();
				for (const h of healthyBodies) {
					bands.set(
						h <= 8 ? "<=8" : h <= 10 ? "9-10" : h <= 12 ? "11-12" : "13+",
						(bands.get(
							h <= 8 ? "<=8" : h <= 10 ? "9-10" : h <= 12 ? "11-12" : "13+",
						) ?? 0) + 1,
					);
				}
				rows.push(
					`HEALTHY BODIES teamDays=${healthyBodies.length} ${[
						"<=8",
						"9-10",
						"11-12",
						"13+",
					]
						.map((k) => `${k}=${bands.get(k) ?? 0}`)
						.join(" ")}`,
				);
			}
			{
				const m = (xs: number[]) =>
					xs.reduce((a, x) => a + x, 0) / Math.max(1, xs.length);
				const pool = [...rosteredOvrs].sort((a, b) => b - a);
				rows.push(
					"",
					`SPREAD tovrSD=${m(tovrSpread).toFixed(1)} best5=${m(tovrBest5).toFixed(1)} ` +
						`worst5=${m(tovrWorst5).toFixed(1)} gap=${(m(tovrBest5) - m(tovrWorst5)).toFixed(1)}`,
					`TALENT poolTop100=${m(pool.slice(0, 100)).toFixed(1)} ` +
						`poolTop500=${m(pool.slice(0, 500)).toFixed(1)} ` +
						`allRostered=${m(rosteredOvrs).toFixed(1)} n=${rosteredOvrs.length}`,
				);
			}
			if (colaRows.length > 0) {
				rows.push(
					"",
					`COLA ${colaRows
						.map(
							(c) =>
								`s${c.season}:max${c.max}/tot${c.total}/nz${c.nonZero}/str${c.strength?.toFixed(2) ?? "?"}/edge${c.edge?.toFixed(2) ?? "?"}${c.optOuts > 0 ? `/out${c.optOuts}` : ""}${c.firstToWorst ? "/worstGot1" : ""}`,
						)
						.join(" ")}`,
				);
			}
			// Diagnostic, not a canary: measured across four seeds the arms
			// overlap (53.9-54.5 with vanilla's minimum-contract refusal in
			// place, 51.4-53.9 with findBargain), so a threshold here would be
			// reading noise. The logic itself is pinned in frontOffice.test.ts;
			// this row is for reading a deep run.
			rows.push(
				`BARGAINS LEFT mean=${avg(bargainsLeftOver).toFixed(1)}/season ` +
					`bestLeft mean=${avg(bestBargainLeft).toFixed(1)} ` +
					`max=${Math.max(0, ...bestBargainLeft)}`,
			);

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

		// A DRAFT PICK IS PRICED OFF THE PROSPECT BOARD (trade/getPickValues.ts):
		// the value of the Nth best prospect available, today. Everything a
		// rebuild does is bought with picks, so if that estimate drifts far from
		// what a pick actually returns, every rebuild in the league is trading
		// at the wrong price. Measured at 0.99-1.02x across slots; this fires
		// only on gross miscalibration.
		if (settledPicks > 20) {
			const ratio = pickRealizedTotal / Math.max(1, pickAssumedTotal);
			assert.isAbove(
				ratio,
				0.5,
				`draft picks return far less than they are priced at (${ratio.toFixed(2)}x)\n${log}`,
			);
			assert.isBelow(
				ratio,
				2,
				`draft picks return far more than they are priced at (${ratio.toFixed(2)}x)\n${log}`,
			);
		}

		// Big money goes to good players. Aging stars on old deals drag this
		// down and should - the check is only that near-maximum contracts are
		// held by better-than-average players, not that every one is a bargain.
		if (maxDeals > 10 && rosterOvrCount > 0) {
			const maxDealMean = maxDealOvrTotal / maxDeals;
			const leagueMean = rosterOvrTotal / rosterOvrCount;
			assert.isAbove(
				maxDealMean,
				leagueMean,
				`near-maximum contracts are going to below-average players (${maxDealMean.toFixed(1)} vs ${leagueMean.toFixed(1)})\n${log}`,
			);
		}

		// An AI team is not burdened by a budget, so payroll is allowed to run
		// well past the cap - talent concentrating on the teams that want it is
		// the intended effect. What is NOT intended is a team locking up several
		// times the league's money, which would mean the retention premium had
		// come off its hinges.
		assert.isBelow(
			worstPayrollShare,
			4,
			`a team carried ${(worstPayrollShare * 100).toFixed(0)}% of the cap\n${log}`,
		);

		// The bottom is ALLOWED to be bad - a team that tears down should look
		// torn down, and measured against stock BBGM over eight real seasons the
		// worst five teams sit near 10 where vanilla's sit near 28. That is the
		// feature, not a fault: the same runs put the BEST five teams ahead of
		// vanilla's. What the bottom may not do is fall out of the league. Before
		// the roster floor was enforced (see `stripped` in autoSign) this reached
		// -56 with ten-man rosters, so the bar sits far below anything measured
		// and far above that, and does not argue about how deep a rebuild goes.
		if (SEASONS >= 8 && tovrWorst5.length > 0) {
			const worst5 = tovrWorst5.reduce((a, x) => a + x, 0) / tovrWorst5.length;
			assert.isAbove(
				worst5,
				-20,
				`the bottom five teams averaged ${worst5.toFixed(1)} team ovr\n${log}`,
			);
		}

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
