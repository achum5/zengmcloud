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
import { bySport } from "../../../common/sportFunctions.ts";
import autoSign from "./autoSign.ts";
import clearSpaceForSignings from "./clearSpace.ts";
import decreaseDemands from "./decreaseDemands.ts";
import newPhaseResignPlayers from "../phase/newPhaseResignPlayers.ts";
import createRandomPlayers from "../league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import finances from "../finances/index.ts";
import { smartBudgetLevels } from "../finances/smartBudget.ts";
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
// REAL_GAMES=1 plays every game instead of synthesizing standings; NO_TRADES=1
// closes the trade market; DRAFT_TYPE=cola runs the real lottery; FO_LOG=1
// records every reasoned decision; CAP_TYPE=hard|none runs the decade under a
// cap rule other than the default soft one.
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

// TWO GROUPS NO ROSTER MAY BE WITHOUT, per sport. The check behind them - a
// team fielding nobody at all somewhere it has to field somebody - is the same
// everywhere; the positions that make it true are not.
//
// RUNNING THIS FILE UNDER ANOTHER SPORT is a one-line wrapper - a
// decadesSim.football.test.ts containing `import "./decadesSim.test.ts"` is
// picked up by the football project, which matches on filename. That is worth
// knowing because nothing here is basketball-specific except what is written
// down, and this pair was the exception: it was hardcoded to PG/SG/G and
// C/PF/FC, so under any other sport every roster read as missing both groups
// and the canary fired on every team-season before a single decision had been
// examined.
//
// What it says once it can run. Six seeds of eight football seasons, smart
// front office against stock, showed the same trade the basketball comments
// describe: the spread widens (every seed), the bottom five fall about five
// points (every seed), the top five rise about two, and the talent actually
// employed across the league is flat - so nobody is worse off for being in a
// league that concentrates. Mean team ovr is down about a point and moves in
// both directions, which is the concave-mean artefact the SPREAD row exists to
// expose rather than a result.
//
// One thing did not match. Dead money runs about sixteen million a season
// higher than stock in ALL SIX seeds, where the same comparison in basketball
// has it lower - a 53-man roster churns far harder than a 15-man one, so
// whatever rosterCuts saves per cut is being swamped by the number of them.
// That is a live lead, not a measured defect: nothing here has looked at why.
//
// Baseball trips the canary below at one or two rosters a season, and it is
// NOT the front office - stock BBGM produces the same rate or worse on the
// same seeds. Left as it is rather than papered over.
const POSITION_GROUPS: [Set<string>, Set<string>] = bySport({
	basketball: [new Set(["C", "PF", "FC"]), new Set(["PG", "SG", "G"])],
	// No quarterback and no line is not a football team.
	football: [new Set(["QB"]), new Set(["OL"])],
	// Somebody has to pitch and somebody has to catch.
	baseball: [new Set(["SP", "RP"]), new Set(["C"])],
	// A goalie and a defence.
	hockey: [new Set(["G"]), new Set(["D"])],
});

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
				// MARKETS ARE NOT ALL THE SAME SIZE, and every team here used to
				// have the same population. That is not a cosmetic simplification:
				// defaultBudgetLevel is a function of population RANK, so thirty
				// tied teams all rank 15.5, all get the middle of the scale, and
				// every department in the league sits at exactly the default
				// forever. The budget half of the front office cannot express
				// anything against a league like that - measured, the clamp its
				// plans run into never fired once in 2160 team-seasons.
				//
				// A geometric spread from one to eighteen million, which is about
				// what BBGM's own default teams span from the smallest market to
				// the largest. Fixed per tid, like real markets are.
				pop: 18 ** (tid / Math.max(1, NUM_TEAMS - 1)),
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
	// AGED UNDER THEIR OWN COACHING STAFF, not a league-wide default.
	//
	// This passed DEFAULT_LEVEL for everybody, which made the entire budget
	// half of the front office invisible here: smartBudgetLevels decides what
	// each plan spends on coaching, coaching drives progs, and progs are the
	// largest single input to how good a league's players get. A harness that
	// develops every player identically cannot see any of it, and a change to
	// that subsystem measured bit-for-bit identical output - which is how this
	// gap was noticed.
	//
	// The real game uses a three-year average of the team's coaching level
	// (getLevelLastThree), fed by expense levels accumulated game by game. The
	// harness applies game results by hand and has no such accumulation, so it
	// uses the CURRENT level instead: one season rather than three, and
	// therefore a slightly sharper response to a plan change than the real game
	// gives. Free agents keep the default - they have no staff.
	const coachingByTid = new Map<number, number>();
	for (const t of await idb.cache.teams.getAll()) {
		coachingByTid.set(t.tid, t.budget?.coaching ?? DEFAULT_LEVEL);
	}
	for (const p of players) {
		const coaching = coachingByTid.get(p.tid) ?? DEFAULT_LEVEL;
		player.addRatingsRow(p, DEFAULT_LEVEL);
		await player.develop(p, 1, false, coaching);
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
		// The ten players per team that team.ovr actually counts, league-wide.
		const rotationOvrs: number[] = [];
		// The same ten-per-team count, taken off the top of the whole league.
		const deployableOvrs: number[] = [];
		let scalesRow = "";
		const seasonOvrs: number[] = [];
		const seasonValues: number[] = [];
		// One row per released contract the first season it shows up as dead money.
		const deadRows: number[][] = [];
		// WHERE THE ROSTER CRUNCH COMES FROM, which is the same question as
		// where the dead money is made: every player over the limit when free
		// agency closes is a release, and a release strands whatever is left on
		// his contract. Measured, most of it is already there before free agency
		// opens - the draft class arrives and the team re-signs its own men, and
		// nothing anywhere counts to fifteen while it happens.
		const overflow = { sign: 0, trade: 0, dump: 0, end: 0 };
		const deadSeen = new Set<string>();
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
		const history: {
			tid: number;
			tier: string;
			winp: number;
			// What a rebuild is supposed to be accumulating, so a rebuild that
			// never ends can be told apart from one that is quietly working.
			ownFirsts: number;
			youngCore: number;
			bestYoung: number;
			// The team's own best ten, which is what team.ovr counts. Win% at the
			// end of a rebuild is concentration-confounded - in a league whose top
			// is heavier, the same roster wins fewer games - so the honest question
			// about a rebuild is whether the TALENT recovered.
			rot: number;
		}[][] = [];
		// Every pick made, so what the AI ASSUMED it was buying can be checked
		// against what the player was actually worth once he had grown into it.
		const picksTaken: {
			season: number;
			slot: number;
			pid: number;
			assumed: number;
			realized?: number;
		}[] = [];
		// DOES THE BOARD EARN ITS KEEP?
		//
		// Every team builds its own draft board - fit, timeline, what it has
		// already taken - and none of that is something a pick-value curve can
		// check. That curve says what slot N is worth; it does not say whether
		// the right player went at slot N.
		//
		// So the class is kept whole, in board order, and settled three years on
		// against the two comparisons that matter: the player who WAS next off
		// the value list, and the best player still unpicked.
		//
		// The answer, over three seeds of sixteen seasons, is that the boards are
		// FREE. Taken and next-off-the-list come out level in all four pick
		// buckets - within a tenth of a point either way - so the fit adjustments
		// move picks to different players of equal expected worth rather than
		// costing the league talent. That is the good outcome, and not the one to
		// assume: multipliers in [0.78, 1.22] raised to the 69th power decide the
		// pick outright, so a board that leaned the wrong way would show up here
		// immediately.
		//
		// The gap to the ceiling is 6 to 11 points and mostly unreachable - see
		// BOARD ANCHOR below for the check that says so.
		const draftClasses: {
			season: number;
			byBoard: number[];
			taken: number[];
			attrs: Map<
				number,
				{ ovr: number; pot: number; age: number; value: number }
			>;
			realized?: Map<number, number>;
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
			// CAP_TYPE=hard|none runs the whole decade under a cap rule this file
			// has never exercised past a single offseason. Real leagues are
			// played under all three, and a front office that only works under a
			// soft cap works for some of the people using it.
			if (nodeEnv.CAP_TYPE === "hard" || nodeEnv.CAP_TYPE === "none") {
				g.setWithoutSavingToDB("salaryCapType", nodeEnv.CAP_TYPE);
			}
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

				// EVERY TEAM PICKS ITS DEPARTMENTS, the way newPhasePreseason does.
				// Without this every budget in the league stays at whatever the
				// fixture left, and a front office feature that decides what a
				// franchise spends on coaching, health, facilities and scouting
				// runs entirely unobserved.
				{
					const activeTeams = (await idb.cache.teams.getAll()).filter(
						(t) => !t.disabled,
					);
					const popRanks = helpers.getPopRanks(activeTeams);
					const budgetCtx = g.get("smartAiFrontOffice")
						? await getLeagueTradeContext()
						: undefined;
					for (const [i, t] of activeTeams.entries()) {
						const baseLevel = finances.defaultBudgetLevel(popRanks[i]!);
						if (budgetCtx) {
							const posture = await getTradePosture(t.tid, budgetCtx);
							const levels = smartBudgetLevels({
								tier: posture.tier,
								baseLevel,
							});
							t.budget = { ...t.budget, ...levels };
						} else {
							// Vanilla's coin flip per department, so the control arm
							// gets the behavior it actually has.
							for (const key of [
								"scouting",
								"coaching",
								"health",
								"facilities",
							] as const) {
								if (Math.random() < 0.5) {
									t.budget[key] = baseLevel;
								}
							}
						}
						await idb.cache.teams.put(t);
					}
				}

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
				const classBefore = (
					await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
				)
					.filter((p) => p.draft.year === season)
					.sort((a, b) => b.value - a.value);
				const boardBefore = classBefore.map((p) => p.value);
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
				draftClasses.push({
					season,
					byBoard: classBefore.map((p) => p.pid),
					taken: [...(draftedPids ?? [])],
					attrs: new Map(
						classBefore.map((p) => [
							p.pid,
							{
								ovr: p.ratings.at(-1)!.ovr,
								pot: p.ratings.at(-1)!.pot,
								age: season - p.born.year,
								value: p.value,
							},
						]),
					),
				});

				g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
				await newPhaseResignPlayers({} as any);

				g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
				const excess = async () => {
					const counts = new Map<number, number>();
					for (const p of await idb.cache.players.indexGetAll("playersByTid", [
						0,
						Infinity,
					])) {
						counts.set(p.tid, (counts.get(p.tid) ?? 0) + 1);
					}
					let total = 0;
					for (const n of counts.values()) {
						total += Math.max(0, n - g.get("maxRosterSize"));
					}
					return total;
				};
				for (let day = FA_DAYS; day > 0; day--) {
					g.setWithoutSavingToDB("daysLeft", day);
					await decreaseDemands();
					let before = await excess();
					await clearSpaceForSignings();
					let after = await excess();
					overflow.dump += after - before;
					before = after;
					await autoSign();
					after = await excess();
					overflow.sign += after - before;
					before = after;
					// The real FA day ends with AI teams talking to each other
					// (freeAgents/play.ts does exactly this), and it is the channel
					// a rebuild actually runs through - veterans out, picks in.
					// NO_TRADES=1 closes it, for measuring what trading contributes.
					if (nodeEnv.NO_TRADES !== "1") {
						await trade.betweenAiTeams();
					}
					overflow.trade += (await excess()) - before;
				}
				overflow.end += await excess();
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
						seasonOvrs.push(rp.ratings.at(-1)!.ovr);
						seasonValues.push(rp.value);
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
						if (POSITION_GROUPS[0].has(pos)) {
							bigs += 1;
						}
						if (POSITION_GROUPS[1].has(pos)) {
							guards += 1;
						}
					}
					if (bigs === 0 || guards === 0) {
						positionless += 1;
					}
					// THE TALENT ACTUALLY DEPLOYED. team.ovr counts a team's best
					// ten players and nobody else, so this is the pool allocation
					// can move around but cannot change the size of - the control
					// for every claim in this file about concentration.
					for (const o of roster
						.map((rp) => rp.ratings.at(-1)!.ovr)
						.sort((a, b) => b - a)
						.slice(0, 10)) {
						rotationOvrs.push(o);
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

					// Each dead contract once, the season it first appears, with
					// enough about the man to say which half of the problem he is.
					const key = `${rp.tid}-${rp.pid}-${rp.contract.exp}`;
					if (!deadSeen.has(key)) {
						deadSeen.add(key);
						const rpp = await idb.cache.players.get(rp.pid);
						deadRows.push([
							rp.contract.amount,
							rp.contract.exp - season + 1,
							rpp ? rpp.ratings.at(-1)!.ovr : -1,
							rpp ? season - rpp.born.year : -1,
							rpp?.draft.tid === rp.tid ? 1 : 0,
						]);
					}
				}
				worstDeadShare = Math.max(
					worstDeadShare,
					deadMoney / (salaryCap * NUM_TEAMS),
				);

				// WHAT THE LEAGUE COULD HAVE DEPLOYED, against what it did.
				//
				// rotation= is the mean of every team's own best ten, and this file
				// called it the measure allocation cannot move. That is only true
				// while nobody is buried: a roster holds fifteen and team.ovr counts
				// ten, so a team stacked deep enough pushes good players into slots
				// that count for nothing while a stripped one fills its tenth seat
				// with whoever is left. deployable= is the same number under perfect
				// allocation - the best 10-per-team players in the league, wherever
				// they actually are - so the gap between the two IS the cost of how
				// the league is arranged, and a change to rotation= can finally be
				// told apart from a change to the talent underneath it.
				{
					const best = [...seasonOvrs]
						.sort((a, b) => b - a)
						.slice(0, 10 * NUM_TEAMS);
					deployableOvrs.push(
						best.reduce((a, x) => a + x, 0) / Math.max(1, best.length),
					);
				}
				{
					const o = seasonOvrs.sort((a, b) => b - a);
					const v = seasonValues.sort((a, b) => b - a);
					const at = (xs: number[], r: number) => (xs[r - 1] ?? 0).toFixed(1);
					scalesRow = [30, 90, 150, 240, 450]
						.map((r) => `r${r}:ovr${at(o, r)}/val${at(v, r)}`)
						.join(" ");
					seasonOvrs.length = 0;
					seasonValues.length = 0;
				}

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
				//
				// AND READ best5/worst5 IN THE RIGHT UNITS. team.ovr is a weighted
				// sum of a team's best ten players - weights 0.333 down to 0.078,
				// summing to 1.59 - rescaled by 50/15. So one point of roster
				// talent moves team ovr by about 5.3, and the enormous-looking
				// bottom-five gap is a small one wearing a magnifying glass:
				// measured over three seeds, the five worst teams under this front
				// office are 2.1 points worse across their top ten than stock's,
				// and that alone prints as the 12-against-26 in the table below.
				// Their best player is 1.8 points worse and they carry one MORE
				// body. There is no husk down there; there is a slightly thinner
				// rotation, amplified.
				//
				// The TALENT row's rotation= is the control that settles it: the
				// mean of every team's best ten, league-wide. Stock 54.3, this
				// front office 54.0, and the seeds split three each way - the same
				// talent is being deployed, in a different arrangement. What is
				// NOT happening is hoarding: players good enough to start
				// somewhere sitting at roster rank eleven and counting for
				// nothing. Measured, that population is essentially empty on both
				// arms (five men across three twelve-season leagues).
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
				const allPicks = await idb.cache.draftPicks.getAll();
				for (let tid = 0; tid < NUM_TEAMS; tid++) {
					const posture = await getTradePosture(tid, ctx);
					const ts = await idb.cache.teamSeasons.indexGet(
						"teamSeasonsBySeasonTid",
						[season, tid],
					);
					const roster = await idb.cache.players.indexGetAll(
						"playersByTid",
						tid,
					);
					const young = roster.filter((p) => season - p.born.year <= 24);
					yearRow.push({
						tid,
						tier: posture.tier,
						winp: ts ? ts.won / Math.max(1, ts.won + ts.lost) : 0.5,
						ownFirsts: allPicks.filter(
							(dp) =>
								dp.round === 1 && dp.originalTid === tid && dp.tid === tid,
						).length,
						youngCore: young.filter((p) => p.value >= ctx.coreValue).length,
						bestYoung: Math.max(0, ...young.map((p) => p.ratings.at(-1)!.ovr)),
						rot: (() => {
							const top = roster
								.map((p) => p.ratings.at(-1)!.ovr)
								.sort((a, b) => b - a)
								.slice(0, 10);
							return top.reduce((a, x) => a + x, 0) / Math.max(1, top.length);
						})(),
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
				for (const cls of draftClasses) {
					if (cls.realized === undefined && season - cls.season === 3) {
						const realized = new Map<number, number>();
						for (const pid of cls.byBoard) {
							const p = await idb.cache.players.get(pid);
							realized.set(pid, p && p.tid !== PLAYER.RETIRED ? p.value : 0);
						}
						cls.realized = realized;
					}
				}
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
			// The same two questions in talent rather than wins.
			const rot3: number[] = [];
			const rot5: number[] = [];
			let noTalentGain = 0;
			let entered = 0;
			let stuck = 0;
			// What the two outcomes were actually holding on the way through. A
			// rebuild that never ends and one that works look identical in win
			// percentage until the last year, so the question is what they were
			// accumulating in between - picks, and young players good enough to
			// build around.
			type Assets = { firsts: number; core: number; best: number };
			const stuckRows: Assets[] = [];
			const escapedRows: Assets[] = [];
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
					if (later3) {
						rot3.push(later3.rot - row.rot);
					}
					if (later5) {
						at5.push(later5.winp - row.winp);
						rot5.push(later5.rot - row.rot);
						if (later5.rot <= row.rot) {
							noTalentGain += 1;
						}
						const during: Assets = { firsts: 0, core: 0, best: 0 };
						let n = 0;
						for (let k = 0; k <= 5; k++) {
							const r = history[y + k]?.[row.tid];
							if (r) {
								during.firsts += r.ownFirsts;
								during.core += r.youngCore;
								during.best += r.bestYoung;
								n += 1;
							}
						}
						if (n > 0) {
							const mean = {
								firsts: during.firsts / n,
								core: during.core / n,
								best: during.best / n,
							};
							if (later5.tier === "teardown" || later5.tier === "seller") {
								stuck += 1;
								stuckRows.push(mean);
							} else {
								escapedRows.push(mean);
							}
						}
					}
				}
			}
			const describe = (a: Assets[]) =>
				a.length === 0
					? "n=0"
					: `n=${a.length} firsts=${avg(a.map((x) => x.firsts)).toFixed(1)} youngCore=${avg(
							a.map((x) => x.core),
						).toFixed(2)} bestYoung=${avg(a.map((x) => x.best)).toFixed(1)}`;
			rows.push(
				"",
				// WHAT ENDS A REBUILD, measured across six twelve-season leagues.
				// Rebuilds that got out and rebuilds that never did held the SAME
				// number of first-round picks - 3.8 against 3.7. What separated
				// them was young players good enough to build on: 2.4 against 1.8,
				// best young player 59.0 against 56.9. Stockpiling picks is not
				// what ends a rebuild; keeping the players is. That is what
				// REBUILD_CORE_RANK in tradePosture was written from.
				//
				// It did NOT move the number it was aimed at: teams still
				// teardown-or-selling five years on ran 35% before and 35% after,
				// against stock's 22%. What it moved was everything around it -
				// fewer teams fall into a full teardown at all (18.2 a run to
				// 16.5), and the talent that used to be shopped out of those
				// rosters stays employed.
				//
				// AND THAT 35% AGAINST 22% IS NOT A DEFECT, which took a second
				// measurement to see and is the reason REBUILD TALENT exists.
				// stillDown@5 asks whether the team is still SELLING five years
				// on, and that is a question about wins, and wins are
				// concentration-confounded exactly the way mean team ovr is: in a
				// league whose top is this much heavier, the wins the rebuilt team
				// needs have already been taken. Ask instead what happened to its
				// TALENT - the mean OVR of its own best ten, which no other team's
				// success can move - and the ranking flips. Six seeds:
				//
				//                      stock    smart
				//   rot+3y             3.67     4.42
				//   rot+5y             4.32     5.85   higher on five of six
				//   no talent gain@5   11%      12%
				//
				// A rebuild under this front office recovers a third more talent
				// than a stock one and fails outright no more often. It just does
				// not get to call itself a contender afterwards, because the teams
				// above it got better too.
				`REBUILDS entered=${entered} winp+3y=${avg(at3).toFixed(3)} winp+5y=${avg(at5).toFixed(3)} stillDown@5=${stuck}/${at5.length}`,
				`REBUILD TALENT rot+3y=${avg(rot3).toFixed(2)} rot+5y=${avg(rot5).toFixed(2)} noGain@5=${noTalentGain}/${rot5.length}`,
				`REBUILD ASSETS stuck[${describe(stuckRows)}] escaped[${describe(escapedRows)}]`,
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

				// THE SHAPE OF THE MARKET. A league where every deal is one player
				// for one player is a league whose front offices cannot consolidate
				// - the single most characteristic move in the sport, two useful
				// players for one good one, would simply never happen.
				const shapes = new Map<string, number>();
				for (const e of (await idb.cache.events.getAll()) as any[]) {
					if (e.type !== "trade" || !e.aiTrade || !e.teams) {
						continue;
					}
					const counts = (e.teams as any[]).map((t) => ({
						players: t.assets.filter((a: any) => a.pid !== undefined).length,
						picks: t.assets.filter((a: any) => a.dpid !== undefined).length,
					}));
					const players = counts
						.map((c) => c.players)
						.sort((a, b) => b - a)
						.join("v");
					const picks = counts.reduce(
						(a: number, c: { picks: number }) => a + c.picks,
						0,
					);
					const key = `p${players}${picks > 0 ? `+${picks}pk` : ""}`;
					shapes.set(key, (shapes.get(key) ?? 0) + 1);
				}
				if (shapes.size > 0) {
					rows.push(
						`TRADE SHAPES ${[...shapes]
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
				// What the boards were worth. Three numbers per bucket, all in
				// realized value three years on:
				//   took  - the player the AI actually chose there
				//   bpa   - the player who was next off the value list, which is
				//           what the league would have done with no boards at all
				//   best  - the best player still on the board, i.e. the ceiling
				// took below bpa means the boards are costing the league talent.
				const settled = draftClasses.filter((c) => c.realized);
				const buckets: [string, number, number][] = [
					["1-3", 1, 3],
					["4-10", 4, 10],
					["11-30", 11, 30],
					["31+", 31, Infinity],
				];
				const parts: string[] = [];
				for (const [label, lo, hi] of buckets) {
					let took = 0;
					let bpa = 0;
					let best = 0;
					let n = 0;
					for (const cls of settled) {
						const realized = cls.realized!;
						const left = new Set(cls.byBoard);
						for (const [i, pid] of cls.taken.entries()) {
							const slot = i + 1;
							if (slot >= lo && slot <= hi) {
								// The ceiling is whoever is still unpicked, which is
								// what makes this a measure of CHOICE rather than of
								// where the team was drafting.
								let ceiling = 0;
								for (const other of left) {
									ceiling = Math.max(ceiling, realized.get(other) ?? 0);
								}
								took += realized.get(pid) ?? 0;
								bpa += realized.get(cls.byBoard[i] ?? -1) ?? 0;
								best += ceiling;
								n += 1;
							}
							left.delete(pid);
						}
					}
					if (n > 0) {
						parts.push(
							`${label}:took${(took / n).toFixed(1)}/bpa${(bpa / n).toFixed(1)}/best${(best / n).toFixed(1)}(n=${n})`,
						);
					}
				}
				if (parts.length > 0) {
					rows.push(`BOARDS ${parts.join(" ")}`);
				}

				// IS THERE A BETTER BOARD TO ANCHOR ON?
				//
				// Everything above measures how well the AI picks off a list
				// ordered by BBGM's `value`. This asks whether that list is the
				// right one: rank each class by a handful of candidate predictors,
				// take the top thirty, and total what those players were actually
				// worth three years on. A predictor that beats `value` here is a
				// better anchor for the board; one that does not is a distraction.
				//
				// None of them beat it. Across three seeds `value` scores 54.81
				// and the nearest rival - an even split of ovr and pot - scores
				// 54.83, which is noise; ovr alone (54.23) and pot alone (54.60)
				// are clearly worse, and tilting either toward youth is worse
				// again. So the six-to-eleven point gap between what teams took
				// and the best player left is not a modelling failure with a fix
				// in it. It is the draft being a draft.
				const predictors: [
					string,
					(a: {
						ovr: number;
						pot: number;
						age: number;
						value: number;
					}) => number,
				][] = [
					["value", (a) => a.value],
					["ovr", (a) => a.ovr],
					["pot", (a) => a.pot],
					["half", (a) => 0.5 * a.ovr + 0.5 * a.pot],
					["young", (a) => a.value + 2 * (21 - a.age)],
					["potYoung", (a) => a.pot + 2 * (21 - a.age)],
				];
				const scores: string[] = [];
				for (const [name, f] of predictors) {
					let total = 0;
					let count = 0;
					for (const cls of settled) {
						const ranked = [...cls.attrs]
							.sort((a, b) => f(b[1]) - f(a[1]))
							.slice(0, 30);
						for (const [pid] of ranked) {
							total += cls.realized!.get(pid) ?? 0;
							count += 1;
						}
					}
					if (count > 0) {
						scores.push(`${name}=${(total / count).toFixed(2)}`);
					}
				}
				if (scores.length > 0) {
					rows.push(`BOARD ANCHOR top30worth3y ${scores.join(" ")}`);
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
			// WHAT SMART AI ACTUALLY DOES TO A LEAGUE, against stock BBGM, so the
			// rows below have something to be read against. Six seeds of twelve
			// real basketball seasons, thirty teams, SMART_AI=0 for the control:
			//
			//                     stock    smart
			//   best5              71.3     75.5   up on five seeds of six
			//   worst5             27.1     10.5   DOWN on all six
			//   tovrSD             14.9     22.1   up on all six
			//   rotation           54.5     53.8   DOWN on five of six
			//   allRostered        49.0     48.4   down on five of six
			//   rostered n         5115     5275   up on all six
			//   titles distinct    8.3      8.7    noise
			//   trades/season      18.7     32.3   up on all six
			//   dead $/season      124M     195M   UP on all six
			//   dead contracts     240      324    up on all six
			//     of them drafted  58       46     DOWN
			//     of them acquired 181      278    UP
			//   starsUnsigned/yr   0.10     0.37   UP on four of six
			//
			// THE ROTATION ROW IS NEW AND IT IS THE WORST NUMBER IN THE TABLE.
			// Until market sizes existed here it read 54.3 against 54.2, three
			// seeds each way - the same talent, differently arranged, which is
			// what every claim in this file about concentration rested on. Give
			// the teams different populations and it becomes 54.5 against 53.8,
			// down on five seeds of six.
			//
			// AND rotation= IS NOT THE CONTROL THIS FILE CALLED IT. It was
			// described as the measure allocation cannot move, which is only true
			// while nobody is buried: a roster holds fifteen and team.ovr counts
			// ten, so a team stacked deep enough pushes good players into slots
			// worth nothing while a stripped one fills its tenth seat with
			// whatever is left. That was empirically fine when it was written -
			// burial measured at essentially zero - and stopped being fine the
			// moment markets made the league more unequal.
			//
			// deployable= is the honest control: the same ten-per-team count
			// taken off the top of the whole league, so the gap between the two
			// IS the arrangement. Six seeds:
			//
			//                       stock    smart
			//   deployable          54.95    54.51   the talent that was there
			//   rotation            54.51    53.80   what got played
			//   cost of arrangement  0.44     0.71
			//
			// So the 0.71 splits about 0.44 talent and 0.27 arrangement. The
			// arrangement half is the feature working harder - this front office
			// concentrates more, and concentration buries people. The TALENT half
			// is not, and it is the open question.
			//
			// It is not the budget plan: running the smart front office on
			// vanilla's coin-flip budgets gives 53.80, identical to three
			// decimals. It is not cap holds either - switching planCapHold off
			// entirely leaves rotation at 53.81 and makes the top five and stars
			// unsigned both WORSE, so the hold is earning its keep.
			//
			// What it looks like is players nobody signs. Against the whole
			// living population (rostered and free agents together) the two arms'
			// top three hundred are near enough level, 54.89 against 55.06; among
			// ROSTERED players they are 54.51 against 54.95. Stock's rostered top
			// three hundred falls 0.11 short of the league's; this front office's
			// falls 0.38 short. It is not the stars - those are 0.37 a season
			// against 0.10, far too few to move a mean over three hundred - and
			// it is not minimum-salary men either, since BARGAINS LEFT runs LOWER
			// here than stock. It is good players asking real money, and the
			// obvious suspect is the roster gate in autoSign that refuses to
			// sign anyone when the cut it would force costs money.
			//
			// The trade the comments elsewhere in this file describe is real and
			// still holds: the top five gain four and a half points and the bottom
			// five lose sixteen. A league run by this front office concentrates,
			// deliberately, and the concentration is the feature.
			//
			// AND IT IS THE SAME FEATURE UNDER ALL THREE CAP RULES, which nothing
			// had ever checked past a single offseason. CAP_TYPE=hard|none runs
			// the whole decade under the other two. Six seeds each, thirty teams,
			// twelve real seasons:
			//
			//              tovrSD      best5      rotation    rebuild rot+5y   dead $/season
			//   hard    13.5 -> 19.4  66.3->72.4  53.9->53.7   4.79 -> 5.13     62M -> 139M
			//   soft    14.4 -> 21.3  68.7->75.5  54.3->54.2      -              139M -> 201M
			//   none    16.4 -> 22.9  71.7->76.2  54.3->54.1   3.98 -> 4.84    197M -> 255M
			//
			// Same shape every time: six points of concentration on the top five,
			// rotation talent within a fifth of a point of stock, rebuilds that
			// recover MORE talent, and dead money up. No illegal roster in any of
			// the thirty-six runs. The one thing the cap rule changes is the size
			// of the dead-money gap relative to stock - +124% under a hard cap,
			// +45% soft, +29% with no cap - which is what it should do, because
			// the tighter the cap the more a stranded contract costs.
			//
			// ONE NUMBER IS NOT THE FEATURE. Dead money is up 42%, on every
			// seed. Stars left unsigned used to be the other one and is now at
			// parity - the whole of it turned out to be a rebuild shopping its own
			// young players into a market with nowhere to put them, and it went
			// away when selectBuildingBlocks stopped letting that happen.
			//
			// The DEADPROF row splits it and the split is the whole story. Dead
			// contracts belonging to men the team DRAFTED are now BELOW stock -
			// 35 against 62 - because justDrafted stopped charging for cuts the
			// rules make free and the stopgap rule stopped handing four years to
			// people who were never going to get them. Every remaining point of
			// the gap is men the team ACQUIRED: 287 against 186, and 1036M
			// against 658M. The median one is 26, 40 ovr, on three and a half
			// million with two years still to run - a body, not a player.
			//
			// They are NOT one mechanism, which is what it looked like at first
			// and is worth correcting here rather than leaving as folklore. The
			// guess was trade churn - this front office trades about seventy
			// percent more than stock - but instrumenting every release showed
			// all of them, on both arms, happening in FREE AGENCY and none from a
			// trade. Smart AI releases 35% more players than stock and strands
			// 55% more money, at 3.9M a release against 3.3M: it signs more, and
			// each signing displaces somebody who is still owed.
			//
			// Across the seeds the two do not even move together - one seed had
			// the largest dead-money gap and FEWER unsigned stars.
			//
			// AND THE DEAD MONEY IS THE PRICE OF THE FEATURE, NOT A DEFECT IN IT.
			// Five separate mechanisms have now been built and measured against
			// it, sharing no code and reached from four different directions, and
			// every one of them lands on the same line. Six seeds each, against
			// the arm without it:
			//
			//                                  dead $/season   rotation
			//   block re-signing at 15              -50.2M       -0.41
			//   the same but only below the bar     -33.3M       -0.23
			//   trade the surplus instead of
			//     releasing it                      -34.2M       -0.27
			//   proportional autoSign margin        +4.7M        (worse both)
			//   autoSign value bar (in autoSign)    -16M         (best5 -2.2)
			//
			// A hundred and twenty to a hundred and forty-five million a season
			// per point of the talent the league actually employs, whichever way
			// you come at it. That is not four bugs; it is one exchange rate. The
			// waste is what it costs to keep this many players employed at this
			// quality, and buying it down buys down the employment with it.
			//
			// The last of those is worth its own sentence because the premise
			// looked airtight and was wrong. Releasing a player does not destroy
			// him - it returns him to a market that reallocates him - and a team
			// with an open roster place does better in free agency than it does
			// taking somebody else's cast-off. Moving 123 surplus contracts a run
			// instead of releasing them took a third off the dead money and cost
			// half a point of everyone rostered, because each one crowded out a
			// better free agent. It is not in the tree.
			//
			// TWO THINGS HAVE COME OFF THE CURVE, and they share a shape: both
			// changed WHICH players a team commits to, not HOW MANY. The stopgap
			// rule declines to guarantee years to a man below the rotation bar
			// while still signing him; REBUILD_CORE_RANK stops a rebuild shopping
			// its own best young players. Neither reduces employment at all, and
			// the second one raised it. That is the test worth applying to the
			// next idea here.
			//
			// Worth knowing that rosterCuts' measured 22% saving was smart-before
			// against smart-after. Against stock this is still well up.
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
						`allRostered=${m(rosteredOvrs).toFixed(1)} n=${rosteredOvrs.length} ` +
						`rotation=${m(rotationOvrs).toFixed(2)} ` +
						`deployable=${m(deployableOvrs).toFixed(2)}`,
					// WHICH HALF THE DEAD MONEY IS: men the team drafted, or men it
					// signed or traded for. They respond to completely different
					// things, and lumping them together hid that for a long time.
					`OVERFLOW sign=${overflow.sign} trade=${overflow.trade} dump=${overflow.dump} atEnd=${overflow.end}`,
					// OVR AND VALUE ARE NOT THE SAME SCALE, and a bar meant for one
					// of them applied to the other is off by dozens of players.
					// This is the last season's league-wide ranks in both, so
					// anywhere the code mixes them can be checked rather than
					// guessed at. See minTradeValue in trade/tradePosture.ts,
					// which does mix them, on purpose, with the measurement.
					`SCALES ${scalesRow}`,
					`DEADPROF ${(() => {
						const bucket = (name: string, f: (x: number[]) => boolean) => {
							const xs = deadRows.filter((x) => f(x));
							const amt = xs.reduce((a, x) => a + x[0]!, 0);
							return (
								`${name}:n${xs.length}/${Math.round(amt / 1000)}M` +
								`/ovr${m(xs.map((x) => x[2]!)).toFixed(0)}` +
								`/age${m(xs.map((x) => x[3]!)).toFixed(0)}` +
								`/yr${m(xs.map((x) => x[1]!)).toFixed(1)}`
							);
						};
						return [
							bucket("all", () => true),
							bucket("ownDraft", (x) => x[4] === 1),
							bucket("acquired", (x) => x[4] === 0),
						].join(" ");
					})()}`,
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

		{
			// WHAT THE DEAD MONEY IS MADE OF. Every release an AI team makes goes
			// through checkRosterSizes, and measuring where they happen settled
			// what was previously a guess: all of them, on both arms, land in
			// FREE AGENCY. Not one comes from a trade. So the dead money this
			// front office carries is the price of its own signings displacing
			// people, and roster churn from trading was never involved.
			const released = await idb.cache.releasedPlayers.getAll();
			let stranded = 0;
			for (const rp of released) {
				stranded += rp.contract.amount;
			}
			rows.push(
				"",
				`RELEASED still-owed n=${released.length} total=${Math.round(stranded / 1000)}M`,
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
