import {
	COLA_ALPHA,
	COLA_NUM_LOTTERY_PICKS,
	COLA_OPT_OUT_PENALTY,
	PHASE,
	PLAYER,
} from "../../../common/constants.ts";
import type { Team } from "../../../common/types.ts";
import { range } from "../../../common/utils.ts";
import { idb } from "../../db/index.ts";
import g from "../../util/g.ts";
import helpers from "../../util/helpers.ts";
import logEvent from "../../util/logEvent.ts";
import getNumPlayoffTeams from "../season/getNumPlayoffTeams.ts";
import { colaLotteryOdds } from "../trade/futurePickOutlook.ts";
import { frontOfficeLog } from "../../util/frontOfficeLog.ts";
import { isAiControlled } from "../../util/isAiControlled.ts";

// All teams up to the final 3 rounds of playoffs
export const getNumColaLotteryTeams = async () => {
	const numPlayoffRounds = g.get("numGamesPlayoffSeries", "current").length;
	let numPlayoffTeams;
	if (numPlayoffRounds <= 3) {
		// This handles byes
		numPlayoffTeams = (await getNumPlayoffTeams(g.get("season")))
			.numPlayoffTeams;
	} else {
		// Final 3 rounds
		numPlayoffTeams = 2 ** 3;
	}

	return g.get("numActiveTeams") - numPlayoffTeams;
};

const logChange = (
	before: number,
	after: number,
	t: Team,
	direction: "increased" | "decreased",
	reason: string,
) => {
	const text = `The lottery chances for the <a href="${helpers.leagueUrl([
		"roster",
		`${t.abbrev}_${t.tid}`,
		g.get("season"),
	])}">${t.name}</a> ${direction} from ${before} to ${after}${reason}.`;

	logEvent({
		type: "draftLottery",
		text,
		showNotification: false,
		pids: [],
		tids: [t.tid],
		score: 0,
	});
};

// Champion gets their lottery chances multiplied by 0. Loser of the finals, 0.25. Loser of the semifinals, 0.5. Loser of the quarterfinals, 0.75.
const PLAYOFF_FACTORS = [0.75, 0.5, 0.25, 0];

// Reaching the final three rounds is what costs a team its stockpile, and it is
// the same line that keeps a team out of the draw - getNumColaLotteryTeams
// counts everybody below it. One predicate, so the two cannot drift apart.
const playoffFactor = (playoffRoundsWon: number) => {
	const numPlayoffRounds = g.get("numGamesPlayoffSeries", "current").length;
	const offset = numPlayoffRounds - PLAYOFF_FACTORS.length + 1;
	if (playoffRoundsWon < 0) {
		return undefined;
	}
	return PLAYOFF_FACTORS[playoffRoundsWon - offset];
};

// Call this at the end of the playoffs
export const updateColaAfterPlayoffs = async () => {
	if (g.get("draftType") !== "cola") {
		return;
	}

	const season = g.get("season");
	const teamSeasons = await idb.getCopies.teamSeasons(
		{ season },
		"noCopyCache",
	);
	for (const row of teamSeasons) {
		const t = await idb.cache.teams.get(row.tid);
		if (!t) {
			throw new Error("Should never happen");
		}
		if (t.draftLottery?.type !== "cola") {
			t.draftLottery = {
				type: "cola",
				chances: 0,
			};
		}

		// Chance changes compound (+= alpha, *= playoff factor), so this update
		// must run exactly once per team per season no matter how many times the
		// surrounding phase change executes (replay, race, double-trigger).
		if (t.draftLottery.updatedAfterPlayoffs === season) {
			continue;
		}
		t.draftLottery.updatedAfterPlayoffs = season;

		const before = t.draftLottery.chances;

		const factor = playoffFactor(row.playoffRoundsWon);
		if (factor !== undefined) {
			t.draftLottery.chances = Math.round(t.draftLottery.chances * factor);
			logChange(
				before,
				t.draftLottery.chances,
				t,
				"decreased",
				" due to their playoff success",
			);
		} else {
			t.draftLottery.chances += COLA_ALPHA;
			logChange(before, t.draftLottery.chances, t, "increased", "");
		}

		await idb.cache.teams.put(t);
	}
};

// Top 4 picks have their draft index multiplied by this amount
const DRAFT_LOTTERY_FACTORS = [0, 0.25, 0.5, 0.75];

export const updateColaAfterLottery = async (tids: number[]) => {
	for (const [i, tid] of tids.entries()) {
		const factor = DRAFT_LOTTERY_FACTORS[i];
		if (factor === undefined) {
			continue;
		}

		const t = await idb.cache.teams.get(tid);
		if (!t) {
			throw new Error("Should never happen");
		}
		if (t.draftLottery?.type !== "cola") {
			t.draftLottery = {
				type: "cola",
				chances: 0,
			};
		}
		const before = t.draftLottery.chances;
		t.draftLottery.chances = Math.round(t.draftLottery.chances * factor);
		await idb.cache.teams.put(t);

		logChange(
			before,
			t.draftLottery.chances,
			t,
			"decreased",
			` due to winning the ${helpers.ordinal(i + 1)} pick`,
		);
	}

	await chargeColaOptOuts();
};

// An opt out is declared before the draw and paid for after it. Charging it is
// separate from the draw itself because a COLA league can reach draft day
// without one - genOrder falls back to "noLottery" when too few teams hold a
// first rounder - and a flag left standing there would sit a team out for free,
// every season, forever.
export const chargeColaOptOuts = async () => {
	const teams = await idb.cache.teams.getAll();
	for (const t of teams) {
		if (t.draftLottery?.type === "cola" && t.draftLottery.optOut) {
			const before = t.draftLottery.chances;
			t.draftLottery.chances = Math.max(
				0,
				t.draftLottery.chances - COLA_OPT_OUT_PENALTY,
			);

			logChange(
				before,
				t.draftLottery.chances,
				t,
				"decreased",
				" due to opting out of the lottery",
			);

			delete t.draftLottery.optOut;

			await idb.cache.teams.put(t);
		}
	}
};

export const initializeCola = async () => {
	const teams = await idb.cache.teams.getAll();

	// Look back to the past 20 completed seasons
	const season = g.get("season");
	const offset = g.get("phase") <= PHASE.PLAYOFFS ? -1 : 0;
	const seasons = [season - 20 + offset, season + offset] as const;

	const colaByTid: Record<number, number> = {};

	for (const season of range(seasons[0], seasons[1] + 1)) {
		const teamSeasons = await idb.getCopies.teamSeasons(
			{ season },
			"noCopyCache",
		);
		if (teamSeasons.length === 0) {
			// No log of playoff history, so just skip this season
			continue;
		}

		// Increase/decrease based on playoff success
		for (const row of teamSeasons) {
			const t = teams[row.tid]!;
			if (t.disabled) {
				continue;
			}

			let cola = colaByTid[t.tid] ?? 0;

			if (row.playoffRoundsWon < 0) {
				cola += COLA_ALPHA;
			} else {
				const numPlayoffRounds = g.get(
					"numGamesPlayoffSeries",
					row.season,
				).length;
				const offset = numPlayoffRounds - PLAYOFF_FACTORS.length + 1;
				const factor = PLAYOFF_FACTORS[row.playoffRoundsWon - offset];
				if (factor === undefined) {
					// In the lottery
					cola += COLA_ALPHA;
				} else {
					// In the final 3 rounds of playoffs
					cola = Math.round(cola * factor);
				}
			}

			colaByTid[t.tid] = cola;
		}

		// Decrease based on lotery success
		const players = await idb.getCopies.players(
			{ draftYear: season },
			"noCopyCache",
		);
		for (const p of players) {
			if (p.draft.round === 1 && p.draft.pick <= 4) {
				const factor = DRAFT_LOTTERY_FACTORS[p.draft.pick - 1];
				if (factor === undefined) {
					throw new Error("Should never happen");
				}
				const tid = p.draft.tid;
				colaByTid[tid] ??= 0;
				colaByTid[tid] = Math.round(colaByTid[tid] * factor);
			}
		}
	}

	for (const t of teams) {
		// type check is for importing leagues, cause this gets run but might already have a value
		if (t.disabled || t.draftLottery?.type === "cola") {
			continue;
		}

		t.draftLottery = {
			type: "cola",
			chances: colaByTid[t.tid] ?? 0,
		};
		await idb.cache.teams.put(t);
	}
};

export const disableCola = async () => {
	const teams = await idb.cache.teams.getAll();

	for (const t of teams) {
		if (t.draftLottery?.type === "cola") {
			delete t.draftLottery;
			await idb.cache.teams.put(t);
		}
	}
};

// ---- Opting out: declining a lottery you would rather not win --------------
//
// COLA lets a team sit out the draw. It costs COLA_OPT_OUT_PENALTY chances and
// forfeits any shot at the top four - which sounds like pure loss until you
// notice what winning costs. Take the first pick and your entire stockpile
// goes to zero; take the second and three quarters of it does. A team that has
// spent five seasons banking chances and is looking at a draft with nothing at
// the top of it can pay a flat penalty instead, keep the rest, and aim the
// stockpile at a class worth winning. It is the one genuinely counter-intuitive
// lever in the system, and until now only a human could pull it -
// toggleColaOptOut reads userTid and nothing else, so AI teams could not use a
// mechanic the rules give them.
//
// There is no threshold here on purpose. Every earlier attempt at one ("opt out
// above N chances", "opt out if next year's class is 10% better") was a guess,
// and a guess is exactly what cannot be checked. What follows instead is the
// decision itself, priced in one currency: what entering the draw is expected
// to WIN you this year, against what the chances it burns would have won you
// next year.
//
// Be warned that the answer this returns is almost always no, and that is not a
// bug to be tuned away. Measured over sixty simulated seasons of a thirty team
// league, the largest stockpile anybody ever built was about a tenth of the
// pool; at that share the extra odds a preserved stockpile buys next year are
// worth around 3% of the odds thrown away now, and no observed class variation
// closes a gap that size. Even at the most favourable stockpile share in the
// whole parameter space - about 30% of the pool, which a full league never
// reaches - this year's lottery would have to be worth under a quarter of next
// year's. Opting out is a lever for lopsided small leagues and freak classes.
// The point of this code is that an AI can now reach it when those arrive, not
// that it should be reaching for it every June.

// What winning costs, averaged over the four picks it could be:
// DRAFT_LOTTERY_FACTORS leaves 0, 25%, 50% and 75% of a stockpile standing, so
// entering and winning burns 62.5% of it on average.
export const MEAN_WIN_BURN =
	1 -
	DRAFT_LOTTERY_FACTORS.reduce((a, x) => a + x, 0) /
		DRAFT_LOTTERY_FACTORS.length;

// What winning the lottery is actually worth in a given class: not the value of
// a top pick, but the UPGRADE over the pick you get anyway. Losing the draw
// does not leave you empty-handed, it leaves you picking on record - so a class
// whose fifth-best prospect is nearly as good as its best is a class where
// winning buys very little, however strong it is overall. This is the quantity
// the decision turns on, and it is why comparing raw class strength was the
// wrong measure.
export type ClassEdge = {
	// Mean value of the picks the lottery hands out.
	lottery: number;
	// Mean value of the picks immediately after them - what a team that stays
	// out, or enters and loses, is looking at instead.
	fallback: number;
};

const edge = (c: ClassEdge) => Math.max(0, c.lottery - c.fallback);

export const shouldOptOutOfCola = ({
	chances,
	total,
	numLotteryPicks,
	thisClass,
	nextClass,
}: {
	chances: number;
	// Every chance in the league, this team's included.
	total: number;
	numLotteryPicks: number;
	// Undefined when a class cannot be read, which is a reason to take the draw
	// rather than gamble on skipping it.
	thisClass: ClassEdge | undefined;
	nextClass: ClassEdge | undefined;
}): boolean => {
	if (thisClass === undefined || nextClass === undefined) {
		return false;
	}
	if (!(total > 0) || !(chances > 0)) {
		return false;
	}

	const odds = colaLotteryOdds({
		chancesShare: chances / total,
		numLotteryPicks,
	});
	if (odds <= 0) {
		return false;
	}

	// Where the stockpile ends up either way. Entering spends it only in the
	// branch where it wins, so in expectation it keeps all but the burn;
	// opting out pays the penalty for certain.
	const afterEntering = chances * (1 - MEAN_WIN_BURN * odds);
	const afterOptingOut = Math.max(0, chances - COLA_OPT_OUT_PENALTY);

	// Next year, with everybody else's stockpile held where it is and both
	// branches banking one more season for missing the playoffs - which a team
	// sitting on a stockpile this size is likely to do.
	const others = Math.max(0, total - chances);
	const oddsNext = (banked: number) => {
		const mine = banked + COLA_ALPHA;
		return colaLotteryOdds({
			chancesShare: mine / (others + mine),
			numLotteryPicks,
		});
	};

	const givenUpNow = odds * edge(thisClass);
	const boughtLater =
		(oddsNext(afterOptingOut) - oddsNext(afterEntering)) * edge(nextClass);

	return boughtLater > givenUpNow;
};

// The two bands of a draft class the decision compares: the picks the lottery
// hands out, and the ones right behind them. Undefined when the class has not
// been generated far enough to see both, which is a reason to stay in the draw
// rather than skip it.
export const classEdge = async (
	season: number,
	numLotteryPicks: number,
): Promise<ClassEdge | undefined> => {
	const values = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
	)
		.filter((p) => p.draft.year === season)
		.map((p) => p.value)
		.sort((a, b) => b - a);
	if (values.length < 2 * numLotteryPicks) {
		return undefined;
	}
	const mean = (band: number[]) =>
		band.reduce((a, x) => a + x, 0) / band.length;
	return {
		lottery: mean(values.slice(0, numLotteryPicks)),
		fallback: mean(values.slice(numLotteryPicks, 2 * numLotteryPicks)),
	};
};

// Who is actually in this year's draw, and how many chances are riding on it.
// Neither is "every team" - genOrder gives zero weight to anybody who reached
// the final three rounds, and zero to anybody whose own first rounder has
// changed hands, because a traded pick cannot win the lottery. A team outside
// the field has nothing to protect and nothing to win, so it must never pay to
// opt out; and a total that counted those teams would badly understate
// everybody else's odds.
const colaLotteryField = async () => {
	const season = g.get("season");
	const [teamSeasons, draftPicks] = await Promise.all([
		idb.getCopies.teamSeasons({ season }, "noCopyCache"),
		idb.cache.draftPicks.getAll(),
	]);

	const deepPlayoffs = new Set<number>();
	for (const row of teamSeasons) {
		if (playoffFactor(row.playoffRoundsWon) !== undefined) {
			deepPlayoffs.add(row.tid);
		}
	}

	const ownFirstRounder = new Set<number>();
	for (const dp of draftPicks) {
		if (dp.season === season && dp.round === 1 && dp.tid === dp.originalTid) {
			ownFirstRounder.add(dp.originalTid);
		}
	}

	const chancesByTid = new Map<number, number>();
	let total = 0;
	for (const t of await idb.cache.teams.getAll()) {
		if (
			t.disabled ||
			t.draftLottery?.type !== "cola" ||
			t.draftLottery.optOut ||
			deepPlayoffs.has(t.tid) ||
			!ownFirstRounder.has(t.tid)
		) {
			continue;
		}
		chancesByTid.set(t.tid, t.draftLottery.chances);
		total += t.draftLottery.chances;
	}

	return { chancesByTid, total };
};

// Every AI team's opt-out decision for this year's lottery, taken once, just
// after the season's chances have settled and before the draw. A team the user
// is actually running is never touched - that button is theirs.
export const setAiColaOptOuts = async () => {
	if (g.get("draftType") !== "cola" || !g.get("smartAiFrontOffice")) {
		return;
	}

	const season = g.get("season");
	const [thisClass, nextClass] = await Promise.all([
		classEdge(season, COLA_NUM_LOTTERY_PICKS),
		classEdge(season + 1, COLA_NUM_LOTTERY_PICKS),
	]);
	if (thisClass === undefined || nextClass === undefined) {
		return;
	}

	const { chancesByTid, total } = await colaLotteryField();
	if (total <= 0) {
		return;
	}

	for (const [tid, chances] of chancesByTid) {
		if (!isAiControlled({ tid })) {
			continue;
		}
		if (
			!shouldOptOutOfCola({
				chances,
				total,
				numLotteryPicks: COLA_NUM_LOTTERY_PICKS,
				thisClass,
				nextClass,
			})
		) {
			continue;
		}
		const t = await idb.cache.teams.get(tid);
		if (t?.draftLottery?.type !== "cola") {
			continue;
		}
		t.draftLottery.optOut = true;
		await idb.cache.teams.put(t);
		frontOfficeLog(season, tid, "cola-opt-out", {
			chances,
			thisEdge: Math.round(thisClass.lottery - thisClass.fallback),
			nextEdge: Math.round(nextClass.lottery - nextClass.fallback),
		});
	}
};
