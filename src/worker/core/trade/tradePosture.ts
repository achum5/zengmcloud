import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import { last } from "../../../common/utils.ts";
import teamOvr from "../team/ovr.ts";
import getPayroll from "../team/getPayroll.ts";
import type { LookingFor } from "./makeItWork.ts";

// ---------------------------------------------------------------------------
// TRADE POSTURE — a team's franchise outlook, used to drive STRATEGIC trade
// initiation (a later phase). This module is READ-ONLY and changes no game
// behavior on its own; it just answers "what is this team trying to do?".
//
// The design is deliberately split into small PURE functions (classifyTier,
// analyzePositions, capPosture, …) that take plain numbers/objects and no
// database, so the strategy logic can be unit-tested exhaustively. The
// orchestrators at the bottom (getLeagueTradeContext / getTradePosture) do the
// data-fetching and hand those pure functions their inputs.
//
// Real-life behaviors this is meant to capture:
//   • Contenders convert future assets (picks/prospects) into present talent —
//     a star if they lack one, else a starter/role player at a position of need.
//   • Rebuilders never let a good, ill-fitting veteran waste away — they shop
//     him for youth + picks while he still has value.
//   • Every team sits on a buy/sell spectrum that moves with its record, roster
//     age, and cap — a genuine, dynamic franchise strategy.
// ---------------------------------------------------------------------------

// Where a franchise sits on the buy/sell spectrum.
export type TradeTier = "allIn" | "buyer" | "fringe" | "seller" | "teardown";

// Basketball is bucketed into three broad slots. These strings are also what
// makeItWork's LookingFor expects: it substring-matches player positions, so
// "G" matches PG/SG/G/GF, "F" matches SF/PF/F, "C" matches C/FC.
export type PosBucket = "G" | "F" | "C";

export type PositionNeed = { pos: PosBucket; severity: number };
export type PositionSurplus = { pos: PosBucket; depth: number };

export type CapPosture = {
	payroll: number;
	capSpace: number;
	overCap: boolean;
	overLuxury: boolean;
	underFloor: boolean;
	// Wants to shed money (a non-contender paying the tax).
	wantsRelief: boolean;
	// Can take money back (room under the cap, or under the floor, or no cap).
	canAbsorb: boolean;
};

export type TradePosture = {
	tid: number;
	tier: TradeTier;
	// 0..1 — how aggressively the team will deal (how far past dv=0 it will go
	// and how many assets it will package). Consumed by a later phase.
	aggression: number;
	winp: number;
	ovrRank: number;
	avgAge: number;
	// A would-be contender with no true star — the "we need our guy" flag.
	starGap: boolean;
	needs: PositionNeed[];
	surpluses: PositionSurplus[];
	// Players this team will NOT trade (young cornerstones, and stars unless
	// tearing down).
	buildingBlockPids: number[];
	// Veterans a selling team should actively shop before they waste away.
	shopVeteranPids: number[];
	cap: CapPosture;
	// Expressed for makeItWork.
	lookingFor: LookingFor;
};

// A slimmed player, the only shape the pure functions need.
export type PosturePlayer = {
	pid: number;
	ovr: number;
	pot: number;
	value: number;
	age: number;
	pos: string;
	contractAmount: number;
	contractExp: number;
};

// ---- Pure classification helpers (no DB — unit-tested directly) -------------

const GUARD_POS = new Set(["PG", "SG", "G", "GF"]);
const CENTER_POS = new Set(["C", "FC"]);

// Map a fine-grained position to a broad G/F/C slot. SF/PF/F and anything
// unrecognized fall through to "F" (the forward slot is the safe default).
export const posBucket = (pos: string): PosBucket =>
	GUARD_POS.has(pos) ? "G" : CENTER_POS.has(pos) ? "C" : "F";

// The heart of the "dynamic strategy": where does this team land on the
// buy/sell spectrum? Contention blends current record with team strength; the
// franchise's established `strategy` nudges it, and roster age decides whether a
// strong team is a win-now all-in or a patient buyer keeping its young core.
export const classifyTier = ({
	winp,
	ovrRankPct,
	avgAge,
	youngCoreCount,
	strategy,
}: {
	winp: number; // 0..1
	ovrRankPct: number; // 0 = best team, 1 = worst team
	avgAge: number;
	youngCoreCount: number;
	strategy: string;
}): TradeTier => {
	let contention = 0.6 * winp + 0.4 * (1 - ovrRankPct);

	// Respect the direction the franchise has already committed to, but let a
	// strong enough record/rating override it.
	if (strategy === "contending") {
		contention += 0.05;
	} else if (strategy === "rebuilding") {
		contention -= 0.05;
	}

	if (contention >= 0.62) {
		// A win-now team (aging or star-less-of-youth core) goes all-in; a team
		// this good but still young stays a buyer and protects its young core.
		return avgAge >= 27 || youngCoreCount === 0 ? "allIn" : "buyer";
	}
	if (contention >= 0.5) {
		return "buyer";
	}
	if (contention >= 0.4) {
		return "fringe";
	}
	if (contention >= 0.28) {
		return "seller";
	}
	return "teardown";
};

// Per-slot needs (best player there is below starter caliber) and surpluses
// (two+ starter-caliber players stacked at one slot). starterOvr is the
// league-relative "replacement starter" bar.
export const analyzePositions = (
	players: { pos: string; ovr: number }[],
	starterOvr: number,
): { needs: PositionNeed[]; surpluses: PositionSurplus[] } => {
	const buckets: Record<PosBucket, number[]> = { G: [], F: [], C: [] };
	for (const p of players) {
		buckets[posBucket(p.pos)].push(p.ovr);
	}

	const needs: PositionNeed[] = [];
	const surpluses: PositionSurplus[] = [];
	for (const pos of ["G", "F", "C"] as const) {
		const ovrs = buckets[pos].sort((a, b) => b - a);
		const best = ovrs[0] ?? 0;

		const severity = starterOvr - best;
		if (severity > 0) {
			needs.push({ pos, severity });
		}

		const starters = ovrs.filter((o) => o >= starterOvr).length;
		if (starters >= 2) {
			surpluses.push({ pos, depth: starters - 1 });
		}
	}

	needs.sort((a, b) => b.severity - a.severity);
	surpluses.sort((a, b) => b.depth - a.depth);
	return { needs, surpluses };
};

export const capPosture = ({
	payroll,
	salaryCap,
	luxuryPayroll,
	minPayroll,
	salaryCapType,
	tier,
}: {
	payroll: number;
	salaryCap: number;
	luxuryPayroll: number;
	minPayroll: number;
	salaryCapType: string;
	tier: TradeTier;
}): CapPosture => {
	const capSpace = salaryCap - payroll;
	const overCap = salaryCapType !== "none" && payroll > salaryCap;
	const overLuxury = payroll > luxuryPayroll;
	const underFloor = payroll < minPayroll;
	const selling = tier === "seller" || tier === "teardown";

	return {
		payroll,
		capSpace,
		overCap,
		overLuxury,
		underFloor,
		// A non-contender paying the luxury tax should be cutting money.
		wantsRelief: overLuxury && selling,
		// No cap, room under the cap, or below the spending floor → can take salary.
		canAbsorb: salaryCapType === "none" || capSpace > 0 || underFloor,
	};
};

// Young cornerstones are always protected; true stars are protected unless the
// team is tearing all the way down (where even a star can be cashed in).
export const selectBuildingBlocks = (
	players: PosturePlayer[],
	{
		youngAge,
		coreValue,
		starValue,
		tier,
	}: {
		youngAge: number;
		coreValue: number;
		starValue: number;
		tier: TradeTier;
	},
): number[] => {
	const out: number[] = [];
	for (const p of players) {
		const youngCore = p.age <= youngAge && p.value >= coreValue;
		const star = p.value >= starValue;
		if (youngCore || (star && tier !== "teardown")) {
			out.push(p.pid);
		}
	}
	return out;
};

// The veterans a seller must move before they waste away: real trade value, old
// enough that they won't be part of the next good team, and not a protected
// building block. Returned most-valuable-first (the priority to shop).
export const selectShopVeterans = (
	players: PosturePlayer[],
	buildingBlocks: Set<number>,
	{
		vetAge,
		minTradeValue,
		tier,
	}: {
		vetAge: number;
		minTradeValue: number;
		tier: TradeTier;
	},
): number[] => {
	if (tier !== "seller" && tier !== "teardown") {
		return [];
	}
	return players
		.filter(
			(p) =>
				!buildingBlocks.has(p.pid) &&
				p.age >= vetAge &&
				p.value >= minTradeValue,
		)
		.sort((a, b) => b.value - a.value)
		.map((p) => p.pid);
};

// What the team is shopping FOR, in makeItWork's terms.
export const lookingForFromPosture = (
	tier: TradeTier,
	needs: PositionNeed[],
	starGap: boolean,
): LookingFor => {
	// Sellers chase the future, position-agnostic: youth + picks.
	if (tier === "seller" || tier === "teardown") {
		return {
			positions: new Set(),
			skills: new Set(),
			draftPicks: true,
			prospects: true,
			bestCurrentPlayers: false,
		};
	}

	// An all-in team missing a star hunts the best player available, anywhere.
	if (tier === "allIn" && starGap) {
		return {
			positions: new Set(),
			skills: new Set(),
			draftPicks: false,
			prospects: false,
			bestCurrentPlayers: true,
		};
	}

	// Otherwise, proven talent at the two most pressing positions of need.
	const positions = new Set<string>();
	for (const need of needs.slice(0, 2)) {
		positions.add(need.pos);
	}
	return {
		positions,
		skills: new Set(),
		draftPicks: false,
		prospects: false,
		bestCurrentPlayers: true,
	};
};

const AGGRESSION: Record<TradeTier, number> = {
	allIn: 0.9,
	buyer: 0.6,
	fringe: 0.35,
	seller: 0.7,
	teardown: 0.95,
};

// A value at a given rank in a descending list, or a fallback when the league
// is too small to have that many players.
const atRankDesc = (sortedDesc: number[], rank: number, fallback: number) => {
	const idx = Math.min(rank, sortedDesc.length) - 1;
	return idx >= 0 && sortedDesc[idx] !== undefined ? sortedDesc[idx]! : fallback;
};

// 0.25..0.75 estimated win% purely from team-strength rank (same shape the
// pick-value code uses), for blending in before enough games are played.
const ovrRankToWinp = (rankPct: number) => 0.75 - 0.5 * rankPct;

// ---- Orchestrators (DB) -----------------------------------------------------

export type LeagueTradeContext = {
	numActiveTeams: number;
	// League-relative bars, derived from every rostered player.
	starterOvr: number;
	rotationOvr: number;
	starValue: number;
	coreValue: number;
	// Team strength ranking (best first).
	teamOvrsSorted: { tid: number; ovr: number }[];
	// Cap settings snapshot.
	salaryCap: number;
	luxuryPayroll: number;
	minPayroll: number;
	salaryCapType: string;
};

// Compute the league-wide reference points ONCE, so scoring every team is cheap
// and consistent.
export const getLeagueTradeContext = async (): Promise<LeagueTradeContext> => {
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const numActiveTeams = teams.length || g.get("numActiveTeams");

	const allPlayers = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);

	const ovrs: number[] = [];
	const values: number[] = [];
	const byTid = new Map<number, typeof allPlayers>();
	for (const p of allPlayers) {
		ovrs.push(last(p.ratings).ovr);
		values.push(p.value);
		const arr = byTid.get(p.tid);
		if (arr) {
			arr.push(p);
		} else {
			byTid.set(p.tid, [p]);
		}
	}
	ovrs.sort((a, b) => b - a);
	values.sort((a, b) => b - a);

	// Replacement starter ≈ the (5 × teams)th best player; rotation ≈ 8th man.
	const starterOvr = atRankDesc(ovrs, numActiveTeams * 5, 45);
	const rotationOvr = atRankDesc(ovrs, numActiveTeams * 8, 40);
	// Star ≈ the best player on an average team; "core" ≈ a quality starter.
	const starValue = Math.max(60, atRankDesc(values, numActiveTeams, 65));
	const coreValue = Math.max(52, atRankDesc(values, numActiveTeams * 3, 55));

	const teamOvrsSorted = teams
		.map((t) => {
			const players = byTid.get(t.tid) ?? [];
			const ovr = teamOvr(
				players.map((p) => ({
					pid: p.pid,
					injury: p.injury,
					value: p.value,
					ratings: {
						ovr: last(p.ratings).ovr,
						ovrs: last(p.ratings).ovrs,
						pos: last(p.ratings).pos,
					},
				})),
			);
			return { tid: t.tid, ovr };
		})
		.sort((a, b) => b.ovr - a.ovr);

	return {
		numActiveTeams,
		starterOvr,
		rotationOvr,
		starValue,
		coreValue,
		teamOvrsSorted,
		salaryCap: g.get("salaryCap"),
		luxuryPayroll: g.get("luxuryPayroll"),
		minPayroll: g.get("minPayroll"),
		salaryCapType: g.get("salaryCapType"),
	};
};

// The full posture for one team, given a precomputed league context.
export const getTradePosture = async (
	tid: number,
	context: LeagueTradeContext,
): Promise<TradePosture> => {
	const season = g.get("season");
	const rawPlayers = await idb.cache.players.indexGetAll("playersByTid", tid);

	const players: PosturePlayer[] = rawPlayers.map((p) => ({
		pid: p.pid,
		ovr: last(p.ratings).ovr,
		pot: last(p.ratings).pot,
		value: p.value,
		age: season - p.born.year,
		pos: last(p.ratings).pos,
		contractAmount: p.contract.amount,
		contractExp: p.contract.exp,
	}));

	// Team strength rank (0 best … 1 worst).
	const rankIdx = context.teamOvrsSorted.findIndex((t) => t.tid === tid);
	const ovrRank = rankIdx < 0 ? context.teamOvrsSorted.length : rankIdx;
	const ovrRankPct =
		context.numActiveTeams > 1
			? Math.min(1, ovrRank / (context.numActiveTeams - 1))
			: 0;

	// Win% — actual record once enough games are in, blended with the
	// strength-implied win% early in the season.
	const teamSeason = await idb.cache.teamSeasons.indexGet(
		"teamSeasonsBySeasonTid",
		[season, tid],
	);
	const won = teamSeason?.won ?? 0;
	const lost = teamSeason?.lost ?? 0;
	const gp = won + lost;
	const impliedWinp = ovrRankToWinp(ovrRankPct);
	const RAMP = 20; // games until we trust the record fully
	const winp =
		gp <= 0
			? impliedWinp
			: (gp >= RAMP ? 1 : gp / RAMP) * (won / gp) +
				(gp >= RAMP ? 0 : 1 - gp / RAMP) * impliedWinp;

	// Value-weighted average age, so the CORE's age drives the timeline, not deep
	// bench youth. Falls back to a plain mean if there's no positive value.
	let ageNum = 0;
	let ageDen = 0;
	let plainAgeNum = 0;
	for (const p of players) {
		const w = Math.max(0, p.value);
		ageNum += p.age * w;
		ageDen += w;
		plainAgeNum += p.age;
	}
	const avgAge =
		ageDen > 0
			? ageNum / ageDen
			: players.length > 0
				? plainAgeNum / players.length
				: 25;

	const YOUNG_AGE = 24;
	const VET_AGE = 29;
	const youngCoreCount = players.filter(
		(p) => p.age <= YOUNG_AGE && p.value >= context.coreValue,
	).length;

	const strategy = (await idb.cache.teams.get(tid))?.strategy ?? "";

	const tier = classifyTier({
		winp,
		ovrRankPct,
		avgAge,
		youngCoreCount,
		strategy,
	});

	const { needs, surpluses } = isSport("basketball")
		? analyzePositions(
				players.map((p) => ({ pos: p.pos, ovr: p.ovr })),
				context.starterOvr,
			)
		: { needs: [], surpluses: [] };

	const starGap =
		(tier === "allIn" || tier === "buyer") &&
		!players.some((p) => p.value >= context.starValue);

	const buildingBlockPids = selectBuildingBlocks(players, {
		youngAge: YOUNG_AGE,
		coreValue: context.coreValue,
		starValue: context.starValue,
		tier,
	});
	const shopVeteranPids = selectShopVeterans(
		players,
		new Set(buildingBlockPids),
		{
			vetAge: VET_AGE,
			minTradeValue: context.rotationOvr, // "someone wants him" bar
			tier,
		},
	);

	const payroll = await getPayroll(tid);
	const cap = capPosture({
		payroll,
		salaryCap: context.salaryCap,
		luxuryPayroll: context.luxuryPayroll,
		minPayroll: context.minPayroll,
		salaryCapType: context.salaryCapType,
		tier,
	});

	return {
		tid,
		tier,
		aggression: AGGRESSION[tier],
		winp,
		ovrRank: ovrRank + 1,
		avgAge,
		starGap,
		needs,
		surpluses,
		buildingBlockPids,
		shopVeteranPids,
		cap,
		lookingFor: lookingForFromPosture(tier, needs, starGap),
	};
};

// Every non-disabled team's posture, for inspection / auditing. Read-only.
export const getTradePostureReport = async (): Promise<TradePosture[]> => {
	const context = await getLeagueTradeContext();
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const out: TradePosture[] = [];
	for (const t of teams) {
		out.push(await getTradePosture(t.tid, context));
	}
	return out;
};
