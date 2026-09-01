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

// Map our five-level franchise tier onto the trade-VALUATION's coarser notion
// of strategy (contending discounts youth/picks; rebuilding boosts them + cap
// relief; "" is neutral). This is what lets the pricing engine value assets from
// our posture instead of BBGM's own contending/rebuilding flag.
export const tierToStrategy = (tier: TradeTier): string => {
	if (tier === "allIn" || tier === "buyer") {
		return "contending";
	}
	if (tier === "seller" || tier === "teardown") {
		return "rebuilding";
	}
	return ""; // fringe → neutral
};

// Basketball is bucketed into three broad slots. These strings are also what
// makeItWork's LookingFor expects: it substring-matches player positions, so
// "G" matches PG/SG/G/GF, "F" matches SF/PF/F, "C" matches C/FC.
export type PosBucket = "G" | "F" | "C";

export type PositionNeed = { pos: PosBucket; severity: number };
export type PositionSurplus = { pos: PosBucket; depth: number };

// WHAT MONEY MEANS TO AN AI TEAM, WHICH IS LESS THAN IT LOOKS.
//
// An AI franchise has no budget and takes no penalty for the luxury tax: the
// cash comes off a balance sheet nothing reads back, no owner fires anybody,
// and the departments are set from market size and posture rather than from
// what is in the bank (see finances/smartBudget.ts). So the only money that
// can change a decision here is the SALARY CAP, and only because it is a
// RULE - room is what decides who may sign a free agent outright and who may
// take salary back in a trade.
//
// `overLuxury` is therefore ADVISORY: it is read by the franchise-outlook
// view, which shows a human what its team looks like, and by nothing that
// decides anything. That is deliberate and not an oversight to be tidied up.
// A rebuilding team over the tax line is not making a mistake; it is spending
// money that does not exist to spend, and talking it out of a player to save
// that money would cost basketball and buy nothing. The same reasoning is
// written out at the re-signing bid ceiling in phase/newPhaseResignPlayers.ts,
// which is where it bites hardest.
export type CapPosture = {
	payroll: number;
	capSpace: number;
	overCap: boolean;
	// See above: shown to humans, never acted on.
	overLuxury: boolean;
	underFloor: boolean;
	// Can take money back (room under the cap, or under the floor, or no cap).
	canAbsorb: boolean;
};

export type TradePosture = {
	tid: number;
	tier: TradeTier;
	// 0..1 — how aggressively the team will deal (how far past dv=0 it will go
	// and how many assets it will package). Consumed by a later phase.
	aggression: number;
	// A top-of-the-league roster (within ELITE_OVR_GAP team-OVR of the best team):
	// a guaranteed, uber-aggressive buyer that hunts a star every year.
	elite: boolean;
	// The raw signals behind the tier, exposed for transparency / diagnostics.
	winp: number;
	ovrRank: number;
	ovrRankPct: number;
	contention: number;
	avgAge: number;
	youngCoreCount: number;
	// A would-be contender with no true star — the "we need our guy" flag.
	starGap: boolean;
	needs: PositionNeed[];
	surpluses: PositionSurplus[];
	// The slot a buyer/contender should upgrade (needs first, else weakest slot).
	targetPos?: PosBucket;
	// Players this team will NOT trade (young cornerstones, and stars unless
	// tearing down).
	buildingBlockPids: number[];
	// Veterans a selling team should actively shop before they waste away.
	shopVeteranPids: number[];
	// Does this team have a genuine star it would actually part with (not a
	// protected building block)? Star-hunting contenders seek these teams out.
	shoppableStar: boolean;
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
	// Games he is still out injured (0 when healthy).
	gamesMissing: number;
};

// ---- Pure classification helpers (no DB — unit-tested directly) -------------

const GUARD_POS = new Set(["PG", "SG", "G"]);
const BIG_POS = new Set(["PF", "FC", "C"]);

// Map a fine-grained position to a broad G/F/C slot. Guards are PG/SG/G; bigs
// (the "C" slot) are PF/FC/C — a power forward is a frontcourt body, so lumping
// it with wings made every team look short a big. Wings (SF/F/GF) and anything
// unrecognized fall through to "F".
export const posBucket = (pos: string): PosBucket =>
	GUARD_POS.has(pos) ? "G" : BIG_POS.has(pos) ? "C" : "F";

// The heart of the "dynamic strategy": where does this team land on the
// buy/sell spectrum? Contention blends current record with team strength; the
// franchise's established `strategy` nudges it, and roster age decides whether a
// strong team is a win-now all-in or a patient buyer keeping its young core.
// How "ready to win now" a team is, 0..1. Record is what really says "we're
// contending"; team strength is a secondary signal (and the early-season
// stand-in, blended in upstream). This is derived ENTIRELY from our own signals
// — BBGM's own "contending/rebuilding" flag is deliberately ignored, because it
// lags reality (a 41-24 team flagged "rebuilding" is nonsense).
export const contentionScore = ({
	winp,
	ovrRankPct,
}: {
	winp: number;
	ovrRankPct: number;
}): number => 0.75 * winp + 0.25 * (1 - ovrRankPct);

// A roster within this many team-OVR points of the league's very best team is
// "elite" — a guaranteed, uber-aggressive buyer that goes star-hunting every
// year (see getTradePosture). Judged on roster strength, not record, so a loaded
// team off to a slow start still buys hard.
export const ELITE_OVR_GAP = 5;

export const isEliteByOvr = (teamOvr: number, topTeamOvr: number): boolean =>
	teamOvr >= topTeamOvr - ELITE_OVR_GAP;

export const classifyTier = ({
	winp,
	ovrRankPct,
	avgAge,
	youngCoreCount,
	hasFoundation,
}: {
	winp: number; // 0..1
	ovrRankPct: number; // 0 = best team, 1 = worst team
	avgAge: number;
	youngCoreCount: number;
	// Does the team have a young cornerstone (or young core) to build around?
	hasFoundation: boolean;
}): TradeTier => {
	const contention = contentionScore({ winp, ovrRankPct });

	// All-in is reserved for genuine title threats: a strong RECORD, not just a
	// strong roster on paper. A win-now core (aging, or no young building blocks)
	// goes all-in; a team this good but still young stays a buyer that protects
	// its young core.
	if (contention >= 0.62 && winp >= 0.55) {
		return avgAge >= 27 || youngCoreCount === 0 ? "allIn" : "buyer";
	}
	if (contention >= 0.5) {
		return "buyer";
	}
	if (contention >= 0.4) {
		return "fringe";
	}

	// Not competitive → selling. A team with a young cornerstone RETOOLS around it
	// (seller); a genuinely hopeless team with nothing to build around fully tears
	// down — commits to the future and shops its whole win-now roster. So a 19-63
	// team with a franchise point guard is a seller (keeps him, cashes in everyone
	// else), but a truly bad team with no young foundation goes all the way. The
	// bar (0.31, ≈ a 26-win-or-worse no-core team) captures the genuinely hopeless
	// without dragging in the merely below-average, which stays a measured seller.
	if (contention < 0.31 && !hasFoundation) {
		return "teardown";
	}
	return "seller";
};

// ---- The tier, as one pure read of a roster --------------------------------
//
// Extracted because it has two callers and they must never disagree. The other
// is trade VALUATION (team/ValueChangeCalculator.ts), which used to run off
// BBGM's own two-value `strategy` flag - so an AI could be planning a teardown
// while pricing your offer as a contender, and the flag itself is only
// refreshed once a year, in the offseason. Both of those are exactly the lag
// this module was written to replace.
//
// Pure, and takes the league bars rather than fetching them, so a caller that
// already has every player in hand (the valuation cache does) pays nothing
// extra to ask.

// Genuine youth: what drives the young-core signal in the tier.
export const YOUNG_AGE = 24;

// A young cornerstone can be this old - past YOUNG_AGE, but still young enough
// that a bad team builds around him rather than shopping him.
const FOUNDATION_AGE = 26;

// Value-weighted, so the CORE's age drives the timeline rather than deep bench
// youth. Falls back to a plain mean when nothing has positive value.
export const valueWeightedAge = (
	players: readonly { age: number; value: number }[],
): number => {
	let num = 0;
	let den = 0;
	let plainNum = 0;
	for (const p of players) {
		const w = Math.max(0, p.value);
		num += p.age * w;
		den += w;
		plainNum += p.age;
	}
	if (den > 0) {
		return num / den;
	}
	return players.length > 0 ? plainNum / players.length : 25;
};

export const tierFromRoster = ({
	players,
	winp,
	ovrRankPct,
	coreValue,
	starValue,
	teamOvr,
	topTeamOvr,
}: {
	players: readonly { age: number; value: number }[];
	winp: number;
	ovrRankPct: number;
	coreValue: number;
	starValue: number;
	// Undefined when this team is not in the league's OVR ranking at all, in
	// which case it cannot be elite.
	teamOvr: number | undefined;
	topTeamOvr: number;
}): {
	tier: TradeTier;
	avgAge: number;
	youngCoreCount: number;
	hasFoundation: boolean;
} => {
	const avgAge = valueWeightedAge(players);
	const youngCoreCount = players.filter(
		(p) => p.age <= YOUNG_AGE && p.value >= coreValue,
	).length;

	// A young cornerstone to build around: a young-and-good star, or a couple of
	// young quality players. Decides retool (seller) vs full teardown for a bad
	// team.
	const hasFoundation =
		youngCoreCount >= 2 ||
		players.some((p) => p.age <= FOUNDATION_AGE && p.value >= starValue);

	let tier = classifyTier({
		winp,
		ovrRankPct,
		avgAge,
		youngCoreCount,
		hasFoundation,
	});

	// A roster right at the top of the league is an UBER-aggressive buyer,
	// guaranteed - a title-caliber team should go get a star every year. This
	// overrides a soft, record-based read: a loaded roster off to a slow start
	// still BUYS, never sells, which would be self-sabotage.
	if (
		teamOvr !== undefined &&
		teamOvr >= topTeamOvr - ELITE_OVR_GAP &&
		tier !== "allIn"
	) {
		tier = "buyer";
	}

	return { tier, avgAge, youngCoreCount, hasFoundation };
};

// Per-slot needs (best player there is below starter caliber) and surpluses
// (two+ starter-caliber players stacked at one slot). starterOvr is the
// league-relative "replacement starter" bar.
// A position whose best player is only a replacement-level starter (this far
// above the starter bar or worse) is a soft spot worth upgrading. Above it, the
// slot is solid and not worth chasing.
const SOFT_UPGRADE_MARGIN = 6;

// A player out for a long stretch does not fill his position TODAY, which is
// the question needs and surpluses answer - free agency consults them daily,
// and a team whose only centre just went down for two months should read as
// having a hole at centre while he is out, exactly like a front office signing
// a stopgap. Short absences are ridden out on existing depth. Injuries heal
// over the offseason (newPhaseBeforeDraft), so a lingering hole here is a
// genuine one carrying into the season, not noise.
export const LONG_INJURY_GAMES = 10;

export const analyzePositions = (
	players: { pos: string; ovr: number; gamesMissing?: number }[],
	starterOvr: number,
): {
	needs: PositionNeed[];
	surpluses: PositionSurplus[];
	// A slot to UPGRADE when there's no outright hole: the weakest NON-surplus
	// position whose best player is only replacement-level. Undefined when the
	// team is solid/deep everywhere (then it just wants the best player available,
	// not a specific position).
	upgradePos?: PosBucket;
} => {
	const buckets: Record<PosBucket, number[]> = { G: [], F: [], C: [] };
	for (const p of players) {
		if ((p.gamesMissing ?? 0) >= LONG_INJURY_GAMES) {
			continue;
		}
		buckets[posBucket(p.pos)].push(p.ovr);
	}

	const needs: PositionNeed[] = [];
	const surpluses: PositionSurplus[] = [];
	const bestByPos: Record<PosBucket, number> = { G: 0, F: 0, C: 0 };
	for (const pos of ["G", "F", "C"] as const) {
		const ovrs = buckets[pos].sort((a, b) => b - a);
		const best = ovrs[0] ?? 0;
		bestByPos[pos] = best;

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

	// The soft upgrade target: weakest slot that isn't already a surplus and isn't
	// already solid. Never a position the team is deep at.
	const surplusPositions = new Set(surpluses.map((s) => s.pos));
	let upgradePos: PosBucket | undefined;
	let upgradeBest = Infinity;
	for (const pos of ["G", "F", "C"] as const) {
		if (surplusPositions.has(pos)) {
			continue;
		}
		const best = bestByPos[pos];
		if (best < starterOvr + SOFT_UPGRADE_MARGIN && best < upgradeBest) {
			upgradeBest = best;
			upgradePos = pos;
		}
	}

	return { needs, surpluses, upgradePos };
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
	return {
		payroll,
		capSpace,
		overCap,
		overLuxury,
		underFloor,
		// No cap, room under the cap, or below the spending floor → can take salary.
		canAbsorb: salaryCapType === "none" || capSpace > 0 || underFloor,
	};
};

// Who a team won't trade. Young cornerstones are always protected. A team that
// is NOT selling also keeps its quality players (its core rotation) — so a real
// contender protects its best guys, not just its youth. A selling team leaves
// its veterans available (only its young future pieces are off-limits), which
// is exactly how it avoids letting a good vet waste away.
//
// A REBUILD KEEPS THE BEST PLAYERS IT HAS, not only the ones the league would
// call good. coreValue is league-relative - about the ninetieth best player in
// a thirty-team league - and a team bad enough to be tearing down frequently
// has nobody at all who clears it. That team protected NOBODY, so
// selectShopVeterans put its entire roster over twenty-five on the block: the
// season a twenty-four-year-old it had drafted turned twenty-six, he was
// shopped, along with everyone else it had spent the rebuild collecting. A
// rebuild run that way can never assemble the core it exists to assemble, and
// the only ones that escaped were the ones lucky enough to draft a player good
// enough for the LEAGUE bar to protect.
//
// Measured over six twelve-season leagues before this: rebuilds that got out
// and rebuilds that never did held the same number of first-round picks (3.8
// against 3.7). What separated them was the young core - 2.4 players against
// 1.8, best young player 59.0 against 56.9. Stockpiling picks is not what ends
// a rebuild; keeping the players is.
//
// The re-signing code found the same thing first and says it in the same
// words: "star" is roughly the best player on an AVERAGE team, so the worst
// clubs have nobody who qualifies and would otherwise liquidate the rotation
// they have to rebuild around. This is that fix, on the trade side.
export const REBUILD_CORE_RANK = 3;

export const selectBuildingBlocks = (
	players: PosturePlayer[],
	{
		coreAge,
		coreValue,
		starValue,
		tier,
	}: {
		coreAge: number;
		coreValue: number;
		starValue: number;
		tier: TradeTier;
	},
): number[] => {
	// The team's own best young players, for the rebuild rule below. Ties break
	// on pid so two devices in a shared league protect the same men.
	const rebuildCore = new Set<number>();
	if (tier === "seller" || tier === "teardown") {
		for (const p of players
			.filter((p2) => p2.age <= coreAge)
			.sort((a, b) => b.value - a.value || a.pid - b.pid)
			.slice(0, REBUILD_CORE_RANK)) {
			rebuildCore.add(p.pid);
		}
	}

	const out: number[] = [];
	for (const p of players) {
		let protect: boolean;
		if (tier === "allIn") {
			// Win-now: only genuine stars are untouchable. Good young players are
			// trade chips to package (with picks) for a present-day upgrade — an
			// aging contender mortgages the future.
			protect = p.value >= starValue;
		} else if (tier === "seller" || tier === "teardown") {
			// Rebuild: keep the young/prime core to build around, cash in the rest.
			// League-good, or the best this team has - see above.
			protect =
				p.age <= coreAge && (p.value >= coreValue || rebuildCore.has(p.pid));
		} else {
			// buyer / fringe: keep every quality player. A young, rising team hoards
			// its core and only adds complementary pieces.
			protect = p.value >= coreValue;
		}
		if (protect) {
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
		teardownAge,
		minTradeValue,
		tier,
	}: {
		vetAge: number;
		teardownAge: number;
		minTradeValue: number;
		tier: TradeTier;
	},
): number[] => {
	if (tier !== "seller" && tier !== "teardown") {
		return [];
	}
	// A full teardown moves anyone past his early 20s who isn't a building block
	// (a genuine fire sale); a milder sell only cashes in clear veterans.
	const minAge = tier === "teardown" ? teardownAge : vetAge;
	return players
		.filter(
			(p) =>
				!buildingBlocks.has(p.pid) &&
				p.age >= minAge &&
				p.value >= minTradeValue,
		)
		.sort((a, b) => b.value - a.value)
		.map((p) => p.pid);
};

// What the team is shopping FOR, in makeItWork's terms. targetPos is the slot to
// upgrade when there's no outright hole, so a contender always has a direction.
export const lookingForFromPosture = (
	tier: TradeTier,
	needs: PositionNeed[],
	starGap: boolean,
	targetPos?: PosBucket,
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

	// Proven talent at the biggest needs, or — failing an outright hole — at the
	// weakest slot to upgrade.
	const positions = new Set<string>();
	for (const need of needs.slice(0, 2)) {
		positions.add(need.pos);
	}
	if (positions.size === 0 && targetPos) {
		positions.add(targetPos);
	}
	return {
		positions,
		skills: new Set(),
		draftPicks: false,
		prospects: false,
		bestCurrentPlayers: true,
	};
};

// How reliably a team takes its business to the market (initiation frequency).
// The two poles are near-certain: a win-now contender is always shopping for the
// upgrade that wins it a title, and a hopeless team is always shopping its
// win-now pieces for the future. Buyers (young contenders) are firmly in the
// market too. Only fringe teams are genuinely wishy-washy. NOTE: this is purely
// how OFTEN a team engages — every resulting deal still clears the same fairness
// bounds, so more conviction never means worse trades.
const AGGRESSION: Record<TradeTier, number> = {
	allIn: 0.95,
	buyer: 0.78,
	fringe: 0.35,
	seller: 0.75,
	teardown: 0.98,
};

// A value at a given rank in a descending list, or a fallback when the league
// is too small to have that many players.
const atRankDesc = (sortedDesc: number[], rank: number, fallback: number) => {
	const idx = Math.min(rank, sortedDesc.length) - 1;
	return idx >= 0 && sortedDesc[idx] !== undefined
		? sortedDesc[idx]!
		: fallback;
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
	// A genuine star's OVR — value is too compressed to tell stars apart, so the
	// star gap is judged on OVR instead.
	starOvr: number;
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
	// Star ≈ roughly the best player on an average team, by OVR.
	const starOvr = atRankDesc(ovrs, numActiveTeams, 65);
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
		starOvr,
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
		gamesMissing: p.injury.gamesRemaining,
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

	const CORE_AGE = 27; // any selling team keeps quality up through its prime
	const VET_AGE = 29; // a mild seller only cashes in clear veterans
	const TEARDOWN_SHOP_AGE = 25; // a teardown moves anyone past his early 20s

	const topTeamOvr = context.teamOvrsSorted[0]?.ovr ?? 0;
	const rankedOvr =
		rankIdx >= 0 ? (context.teamOvrsSorted[rankIdx]?.ovr ?? 0) : undefined;

	// One shared read of the roster - see tierFromRoster. Trade VALUATION asks
	// the same question of the same function, so a team cannot plan a teardown
	// and price your offer as a contender.
	const { tier, avgAge, youngCoreCount } = tierFromRoster({
		players,
		winp,
		ovrRankPct,
		coreValue: context.coreValue,
		starValue: context.starValue,
		teamOvr: rankedOvr,
		topTeamOvr,
	});

	// A roster right at the top of the league (within ELITE_OVR_GAP team-OVR of the
	// very best team) is an UBER-aggressive buyer, guaranteed. A title-caliber team
	// should go get a star every year — it's shocking if it doesn't. This overrides
	// a soft, record-based read: a loaded roster off to a slow start still BUYS,
	// never sells (that would be self-sabotage). Elite teams also initiate far more
	// often (see betweenAiTeams) and, when already contending, keep their allIn
	// urgency. Decision-making only — valuation is untouched.
	const elite =
		rankedOvr !== undefined && rankedOvr >= topTeamOvr - ELITE_OVR_GAP;

	const { needs, surpluses, upgradePos } = isSport("basketball")
		? analyzePositions(
				players.map((p) => ({ pos: p.pos, ovr: p.ovr })),
				context.starterOvr,
			)
		: { needs: [], surpluses: [], upgradePos: undefined };

	// A buyer's target: an outright hole if it has one, else a soft slot to
	// upgrade (undefined when it's solid everywhere → best player available).
	const targetPos = needs[0]?.pos ?? upgradePos;

	// Star gap judged on OVR (value is too compressed to separate a genuine star
	// from a good starter).
	const bestOvr = players.reduce((max, p) => Math.max(max, p.ovr), 0);
	const starGap =
		(tier === "allIn" || tier === "buyer") && bestOvr < context.starOvr;

	const buildingBlockPids = selectBuildingBlocks(players, {
		coreAge: CORE_AGE,
		coreValue: context.coreValue,
		starValue: context.starValue,
		tier,
	});
	const shopVeteranPids = selectShopVeterans(
		players,
		new Set(buildingBlockPids),
		{
			vetAge: VET_AGE,
			teardownAge: TEARDOWN_SHOP_AGE,
			// The "someone wants him" bar. This is an OVR being compared against
			// a VALUE, which is not a mistake left standing out of laziness: it
			// was found, corrected, measured and put back.
			//
			// The two scales are close but not the same, and they separate as
			// you go down. In a thirty-team league the 240th best player is 49
			// by OVR and 52.4 by value (SCALES, in decadesSim.test.ts), so this
			// bar sits about three points below the eighth-man line it names and
			// admits perhaps a hundred more players a season than it reads like
			// it does.
			//
			// Raising it to the honest value-scale line was worse on six seeds:
			// dead money up 9% on all six, rotation talent down 0.12 on five,
			// mean team ovr down 0.63 on five. The mechanism is the fallback in
			// betweenAiTeams - a seller whose block comes up EMPTY offers from
			// its whole roster, so tightening this bar does not make teams shop
			// less, it makes them shop worse. Fix that first if this is ever
			// revisited; on its own the honest bar is a regression.
			minTradeValue: context.rotationOvr,
			tier,
		},
	);

	// A star this team would consider moving (usually an aging star on a seller)
	// — the supply side of a blockbuster. Deliberately looser than the block
	// itself: this is "worth a phone call about", not "already on the table".
	//
	// Tightening it to shopVeteranPids was tried and measured worse on every
	// seed of a twenty-season run — two points of league quality and more stars
	// left unemployed — because both uses of this flag are about WHO TO CALL,
	// and the wider net found more real deals than the false advertisements
	// cost. (The gap is real: a seller stops protecting a player at CORE_AGE
	// but only starts shopping him at VET_AGE, so a 28-year-old star reads as
	// available here while his own team would not lead with him. It just turns
	// out to be a good lead anyway.)
	const blockSet = new Set(buildingBlockPids);
	const shoppableStar = players.some(
		(p) => !blockSet.has(p.pid) && p.ovr >= context.starOvr,
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
		// Elite rosters are near-certain to engage; otherwise the tier baseline.
		aggression: elite ? Math.max(AGGRESSION[tier], 0.97) : AGGRESSION[tier],
		elite,
		winp,
		ovrRank: ovrRank + 1,
		ovrRankPct,
		contention: contentionScore({ winp, ovrRankPct }),
		avgAge,
		youngCoreCount,
		starGap,
		needs,
		surpluses,
		targetPos,
		buildingBlockPids,
		shopVeteranPids,
		shoppableStar,
		cap,
		lookingFor: lookingForFromPosture(tier, needs, starGap, targetPos),
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
