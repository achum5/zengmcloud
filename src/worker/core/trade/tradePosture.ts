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
	strategy: string;
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

// Per-slot needs (best player there is below starter caliber) and surpluses
// (two+ starter-caliber players stacked at one slot). starterOvr is the
// league-relative "replacement starter" bar.
// A position whose best player is only a replacement-level starter (this far
// above the starter bar or worse) is a soft spot worth upgrading. Above it, the
// slot is solid and not worth chasing.
const SOFT_UPGRADE_MARGIN = 6;

export const analyzePositions = (
	players: { pos: string; ovr: number }[],
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

// Who a team won't trade. Young cornerstones are always protected. A team that
// is NOT selling also keeps its quality players (its core rotation) — so a real
// contender protects its best guys, not just its youth. A selling team leaves
// its veterans available (only its young future pieces are off-limits), which
// is exactly how it avoids letting a good vet waste away.
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
			protect = p.value >= coreValue && p.age <= coreAge;
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

	const YOUNG_AGE = 24; // genuine youth (drives the tier's young-core signal)
	const CORE_AGE = 27; // any selling team keeps quality up through its prime
	const VET_AGE = 29; // a mild seller only cashes in clear veterans
	const TEARDOWN_SHOP_AGE = 25; // a teardown moves anyone past his early 20s
	const youngCoreCount = players.filter(
		(p) => p.age <= YOUNG_AGE && p.value >= context.coreValue,
	).length;

	// A young cornerstone to build around: a young-and-good star, or a couple of
	// young quality players. Decides retool (seller) vs full teardown for a bad
	// team.
	const hasFoundation =
		youngCoreCount >= 2 ||
		players.some((p) => p.age <= 26 && p.value >= context.starValue);

	// BBGM's own strategy flag is read only for reference in the diagnostics — our
	// classification deliberately ignores it (see contentionScore).
	const strategy = (await idb.cache.teams.get(tid))?.strategy ?? "";

	let tier = classifyTier({
		winp,
		ovrRankPct,
		avgAge,
		youngCoreCount,
		hasFoundation,
	});

	// A roster right at the top of the league (within ELITE_OVR_GAP team-OVR of the
	// very best team) is an UBER-aggressive buyer, guaranteed. A title-caliber team
	// should go get a star every year — it's shocking if it doesn't. This overrides
	// a soft, record-based read: a loaded roster off to a slow start still BUYS,
	// never sells (that would be self-sabotage). Elite teams also initiate far more
	// often (see betweenAiTeams) and, when already contending, keep their allIn
	// urgency. Decision-making only — valuation is untouched.
	const topTeamOvr = context.teamOvrsSorted[0]?.ovr ?? 0;
	const teamOvr =
		rankIdx >= 0 ? (context.teamOvrsSorted[rankIdx]?.ovr ?? 0) : 0;
	const elite = rankIdx >= 0 && teamOvr >= topTeamOvr - ELITE_OVR_GAP;
	if (elite && tier !== "allIn") {
		tier = "buyer";
	}

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
			minTradeValue: context.rotationOvr, // "someone wants him" bar
			tier,
		},
	);

	// A star this team would actually move (usually an aging star on a seller) —
	// the supply side of a blockbuster.
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
		strategy,
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
