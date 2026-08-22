import { ageFitMultiplier } from "../freeAgents/frontOffice.ts";
import {
	posBucket,
	type PosBucket,
	type TradePosture,
} from "../trade/tradePosture.ts";

// ---------------------------------------------------------------------------
// WHO A FRONT OFFICE LETS GO
//
// Cuts were the last roster decision nothing thought about. In basketball
// dropPlayers sorted by raw `value` and released from the bottom, with the
// positional protection other sports get explicitly turned off. Two things
// followed, and both are visible on a roster page:
//
//   - A REBUILDING TEAM CUT ITS 20-YEAR-OLD. A project's current value is low
//     by definition - that is what makes him a project - so the team whose
//     entire plan is young players released the youngest one and kept a
//     33-year-old with the same value. The mirror image happened at contenders,
//     which kept the project and released the veteran who could actually play.
//   - A TEAM COULD CUT ITS ONLY CENTRE. Nothing counted positions, and team
//     overall punishes a roster that cannot field one.
//
// So cuts run off the same posture as everything else. The ordering is still
// anchored on value - this decides between comparable players, it does not
// keep a bad one - and the age lean is the one already written and tested for
// free agency, so a team does not want a player in July and cut him in October.
//
// Pure - no database - so it is unit-testable on its own.
// ---------------------------------------------------------------------------

export type CutCandidate = {
	pid: number;
	value: number;
	age: number;
	pos: string;
	// What is still owed to him, and through when. Optional so a caller that
	// only has ratings in hand still gets the old ordering.
	contractAmount?: number;
	contractExp?: number;
};

// Below this many at a position bucket, a player is the thing keeping the
// roster legal rather than a body at the end of the bench. Five on the floor
// across three buckets means two apiece is thin and one is a hole.
export const SCARCE_AT_POSITION = 2;

// How much being one of the last players at a position protects him. Big
// enough to save a genuinely scarce player from a marginal one, small enough
// that a replacement-level body cannot survive on his position alone.
export const SCARCITY_PROTECTION = 1.35;

// HOW FAR AGE MAY MOVE A CUT, and it is not as far as the free-agency lean.
//
// keepScore used to apply ageFitMultiplier raw, which runs from 1.25 down to
// 0.55 for a selling team. That is a 2.3x swing on a player's value, so a
// seller cut a 60-value 31-year-old (33) ahead of a 35-value 23-year-old
// (43.75) - it inverted a twenty-five point talent gap, which is not
// "deciding between comparable players" at all.
//
// So the lean is clamped. Two players within fifteen percent of each other are
// separated by what the team is trying to do; a clearly better player is kept
// either way.
export const KEEP_AGE_FLOOR = 0.85;
export const KEEP_AGE_CEILING = 1.15;

// WHAT THE CUT COSTS, which nothing here used to ask.
//
// Releasing a player does not release his money: the remaining guaranteed
// years stay on the payroll as dead money, paying nobody to play. So between
// two comparable players a front office lets go of the one who is nearly off
// the books, not the one with three years left - cutting the expensive man
// converts live salary, which can still be played or traded, into a hole in
// the cap for as long as the deal runs. An expiring contract is free to cut by
// this measure, which is exactly why real teams hoard them.
//
// Clamped like the age lean and for the same reason: this decides between
// comparable players. A genuinely worse player is still the one who goes, and
// nobody is kept purely because he is expensive.
export const KEEP_COST_CEILING = 1.15;

// Guaranteed money still owed after this season, as a share of the cap, and
// how much of a keep bonus a full cap's worth of it buys.
export const cutCostLean = ({
	contractAmount,
	contractExp,
	season,
	salaryCap,
}: {
	contractAmount: number | undefined;
	contractExp: number | undefined;
	season: number;
	salaryCap: number;
}): number => {
	if (
		contractAmount === undefined ||
		contractExp === undefined ||
		salaryCap <= 0
	) {
		return 1;
	}
	// This season is being paid either way, so only the years beyond it are a
	// cost of cutting rather than a cost of the contract.
	const yearsAfterThis = Math.max(0, contractExp - season);
	if (yearsAfterThis === 0) {
		return 1;
	}
	const share = (contractAmount * yearsAfterThis) / salaryCap;
	return Math.min(KEEP_COST_CEILING, 1 + share);
};

export const positionCounts = (
	players: readonly { pos: string }[],
): Map<PosBucket, number> => {
	const counts = new Map<PosBucket, number>();
	for (const p of players) {
		const bucket = posBucket(p.pos);
		counts.set(bucket, (counts.get(bucket) ?? 0) + 1);
	}
	return counts;
};

// How much this team wants to keep him. LOWEST goes first.
export const keepScore = ({
	p,
	tier,
	counts,
	season,
	salaryCap,
}: {
	p: CutCandidate;
	tier: TradePosture["tier"] | undefined;
	// Position counts across the roster being cut down, so the last centre is
	// recognisable as the last centre.
	counts: Map<PosBucket, number>;
	// For pricing what the cut leaves behind. Omitted keeps the old ordering.
	season?: number;
	salaryCap?: number;
}): number => {
	const base = Number.isFinite(p.value) ? p.value : 0;

	// Without a posture this is exactly the old ordering, which is what an
	// unsmart league should keep getting.
	if (tier === undefined) {
		return base;
	}

	const bucket = posBucket(p.pos);
	const atPos = counts.get(bucket) ?? 0;
	const scarcity = atPos <= SCARCE_AT_POSITION ? SCARCITY_PROTECTION : 1;

	const lean = Math.min(
		KEEP_AGE_CEILING,
		Math.max(KEEP_AGE_FLOOR, ageFitMultiplier(tier, p.age)),
	);

	const cost =
		season !== undefined && salaryCap !== undefined
			? cutCostLean({
					contractAmount: p.contractAmount,
					contractExp: p.contractExp,
					season,
					salaryCap,
				})
			: 1;

	const score = base * lean * scarcity * cost;
	return Number.isFinite(score) ? score : 0;
};

// The roster in the order a front office would let it go: first to be cut
// first. Ties break on pid so a cut is reproducible across devices in a shared
// league - two devices that ordered an identical roster differently would
// release different players and diverge.
export const cutOrder = <T extends CutCandidate>(
	players: readonly T[],
	tier: TradePosture["tier"] | undefined,
	// Present when the caller can price a cut; omitted keeps the old ordering.
	money?: { season: number; salaryCap: number },
): T[] => {
	const counts = positionCounts(players);
	const score = (p: T) =>
		keepScore({
			p,
			tier,
			counts,
			season: money?.season,
			salaryCap: money?.salaryCap,
		});
	return [...players].sort((a, b) => {
		const diff = score(a) - score(b);
		return diff !== 0 ? diff : a.pid - b.pid;
	});
};
