import type { TradeTier } from "./tradePosture.ts";

// ---------------------------------------------------------------------------
// WHERE A FRANCHISE'S PICK IS HEADING
//
// A future draft pick was projected by regressing this year's slot toward a
// constant: a quarter of the way down the round for anyone, three quarters for
// the user. That constant is a difficulty thumb on the scale, not a
// projection - and there is a second, explicit one right below it, so future
// picks were paying the tax twice while nothing actually estimated where the
// pick would land.
//
// The cost of that shows up in the trade every human tries to make. A team in
// year one of a teardown will be bad for years, so its pick four seasons out is
// probably near the top of the round; a stacked young contender will be good for
// years, so its is probably near the bottom. Both regressed to the same number,
// which means the AI could not tell a genuinely valuable future first from a
// worthless one - the single read that separates a good GM from a bad one when
// picks change hands.
//
// So the projection is built from what the franchise IS: the same tier the rest
// of the front office runs on, tilted by how old the roster is, and pulled
// toward the middle of the round as the horizon grows because nobody knows
// anything four years out.
//
// Pure - no database - so it is unit-testable on its own.
// ---------------------------------------------------------------------------

// Where a franchise of each tier tends to pick, as a share of the round.
// 0 is the first pick, 1 is the last.
//
// Not symmetric, and deliberately so: the bad end of the league is much
// stickier than the good end. A team that tears down is choosing to be bad and
// will be for a while; a team that wins 55 games has to keep an expensive
// roster together to stay there, and usually cannot.
export const TIER_SLOT_SHARE: Record<TradeTier, number> = {
	teardown: 0.14,
	seller: 0.3,
	fringe: 0.5,
	buyer: 0.62,
	allIn: 0.68,
};

// The age at which a roster is neither getting better nor worse on its own.
export const NEUTRAL_ROSTER_AGE = 26.5;

// How far a year of roster age either side of neutral moves the projection, as
// a share of the round. An old roster is going to decline and a young one is
// going to improve, which is the read a GM is really making when he says a
// contender's pick four years out is worth having.
export const AGE_DRIFT_PER_YEAR = 0.035;

// And how much of that drift a single season away actually earns. A pick one
// year out barely feels the roster's age; one five years out feels all of it.
export const AGE_DRIFT_FULL_SEASONS = 4;

// How much of the projection survives the horizon. Four years out a team's
// current identity tells you something, but not much - most of what you are
// buying is that a draft pick is a draft pick.
export const UNCERTAINTY_FULL_SEASONS = 5;
export const MAX_UNCERTAINTY = 0.55;

const clamp01 = (x: number) => Math.max(0, Math.min(1, x));

// Where this franchise's pick is expected to land, as a share of the round.
export const projectedSlotShare = ({
	tier,
	avgAge,
	seasons,
}: {
	tier: TradeTier;
	// Value-weighted roster age, or undefined when it cannot be read.
	avgAge: number | undefined;
	// How many seasons in the future this pick is.
	seasons: number;
}): number => {
	const base = TIER_SLOT_SHARE[tier];

	// An old roster slides toward the top of the draft, a young one away from
	// it - and the further out the pick, the more of that has happened.
	let drift = 0;
	if (avgAge !== undefined && Number.isFinite(avgAge)) {
		const years = Math.max(-6, Math.min(6, NEUTRAL_ROSTER_AGE - avgAge));
		const earned = clamp01(Math.max(0, seasons) / AGE_DRIFT_FULL_SEASONS);
		drift = years * AGE_DRIFT_PER_YEAR * earned;
	}

	// Toward the middle of the round as the horizon grows: four years out,
	// nobody knows anything.
	const uncertainty =
		MAX_UNCERTAINTY * clamp01(Math.max(0, seasons) / UNCERTAINTY_FULL_SEASONS);

	const projected = clamp01(base + drift);
	return clamp01(projected * (1 - uncertainty) + 0.5 * uncertainty);
};

// The projected slot itself, 1-based, within a round of this many picks.
export const projectedSlot = ({
	tier,
	avgAge,
	seasons,
	numPicksPerRound,
}: {
	tier: TradeTier;
	avgAge: number | undefined;
	seasons: number;
	numPicksPerRound: number;
}): number => {
	if (!(numPicksPerRound > 0)) {
		return 1;
	}
	const share = projectedSlotShare({ tier, avgAge, seasons });
	// Share 0 is pick 1 and share 1 is the last pick, so the round's own size
	// decides the spacing rather than a hardcoded league size.
	const slot = 1 + share * (numPicksPerRound - 1);
	return Math.max(1, Math.min(numPicksPerRound, Math.round(slot)));
};

// ---------------------------------------------------------------------------
// COLA CHANGES WHAT A PICK IS.
//
// Under a normal lottery, chances are handed out fresh each year by record, so
// where a pick lands is a question about how good its team will be - which is
// exactly what everything above projects. Carry-Over Lottery Allocation is a
// different game. Chances ACCUMULATE: a team banks COLA_ALPHA every season it
// misses the deep playoffs, keeps the balance across years, has it cut by
// playoff success, and spends it entirely on winning the first pick. So a
// franchise's odds are a stockpile it has been building, not a reading of last
// season.
//
// And one rule matters more than any of that: a first-round pick that has
// CHANGED HANDS is excluded from the lottery outright (see genOrder.ts - its
// weight is zero). Which means the pick you acquire is never the pick you were
// looking at. A rebuilding team's first is a lottery ticket while that team
// holds it and a mid-round pick the moment you trade for it, and an AI that
// prices those the same will happily pay lottery money for something that
// cannot win the lottery.
//
// Both facts go through the same small model below.
// ---------------------------------------------------------------------------

// A rough chance of landing one of the lottery picks, given this team's share
// of every chance in the pool. Deliberately crude - the real draw is without
// replacement across several picks - but it has the two properties that
// matter: no chances means no lottery, and a dominant stockpile means a near
// certainty.
export const colaLotteryOdds = ({
	chancesShare,
	numLotteryPicks,
}: {
	// This team's share of all chances in the pool, 0 to 1.
	chancesShare: number;
	numLotteryPicks: number;
}): number => {
	const share = clamp01(chancesShare);
	if (share <= 0 || numLotteryPicks <= 0) {
		return 0;
	}
	// The chance of being missed by every one of the draws.
	return clamp01(1 - (1 - share) ** numLotteryPicks);
};

// Where a pick actually lands under COLA, given where its team's RECORD would
// put it. Two outcomes, blended by the odds: win a lottery pick and land in
// the top few, or miss and slot in by record - which, having missed, can never
// be better than the first pick after the lottery.
export const colaAdjustedSlot = ({
	recordSlot,
	chancesShare,
	lotteryEligible,
	numLotteryPicks,
	numPicksPerRound,
}: {
	// The slot the team's own outlook implies, 1-based.
	recordSlot: number;
	chancesShare: number;
	// False for a pick that has changed hands, or would by this trade.
	lotteryEligible: boolean;
	numLotteryPicks: number;
	numPicksPerRound: number;
}): number => {
	if (!(numPicksPerRound > 0) || !(numLotteryPicks > 0)) {
		return recordSlot;
	}

	// Missing the lottery means everyone who won it picks ahead of you.
	const missSlot = Math.min(
		numPicksPerRound,
		Math.max(recordSlot, numLotteryPicks + 1),
	);
	if (!lotteryEligible) {
		return missSlot;
	}

	// Winning one is equally likely to be any of them.
	const winSlot = (numLotteryPicks + 1) / 2;
	const odds = colaLotteryOdds({ chancesShare, numLotteryPicks });
	return odds * winSlot + (1 - odds) * missSlot;
};
