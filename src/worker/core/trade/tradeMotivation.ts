import type { TradeTier } from "./tradePosture.ts";

// ---------------------------------------------------------------------------
// TRADE MOTIVATION — the pure rules that turn a team's posture (and player
// moods / expiring contracts) into how it acts on the market: how often it
// deals as the deadline nears, whether it dumps a walk-year player, and whether
// it will take on a rental. Split out so every rule is unit-testable without a
// database. Consumed by betweenAiTeams.
// ---------------------------------------------------------------------------

// --- Deadline ramp: CPU trades get more frequent as the deadline approaches ---
export const DEADLINE_WINDOW_DAYS = 21;
export const DEADLINE_PEAK_MULTIPLIER = 3.5;

// 1× until ~3 weeks out, then ramps smoothly up to the peak at the deadline.
export const deadlineRampMultiplier = (
	daysToDeadline: number | undefined,
): number => {
	if (daysToDeadline === undefined || daysToDeadline > DEADLINE_WINDOW_DAYS) {
		return 1;
	}
	const d = Math.max(0, daysToDeadline);
	return (
		1 +
		((DEADLINE_PEAK_MULTIPLIER - 1) * (DEADLINE_WINDOW_DAYS - d)) /
			DEADLINE_WINDOW_DAYS
	);
};

// A player is unlikely to re-sign if their willingness (moodInfo.probWilling,
// 0..1) is below this.
export const RESIGN_UNLIKELY = 0.5;

const CONTENDER_TIERS = new Set<TradeTier>(["allIn", "buyer"]);

// A team should AGGRESSIVELY move an expiring player who won't re-sign when it
// isn't contending this year — he walks for nothing otherwise and there's no
// title to chase, so cash him in now.
export const shouldDumpExpiring = ({
	isExpiring,
	probWillingCurrent,
	tier,
}: {
	isExpiring: boolean;
	probWillingCurrent: number;
	tier: TradeTier;
}): boolean =>
	isExpiring && probWillingCurrent < RESIGN_UNLIKELY && !CONTENDER_TIERS.has(tier);

// Acquiring an expiring player who won't re-sign with you (low mood toward the
// new team) is a RENTAL — only a genuine win-now contender (all-in) should take
// that on. Everyone else should steer well clear.
export const isBadRental = ({
	isExpiring,
	probWillingAcquirer,
	acquirerTier,
}: {
	isExpiring: boolean;
	probWillingAcquirer: number;
	acquirerTier: TradeTier;
}): boolean =>
	isExpiring &&
	probWillingAcquirer < RESIGN_UNLIKELY &&
	acquirerTier !== "allIn";

// How lopsided (against itself) a deal the initiator will accept. Normally a
// trade must be roughly fair to it; when it's dumping a walk-year player it will
// swallow a much worse return rather than lose him for nothing.
export const NORMAL_DV_TOLERANCE = 15;
export const MOTIVATED_DUMP_DV = -35;

// --- Blockbusters: a contender empties the war chest for a genuine star ------
// Prying a star loose takes far more than a role player — realistically a stack
// of first-rounders plus salary filler. makeItWork only ever assembles the
// MINIMAL package that clears (it stops the instant the other side says yes), so
// a low ceiling doesn't shrink ordinary deals — it just kills the big ones before
// they can come together. So the ceiling is raised only when a win-now contender
// is hunting talent; everything else keeps the tight ceiling.
export const NORMAL_MAX_ASSETS = 6;
export const BLOCKBUSTER_MAX_ASSETS = 14;

// A contender will pay a premium to land a genuine star — the marquee "give up
// a lot for the guy" deal. This is the most lopsided (against itself) return it
// will swallow, past the normal fairness bound. Measured in HONEST value units
// (the valuation itself is stock BBGM — strategy must never warp what assets
// are worth, only what deals a team is willing to make).
export const STAR_PREMIUM_DV = -25;

// A seller with a legit star deliberately works to move him to a win-now team —
// it will take a somewhat-light (but never giveaway) return to get the deal
// done rather than sit on an asset that doesn't fit its timeline.
export const STAR_SALE_DV = -25;

// Is this acquisition a genuine star landing on a win-now contender? Only then do
// we open the package ceiling and allow the overpay premium — so blockbusters are
// reserved for real stars going to teams built to win now, not routine deals.
export const isStarAcquisition = ({
	bestReceivedOvr,
	acquirerTier,
	starOvr,
}: {
	bestReceivedOvr: number;
	acquirerTier: TradeTier;
	starOvr: number;
}): boolean => CONTENDER_TIERS.has(acquirerTier) && bestReceivedOvr >= starOvr;

// Contenders and sellers are the most natural partners (win-now vets one way,
// youth + picks the other), so weight those pairings up.
export const partnerWeight = (
	initiatorTier: TradeTier,
	partnerTier: TradeTier,
): number => {
	const initContender = CONTENDER_TIERS.has(initiatorTier);
	const partnerContender = CONTENDER_TIERS.has(partnerTier);
	return initContender !== partnerContender ? 3 : 1;
};

// Is this team a seller (would move present talent for the future)?
export const isSelling = (tier: TradeTier): boolean =>
	tier === "fringe" || tier === "seller" || tier === "teardown";

// --- Timeline coherence -------------------------------------------------------
// A trade must fit BOTH teams' timelines, not just clear the value math.

// A rebuilding team (seller/teardown) must never ACQUIRE a real veteran unless
// it's being paid in draft capital to absorb him — that's the only reason a
// bad team takes on age. Stops "26-56 team trades assets for a 33yo on $60M".
export const TIMELINE_VET_AGE = 29;
export const TIMELINE_VET_VALUE = 38; // good enough that acquiring him means something

export const sellerAcquiresVet = ({
	acquirerTier,
	age,
	value,
	receivesPicks,
}: {
	acquirerTier: TradeTier;
	age: number;
	value: number;
	receivesPicks: boolean;
}): boolean =>
	(acquirerTier === "seller" || acquirerTier === "teardown") &&
	!receivesPicks &&
	age >= TIMELINE_VET_AGE &&
	value >= TIMELINE_VET_VALUE;

// A win-now contender must never come out of a trade with a clearly worse best
// player than it gave up — it doesn't matter that the return is younger or
// includes picks; contenders don't collect futures at the cost of the present.
// (Consolidation passes: giving two mid players for one better player IMPROVES
// the best. Buying with picks/depth passes: nothing good is given up.)
export const CONTENDER_BEST_GIVEN_BAR = 50; // only guard when a real player leaves
export const CONTENDER_BEST_SLACK = 8;

export const contenderDowngradesBest = ({
	acquirerTier,
	bestGivenValue,
	bestReceivedValue,
}: {
	acquirerTier: TradeTier;
	bestGivenValue: number;
	bestReceivedValue: number;
}): boolean =>
	CONTENDER_TIERS.has(acquirerTier) &&
	bestGivenValue >= CONTENDER_BEST_GIVEN_BAR &&
	bestReceivedValue < bestGivenValue - CONTENDER_BEST_SLACK;

// --- Re-trade cooldown ---------------------------------------------------------
// AI teams don't flip a player they just traded for — no same-season ping-pong
// (a player bouncing across three rosters in one year reads as chaos, not
// strategy). Pure check on the player's transaction log.
export const wasTradedThisSeason = (
	transactions: { type: string; season: number }[] | undefined,
	season: number,
): boolean => {
	const lastTransaction = transactions?.at(-1);
	return lastTransaction?.type === "trade" && lastTransaction.season === season;
};

// --- On-court sanity backstop ------------------------------------------------
// The valuation can occasionally be fooled (compressed ratings, quantity of
// mediocre players, an aggressive strategy tilt) into approving a deal that
// simply makes a team worse. Independently of the value math, no team should
// come out of a trade with LESS talent AND no younger — unless it got draft
// picks back (a legitimate rebuild return). That's a pure downgrade.
export const DOWNGRADE_TALENT_FLOOR = 0.9; // received < 90% of talent given
export const DOWNGRADE_AGE_SLACK = 0.5; // and no younger (within half a year)

export const isPureDowngrade = ({
	givenValue,
	receivedValue,
	givenAge,
	receivedAge,
	receivedPicks,
}: {
	givenValue: number;
	receivedValue: number;
	givenAge: number;
	receivedAge: number;
	receivedPicks: boolean;
}): boolean => {
	if (receivedPicks) {
		// Getting draft capital is a legitimate reason to take back less talent.
		return false;
	}
	return (
		receivedValue < givenValue * DOWNGRADE_TALENT_FLOOR &&
		receivedAge >= givenAge - DOWNGRADE_AGE_SLACK
	);
};
