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
