import type { TradeTier } from "../trade/tradePosture.ts";

// ---------------------------------------------------------------------------
// WHAT A PLAYER WHO IS WINNING GAMES RIGHT NOW IS WORTH TO THE TEAM HE IS
// WINNING THEM FOR.
//
// Trade value is a league-wide number: what a player is worth to a typical
// team, potential included, age discounted. A contender pricing its own
// starter off that number is pricing him the way a rebuilder would - a
// thirty-year-old sixty is "worth" less than a twenty-one-year-old fifty with
// a high ceiling, and the tier age tables only trim the youngster's number by
// a quarter. So a person could take a contender's third-best player for a
// prospect and a pick and clear the arithmetic, and the contender would go
// into its own title run a little worse for a future it does not care about.
//
// This is the other half of the price: what the deal does to the team ON THE
// FLOOR, this season. It is measured the way the game itself measures a
// roster - team.ovr, the predictor of margin of victory - before the trade
// and after it, and a team trying to win charges for every point it would
// lose. A like-for-like swap costs nothing extra. A consolidation that makes
// the team better costs nothing extra. Only a deal that takes strength off
// the court this season pays, and only for a team that has a use for it.
//
// Pure - no database - so the table is unit-testable.
// ---------------------------------------------------------------------------

// How much a team minds a point of team ovr leaving the floor, as a share of
// what it gives up in the deal. A team going all-in has one season it cares
// about and a starter is most of the reason to make any trade at all; a
// buyer is nearly as attached; a fringe team is still trying and so minds a
// little; a selling team is choosing to get worse and is charged nothing.
export const PRESENT_HELP_WEIGHT: Record<TradeTier, number> = {
	teardown: 0,
	seller: 0,
	fringe: 0.05,
	buyer: 0.14,
	allIn: 0.2,
};

// A ceiling, so a strange league (ten men out injured, a roster of two) can
// never turn the premium into a wall. Ten points of team ovr is three points
// of margin a night, which is a title contender turning into a lottery team.
export const MAX_PRESENT_DROP = 10;

// The extra a team of this tier wants back, as a share of what it gives up,
// for the on-court strength a trade takes away this season.
export const presentHelpPremium = ({
	tier,
	ovrBefore,
	ovrAfter,
}: {
	tier: TradeTier;
	ovrBefore: number;
	ovrAfter: number;
}): number => {
	const weight = PRESENT_HELP_WEIGHT[tier];
	if (!(weight > 0)) {
		return 0;
	}
	const drop = ovrBefore - ovrAfter;
	if (!Number.isFinite(drop) || drop <= 0) {
		return 0;
	}
	return weight * Math.min(drop, MAX_PRESENT_DROP);
};
