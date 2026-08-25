import type { PosBucket, TradePosture } from "../trade/tradePosture.ts";
import { posBucket } from "../trade/tradePosture.ts";
import { INJURY_WEIGHT } from "../team/tierValuation.ts";

// ---------------------------------------------------------------------------
// FREE AGENCY, RUN LIKE A FRONT OFFICE
//
// The AI's free agency was "sign the highest-VALUE player you can afford, at
// random, most days". Nothing about it knew what the franchise was trying to
// do: a 60-win team and a 20-win team chased the same 33-year-old, a team with
// three All-Star guards signed a fourth, and - the one that shows up most in a
// league you actually watch - a team with max cap space blew it on the fourth-
// best free agent on day one, because holding money is not something the old
// code could express.
//
// This module is the missing brain. It reuses the franchise posture already
// computed for trades (tier, positional needs, cap situation), so a team's
// free agency and its trades finally come from the same plan.
//
// Everything here is PURE - plain numbers in, decisions out, no database - so
// the strategy can be unit-tested exhaustively. autoSign does the fetching and
// hands these functions their inputs.
// ---------------------------------------------------------------------------

export type FaPlayer = {
	pid: number;
	ovr: number;
	pot: number;
	value: number;
	age: number;
	pos: string;
	// What he is asking for right now (decreaseDemands walks this down daily).
	amount: number;
	// Last season of the deal he is asking for.
	exp: number;
	injuredGames: number;
	// How likely he is to sign HERE, 0..1, from the mood system. Optional so
	// every existing caller that only wants a fit score is unaffected; the cap
	// hold planner is the one that needs it, because freezing an offseason for
	// a player who has no interest is the most expensive way to be wrong.
	probWilling?: number;
};

// ---- Fit: is this the right player FOR THIS TEAM? --------------------------

// A rebuilding team signing a 34-year-old is the single most obviously wrong
// thing the old code did, and a contender passing on a proven 31-year-old to
// sign an unproven 22-year-old is the mirror image of it. Age is weighted by
// what the team is actually trying to do.
export const ageFitMultiplier = (
	tier: TradePosture["tier"],
	age: number,
): number => {
	if (tier === "teardown" || tier === "seller") {
		// Youth is the whole point. A vet is worth something only as a flippable
		// asset, never as a multi-year commitment.
		if (age <= 24) {
			return 1.25;
		}
		if (age <= 27) {
			return 1.05;
		}
		if (age <= 30) {
			return 0.8;
		}
		return 0.55;
	}

	if (tier === "allIn") {
		// Win now. A prospect is worth less to this team than to anyone else -
		// he cannot help in the only season it cares about - and a 31-year-old
		// who is good today is worth more.
		if (age <= 22) {
			return 0.8;
		}
		if (age <= 24) {
			return 0.95;
		}
		if (age <= 32) {
			return 1.1;
		}
		return 0.95;
	}

	if (tier === "buyer") {
		if (age <= 22) {
			return 0.95;
		}
		if (age <= 31) {
			return 1.1;
		}
		return 0.8;
	}

	// fringe - the awkward middle. Mildly prefers players who won't be old by
	// the time the team is good.
	if (age <= 26) {
		return 1.1;
	}
	if (age <= 30) {
		return 1;
	}
	return 0.75;
};

// A hole at a position is worth more than another body where the team is
// already deep. Severity/depth come straight from the posture's own analysis of
// the roster, so this agrees with what the trade AI thinks the team needs.
export const positionFitMultiplier = (
	posture: Pick<TradePosture, "needs" | "surpluses" | "targetPos">,
	pos: string,
): number => {
	const bucket: PosBucket = posBucket(pos);

	const need = posture.needs.find((n) => n.pos === bucket);
	if (need) {
		// A gaping hole (severity is OVR points below a league-average starter)
		// is worth a big bump; a small one, a small bump.
		return 1 + Math.min(0.35, need.severity / 40);
	}

	const surplus = posture.surpluses.find((s) => s.pos === bucket);
	if (surplus) {
		return Math.max(0.7, 1 - 0.12 * surplus.depth);
	}

	if (posture.targetPos === bucket) {
		return 1.1;
	}

	return 1;
};

// A long deal for an old player is how AI teams used to strand themselves with
// unmovable money for years. Only teams trying to win now accept that risk.
export const contractRiskMultiplier = ({
	tier,
	age,
	years,
	amount,
	minContract,
}: {
	tier: TradePosture["tier"];
	age: number;
	years: number;
	amount: number;
	minContract: number;
}): number => {
	// A minimum deal is never a mistake worth modelling, and neither is a
	// one-year deal at any price: the risk being priced here is COMMITTING to a
	// player's decline years, and a expiring contract commits to nothing. (It is
	// the move a smart team makes with an old player, not one to discourage.)
	//
	// WHICH MAKES `years` HERE THE WRONG LENGTH, and deliberately so. Callers
	// pass the length off the player's DEMAND, but no team ever signs that
	// deal: signingYears rewrites it first, and for old men on selling teams it
	// rewrites four years to one - a commitment this function would then score
	// as no risk at all. So a rebuild discounts a thirty-one-year-old for a
	// four-year deal it was about to convert to a single season.
	//
	// That population is real and it is where this front office's unemployed
	// talent sits: thirteen players a season above 50 ovr left unsigned against
	// stock's nine, and the ones left over are better (54.3 against 52.4),
	// dearer ($12.3M against $4.7M) and older (31.2 against 30.1).
	//
	// Passing the length actually offered was built and measured over six
	// seeds. It works, and it is not worth it. Rotation talent +0.09, the
	// deployable pool +0.06, the top hundred +0.55 on five seeds - bought with
	// 20.6M a season more dead money. That is about 230M per point of employed
	// talent, against the 120M-145M this file measures for every other lever on
	// the same curve (see decadesSim.test.ts), so it is a strictly expensive way
	// to buy something. Distinct champions also fell by more than one on five
	// seeds of six.
	if (amount <= minContract * 1.5 || years <= 1) {
		return 1;
	}

	// How old he'll be in the last year of the deal.
	const ageAtEnd = age + Math.max(0, years - 1);
	if (ageAtEnd <= 30) {
		return 1;
	}

	const overage = ageAtEnd - 30;
	const tolerance =
		tier === "allIn"
			? 0.02
			: tier === "buyer"
				? 0.05
				: tier === "fringe"
					? 0.09
					: 0.14;
	return Math.max(0.4, 1 - overage * tolerance);
};

// How far the fit adjustments may move a player from his raw value. These exist
// because the adjustments MULTIPLY: an old player on a long expensive deal, at a
// position the team is deep at, on a team wanting cap relief used to come out at
// 0.8 * 0.85 * 0.4 * 0.5 = 0.14 of his value - which sorts a 72-ovr star below a
// replacement-level scrub.
//
// That would be survivable if teams disagreed, but they don't: age and contract
// risk point the same way at almost every team, so a player buried by them is
// buried EVERYWHERE and simply never signs. Measured over eight seasons, that
// left six stars (including a 32-year-old 72 ovr) permanently unemployed while
// stock BBGM signed all of them. Fit decides between comparable players; it does
// not get to remove someone from the market.
// HOW RAW HE IS: 0 for a finished player, 1 for one whose ceiling is thirty
// points above what he can do today.
export const UPSIDE_FULL_GAP = 30;

export const upsideShare = (p: { ovr: number; pot: number }): number =>
	Math.min(1, Math.max(0, (p.pot - p.ovr) / UPSIDE_FULL_GAP));

// A rebuilder is buying the player's future, a win-now team his present.
//
// This used to be an ADDITIVE bonus, which meant it escaped the fit clamp
// below: a teardown scored a 44-ovr/62-pot prospect at 46 + 0.4x18 = 53, then
// multiplied by a 1.3 ceiling for 69 - above a 72-ovr star floored to 50. Free
// agency signs the FIRST acceptable name in this order (see getBest), so the
// star's roster spot went to the prospect and the star went unsigned.
//
// As a multiplier inside the clamp, a rebuilder still takes the prospect over
// a COMPARABLE finished player - which is the whole point - but no preference
// can outrank a genuinely better one. The exact case above is pinned in
// frontOffice.test.ts.
export const UPSIDE_LEAN: Record<TradePosture["tier"], number> = {
	teardown: 0.25,
	seller: 0.2,
	fringe: 0,
	buyer: -0.08,
	allIn: -0.2,
};

export const upsideFitMultiplier = (
	tier: TradePosture["tier"],
	p: { ovr: number; pot: number },
): number => 1 + UPSIDE_LEAN[tier] * upsideShare(p);

// HOW MUCH OF A COMMITMENT THIS CONTRACT IS, 0 to 1.
//
// Fit is a statement about a multi-year investment: whether this player will
// still be useful when the team is good, whether the money is affordable,
// whether he plays where the hole is. None of that is at stake in a minimum
// one-year deal for the last bench spot, where two candidates cost exactly
// the same and the only real question is which one is better - so at the
// bottom of the market, the ordering should collapse to value.
//
// A deal at half the max is treated as a full commitment; the floor keeps a
// little fit alive even at the minimum, because a team with a gaping hole at
// centre would still rather its last man be a centre.
export const COMMITMENT_FLOOR = 0.25;

export const commitmentShare = ({
	amount,
	minContract,
	maxContract,
}: {
	amount: number;
	minContract: number;
	maxContract: number;
}): number => {
	const full = Math.max(minContract, maxContract / 2);
	const share =
		full > minContract
			? (amount - minContract) / (full - minContract)
			: amount > minContract
				? 1
				: 0;
	return (
		COMMITMENT_FLOOR + (1 - COMMITMENT_FLOOR) * Math.min(1, Math.max(0, share))
	);
};

// What an injury costs a middle-of-the-road team in free agency - the flat
// number stock BBGM applied to everyone, now the centre of a range.
export const INJURED_FA_PENALTY = 0.15;

export const FIT_FLOOR = 0.7;
export const FIT_CEILING = 1.3;

// What this free agent is worth TO THIS TEAM. Ordering by this instead of by
// raw value is what stops every team wanting the same player.
export const scoreFreeAgent = ({
	p,
	posture,
	season,
	minContract,
	maxContract,
	daysLeft,
}: {
	p: FaPlayer;
	posture: TradePosture;
	season: number;
	minContract: number;
	maxContract: number;
	// Days of free agency left, if this is the offseason. Fit matters less as the
	// market empties - see the urgency ramp below.
	daysLeft?: number;
}): number => {
	const years = Math.max(1, p.exp - season + 1);

	const score0 = p.value;
	let score = score0;

	let fit =
		ageFitMultiplier(posture.tier, p.age) *
		positionFitMultiplier(posture, p.pos) *
		upsideFitMultiplier(posture.tier, p) *
		contractRiskMultiplier({
			tier: posture.tier,
			age: p.age,
			years,
			amount: p.amount,
			minContract,
		});

	// Nobody's first choice is a player who can't play yet - but how much that
	// costs depends on what the season is for. A contender is signing him to
	// play now; a rebuild is signing him for the year after, and a hurt player
	// it can get cheap is a bargain rather than a problem. Same weights the
	// trade valuation uses (team/tierValuation.ts), so the two agree on what an
	// injury is worth to a given team.
	//
	// TIER ONLY, deliberately - not scaled by how long he is out. A first
	// version multiplied the penalty by the length of the injury as well, which
	// reads as the more careful model and is not: it made every team mind
	// injuries more rather than making them disagree, and over fifteen seasons
	// it doubled the number of stars left unsigned (6 to 12 on one seed, 2 to 5
	// on another) while costing the league a point of average team rating. A
	// fringe team lands on 0.85 here, which is exactly what every team used to
	// get, so this rotates the old behaviour around its centre instead of
	// moving it.
	if (p.injuredGames > 0) {
		fit *= 1 - INJURED_FA_PENALTY * INJURY_WEIGHT[posture.tier];
	}

	fit = Math.min(FIT_CEILING, Math.max(FIT_FLOOR, fit));

	// Scaled by how big a commitment the contract is - see commitmentShare.
	fit =
		1 +
		(fit - 1) * commitmentShare({ amount: p.amount, minContract, maxContract });

	score *= fit;

	// Late in free agency a front office stops shopping for the ideal fit and
	// starts taking the best player still on the board - the same instinct that
	// makes teams give up their cap holds at PURSUIT_GIVE_UP_DAYS.
	//
	// This ramps the WHOLE adjustment away, tilt included, so on the last day the
	// ordering is exactly p.value - which is stock BBGM's ordering. That is the
	// point: it makes "the market clears at least as well as vanilla" structural
	// rather than something to be re-checked every time a multiplier is tuned.
	if (daysLeft !== undefined && daysLeft < PURSUIT_GIVE_UP_DAYS) {
		const urgency = Math.max(0, daysLeft) / PURSUIT_GIVE_UP_DAYS;
		score = score0 + (score - score0) * urgency;
	}

	// A score that is not a number does not fail loudly - it sorts arbitrarily,
	// and the front office quietly starts picking free agents at random. An
	// imported league or a God Mode edit can hand this any of ovr, pot, age or
	// a contract as NaN. Same guard, and the same reasoning, as scoreProspect
	// and keepScore.
	return Number.isFinite(score) ? score : 0;
};

// HOW LONG A DEAL THIS FRONT OFFICE WANTS, given who it is signing. The ask's
// years come from a league-wide regression that knows nothing about the
// signing team's plan; the plan is what decides structure. A seller keeps its
// veterans on expiring deals - the contract IS the trade asset at the
// deadline - and locks up a real investment in a young player. A win-now team
// never anchors itself to a thirty-something's decline years. Amount is
// untouched (willingness is priced on money, and the user picks lengths
// freely in their own negotiations - this is the same power).
//
// SIGNINGS ONLY. Applying this to a team's OWN expiring players looks like an
// obvious missing symmetry - the AI caps an outside 33-year-old at two years
// and hands its own whatever the regression asks - and it was built and
// measured over eight seeds of twenty seasons. It fires plenty (320 deals a
// run were shortened) and made leagues worse: team ovr down 1.1 with five of
// eight seeds negative, MORE stars left stranded, rebuild payoff down.
//
// The asymmetry is real, not an oversight. For an outside free agent a short
// deal is a cheap flier that costs nothing if he does not work out. For a
// player already here it is the opposite: shouldLetWalk has ALREADY decided
// whether this team wants him, so shortening the commitment it just chose to
// make only means losing him for nothing a year sooner - and a seller's
// veteran on three years is a better deadline asset than the same man on one.
// Restricting it to contenders (where that reason does not apply) came out a
// wash on quality and worse on dead money, so neither version is here.
// Below this a cheap signing is a prospect worth committing to rather than a
// stopgap; above it he is filling a roster spot.
export const STOPGAP_AGE = 25;
export const STOPGAP_YEARS = 2;

export const signingYears = ({
	tier,
	age,
	askedYears,
	amount,
	ovr,
	rotationOvr,
	minContract,
	minLength,
	maxLength,
}: {
	tier: TradePosture["tier"];
	age: number;
	askedYears: number;
	amount: number;
	ovr: number;
	rotationOvr: number;
	minContract: number;
	minLength: number;
	maxLength: number;
}): number => {
	// A NaN here would be written straight onto a contract, which is worse than
	// a bad sort: nothing downstream ever checks a contract length again.
	let years = Number.isFinite(askedYears) ? askedYears : minLength;
	if (tier === "teardown" || tier === "seller") {
		if (age >= 28) {
			years = Math.min(years, 1);
		} else if (age <= 24 && amount > minContract * 1.5) {
			years = Math.max(years, 3);
		}
	} else if ((tier === "allIn" || tier === "buyer") && age >= 32) {
		years = Math.min(years, 2);
	}

	// A CHEAP VETERAN IS A STOPGAP, AND A STOPGAP DOES NOT NEED YEARS.
	//
	// Every release an AI team makes happens in free agency - measured, all of
	// them, on both arms - and a release does not release the money: the
	// guaranteed years left become dead money. The men being released are
	// overwhelmingly this population, signed near the minimum to fill out a
	// roster, and they were being signed for four years at a time. Four years
	// on a stopgap is three years of dead money the moment somebody better
	// turns up, and nothing about a minimum-salary thirty-year-old requires the
	// commitment.
	//
	// Unlike the gate in autoSign this blocks no signing at all - the team still
	// gets the player - so it is not another point on the same trade-off curve
	// between how many free agents get signed and how flat the league ends up.
	// Getting off that curve was the point: every other lever tried on this
	// problem just slid along it. Making the gate's margin proportional to the
	// money stranded loosened it and cost 4.7M a season MORE dead money and 1.5
	// more unsigned stars; moving the gate's value bar tightened it and cost 2.2
	// points of the top five.
	//
	// Six seeds of twelve real seasons, against the same code without this rule:
	// the bottom five gain 2.1 points on five seeds of six and the talent
	// actually employed across the league gains 0.28 on five of six - the first
	// thing measured in this file to move that number at all. Dead money and
	// unsigned stars come down a little, on four seeds of six. Nothing
	// regresses.
	//
	// Young men are exempt: a cheap 24-year-old is the flier that is supposed to
	// pay off, and the rule right above deliberately LENGTHENS those.
	//
	// WHAT MAKES A STOPGAP IS WHAT HE IS, NOT ONLY WHAT HE COSTS. The rule
	// first shipped keyed on price alone, and that turned out to be the smaller
	// half of the population it was written for. Profiling every dead contract
	// in the league over twelve seasons splits them cleanly in two: men the
	// team DRAFTED, and men it acquired. The draft half is down a third since
	// justDrafted and the rule above; the acquired half is the whole of what
	// this front office carries over stock - 55% more of them, 44% more money -
	// and the median one is a twenty-six-year-old at 40 ovr on three and a half
	// million, cut with two years still to run. He is nowhere near the league's
	// eighth-best-per-team line, so nobody was ever going to want him: he is a
	// body, signed for four years, and the last two are dead the day a better
	// body turns up.
	//
	// Being under the rotation bar is exactly that fact, and it is what the
	// trade AI already uses for "someone wants him" (minTradeValue in
	// tradePosture). Reusing it here costs the team nothing - as above, this
	// blocks no signing, it only declines to guarantee the back end.
	const belowRotation = Number.isFinite(rotationOvr) && ovr < rotationOvr;
	if ((amount <= minContract * 2 || belowRotation) && age >= STOPGAP_AGE) {
		years = Math.min(years, STOPGAP_YEARS);
	}

	return Math.max(minLength, Math.min(maxLength, years));
};

// ---- Cap clearing: keeping the powder dry ---------------------------------

// A free agent worth reorganising a payroll around. Deliberately strict: this
// is the guy a front office tells the press it has "flexibility" for, not a
// good starter. Judged on OVR (value is too compressed at the top to separate
// stars) plus a price that actually requires clearing space.
export const isPrize = ({
	p,
	starOvr,
	minContract,
}: {
	p: FaPlayer;
	starOvr: number;
	minContract: number;
}): boolean =>
	p.ovr >= starOvr && p.amount > minContract * 4 && p.injuredGames === 0;

// How badly a team wants a given prize, used to decide which teams get to sit
// on their cap space for him. Not the same as the fit score: this is about
// whether pursuing is CREDIBLE, so it leans on the team being a real
// destination (contention) as well as on fit.
export const pursuitScore = ({
	p,
	posture,
	season,
	minContract,
	maxContract,
}: {
	p: FaPlayer;
	posture: TradePosture;
	season: number;
	minContract: number;
	maxContract: number;
}): number => {
	const fit = scoreFreeAgent({ p, posture, season, minContract, maxContract });

	// A star picks a winner. A 20-win team clearing space for him is the kind of
	// plan that never actually lands anybody, so it shouldn't get to sit out free
	// agency pretending otherwise.
	const destination = 0.6 + 0.8 * posture.contention;

	// A contender with no star has the clearest motive of all.
	const desperation = posture.starGap ? 1.2 : 1;

	return fit * destination * desperation;
};

// Below this chance of signing, a pursuit is a fantasy and the payroll stays as
// it is. Deliberately near zero: a neutral AI team sits around 0.05 on this
// scale (mood docks every non-user team three points), so anything higher stops
// being a filter on hopeless cases and becomes a blanket ban. What it is for is
// the genuinely impossible - a challenge mode with free agency switched off, a
// player who will not deal with this team at any price.
//
// Shared with the cap-clearing path (clearSpace.ts) so both halves of the same
// pursuit agree on what "hopeless" means.
export const MIN_PURSUIT_CONFIDENCE = 0.02;

// How many teams may hold space for the same player. Without a limit, one
// tempting free agent freezes half the league's cap and nobody signs anyone.
export const MAX_PURSUERS_PER_PRIZE = 3;

// With fewer than this many days of free agency left, stop waiting and spend.
// A front office that has missed on its target pivots to the rest of the
// market rather than carrying its space into the season.
export const PURSUIT_GIVE_UP_DAYS = 8;

// Asking prices fall every day of free agency, so a team should be willing to
// wait on a player it cannot QUITE afford today - waiting is the entire point
// of clearing space, and demanding present-day affordability would mean a team
// only ever "holds" room for someone it could already sign this second.
export const PURSUIT_PRICE_PATIENCE = 0.8;

// What a team expects to actually pay for a prize once he has sat unsigned a
// while - the number its cap room has to cover. Exported so a caller can rule
// out the prizes it could never fit before doing anything expensive with the
// rest; planCapHold applies it again itself and is the authority.
export const capHoldTarget = (amount: number): number =>
	amount * PURSUIT_PRICE_PATIENCE;

export type CapHold = {
	// The player this team is keeping room for.
	pid: number;
	// It may still spend down to this payroll without losing him.
	spendCeiling: number;
};

// Should this team sit on its cap space, and how much may it still spend?
//
// Returns undefined when the team should just shop normally - which is most
// teams, most of the time. A hold is only for a team that can actually sign the
// player TODAY if he says yes.
export const planCapHold = ({
	posture,
	prizes,
	payroll,
	salaryCap,
	salaryCapType,
	daysLeft,
	season,
	minContract,
	maxContract,
}: {
	posture: TradePosture;
	// Prizes still unsigned, in any order.
	prizes: FaPlayer[];
	payroll: number;
	salaryCap: number;
	salaryCapType: string;
	// Days of free agency remaining.
	daysLeft: number;
	season: number;
	minContract: number;
	maxContract: number;
}): CapHold | undefined => {
	// Holding space is meaningless without a cap to hold it under - and a cap
	// that is not a number is the same situation wearing a disguise. Without
	// this the affordability check below quietly passes (every comparison
	// against NaN is false), and the team comes away holding room for a player
	// against a ceiling of NaN, which it then never spends under.
	if (salaryCapType === "none" || !Number.isFinite(salaryCap)) {
		return undefined;
	}

	// A team tearing it down is not signing a star, and a team that has given up
	// on this free agency period should get on with filling its roster.
	if (posture.tier === "teardown" || daysLeft < PURSUIT_GIVE_UP_DAYS) {
		return undefined;
	}

	let best: { p: FaPlayer; target: number; score: number } | undefined;
	for (const p of prizes) {
		// What we expect to actually pay, once he has sat unsigned a while.
		const target = capHoldTarget(p.amount);

		// Credible only if the room would genuinely be there for him - and a
		// price that cannot be read is not a price the room can be checked
		// against. Every comparison against NaN is false, so without the first
		// half of this the affordability test passes by default and the team
		// ends up holding room against a ceiling of NaN that it never spends
		// under.
		if (!Number.isFinite(target) || payroll + target > salaryCap) {
			continue;
		}

		// Position is NOT a veto here, deliberately. It is already in the score
		// below (pursuitScore -> scoreFreeAgent -> positionFitMultiplier), so
		// between two comparable prizes a team waits on the one who fills its
		// hole. Making depth disqualifying instead had teams passing on a
		// 75-ovr star because they already had two starters at his position,
		// which is not something a front office with cap space does - every
		// prize here is a star by definition (see isPrize).

		// Hopeless is hopeless: no amount of fit justifies freezing an offseason
		// for a player who will not deal with this team.
		if ((p.probWilling ?? 1) < MIN_PURSUIT_CONFIDENCE) {
			continue;
		}

		// Weighted by whether he would actually come, which makes this an
		// expected value rather than a wish: fit x how badly this team wants him
		// x the chance it gets him.
		//
		// A hold needs this more than the cap-clearing path does, and for a
		// structural reason. There, the dump and the signing happen in the same
		// breath, so a team cannot clear space and then lose him. A hold has no
		// such guarantee - it is days of free agency spent NOT signing anyone,
		// on the chance that one player says yes at the end of it. Planning an
		// offseason around a player who was never coming is the most expensive
		// way for a front office to be wrong.
		const score =
			pursuitScore({
				p,
				posture,
				season,
				minContract,
				maxContract,
			}) * (p.probWilling ?? 1);
		if (!best || score > best.score) {
			best = { p, target, score };
		}
	}

	// Nobody worth waiting for. Spend the money on the market instead of
	// carrying it into the season.
	if (!best) {
		return undefined;
	}

	return {
		pid: best.p.pid,
		// Everything not earmarked for him is still spendable. The player himself
		// is exempt from this - it is his money.
		spendCeiling: salaryCap - best.target,
	};
};

// Of every team that wants to hold space for a player, only the most credible
// few actually do. Everyone else releases the hold and shops - which is what
// keeps a market moving instead of thirty teams all waiting on one man.
export const resolveCapHolds = (
	// One entry per team that wants to hold, with the score behind it.
	wanted: { tid: number; hold: CapHold; score: number }[],
	maxPursuers = MAX_PURSUERS_PER_PRIZE,
): Map<number, CapHold> => {
	const byPrize = new Map<number, typeof wanted>();
	for (const entry of wanted) {
		const arr = byPrize.get(entry.hold.pid);
		if (arr) {
			arr.push(entry);
		} else {
			byPrize.set(entry.hold.pid, [entry]);
		}
	}

	const resolved = new Map<number, CapHold>();
	for (const entries of byPrize.values()) {
		// Ties broken by tid so a sim is reproducible.
		entries.sort((a, b) => b.score - a.score || a.tid - b.tid);
		for (const entry of entries.slice(0, maxPursuers)) {
			resolved.set(entry.tid, entry.hold);
		}
	}
	return resolved;
};

// ---- Re-signing ------------------------------------------------------------

// The most a team will pay OVER a player's asking price to stop him leaving.
//
// Until now a player who did not want to stay simply left, whatever he was worth
// and whatever the team was willing to pay - mood.willing was a hard gate with
// no price attached. Real front offices do not accept that; they overpay to keep
// the players they are built around, and they let everyone else go.
//
// Returns 1 for "will not go above the asking price", which is the answer for
// most players on most teams.
export const MAX_RETENTION_OVERPAY = 1.4;

// Where a player sits on his own team, by OVR. 0 is the best player on the
// roster. A team fights hardest for the players it is built around, a little
// for the rest of its rotation, and not at all for the end of the bench.
export const RETENTION_CORE_RANK = 3;
export const RETENTION_ROTATION_RANK = 8;

// How much better than his replacement a player must be before paying a premium
// to keep him makes any sense.
//
// This is the question the first version of this function forgot to ask, and it
// is the one that decides whether the whole idea helps or hurts. Overpaying to
// keep a player you could have replaced from the market for less is not
// shrewdness, it is just spending more for the same team - and the money is
// gone league-wide, so it shows up as GOOD PLAYERS GOING UNEMPLOYED elsewhere.
// Measured over eight seasons, an overpay that ignored replacement level cost
// the league eleven useful jobs to save four or five players nobody would have
// missed.
export const RETENTION_MIN_EDGE = 4;

export const retentionOverpay = ({
	tier,
	rosterRank,
	isStar,
	age,
	ovr,
	replacementOvr,
}: {
	tier: TradePosture["tier"];
	// 0-based position among his own team's players by OVR.
	rosterRank: number;
	// A star by league-wide standards.
	isStar: boolean;
	age: number;
	ovr: number;
	// What the team could reasonably sign instead, for the role he plays.
	replacementOvr: number;
}): number => {
	// A rebuild is not outbidding anyone to keep a player it is content to lose -
	// that is the whole point of the tier, and paying a premium here would undo
	// the walk-away logic directly below.
	if (tier === "teardown" || tier === "seller") {
		return 1;
	}

	// The end of the bench is replaceable at the market price. Paying over the
	// odds there is not a plan, it is just spending more.
	const core = isStar || rosterRank < RETENTION_CORE_RANK;
	const rotation = rosterRank < RETENTION_ROTATION_RANK;
	if (!core && !rotation) {
		return 1;
	}

	// What is he worth OVER the alternative? A player the market can replace is
	// not worth a premium however highly his own team rates him.
	const edge = ovr - replacementOvr;
	if (edge < RETENTION_MIN_EDGE) {
		return 1;
	}

	let over = tier === "allIn" ? 0.4 : tier === "buyer" ? 0.25 : 0.12;

	if (isStar) {
		over += 0.1;
	}

	// A rotation player is worth keeping, but not worth the premium you would
	// pay for someone you build around.
	if (!core) {
		over *= 0.5;
	}

	// Scale with how irreplaceable he actually is, so the biggest premiums are
	// reserved for the players there is genuinely no substitute for. Full weight
	// arrives at roughly a starter's worth of separation.
	over *= Math.min(1, edge / 12);

	// Paying over the odds for a player who will be finished before the team is
	// done competing is the classic way to end up with unmovable money. Win-now
	// teams still do it, because their window is now.
	if (age >= 33) {
		over *= tier === "allIn" ? 0.7 : 0.3;
	} else if (age >= 30) {
		over *= tier === "allIn" ? 0.9 : 0.6;
	}

	// 1 is "pay the asking price and no more", which is the safe answer to a
	// question this cannot read - a NaN would compare false against every rung
	// of the offer ladder and silently let the player walk.
	const multiplier = Math.min(MAX_RETENTION_OVERPAY, 1 + over);
	return Number.isFinite(multiplier) ? multiplier : 1;
};

// Should a team let its own expiring player walk on strategic grounds, before
// any of the value math runs?
//
// This is the other half of "going in a legitimate direction": a team in a
// teardown re-signing its 33-year-old to a four-year deal is how AI rosters
// used to end up as neither young nor good. A genuine star is exempt - you
// keep those and trade them, you don't let them leave for nothing.
export const shouldLetWalk = ({
	tier,
	age,
	amount,
	years,
	isStar,
	isCore,
	minContract,
}: {
	tier: TradePosture["tier"];
	age: number;
	amount: number;
	years: number;
	isStar: boolean;
	// One of this team's few best players, whatever that is worth league-wide.
	isCore?: boolean;
	minContract: number;
}): boolean => {
	// The league-relative exemption is not enough on its own. "Star" means roughly
	// the best player on an average team, so the WORST teams have nobody who
	// qualifies and would liquidate their entire rotation for nothing - which is
	// not a rebuild, and left doormats they could never climb out of. Measured
	// over eight seasons, team ovrs ranged -56 to 84 against -8 to 64 for stock
	// BBGM. Whoever your best players are, you keep them or you trade them.
	if (isStar || isCore) {
		return false;
	}

	// Cheap short deals are never the problem.
	if (amount <= minContract * 2 || years <= 1) {
		return false;
	}

	if (tier === "teardown") {
		return age >= 28;
	}
	if (tier === "seller") {
		return age >= 30;
	}
	// A fringe team should stop paying real money to keep players who will be
	// finished before it is good.
	if (tier === "fringe") {
		return age >= 33 && amount > minContract * 4;
	}

	return false;
};

// ---- Bargains: quality that costs nothing but a roster spot ---------------

// Vanilla will not sign a minimum-contract free agent unless the roster is two
// men short of full. The instinct behind that is right - a front office should
// not stuff its last seats with replacement-level bodies - but the tool is
// blunt, because it catches every minimum player, including the ones who are
// plainly better than somebody already on the roster.
//
// Measured over twenty seasons of thirty teams, that rule left between one and
// twenty-two healthy free agents rated 50 and up unsigned at the minimum EVERY
// summer, while eight to sixteen teams sat a man or two short of full and
// almost none was ever under thirteen - so the gate essentially never opened.
// The best of them each year rated 51 to 57 in a league averaging 46. That is
// a rotation player, for nothing, going nowhere.
//
// So: keep the instinct, replace the tool. A minimum deal is worth a roster
// spot when the player is a clear upgrade on the worst man already holding one.
// This is the margin at the LAST spot a team will fill; it eases as seats go
// spare (see findBargain).
export const BARGAIN_VALUE_MARGIN = 5;

// One seat stays empty regardless. A team still wants somewhere to put a
// midseason injury replacement, and the point here is to stop passing on
// quality, not to run every roster to the brim in July.
//
// Unless the injuries have already happened. A team that cannot field a legal
// rotation has no use for a spare seat - the emergency it was being saved for
// is the one it is in - so the last spot opens up. See shortHanded.
export const bargainRosterHeadroom = (
	maxRosterSize: number,
	shortHanded = false,
): number => (shortHanded ? maxRosterSize : maxRosterSize - 1);

// Too few healthy bodies to field a rotation. Tied to the league's own floor
// for how few players a team may carry, because that is the game's statement
// of what "enough" means; injured men count toward it but cannot play, which
// is exactly the hole this closes. Measured over eight real seasons of thirty
// teams, teams played 338 team-days with ten or fewer healthy players while
// useful free agents sat available at the minimum - and 314 of those had an
// open roster spot they simply never used. A front office signs a body.
export const shortHanded = ({
	healthyCount,
	minRosterSize,
}: {
	// Players fit to play. Undefined outside the season, when there is no game
	// tomorrow and nothing is urgent.
	healthyCount: number | undefined;
	minRosterSize: number;
}): boolean => healthyCount !== undefined && healthyCount < minRosterSize;

// A rebuilding team's last roster spots belong to players who will still be
// there when it is good again. Signing a 33-year-old to a minimum deal is free,
// but the minutes are not.
const bargainAgeLimit = (tier: TradePosture["tier"]): number => {
	if (tier === "teardown") {
		return 26;
	}
	if (tier === "seller") {
		return 29;
	}
	return Infinity;
};

// The best minimum-contract free agent worth a roster spot, or undefined if
// none of them is. Pure: candidates in, choice out.
export const findBargain = ({
	posture,
	candidates,
	worstRosterValue,
	rosterSize,
	maxRosterSize,
	healthyCount,
	minRosterSize,
	season,
	minContract,
	maxContract,
}: {
	posture: TradePosture;
	// Free agents asking the minimum, in any order.
	candidates: FaPlayer[];
	// Value of the least valuable player already on the roster.
	worstRosterValue: number;
	rosterSize: number;
	maxRosterSize: number;
	// Players fit to play right now. Only meaningful in season - see shortHanded.
	healthyCount?: number;
	minRosterSize: number;
	season: number;
	minContract: number;
	maxContract: number;
}): FaPlayer | undefined => {
	const short = shortHanded({ healthyCount, minRosterSize });

	if (rosterSize >= bargainRosterHeadroom(maxRosterSize, short)) {
		return undefined;
	}

	// A team short of bodies is not choosing between this player and the worst
	// man on its roster - it is choosing between him and a lineup it cannot
	// fill. Whoever is healthy is the upgrade, and the timeline can wait until
	// everyone is back.
	const ageLimit = short ? Infinity : bargainAgeLimit(posture.tier);

	// The fuller the roster, the better he has to be. The margin exists because
	// the last seat has option value - a team wants somewhere to put a midseason
	// addition - and that value falls away when there are several going spare.
	// A team with four empty seats is not choosing between this player and the
	// worst man on its roster; it is choosing between him and nobody. (Measured:
	// an eleven-man team passing on the best player in a 240-man free agent pool
	// because its own worst man rated two points higher.)
	const seatsToSpare = bargainRosterHeadroom(maxRosterSize, short) - rosterSize;
	const margin = BARGAIN_VALUE_MARGIN / seatsToSpare;

	let best: { p: FaPlayer; score: number } | undefined;
	for (const p of candidates) {
		// An injured minimum signing is exactly the warm body vanilla is right to
		// refuse: he cannot be an upgrade on anyone this season.
		if (p.injuredGames > 0) {
			continue;
		}
		if (p.age > ageLimit) {
			continue;
		}
		// The whole justification for the roster spot. Value rather than ovr, so
		// a 33-year-old does not displace a 22-year-old of the same rating.
		//
		// A short-handed team skips this outright rather than easing it. The
		// comparison is against the worst man on the roster because normally
		// that is who the newcomer is taking minutes from; a team that cannot
		// fill its lineup is not taking minutes from anybody, so there is no
		// one to be better than.
		if (!short && p.value < worstRosterValue + margin) {
			continue;
		}

		const score = scoreFreeAgent({
			p,
			posture,
			season,
			minContract,
			maxContract,
		});
		if (!best || score > best.score) {
			best = { p, score };
		}
	}

	return best?.p;
};
