import type { PosBucket, TradePosture } from "../trade/tradePosture.ts";
import { posBucket } from "../trade/tradePosture.ts";

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

	// A team already paying the tax with nothing to show for it should not be
	// adding salary at all.
	if (posture.cap.wantsRelief && p.amount > minContract * 1.5) {
		fit *= 0.5;
	}

	// Nobody's first choice is a player who can't play yet.
	if (p.injuredGames > 0) {
		fit *= 0.85;
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

	return score;
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
	// Holding space is meaningless without a cap to hold it under.
	if (salaryCapType === "none") {
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
		const target = p.amount * PURSUIT_PRICE_PATIENCE;

		// Credible only if the room would genuinely be there for him.
		if (payroll + target > salaryCap) {
			continue;
		}
		const score = pursuitScore({
			p,
			posture,
			season,
			minContract,
			maxContract,
		});
		if (!best || score > best.score) {
			best = { p, target, score };
		}
	}

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
	wantsRelief,
	ovr,
	replacementOvr,
}: {
	tier: TradePosture["tier"];
	// 0-based position among his own team's players by OVR.
	rosterRank: number;
	// A star by league-wide standards.
	isStar: boolean;
	age: number;
	// Already paying a tax it cannot justify.
	wantsRelief: boolean;
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

	// A team already over the tax with nothing to show for it has no business
	// bidding against itself.
	if (wantsRelief) {
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

	return Math.min(MAX_RETENTION_OVERPAY, 1 + over);
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
