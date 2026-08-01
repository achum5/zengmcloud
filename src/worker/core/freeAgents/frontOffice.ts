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
		tier === "allIn" ? 0.02 : tier === "buyer" ? 0.05 : tier === "fringe" ? 0.09 : 0.14;
	return Math.max(0.4, 1 - overage * tolerance);
};

// What this free agent is worth TO THIS TEAM. Ordering by this instead of by
// raw value is what stops every team wanting the same player.
export const scoreFreeAgent = ({
	p,
	posture,
	season,
	minContract,
}: {
	p: FaPlayer;
	posture: TradePosture;
	season: number;
	minContract: number;
}): number => {
	const years = Math.max(1, p.exp - season + 1);

	let score = p.value;

	// A rebuilder is buying the player's future, a win-now team his present.
	// value already blends the two; this tilts it.
	// `value` already blends present and future; this tilts it hard enough to
	// actually decide between a 22-year-old project and a 31-year-old starter,
	// which is the choice these two kinds of team should answer differently.
	if (posture.tier === "teardown" || posture.tier === "seller") {
		score += (p.pot - p.ovr) * 0.4;
	} else if (posture.tier === "allIn") {
		score += (p.ovr - p.pot) * 0.4;
	} else if (posture.tier === "buyer") {
		score += (p.ovr - p.pot) * 0.15;
	}

	score *= ageFitMultiplier(posture.tier, p.age);
	score *= positionFitMultiplier(posture, p.pos);
	score *= contractRiskMultiplier({
		tier: posture.tier,
		age: p.age,
		years,
		amount: p.amount,
		minContract,
	});

	// A team already paying the tax with nothing to show for it should not be
	// adding salary at all.
	if (posture.cap.wantsRelief && p.amount > minContract * 1.5) {
		score *= 0.5;
	}

	// Nobody's first choice is a player who can't play yet.
	if (p.injuredGames > 0) {
		score *= 0.85;
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
}: {
	p: FaPlayer;
	posture: TradePosture;
	season: number;
	minContract: number;
}): number => {
	const fit = scoreFreeAgent({ p, posture, season, minContract });

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
		const score = pursuitScore({ p, posture, season, minContract });
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
	minContract,
}: {
	tier: TradePosture["tier"];
	age: number;
	amount: number;
	years: number;
	isStar: boolean;
	minContract: number;
}): boolean => {
	if (isStar) {
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
