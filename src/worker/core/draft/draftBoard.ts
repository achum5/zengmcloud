import {
	posBucket,
	type PosBucket,
	type TradePosture,
} from "../trade/tradePosture.ts";

// ---------------------------------------------------------------------------
// THE DRAFT, RUN LIKE A FRONT OFFICE
//
// Free agency, re-signing and trades all ask what the franchise is actually
// trying to do (see freeAgents/frontOffice.ts and trade/tradePosture.ts). The
// draft did not. It scored every prospect as `value ** 69` for every team, so:
//
//   - A 58-win team and a 17-win team wanted the same player. One of them
//     needs someone who can play next season and the other needs the highest
//     ceiling in the room, and BBGM's `value` is a single blend of the two that
//     cannot tell them apart.
//   - A team with three young centers took a fourth, because nothing in the
//     score knew what was already on the roster.
//   - A team that took a guard at pick 4 took another at 34, because nothing
//     knew what it had done twenty minutes earlier.
//
// This is the missing board. It reuses the same posture the rest of the front
// office runs on, so a team's draft, its free agency and its trades finally
// come from one plan.
//
// EVERYTHING HERE IS A MULTIPLIER ON `value`, deliberately. The selection in
// runPicks raises the score to a high power, which is calibrated against
// value's 40-70 range - a score on any other scale would silently turn the
// draft deterministic or random. Anchoring on value also keeps the board
// honest: a team leans, it does not reach.
//
// Pure functions only - no database - so the strategy is unit-testable.
// ---------------------------------------------------------------------------

export type DraftProspect = {
	pid: number;
	ovr: number;
	pot: number;
	value: number;
	age: number;
	pos: string;
};

// ---- Upside versus readiness ----------------------------------------------

// THE ONE THING EVERY DRAFT ARGUMENT IS ABOUT. Two prospects with the same
// `value` can be a 21-year-old who is already a rotation player and a
// 19-year-old who is years away and might be a star. Which one you want is
// entirely a question of what your team is doing next season, and `value`
// averages that question away.
//
// Positive numbers lean toward potential, negative toward what a player can do
// now. A teardown cares only about the ceiling; a team going all-in cannot use
// a project at all, and would rather have someone who can take the floor.
export const UPSIDE_LEAN: Record<TradePosture["tier"], number> = {
	teardown: 0.55,
	seller: 0.4,
	fringe: 0.1,
	buyer: -0.15,
	allIn: -0.35,
};

// The gap between potential and current ability, as a share of what a prospect
// is. Divided by 40 because a 40-point pot-ovr gap is about as raw as anyone in
// a draft class gets, so the lean is expressed against the full realistic
// range rather than an arbitrary constant.
const RAWNESS = (p: DraftProspect) => Math.max(0, p.pot - p.ovr) / 40;

export const upsideMultiplier = (
	tier: TradePosture["tier"],
	p: DraftProspect,
): number => 1 + UPSIDE_LEAN[tier] * RAWNESS(p);

// ---- Age, inside the draft's narrow band ----------------------------------

// Free agency's age fit buckets everyone under 24 together, which is every
// prospect in the room. Within a draft class a year is a lot: a 19-year-old and
// a 22-year-old with the same rating are not the same asset, because the
// younger one has three more years of development in front of him and three
// more years of team control behind him.
//
// A rebuilding team is buying those years. A team trying to win now is not.
export const AGE_LEAN: Record<TradePosture["tier"], number> = {
	teardown: 0.035,
	seller: 0.025,
	fringe: 0.01,
	buyer: -0.01,
	allIn: -0.025,
};

// Ages either side of this are worth more or less depending on the lean. Draft
// classes centre near here, so the multiplier is 1 for a typical prospect and
// the adjustment stays small at the edges.
export const DRAFT_PIVOT_AGE = 21;

export const draftAgeMultiplier = (
	tier: TradePosture["tier"],
	age: number,
): number => {
	if (!Number.isFinite(age)) {
		return 1;
	}
	// Clamped so an implausible age in an imported league cannot dominate.
	const yearsYoung = Math.max(-4, Math.min(4, DRAFT_PIVOT_AGE - age));
	return 1 + AGE_LEAN[tier] * yearsYoung;
};

// ---- What the roster already has ------------------------------------------

// Need matters far less here than in free agency. A free agent is a finished
// player you are buying to fill a specific hole; a draft pick is an asset you
// will have for years, and the hole you have today is very unlikely to be the
// hole you have when he is good. Every real front office says "best player
// available" and then leans, which is exactly what this weight is.
export const DRAFT_NEED_WEIGHT = 0.4;

export const draftNeedMultiplier = (
	posture: Pick<TradePosture, "needs" | "surpluses" | "targetPos">,
	pos: string,
): number => {
	const bucket: PosBucket = posBucket(pos);

	let raw = 1;
	const need = posture.needs.find((n) => n.pos === bucket);
	const surplus = posture.surpluses.find((s) => s.pos === bucket);
	if (need) {
		raw = 1 + Math.min(0.35, need.severity / 40);
	} else if (surplus) {
		raw = Math.max(0.7, 1 - 0.12 * surplus.depth);
	} else if (posture.targetPos === bucket) {
		raw = 1.1;
	}

	return 1 + (raw - 1) * DRAFT_NEED_WEIGHT;
};

// WHAT THIS TEAM HAS ALREADY DONE TODAY.
//
// The roster the posture was built from is the roster before the draft started.
// A team that takes a centre at pick 4 still looks centre-hungry at pick 34,
// and used to take another one - the single most obviously wrong thing about
// the old draft, because it is the mistake no human has ever made.
//
// Steeper than the positional need above, because this is not a soft
// preference: you just spent a pick there.
export const REPEAT_POSITION_PENALTY = 0.12;

export const repeatPositionMultiplier = (
	alreadyDraftedAtPos: number,
	pos: string,
): number => {
	void pos;
	if (alreadyDraftedAtPos <= 0) {
		return 1;
	}
	return Math.max(0.6, 1 - REPEAT_POSITION_PENALTY * alreadyDraftedAtPos);
};

// ---- The board -------------------------------------------------------------

// Same reasoning as free agency's FIT_FLOOR/FIT_CEILING, and the same lesson.
// These multiply, so an old-for-the-draft, position-blocked, already-drafted-
// there prospect on a win-now team would otherwise come out at a fraction of
// his value and sort below players nobody would take in the second round.
//
// Tighter than free agency's band on purpose. Free agency is a market where
// somebody else will sign the player you pass on; a draft is a ranked list
// where passing on the best player in the room is a mistake you cannot undo.
export const DRAFT_FIT_FLOOR = 0.78;
export const DRAFT_FIT_CEILING = 1.22;

export const scoreProspect = ({
	p,
	posture,
	alreadyDraftedAtPos = 0,
}: {
	p: DraftProspect;
	posture: TradePosture;
	// How many players this team has already taken at this position in this
	// draft.
	alreadyDraftedAtPos?: number;
}): number => {
	const fit =
		upsideMultiplier(posture.tier, p) *
		draftAgeMultiplier(posture.tier, p.age) *
		draftNeedMultiplier(posture, p.pos) *
		repeatPositionMultiplier(alreadyDraftedAtPos, p.pos);

	const clamped = Math.max(DRAFT_FIT_FLOOR, Math.min(DRAFT_FIT_CEILING, fit));

	// Never zero, negative or NaN, whatever a strange league hands in. The
	// selection weights by this, and one bad weight does not spoil one pick - it
	// spoils the whole draw, because Math.max propagates NaN rather than
	// rejecting it.
	const score = p.value * clamped;
	return Number.isFinite(score) ? Math.max(0.01, score) : 0.01;
};
