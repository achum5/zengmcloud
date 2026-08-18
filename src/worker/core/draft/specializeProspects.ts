// SPECIALIZED DRAFT PROSPECTS.
//
// Stock BBGM draft classes are full of well-rounded players: most prospects are
// decent-ish at everything and prog into either all-around stars or all-around
// bench guys. Real draft classes are not like that. They are full of specialists
// - the three-point sniper who cannot finish inside, the post big who will never
// take a jumper, the slasher with no jumpshot at all.
//
// This makes good ratings better and bad ratings worse, which turns a flat
// prospect into a shaped one. Each rating is scaled up and then a flat penalty
// is subtracted, so there is a break-even point: above it a rating gains, below
// it a rating loses, and the further from it the bigger the move.
//
// The numbers come from a well-tested community script (ClevelandFan295's
// "Specialize Draft Prospects", r/BasketballGM), kept exactly as published
// rather than re-derived, because they were tuned against actual simmed leagues:
//
//   - Athletic ratings (hgt, stre, spd, jmp, endu) and the IQ ratings (oiq, diq)
//     are NOT touched at all. Those describe the athlete, not a skill, and
//     polarizing them makes broken players rather than specialists.
//   - Scoring/rebounding skills get the strong treatment (x1.44, break-even 34).
//   - Ballhandling-ish skills - ins, drb, pss - get a gentler one (x1.1,
//     break-even 40), because BBGM already generates plenty of pure passers and
//     handlers, so the strong version overshoots there.
//   - The caps (85 for ft/pss, 90 for the rest) stop the boost from minting
//     elite ratings out of merely-good ones.
//
// Applied to a whole generated class, this doesn't just reshape individuals - it
// reshapes the league, since every drafted player carries the shape for a
// career. That is the point.

import limitRating from "../player/limitRating.ts";
import type { PlayerWithoutKey } from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";

// Scale factors and the league-average rating each one is balanced around. The
// penalty is what an average rating would gain from the boost, so an average
// rating comes out unchanged and everything else spreads away from it.
const SKILL_BOOST = 1.44;
const SKILL_AVERAGE = 35.2;
const HANDLING_BOOST = 1.1;
const HANDLING_AVERAGE = 41;

export const SKILL_PENALTY = Math.round(
	SKILL_BOOST * SKILL_AVERAGE - SKILL_AVERAGE,
);
export const HANDLING_PENALTY = Math.round(
	HANDLING_BOOST * HANDLING_AVERAGE - HANDLING_AVERAGE,
);

type Rule = { boost: number; penalty: number; cap: number };

const SKILL = (cap: number): Rule => ({
	boost: SKILL_BOOST,
	penalty: SKILL_PENALTY,
	cap,
});
const HANDLING = (cap: number): Rule => ({
	boost: HANDLING_BOOST,
	penalty: HANDLING_PENALTY,
	cap,
});

// Only these. Anything absent here is deliberately left alone - see above.
export const SPECIALIZE_RULES: Record<string, Rule> = {
	ins: HANDLING(90),
	dnk: SKILL(90),
	ft: SKILL(85),
	fg: SKILL(90),
	tp: SKILL(90),
	drb: HANDLING(90),
	pss: HANDLING(85),
	reb: SKILL(90),
};

// One rating, specialized. Pure, so the shape of the curve is a test rather
// than a claim.
export const specializeRating = (value: number, rule: Rule): number =>
	Math.min(
		rule.cap,
		limitRating(Math.round(value * rule.boost) - rule.penalty),
	);

// Reshape one prospect's current ratings in place. The caller must recalculate
// ovr/pot/pos/skills afterwards (player.develop(p, 0)), since all four are
// derived from the ratings this just changed.
export const specializeProspect = (p: PlayerWithoutKey) => {
	const ratings = last(p.ratings) as any;

	for (const [key, rule] of Object.entries(SPECIALIZE_RULES)) {
		if (typeof ratings[key] === "number") {
			ratings[key] = specializeRating(ratings[key], rule);
		}
	}
};
