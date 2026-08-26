import { helpers } from "../../util/index.ts";
import playThroughInjuriesFactor from "../../../common/playThroughInjuriesFactor.ts";
import compositeRating from "../player/compositeRating.ts";
import { COMPOSITE_WEIGHTS } from "../../../common/constants.ts";
import type { MinimalPlayerRatings } from "../../../common/types.ts";
import { isSport } from "../../../common/sportFunctions.ts";

// ---------------------------------------------------------------------------
// LINEUP SYNERGY, IN ONE PLACE
//
// Five players do not add up. A lineup with two shooters, a ball-handler and a
// rim protector scores and defends better than five men of the same individual
// quality who all do the same thing, and the game sim has always modelled that
// - it adds up to 0.1 (0.25 in the playoffs) to a team's dribbling, passing,
// rebounding, defense, perimeter defense and blocking, purely from the MIX of
// what is on the floor.
//
// Nothing outside the sim knew any of this existed. team.ovr, which is what the
// front office builds toward, is a weighted sum of individual overalls and
// cannot see a roster of five identical players as anything but the sum of
// five players. So a front office optimising team.ovr optimises the wrong
// thing, slightly, every time it has a choice between comparable players.
//
// This was inside GameSim, reading GameSim's own player objects. It is now a
// pure function of composite ratings so the front office can ask the same
// question the sim will ask on game night, off the same code. A second
// implementation would drift within a season - the cutoffs here are already
// documented as needing to stay in sync with the ones in player/skills.ts.
//
// WHAT THE LEAGUE ACTUALLY LOOKS LIKE, measured over six twelve-season runs
// (the SYNERGY rows in freeAgents/decadesSim.test.ts):
//
//   off 0.44 of a possible 1.00   def 0.51 of 0.83   reb 0.26 of 0.50
//   the average starting five holds 1.71 three-point shooters
//
// That last number is the interesting one. The single largest term in
// offensive synergy - five of the seventeen points it is built from - is a
// sigmoid on the shooter count centred at TWO, and the league sits below it,
// on the CONVEX side, where a team already at 1.8 shooters gains about
// fourteen times as much from one more as a team at 0.4 does. On paper that
// makes lineup fit a rare non-zero-sum lever: teams that sorted themselves,
// the near-threshold ones paying up and the rest building another way, would
// raise league synergy out of the same players.
//
// AND FREE AGENCY IS NOT WHERE THAT HAPPENS. A fit multiplier on
// scoreFreeAgent, weighting a signing by how much it would raise the signing
// team's own starting five, was built and measured over six seeds: league
// synergy moved -0.005 out of 2.71, and the shooter count 1.74 -> 1.71.
// Nothing. It was not inert - every seed's league diverged from the second
// season on - it simply did not accumulate.
//
// The reason is worth keeping, because it applies to any league-wide
// preference: scoreFreeAgent picks which player a team WANTS MOST, not what it
// bids, and whoever it passes on is signed by somebody else within the day.
// Cap room, mood and asking price decide who actually lands where. So every
// team reordering its shopping list against the same fixed pool of shooters
// reshuffles the allocation without improving it.
//
// If fit is worth chasing at all it has to be somewhere the allocation is
// FIXED rather than bid for - a draft slot, or a two-team swap where both
// sides have to agree and both can come out ahead. What none of this settles
// is whether the prize is big enough to be worth a point of talent, which is
// what SYNERGY PRICE in the harness exists to answer.
// ---------------------------------------------------------------------------

// The composites synergy is computed from - the only ones it reads.
export type SynergyCompositeRating = {
	shootingThreePointer: number;
	athleticism: number;
	dribbling: number;
	defenseInterior: number;
	defensePerimeter: number;
	shootingLowPost: number;
	passing: number;
	rebounding: number;
};

export type Synergy = {
	off: number;
	def: number;
	reb: number;
};

// FRACTIONAL skills, not the discrete badges shown next to a player's name. A
// player just under the cutoff still contributes most of a skill, and a lineup
// of four such players is not skill-less. The cutoffs match player/skills.ts,
// which is why both files carry the same warning.
export const synergySkillCounts = (
	players: readonly { compositeRating: SynergyCompositeRating }[],
) => {
	const counts = {
		"3": 0,
		A: 0,
		B: 0,
		Di: 0,
		Dp: 0,
		Po: 0,
		Ps: 0,
		R: 0,
	};

	for (const p of players) {
		const c = p.compositeRating;
		counts["3"] += helpers.sigmoid(c.shootingThreePointer, 15, 0.59);
		counts.A += helpers.sigmoid(c.athleticism, 15, 0.63);
		counts.B += helpers.sigmoid(c.dribbling, 15, 0.68);
		counts.Di += helpers.sigmoid(c.defenseInterior, 15, 0.57);
		counts.Dp += helpers.sigmoid(c.defensePerimeter, 15, 0.61);
		counts.Po += helpers.sigmoid(c.shootingLowPost, 15, 0.61);
		counts.Ps += helpers.sigmoid(c.passing, 15, 0.63);
		counts.R += helpers.sigmoid(c.rebounding, 15, 0.61);
	}

	return counts;
};

// The three synergy numbers for a lineup. Each lands in 0..1 and is multiplied
// by the sim's synergyFactor before being added to the team's composites.
export const synergyFromSkillCounts = (
	skillsCount: ReturnType<typeof synergySkillCounts>,
): Synergy => {
	// Base offensive synergy
	let off = 0;
	off += 5 * helpers.sigmoid(skillsCount["3"], 3, 2); // 5 / (1 + e^-(3 * (x - 2))) from 0 to 5

	off +=
		3 * helpers.sigmoid(skillsCount.B, 15, 0.75) +
		helpers.sigmoid(skillsCount.B, 5, 1.75); // 3 / (1 + e^-(15 * (x - 0.75))) + 1 / (1 + e^-(5 * (x - 1.75))) from 0 to 5

	off +=
		3 * helpers.sigmoid(skillsCount.Ps, 15, 0.75) +
		helpers.sigmoid(skillsCount.Ps, 5, 1.75) +
		helpers.sigmoid(skillsCount.Ps, 5, 2.75); // 3 / (1 + e^-(15 * (x - 0.75))) + 1 / (1 + e^-(5 * (x - 1.75))) + 1 / (1 + e^-(5 * (x - 2.75))) from 0 to 5

	off += helpers.sigmoid(skillsCount.Po, 15, 0.75); // 1 / (1 + e^-(15 * (x - 0.75))) from 0 to 5

	off +=
		helpers.sigmoid(skillsCount.A, 15, 1.75) +
		helpers.sigmoid(skillsCount.A, 5, 2.75); // 1 / (1 + e^-(15 * (x - 1.75))) + 1 / (1 + e^-(5 * (x - 2.75))) from 0 to 5

	off /= 17; // Punish teams for not having multiple perimeter skills

	const perimFactor =
		helpers.bound(
			Math.sqrt(1 + skillsCount.B + skillsCount.Ps + skillsCount["3"]) - 1,
			0,
			2,
		) / 2; // Between 0 and 1, representing the perimeter skills

	off *= 0.5 + 0.5 * perimFactor;

	// Defensive synergy
	let def = 0;
	def += helpers.sigmoid(skillsCount.Dp, 15, 0.75); // 1 / (1 + e^-(15 * (x - 0.75))) from 0 to 5

	def += 2 * helpers.sigmoid(skillsCount.Di, 15, 0.75); // 2 / (1 + e^-(15 * (x - 0.75))) from 0 to 5

	def +=
		helpers.sigmoid(skillsCount.A, 5, 2) +
		helpers.sigmoid(skillsCount.A, 5, 3.25); // 1 / (1 + e^-(5 * (x - 2))) + 1 / (1 + e^-(5 * (x - 3.25))) from 0 to 5

	def /= 6;

	// Rebounding synergy
	let reb = 0;
	reb +=
		helpers.sigmoid(skillsCount.R, 15, 0.75) +
		helpers.sigmoid(skillsCount.R, 5, 1.75); // 1 / (1 + e^-(15 * (x - 0.75))) + 1 / (1 + e^-(5 * (x - 1.75))) from 0 to 5

	reb /= 4;

	return { off, def, reb };
};

export const synergyForLineup = (
	players: readonly { compositeRating: SynergyCompositeRating }[],
): Synergy => synergyFromSkillCounts(synergySkillCounts(players));

// The composites synergy reads, straight off a ratings row.
//
// The sim gets these from loadTeams, which builds every composite for every
// player in a game. A front office weighing a signing has only the ratings, and
// wants eight numbers rather than twenty, so this computes exactly the eight -
// with fuzz off, matching loadTeams, because fuzz is the user's uncertainty and
// the AI is not the one being kept honest by it.
const SYNERGY_COMPOSITES = [
	"shootingThreePointer",
	"athleticism",
	"dribbling",
	"defenseInterior",
	"defensePerimeter",
	"shootingLowPost",
	"passing",
	"rebounding",
] as const;

export const synergyCompositeRating = (
	ratings: MinimalPlayerRatings,
): SynergyCompositeRating => {
	const out = {} as SynergyCompositeRating;
	// Synergy is a basketball model. The other sports share the callers (free
	// agency is one file for all four), so answer with zeros rather than
	// computing eight meaningless weighted sums off whatever their composites
	// happen to be called - nothing downstream reads them, because a posture
	// only carries a lineup in basketball.
	if (!isSport("basketball")) {
		for (const key of SYNERGY_COMPOSITES) {
			out[key] = 0;
		}
		return out;
	}
	for (const key of SYNERGY_COMPOSITES) {
		const weightInfo = COMPOSITE_WEIGHTS[key];
		out[key] = weightInfo
			? compositeRating(ratings, weightInfo.ratings, weightInfo.weights, false)
			: 0;
	}
	return out;
};

// What a lineup's synergy is worth as ONE number, for comparing rosters.
//
// The sim spends these three on six different composites - off on dribbling and
// passing, def on defense, perimeter defense and blocking, reb on rebounding -
// so a team gets paid for defensive synergy on three fronts and for rebounding
// on one. Weighting by that count is the only non-arbitrary way to add them up,
// and it matters: def and reb do not even share off's 0..1 range (they top out
// at 5/6 and 1/2), so a plain sum would quietly value rebounding at half what
// it looked like.
export const synergyTotal = (s: Synergy): number =>
	2 * s.off + 3 * s.def + s.reb;

// --- Pregame synergy, for the point spread --------------------------------
//
// The spread formula (common/getGameSpread.ts) takes an optional synergy
// difference, measured to be worth 8.6 points of margin per synergyTotal unit
// against the engine. These build that number BEFORE a game, from whichever
// shape of roster the caller has.
//
// The predictor is a 70/30 blend of the first five and the second five - the
// shape that measured best against the engine (starters alone was ~0.2 points
// worse). Fewer than ten available men falls back to the first five; fewer
// than five is no reading at all, and the spread falls back to its ovr-only
// model rather than pricing a lineup that does not exist.
const PREGAME_STARTER_WEIGHT = 0.7;

// From a PROCESSED team (loadTeams/processTeam output): players are already in
// rotation order, injury-factored, and carrying every composite. This is
// exactly the roster the sim is about to be handed, which makes it the exact
// quantity the coefficient was fitted on.
export const pregameLineupSynergy = (
	players: readonly {
		injured?: boolean;
		compositeRating: SynergyCompositeRating;
	}[],
): number | undefined => {
	if (!isSport("basketball")) {
		return undefined;
	}
	const available = players.filter((p) => !p.injured);
	if (available.length < 5) {
		return undefined;
	}
	const first = synergyTotal(synergyForLineup(available.slice(0, 5)));
	if (available.length < 10) {
		return first;
	}
	const second = synergyTotal(synergyForLineup(available.slice(5, 10)));
	return PREGAME_STARTER_WEIGHT * first + (1 - PREGAME_STARTER_WEIGHT) * second;
};

// From RAW player rows (schedule view, sportsbook), where the game is still in
// the future. Availability mirrors team.ovr's accountForInjuredPlayers rule -
// back in time for the game, or playing through - and a man playing through has
// his composites damped by the same factor loadTeams would apply. Ordered by
// value, the same ordering the game's own roster sort produces for AI teams.
export const pregameLineupSynergyFromPlayers = (
	players: readonly {
		injury: { gamesRemaining: number };
		ratings: MinimalPlayerRatings[] | readonly MinimalPlayerRatings[];
		value: number;
	}[],
	{
		numDaysInFuture,
		playThroughInjuries,
		playoffs,
	}: {
		numDaysInFuture: number;
		playThroughInjuries: [number, number];
		playoffs: boolean;
	},
): number | undefined => {
	if (!isSport("basketball")) {
		return undefined;
	}
	const currentPlayThroughInjuries = playThroughInjuries[playoffs ? 1 : 0]!;
	const available: {
		value: number;
		compositeRating: SynergyCompositeRating;
	}[] = [];
	for (const p of players) {
		const gamesRemaining = p.injury.gamesRemaining - numDaysInFuture;
		let factor;
		if (gamesRemaining <= 0) {
			factor = 1;
		} else if (gamesRemaining <= currentPlayThroughInjuries) {
			factor = playThroughInjuriesFactor(gamesRemaining);
		} else {
			continue;
		}
		const ratings = p.ratings.at(-1);
		if (!ratings) {
			continue;
		}
		const composite = synergyCompositeRating(ratings);
		if (factor !== 1) {
			for (const key of Object.keys(
				composite,
			) as (keyof SynergyCompositeRating)[]) {
				composite[key] *= factor;
			}
		}
		available.push({ value: p.value, compositeRating: composite });
	}
	available.sort((x, y) => y.value - x.value);
	return pregameLineupSynergy(available);
};
