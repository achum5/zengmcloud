import { COMPOSITE_WEIGHTS } from "../../../common/constants.ts";
import compositeRating from "../player/compositeRating.ts";

// WHAT A LINEUP IS WORTH, measured in this game's own sim.
//
// Overall is a linear fit of plus-minus on ratings, so two players with the same
// overall can be worth very different amounts on the floor, and a human who
// notices that can fleece any AI that prices by it. This is fit instead on
// simulated games (src/test/impact), one row per ten-man stint, so a player is
// only ever credited with what happened while he was on the floor.
//
// A lineup is not five players added up. On every possession the sim hands the
// shot to one of the five, in proportion to usage to the 1.25 power, so what a
// scorer is worth depends on who else wants the ball. A lineup's value is
// therefore three things:
//
//   - what each player adds regardless of role (defense, rebounding, ...),
//   - each player's offense weighted by his share of the shots,
//   - how concentrated those shots are.
//
// Every composite rating the sim reads is a weighted average of the fifteen
// ratings below, so linear and squared terms in them cover what it can see.

export const IMPACT_RATINGS = [
	"hgt",
	"stre",
	"spd",
	"jmp",
	"endu",
	"ins",
	"dnk",
	"ft",
	"fg",
	"tp",
	"oiq",
	"diq",
	"drb",
	"pss",
	"reb",
] as const;

export type ImpactRatings = Record<(typeof IMPACT_RATINGS)[number], number>;

const NUM_RATINGS = IMPACT_RATINGS.length;

// Each rating and its square, on a 0-1 scale.
export const NUM_PLAYER_FEATURES = 2 * NUM_RATINGS;

export const playerFeatures = (ratings: ImpactRatings): number[] => {
	const out = new Array<number>(NUM_PLAYER_FEATURES);
	for (let i = 0; i < NUM_RATINGS; i++) {
		const r = ratings[IMPACT_RATINGS[i]!] / 100;
		out[i] = r;
		out[NUM_RATINGS + i] = r * r;
	}
	return out;
};

// How hard the sim pushes the ball to him: usage, to the power it uses.
export const shotWeight = (ratings: ImpactRatings) =>
	compositeRating(
		ratings as any,
		COMPOSITE_WEIGHTS.usage!.ratings,
		COMPOSITE_WEIGHTS.usage!.weights,
		false,
	) ** 1.25;

// Role-free part, shot-weighted part, then shot concentration.
export const NUM_LINEUP_FEATURES = 2 * NUM_PLAYER_FEATURES + 1;

// A lineup's features given each player's features and his share of the shots.
// Works for any per-player feature set: the output is twice its length, plus
// one.
export const lineupFeatures = (
	players: readonly { features: readonly number[]; share: number }[],
): number[] => {
	const numPlayerFeatures = players[0]?.features.length ?? NUM_PLAYER_FEATURES;
	const out = new Array<number>(2 * numPlayerFeatures + 1).fill(0);
	for (const { features, share } of players) {
		for (let i = 0; i < numPlayerFeatures; i++) {
			out[i]! += features[i]!;
			out[numPlayerFeatures + i]! += share * features[i]!;
		}
		out[2 * numPlayerFeatures]! += share * share;
	}
	return out;
};

// Shares of the shots within a lineup, with the sim's floor: nobody on the
// floor gets less than 5% of the weight there is.
export const shotShares = (weights: readonly number[]) => {
	const total = weights.reduce((a, w) => a + w, 0);
	const floored = weights.map((w) => Math.max(w, 0.05 * total));
	const sum = floored.reduce((a, w) => a + w, 0);
	return floored.map((w) => (sum > 0 ? w / sum : 1 / weights.length));
};
