// REALISTIC DRAFT PROSPECTS.
//
// Specialized Draft Prospects (specializeProspects.ts) stretches each skill
// rating away from a fixed break-even point. That makes classes more lopsided,
// but a stretch can't change WHICH skills come bundled together, and that is
// where generated players differ most from real ones. genRatings rolls ft, fg
// and tp off one shared shooting factor and drb, pss and reb off one shared
// skill factor, so a generated shooter is good at every kind of shot (tp vs fg
// correlate 0.8; in real NBA rotation players, 0.28) and every ballhandler is
// also a passer (0.8 vs 0.53). Real players also split into 3-point shooters
// and non-shooters with few in between, and their dunking, mid-range and
// 3-point ratings are far more spread out, while passing is LESS spread out.
//
// This reshapes the eight skill ratings of a generated prospect so a class
// matches real NBA players in all of that - how spread out each skill is, the
// shape of its distribution, and which skills go together - measured on the
// game's own real-player ratings (2023-2026 rotation players). It works on
// ranks (a Gaussian copula):
//
//   1. Each rating becomes a normal score from where it ranks among generated
//      prospects (source).
//   2. Each skill score is split into the part the prospect's body explains
//      (hgt, stre, spd, jmp - bodySlopes, as the game generates it) and the
//      rest. Only the rest is reshaped (reshape), which is what pairs skills
//      like real players without tying skill to athleticism any harder than
//      the game already does - real data has that tie only because the players
//      who made the league are the ones good at several things, and copying it
//      just makes athletic prospects better.
//   3. The new score is turned back into a rating from the real-NBA-shaped
//      distribution for that skill (target).
//
// Body ratings and both IQ ratings are never touched, and each skill's average
// stays where stock generation puts it, so league scoring, turnovers and the
// like stay calibrated. The tables come out of
// realisticProspectsCalibration.test.ts (into realisticProspectsData.ts), which
// builds leagues from generated classes, ages them, sims them and iterates
// until their rotation players look like the real ones. Rerun it rather than
// editing the tables by hand.

import limitRating from "../player/limitRating.ts";
import type { PlayerWithoutKey } from "../../../common/types.ts";
import { last } from "../../../common/utils.ts";
import { REALISTIC_PROSPECTS } from "./realisticProspectsData.ts";

export const BODY_KEYS = ["hgt", "stre", "spd", "jmp"] as const;
export const SKILL_KEYS = [
	"ins",
	"dnk",
	"ft",
	"fg",
	"tp",
	"drb",
	"pss",
	"reb",
] as const;

// The quantiles every distribution table is given at - finer in the tails,
// where the non-shooters and the elite ratings live.
export const QUANTILES = [
	0.0025, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.125, 0.15, 0.2,
	0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.875,
	0.9, 0.925, 0.95, 0.96, 0.97, 0.98, 0.99, 0.995, 0.9975,
] as const;

export type ReshapeTables = {
	// Generated prospects' distribution of every body and skill rating.
	source: Record<
		(typeof BODY_KEYS)[number] | (typeof SKILL_KEYS)[number],
		readonly number[]
	>;
	// How much of each skill's normal score the body ratings explain.
	bodySlopes: readonly (readonly number[])[];
	// Applied to the rest of each skill's score.
	reshape: readonly (readonly number[])[];
	// The spread of each reshaped score, to bring it back to a normal score.
	spread: readonly number[];
	// The real-NBA-shaped distribution each skill comes out in.
	target: Record<(typeof SKILL_KEYS)[number], readonly number[]>;
};

// Where x falls in a distribution given by its value at each of QUANTILES
// (piecewise linear between them).
export const quantileOf = (table: readonly number[], x: number) => {
	if (x <= table[0]!) {
		return QUANTILES[0]! / 2;
	}
	if (x >= table.at(-1)!) {
		return (1 + QUANTILES.at(-1)!) / 2;
	}
	for (let i = 1; i < table.length; i++) {
		if (x <= table[i]!) {
			const lo = table[i - 1]!;
			const hi = table[i]!;
			const t = hi > lo ? (x - lo) / (hi - lo) : 0.5;
			return QUANTILES[i - 1]! + t * (QUANTILES[i]! - QUANTILES[i - 1]!);
		}
	}
	return QUANTILES.at(-1)!;
};

// The inverse: the value at quantile q.
export const valueAt = (table: readonly number[], q: number) => {
	if (q <= QUANTILES[0]!) {
		return table[0]!;
	}
	for (let i = 1; i < QUANTILES.length; i++) {
		if (q <= QUANTILES[i]!) {
			const t = (q - QUANTILES[i - 1]!) / (QUANTILES[i]! - QUANTILES[i - 1]!);
			return table[i - 1]! + t * (table[i]! - table[i - 1]!);
		}
	}
	return table.at(-1)!;
};

// Inverse standard normal CDF (Acklam's approximation, ~1e-9).
export const normInv = (p: number) => {
	const a = [
		-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.357751867269,
		-30.66479806614716, 2.506628277459239,
	] as const;
	const b = [
		-54.47609879822406, 161.5858368580409, -155.6989798598866,
		66.80131188771972, -13.28068155288572,
	] as const;
	const c = [
		-0.007784894002430293, -0.3223964580411365, -2.400758277161838,
		-2.549732539343734, 4.374664141464968, 2.938163982698783,
	] as const;
	const d = [
		0.007784695709041462, 0.3224671290700398, 2.445134137142996,
		3.754408661907416,
	] as const;
	const tail = (q: number) =>
		(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
		((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
	if (p < 0.02425) {
		return tail(Math.sqrt(-2 * Math.log(p)));
	}
	if (p > 1 - 0.02425) {
		return -tail(Math.sqrt(-2 * Math.log(1 - p)));
	}
	const q = p - 0.5;
	const r = q * q;
	return (
		((((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) *
			q) /
		(((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
	);
};

// Standard normal CDF (Abramowitz-Stegun 7.1.26, ~1e-7).
export const normCdf = (z: number) => {
	const t = 1 / (1 + 0.3275911 * (Math.abs(z) / Math.SQRT2));
	const y =
		1 -
		((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) *
			t +
			0.254829592) *
			t *
			Math.exp(-(z * z) / 2);
	return z >= 0 ? (1 + y) / 2 : (1 - y) / 2;
};

const mulVec = (matrix: readonly (readonly number[])[], v: number[]) =>
	matrix.map((row) => row.reduce((total, x, j) => total + x * v[j]!, 0));

// Ratings are whole numbers, so many prospects share each value. Spreading
// each one uniformly over its half-point either side gives tied prospects
// different ranks instead of all landing on the same one.
const normalScore = (table: readonly number[], rating: number) =>
	normInv(quantileOf(table, rating + Math.random() - 0.5));

// Reshape one prospect's current ratings in place, with the given tables (the
// calibration passes its own; everyone else wants realisticProspect).
export const reshapeProspect = (
	p: Pick<PlayerWithoutKey, "ratings">,
	tables: ReshapeTables,
) => {
	const ratings = last(p.ratings) as any;

	const body = BODY_KEYS.map((key) =>
		normalScore(tables.source[key], ratings[key]),
	);
	const skills = SKILL_KEYS.map((key) =>
		normalScore(tables.source[key], ratings[key]),
	);

	const fromBody = mulVec(tables.bodySlopes, body);
	const reshaped = mulVec(
		tables.reshape,
		skills.map((z, i) => z - fromBody[i]!),
	);

	SKILL_KEYS.forEach((key, i) => {
		const z = (fromBody[i]! + reshaped[i]!) / tables.spread[i]!;
		ratings[key] = limitRating(
			Math.round(valueAt(tables.target[key], normCdf(z))),
		);
	});
};

// The caller must recalculate ovr/pot/pos/skills afterwards
// (player.develop(p, 0)), since all four are derived from the ratings this
// just changed.
export const realisticProspect = (p: PlayerWithoutKey) => {
	reshapeProspect(p, REALISTIC_PROSPECTS);
};
