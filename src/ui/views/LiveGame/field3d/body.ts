import { bodyOf, girthOf, type Body } from "../court3d/poses.ts";

// A FOOTBALL PLAYER'S BUILD: a grown man, not the court's cartoon athlete
// (see court3d/poses.ts) - his height the same way, but a man's
// proportions, and thickened the football way: a 320-pound tackle is a
// wall next to a receiver, not a basketball center a little broader than
// his guard, and every one of them heavier in the arms and legs than a
// basketball player.

// Lengths, over his height: a head an eighth of him or so (crown to chin
// 2.45 head radii - and his helmet a little over life size round it, to be
// made out at the broadcast's distance), on a short neck; his hips half
// way up him; his arms hanging to the middle of his thighs.
const ANKLE = 0.03;
const SHIN = 0.23;
const THIGH = 0.24;
const TORSO = 0.3;
const NECK = 0.042;
const HEAD = 0.072;
const UPPER = 0.18;
const FORE = 0.148;
const HAND = 0.03;

// The league's typical build, as a body mass index.
const FOOTBALL_BMI = 27.5;

export const footballGirth = (hgt: number, weight: number): number => {
	const bmi = (703 * weight) / (hgt * hgt);
	return Math.min(1.42, Math.max(0.88, (bmi / FOOTBALL_BMI) ** 0.9));
};

export const DEFAULT_FOOTBALL_HGT = 74;
export const DEFAULT_FOOTBALL_WEIGHT = 230;

export const footballBody = (
	hgt = DEFAULT_FOOTBALL_HGT,
	weight = DEFAULT_FOOTBALL_WEIGHT,
): Body => {
	const b = bodyOf(hgt, weight);
	const k = footballGirth(hgt, weight) / girthOf(hgt, weight);
	const { H } = b;
	return {
		...b,
		hipH: H * (ANKLE + SHIN + THIGH),
		ankleH: H * ANKLE,
		thigh: H * THIGH,
		shin: H * SHIN,
		torso: H * TORSO,
		neck: H * NECK,
		headR: H * HEAD,
		upper: H * UPPER,
		fore: H * FORE,
		handR: H * HAND,
		// (Broad across the shoulders, and deeper through the chest - and
		// his pads deeper still, see sculpt.ts.)
		shoulderW: b.shoulderW * k * 1.1,
		hipW: b.hipW * k * 1.04,
		depth: b.depth * k * 1.18,
		kneeR: b.kneeR * k * 1.1,
		// (Arms and legs heavier than a basketball player's too.)
		thighR: b.thighR * k * 1.22,
		calfR: b.calfR * k * 1.14,
		upperR: b.upperR * k * 1.24,
		foreR: b.foreR * k * 1.14,
	};
};

// An official: a man of ordinary build, in no pads.
export const officialBody = (hgt = 72, weight = 205): Body => {
	const b = footballBody(hgt, weight);
	return { ...b, depth: b.depth / 1.18 };
};

// A cheerleader: slighter than an official, narrower across the shoulders,
// lighter in the arms.
export const cheerBody = (hgt = 66, weight = 125): Body => {
	const b = officialBody(hgt, weight);
	return {
		...b,
		shoulderW: b.shoulderW * 0.84,
		depth: b.depth * 0.92,
		upperR: b.upperR * 0.78,
		foreR: b.foreR * 0.82,
		calfR: b.calfR * 0.92,
	};
};

// Bigger than life on the field, as a game draws them, so a man can be
// picked out at the broadcast's distance: every length of him this much
// over. (Only as drawn - where he is and how high anything goes stays
// true, scaled the same way up off the turf when it is drawn.)
export const FIGURE_SCALE = 1.2;
export const drawnBody = (b: Body): Body => {
	const out = { ...b };
	for (const k of Object.keys(out) as (keyof Body)[]) {
		out[k] = b[k] * FIGURE_SCALE;
	}
	return out;
};
