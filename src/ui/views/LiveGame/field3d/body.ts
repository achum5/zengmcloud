import { bodyOf, girthOf, type Body } from "../court3d/poses.ts";

// A FOOTBALL PLAYER'S BUILD: the court's cartoon athlete (see court3d/
// poses.ts), his height the same way, but thickened the football way - a
// 320-pound tackle is a wall next to a receiver, not a basketball center a
// little broader than his guard - and every one of them heavier in the arms
// and legs than a basketball player.

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
	return {
		...b,
		shoulderW: b.shoulderW * k,
		hipW: b.hipW * k,
		// (Deeper through the chest - and his pads deeper still, see
		// sculpt.ts.)
		depth: b.depth * k * 1.18,
		kneeR: b.kneeR * k,
		// (Arms and legs a little heavier than a basketball player's too.)
		thighR: b.thighR * k * 1.1,
		calfR: b.calfR * k * 1.06,
		upperR: b.upperR * k * 1.12,
		foreR: b.foreR * k * 1.08,
	};
};

// An official: a man of ordinary build, in no pads.
export const officialBody = (hgt = 72, weight = 205): Body => {
	const b = footballBody(hgt, weight);
	return { ...b, depth: b.depth / 1.18 };
};
