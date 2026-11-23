import { packageOf } from "./packages.ts";
import type { Signature } from "./signature.ts";

// HOW QUICKLY HIS STEPS GO ROUND, at each pace: what makes a man's running
// look like his - neither the patter of a man whose steps are too short
// for how fast he goes, nor the slow loping of one whose are too long.
//
// Running, from real runners on a track (Dorn, Schache & Pandy 2012, J Exp
// Biol 215, table 2: nine runners, legs 0.93 m long): at 11.5 ft/s their
// steps went 2.62 a second, at 17 ft/s 2.94, at 22.8 3.50, at 29.5 4.36 -
// each step longer as they went faster, and quicker only nearer flat out.
// Jogging slower than that, a runner's steps hardly go round any slower:
// about 2.5 a second. Walking (from the usual measures of men walking):
// 1.25 steps a second at 1.6 ft/s, 1.9 at 4.6, 2.2 at 6.6.
//
// A bigger man's go round more slowly at the same pace, each step longer:
// men of different sizes move alike at the same pace for their legs (as
// v / sqrt(leg) - their Froude number), and then their steps go round as
// sqrt(leg) slower. His legs are his height's share of a real man's (a
// leg as long as the runners' on a man 5 ft 9 in tall).
const REF_INCHES = 69.3;
const RUN: readonly [number, number][] = [
	[0, 2.4],
	[7, 2.5],
	[11.45, 2.62],
	[16.96, 2.94],
	[22.83, 3.5],
	[29.49, 4.36],
];
const WALK: readonly [number, number][] = [
	[0, 1.1],
	[1.64, 1.25],
	[2.62, 1.55],
	[3.61, 1.75],
	[4.59, 1.9],
	[5.58, 2.05],
	[6.56, 2.2],
];
// Backpedaling, each step is shorter for the pace, and quicker, than going
// forward (backward running: about a tenth and a half quicker).
const BACKPEDAL = 1.15;

const along = (keys: readonly [number, number][], v: number): number => {
	if (v <= keys[0]![0]) {
		return keys[0]![1];
	}
	for (let i = 1; i < keys.length; i++) {
		const [x1, y1] = keys[i]!;
		if (v <= x1) {
			const [x0, y0] = keys[i - 1]!;
			return y0 + ((y1 - y0) * (v - x0)) / (x1 - x0);
		}
	}
	const [x0, y0] = keys.at(-2)!;
	const [x1, y1] = keys.at(-1)!;
	return y1 + ((y1 - y0) * (v - x1)) / (x1 - x0);
};

export type StepKind = "walk" | "run" | "backpedal" | "stance";

// What the picture needs of a man's own way of getting about: his
// signature, and how tall he is (inches) - carried on the timeline the new
// possession sim stages (see CourtTimeline.gaits).
export type OwnGait = { sig: Signature; inches: number };

// How tall his ratings say he is (inches): the league's own height rating,
// 66 inches at 0 to 93 at 100.
export const inchesOf = (hgt: number | undefined): number =>
	66 + 0.27 * (hgt ?? 45);

// His steps a second going v (feet a second), stepping that way: an
// ordinary man of his height's, quickened or slowed his own way (his
// package's - see packages.ts).
export const stepsPerSecond = (
	how: StepKind,
	v: number,
	inches: number,
	sig?: Signature,
): number => {
	const k = Math.sqrt(REF_INCHES / inches);
	const own = packageOf(sig?.[how] ?? "")?.timing.cadence ?? 1;
	const base =
		how === "walk"
			? along(WALK, v * k)
			: along(RUN, v * k) * (how === "backpedal" ? BACKPEDAL : 1);
	return base * k * own;
};
