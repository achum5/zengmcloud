// WHAT HIS BODY CAN DO ON THE FLOOR, from his own ratings (see README.md).
//
// Measured off real NBA player tracking (tools/court3d-tape/nba, 17 games):
// a man's top speed in games runs from about 17.8 ft/s (a slow big) to 21.2
// (a quick guard), 5th to 95th percentile; from a standstill the quickest
// guards are at 12 ft/s in 0.8-0.9 s at their quickest, bigs in 1.3-1.5;
// speeding up, braking and turning (sideways) all top out at about 14-18
// ft/s2 whatever the pace. So every man moves like an NBA player - the
// spread between the best and the rest is a fifth or so, not a different
// kind of mover - and his ratings say where in it he is.

import { type CourtRatings, levelOf } from "../ratings.ts";

export type Body = {
	// Flat out (feet a second).
	top: number;
	// Pushing off from a standstill (feet a second, a second) - less the
	// nearer he is to flat out.
	go: number;
	// The hardest he brakes.
	brake: number;
	// The hardest he turns, sideways, at any pace.
	side: number;
	// How quickly a push builds or lets go (feet a second, a second, a
	// second): a first step is a step, not a jolt.
	jerk: number;
	// Turning himself round, stood (radians a second).
	spin: number;
};

// Nothing to go by: the man in the middle.
const MID = 0.5;

// How quick he is off the mark (0 to 1): his speed, and a big man less so.
export const quicknessOf = (r: CourtRatings | undefined): number => {
	const spd = levelOf(r, ["spd"]) ?? MID;
	const hgt = levelOf(r, ["hgt"]) ?? MID;
	return Math.min(1, Math.max(0, 0.75 * spd + 0.25 * (1 - hgt)));
};

// `tired` (0 to 1): worn down, he has less of his top speed and less still
// of his burst.
export const bodyOf = (r: CourtRatings | undefined, tired = 0): Body => {
	const spd = levelOf(r, ["spd"]) ?? MID;
	const q = quicknessOf(r);
	const t = Math.min(1, Math.max(0, tired));
	return {
		top: (17.6 + 4 * spd) * (1 - 0.06 * t),
		go: (14 + 7 * q) * (1 - 0.12 * t),
		brake: 15 + 5 * q,
		side: 14 + 4 * q,
		jerk: 100 + 60 * q,
		spin: 8 + 4 * q,
	};
};
