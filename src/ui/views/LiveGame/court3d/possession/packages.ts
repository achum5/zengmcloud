import type { CourtRatings, RatingKey } from "../ratings.ts";

// HOW HE DOES EACH THING: the library of moves (see README.md). Every kind
// of move a man makes - walking, running, down in his stance - he makes his
// own way, one version of it out of several: a package. Which package he
// has for each kind is his signature (see signature.ts): picked from his
// ratings until somebody chooses for him.
//
// A package has three parts, kept apart:
// - timing, which the staging reads: how quickly his steps go round, as a
//   share of an ordinary man's of his size at the same pace (see gait.ts);
// - a look, which only the picture reads: how he carries himself doing it;
// - who it fits: which ratings lean a man toward it, and the least he must
//   have of any to be given it at all.
// Its id is for good: a signature saved in a league names packages by id,
// so ids are never changed or reused, only added.

export type MoveType = "walk" | "run" | "stance" | "backpedal";
export const MOVE_TYPES: readonly MoveType[] = [
	"walk",
	"run",
	"stance",
	"backpedal",
];

export type Timing = {
	// His steps a second, as a share of an ordinary man's of his build going
	// the same pace (1: the same). Under 1, the longer each step for it.
	cadence: number;
};

// How he carries himself (see poses.ts RunStyle - each to that, at the
// middle of its range): how much his arms swing (1 an ordinary man's), how
// much more bent at the elbows (degrees), how far out from his sides
// (degrees), how much more he leans into it (degrees), how much his
// shoulders roll (1 an ordinary man's), how low he sits (-1 to 1).
export type Look = {
	swing: number;
	elbow: number;
	carry: number;
	lean: number;
	roll: number;
	low: number;
};

export type Fit = {
	// How each rating leans a man toward it: 1, the better he is at it the
	// likelier; -1, the worse.
	prefer: Partial<Record<RatingKey, number>>;
	// The least he must have of each to be given it.
	floor?: Partial<Record<RatingKey, number>>;
	// How common it is, everything else even.
	share: number;
};

export type Package = {
	id: string;
	type: MoveType;
	name: string;
	timing: Timing;
	look: Look;
	fit: Fit;
};

const LOOK: Look = { swing: 1, elbow: 0, carry: 0, lean: 0, roll: 1, low: 0 };

export const PACKAGES: readonly Package[] = [
	// RUNNING, jogging up the floor through flat out. Real runners of the
	// same build at the same pace differ in how quickly their steps go
	// round by about a tenth either way (see gait.ts).
	{
		id: "run.easy",
		type: "run",
		name: "Easy",
		timing: { cadence: 1 },
		look: { ...LOOK, swing: 0.85, elbow: -8, lean: -1 },
		fit: { prefer: {}, share: 1 },
	},
	// Long, loping strides, upright and smooth: a long, quick wing.
	{
		id: "run.long",
		type: "run",
		name: "Long strides",
		timing: { cadence: 0.93 },
		look: { ...LOOK, swing: 0.95, elbow: -4, roll: 0.8 },
		fit: { prefer: { hgt: 0.6, spd: 0.8 }, floor: { spd: 45 }, share: 0.8 },
	},
	// Driving into it: leaning on, arms pumping hard and tight.
	{
		id: "run.drive",
		type: "run",
		name: "Driver",
		timing: { cadence: 1.03 },
		look: { ...LOOK, swing: 1.25, elbow: 10, lean: 4, roll: 1.1 },
		fit: {
			prefer: { spd: 1, jmp: 0.8, stre: 0.4 },
			floor: { spd: 55 },
			share: 0.8,
		},
	},
	// Short, quick steps, low to the floor: a small, quick guard.
	{
		id: "run.quick",
		type: "run",
		name: "Quick steps",
		timing: { cadence: 1.08 },
		look: { ...LOOK, swing: 1.05, elbow: 6, lean: 2, low: 0.4 },
		fit: { prefer: { spd: 1, hgt: -1 }, floor: { spd: 50 }, share: 0.7 },
	},
	// Heavy on his feet: upright, arms out from his sides, shoulders
	// rolling - a big man who is no runner.
	{
		id: "run.heavy",
		type: "run",
		name: "Heavy",
		timing: { cadence: 0.97 },
		look: { ...LOOK, swing: 0.75, carry: 6, lean: -3, roll: 1.3 },
		fit: { prefer: { hgt: 1, stre: 0.6, spd: -1 }, share: 0.6 },
	},

	// WALKING.
	{
		id: "walk.easy",
		type: "walk",
		name: "Easy",
		timing: { cadence: 1 },
		look: LOOK,
		fit: { prefer: {}, share: 1 },
	},
	{
		id: "walk.tall",
		type: "walk",
		name: "Tall",
		timing: { cadence: 0.96 },
		look: { ...LOOK, swing: 0.8, carry: -2, lean: -2 },
		fit: { prefer: { oiq: 0.5 }, share: 0.6 },
	},
	{
		id: "walk.heavy",
		type: "walk",
		name: "Heavy",
		timing: { cadence: 0.95 },
		look: { ...LOOK, swing: 0.9, carry: 6, roll: 1.3 },
		fit: { prefer: { hgt: 1, stre: 0.6, spd: -0.6 }, share: 0.6 },
	},
	{
		id: "walk.bounce",
		type: "walk",
		name: "Bounce",
		timing: { cadence: 1.05 },
		look: { ...LOOK, swing: 1.15, elbow: 6, roll: 1.1 },
		fit: { prefer: { jmp: 0.8, spd: 0.6, hgt: -0.4 }, share: 0.6 },
	},

	// DOWN IN HIS STANCE, and sliding in it. (A slide's push steps go round
	// by how far each spreads his feet - see stepping.ts reachOf - so far
	// the same for every package.)
	{
		id: "stance.balanced",
		type: "stance",
		name: "Balanced",
		timing: { cadence: 1 },
		look: LOOK,
		fit: { prefer: {}, share: 1 },
	},
	// Sat right down in it, wide: a good, quick defender.
	{
		id: "stance.low",
		type: "stance",
		name: "Sits down",
		timing: { cadence: 1 },
		look: { ...LOOK, low: 1 },
		fit: { prefer: { diq: 1, spd: 0.6 }, floor: { diq: 50 }, share: 0.8 },
	},
	// Played taller: a big man out on the floor, or a man no defender.
	{
		id: "stance.tall",
		type: "stance",
		name: "Plays tall",
		timing: { cadence: 1 },
		look: { ...LOOK, low: -1 },
		fit: { prefer: { hgt: 0.8, diq: -0.8 }, share: 0.7 },
	},

	// BACKPEDALING.
	{
		id: "backpedal.low",
		type: "backpedal",
		name: "Low",
		timing: { cadence: 1.04 },
		look: { ...LOOK, low: 0.6, lean: 2 },
		fit: { prefer: { spd: 0.8, diq: 0.6, hgt: -0.4 }, share: 1 },
	},
	{
		id: "backpedal.upright",
		type: "backpedal",
		name: "Upright",
		timing: { cadence: 0.96 },
		look: { ...LOOK, low: -0.6, lean: -2 },
		fit: { prefer: { hgt: 0.8, spd: -0.6 }, share: 1 },
	},
];

const BY_ID = new Map(PACKAGES.map((p) => [p.id, p]));
export const packageOf = (id: string): Package | undefined => BY_ID.get(id);
export const packagesFor = (type: MoveType): Package[] =>
	PACKAGES.filter((p) => p.type === type);

// Whether his ratings are enough for it (no ratings: anything goes).
export const fits = (p: Package, r: CourtRatings | undefined): boolean =>
	!r ||
	Object.entries(p.fit.floor ?? {}).every(
		([k, least]) => (r[k as RatingKey] ?? least) >= least,
	);
