import {
	ANIMS,
	bounceAt,
	flexOf,
	lerpPose,
	mirror,
	poseAt,
	shortLift,
	shortSteps,
	skeleton,
	stepBeat,
	type AnimName,
	type Body,
	type Pose,
	type V3,
} from "../court3d/poses.ts";
import { drawnBody, footballBody } from "./body.ts";
import type { BallHold } from "./director.ts";
import type { Grip, HeldBall } from "./sculpt.ts";
import { BALL_LEN, BALL_R } from "./sculpt.ts";

// HOW A FOOTBALL PLAYER STANDS AND MOVES.
//
// In the court's terms (see court3d/poses.ts - the same skeleton, the same
// joint angles, and its walks and runs and backpedals as they are): what is
// football's own is here. The stances first - a lineman down in three
// points, back flat, hand on the turf; the center over the ball; the
// quarterback under center or back in the gun; a back with his hands on his
// thighs; a receiver split out, staggered, leaning; a linebacker square and
// ready; a corner low over his man - and how each holds his hands.

type FieldAnim =
	| { kind: "still"; pose: Pose }
	| { kind: "loop"; n: number; fps: number; pose: (i: number) => Pose }
	| { kind: "cycle"; n: number; stride: number; pose: (i: number) => Pose }
	| { kind: "act"; keys: [number, Pose][] };

// Every angle the court's skeleton takes, from standing easy - so a new one
// it learns starts here as it does there.
const BASE: Pose = {
	...poseAt("ready", 0),
	hipN: 0,
	kneeN: 6,
	hipF: 4,
	kneeF: 8,
	shN: 8,
	elN: 22,
	shF: 10,
	elF: 26,
	lean: 3,
	abN: 12,
	abF: 12,
	wide: 0.3,
	wideN: 0,
	wideF: 0,
	wrN: 0,
	wrF: 0,
	flare: 0,
	plant: 0,
	tuck: 0,
	free: 0,
	toe: 0,
	toeN: 0,
	toeF: 0,
	legs: 0,
	spinN: 0,
	spinF: 0,
	twist: 0,
	tilt: 0,
};
const pose = (o: Partial<Pose>): Pose => ({ ...BASE, ...o });

// Through an act's keys on a smooth curve, not straight from one to the
// next: each angle coming into a key and on out of it at one speed, never
// past it (a monotone cubic - see slopesOf).
const keyed = (keys: [number, Pose][], u: number): Pose => {
	for (let i = 1; i < keys.length; i++) {
		const [u1, p1] = keys[i]!;
		if (u <= u1) {
			const [u0, p0] = keys[i - 1]!;
			if (u1 === u0) {
				return p1;
			}
			const h = u1 - u0;
			const t = (u - u0) / h;
			const m = slopesOf(keys);
			const m0 = m[i - 1]!;
			const m1 = m[i]!;
			const a = (2 * t - 3) * t * t + 1;
			const b = ((t - 2) * t + 1) * t * h;
			const c = (3 - 2 * t) * t * t;
			const d = (t - 1) * t * t * h;
			const out = { ...p0 };
			for (const k of Object.keys(p0) as (keyof Pose)[]) {
				out[k] = a * p0[k] + b * m0[k] + c * p1[k] + d * m1[k];
			}
			return out;
		}
	}
	return keys.at(-1)![1];
};
// How fast each angle goes through each key (a unit of phase): where it
// turns back, or holds, none; otherwise a mean of the way in and the way
// out weighted to the shorter (Fritsch-Carlson, as PCHIP has it - the
// ends from the two ways next to them, no faster than three times the
// one).
const slopes = new WeakMap<[number, Pose][], Pose[]>();
const slopesOf = (keys: [number, Pose][]): Pose[] => {
	const had = slopes.get(keys);
	if (had) {
		return had;
	}
	const n = keys.length;
	const out = keys.map(() => ({ ...BASE }));
	for (const k of Object.keys(BASE) as (keyof Pose)[]) {
		const h: number[] = [];
		const d: number[] = [];
		for (let i = 0; i + 1 < n; i++) {
			h.push(keys[i + 1]![0] - keys[i]![0]);
			d.push(h[i]! > 0 ? (keys[i + 1]![1][k] - keys[i]![1][k]) / h[i]! : 0);
		}
		for (let i = 1; i + 1 < n; i++) {
			const d0 = d[i - 1]!;
			const d1 = d[i]!;
			const w0 = 2 * h[i]! + h[i - 1]!;
			const w1 = h[i]! + 2 * h[i - 1]!;
			out[i]![k] =
				d0 * d1 <= 0 || h[i - 1]! <= 0 || h[i]! <= 0
					? 0
					: (w0 + w1) / (w0 / d0 + w1 / d1);
		}
		const end = (h0: number, h1: number, d0: number, d1: number) => {
			if (n < 3 || h0 + h1 <= 0) {
				return d0;
			}
			const m = ((2 * h0 + h1) * d0 - h0 * d1) / (h0 + h1);
			return m * d0 <= 0
				? 0
				: d0 * d1 < 0 && Math.abs(m) > 3 * Math.abs(d0)
					? 3 * d0
					: m;
		};
		out[0]![k] = end(h[0]!, h[1] ?? 0, d[0] ?? 0, d[1] ?? 0);
		out[n - 1]![k] =
			n < 2 ? 0 : end(h[n - 2]!, h[n - 3] ?? 0, d[n - 2]!, d[n - 3] ?? 0);
	}
	slopes.set(keys, out);
	return out;
};

// A lineman down in his stance: feet a little wider than his shoulders, his
// right foot back (toe to the other's instep), thighs low, back flat, head
// up; the right hand down on the turf just inside his back foot, the other
// forearm resting on his thigh. (His left-handed stance: mirrored.)
const THREE_POINT = pose({
	hipN: 50,
	kneeN: 112,
	hipF: 72,
	kneeF: 104,
	lean: 72,
	shN: 4,
	elN: 2,
	abN: 6,
	shF: 52,
	elF: 58,
	abF: 10,
	wide: 0.95,
});

// The center over the ball: square, both feet even, a little wider; his
// right hand in on the ball out in front of him, the left on his knee.
const CENTER = pose({
	hipN: 64,
	kneeN: 108,
	hipF: 64,
	kneeF: 108,
	lean: 66,
	shN: 22,
	elN: 4,
	abN: -30,
	shF: 40,
	elF: 50,
	abF: 12,
	wide: 1.25,
});

// Lying face down on the turf: legs out behind him, his arms out in front.
const PRONE = pose({
	hipN: -90,
	kneeN: 2,
	hipF: -90,
	kneeF: 4,
	lean: 86,
	shN: 96,
	elN: 10,
	abN: 18,
	shF: 92,
	elF: 14,
	abF: 20,
	wide: 0.5,
});

// Lying on his back on the turf: legs out in front of him, arms out by his
// sides.
const SUPINE = pose({
	hipN: 88,
	kneeN: 8,
	hipF: 116,
	kneeF: 74,
	lean: -84,
	shN: 18,
	elN: 30,
	abN: 72,
	shF: 40,
	elF: 60,
	abF: 50,
	wide: 0.6,
});

// On a pile: down on both knees at the edge of it, shins flat on the turf
// behind him, laid forward over the heap, his arms out over it.
const OVER_PILE = pose({
	hipN: 14,
	kneeN: 104,
	hipF: 20,
	kneeF: 110,
	lean: 76,
	shN: 74,
	elN: 34,
	abN: 34,
	shF: 82,
	elF: 26,
	abF: 40,
	wide: 0.75,
});

// Sat on the turf at the end of a slide: the lead leg out in front of him,
// the other knee up, leaning back on a hand.
const SLID = pose({
	hipN: 92,
	kneeN: 6,
	hipF: 128,
	kneeF: 128,
	lean: -30,
	shN: 20,
	elN: 60,
	abN: 20,
	shF: -14,
	elF: 40,
	abF: 34,
	wide: 0.5,
});

// The quarterback set in the pocket: on the balls of his feet, the ball up
// at his chest in both hands.
const HUDDLE = pose({
	hipN: 28,
	kneeN: 26,
	hipF: 30,
	kneeF: 28,
	lean: 42,
	shN: 26,
	elN: 18,
	abN: 10,
	shF: 26,
	elF: 18,
	abF: 10,
	wide: 0.6,
});
const UNDER_CENTER = pose({
	hipN: 34,
	kneeN: 52,
	hipF: 34,
	kneeF: 52,
	lean: 28,
	shN: 54,
	elN: 16,
	abN: -8,
	shF: 54,
	elF: 16,
	abF: -8,
	wide: 0.7,
});
const SHOTGUN = pose({
	hipN: 14,
	kneeN: 22,
	hipF: 16,
	kneeF: 24,
	lean: 12,
	shN: 42,
	elN: 74,
	abN: 6,
	shF: 42,
	elF: 74,
	abF: 6,
	wide: 0.7,
});
// Stood up behind the center, hands at his belt.
const QB_SURVEY = pose({
	hipN: 8,
	kneeN: 14,
	hipF: 10,
	kneeF: 14,
	lean: 8,
	shN: 18,
	elN: 52,
	abN: 12,
	shF: 18,
	elF: 52,
	abF: 12,
	wide: 0.75,
});
// One arm out straight, pointing; both hands in together, a clap.
const POINT_OUT = { shF: 94, elF: 6, abF: 4 };
const BACKER = pose({
	hipN: 34,
	kneeN: 50,
	hipF: 36,
	kneeF: 52,
	lean: 30,
	shN: 34,
	elN: 56,
	abN: 14,
	shF: 34,
	elF: 56,
	abF: 14,
	wide: 0.9,
});
const BACKER_UP = {
	...BACKER,
	hipN: 18,
	kneeN: 26,
	hipF: 20,
	kneeF: 28,
	lean: 14,
};
const CLAP = { abN: -6, abF: -6, shF: 60, elF: 56 };
// A play sheet held up at his chest, in his left hand.
const SHEET = { shF: 34, elF: 104, abF: -18, wrF: 10 };
// (Both hands on the ball, at his chest.)
// A receiver sat down, hands up (see settle).
const SETTLE = pose({
	hipN: 24,
	kneeN: 36,
	hipF: 26,
	kneeF: 38,
	lean: 20,
	shN: 50,
	elN: 64,
	abN: 12,
	shF: 50,
	elF: 64,
	abF: 12,
	wide: 0.8,
});
const QB_SET = pose({
	hipN: 12,
	kneeN: 22,
	hipF: 16,
	kneeF: 22,
	lean: 10,
	shN: 30,
	elN: 92,
	abN: -9,
	shF: 30,
	elF: 92,
	abF: -9,
	wide: 0.8,
});
// Coming up into it: the ball off his chest on its way back and up, the
// off hand coming off it out in front, his front knee up for the stride,
// his weight back on his back foot.
const THROW_LIFT = pose({
	hipN: 10,
	kneeN: 28,
	hipF: 46,
	kneeF: 62,
	lean: 6,
	shN: 110,
	elN: 110,
	abN: 30,
	wrN: 10,
	flexN: 0.5,
	shF: 70,
	elF: 36,
	abF: 2,
	twist: -18,
	wide: 0.9,
});
// Cocked: his stride down at the target, the ball back by his ear - his
// elbow up out to the side, level with his shoulder - his shoulders turned
// away from the target, the off arm out at it, his weight back.
const THROW_COCKED = pose({
	hipN: 2,
	kneeN: 26,
	hipF: 30,
	kneeF: 18,
	lean: 4,
	shN: 150,
	elN: 100,
	abN: 35,
	wrN: 25,
	flexN: 1,
	shF: 84,
	elF: 14,
	abF: 10,
	twist: -28,
	wide: 1.15,
});
// Let go: the arm out straight up over the top in front of his head, the
// off arm pulled in to his side, his shoulders come round, his weight onto
// his front foot and his back one up on its toes.
const THROW_RELEASE = pose({
	hipN: -16,
	kneeN: 30,
	hipF: 26,
	kneeF: 20,
	lean: 16,
	shN: 140,
	elN: 18,
	abN: 30,
	wrN: -15,
	shF: 26,
	elF: 96,
	abF: 20,
	toeN: 0.5,
	twist: 16,
	wide: 1.15,
});
// On through: the arm coming down across him, the off arm pulled in to
// his side, his back foot pushing off.
const THROW_ACROSS = pose({
	hipN: -12,
	kneeN: 40,
	hipF: 22,
	kneeF: 24,
	lean: 26,
	shN: 66,
	elN: 18,
	abN: -32,
	wrN: -20,
	shF: -6,
	elF: 76,
	abF: 12,
	toeN: 0.6,
	twist: 30,
	wide: 1.0,
});
// Followed through: the arm down across him to his far hip, the off arm
// back by his side - not both out in front of him - his back foot come up
// off the turf and through.
const THROW_THROUGH = pose({
	hipN: 14,
	kneeN: 70,
	hipF: 20,
	kneeF: 26,
	lean: 32,
	shN: 18,
	elN: 18,
	abN: -44,
	wrN: -30,
	shF: -30,
	elF: 50,
	abF: 10,
	twist: 34,
	wide: 0.8,
});

// A step in a blocker's or a rusher's feet, short and choppy: frame i of n,
// one foot then the other.
const chop = (i: number, n: number, lo: number, hi: number, knee: number) => {
	const a = Math.sin((2 * Math.PI * i) / n);
	return {
		hipN: lo + (hi - lo) * Math.max(0, a),
		kneeN: knee + 26 * Math.max(0, a),
		hipF: lo + (hi - lo) * Math.max(0, -a),
		kneeF: knee + 26 * Math.max(0, -a),
	};
};

// The officials' signals (the NFL's): a signal held a moment - up into it,
// held, and back down - or one made over and over, from the one pose to the
// other and back.
const held = (o: Partial<Pose>): FieldAnim => ({
	kind: "act",
	keys: [
		[0, BASE],
		[0.18, pose(o)],
		[0.84, pose(o)],
		[1, BASE],
	],
});
// Taking a handoff, his arms over whatever his legs are doing: in off
// his stride, open round his belly for it as he comes to it - the arm on
// the quarterback's side (here his right) across the top, the other under
// - and shut round it as it comes in, two thirds of the way through (see
// action.ts TAKE_BEFORE_MS).
const TAKE_OPEN = pose({
	shN: 2,
	elN: 129,
	abN: -36,
	shF: 13,
	elF: 35,
	abF: -31,
});
const TAKE_SHUT = pose({
	shN: 1,
	elN: 97,
	abN: -21,
	shF: 5,
	elF: 103,
	abF: -14,
});
const TAKE_KEYS: [number, Pose][] = [
	[0, pose({ shN: 10, elN: 85, shF: 10, elF: 85 })],
	[0.4, TAKE_OPEN],
	[0.64, TAKE_OPEN],
	[0.8, TAKE_SHUT],
	[1, TAKE_SHUT],
];
// Both hands on top of his head (in close: see tuck).
const HANDS_ON_HEAD: Partial<Pose> = {
	shN: 162,
	elN: 142,
	abN: 14,
	shF: 162,
	elF: 142,
	abF: 14,
	tuck: 1,
};
// The hand held out to shake, his elbow in by his side (and the shake down
// from it); in close, his left arm round a man's back (and patting it);
// clapping.
const SHAKE: Partial<Pose> = { shN: 22, elN: 80, abN: -8, lean: 4 };
const SHAKE_DOWN: Partial<Pose> = { ...SHAKE, shN: 16, elN: 86 };
const HUG: Partial<Pose> = {
	shN: 30,
	elN: 96,
	abN: -16,
	shF: 90,
	elF: 50,
	abF: -14,
	lean: 8,
};
const HUG_SLAP: Partial<Pose> = { ...HUG, shF: 100, elF: 74 };
// A cheerleader's arms (see poms): up in a V, out level, down in a V.
const POMS_UP: Partial<Pose> = {
	shN: 158,
	elN: 6,
	abN: 40,
	shF: 158,
	elF: 6,
	abF: 40,
	lean: -3,
};
const POMS_OUT: Partial<Pose> = {
	shN: 12,
	elN: 6,
	abN: 86,
	shF: 12,
	elF: 6,
	abF: 86,
};
const POMS_DOWN: Partial<Pose> = {
	shN: 12,
	elN: 4,
	abN: 36,
	shF: 12,
	elF: 4,
	abF: 36,
};
const CLAP_APART: Partial<Pose> = {
	shN: 56,
	elN: 66,
	abN: -8,
	shF: 56,
	elF: 66,
	abF: -8,
};
const CLAP_TOGETHER: Partial<Pose> = {
	...CLAP_APART,
	shN: 54,
	elN: 64,
	abN: -24,
	shF: 54,
	elF: 64,
	abF: -24,
};
// Where in a high five the two hands meet - up on his toes into it, this
// far (feet).
export const HIGH_FIVE_AT = 0.5;
const HIGH_FIVE_HOP = 0.3;
// The cooler's handle in his right hand, hung at his side; the cooler over
// him (hunched, his arms up and out).
const COOLER_CARRY: Partial<Pose> = { shN: 10, elN: 8, abN: 8, abF: 20 };
const DOUSED: Partial<Pose> = {
	shN: 60,
	elN: 100,
	abN: 46,
	shF: 60,
	elF: 100,
	abF: 46,
	hipN: 30,
	kneeN: 24,
	hipF: 30,
	kneeF: 24,
	lean: 26,
};
const over = (a: Partial<Pose>, b: Partial<Pose>): FieldAnim => ({
	kind: "act",
	keys: [
		[0, BASE],
		[0.14, pose(a)],
		[0.32, pose(b)],
		[0.5, pose(a)],
		[0.68, pose(b)],
		[0.86, pose(a)],
		[1, BASE],
	],
});

// A dive (see FIELD_ANIMS) - onto the top of a pile too.
const DIVE_KEYS: [number, Pose][] = [
	[
		0,
		pose({
			hipN: 30,
			kneeN: 50,
			hipF: -10,
			kneeF: 30,
			lean: 20,
			shN: 50,
			elN: 60,
			shF: 50,
			elF: 60,
			abN: 20,
			abF: 20,
		}),
	],
	[
		0.35,
		pose({
			hipN: -30,
			kneeN: 20,
			hipF: 44,
			kneeF: 80,
			lean: 52,
			shN: 86,
			elN: 46,
			abN: 40,
			shF: 86,
			elF: 46,
			abF: 40,
		}),
	],
	[
		0.6,
		pose({
			hipN: -63,
			kneeN: 11,
			hipF: -31,
			kneeF: 40,
			lean: 70,
			shN: 90,
			elN: 49,
			abN: 33,
			shF: 90,
			elF: 49,
			abF: 33,
		}),
	],
	[
		0.8,
		pose({
			hipN: -80,
			kneeN: 6,
			hipF: -70,
			kneeF: 20,
			lean: 80,
			shN: 92,
			elN: 50,
			abN: 30,
			shF: 92,
			elF: 50,
			abF: 30,
		}),
	],
	[1, { ...PRONE, shN: 94, elN: 40, abN: 26, shF: 94, elF: 40, abF: 26 }],
];

// The griddy, a score's dance: a skip on one foot, the other heel kicked
// up behind him, his arms swinging the other way - then the other foot -
// four times through, in and out of standing easy.
const GRIDDY_STEP = (i: number): Pose =>
	i % 2 === 1
		? pose({
				hipN: 6,
				kneeN: 24,
				hipF: 6,
				kneeF: 24,
				lean: 10,
				shN: 34,
				elN: 104,
				abN: 18,
				shF: 34,
				elF: 104,
				abF: 18,
			})
		: i === 0
			? pose({
					hipN: -14,
					kneeN: 104,
					hipF: 8,
					kneeF: 18,
					lean: 12,
					shN: -55,
					elN: 72,
					abN: 16,
					shF: 70,
					elF: 94,
					abF: 16,
				})
			: pose({
					hipN: 8,
					kneeN: 18,
					hipF: -14,
					kneeF: 104,
					lean: 12,
					shN: 70,
					elN: 94,
					abN: 16,
					shF: -55,
					elF: 72,
					abF: 16,
				});
const GRIDDY_KEYS: [number, Pose][] = Array.from({ length: 17 }, (_, k) => [
	k / 16,
	k === 0 || k === 16 ? BASE : GRIDDY_STEP(k % 4),
]);

// Getting up (see FIELD_ANIMS) - off the top of a pile too.
const GET_UP_KEYS: [number, Pose][] = [
	[0, PRONE],
	// (Up on his hands and knees first: or his near leg, swung through under
	// him, has him up and down again on the way.)
	[
		0.18,
		pose({
			hipN: 0,
			kneeN: 90,
			hipF: -6,
			kneeF: 86,
			lean: 82,
			shN: 10,
			elN: 0,
			abN: 10,
			shF: 10,
			elF: 0,
			abF: 10,
			wide: 0.6,
		}),
	],
	[
		0.38,
		pose({
			hipN: 80,
			kneeN: 130,
			hipF: -50,
			kneeF: 70,
			lean: 60,
			shN: 16,
			elN: 0,
			abN: 10,
			shF: 16,
			elF: 0,
			abF: 10,
			wide: 0.6,
		}),
	],
	[
		0.68,
		pose({
			hipN: 78,
			kneeN: 86,
			hipF: -18,
			kneeF: 104,
			lean: 26,
			shN: 20,
			elN: 20,
			shF: 30,
			elF: 30,
			wide: 0.5,
		}),
	],
	[1, BASE],
];

const FIELD_ANIMS = {
	// Standing easy: between plays, on the sideline.
	stand: { kind: "still", pose: BASE },
	// Bent over, hands on his knees: catching his breath, in the huddle.
	hands: { kind: "still", pose: HUDDLE },
	threePoint: { kind: "still", pose: THREE_POINT },
	// A defensive lineman's: more of his weight on his hand, hips up -
	// ready to fire off the ball.
	dlThree: {
		kind: "still",
		pose: {
			...THREE_POINT,
			hipN: 44,
			kneeN: 96,
			hipF: 64,
			kneeF: 92,
			lean: 76,
			shN: 14,
		},
	},
	center: { kind: "still", pose: CENTER },
	// The quarterback under center: knees flexed, feet under his shoulders,
	// hands together under the center, eyes up over the defense.
	underCenter: { kind: "still", pose: UNDER_CENTER },
	// In the shotgun: standing, a little crouched, hands up in front of him
	// for the snap.
	shotgun: { kind: "still", pose: SHOTGUN },
	// At the line before the snap: stood up behind the center reading the
	// defense, his head round one way and the other - now and then an arm
	// out at the man he picks out - and down under him for the ball.
	qbSurvey: {
		kind: "act",
		keys: [
			[0, QB_SURVEY],
			[0.22, { ...QB_SURVEY, twist: 26 }],
			[0.42, { ...QB_SURVEY, twist: 26 }],
			[0.68, { ...QB_SURVEY, twist: -24 }],
			[0.84, { ...QB_SURVEY, twist: -24 }],
			[1, QB_SURVEY],
		],
	},
	qbPoint: {
		kind: "act",
		keys: [
			[0, QB_SURVEY],
			[0.2, { ...QB_SURVEY, twist: 22 }],
			[0.36, { ...QB_SURVEY, twist: 8, ...POINT_OUT }],
			[0.6, { ...QB_SURVEY, twist: 8, ...POINT_OUT }],
			[0.8, { ...QB_SURVEY, twist: -20 }],
			[1, QB_SURVEY],
		],
	},
	getUnder: {
		kind: "act",
		keys: [
			[0, QB_SURVEY],
			[1, UNDER_CENTER],
		],
	},
	// In the gun: the same look round and an arm out, then a clap for the
	// ball.
	gunCall: {
		kind: "act",
		keys: [
			[0, SHOTGUN],
			[0.16, { ...SHOTGUN, twist: 26 }],
			[0.32, { ...SHOTGUN, twist: 26 }],
			[0.48, { ...SHOTGUN, twist: -12, ...POINT_OUT }],
			[0.64, { ...SHOTGUN, twist: -12, ...POINT_OUT }],
			[0.78, SHOTGUN],
			[0.86, { ...SHOTGUN, ...CLAP }],
			[0.93, SHOTGUN],
			[1, SHOTGUN],
		],
	},
	// The huddle broken: up off his knees and a clap, all of them at once.
	huddleBreak: {
		kind: "act",
		keys: [
			[0, HUDDLE],
			[
				0.4,
				{
					...BASE,
					lean: 14,
					shN: 58,
					elN: 50,
					abN: 24,
					shF: 58,
					elF: 50,
					abF: 24,
				},
			],
			[0.55, { ...BASE, lean: 12, shN: 60, elN: 56, ...CLAP }],
			[
				0.7,
				{
					...BASE,
					lean: 10,
					shN: 54,
					elN: 46,
					abN: 16,
					shF: 54,
					elF: 46,
					abF: 16,
				},
			],
			[1, BASE],
		],
	},
	// A running back set behind the quarterback: hands on his thighs, weight
	// forward.
	backStance: {
		kind: "still",
		pose: pose({
			hipN: 40,
			kneeN: 48,
			hipF: 42,
			kneeF: 50,
			lean: 38,
			shN: 30,
			elN: 26,
			abN: 12,
			shF: 30,
			elF: 26,
			abF: 12,
			wide: 0.75,
		}),
	},
	// A receiver split out: his inside foot up, weight on it, leaning out
	// over it, arms ready.
	receiverStance: {
		kind: "still",
		pose: pose({
			hipN: 34,
			kneeN: 42,
			hipF: -14,
			kneeF: 30,
			lean: 28,
			shN: 34,
			elN: 64,
			abN: 10,
			shF: -10,
			elF: 50,
			abF: 12,
			wide: 0.35,
		}),
	},
	// A linebacker: square, knees bent, hands out in front, reading the
	// backfield.
	backerStance: { kind: "still", pose: BACKER },
	// The middle linebacker making the call: up out of it a little, turned
	// one way with an arm out at the strength, then the other, and back
	// down.
	backerCall: {
		kind: "act",
		keys: [
			[0, BACKER],
			[0.14, { ...BACKER_UP, twist: 24 }],
			[0.28, { ...BACKER_UP, twist: 20, ...POINT_OUT }],
			[0.5, { ...BACKER_UP, twist: 20, ...POINT_OUT }],
			[0.66, { ...BACKER_UP, twist: -22 }],
			[0.86, { ...BACKER_UP, twist: -22 }],
			[1, BACKER],
		],
	},
	// A corner over his man: low, a stagger, eyes in.
	cornerStance: {
		kind: "still",
		pose: pose({
			hipN: 42,
			kneeN: 60,
			hipF: 20,
			kneeF: 46,
			lean: 32,
			shN: 26,
			elN: 54,
			abN: 16,
			shF: 22,
			elF: 52,
			abF: 16,
			wide: 0.6,
		}),
	},
	// A safety deep: upright, square, at ease but live.
	safetyStance: {
		kind: "still",
		pose: pose({
			hipN: 16,
			kneeN: 26,
			hipF: 18,
			kneeF: 28,
			lean: 16,
			shN: 22,
			elN: 40,
			abN: 14,
			shF: 22,
			elF: 40,
			abF: 14,
			wide: 0.75,
		}),
	},
	// The holder on a knee seven yards back, just to the side of the spot:
	// his right knee down, his left foot up under him, both hands out for
	// the snap.
	holdKneel: {
		kind: "still",
		pose: pose({
			hipN: -24,
			kneeN: 104,
			hipF: 82,
			kneeF: 88,
			lean: 14,
			shN: 50,
			elN: 34,
			abN: 10,
			shF: 58,
			elF: 22,
			abF: 6,
			wide: 0.5,
		}),
	},
	// Bringing it down to its spot beside him: turned and leant over to his
	// left, both arms down to it.
	holdPlace: {
		kind: "still",
		pose: pose({
			hipN: -24,
			kneeN: 104,
			hipF: 82,
			kneeF: 88,
			lean: 38,
			twist: 50,
			tilt: 35,
			shN: 6,
			elN: 2,
			abN: -21,
			shF: 6,
			elF: 2,
			abF: 21,
			wide: 0.5,
		}),
	},
	// And the ball down: leaning over it, a finger of his left hand on top
	// of it, the right back by his side out of the kicker's way.
	holdDown: {
		kind: "still",
		pose: pose({
			hipN: -24,
			kneeN: 104,
			hipF: 82,
			kneeF: 88,
			lean: 40,
			twist: 37,
			tilt: 30,
			shN: -30,
			elN: 4,
			abN: 5,
			shF: 6,
			elF: 8,
			abF: 15,
			wide: 0.5,
		}),
	},
	// A kicker set back off the ball: still, a little crouched, eyes on the
	// spot.
	kickReady: {
		kind: "still",
		pose: pose({
			hipN: 8,
			kneeN: 16,
			hipF: 12,
			kneeF: 18,
			lean: 14,
			shN: 4,
			elN: 18,
			abN: 18,
			shF: 4,
			elF: 18,
			abF: 18,
			wide: 0.55,
		}),
	},

	prone: { kind: "still", pose: PRONE },
	supine: { kind: "still", pose: SUPINE },
	qbSet: { kind: "still", pose: QB_SET },
	// Set in the pocket, the ball up in both hands: up on the balls of his
	// feet, his weight from one to the other, never stood still.
	pocket: {
		kind: "loop",
		n: 8,
		fps: 11,
		pose: (i) => {
			const a = Math.sin((Math.PI * 2 * i) / 8);
			return {
				...QB_SET,
				hipN: 12 + 4 * a,
				kneeN: 22 + 8 * a,
				hipF: 16 - 4 * a,
				kneeF: 22 - 8 * a,
				toeN: Math.max(0, a) * 0.4,
				toeF: Math.max(0, -a) * 0.4,
				twist: 3 * a,
			};
		},
	},
	// A receiver sat down in a hole in the zone, turned to the quarterback,
	// hands up for it: his feet alive under him, his weight from one to the
	// other - not stood stock still waiting.
	settle: {
		kind: "loop",
		n: 8,
		fps: 9,
		pose: (i) => {
			const a = Math.sin((Math.PI * 2 * i) / 8);
			return {
				...SETTLE,
				hipN: SETTLE.hipN + 4 * a,
				kneeN: SETTLE.kneeN + 8 * a,
				hipF: SETTLE.hipF - 4 * a,
				kneeF: SETTLE.kneeF - 8 * a,
				toeN: Math.max(0, a) * 0.4,
				toeF: Math.max(0, -a) * 0.4,
				shN: SETTLE.shN + 2 * a,
				shF: SETTLE.shF - 2 * a,
				twist: 2 * a,
			};
		},
	},
	// Taking a knee: the right one down, the ball in both hands.
	kneelDown: {
		kind: "still",
		pose: pose({
			hipN: -20,
			kneeN: 104,
			hipF: 80,
			kneeF: 88,
			lean: 12,
			shN: 26,
			elN: 96,
			abN: 12,
			shF: 26,
			elF: 96,
			abF: 12,
			wide: 0.5,
		}),
	},
	// In pass protection: sat down in his stance, back straight, hands up
	// at his chest to punch - and his feet going, short kick-slide steps.
	passSet: {
		kind: "cycle",
		n: 4,
		stride: 2.4,
		pose: (i) =>
			pose({
				...chop(i, 4, 30, 44, 52),
				lean: 24,
				shN: 58,
				elN: 74,
				abN: 10,
				shF: 58,
				elF: 74,
				abF: 10,
				wide: 1.15,
			}),
	},
	// Run blocking: into the man in front of him, low, hands inside on his
	// chest, driving his legs.
	drive: {
		kind: "cycle",
		n: 4,
		stride: 2.2,
		pose: (i) =>
			pose({
				...chop(i, 4, 18, 62, 58),
				lean: 44,
				shN: 84,
				elN: 18,
				abN: 8,
				shF: 84,
				elF: 18,
				abF: 8,
				wide: 0.95,
			}),
	},
	// A rusher (or a man being blocked) locked up with his blocker, arms
	// out into him, working his feet.
	engage: {
		kind: "cycle",
		n: 4,
		stride: 2.2,
		pose: (i) =>
			pose({
				...chop(i, 4, 20, 58, 56),
				lean: 40,
				shN: 80,
				elN: 24,
				abN: 14,
				shF: 76,
				elF: 30,
				abF: 14,
				wide: 0.95,
			}),
	},
	throwWind: {
		kind: "act",
		keys: [
			[0, QB_SET],
			[0.3, THROW_LIFT],
			[0.55, THROW_COCKED],
			[1, THROW_RELEASE],
		],
	},
	// A pump: the ball up and the arm on through as if to let it go, and
	// back down into both hands.
	pump: {
		kind: "act",
		keys: [
			[0, QB_SET],
			[0.4, THROW_COCKED],
			[0.6, lerpPose(THROW_COCKED, THROW_RELEASE, 0.35)],
			[1, QB_SET],
		],
	},
	throwFollow: {
		kind: "act",
		keys: [
			[0, THROW_RELEASE],
			[0.35, THROW_ACROSS],
			[1, THROW_THROUGH],
		],
	},
	// Handing it off: the ball out in both hands, low, into the back's belly.
	handOff: {
		kind: "act",
		keys: [
			[0, QB_SET],
			[
				0.5,
				pose({
					hipN: 20,
					kneeN: 30,
					hipF: 8,
					kneeF: 20,
					lean: 22,
					shN: 31,
					elN: 37,
					abN: -3,
					shF: 42,
					elF: 20,
					abF: -18,
					twist: 8,
					wide: 1.0,
				}),
			],
			[
				1,
				pose({
					hipN: 14,
					kneeN: 22,
					hipF: 12,
					kneeF: 18,
					lean: 12,
					shN: 34,
					elN: 40,
					abN: 10,
					shF: 30,
					elF: 50,
					abF: 10,
					wide: 0.9,
				}),
			],
		],
	},
	// The catch: his arms carried as he goes, up in front of him late - the
	// elbows bent, only reached out to it at the last - his hands together at
	// the ball, and in to his chest.
	catch: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 20,
					kneeN: 30,
					hipF: 22,
					kneeF: 32,
					lean: 14,
					shN: 26,
					elN: 92,
					abN: 8,
					shF: 26,
					elF: 92,
					abF: 8,
				}),
			],
			[
				0.3,
				pose({
					hipN: 15,
					kneeN: 24,
					hipF: 18,
					kneeF: 27,
					lean: 9,
					shN: 56,
					elN: 84,
					abN: 4,
					shF: 56,
					elF: 84,
					abF: 4,
					wrN: 10,
					wrF: 10,
				}),
			],
			[
				0.47,
				pose({
					hipN: 11,
					kneeN: 20,
					hipF: 15,
					kneeF: 23,
					lean: 5,
					shN: 92,
					elN: 44,
					abN: -4,
					shF: 92,
					elF: 44,
					abF: -4,
					wrN: 20,
					wrF: 20,
				}),
			],
			[
				0.55,
				pose({
					hipN: 10,
					kneeN: 18,
					hipF: 14,
					kneeF: 22,
					lean: 4,
					shN: 108,
					elN: 24,
					abN: -8,
					shF: 108,
					elF: 24,
					abF: -8,
					wrN: 24,
					wrF: 24,
				}),
			],
			[
				0.72,
				pose({
					hipN: 13,
					kneeN: 22,
					hipF: 16,
					kneeF: 25,
					lean: 10,
					shN: 62,
					elN: 82,
					abN: -2,
					shF: 62,
					elF: 82,
					abF: -2,
					wrN: 8,
					wrF: 8,
				}),
			],
			[
				1,
				pose({
					hipN: 16,
					kneeN: 26,
					hipF: 18,
					kneeF: 28,
					lean: 16,
					shN: 30,
					elN: 104,
					abN: 4,
					shF: 30,
					elF: 104,
					abF: 4,
				}),
			],
		],
	},
	// Up for it at its highest: off both feet, his hands up over his head
	// to it at the top, and down with it into his chest, landing.
	leapCatch: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 36,
					kneeN: 64,
					hipF: 36,
					kneeF: 64,
					lean: 20,
					shN: 20,
					elN: 40,
					shF: 20,
					elF: 40,
				}),
			],
			[
				0.3,
				pose({
					hipN: 8,
					kneeN: 18,
					hipF: 30,
					kneeF: 50,
					lean: 4,
					shN: 150,
					elN: 20,
					abN: 10,
					shF: 150,
					elF: 20,
					abF: 10,
					toe: 1,
				}),
			],
			[
				0.5,
				pose({
					hipN: 4,
					kneeN: 20,
					hipF: 58,
					kneeF: 84,
					lean: -4,
					shN: 168,
					elN: 8,
					abN: 8,
					shF: 168,
					elF: 8,
					abF: 8,
					wrN: 20,
					wrF: 20,
					toe: 1,
				}),
			],
			[
				0.75,
				pose({
					hipN: 20,
					kneeN: 30,
					hipF: 26,
					kneeF: 36,
					lean: 8,
					shN: 40,
					elN: 100,
					abN: 4,
					shF: 40,
					elF: 100,
					abF: 4,
				}),
			],
			[
				1,
				pose({
					hipN: 30,
					kneeN: 54,
					hipF: 30,
					kneeF: 54,
					lean: 18,
					shN: 30,
					elN: 104,
					abN: 4,
					shF: 30,
					elF: 104,
					abF: 4,
				}),
			],
		],
	},
	// A hand up at the ball as it comes, to knock it down.
	swat: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 30,
					kneeN: 40,
					hipF: 20,
					kneeF: 30,
					lean: 24,
					shN: 60,
					elN: 40,
					shF: 40,
					elF: 50,
				}),
			],
			[
				0.5,
				pose({
					hipN: -10,
					kneeN: 20,
					hipF: 40,
					kneeF: 20,
					lean: 20,
					shN: 150,
					elN: 6,
					abN: 20,
					shF: 70,
					elF: 30,
				}),
			],
			[
				1,
				pose({
					hipN: 16,
					kneeN: 26,
					hipF: 18,
					kneeF: 28,
					lean: 14,
					shN: 50,
					elN: 40,
					shF: 40,
					elF: 50,
				}),
			],
		],
	},
	// Brought down: his legs taken, pitching forward onto the turf.
	tackled: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 30,
					kneeN: 50,
					hipF: -10,
					kneeF: 30,
					lean: 16,
					shN: 30,
					elN: 100,
					shF: 40,
					elF: 60,
				}),
			],
			[
				0.35,
				pose({
					hipN: -10,
					kneeN: 50,
					hipF: 20,
					kneeF: 60,
					lean: 46,
					shN: 30,
					elN: 100,
					shF: 70,
					elF: 30,
				}),
			],
			[
				0.7,
				pose({
					hipN: -50,
					kneeN: 30,
					hipF: -40,
					kneeF: 40,
					lean: 70,
					shN: 40,
					elN: 100,
					shF: 88,
					elF: 20,
					abF: 20,
				}),
			],
			[1, PRONE],
		],
	},
	// A spin move: a foot planted, low, round on it with his free arm out -
	// and off again.
	spin: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 30,
					kneeN: 50,
					hipF: -10,
					kneeF: 30,
					lean: 20,
					shF: 30,
					elF: 70,
				}),
			],
			[
				0.3,
				pose({
					hipN: 40,
					kneeN: 72,
					hipF: 30,
					kneeF: 62,
					lean: 28,
					wide: 0.6,
					shF: 70,
					elF: 40,
					abF: 44,
				}),
			],
			[
				0.65,
				pose({
					hipN: 20,
					kneeN: 42,
					hipF: 22,
					kneeF: 44,
					lean: 16,
					shF: 50,
					elF: 60,
					abF: 52,
				}),
			],
			[
				1,
				pose({
					hipN: -6,
					kneeN: 30,
					hipF: 40,
					kneeF: 66,
					lean: 22,
					shF: 40,
					elF: 60,
				}),
			],
		],
	},
	// Over a man who has gone low at his legs: off one foot, the lead leg
	// out straight in front of him, the other tucked up behind - and down
	// running.
	hurdle: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 30,
					kneeN: 50,
					hipF: -20,
					kneeF: 40,
					lean: 22,
					shF: 40,
					elF: 60,
				}),
			],
			[
				0.35,
				pose({
					hipN: 84,
					kneeN: 10,
					hipF: -40,
					kneeF: 104,
					lean: 34,
					shF: 80,
					elF: 20,
					abF: 20,
				}),
			],
			[
				0.7,
				pose({
					hipN: 60,
					kneeN: 22,
					hipF: -22,
					kneeF: 82,
					lean: 26,
					shF: 60,
					elF: 40,
					abF: 30,
				}),
			],
			[
				1,
				pose({
					hipN: 20,
					kneeN: 40,
					hipF: -10,
					kneeF: 30,
					lean: 18,
					shF: 30,
					elF: 70,
				}),
			],
		],
	},
	// A cut on the run, to his left: the right foot planted out wide, low,
	// his shoulders dipped into it, and off the left.
	juke: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 34,
					kneeN: 56,
					hipF: -8,
					kneeF: 34,
					lean: 22,
					shF: 30,
					elF: 70,
				}),
			],
			[
				0.35,
				pose({
					hipN: 30,
					kneeN: 76,
					hipF: 18,
					kneeF: 52,
					lean: 26,
					wide: 0.5,
					wideN: 0.9,
					tilt: 16,
					twist: -8,
					legs: 10,
					shF: 50,
					elF: 40,
					abF: 40,
				}),
			],
			[
				0.7,
				pose({
					hipN: -6,
					kneeN: 30,
					hipF: 40,
					kneeF: 66,
					lean: 22,
					wide: 0.4,
					wideF: 0.4,
					tilt: 10,
					legs: 20,
					shF: 40,
					elF: 60,
					abF: 20,
				}),
			],
			[
				1,
				pose({
					hipN: 30,
					kneeN: 50,
					hipF: -10,
					kneeF: 30,
					lean: 18,
					shF: 30,
					elF: 70,
				}),
			],
		],
	},
	// The free arm out straight at a man coming at him, the heel of the
	// hand into him.
	stiffArm: {
		kind: "still",
		pose: pose({ shF: 80, elF: 4, abF: 6, wrF: 30 }),
	},
	// Both hands into a man, out from his chest: seeing him out of bounds.
	shove: {
		kind: "still",
		pose: pose({
			shN: 76,
			elN: 24,
			abN: -12,
			wrN: 30,
			shF: 76,
			elF: 24,
			abF: -12,
			wrF: 30,
		}),
	},
	// Met head on and driven back: his feet out from under him, down on his
	// seat, and over onto his back.
	tackledBack: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 18,
					kneeN: 34,
					hipF: 8,
					kneeF: 26,
					lean: 8,
					shN: 30,
					elN: 100,
					shF: 40,
					elF: 70,
				}),
			],
			[
				0.3,
				pose({
					hipN: 44,
					kneeN: 66,
					hipF: 36,
					kneeF: 58,
					lean: -22,
					shN: 50,
					elN: 90,
					shF: 64,
					elF: 40,
					abF: 22,
				}),
			],
			[
				0.65,
				pose({
					hipN: 84,
					kneeN: 40,
					hipF: 80,
					kneeF: 50,
					lean: -52,
					shN: 50,
					elN: 60,
					abN: 22,
					shF: 64,
					elF: 30,
					abF: 32,
				}),
			],
			[1, SUPINE],
		],
	},
	// The tackle: off his feet into him, arms round him, taking him down.
	dive: { kind: "act", keys: DIVE_KEYS },
	// Laid out for a ball: off his feet, flat out over the turf with his
	// arms out ahead of him to it, and down on it.
	diveCatch: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 30,
					kneeN: 50,
					hipF: -10,
					kneeF: 30,
					lean: 22,
					shN: 60,
					elN: 40,
					shF: 60,
					elF: 40,
				}),
			],
			[
				0.35,
				pose({
					hipN: -30,
					kneeN: 20,
					hipF: 44,
					kneeF: 80,
					lean: 52,
					shN: 112,
					elN: 12,
					abN: 6,
					shF: 112,
					elF: 12,
					abF: 6,
				}),
			],
			[
				0.6,
				pose({
					hipN: -70,
					kneeN: 10,
					hipF: -60,
					kneeF: 24,
					lean: 76,
					shN: 104,
					elN: 16,
					abN: 4,
					shF: 104,
					elF: 16,
					abF: 4,
					wrN: 20,
					wrF: 20,
				}),
			],
			[
				0.8,
				pose({
					hipN: -84,
					kneeN: 6,
					hipF: -80,
					kneeF: 14,
					lean: 84,
					shN: 70,
					elN: 80,
					abN: 10,
					shF: 70,
					elF: 80,
					abF: 10,
				}),
			],
			[1, { ...PRONE, shN: 24, elN: 104, abN: 8, wrN: 4 }],
		],
	},
	// A loose ball picked up on the run: down low over it, both hands
	// under it, and up with it into him.
	scoop: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 34,
					kneeN: 46,
					hipF: -6,
					kneeF: 36,
					lean: 26,
					shN: 50,
					elN: 40,
					shF: 50,
					elF: 40,
				}),
			],
			[
				0.4,
				pose({
					hipN: 110,
					kneeN: 56,
					hipF: -20,
					kneeF: 118,
					lean: 41,
					shN: 32,
					elN: 14,
					abN: -11,
					shF: 32,
					elF: 14,
					abF: -11,
					wrN: 20,
					wrF: 20,
				}),
			],
			[
				1,
				pose({
					hipN: 30,
					kneeN: 40,
					hipF: -4,
					kneeF: 30,
					lean: 20,
					shN: 30,
					elN: 104,
					abN: 4,
					shF: 30,
					elF: 104,
					abF: 4,
				}),
			],
		],
	},
	// Up off the turf: a foot under him and his hands down, onto a knee,
	// then his feet.
	getUp: { kind: "act", keys: GET_UP_KEYS },
	// Up off his back: sat up, his feet drawn under him, up off them.
	getUpBack: {
		kind: "act",
		keys: [
			[0, SUPINE],
			[
				0.35,
				pose({
					hipN: 118,
					kneeN: 112,
					hipF: 124,
					kneeF: 118,
					lean: -14,
					shN: 20,
					elN: 10,
					abN: 20,
					shF: 20,
					elF: 10,
					abF: 20,
					wide: 0.6,
				}),
			],
			[
				0.65,
				pose({
					hipN: 96,
					kneeN: 130,
					hipF: 100,
					kneeF: 134,
					lean: 34,
					shN: 30,
					elN: 30,
					shF: 30,
					elF: 30,
					wide: 0.6,
				}),
			],
			[1, BASE],
		],
	},
	// In on a pile: still driving his legs, then pitched forward onto his
	// knees over it.
	pileIn: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 24,
					kneeN: 56,
					hipF: 12,
					kneeF: 48,
					lean: 42,
					shN: 82,
					elN: 20,
					shF: 82,
					elF: 20,
				}),
			],
			[
				0.45,
				pose({
					hipN: 44,
					kneeN: 96,
					hipF: -6,
					kneeF: 70,
					lean: 62,
					shN: 88,
					elN: 22,
					abN: 26,
					shF: 88,
					elF: 22,
					abF: 26,
					wide: 0.7,
				}),
			],
			[1, OVER_PILE],
		],
	},
	overPile: { kind: "still", pose: OVER_PILE },
	// Over the top of a pile: off his feet onto it, laid out flat across
	// the top of it, and up off it.
	pileDive: { kind: "act", keys: DIVE_KEYS },
	pileTop: { kind: "still", pose: PRONE },
	offTop: { kind: "act", keys: GET_UP_KEYS },
	// Up off a pile: hands down on it, pushed up off his knees, a foot under
	// him, and up.
	offPile: {
		kind: "act",
		keys: [
			[0, OVER_PILE],
			[
				0.35,
				pose({
					hipN: 30,
					kneeN: 112,
					hipF: 22,
					kneeF: 108,
					lean: 52,
					shN: 56,
					elN: 8,
					abN: 16,
					shF: 56,
					elF: 8,
					abF: 16,
					wide: 0.7,
				}),
			],
			[
				0.7,
				pose({
					hipN: 78,
					kneeN: 88,
					hipF: -16,
					kneeF: 100,
					lean: 28,
					shN: 24,
					elN: 22,
					shF: 30,
					elF: 30,
					wide: 0.5,
				}),
			],
			[1, BASE],
		],
	},
	// A quarterback giving himself up: down feet first on the run, on his
	// seat, the lead leg out ahead of him and the other drawn up, leaning
	// back away from the hit.
	qbSlide: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 34,
					kneeN: 40,
					hipF: -18,
					kneeF: 50,
					lean: 12,
					shF: 30,
					elF: 60,
					abF: 20,
				}),
			],
			[
				0.3,
				pose({
					hipN: 64,
					kneeN: 18,
					hipF: 52,
					kneeF: 96,
					lean: -14,
					shF: 10,
					elF: 50,
					abF: 30,
				}),
			],
			[0.6, { ...SLID, lean: -40, hipF: 116, kneeF: 120 }],
			[1, SLID],
		],
	},
	slid: { kind: "still", pose: SLID },
	// Up off his seat: his feet drawn under him, up off them.
	sitUp: {
		kind: "act",
		keys: [
			[0, SLID],
			[
				0.5,
				pose({
					hipN: 96,
					kneeN: 130,
					hipF: 100,
					kneeF: 134,
					lean: 34,
					shN: 30,
					elN: 30,
					shF: 30,
					elF: 30,
					wide: 0.6,
				}),
			],
			[1, BASE],
		],
	},

	// The kicking game.
	// The long snap: both hands on the ball, whipped back between his legs
	// and up after it, and his head up to block.
	longSnap: {
		kind: "act",
		keys: [
			[0, { ...CENTER, shF: 22, elF: 4, abF: -30 }],
			[
				0.3,
				{
					...CENTER,
					hipN: 58,
					hipF: 58,
					lean: 74,
					shN: -46,
					elN: 2,
					abN: 4,
					shF: -46,
					elF: 2,
					abF: 4,
				},
			],
			[
				1,
				pose({
					hipN: 44,
					kneeN: 70,
					hipF: 44,
					kneeF: 70,
					lean: 34,
					shN: 50,
					elN: 70,
					abN: 10,
					shF: 50,
					elF: 70,
					abF: 10,
					wide: 1.1,
				}),
			],
		],
	},
	// A place kick, right-footed, soccer style: the left foot planted beside
	// the ball, the right leg cocked back and whipped through it (at 0.4)
	// and on up, the left arm out wide for balance, the shoulders coming
	// round after it.
	kick: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: -28,
					kneeN: 40,
					hipF: 26,
					kneeF: 14,
					lean: 6,
					toeN: 0.5,
					shN: -16,
					elN: 30,
					abN: 18,
					shF: 46,
					elF: 22,
					abF: 36,
					wide: 0.4,
				}),
			],
			[
				0.22,
				pose({
					hipN: -40,
					kneeN: 104,
					hipF: 16,
					kneeF: 24,
					lean: 2,
					tilt: 8,
					shN: 26,
					elN: 30,
					abN: 14,
					shF: 30,
					elF: 14,
					abF: 74,
					wide: 0.5,
				}),
			],
			[
				0.4,
				pose({
					hipN: 22,
					kneeN: 34,
					hipF: 12,
					kneeF: 26,
					lean: -2,
					tilt: 10,
					shN: 8,
					elN: 26,
					abN: 30,
					shF: 30,
					elF: 12,
					abF: 80,
					wide: 0.5,
				}),
			],
			[
				0.66,
				pose({
					hipN: 118,
					kneeN: 6,
					toeN: 1,
					hipF: -6,
					kneeF: 8,
					toeF: 0.6,
					lean: -16,
					tilt: 6,
					twist: 22,
					shN: 50,
					elN: 30,
					abN: -6,
					shF: 40,
					elF: 14,
					abF: 70,
					wide: 0.4,
				}),
			],
			[
				1,
				pose({
					hipN: 12,
					kneeN: 20,
					hipF: 18,
					kneeF: 22,
					lean: 6,
					shN: 16,
					elN: 26,
					abN: 16,
					shF: 22,
					elF: 26,
					abF: 22,
				}),
			],
		],
	},
	// A punt: two steps with the ball held out in front in both hands, the
	// drop (0.42), the right foot up through it (0.55) and on up over his
	// head, his arms flung out wide.
	punt: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: -14,
					kneeN: 30,
					hipF: 22,
					kneeF: 16,
					lean: 8,
					shN: 62,
					elN: 26,
					abN: 6,
					shF: 62,
					elF: 26,
					abF: 6,
				}),
			],
			[
				0.3,
				pose({
					hipN: -30,
					kneeN: 72,
					hipF: 26,
					kneeF: 18,
					lean: 6,
					shN: 66,
					elN: 12,
					abN: 4,
					shF: 60,
					elF: 16,
					abF: 8,
				}),
			],
			[
				0.42,
				pose({
					hipN: -4,
					kneeN: 76,
					hipF: 16,
					kneeF: 22,
					lean: 4,
					shN: 52,
					elN: 10,
					abN: 24,
					shF: 50,
					elF: 12,
					abF: 30,
				}),
			],
			[
				0.55,
				pose({
					hipN: 62,
					kneeN: 20,
					hipF: 10,
					kneeF: 16,
					lean: -4,
					shN: 40,
					elN: 10,
					abN: 50,
					shF: 46,
					elF: 10,
					abF: 56,
				}),
			],
			[
				0.8,
				pose({
					hipN: 128,
					kneeN: 2,
					toeN: 1,
					hipF: -14,
					kneeF: 6,
					toeF: 1,
					lean: -24,
					shN: 70,
					elN: 10,
					abN: 50,
					shF: 70,
					elF: 10,
					abF: 50,
				}),
			],
			[
				1,
				pose({
					hipN: 14,
					kneeN: 22,
					hipF: 12,
					kneeF: 20,
					lean: 8,
					shN: 20,
					elN: 26,
					abN: 18,
					shF: 22,
					elF: 26,
					abF: 20,
				}),
			],
		],
	},

	// ---- after a big play ----
	// The ball spiked: up over his head, and slammed down into the turf in
	// front of him.
	spike: {
		kind: "act",
		keys: [
			[0, pose({ shN: 120, elN: 70, abN: 24, wrN: 20, lean: -2 })],
			[0.4, pose({ shN: 172, elN: 26, abN: 16, wrN: 20, lean: -8, kneeN: 12 })],
			[
				0.6,
				pose({
					shN: 36,
					elN: 8,
					abN: 8,
					wrN: -20,
					lean: 32,
					hipN: 30,
					kneeN: 42,
					hipF: 18,
					kneeF: 36,
				}),
			],
			[0.8, pose({ shN: 6, elN: 20, abN: 10, lean: 20, hipN: 14, kneeN: 24 })],
			[1, BASE],
		],
	},
	griddy: { kind: "act", keys: GRIDDY_KEYS },
	// Both arms straight up, pointing at the sky.
	pointSky: {
		kind: "still",
		pose: pose({
			shN: 168,
			elN: 6,
			abN: 16,
			shF: 168,
			elF: 6,
			abF: 16,
			lean: -8,
		}),
	},
	// Down on a knee in the end zone, head bowed, a hand up to the sky.
	kneelPoint: {
		kind: "still",
		pose: pose({
			hipN: -20,
			kneeN: 104,
			hipF: 80,
			kneeF: 88,
			lean: 18,
			shN: 20,
			elN: 30,
			abN: 12,
			shF: 166,
			elF: 8,
			abF: 14,
			wide: 0.5,
		}),
	},
	// Bowling it: a step into it, the ball swung back, down low in a lunge
	// as it is let go along the turf, and the arm on up after it.
	bowl: {
		kind: "act",
		keys: [
			[0, pose({ shN: 40, elN: 80, abN: 10, lean: 8 })],
			[
				0.35,
				pose({
					hipN: -16,
					kneeN: 28,
					hipF: 40,
					kneeF: 50,
					lean: 30,
					shN: -56,
					elN: 12,
					abN: 10,
					shF: 50,
					elF: 40,
					abF: 30,
				}),
			],
			[
				0.68,
				pose({
					hipN: -30,
					kneeN: 56,
					hipF: 76,
					kneeF: 100,
					lean: 52,
					shN: 46,
					elN: 6,
					abN: 6,
					shF: 30,
					elF: 30,
					abF: 50,
				}),
			],
			[
				1,
				pose({
					hipN: -20,
					kneeN: 40,
					hipF: 60,
					kneeF: 80,
					lean: 36,
					shN: 120,
					elN: 10,
					abN: 8,
					shF: 30,
					elF: 30,
					abF: 50,
				}),
			],
		],
	},
	// The ball held up high, the other fist pumped.
	ballUp: {
		kind: "still",
		pose: pose({
			shN: 172,
			elN: 12,
			abN: 14,
			shF: 128,
			elF: 70,
			abF: 30,
			lean: -4,
		}),
	},
	// Up off both feet, both arms up: a teammate's score, a chest bump.
	leap: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 40,
					kneeN: 70,
					hipF: 40,
					kneeF: 70,
					lean: 24,
					shN: -24,
					elN: 30,
					shF: -24,
					elF: 30,
				}),
			],
			[
				0.3,
				pose({
					hipN: 6,
					kneeN: 14,
					hipF: 6,
					kneeF: 14,
					lean: 2,
					shN: 160,
					elN: 20,
					abN: 20,
					shF: 160,
					elF: 20,
					abF: 20,
				}),
			],
			[
				0.55,
				pose({
					hipN: 34,
					kneeN: 64,
					hipF: 30,
					kneeF: 60,
					lean: -4,
					shN: 172,
					elN: 10,
					abN: 16,
					shF: 172,
					elF: 10,
					abF: 16,
				}),
			],
			[
				0.82,
				pose({
					hipN: 34,
					kneeN: 54,
					hipF: 34,
					kneeF: 54,
					lean: 16,
					shN: 120,
					elN: 30,
					abN: 24,
					shF: 120,
					elF: 30,
					abF: 24,
				}),
			],
			[1, BASE],
		],
	},
	// A sack: the arms flung out wide, and up into a V, and out again.
	flex: over(
		{ shN: 100, elN: 40, abN: 80, shF: 100, elF: 40, abF: 80, lean: -8 },
		{ shN: 150, elN: 26, abN: 50, shF: 150, elF: 26, abF: 50, lean: -10 },
	),
	// A new set of downs: his free arm up and thrust out, pointing the way
	// they are going, held there a moment.
	pointFirst: {
		kind: "act",
		keys: [
			[0, BASE],
			[0.28, pose({ shF: 150, elF: 76, abF: 24, lean: -4 })],
			[
				0.5,
				pose({
					hipN: 22,
					kneeN: 20,
					hipF: -6,
					kneeF: 8,
					lean: 8,
					shF: 92,
					elF: 2,
					abF: 8,
					wrF: 0,
				}),
			],
			[
				0.84,
				pose({
					hipN: 22,
					kneeN: 20,
					hipF: -6,
					kneeF: 8,
					lean: 6,
					shF: 94,
					elF: 4,
					abF: 8,
					wrF: 0,
				}),
			],
			[1, BASE],
		],
	},
	// Caught at the sideline: his hands up to it as it comes, then both
	// toes dragged in along the turf as he goes over, tipped toward it, the
	// ball pulled into his chest; and a step out.
	toeTap: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 30,
					kneeN: 50,
					hipF: -16,
					kneeF: 30,
					lean: 12,
					shN: 90,
					elN: 40,
					shF: 90,
					elF: 40,
					toeF: 0.5,
				}),
			],
			[
				0.28,
				pose({
					hipN: 8,
					kneeN: 12,
					hipF: -24,
					kneeF: 8,
					lean: -8,
					tilt: -12,
					shN: 116,
					elN: 20,
					abN: 6,
					shF: 116,
					elF: 20,
					abF: 6,
					wrN: 15,
					wrF: 15,
					toeF: 1,
					wideF: 0.4,
				}),
			],
			[
				0.62,
				pose({
					hipN: 14,
					kneeN: 20,
					hipF: -40,
					kneeF: 4,
					lean: -18,
					tilt: -24,
					shN: 30,
					elN: 104,
					abN: 16,
					shF: 30,
					elF: 104,
					abF: 16,
					toeN: 0.8,
					toeF: 1,
					wide: 0.8,
				}),
			],
			[
				1,
				pose({
					hipN: 26,
					kneeN: 40,
					hipF: -4,
					kneeF: 16,
					lean: 2,
					tilt: -6,
					shN: 22,
					elN: 106,
					abN: 12,
					shF: 22,
					elF: 106,
					abF: 12,
				}),
			],
		],
	},
	// Hands on his hips: the other side's score.
	onHips: {
		kind: "still",
		pose: pose({
			shN: -14,
			elN: 106,
			abN: 48,
			wrN: 10,
			shF: -14,
			elF: 106,
			abF: 48,
			wrF: 10,
			lean: 8,
		}),
	},
	// Arms folded across his chest: a coach watching.
	armsFolded: {
		kind: "still",
		pose: pose({ shN: 46, elN: 124, abN: -34, shF: 50, elF: 120, abF: -34 }),
	},
	// Sat on the bench, leaning on his forearms on his thighs.
	sit: {
		kind: "still",
		pose: pose({
			hipN: 88,
			kneeN: 90,
			hipF: 82,
			kneeF: 84,
			lean: 16,
			shN: 34,
			elN: 46,
			shF: 30,
			elF: 50,
			wide: 0.9,
		}),
	},
	// ---- a man hurt ----
	// Down on his back on the turf, one knee drawn up, a hand to it.
	hurt: {
		kind: "still",
		pose: pose({
			hipN: 90,
			kneeN: 6,
			hipF: 130,
			kneeF: 80,
			lean: -84,
			shN: 40,
			elN: 30,
			abN: 30,
			shF: 70,
			elF: 50,
			abF: 10,
			wide: 0.5,
		}),
	},
	// Face down on the turf as he was brought down, the far knee drawn up,
	// his arms out by his head.
	hurtProne: {
		kind: "still",
		pose: {
			...PRONE,
			hipF: -66,
			kneeF: 40,
			shN: 96,
			elN: 30,
			abN: 44,
			shF: 40,
			elF: 60,
			abF: 30,
		},
	},
	// Up on a knee with them (part of the way up - see GET_UP_KEYS), a hand
	// on his knee, the other down by him.
	hurtKnee: {
		kind: "still",
		pose: pose({
			hipN: 79,
			kneeN: 112,
			hipF: -37,
			kneeF: 84,
			lean: 40,
			shN: 44,
			elN: 50,
			abN: 10,
			shF: 20,
			elF: 20,
			abF: 14,
			wide: 0.56,
		}),
	},
	// Sat up, both hands round the knee.
	hurtUp: {
		kind: "still",
		pose: pose({
			hipN: 90,
			kneeN: 6,
			hipF: 122,
			kneeF: 72,
			lean: -18,
			shN: 62,
			elN: 40,
			abN: 6,
			shF: 58,
			elF: 52,
			abF: 4,
			wide: 0.5,
		}),
	},
	// A trainer down on a knee beside him, leaning over, hands to him.
	tend: {
		kind: "still",
		pose: pose({
			hipN: -8,
			kneeN: 86,
			hipF: 92,
			kneeF: 62,
			lean: 34,
			shN: 58,
			elN: 28,
			abN: 14,
			shF: 52,
			elF: 36,
			abF: 14,
			wide: 0.5,
		}),
	},
	// A hand down to a man on the turf, and him pulled up by it.
	helpUp: {
		kind: "act",
		keys: [
			[0, BASE],
			[
				0.3,
				pose({
					hipN: 22,
					kneeN: 30,
					hipF: 26,
					kneeF: 32,
					lean: 38,
					shN: 58,
					elN: 8,
					abN: 8,
					shF: 14,
					elF: 30,
				}),
			],
			[
				0.65,
				pose({
					hipN: 10,
					kneeN: 18,
					hipF: 12,
					kneeF: 18,
					lean: 18,
					shN: 46,
					elN: 36,
					abN: 6,
					shF: 10,
					elF: 26,
				}),
			],
			[1, BASE],
		],
	},
	// The men round him on a knee, waiting.
	takeKnee: {
		kind: "still",
		pose: pose({
			hipN: -8,
			kneeN: 86,
			hipF: 92,
			kneeF: 62,
			lean: 6,
			shN: 18,
			elN: 40,
			abN: 14,
			shF: 34,
			elF: 62,
			abF: 8,
			wide: 0.5,
		}),
	},
	// Helped off: his arms over the shoulders of the men either side of him.
	overShoulders: {
		kind: "still",
		pose: pose({ shN: 12, elN: 44, abN: 78, shF: 12, elF: 44, abF: 78 }),
	},

	// ---- the game over ----
	// A handshake: his right hand out in front of him at his belt, taken,
	// shaken twice and let go.
	// The coin toss: the coin out on his thumb, flicked up, his hand up after
	// it - and leaning to look down at it on the turf.
	coinFlip: {
		kind: "act",
		keys: [
			[0, BASE],
			[0.22, pose({ shN: 46, elN: 98, abN: -8 })],
			[0.34, pose({ shN: 58, elN: 84, abN: -8, wrN: 24 })],
			[0.44, pose({ shN: 104, elN: 30, abN: -4, wrN: -16 })],
			[0.56, pose({ shN: 112, elN: 22, abN: -4 })],
			[0.8, pose({ shN: 30, elN: 64, abN: 6, lean: 16 })],
			[1, BASE],
		],
	},
	// An official putting the ball down for the next snap: bent to it, his
	// hands on it, and up off it.
	spotBall: {
		kind: "act",
		keys: [
			[
				0,
				pose({
					hipN: 62,
					kneeN: 70,
					hipF: 50,
					kneeF: 62,
					lean: 52,
					shN: 64,
					elN: 12,
					abN: 4,
					shF: 64,
					elF: 12,
					abF: 4,
				}),
			],
			[
				0.45,
				pose({
					hipN: 60,
					kneeN: 68,
					hipF: 48,
					kneeF: 60,
					lean: 50,
					shN: 56,
					elN: 20,
					abN: 4,
					shF: 56,
					elF: 20,
					abF: 4,
				}),
			],
			[
				0.75,
				pose({
					hipN: 24,
					kneeN: 28,
					hipF: 18,
					kneeF: 24,
					lean: 22,
					shN: 24,
					elN: 30,
					shF: 24,
					elF: 30,
				}),
			],
			[1, BASE],
		],
	},
	// A coach going over it with a man: his play sheet held up at his chest in
	// one hand, the other hand out, making his points with it.
	coachTalk: {
		kind: "act",
		keys: [
			[0, pose({ ...SHEET })],
			[0.14, pose({ ...SHEET, shN: 34, elN: 84, abN: 6, lean: 6 })],
			[0.28, pose({ ...SHEET, shN: 52, elN: 46, abN: 18, lean: 6, twist: 6 })],
			[0.42, pose({ ...SHEET, shN: 36, elN: 80, abN: 4, lean: 8 })],
			[0.58, pose({ ...SHEET, shN: 56, elN: 40, abN: 22, lean: 8, twist: -6 })],
			[0.72, pose({ ...SHEET, shN: 40, elN: 74, abN: 8, lean: 6 })],
			[0.86, pose({ ...SHEET, shN: 50, elN: 52, abN: 14, lean: 4 })],
			[1, pose({ ...SHEET })],
		],
	},
	shake: {
		kind: "act",
		keys: [
			[0, BASE],
			[0.24, pose(SHAKE)],
			[0.4, pose(SHAKE_DOWN)],
			[0.56, pose(SHAKE)],
			[0.72, pose(SHAKE_DOWN)],
			[0.84, pose(SHAKE)],
			[1, BASE],
		],
	},
	// In close: the right hands clasped between them, his left arm round
	// the other man's back, slapping it twice.
	hug: {
		kind: "act",
		keys: [
			[0, BASE],
			[0.22, pose(HUG)],
			[0.38, pose(HUG_SLAP)],
			[0.5, pose(HUG)],
			[0.62, pose(HUG_SLAP)],
			[0.78, pose(HUG)],
			[1, BASE],
		],
	},
	// A step in, up off both feet, chests together in the air with the arms
	// flung back, and down (the court's).
	chestBump: {
		kind: "act",
		keys: [
			[0, BASE],
			[
				0.25,
				pose({
					shN: -20,
					elN: 30,
					abN: 20,
					shF: -20,
					elF: 30,
					abF: 20,
					hipN: 40,
					kneeN: 70,
					hipF: 40,
					kneeF: 70,
					lean: 18,
				}),
			],
			[
				0.5,
				pose({
					shN: -40,
					elN: 20,
					abN: 40,
					shF: -40,
					elF: 20,
					abF: 40,
					hipN: 10,
					kneeN: 30,
					hipF: 14,
					kneeF: 36,
					lean: -14,
				}),
			],
			[
				0.75,
				pose({
					shN: 20,
					elN: 40,
					abN: 30,
					shF: 20,
					elF: 40,
					abF: 30,
					hipN: 30,
					kneeN: 50,
					hipF: 30,
					kneeF: 50,
					lean: 6,
				}),
			],
			[1, BASE],
		],
	},
	// A high five: his right arm up out to his side, back by his helmet, then
	// up and through, reaching, to meet the other man's up over their heads
	// (at HIGH_FIVE_AT), and round and down out to his side.
	highFive: {
		kind: "act",
		keys: [
			[0, BASE],
			[0.15, pose({ shN: 30, elN: 90, abN: 85, shF: 6, elF: 24, abF: 10 })],
			[
				0.32,
				pose({
					shN: 152,
					elN: 75,
					abN: 26,
					shF: 10,
					elF: 30,
					abF: 12,
					tuck: 1,
					lean: -4,
				}),
			],
			[
				HIGH_FIVE_AT,
				pose({
					shN: 135,
					elN: 4,
					abN: -8,
					shF: 16,
					elF: 30,
					abF: 14,
					tuck: 1,
					lean: 9,
				}),
			],
			[
				0.6,
				pose({
					shN: 150,
					elN: 15,
					abN: 60,
					shF: 14,
					elF: 30,
					abF: 12,
					lean: 5,
				}),
			],
			[0.76, pose({ shN: 40, elN: 20, abN: 70, shF: 8, elF: 24, lean: 2 })],
			[1, BASE],
		],
	},
	// His helmet off: both hands up to it, pulled up off his head, and down
	// in his left hand.
	helmetOff: {
		kind: "act",
		keys: [
			[0, BASE],
			[
				0.3,
				pose({
					shN: 150,
					elN: 125,
					abN: 26,
					shF: 150,
					elF: 125,
					abF: 26,
					tuck: 1,
				}),
			],
			[
				0.5,
				pose({
					shN: 168,
					elN: 70,
					abN: 18,
					shF: 168,
					elF: 70,
					abF: 18,
					lean: -4,
					tuck: 1,
				}),
			],
			[0.75, pose({ shN: 20, elN: 30, abN: 12, shF: 80, elF: 60, abF: 20 })],
			[1, BASE],
		],
	},
	// The cooler (see shower.ts) carried between two men, a handle each: in
	// his right hand, hung at his side; and, the two of them, up to their
	// chests with it, over their heads, tipped forward over the man in front
	// of them - and let go.
	coolerCarry: { kind: "still", pose: pose(COOLER_CARRY) },
	coolerDump: {
		kind: "act",
		keys: [
			[0, pose(COOLER_CARRY)],
			[
				0.28,
				pose({
					shN: 75,
					elN: 70,
					abN: 8,
					shF: 80,
					elF: 80,
					abF: -34,
					hipN: 14,
					kneeN: 20,
					hipF: 14,
					kneeF: 20,
				}),
			],
			[
				0.5,
				pose({
					shN: 162,
					elN: 22,
					abN: 8,
					shF: 155,
					elF: 36,
					abF: -22,
					lean: -6,
					tuck: 1,
				}),
			],
			[
				0.64,
				pose({
					shN: 142,
					elN: 8,
					abN: 8,
					shF: 136,
					elF: 14,
					abF: -20,
					lean: 6,
					tuck: 1,
				}),
			],
			[
				0.86,
				pose({
					shN: 136,
					elN: 6,
					abN: 8,
					shF: 130,
					elF: 12,
					abF: -20,
					lean: 9,
					tuck: 1,
				}),
			],
			[1, BASE],
		],
	},
	// Caught by it from behind: hunched, his arms up and out, then his hands
	// to his face and head to wipe it off - and his arms out, laughing.
	doused: {
		kind: "act",
		keys: [
			[0, BASE],
			[0.08, pose(DOUSED)],
			[0.3, pose({ ...DOUSED, lean: 30, shN: 66, shF: 66 })],
			[0.46, pose({ ...HANDS_ON_HEAD, elN: 130, elF: 130, lean: 16 })],
			[0.62, pose({ ...HANDS_ON_HEAD, shN: 150, shF: 150, lean: 12 })],
			[
				0.82,
				pose({
					shN: 40,
					elN: 30,
					abN: 50,
					shF: 40,
					elF: 30,
					abF: 50,
					lean: -4,
				}),
			],
			[1, BASE],
		],
	},
	// His right fist pumped up over his head, again and again.
	fistPump: over(
		{ shN: 132, elN: 96, abN: 24, shF: 10, elF: 70, abF: 16, lean: -2 },
		{ shN: 172, elN: 16, abN: 16, shF: 6, elF: 96, abF: 16, lean: -6 },
	),
	// A call gone against his side (the 3D court's coach's): both arms out
	// from his sides, palms up, leaning back from it.
	protest: {
		kind: "act",
		keys: [
			[0, BASE],
			[
				0.25,
				pose({
					shN: 44,
					elN: 62,
					abN: 42,
					wrN: 46,
					shF: 44,
					elF: 62,
					abF: 42,
					wrF: 46,
					lean: -6,
				}),
			],
			[
				0.8,
				pose({
					shN: 50,
					elN: 56,
					abN: 48,
					wrN: 50,
					shF: 50,
					elF: 56,
					abF: 48,
					wrF: 50,
					lean: -8,
				}),
			],
			[1, BASE],
		],
	},
	// Clapping, hands out in front of his chest.
	clap: {
		kind: "act",
		keys: [
			[0, BASE],
			[0.16, pose(CLAP_APART)],
			[0.26, pose(CLAP_TOGETHER)],
			[0.38, pose(CLAP_APART)],
			[0.48, pose(CLAP_TOGETHER)],
			[0.6, pose(CLAP_APART)],
			[0.7, pose(CLAP_TOGETHER)],
			[0.82, pose(CLAP_APART)],
			[1, BASE],
		],
	},
	// A cheerleader's poms going between plays: up in a V over her head,
	// out level, down in a V at her sides, out level again - a bounce at
	// the knees on each.
	poms: {
		kind: "loop",
		n: 4,
		fps: 6,
		pose: (i) =>
			pose(
				i % 2 === 1
					? { ...POMS_OUT, kneeN: 20, kneeF: 20, hipN: 8, hipF: 8 }
					: i === 0
						? POMS_UP
						: POMS_DOWN,
			),
	},
	// A camera up to the eye: the right hand on the grip at his face, the
	// left under the lens out in front of it.
	shoot: {
		kind: "still",
		pose: pose({
			shN: 70,
			elN: 105,
			abN: -10,
			shF: 85,
			elF: 55,
			abF: -22,
			tuck: 1,
		}),
	},
	// A TV camera on his right shoulder: the right hand up on its grip by
	// his head, the left out in front on the lens.
	shoulderCam: {
		kind: "still",
		pose: pose({
			shN: 84,
			elN: 104,
			abN: 46,
			shF: 58,
			elF: 72,
			abF: -14,
			tuck: 1,
		}),
	},
	// A pole held upright in front of him in both hands, the right over the
	// left: the chain crew's rods, the down box.
	holdPole: {
		kind: "still",
		pose: pose({
			shN: 38,
			elN: 98,
			abN: -4,
			shF: 24,
			elF: 84,
			abF: -4,
		}),
	},

	// ---- a timeout ----
	// A drink: a bottle up over his face mask in his right hand, his head
	// back a touch for it, a squirt or two - and down.
	drink: {
		kind: "act",
		keys: [
			[0, pose({ shN: 12, elN: 30, abN: 8 })],
			[0.2, pose({ shN: 104, elN: 82, abN: 4, lean: -6, shF: 8, elF: 16 })],
			[0.38, pose({ shN: 110, elN: 86, abN: 2, lean: -10, shF: 8, elF: 16 })],
			[0.52, pose({ shN: 106, elN: 80, abN: 4, lean: -8, shF: 8, elF: 16 })],
			[0.7, pose({ shN: 112, elN: 88, abN: 2, lean: -11, shF: 8, elF: 16 })],
			[0.86, pose({ shN: 60, elN: 70, abN: 6, lean: -2 })],
			[1, pose({ shN: 12, elN: 30, abN: 8 })],
		],
	},
	// A trainer with a carrier of bottles hung from his right hand, out from
	// his leg.
	carryWater: {
		kind: "still",
		pose: pose({ shN: 6, elN: 10, abN: 16, shF: 10, elF: 24 }),
	},

	// ---- the officials ----
	// The flag: a hand to it at his belt, up over his head, and thrown.
	flagToss: {
		kind: "act",
		keys: [
			[0, BASE],
			[0.25, pose({ shN: -20, elN: 70, abN: 30 })],
			[0.55, pose({ shN: 165, elN: 40, abN: 10 })],
			[0.75, pose({ shN: 120, elN: 8, abN: 6, wrN: -20 })],
			[1, pose({ shN: 60, elN: 20, abN: 10 })],
		],
	},
	// A score, a kick good: both arms straight up.
	sigTD: held({ shN: 178, elN: 2, abN: 6, shF: 178, elF: 2, abF: 6 }),
	// A safety: the palms together over his head.
	sigSafety: held({
		shN: 176,
		elN: 24,
		abN: -12,
		shF: 176,
		elF: 24,
		abF: -12,
		tuck: 1,
	}),
	// Incomplete, no good, declined: the arms swung across in front of him.
	sigWave: over(
		{ shN: 90, elN: 4, abN: 84, shF: 90, elF: 4, abF: 84 },
		{ shN: 88, elN: 10, abN: -22, shF: 92, elF: 10, abF: -30 },
	),
	// Time out, the clock stopped: both arms up, crossed and uncrossed over
	// his head.
	sigTime: over(
		{ shN: 166, elN: 6, abN: 26, shF: 166, elF: 6, abF: 26, tuck: 1 },
		{ shN: 166, elN: 6, abN: -16, shF: 166, elF: 6, abF: -16, tuck: 1 },
	),
	// An arm straight out to his right: the way the offense goes for a
	// first down, at the side that fouled.
	sigSide: held({ shN: 90, elN: 2, abN: 86 }),
	// Holding: one wrist gripped in front of him, and pulled down.
	sigHold: over(
		{ shN: 52, elN: 84, abN: -24, shF: 48, elF: 96, abF: -16 },
		{ shN: 34, elN: 80, abN: -24, shF: 30, elF: 92, abF: -16 },
	),
	// A false start, an illegal formation or shift: the forearms rolled over
	// each other.
	sigRoll: over(
		{ shN: 62, elN: 96, abN: -10, shF: 50, elF: 110, abF: -10 },
		{ shN: 48, elN: 110, abN: -10, shF: 64, elF: 96, abF: -10 },
	),
	// Offside, encroachment, the neutral zone: hands on hips.
	sigHips: held({
		shN: -14,
		elN: 106,
		abN: 48,
		wrN: 10,
		shF: -14,
		elF: 106,
		abF: 48,
		wrF: 10,
	}),
	// Pass interference, a block in the back: both hands pushed out.
	sigPush: over(
		{ shN: 70, elN: 110, abN: 4, shF: 70, elF: 110, abF: 4 },
		{ shN: 92, elN: 8, abN: 4, shF: 92, elF: 8, abF: 4 },
	),
	// A personal foul: one wrist struck down on the other over his head.
	sigChop: over(
		{ shN: 175, elN: 10, abN: -5, shF: 145, elF: 95, abF: -35 },
		{ shN: 140, elN: 60, abN: -25, shF: 145, elF: 95, abF: -35 },
	),
	// The face mask: a hand to his own, pulled down.
	sigMask: over({ shN: 70, elN: 150, abN: 4 }, { shN: 52, elN: 120, abN: 4 }),
	// Unsportsmanlike conduct: the arms out wide, palms down.
	sigT: held({ shN: 90, elN: 2, abN: 88, shF: 90, elF: 2, abF: 88 }),
	// Delay of game: the arms folded across his chest.
	sigFold: held({ shN: 46, elN: 124, abN: -34, shF: 50, elF: 120, abF: -34 }),
	// Too many men, a man downfield who should not be: hands on his head.
	sigHead: held(HANDS_ON_HEAD),
	// A fake handoff taken: his arms shut round his belly on nothing.
	fakeCarry: held({ shN: 24, elN: 104, abN: -16, shF: 30, elF: 98, abF: -16 }),
	// A handoff taken from his right, or his left (see TAKE_KEYS).
	takeR: { kind: "act", keys: TAKE_KEYS },
	takeL: {
		kind: "act",
		keys: TAKE_KEYS.map(([k, q]): [number, Pose] => [k, mirror(q)]),
	},
	// A ball he should have had: his hands to his helmet, his head down.
	handsOnHelmet: held({ ...HANDS_ON_HEAD, lean: 10 }),
	// Illegal contact, illegal use of the hands: an open hand out in front.
	sigContact: held({ shN: 92, elN: 6, abN: 0, wrN: -40 }),
	// A fair catch called for: his right arm up straight over his head,
	// waved from side to side.
	fairCatch: over(
		{ shN: 172, elN: 4, abN: 32, tuck: 1 },
		{ shN: 172, elN: 4, abN: -6, tuck: 1 },
	),
} satisfies Record<string, FieldAnim>;

export type FieldAnimName = keyof typeof FIELD_ANIMS;

// The court's moves a football player makes as they are: walking, jogging,
// running, sprinting, backpedaling, sliding, shuffling across. (And the
// dance team's routine, the cheerleaders' at a timeout.)
const COURT_MOVES = [
	"walk",
	"jog",
	"run",
	"sprint",
	"back",
	"slide",
	"shuffle",
	"dance",
] as const;
export type CourtMove = (typeof COURT_MOVES)[number];
export type Move = FieldAnimName | CourtMove;

const isCourtMove = (m: Move): m is CourtMove =>
	(COURT_MOVES as readonly string[]).includes(m);

// What kind of thing a move is (see court3d/poses.ts): a still one, a loop
// on the clock, a cycle on the distance covered, an act from start to
// finish.
export const moveKind = (m: Move): "still" | "loop" | "cycle" | "act" =>
	isCourtMove(m) ? ANIMS[m as AnimName].kind : FIELD_ANIMS[m].kind;

// How many times round a loop goes a second.
export const loopRate = (m: Move): number => {
	const a = isCourtMove(m)
		? ANIMS[m as AnimName]
		: (FIELD_ANIMS[m] as FieldAnim | undefined);
	return a?.kind === "loop" ? a.fps / a.n : 0;
};

// How long a stride of a cycle is (feet). (The court's, a little longer
// for a man drawn bigger - his foot down staying put on the turf.)
const COURT_STRIDE = 1.1;
export const strideOf = (m: Move): number => {
	if (isCourtMove(m)) {
		const a = ANIMS[m as AnimName];
		return a.kind === "cycle" ? a.stride * COURT_STRIDE : 5;
	}
	const a = FIELD_ANIMS[m] as FieldAnim;
	return a.kind === "cycle" ? a.stride : 5;
};

// The beat a gait's strides keep going slowly (strides a second - see
// court3d/poses.ts shortSteps); none, for steps that keep none.
export const beatOf = (m: Move): number =>
	isCourtMove(m) ? stepBeat(m as AnimName) : 0;

// The pose partway through a move: an act from start (0) to finish (1), a
// loop or a cycle through one turn - mirrored, if he does it left-handed.
// (A gait's strides `len` of a full one: short steps.)
export const poseOfMove = (
	m: Move,
	phase: number,
	mirrored = false,
	len = 1,
): Pose => {
	let q: Pose;
	if (isCourtMove(m)) {
		q = poseAt(m as AnimName, phase);
		if (len < 1) {
			q = shortSteps(q, m as AnimName, [len, len], phase);
		}
	} else {
		const a = FIELD_ANIMS[m] as FieldAnim;
		if (a.kind === "still") {
			q = a.pose;
		} else if (a.kind === "act") {
			q = keyed(a.keys, Math.min(1, Math.max(0, phase)));
		} else {
			const p = (((phase % 1) + 1) % 1) * a.n;
			const i0 = Math.floor(p);
			q = lerpPose(a.pose(i0), a.pose((i0 + 1) % a.n), p - i0);
		}
	}
	return mirrored ? mirror(q) : q;
};

// THE FRAMES AN ACT IS DRAWN IN (phases, 0 to 1, the first and last its
// own): through each stretch between its keys as many as `least` over the
// whole act gives it, or more where he moves a long way in it - as many as
// no joint more than `most` degrees from one to the next would take - and
// none through a stretch he holds.
const DEGREES: (keyof Pose)[] = [
	"hipN",
	"kneeN",
	"hipF",
	"kneeF",
	"shN",
	"elN",
	"shF",
	"elF",
	"lean",
	"abN",
	"abF",
	"wrN",
	"wrF",
	"flare",
	"spinN",
	"spinF",
	"twist",
	"tilt",
];
// (Anything else - how wide his feet are, up on his toes, ... - as if this
// many degrees all the way.)
const SHARE_DEG = 60;
const apartBy = (a: Pose, b: Pose): number => {
	let most = 0;
	for (const k of Object.keys(a) as (keyof Pose)[]) {
		const d = Math.abs(a[k] - b[k]) * (DEGREES.includes(k) ? 1 : SHARE_DEG);
		most = Math.max(most, d);
	}
	return most;
};
// (Spread through each stretch by how far he moves, not how long it takes:
// he comes into a key and out of it slowly - see keyed - and evenly in
// time they would bunch there and jump in between. Measured in so many
// small steps, on a man of the usual build: how far his head, hands,
// feet, knees and elbows go.)
const FRAME_SAMPLES = 32;
const FRAME_BODY = drawnBody(footballBody());
const framePoints = (q: Pose): V3[] => {
	const k = skeleton(FRAME_BODY, q);
	return [
		k.head,
		k.pelvis,
		k.armR.mid,
		k.armR.end,
		k.armL.mid,
		k.armL.end,
		k.legR.mid,
		k.legR.end,
		k.legL.mid,
		k.legL.end,
	];
};
const actFrameSets = new Map<string, number[]>();
export const actFrames = (m: Move, least: number, most: number): number[] => {
	const key = `${m}|${least}|${most}`;
	let out = actFrameSets.get(key);
	if (!out) {
		const a = FIELD_ANIMS[m as FieldAnimName] as FieldAnim | undefined;
		out = [0];
		if (a?.kind === "act") {
			for (let i = 1; i < a.keys.length; i++) {
				const [u0, p0] = a.keys[i - 1]!;
				const [u1, p1] = a.keys[i]!;
				const d = apartBy(p0, p1);
				if (u1 <= u0 || d < 0.5) {
					continue;
				}
				// (How far he has gone through it, a little at a time: his
				// joints, and the most any part of him has moved.)
				let turned = 0;
				const gone = [0];
				let was = p0;
				let wasAt = framePoints(p0);
				for (let j = 1; j <= FRAME_SAMPLES; j++) {
					const q = keyed(a.keys, u0 + ((u1 - u0) * j) / FRAME_SAMPLES);
					const at = framePoints(q);
					let moved = 0;
					for (let k = 0; k < at.length; k++) {
						const x = at[k]!;
						const y = wasAt[k]!;
						moved = Math.max(
							moved,
							Math.hypot(x.f - y.f, x.s - y.s, x.u - y.u),
						);
					}
					turned += apartBy(was, q);
					gone.push(gone[j - 1]! + moved);
					was = q;
					wasAt = at;
				}
				const all = gone[FRAME_SAMPLES]!;
				const n = Math.max(
					Math.ceil((u1 - u0) * (least - 1) - 1e-6),
					Math.ceil(turned / most),
				);
				for (let j = 1, s = 1; j < n; j++) {
					const want = (all * j) / n;
					while (s < FRAME_SAMPLES && gone[s]! < want) {
						s++;
					}
					const g0 = gone[s - 1]!;
					const g1 = gone[s]!;
					const v = g1 > g0 ? (want - g0) / (g1 - g0) : 1;
					out.push(u0 + ((u1 - u0) * (s - 1 + v)) / FRAME_SAMPLES);
				}
				out.push(u1);
			}
		}
		if (out.at(-1)! < 1) {
			out.push(1);
		}
		actFrameSets.set(key, out);
	}
	return out;
};

// Two men meeting right hand to right hand partway through a move (a
// handshake, a high five): how far apart they stand, center to center, and
// how far each is turned to his left of straight at the other (radians)
// for the two hands to meet halfway.
export const handsMeet = (
	b: Body,
	m: Move,
	phase: number,
): { apart: number; turn: number } => {
	const hand = skeleton(b, poseOfMove(m, phase)).armR.end;
	return {
		apart: 2 * Math.hypot(hand.f, hand.s),
		turn: Math.atan2(-hand.s, hand.f),
	};
};

// How far off the turf his strides take him partway through a move (feet,
// as the court has it): running, between one foot and the next; walking,
// up on his toes.
export const strideLift = (m: Move, phase: number, len = 1): number =>
	!isCourtMove(m)
		? 0
		: len < 1
			? shortLift(m as AnimName, [len, len], phase)
			: bounceAt(m as AnimName, phase);

// How far off the turf he is partway through a move (feet): up off both
// feet in a leap, a chest bump - laid out on top of a pile, this high.
const PILE_TOP = 1.2;
const GRIDDY_HOP = 0.22;
export const liftOf = (m: Move, phase: number): number => {
	const arc = (lo: number, hi: number, up: number) =>
		phase > lo && phase < hi
			? up * Math.sin((Math.PI * (phase - lo)) / (hi - lo))
			: 0;
	switch (m) {
		case "leap":
			return arc(0.3, 0.82, 1.7);
		case "leapCatch":
			return arc(0.2, 0.8, 2.4);
		case "diveCatch":
			return arc(0.25, 0.75, 0.8);
		case "chestBump":
			return arc(0.32, 0.72, 1.5);
		case "highFive":
			return arc(0.36, 0.64, HIGH_FIVE_HOP);
		case "hurdle":
			return arc(0.12, 0.85, 2.4);
		case "griddy":
			return phase > 0.05 && phase < 0.95
				? GRIDDY_HOP * Math.abs(Math.cos(Math.PI * 8 * phase))
				: 0;
		case "pileDive":
			return (
				PILE_TOP * Math.min(1, phase / 0.6) +
				0.6 * Math.sin(Math.PI * Math.min(1, phase / 0.7))
			);
		case "pileTop":
			return PILE_TOP;
		case "offTop":
			return PILE_TOP * (1 - Math.min(1, phase / 0.45));
		default:
			return 0;
	}
};

// How he holds his hands in a move: running, loose fists; down in a stance
// or reading the play, open; standing about, relaxed.
const FISTS = new Set<Move>([
	"run",
	"sprint",
	"holdPole",
	"coolerCarry",
	"coolerDump",
]);
const RELAXED = new Set<Move>(["stand", "walk", "jog", "hands"]);
export const gripsOf = (m: Move, hold?: BallHold): [Grip, Grip] => {
	const g: Grip = FISTS.has(m) ? "fist" : RELAXED.has(m) ? "relaxed" : "open";
	if (hold === "carry" || hold === "throw") {
		return ["ball", g];
	}
	if (hold === "hands") {
		return ["ball", "ball"];
	}
	return [g, g];
};

// FROM ONE WAY OF STANDING INTO ANOTHER (the way the court has it - see
// court3d's settleStep): his feet stepped over, not slid. One foot at a
// time - the one already up first, or the one with furthest to go - each
// up off the turf as it goes and down again where the next one has it,
// the other where it was till its turn.
const STANDS = new Set<Move>([
	"stand",
	"armsFolded",
	"onHips",
	"hands",
	"pointSky",
	"qbSet",
	"shotgun",
	"settle",
	"overShoulders",
	"kickReady",
	"receiverStance",
	"cornerStance",
	"safetyStance",
	"backerStance",
]);
export const stepsOver = (from: Move, to: Move) =>
	from !== to && STANDS.has(from) && STANDS.has(to);
// A step's lift (degrees at the hip and knee); a full one for a leg this
// far from where it is going (degrees at the hip, half that again of its
// knee, and his feet's spread at so many a foot); each step's share of the
// way, overlapping; legs this near to as far to go swapping over which goes
// first gradually; one foot this much higher than the other up off the
// turf.
const SETTLE_HIP = 14;
const SETTLE_KNEE = 30;
const SETTLE_FULL = 30;
const SETTLE_SPREAD = 40;
const SETTLE_STEP = 0.6;
const SETTLE_TIE = 6;
const SETTLE_UP = 0.12;
const smooth01 = (u: number) => {
	const v = Math.min(1, Math.max(0, u));
	return v * v * (3 - 2 * v);
};
// `p` the two eased together the usual way, u of the way from `was` to
// `to`.
export const settleStep = (p: Pose, to: Pose, was: Pose, u: number): Pose => {
	const away = (a: Pose, b: Pose) => {
		const spread = Math.abs(a.wide - b.wide) * SETTLE_SPREAD;
		return [
			Math.abs(a.hipN - b.hipN) + 0.5 * Math.abs(a.kneeN - b.kneeN) + spread,
			Math.abs(a.hipF - b.hipF) + 0.5 * Math.abs(a.kneeF - b.kneeF) + spread,
		] as const;
	};
	const [dn, df] = away(was, to);
	const leg = (d: number, from: number) => {
		const v = Math.min(1, Math.max(0, (u - from) / SETTLE_STEP));
		return {
			on: smooth01(v),
			up: Math.sin(Math.PI * v) * Math.min(1, d / SETTLE_FULL),
		};
	};
	const wasFlex = flexOf(was);
	const toFlex = flexOf(to);
	const RAD = Math.PI / 180;
	const lift = (hip: number, knee: number) =>
		-Math.cos((hip + wasFlex) * RAD) - Math.cos((hip + wasFlex - knee) * RAD);
	const high = lift(was.hipN, was.kneeN) - lift(was.hipF, was.kneeF);
	const byFar = smooth01((dn - df) / SETTLE_TIE + 0.5);
	const lifted = Math.min(1, Math.abs(high) / SETTLE_UP);
	const first = byFar + ((high > 0 ? 1 : 0) - byFar) * lifted;
	const n = leg(dn, (1 - first) * (1 - SETTLE_STEP));
	const f = leg(df, first * (1 - SETTLE_STEP));
	const at = (a: number, b: number, w: number) => a + (b - a) * w;
	// (His hips' bend over his feet goes with each leg as it steps, not with
	// both at once - which would slide the one still down.)
	return {
		...p,
		plant: 0,
		hipN: at(was.hipN + wasFlex, to.hipN + toFlex, n.on) + SETTLE_HIP * n.up,
		kneeN: at(was.kneeN, to.kneeN, n.on) + SETTLE_KNEE * n.up,
		toeN: at(was.toeN, to.toeN, n.on),
		wideN: at(was.wide + was.wideN, to.wide + to.wideN, n.on) - p.wide,
		hipF: at(was.hipF + wasFlex, to.hipF + toFlex, f.on) + SETTLE_HIP * f.up,
		kneeF: at(was.kneeF, to.kneeF, f.on) + SETTLE_KNEE * f.up,
		toeF: at(was.toeF, to.toeF, f.on),
		wideF: at(was.wide + was.wideF, to.wide + to.wideF, f.on) - p.wide,
	};
};

// His pose with the ball: carried, his right arm folded up across him,
// high and tight, whatever the rest of him is doing.
export const holding = (q: Pose, hold: BallHold | undefined): Pose =>
	hold === "carry" ? { ...q, shN: 24, elN: 104, abN: 8, wrN: 4, flexN: 0 } : q;

// His arms out of one pose over the rest of him in another: reaching up for
// a ball he catches in stride.
const ARMS = [
	"shN",
	"elN",
	"abN",
	"wrN",
	"shF",
	"elF",
	"abF",
	"wrF",
	"flare",
	"tuck",
	"free",
	"flexN",
	"flexF",
] as const;
export const withArms = (q: Pose, arms: Pose): Pose => {
	const out = { ...q };
	for (const k of ARMS) {
		out[k] = arms[k];
	}
	return out;
};

const unitV = (v: V3): V3 => {
	const l = Math.hypot(v.f, v.s, v.u) || 1;
	return { f: v.f / l, s: v.s / l, u: v.u / l };
};

// Where the ball is in his frame, the way he has it: under the center's
// right hand on the turf, its points up and down the field, laces down;
// tucked along his right forearm, its nose up out in front of his hand; in
// both hands at his chest, nose up and out; cocked back in his throwing
// hand, nose to the target.
// (In both hands, how far out past their middle its own is, feet.)
const HANDS_ON = 0.3;
export const heldBall = (b: Body, q: Pose, hold: BallHold): HeldBall => {
	const sk = skeleton(b, q);
	const hand = sk.armR;
	const tip = hand.tip ?? hand.end;
	if (hold === "snap") {
		return {
			at: { f: (hand.end.f + tip.f) / 2, s: hand.end.s, u: BALL_R },
			axis: { f: 1, s: 0, u: 0 },
			laces: { f: 0, s: 0, u: -1 },
		};
	}
	if (hold === "carry") {
		const along = unitV({
			f: hand.end.f - hand.mid.f,
			s: hand.end.s - hand.mid.s,
			u: hand.end.u - hand.mid.u,
		});
		return {
			at: {
				f: hand.end.f - along.f * BALL_LEN * 0.18,
				s: hand.end.s - along.s * BALL_LEN * 0.18 + BALL_R * 0.4,
				u: hand.end.u - along.u * BALL_LEN * 0.18,
			},
			axis: along,
			laces: { f: 0, s: -1, u: 0 },
		};
	}
	if (hold === "hands") {
		// (His hands on the back half of it, its nose out past them.)
		const l = sk.armL.end;
		const axis = unitV({ f: 0.75, s: 0, u: 0.65 });
		return {
			at: {
				f: (hand.end.f + l.f) / 2 + BALL_R * 0.5 + axis.f * HANDS_ON,
				s: (hand.end.s + l.s) / 2,
				u: (hand.end.u + l.u) / 2 + BALL_R * 0.3 + axis.u * HANDS_ON,
			},
			axis,
			laces: { f: 0, s: 0, u: 1 },
		};
	}
	return {
		at: {
			f: (hand.end.f + tip.f) / 2,
			s: (hand.end.s + tip.s) / 2,
			u: (hand.end.u + tip.u) / 2,
		},
		axis: unitV({ f: 1, s: 0, u: 0.35 }),
		laces: { f: 0, s: 1, u: 0 },
	};
};
