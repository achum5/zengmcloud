import {
	ANIMS,
	lerpPose,
	mirror,
	poseAt,
	skeleton,
	type AnimName,
	type Body,
	type Pose,
} from "../court3d/poses.ts";
import type { Grip, HeldBall } from "./sculpt.ts";
import { BALL_R } from "./sculpt.ts";

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

const keyed = (keys: [number, Pose][], u: number): Pose => {
	for (let i = 1; i < keys.length; i++) {
		const [u1, p1] = keys[i]!;
		if (u <= u1) {
			const [u0, p0] = keys[i - 1]!;
			return lerpPose(p0, p1, u1 === u0 ? 1 : (u - u0) / (u1 - u0));
		}
	}
	return keys.at(-1)![1];
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
// right hand on the ball out in front of him, the left on his knee.
const CENTER = pose({
	hipN: 64,
	kneeN: 108,
	hipF: 64,
	kneeF: 108,
	lean: 66,
	shN: 22,
	elN: 4,
	abN: 4,
	shF: 40,
	elF: 50,
	abF: 12,
	wide: 1.25,
});

const FIELD_ANIMS = {
	// Standing easy: between plays, on the sideline.
	stand: { kind: "still", pose: BASE },
	// Bent over, hands on his knees: catching his breath, in the huddle.
	hands: {
		kind: "still",
		pose: pose({
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
		}),
	},
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
	underCenter: {
		kind: "still",
		pose: pose({
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
		}),
	},
	// In the shotgun: standing, a little crouched, hands up in front of him
	// for the snap.
	shotgun: {
		kind: "still",
		pose: pose({
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
		}),
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
	backerStance: {
		kind: "still",
		pose: pose({
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
		}),
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
	// The holder on a knee seven yards back: his right knee down, his left
	// foot up under him, his left hand out for the snap, the right down by
	// the spot.
	holdKneel: {
		kind: "still",
		pose: pose({
			hipN: -24,
			kneeN: 104,
			hipF: 82,
			kneeF: 88,
			lean: 14,
			shN: 20,
			elN: 10,
			abN: 10,
			shF: 58,
			elF: 22,
			abF: 6,
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
} satisfies Record<string, FieldAnim>;

export type FieldAnimName = keyof typeof FIELD_ANIMS;

// The court's moves a football player makes as they are: walking, jogging,
// running, sprinting, backpedaling, sliding.
const COURT_MOVES = ["walk", "jog", "run", "sprint", "back", "slide"] as const;
export type CourtMove = (typeof COURT_MOVES)[number];
export type Move = FieldAnimName | CourtMove;

const isCourtMove = (m: Move): m is CourtMove =>
	(COURT_MOVES as readonly string[]).includes(m);

// What kind of thing a move is (see court3d/poses.ts): a still one, a loop
// on the clock, a cycle on the distance covered, an act from start to
// finish.
export const moveKind = (m: Move): "still" | "loop" | "cycle" | "act" =>
	isCourtMove(m) ? ANIMS[m as AnimName].kind : FIELD_ANIMS[m].kind;

// How long a stride of a cycle is (feet).
export const strideOf = (m: Move): number => {
	if (isCourtMove(m)) {
		const a = ANIMS[m as AnimName];
		return a.kind === "cycle" ? a.stride : 5;
	}
	return 5;
};

// The pose partway through a move: an act from start (0) to finish (1), a
// loop or a cycle through one turn - mirrored, if he does it left-handed.
export const poseOfMove = (m: Move, phase: number, mirrored = false): Pose => {
	let q: Pose;
	if (isCourtMove(m)) {
		q = poseAt(m as AnimName, phase);
	} else {
		// (All of them still, so far.)
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

// How he holds his hands in a move: running, loose fists; down in a stance
// or reading the play, open; standing about, relaxed.
const FISTS = new Set<Move>(["run", "sprint"]);
const RELAXED = new Set<Move>(["stand", "walk", "jog", "hands"]);
export const gripsOf = (m: Move): [Grip, Grip] => {
	const g: Grip = FISTS.has(m) ? "fist" : RELAXED.has(m) ? "relaxed" : "open";
	return [g, g];
};

// The ball under the center's hand before the snap: on the turf under his
// right hand, its points up and down the field, laces down - in his frame.
export const snapBall = (b: Body, q: Pose): HeldBall => {
	const hand = skeleton(b, q).armR;
	const tip = hand.tip ?? hand.end;
	return {
		at: { f: (hand.end.f + tip.f) / 2, s: hand.end.s, u: BALL_R },
		axis: { f: 1, s: 0, u: 0 },
		laces: { f: 0, s: 0, u: -1 },
	};
};
