import {
	ANIMS,
	lerpPose,
	mirror,
	poseAt,
	skeleton,
	type AnimName,
	type Body,
	type Pose,
	type V3,
} from "../court3d/poses.ts";
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

// The quarterback set in the pocket: on the balls of his feet, the ball up
// at his chest in both hands.
const QB_SET = pose({
	hipN: 12,
	kneeN: 22,
	hipF: 16,
	kneeF: 22,
	lean: 10,
	shN: 30,
	elN: 92,
	abN: 14,
	shF: 30,
	elF: 92,
	abF: 14,
	wide: 0.8,
});
// Cocked: the ball back up by his ear, his shoulders turned to the
// target, the off arm out at it, his weight back.
const THROW_COCKED = pose({
	hipN: 4,
	kneeN: 22,
	hipF: 26,
	kneeF: 14,
	lean: 2,
	shN: 104,
	elN: 70,
	abN: 72,
	wrN: 20,
	shF: 86,
	elF: 16,
	abF: 8,
	twist: -34,
	wide: 1.1,
});
// Let go: the arm over the top and out in front, his weight onto his front
// foot.
const THROW_RELEASE = pose({
	hipN: -12,
	kneeN: 26,
	hipF: 30,
	kneeF: 16,
	lean: 16,
	shN: 150,
	elN: 22,
	abN: 40,
	wrN: -10,
	shF: 40,
	elF: 50,
	abF: 14,
	twist: 8,
	wide: 1.1,
});
// Followed through: the arm down across him, the back foot coming
// through.
const THROW_THROUGH = pose({
	hipN: -26,
	kneeN: 44,
	hipF: 34,
	kneeF: 30,
	lean: 30,
	shN: 46,
	elN: 30,
	abN: -18,
	wrN: -20,
	shF: 14,
	elF: 50,
	abF: 16,
	twist: 30,
	wide: 1.0,
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

	prone: { kind: "still", pose: PRONE },
	qbSet: { kind: "still", pose: QB_SET },
	// A receiver sat down in a hole in the zone, turned to the quarterback,
	// hands up for it.
	settle: {
		kind: "still",
		pose: pose({
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
		}),
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
			[0.55, THROW_COCKED],
			[1, THROW_RELEASE],
		],
	},
	throwFollow: {
		kind: "act",
		keys: [
			[0, THROW_RELEASE],
			[1, THROW_THROUGH],
		],
	},
	// Handing it off: turned, the ball out in both hands into the back's
	// belly.
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
					lean: 18,
					shN: 62,
					elN: 30,
					abN: 34,
					shF: 54,
					elF: 40,
					abF: 0,
					twist: 24,
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
	// The catch: hands up together at the ball, and in to his chest.
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
					shN: 40,
					elN: 70,
					shF: 40,
					elF: 70,
					abN: 12,
					abF: 12,
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
					shN: 118,
					elN: 14,
					abN: -6,
					shF: 118,
					elF: 14,
					abF: -6,
					wrN: 20,
					wrF: 20,
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
	// The tackle: off his feet into him, arms round him, taking him down.
	dive: {
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
				0.7,
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
		],
	},
	// Up off the turf: a foot under him and his hands down, onto a knee,
	// then his feet.
	getUp: {
		kind: "act",
		keys: [
			[0, PRONE],
			[
				0.3,
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
				0.65,
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
		],
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
	const a = FIELD_ANIMS[m] as FieldAnim;
	return a.kind === "cycle" ? a.stride : 5;
};

// The pose partway through a move: an act from start (0) to finish (1), a
// loop or a cycle through one turn - mirrored, if he does it left-handed.
export const poseOfMove = (m: Move, phase: number, mirrored = false): Pose => {
	let q: Pose;
	if (isCourtMove(m)) {
		q = poseAt(m as AnimName, phase);
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

// How he holds his hands in a move: running, loose fists; down in a stance
// or reading the play, open; standing about, relaxed.
const FISTS = new Set<Move>(["run", "sprint"]);
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

// His pose with the ball: carried, his right arm folded up across him,
// high and tight, whatever the rest of him is doing.
export const holding = (q: Pose, hold: BallHold | undefined): Pose =>
	hold === "carry" ? { ...q, shN: 24, elN: 104, abN: 8, wrN: 4 } : q;

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
		const l = sk.armL.end;
		return {
			at: {
				f: (hand.end.f + l.f) / 2 + BALL_R * 0.5,
				s: (hand.end.s + l.s) / 2,
				u: (hand.end.u + l.u) / 2 + BALL_R * 0.3,
			},
			axis: unitV({ f: 0.75, s: 0, u: 0.65 }),
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
