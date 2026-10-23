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
const CLAP = { abN: -6, abF: -6, shF: 60, elF: 56 };
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
	// And the ball down: leaning over it, a finger of his left hand on top
	// of it, the right out of the kicker's way.
	holdDown: {
		kind: "still",
		pose: pose({
			hipN: -24,
			kneeN: 104,
			hipF: 82,
			kneeF: 88,
			lean: 34,
			shN: 4,
			elN: 40,
			abN: 24,
			shF: 40,
			elF: 4,
			abF: -14,
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
				0.55,
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
					hipN: 84,
					kneeN: 96,
					hipF: 20,
					kneeF: 70,
					lean: 56,
					shN: 66,
					elN: 8,
					abN: 10,
					shF: 66,
					elF: 8,
					abF: 10,
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
			[0, { ...CENTER, shF: 24, elF: 4, abF: 4 }],
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
	sigSafety: held({ shN: 176, elN: 24, abN: -12, shF: 176, elF: 24, abF: -12 }),
	// Incomplete, no good, declined: the arms swung across in front of him.
	sigWave: over(
		{ shN: 90, elN: 4, abN: 84, shF: 90, elF: 4, abF: 84 },
		{ shN: 88, elN: 10, abN: -22, shF: 92, elF: 10, abF: -30 },
	),
	// Time out, the clock stopped: both arms up, crossed and uncrossed over
	// his head.
	sigTime: over(
		{ shN: 166, elN: 6, abN: 26, shF: 166, elF: 6, abF: 26 },
		{ shN: 166, elN: 6, abN: -16, shF: 166, elF: 6, abF: -16 },
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
	sigHead: held({ shN: 160, elN: 128, abN: 34, shF: 160, elF: 128, abF: 34 }),
	// Illegal contact, illegal use of the hands: an open hand out in front.
	sigContact: held({ shN: 92, elN: 6, abN: 0, wrN: -40 }),
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

// How far off the turf he is partway through a move (feet): up off both
// feet in a leap.
export const liftOf = (m: Move, phase: number): number =>
	m === "leap" && phase > 0.3 && phase < 0.82
		? 1.7 * Math.sin((Math.PI * (phase - 0.3)) / 0.52)
		: m === "leapCatch" && phase > 0.2 && phase < 0.8
			? 2.4 * Math.sin((Math.PI * (phase - 0.2)) / 0.6)
			: m === "diveCatch" && phase > 0.25 && phase < 0.75
				? 0.8 * Math.sin((Math.PI * (phase - 0.25)) / 0.5)
				: 0;

// How he holds his hands in a move: running, loose fists; down in a stance
// or reading the play, open; standing about, relaxed.
const FISTS = new Set<Move>(["run", "sprint", "holdPole"]);
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
