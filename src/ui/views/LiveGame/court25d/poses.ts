// THE BODY AND HOW IT MOVES.
//
// A player is a skeleton - hips, knees, shoulders, elbows - posed by a
// handful of joint angles, then turned to face wherever he faces on the floor.
// Angles are in degrees in the plane he faces along: 0 points straight down,
// positive swings forward. A knee bends backward from its thigh, an elbow
// forward from its upper arm. N is his right side (his shooting and dribbling
// hand), F his left.
//
// Everything here is plain arithmetic, so the director and the tests can ask
// where a hand is without a canvas.

export type Pose = {
	hipN: number;
	kneeN: number;
	hipF: number;
	kneeF: number;
	shN: number;
	elN: number;
	shF: number;
	elF: number;
	lean: number;
	// How far each arm swings out from his side (degrees), and how far apart
	// his feet are spread (feet) - a defensive stance is wide and arms out.
	abN: number;
	abF: number;
	wide: number;
	// The hand's bend at the wrist, the same way the elbow bends: positive
	// tips the fingers back over the top (a hand cocked under the ball),
	// negative folds them forward and down (a shooter's follow-through).
	wrN: number;
	wrF: number;
	// Holding the ball in both hands, how far his elbows swing out wide
	// (degrees) - a rebounder chinning it, keeping it away from hands.
	flare: number;
	// How much his hips flex to keep his feet under him as his knees bend
	// (0 to 1): all the way standing or crouched, none mid-stride.
	plant: number;
};

const BASE: Pose = {
	hipN: -6,
	kneeN: 14,
	hipF: 8,
	kneeF: 16,
	shN: 18,
	elN: 38,
	shF: 26,
	elF: 44,
	lean: 6,
	abN: 14,
	abF: 14,
	wide: 0.08,
	wrN: 0,
	wrF: 0,
	flare: 0,
	plant: 1,
};
const pose = (o: Partial<Pose>): Pose => ({ ...BASE, ...o });

export const lerpPose = (a: Pose, b: Pose, f: number): Pose => {
	const out = { ...a };
	for (const key of Object.keys(a) as (keyof Pose)[]) {
		out[key] = a[key] + (b[key] - a[key]) * f;
	}
	return out;
};

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

const P = {
	// Standing easy: arms hanging loose, a little bend at the elbows.
	ready: pose({ shN: 8, elN: 24, shF: 12, elF: 28, abN: 12, abF: 12 }),
	stance: pose({
		hipN: 30,
		kneeN: 62,
		hipF: 36,
		kneeF: 66,
		shN: 52,
		elN: 48,
		shF: 46,
		elF: 52,
		lean: 18,
		abN: 44,
		abF: 44,
		wide: 0.85,
	}),
	hold: pose({
		hipN: -12,
		kneeN: 30,
		hipF: 14,
		kneeF: 32,
		shN: 36,
		elN: 92,
		shF: 30,
		elF: 98,
		lean: 10,
	}),
	catch: pose({
		hipN: -8,
		kneeN: 20,
		hipF: 10,
		kneeF: 22,
		shN: 80,
		elN: 26,
		shF: 74,
		elF: 30,
		lean: 6,
	}),
	// Stepping into it: the left foot out toward him, the right behind.
	passOut: pose({
		hipN: -24,
		kneeN: 16,
		hipF: 38,
		kneeF: 26,
		shN: 92,
		elN: 2,
		shF: 86,
		elF: 6,
		lean: 16,
		wide: 0.16,
	}),
	// Winding up a pass: the ball pulled into his chest, a step coming.
	passWind: pose({
		hipN: -8,
		kneeN: 34,
		hipF: 12,
		kneeF: 34,
		shN: 30,
		elN: 112,
		shF: 26,
		elF: 116,
		lean: 8,
	}),
	// A bounce pass let go: arms driven down and out, low over a bent knee.
	passBounceOut: pose({
		hipN: -26,
		kneeN: 30,
		hipF: 42,
		kneeF: 46,
		shN: 50,
		elN: 0,
		shF: 46,
		elF: 4,
		lean: 24,
	}),
	// An overhead pass: the ball up over his head, then whipped forward.
	overheadUp: pose({
		hipN: -8,
		kneeN: 18,
		hipF: 10,
		kneeF: 20,
		shN: 168,
		elN: 46,
		shF: 162,
		elF: 50,
		lean: -4,
	}),
	overheadOut: pose({
		hipN: -22,
		kneeN: 16,
		hipF: 34,
		kneeF: 24,
		shN: 118,
		elN: 4,
		shF: 112,
		elF: 8,
		lean: 12,
	}),
	gather: pose({
		hipN: -14,
		kneeN: 52,
		hipF: 14,
		kneeF: 52,
		shN: 40,
		elN: 95,
		shF: 34,
		elF: 100,
		lean: 10,
	}),
	land: pose({
		hipN: -6,
		kneeN: 40,
		hipF: 14,
		kneeF: 44,
		shN: 34,
		elN: 30,
		shF: 22,
		elF: 36,
		lean: 8,
	}),
	// A rebound pulled down and chinned: feet wide, knees bent, the ball
	// tight under his chin and his elbows out.
	chin: pose({
		hipN: -10,
		kneeN: 42,
		hipF: 12,
		kneeF: 44,
		shN: 0,
		elN: 160,
		shF: 0,
		elF: 160,
		lean: 10,
		wide: 0.55,
		flare: 62,
	}),
	// Dribbling where he stands, low.
	dribbling: pose({
		hipN: -14,
		kneeN: 34,
		hipF: 16,
		kneeF: 36,
		lean: 12,
		shN: 32,
		elN: 18,
		shF: 52,
		elF: 62,
	}),
	// Triple threat: caught and facing up, knees bent, the ball on his hip -
	// ready to shoot it, drive it or move it.
	triple: pose({
		hipN: -10,
		kneeN: 46,
		hipF: 24,
		kneeF: 48,
		lean: 16,
		shN: 16,
		elN: 92,
		shF: 10,
		elF: 100,
		abN: 20,
		abF: 8,
		wide: 0.42,
	}),
	// A jab step: his lead foot stabbed out at his man, the ball ripped
	// through low to his hip.
	jabOut: pose({
		hipN: -4,
		kneeN: 44,
		hipF: 50,
		kneeF: 34,
		lean: 22,
		shN: 8,
		elN: 96,
		shF: 4,
		elF: 104,
		abN: 26,
		abF: 6,
		wide: 0.36,
	}),
	// A shot fake: the ball up to his eyes as if to shoot, heels down.
	fakeUp: pose({
		hipN: -8,
		kneeN: 30,
		hipF: 14,
		kneeF: 32,
		lean: 4,
		shN: 128,
		elN: 118,
		shF: 120,
		elF: 124,
		wrN: 30,
		wrF: 20,
	}),
	// Clapping for the ball: hands apart, and together.
	clapOpen: pose({
		shN: 70,
		elN: 56,
		abN: 18,
		shF: 70,
		elF: 56,
		abF: 18,
		lean: 2,
	}),
	clapShut: pose({
		shN: 72,
		elN: 52,
		abN: -10,
		shF: 72,
		elF: 52,
		abF: -10,
		lean: 2,
	}),
};

// Hands up as a target for a pass on its way: out in front of his chest,
// fingers up, palms to the ball.
const TARGET: Partial<Pose> = {
	shN: 50,
	elN: 70,
	shF: 46,
	elF: 74,
	abN: 18,
	abF: 18,
	wrN: 50,
	wrF: 50,
};

type RunMode =
	| "run"
	| "sprint"
	| "dribble"
	| "back"
	| "walk"
	| "carry"
	| "drift"
	| "shuffle"
	| "closeout";
const stride = (ph: number, mode: RunMode): Pose => {
	const a = Math.sin(2 * Math.PI * ph);
	const c = Math.cos(2 * Math.PI * ph);
	if (mode === "shuffle") {
		// A defensive slide sideways: down in his stance, the lead foot stepping
		// out and the other closing up behind it - never crossing - with his
		// arms out wide.
		return pose({
			hipN: 30 + 4 * a,
			hipF: 34 - 4 * a,
			kneeN: 64 + 6 * c,
			kneeF: 66 + 6 * c,
			lean: 20,
			shN: 52,
			elN: 58,
			shF: 48,
			elF: 62,
			abN: 50,
			abF: 50,
			wide: 0.78 + 0.32 * c,
		});
	}
	if (mode === "closeout") {
		// Closing out on a shooter: short, choppy steps to break down, one hand
		// high at the shot and the other out at the drive.
		return pose({
			hipN: 18 + 12 * a,
			hipF: 26 - 12 * a,
			kneeN: 46 + 14 * Math.max(0, c),
			kneeF: 48 + 14 * Math.max(0, -c),
			lean: 12,
			shN: 156,
			elN: 16,
			abN: 18,
			shF: 50,
			elF: 34,
			abF: 50,
			wide: 0.55,
		});
	}
	if (mode === "walk" || mode === "carry" || mode === "drift") {
		const legs = {
			hipN: 20 * a,
			hipF: -20 * a,
			kneeN: 10 + 30 * Math.max(0, c),
			kneeF: 10 + 30 * Math.max(0, -c),
		};
		if (mode === "drift") {
			// Off the ball, sliding along the arc: knees bent, hands up ready
			// for it.
			return pose({
				...legs,
				kneeN: legs.kneeN + 14,
				kneeF: legs.kneeF + 14,
				lean: 9,
				shN: 42,
				elN: 58,
				shF: 40,
				elF: 60,
				abN: 16,
				abF: 16,
				wrN: 20,
				wrF: 20,
			});
		}
		return mode === "carry"
			? pose({ ...legs, lean: 6, shN: 36, elN: 92, shF: 30, elF: 98 })
			: pose({ ...legs, lean: 4, shN: -16 * a, elN: 22, shF: 16 * a, elF: 26 });
	}
	if (mode === "back") {
		// A defensive slide / backpedal: low, short steps, hands active.
		return pose({
			hipN: 10 - 18 * a,
			hipF: 30 + 18 * a,
			kneeN: 50 + 16 * Math.max(0, -c),
			kneeF: 54 + 16 * Math.max(0, c),
			lean: 18,
			shN: 64,
			elN: 22,
			shF: 46,
			elF: 30,
			abN: 34,
			abF: 34,
			wide: 0.4,
		});
	}
	if (mode === "sprint") {
		// Flat out: leaning into it, knees driving high, arms pumping.
		return pose({
			hipN: 42 * a,
			hipF: -42 * a,
			kneeN: 18 + 80 * Math.max(0, c) ** 1.3,
			kneeF: 18 + 80 * Math.max(0, -c) ** 1.3,
			lean: 18,
			shN: -62 * a,
			elN: 88,
			shF: 62 * a,
			elF: 88,
		});
	}
	const legs = {
		hipN: 30 * a,
		hipF: -30 * a,
		kneeN: 14 + 56 * Math.max(0, c) ** 1.3,
		kneeF: 14 + 56 * Math.max(0, -c) ** 1.3,
		lean: 10,
	};
	if (mode === "dribble") {
		return pose({
			...legs,
			shN: 34,
			elN: 22 + 18 * Math.abs(a),
			shF: 52,
			elF: 62,
		});
	}
	return pose({ ...legs, shN: -42 * a, elN: 78, shF: 42 * a, elF: 78 });
};

// Mid-stride, his feet are where his stride puts them - not planted under
// him.
const runPose = (ph: number, mode: RunMode): Pose => ({
	...stride(ph, mode),
	plant: 0,
});

// "loop" anims play on the clock, "cycle" ones on distance covered (so feet
// never skate), "act" ones across their own span from 0 to 1.
type Anim =
	| { kind: "loop"; n: number; fps: number; pose: (i: number) => Pose }
	| { kind: "cycle"; n: number; stride: number; pose: (i: number) => Pose }
	| { kind: "act"; n: number; keys: [number, Pose][] };

export const ANIMS = {
	ready: {
		kind: "loop",
		n: 2,
		fps: 1.5,
		pose: (i) => (i ? { ...P.ready, kneeN: 20, kneeF: 22 } : P.ready),
	},
	stance: {
		kind: "loop",
		n: 2,
		fps: 2.5,
		pose: (i) =>
			i ? { ...P.stance, hipN: 26, hipF: 28, shN: 72, shF: 40 } : P.stance,
	},
	// Up on the man with the ball: down low, feet wide, one hand up in his
	// face and the other down at the ball - trading them as he goes.
	guard: {
		kind: "loop",
		n: 2,
		fps: 1.4,
		pose: (i) =>
			pose({
				hipN: 34,
				kneeN: 72,
				hipF: 40,
				kneeF: 74,
				lean: 20,
				wide: 0.95,
				...(i
					? { shN: 48, elN: 30, abN: 46, shF: 150, elF: 22, abF: 20 }
					: { shN: 150, elN: 22, abN: 20, shF: 48, elF: 30, abF: 46 }),
			}),
	},
	hold: { kind: "loop", n: 1, fps: 1, pose: () => P.hold },
	// Boxing out: his back into the man behind him, sat down low and wide,
	// arms up and out to keep him there, eyes on the ball.
	boxOut: {
		kind: "loop",
		n: 2,
		fps: 2.2,
		pose: (i) =>
			pose({
				hipN: 40,
				kneeN: 76 + i * 6,
				hipF: 44,
				kneeF: 78 + i * 6,
				lean: 22,
				shN: 66 + i * 6,
				elN: 104,
				abN: 56,
				shF: 66 - i * 6,
				elF: 104,
				abF: 56,
				wide: 1.05,
			}),
	},
	// Fighting a box-out: leaning into the man in front, one arm up over him
	// for the ball, the other hand on his back.
	fight: {
		kind: "loop",
		n: 2,
		fps: 2.4,
		pose: (i) =>
			pose({
				hipN: 18,
				kneeN: 44,
				hipF: 40,
				kneeF: 52,
				lean: 24 + i * 4,
				shN: 150 + i * 8,
				elN: 22,
				abN: 18,
				shF: 70,
				elF: 56,
				abF: 24,
				wide: 0.5,
			}),
	},
	// Sat in his triple threat, the ball on his hip, rocking a little.
	triple: {
		kind: "loop",
		n: 2,
		fps: 1.6,
		pose: (i) =>
			i ? { ...P.triple, kneeN: 50, kneeF: 52, lean: 18 } : P.triple,
	},
	// A jab step at his man and back.
	jab: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.triple],
			[0.32, P.jabOut],
			[0.6, P.jabOut],
			[1, P.triple],
		],
	},
	// Up as if to shoot, and back down.
	shotFake: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.triple],
			[0.34, P.fakeUp],
			[0.56, P.fakeUp],
			[1, P.triple],
		],
	},
	dribbleIdle: {
		kind: "loop",
		n: 4,
		fps: 7,
		pose: (i) =>
			pose({
				hipN: -14,
				kneeN: 34,
				hipF: 16,
				kneeF: 36,
				lean: 12,
				shN: 32,
				elN: [34, 18, 8, 18][i]!,
				shF: 52,
				elF: 62,
			}),
	},
	hurt: {
		kind: "loop",
		n: 2,
		fps: 1.2,
		pose: (i) =>
			pose({
				hipN: 46,
				kneeN: 104,
				hipF: 14,
				kneeF: 40,
				lean: 38 + i * 4,
				shN: 40,
				elN: 30,
				shF: 26,
				elF: 40,
			}),
	},
	run: { kind: "cycle", n: 6, stride: 8.6, pose: (i) => runPose(i / 6, "run") },
	sprint: {
		kind: "cycle",
		n: 6,
		stride: 12.4,
		pose: (i) => runPose(i / 6, "sprint"),
	},
	dribble: {
		kind: "cycle",
		n: 6,
		stride: 8.2,
		pose: (i) => runPose(i / 6, "dribble"),
	},
	// Backing his man down in the post: low and wide, his back to the rim,
	// one arm out to keep him there.
	post: {
		kind: "cycle",
		n: 6,
		stride: 2.6,
		pose: (i) => {
			const a = Math.sin((2 * Math.PI * i) / 6);
			return pose({
				hipN: 18 + 10 * a,
				kneeN: 56 + 8 * a,
				hipF: 26 - 10 * a,
				kneeF: 60 - 8 * a,
				shN: 34,
				elN: 26 + 14 * Math.abs(a),
				shF: 70,
				elF: 50,
				abF: 40,
				lean: 24,
				wide: 0.6,
			});
		},
	},
	back: {
		kind: "cycle",
		n: 6,
		stride: 4.6,
		pose: (i) => runPose(i / 6, "back"),
	},
	// A defender sliding with his man: the same low steps, eyes on the ball
	// whichever way he goes.
	slide: {
		kind: "cycle",
		n: 6,
		stride: 4.6,
		pose: (i) => runPose(i / 6, "back"),
	},
	// Setting a screen: planted wide and low, arms folded in front to take
	// the hit.
	screen: {
		kind: "loop",
		n: 2,
		fps: 1.2,
		pose: (i) =>
			pose({
				hipN: -8,
				kneeN: 30 + i * 4,
				hipF: 12,
				kneeF: 32 + i * 4,
				lean: 6,
				shN: 24,
				elN: 74,
				shF: 24,
				elF: 74,
				abN: -26,
				abF: -26,
				wide: 0.75,
			}),
	},
	// Sealed in the post: low and wide, one arm holding his man off, the
	// other up asking for the ball.
	postUp: {
		kind: "loop",
		n: 2,
		fps: 1.4,
		pose: (i) =>
			pose({
				hipN: 20,
				kneeN: 52,
				hipF: 24,
				kneeF: 54,
				lean: 14,
				shN: 138 + i * 8,
				elN: 26,
				shF: 66,
				elF: 36,
				abF: 46,
				wide: 0.75,
			}),
	},
	walk: {
		kind: "cycle",
		n: 6,
		stride: 4.8,
		pose: (i) => runPose(i / 6, "walk"),
	},
	carry: {
		kind: "cycle",
		n: 6,
		stride: 4.6,
		pose: (i) => runPose(i / 6, "carry"),
	},
	// Drifting along the arc off the ball, eyes on it.
	drift: {
		kind: "cycle",
		n: 6,
		stride: 3.8,
		pose: (i) => runPose(i / 6, "drift"),
	},
	// A defender sliding sideways with his man, square to him.
	shuffle: {
		kind: "cycle",
		n: 6,
		stride: 3.4,
		pose: (i) => runPose(i / 6, "shuffle"),
	},
	// Running at a shooter and breaking down in front of him, a hand up.
	closeout: {
		kind: "cycle",
		n: 6,
		stride: 2.6,
		pose: (i) => runPose(i / 6, "closeout"),
	},
	shoot: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.gather],
			[
				0.28,
				pose({
					hipN: -6,
					kneeN: 18,
					hipF: 6,
					kneeF: 22,
					shN: 140,
					elN: 75,
					shF: 130,
					elF: 80,
					lean: 2,
					wrN: 50,
				}),
			],
			[
				0.55,
				pose({
					hipN: -4,
					kneeN: 8,
					hipF: 8,
					kneeF: 30,
					shN: 170,
					elN: 4,
					shF: 150,
					elF: 40,
					lean: -2,
					wrN: 15,
				}),
			],
			[
				0.63,
				pose({
					hipN: -4,
					kneeN: 10,
					hipF: 8,
					kneeF: 32,
					shN: 162,
					elN: 14,
					shF: 118,
					elF: 36,
					lean: -2,
					wrN: -100,
				}),
			],
			[
				0.8,
				pose({
					hipN: -4,
					kneeN: 10,
					hipF: 8,
					kneeF: 32,
					shN: 162,
					elN: 14,
					shF: 118,
					elF: 36,
					lean: -2,
					wrN: -110,
				}),
			],
			[1, P.land],
		],
	},
	layup: {
		kind: "act",
		n: 7,
		keys: [
			[
				0,
				pose({
					hipN: 10,
					kneeN: 40,
					hipF: -20,
					kneeF: 30,
					shN: 45,
					elN: 90,
					shF: 40,
					elF: 95,
					lean: 14,
				}),
			],
			[
				0.3,
				pose({
					hipN: 94,
					kneeN: 102,
					hipF: -16,
					kneeF: 24,
					shN: 120,
					elN: 50,
					shF: 60,
					elF: 70,
					lean: 6,
					wrN: 30,
				}),
			],
			[
				0.6,
				pose({
					hipN: 88,
					kneeN: 106,
					hipF: -10,
					kneeF: 26,
					shN: 168,
					elN: 6,
					shF: 70,
					elF: 60,
					lean: 0,
					wrN: 10,
				}),
			],
			[
				0.72,
				pose({
					hipN: 88,
					kneeN: 106,
					hipF: -10,
					kneeF: 26,
					shN: 168,
					elN: 6,
					shF: 70,
					elF: 60,
					lean: 0,
					wrN: -70,
				}),
			],
			[1, P.land],
		],
	},
	// Two hands: up with it over his head, cocked behind it at the top, then
	// thrown down over the front of the rim - and hanging on it.
	dunk: {
		kind: "act",
		n: 12,
		keys: [
			[
				0,
				pose({
					hipN: -20,
					kneeN: 70,
					hipF: 18,
					kneeF: 72,
					shN: 40,
					elN: 95,
					shF: 36,
					elF: 98,
					lean: 18,
				}),
			],
			[
				0.22,
				pose({
					hipN: -10,
					kneeN: 30,
					hipF: 30,
					kneeF: 50,
					shN: 120,
					elN: 60,
					shF: 115,
					elF: 62,
					lean: 8,
				}),
			],
			[
				0.34,
				pose({
					hipN: 12,
					kneeN: 34,
					hipF: 46,
					kneeF: 84,
					shN: 170,
					elN: 30,
					shF: 166,
					elF: 32,
					lean: 2,
				}),
			],
			[
				0.42,
				pose({
					hipN: 6,
					kneeN: 40,
					hipF: 30,
					kneeF: 70,
					shN: 184,
					elN: 30,
					shF: 180,
					elF: 32,
					lean: -4,
				}),
			],
			[
				0.47,
				pose({
					hipN: 10,
					kneeN: 30,
					hipF: 20,
					kneeF: 50,
					shN: 150,
					elN: 6,
					shF: 146,
					elF: 8,
					wrN: -60,
					wrF: -60,
					lean: 12,
				}),
			],
			[
				0.53,
				pose({
					hipN: 4,
					kneeN: 24,
					hipF: 10,
					kneeF: 36,
					shN: 172,
					elN: 6,
					shF: 170,
					elF: 8,
					lean: 6,
				}),
			],
			[
				0.7,
				pose({
					hipN: -2,
					kneeN: 20,
					hipF: 8,
					kneeF: 30,
					shN: 174,
					elN: 4,
					shF: 172,
					elF: 6,
					lean: 2,
				}),
			],
			[
				0.78,
				pose({
					hipN: 0,
					kneeN: 28,
					hipF: 12,
					kneeF: 34,
					shN: 120,
					elN: 30,
					shF: 116,
					elF: 34,
					lean: 4,
				}),
			],
			[0.88, P.land],
			[1, P.land],
		],
	},
	// One hand, the other out for balance.
	dunk1: {
		kind: "act",
		n: 12,
		keys: [
			[
				0,
				pose({
					hipN: -20,
					kneeN: 70,
					hipF: 18,
					kneeF: 72,
					shN: 40,
					elN: 95,
					shF: 36,
					elF: 98,
					lean: 18,
				}),
			],
			[
				0.22,
				pose({
					hipN: -10,
					kneeN: 30,
					hipF: 30,
					kneeF: 50,
					shN: 120,
					elN: 60,
					shF: 70,
					elF: 60,
					abF: 20,
					lean: 8,
				}),
			],
			[
				0.34,
				pose({
					hipN: 12,
					kneeN: 34,
					hipF: 46,
					kneeF: 84,
					shN: 170,
					elN: 30,
					shF: 80,
					elF: 50,
					abF: 30,
					lean: 2,
				}),
			],
			[
				0.42,
				pose({
					hipN: 6,
					kneeN: 40,
					hipF: 30,
					kneeF: 70,
					shN: 186,
					elN: 34,
					shF: 70,
					elF: 40,
					abF: 34,
					lean: -4,
				}),
			],
			[
				0.47,
				pose({
					hipN: 10,
					kneeN: 30,
					hipF: 20,
					kneeF: 50,
					shN: 148,
					elN: 4,
					shF: 60,
					elF: 30,
					abF: 30,
					wrN: -70,
					lean: 12,
				}),
			],
			[
				0.53,
				pose({
					hipN: 4,
					kneeN: 24,
					hipF: 10,
					kneeF: 36,
					shN: 172,
					elN: 6,
					shF: 170,
					elF: 8,
					lean: 6,
				}),
			],
			[
				0.7,
				pose({
					hipN: -2,
					kneeN: 20,
					hipF: 8,
					kneeF: 30,
					shN: 174,
					elN: 4,
					shF: 172,
					elF: 6,
					lean: 2,
				}),
			],
			[
				0.78,
				pose({
					hipN: 0,
					kneeN: 28,
					hipF: 12,
					kneeF: 34,
					shN: 120,
					elN: 30,
					shF: 116,
					elF: 34,
					lean: 4,
				}),
			],
			[0.88, P.land],
			[1, P.land],
		],
	},
	// One hand, cocked way back behind his head and whipped over.
	tomahawk: {
		kind: "act",
		n: 12,
		keys: [
			[
				0,
				pose({
					hipN: -20,
					kneeN: 70,
					hipF: 18,
					kneeF: 72,
					shN: 40,
					elN: 95,
					shF: 36,
					elF: 98,
					lean: 18,
				}),
			],
			[
				0.22,
				pose({
					hipN: -10,
					kneeN: 30,
					hipF: 30,
					kneeF: 50,
					shN: 120,
					elN: 60,
					shF: 90,
					elF: 50,
					lean: 8,
				}),
			],
			[
				0.34,
				pose({
					hipN: 12,
					kneeN: 34,
					hipF: 46,
					kneeF: 84,
					shN: 170,
					elN: 70,
					shF: 90,
					elF: 50,
					lean: 2,
				}),
			],
			[
				0.4,
				pose({
					hipN: 6,
					kneeN: 40,
					hipF: 30,
					kneeF: 70,
					shN: 205,
					elN: 70,
					shF: 70,
					elF: 40,
					lean: -4,
				}),
			],
			[
				0.47,
				pose({
					hipN: 10,
					kneeN: 30,
					hipF: 20,
					kneeF: 50,
					shN: 146,
					elN: 4,
					shF: 60,
					elF: 30,
					wrN: -80,
					lean: 12,
				}),
			],
			[
				0.53,
				pose({
					hipN: 4,
					kneeN: 24,
					hipF: 10,
					kneeF: 36,
					shN: 172,
					elN: 6,
					shF: 170,
					elF: 8,
					lean: 6,
				}),
			],
			[
				0.7,
				pose({
					hipN: -2,
					kneeN: 20,
					hipF: 8,
					kneeF: 30,
					shN: 174,
					elN: 4,
					shF: 172,
					elF: 6,
					lean: 2,
				}),
			],
			[
				0.78,
				pose({
					hipN: 0,
					kneeN: 28,
					hipF: 12,
					kneeF: 34,
					shN: 120,
					elN: 30,
					shF: 116,
					elF: 34,
					lean: 4,
				}),
			],
			[0.88, P.land],
			[1, P.land],
		],
	},
	// A hook from the post: side-on, the shooting arm sweeping up over his
	// head, the other arm up to keep the defender off.
	hook: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.gather],
			[
				0.3,
				pose({
					hipN: 34,
					kneeN: 70,
					hipF: -4,
					kneeF: 12,
					shN: 105,
					elN: 20,
					abN: 70,
					shF: 95,
					elF: 70,
					abF: 22,
					lean: 6,
				}),
			],
			[
				0.55,
				pose({
					hipN: 42,
					kneeN: 82,
					hipF: -2,
					kneeF: 14,
					shN: 172,
					elN: 8,
					abN: 26,
					shF: 92,
					elF: 74,
					abF: 22,
					lean: -2,
				}),
			],
			[
				0.8,
				pose({
					hipN: 20,
					kneeN: 40,
					hipF: 0,
					kneeF: 18,
					shN: 158,
					elN: 46,
					abN: 20,
					shF: 70,
					elF: 50,
					lean: 0,
				}),
			],
			[1, P.land],
		],
	},
	// A turnaround fadeaway: up and drifting back, legs out in front.
	fade: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.gather],
			[
				0.28,
				pose({
					hipN: 6,
					kneeN: 16,
					hipF: 14,
					kneeF: 26,
					shN: 140,
					elN: 75,
					shF: 130,
					elF: 80,
					lean: -6,
					wrN: 50,
				}),
			],
			[
				0.55,
				pose({
					hipN: 26,
					kneeN: 22,
					hipF: 38,
					kneeF: 44,
					shN: 168,
					elN: 6,
					shF: 150,
					elF: 40,
					lean: -16,
					wrN: 15,
				}),
			],
			[
				0.63,
				pose({
					hipN: 22,
					kneeN: 26,
					hipF: 34,
					kneeF: 46,
					shN: 160,
					elN: 16,
					shF: 118,
					elF: 36,
					lean: -12,
					wrN: -100,
				}),
			],
			[
				0.8,
				pose({
					hipN: 22,
					kneeN: 26,
					hipF: 34,
					kneeF: 46,
					shN: 160,
					elN: 16,
					shF: 118,
					elF: 36,
					lean: -12,
					wrN: -110,
				}),
			],
			[1, P.land],
		],
	},
	// Arms up and bent: the flex after a big finish.
	flex: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.land],
			[
				0.3,
				pose({
					hipN: -10,
					kneeN: 26,
					hipF: 14,
					kneeF: 30,
					shN: 92,
					elN: 115,
					abN: 82,
					shF: 92,
					elF: 115,
					abF: 82,
					lean: -6,
					wide: 0.5,
				}),
			],
			[
				0.75,
				pose({
					hipN: -10,
					kneeN: 22,
					hipF: 14,
					kneeF: 26,
					shN: 96,
					elN: 125,
					abN: 84,
					shF: 96,
					elF: 125,
					abF: 84,
					lean: -8,
					wide: 0.5,
				}),
			],
			[1, P.ready],
		],
	},
	// Calling the set from the top, the ball on the bounce: his free hand
	// up.
	callPlay: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.dribbling],
			[0.22, { ...P.dribbling, shF: 166, elF: 16, abF: 16, wrF: 8 }],
			[0.78, { ...P.dribbling, shF: 162, elF: 22, abF: 18, wrF: 10 }],
			[1, P.dribbling],
		],
	},
	// Off the ball and ready for it: down in his stance, hands up and out to
	// show the passer a target.
	spotUp: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.ready],
			[
				0.25,
				pose({
					hipN: 16,
					kneeN: 40,
					hipF: 22,
					kneeF: 42,
					lean: 14,
					shN: 62,
					elN: 64,
					shF: 58,
					elF: 66,
					abN: 18,
					abF: 18,
					wrN: 30,
					wrF: 30,
					wide: 0.45,
				}),
			],
			[
				0.8,
				pose({
					hipN: 18,
					kneeN: 44,
					hipF: 24,
					kneeF: 46,
					lean: 15,
					shN: 66,
					elN: 60,
					shF: 60,
					elF: 64,
					abN: 18,
					abF: 18,
					wrN: 30,
					wrF: 30,
					wide: 0.45,
				}),
			],
			[1, P.ready],
		],
	},
	// Calling for it, a hand up.
	callBall: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.ready],
			[
				0.25,
				pose({
					shN: 158,
					elN: 18,
					abN: 16,
					wrN: 20,
					shF: 22,
					elF: 34,
					lean: 2,
				}),
			],
			[
				0.55,
				pose({
					shN: 148,
					elN: 28,
					abN: 24,
					wrN: 26,
					shF: 24,
					elF: 36,
					lean: 2,
				}),
			],
			[
				0.75,
				pose({
					shN: 160,
					elN: 14,
					abN: 14,
					wrN: 16,
					shF: 22,
					elF: 34,
					lean: 2,
				}),
			],
			[1, P.ready],
		],
	},
	// Two claps: here!
	clapCall: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.ready],
			[0.2, P.clapOpen],
			[0.32, P.clapShut],
			[0.46, P.clapOpen],
			[0.58, P.clapShut],
			[0.78, P.clapOpen],
			[1, P.ready],
		],
	},
	// Off the ball in his stance, his hands up and working into the lane.
	stanceHands: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.stance],
			[
				0.3,
				{ ...P.stance, shN: 122, elN: 28, abN: 30, shF: 108, elF: 34, abF: 34 },
			],
			[
				0.7,
				{ ...P.stance, shN: 104, elN: 38, abN: 36, shF: 128, elF: 26, abF: 28 },
			],
			[1, P.stance],
		],
	},
	// Calling out his man - or the screen coming - pointing.
	stancePoint: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.stance],
			[0.3, { ...P.stance, shN: 96, elN: 4, abN: 22 }],
			[0.8, { ...P.stance, shN: 92, elN: 6, abN: 26 }],
			[1, P.stance],
		],
	},
	// One arm up, pointing at the crowd.
	point: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.ready],
			[
				0.3,
				pose({
					shN: 176,
					elN: 0,
					abN: 12,
					shF: 20,
					elF: 30,
					lean: -2,
				}),
			],
			[0.8, pose({ shN: 172, elN: 4, abN: 14, shF: 22, elF: 34 })],
			[1, P.ready],
		],
	},
	rebound: {
		kind: "act",
		n: 7,
		keys: [
			[0, P.gather],
			[
				0.35,
				pose({
					hipN: -4,
					kneeN: 20,
					hipF: 10,
					kneeF: 40,
					shN: 172,
					elN: 4,
					shF: 168,
					elF: 8,
					lean: 0,
				}),
			],
			[
				0.7,
				pose({
					hipN: -6,
					kneeN: 24,
					hipF: 10,
					kneeF: 42,
					shN: 150,
					elN: 50,
					shF: 145,
					elF: 55,
					lean: 2,
				}),
			],
			[1, P.hold],
		],
	},
	block: {
		kind: "act",
		n: 7,
		keys: [
			[0, P.gather],
			[
				0.4,
				pose({
					hipN: 0,
					kneeN: 18,
					hipF: 14,
					kneeF: 40,
					shN: 165,
					elN: 0,
					shF: 120,
					elF: 30,
					lean: 0,
				}),
			],
			[
				0.6,
				pose({
					hipN: 0,
					kneeN: 20,
					hipF: 14,
					kneeF: 40,
					shN: 130,
					elN: 0,
					shF: 110,
					elF: 30,
					lean: 8,
				}),
			],
			[1, P.land],
		],
	},
	contest: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.ready],
			[
				0.4,
				pose({
					hipN: -2,
					kneeN: 16,
					hipF: 10,
					kneeF: 30,
					shN: 168,
					elN: 6,
					shF: 40,
					elF: 40,
					lean: -4,
				}),
			],
			[1, P.land],
		],
	},
	// A hand in: a swipe at the ball (a steal), or a hack on the shooter.
	reach: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.stance],
			[
				0.45,
				pose({
					hipN: -24,
					kneeN: 40,
					hipF: 36,
					kneeF: 50,
					shN: 100,
					elN: 0,
					shF: 60,
					elF: 30,
					lean: 26,
				}),
			],
			[1, P.stance],
		],
	},
	// Taking a charge: set, hit, knocked back on his heels, arms flung up.
	fall: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.stance],
			[
				0.3,
				pose({
					hipN: 14,
					kneeN: 30,
					hipF: 24,
					kneeF: 36,
					shN: 120,
					elN: 30,
					shF: 130,
					elF: 34,
					lean: -22,
					wide: 0.4,
				}),
			],
			[
				0.65,
				pose({
					hipN: 62,
					kneeN: 96,
					hipF: 48,
					kneeF: 84,
					shN: 70,
					elN: 20,
					shF: 84,
					elF: 26,
					lean: -34,
					wide: 0.5,
				}),
			],
			[1, P.stance],
		],
	},
	// A chest pass: pulled in, stepped into, arms snapped straight, thumbs
	// down in the follow-through.
	pass: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.hold],
			[0.22, P.passWind],
			[0.42, P.passOut],
			[0.7, P.passOut],
			[1, P.ready],
		],
	},
	passBounce: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.hold],
			[0.22, P.passWind],
			[0.42, P.passBounceOut],
			[0.72, P.passBounceOut],
			[1, P.ready],
		],
	},
	// Over the top: up over his head from his chest, then whipped out.
	passOverhead: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.hold],
			[0.3, P.overheadUp],
			[0.5, P.overheadUp],
			[0.62, P.overheadOut],
			[0.8, P.overheadOut],
			[1, P.ready],
		],
	},
	catch: {
		kind: "act",
		n: 3,
		keys: [
			[0, P.catch],
			[1, P.hold],
		],
	},
	// Going up and coming down with it: pulled in and chinned once he lands.
	board: {
		kind: "act",
		n: 9,
		keys: [
			[0, P.gather],
			[
				0.26,
				pose({
					hipN: -4,
					kneeN: 20,
					hipF: 10,
					kneeF: 40,
					shN: 172,
					elN: 4,
					shF: 168,
					elF: 8,
					lean: 0,
				}),
			],
			[
				0.5,
				pose({
					hipN: -6,
					kneeN: 24,
					hipF: 10,
					kneeF: 42,
					shN: 150,
					elN: 50,
					shF: 145,
					elF: 55,
					lean: 2,
				}),
			],
			[0.66, P.chin],
			[1, P.chin],
		],
	},
	// Down to the floor for it - knees bent deep, reaching out - and up
	// with it in both hands.
	pickup: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.ready],
			[
				0.5,
				pose({
					hipN: 60,
					kneeN: 120,
					hipF: 84,
					kneeF: 126,
					shN: 30,
					elN: 6,
					shF: 26,
					elF: 8,
					abN: 6,
					abF: 6,
					lean: 70,
					wide: 0.35,
				}),
			],
			[1, P.hold],
		],
	},
	// On the bench.
	sit: {
		kind: "loop",
		n: 2,
		fps: 0.4,
		pose: (i) =>
			pose({
				hipN: 88,
				kneeN: 94,
				hipF: 84,
				kneeF: 90,
				shN: 30 + i * 4,
				elN: 52,
				shF: 26,
				elF: 56,
				lean: -6 + i,
				abN: 14,
				abF: 14,
				wide: 0.35,
			}),
	},
	// On his feet on the bench, cheering.
	cheer: {
		kind: "loop",
		n: 2,
		fps: 3,
		pose: (i) =>
			pose({
				shN: i ? 168 : 150,
				elN: i ? 10 : 40,
				abN: 20,
				shF: i ? 150 : 166,
				elF: i ? 40 : 12,
				abF: 20,
				kneeN: 10,
				kneeF: 12,
				lean: -2,
			}),
	},
	celebrate: {
		kind: "loop",
		n: 2,
		fps: 4,
		pose: (i) =>
			pose({
				shN: i ? 172 : 150,
				elN: i ? 18 : 64,
				shF: -12,
				elF: 30,
				kneeN: i ? 10 : 22,
				kneeF: i ? 12 : 24,
				lean: 2,
			}),
	},

	// Holding the follow-through - arm up, wrist snapped down - until the
	// ball gets there, then down.
	follow: {
		kind: "act",
		n: 5,
		keys: [
			[
				0,
				pose({
					kneeN: 10,
					kneeF: 14,
					shN: 162,
					elN: 14,
					shF: 118,
					elF: 36,
					lean: -2,
					wrN: -110,
				}),
			],
			[
				0.7,
				pose({
					kneeN: 8,
					kneeF: 12,
					shN: 158,
					elN: 16,
					shF: 96,
					elF: 40,
					lean: -1,
					wrN: -104,
				}),
			],
			[1, P.ready],
		],
	},
	// A slap of the hands with a teammate.
	highFive: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.ready],
			[0.4, pose({ shN: 148, elN: 22, abN: 18, wrN: 20, lean: -4 })],
			[0.6, pose({ shN: 140, elN: 30, abN: 18, wrN: 10, lean: -3 })],
			[1, P.ready],
		],
	},
	// ---- the officials ----
	// The whistle: a fist straight up, the clock stopped.
	signalUp: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.ready],
			[0.2, pose({ shN: 178, elN: 0, abN: 8, shF: 12, elF: 20, lean: -2 })],
			[0.85, pose({ shN: 176, elN: 2, abN: 8, shF: 12, elF: 22, lean: -2 })],
			[1, P.ready],
		],
	},
	// An arm straight out to his right: the way the ball goes, the way the
	// play is headed.
	signalSide: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.ready],
			[0.25, pose({ shN: 90, elN: 0, abN: 86, shF: 14, elF: 22 })],
			[0.85, pose({ shN: 92, elN: 2, abN: 84, shF: 14, elF: 22 })],
			[1, P.ready],
		],
	},
	// Three-point field goal: both arms up.
	threeUp: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.ready],
			[
				0.2,
				pose({
					shN: 176,
					elN: 0,
					abN: 12,
					shF: 176,
					elF: 0,
					abF: 12,
					lean: -3,
				}),
			],
			[
				0.85,
				pose({
					shN: 174,
					elN: 4,
					abN: 14,
					shF: 174,
					elF: 4,
					abF: 14,
					lean: -3,
				}),
			],
			[1, P.ready],
		],
	},
	// Traveling: the fists rolling over each other in front of him.
	travel: {
		kind: "loop",
		n: 4,
		fps: 5,
		pose: (i) => {
			const a = (i / 4) * Math.PI * 2;
			return pose({
				shN: 62 + 14 * Math.sin(a),
				elN: 84 - 18 * Math.cos(a),
				abN: 2,
				shF: 62 - 14 * Math.sin(a),
				elF: 84 + 18 * Math.cos(a),
				abF: 2,
				lean: 4,
			});
		},
	},
	// The jump ball: held out between the two of them, then thrown up.
	toss: {
		kind: "act",
		n: 6,
		keys: [
			[0, pose({ shN: 58, elN: 34, shF: 58, elF: 34, abN: 4, abF: 4 })],
			[0.35, pose({ shN: 70, elN: 26, shF: 60, elF: 30, abN: 4, abF: 4 })],
			[0.6, pose({ shN: 168, elN: 2, shF: 40, elF: 30, lean: -4 })],
			[1, pose({ shN: 150, elN: 10, shF: 24, elF: 26, lean: -2 })],
		],
	},

	// ---- the coaches ----
	// Arms folded, watching.
	crossed: {
		kind: "loop",
		n: 2,
		fps: 0.5,
		pose: (i) =>
			pose({
				shN: 26,
				elN: 116,
				abN: -14,
				shF: 30,
				elF: 112,
				abF: -14,
				lean: 1 + i,
				kneeN: 8,
				kneeF: 10,
			}),
	},
	clap: {
		kind: "loop",
		n: 2,
		fps: 4,
		pose: (i) =>
			pose({
				shN: 64,
				elN: 58,
				abN: i ? 16 : -12,
				shF: 64,
				elF: 58,
				abF: i ? 16 : -12,
				lean: 2,
			}),
	},
	// Hands on his hips, head down: scored on, a call gone against him, the
	// game lost.
	hips: {
		kind: "loop",
		n: 2,
		fps: 0.8,
		pose: (i) =>
			pose({
				shN: -10,
				elN: 108,
				abN: 56,
				wrN: -14,
				shF: -10,
				elF: 108,
				abF: 56,
				wrF: -14,
				lean: i ? 13 : 9,
				kneeN: 8,
				kneeF: 10,
				wide: 0.3,
			}),
	},
	// What was that? Arms out, palms up.
	protest: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.ready],
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
			[1, P.ready],
		],
	},
	// Bent over, hands on his knees.
	crouch: {
		kind: "loop",
		n: 2,
		fps: 0.6,
		pose: (i) =>
			pose({
				hipN: 38,
				kneeN: 52,
				hipF: 42,
				kneeF: 56,
				lean: 40 + i * 2,
				shN: 6,
				elN: 8,
				abN: 10,
				shF: 8,
				elF: 8,
				abF: 10,
				wide: 0.35,
			}),
	},
	// Talking it over: one hand making the point.
	talk: {
		kind: "loop",
		n: 4,
		fps: 2.2,
		pose: (i) =>
			pose({
				shN: [44, 58, 50, 62][i]!,
				elN: [70, 54, 80, 48][i]!,
				wrN: 20,
				shF: 24,
				elF: 96,
				abF: -8,
				lean: 4,
			}),
	},

	// ---- the photographers ----
	// Down on one knee on the baseline, the camera resting on the other.
	kneel: {
		kind: "loop",
		n: 2,
		fps: 0.3,
		pose: (i) =>
			pose({
				hipN: -4,
				kneeN: 96,
				hipF: 84,
				kneeF: 86,
				lean: 8 + i,
				shN: 38,
				elN: 58,
				shF: 44,
				elF: 52,
				wide: 0.2,
			}),
	},
	// The camera up to his eye.
	kneelShoot: {
		kind: "loop",
		n: 2,
		fps: 0.3,
		pose: (i) =>
			pose({
				hipN: -4,
				kneeN: 96,
				hipF: 84,
				kneeF: 86,
				lean: 6 + i,
				shN: 74,
				elN: 112,
				abN: -6,
				shF: 80,
				elF: 104,
				abF: -6,
				wide: 0.2,
			}),
	},
} satisfies Record<string, Anim>;

export type AnimName = keyof typeof ANIMS;

export const animFrames = (anim: AnimName): number => ANIMS[anim].n;

export const poseFor = (anim: AnimName, frame: number): Pose => {
	const a: Anim = ANIMS[anim];
	if (a.kind === "act") {
		return keyed(a.keys, a.n > 1 ? frame / (a.n - 1) : 0);
	}
	return a.pose(frame);
};

// Running, he leaves the floor between strides: how high (feet) at a point
// through the stride, highest with his legs spread wide.
const BOUNCE: Partial<Record<AnimName, number>> = { run: 0.1, sprint: 0.2 };
export const bounceAt = (anim: AnimName, phase: number): number => {
	const h = BOUNCE[anim];
	return h ? h * Math.sin(2 * Math.PI * phase) ** 2 : 0;
};

// The pose partway through an animation - an act from start (0) to finish
// (1), a cycle or loop through one turn - blended between its key frames, so
// a body moves smoothly instead of stepping frame to frame.
export const poseAt = (anim: AnimName, phase: number): Pose => {
	const a: Anim = ANIMS[anim];
	if (a.kind === "act") {
		return keyed(a.keys, Math.min(1, Math.max(0, phase)));
	}
	const p = (((phase % 1) + 1) % 1) * a.n;
	if (a.kind === "cycle") {
		// Cycles are written as continuous strides.
		return a.pose(p);
	}
	const i0 = Math.floor(p);
	return lerpPose(a.pose(i0), a.pose((i0 + 1) % a.n), p - i0);
};

// Which frame of an animation shows at a moment: an act by how far through it
// is, a cycle by how far the body has run, a loop by the clock.
export const actFrame = (anim: AnimName, u: number): number => {
	const n = ANIMS[anim].n;
	return Math.min(n - 1, Math.max(0, Math.floor(u * n)));
};
export const cycleFrame = (anim: AnimName, feet: number): number => {
	const a: Anim = ANIMS[anim];
	const stride = a.kind === "cycle" ? a.stride : 5;
	return Math.floor((feet / stride) * a.n) % a.n;
};
export const loopFrame = (anim: AnimName, ms: number, phase = 0): number => {
	const a: Anim = ANIMS[anim];
	const fps = a.kind === "loop" ? a.fps : 2;
	return Math.floor((ms / 1000) * fps + phase) % a.n;
};

// A player's build, in feet: an athlete's, in true proportion - long legs,
// broad shoulders tapering to a narrow waist, lean limbs - but for his head,
// a little big, so his face still reads from up in the stands. Height drives
// everything, a little exaggerated so a seven-footer towers over a
// six-footer; girth comes from his weight for his height, so a 250-pound
// center is long and lean and a 250-pound forward is thick.
export type Body = {
	H: number;
	hipH: number;
	ankleH: number;
	thigh: number;
	shin: number;
	foot: number;
	torso: number;
	neck: number;
	headR: number;
	upper: number;
	fore: number;
	shoulderW: number;
	hipW: number;
	depth: number;
	thighR: number;
	kneeR: number;
	calfR: number;
	ankleR: number;
	upperR: number;
	foreR: number;
	handR: number;
};

export const DEFAULT_HGT = 78;
export const DEFAULT_WEIGHT = 215;
// The league's typical build, as a body mass index.
const DEFAULT_BMI = 24.6;

// How thick he is for his height: 1 for a typical build.
export const girthOf = (hgt: number, weight: number): number => {
	const bmi = (703 * weight) / (hgt * hgt);
	return Math.min(1.28, Math.max(0.84, (bmi / DEFAULT_BMI) ** 0.9));
};

// A cartoon athlete, Hoop Land style: a big head - a little over a quarter
// of him - on a neck, over a compact body with sturdy legs and arms. His
// height and build still tell: a center towers over a point guard, a big man
// is broad. (His face is drawn a little bigger than headR: crown to chin is
// 2.45 headR.)
export const bodyOf = (hgt = DEFAULT_HGT, weight = DEFAULT_WEIGHT): Body => {
	const H = (DEFAULT_HGT * (hgt / DEFAULT_HGT) ** 1.25) / 12;
	const g = girthOf(hgt, weight);
	return {
		H,
		hipH: H * 0.4,
		ankleH: H * 0.03,
		thigh: H * 0.19,
		shin: H * 0.18,
		foot: H * 0.15,
		torso: H * 0.235,
		neck: H * 0.08,
		headR: H * 0.12,
		upper: H * 0.13,
		fore: H * 0.12,
		shoulderW: H * 0.128 * g,
		hipW: H * 0.078 * g,
		depth: H * 0.13 * g,
		thighR: H * 0.057 * g,
		kneeR: H * 0.044 * g,
		calfR: H * 0.048 * g,
		ankleR: H * 0.03,
		upperR: H * 0.045 * g,
		foreR: H * 0.039 * g,
		handR: H * 0.036,
	};
};

// How high he reaches standing, arms straight up - shoulders, arms at a
// raised arm's stretch, hand (feet).
export const standingReach = (b: Body): number =>
	b.ankleH +
	b.thigh +
	b.shin +
	b.torso -
	b.H * 0.022 +
	(b.upper + b.fore) * 1.32 +
	b.handR * 2.2;

// A point on the body: f forward, s to his left, u up - feet, with his feet
// on the floor at the origin.
export type V3 = { f: number; s: number; u: number };
export type Limb = { root: V3; mid: V3; end: V3; tip?: V3 };
export type Skeleton = {
	pelvis: V3;
	chest: V3;
	head: V3;
	// Right (the shooting hand, the dribbling hand) and left.
	legR: Limb;
	legL: Limb;
	armR: Limb;
	armL: Limb;
};

const v3 = (f: number, s: number, u: number): V3 => ({ f, s, u });

// The pose's angles live in the plane he faces along; arms swing out from
// his sides a little (more in a stance), feet spread with the knees.
export const skeleton = (b: Body, q: Pose): Skeleton => {
	const rad = Math.PI / 180;
	// An angle in the facing plane: 0 straight down, 90 straight ahead.
	const dir = (deg: number) => ({
		f: Math.sin(deg * rad),
		u: -Math.cos(deg * rad),
	});
	const wide = q.wide;
	const leg = (hipDeg: number, kneeDeg: number, side: 1 | -1) => {
		const a = dir(hipDeg);
		const c = dir(hipDeg - kneeDeg);
		const root = v3(0, side * b.hipW, b.hipH);
		const mid = v3(
			a.f * b.thigh,
			side * (b.hipW + wide * 0.5),
			b.hipH + a.u * b.thigh,
		);
		const end = v3(
			mid.f + c.f * b.shin,
			side * (b.hipW + wide),
			mid.u + c.u * b.shin,
		);
		return { root, mid, end };
	};
	// Balanced: a bend at the knees is a bend at the hips too, the way a
	// body crouches - so his feet stay under him instead of trailing behind.
	const ankleF = (h: number, k: number) =>
		b.thigh * Math.sin(h * rad) + b.shin * Math.sin((h - k) * rad);
	const avgF = (d: number) =>
		(ankleF(q.hipN + d, q.kneeN) + ankleF(q.hipF + d, q.kneeF)) / 2;
	let flex = 0;
	for (let i = 0; i < 4; i++) {
		const f0 = avgF(flex);
		const slope = avgF(flex + 1) - f0;
		if (Math.abs(slope) < 1e-6) {
			break;
		}
		flex -= f0 / slope;
	}
	flex = Math.max(-25, Math.min(25, flex)) * 0.8 * q.plant;
	const legR = leg(q.hipN + flex, q.kneeN, -1);
	const legL = leg(q.hipF + flex, q.kneeF, 1);
	// Down onto the floor: the lower ankle sits at ankle height.
	const off = b.ankleH - Math.min(legR.end.u, legL.end.u);
	for (const l of [legR, legL]) {
		l.root.u += off;
		l.mid.u += off;
		l.end.u += off;
		(l as Limb).tip = v3(
			l.end.f + b.foot * 0.72,
			l.end.s,
			Math.max(b.ankleR, l.end.u - b.ankleH * 0.55),
		);
	}
	const L = q.lean * rad;
	const pelvis = v3(0, 0, b.hipH + off);
	const chest = v3(
		pelvis.f + Math.sin(L) * b.torso,
		0,
		pelvis.u + Math.cos(L) * b.torso,
	);
	const up = b.neck + b.headR;
	const head = v3(
		chest.f + Math.sin(L) * up + b.headR * 0.12,
		0,
		chest.u + Math.cos(L) * up,
	);
	return {
		pelvis,
		chest,
		head,
		legR,
		legL,
		armR: armLimb(b, chest, q.shN, q.elN, q.abN, q.wrN, -1),
		armL: armLimb(b, chest, q.shF, q.elF, q.abF, q.wrF, 1),
	};
};

const RAD = Math.PI / 180;

// An angle in the plane he faces along: 0 straight down, 90 straight ahead.
const dirOf = (deg: number) => ({
	f: Math.sin(deg * RAD),
	u: -Math.cos(deg * RAD),
});

// A cartoon's reach: an arm thrown up over his head stretches and swings a
// little wide, so the hand clears that big head instead of hiding behind it.
const reachOf = (shDeg: number): number => {
	const up0 = Math.min(1, Math.max(0, (shDeg - 105) / 60));
	return up0 * up0 * (3 - 2 * up0);
};

const shoulderOf = (b: Body, chest: V3, side: 1 | -1): V3 =>
	v3(chest.f, side * b.shoulderW, chest.u - b.H * 0.022);

// An arm built from its angles - the upper arm swung `shDeg`, the elbow
// bent `elDeg` more, the whole arm `abRad` out from his side (already
// widened for a raised arm), each segment `reach` times its length - and
// its hand bent `wrDeg` at the wrist.
const buildArm = (
	b: Body,
	chest: V3,
	shDeg: number,
	elDeg: number,
	abRad: number,
	reach: number,
	wrDeg: number,
	side: 1 | -1,
): Limb => {
	const along = (deg: number) => {
		const d = dirOf(deg);
		return {
			f: d.f * Math.cos(abRad),
			s: side * Math.sin(abRad),
			u: d.u * Math.cos(abRad),
		};
	};
	const root = shoulderOf(b, chest, side);
	const a = along(shDeg);
	const upper = b.upper * reach;
	const fore = b.fore * reach;
	const mid = v3(
		root.f + a.f * upper,
		root.s + a.s * upper,
		root.u + a.u * upper,
	);
	const c = along(shDeg + elDeg);
	const end = v3(mid.f + c.f * fore, mid.s + c.s * fore, mid.u + c.u * fore);
	const h = along(shDeg + elDeg + wrDeg);
	const hand = b.handR * 2.2;
	const tip = v3(end.f + h.f * hand, end.s + h.s * hand, end.u + h.u * hand);
	return { root, mid, end, tip };
};

const armLimb = (
	b: Body,
	chest: V3,
	shDeg: number,
	elDeg: number,
	abDeg: number,
	wrDeg: number,
	side: 1 | -1,
): Limb => {
	const up = reachOf(shDeg);
	return buildArm(
		b,
		chest,
		shDeg,
		elDeg,
		(abDeg + Math.max(0, 22 - abDeg) * up) * RAD,
		1 + 0.32 * up,
		wrDeg,
		side,
	);
};

// The arm that puts his wrist at `target`, worked back from the arm's own
// geometry (the cartoon reach included): out from his side as far as the
// target is, then shoulder and elbow from the triangle the two bones make.
export const armTo = (
	b: Body,
	chest: V3,
	target: V3,
	wrDeg: number,
	side: 1 | -1,
): Limb => {
	const root = shoulderOf(b, chest, side);
	let reach = 1;
	let sh = 0;
	let el = 0;
	let ab = 0;
	for (let it = 0; it < 3; it++) {
		const L1 = b.upper * reach;
		const L2 = b.fore * reach;
		const out = (side * (target.s - root.s)) / (L1 + L2);
		ab = Math.asin(Math.min(0.95, Math.max(-0.95, out)));
		const c = Math.cos(ab);
		const F = (target.f - root.f) / c;
		const U = (target.u - root.u) / c;
		const D = Math.min(
			L1 + L2 - 0.01,
			Math.max(Math.abs(L1 - L2) + 0.01, Math.hypot(F, U)),
		);
		el = Math.acos(
			Math.min(1, Math.max(-1, (D * D - L1 * L1 - L2 * L2) / (2 * L1 * L2))),
		);
		sh =
			Math.atan2(F, -U) - Math.atan2(L2 * Math.sin(el), L1 + L2 * Math.cos(el));
		reach = 1 + 0.32 * reachOf(sh / RAD);
	}
	return buildArm(b, chest, sh / RAD, el / RAD, ab, reach, wrDeg, side);
};

// The arm with its elbow swung `deg` degrees out about the line from his
// shoulder to his hand - the hand where it was, the elbow out wide.
const flared = (arm: Limb, deg: number, side: 1 | -1): Limb => {
	if (deg === 0) {
		return arm;
	}
	const a = arm.root;
	const ax = v3(arm.end.f - a.f, arm.end.s - a.s, arm.end.u - a.u);
	const l = Math.hypot(ax.f, ax.s, ax.u) || 1;
	const k = v3(ax.f / l, ax.s / l, ax.u / l);
	const v = v3(arm.mid.f - a.f, arm.mid.s - a.s, arm.mid.u - a.u);
	const kv = k.f * v.f + k.s * v.s + k.u * v.u;
	const kxv = v3(
		k.s * v.u - k.u * v.s,
		k.u * v.f - k.f * v.u,
		k.f * v.s - k.s * v.f,
	);
	const turn = (phi: number): V3 => {
		const c = Math.cos(phi);
		const s = Math.sin(phi);
		return v3(
			a.f + v.f * c + kxv.f * s + k.f * kv * (1 - c),
			a.s + v.s * c + kxv.s * s + k.s * kv * (1 - c),
			a.u + v.u * c + kxv.u * s + k.u * kv * (1 - c),
		);
	};
	const one = turn(deg * RAD);
	const two = turn(-deg * RAD);
	return { ...arm, mid: side * one.s >= side * two.s ? one : two };
};

// How a move holds the ball: in both hands, one on each side of it; up on
// the shooting hand with the other guiding it; or palmed in one hand.
export type Grip = "two" | "shot" | "palm";
const GRIPS: Partial<Record<AnimName, Grip>> = {
	shoot: "shot",
	fade: "shot",
	layup: "palm",
	dunk: "palm",
	dunk1: "palm",
	tomahawk: "palm",
	hook: "palm",
};
export const gripOf = (anim: AnimName): Grip => GRIPS[anim] ?? "two";

// A basketball's radius, feet.
const BALL_RADIUS = 0.39;

// On the move with the ball in both hands - not dribbling it - he carries
// it at his chest, his arms still.
const CARRIES = new Set<AnimName>([
	"run",
	"sprint",
	"walk",
	"back",
	"slide",
	"dribble",
	"post",
]);
const carried = (q: Pose, anim: AnimName): Pose =>
	CARRIES.has(anim)
		? {
				...q,
				shN: P.hold.shN,
				elN: P.hold.elN,
				shF: P.hold.shF,
				elF: P.hold.elF,
				abN: P.hold.abN,
				abF: P.hold.abF,
				wrN: 0,
				wrF: 0,
			}
		: q;

// Where the ball is while he holds it, and his skeleton with his hands put
// on it the way the move holds it - so the ball is in his hands, not
// floating somewhere between them.
export const holdBall = (
	b: Body,
	q0: Pose,
	anim: AnimName,
): { sk: Skeleton; ball: V3 } => {
	const q = carried(q0, anim);
	const sk = skeleton(b, q);
	const grip = gripOf(anim);
	const R = BALL_RADIUS;
	const r = sk.armR;
	const l = sk.armL;
	const mix = (p: V3, o: V3, w: number): V3 =>
		v3(p.f + (o.f - p.f) * w, p.s + (o.s - p.s) * w, p.u + (o.u - p.u) * w);
	const side = (ball: V3, sgn: 1 | -1): V3 =>
		v3(ball.f - 0.06, ball.s + sgn * R * 1.05, ball.u);
	// Both hands: between where the move puts them, out in front of him.
	const two = v3(
		Math.max((r.end.f + l.end.f) / 2 + 0.3, sk.chest.f + R + 0.22),
		((r.end.s + l.end.s) / 2) * 0.5,
		(r.end.u + l.end.u) / 2,
	);
	if (grip === "two") {
		return {
			sk: {
				...sk,
				armR: flared(armTo(b, sk.chest, side(two, -1), q.wrN, -1), q.flare, -1),
				armL: flared(armTo(b, sk.chest, side(two, 1), q.wrF, 1), q.flare, 1),
			},
			ball: two,
		};
	}
	// One hand takes it as his arm comes up: two hands while the shooting
	// hand is down by his chest, all his once it is up past the shoulder.
	const up = Math.min(1, Math.max(0, (r.end.u - (r.root.u - 0.45)) / 0.9));
	const w = up * up * (3 - 2 * up);
	const tip = r.tip ?? r.end;
	const len =
		Math.hypot(tip.f - r.end.f, tip.s - r.end.s, tip.u - r.end.u) || 1;
	const d = {
		f: (tip.f - r.end.f) / len,
		s: (tip.s - r.end.s) / len,
		u: (tip.u - r.end.u) / len,
	};
	// Up on the shooting hand, or palmed out past the fingers' roots.
	const k = grip === "shot" ? 0.75 : 1.15;
	const one = v3(
		r.end.f + d.f * R * k,
		r.end.s + d.s * R * k,
		r.end.u + d.u * R * k + (grip === "shot" ? R * 0.55 : 0),
	);
	const ball = mix(two, one, w);
	const armR =
		w >= 0.999
			? r
			: armTo(b, sk.chest, mix(side(two, -1), r.end, w), q.wrN, -1);
	// The other hand guides a jumper all the way up; on a layup or a dunk it
	// lets go.
	const armL =
		grip === "palm" && w >= 0.999
			? l
			: armTo(
					b,
					sk.chest,
					grip === "shot" ? side(ball, 1) : mix(side(ball, 1), l.end, w),
					q.wrF,
					1,
				);
	return { sk: { ...sk, armR, armL }, ball };
};

// The dribbling hand through one bounce: on the ball at the top (0),
// pushing it down until it leaves him, then back up to meet it (1). With the
// ball in his left hand the arms trade jobs: the right is the one held out
// to keep his man off.
export type Hand = "R" | "L";
export const dribbleArm = (q: Pose, ph: number, hand: Hand = "R"): Pose => {
	const push = ph < 0.22 ? ph / 0.22 : 1 - (ph - 0.22) / 0.78;
	const e = push * push * (3 - 2 * push);
	if (hand === "L") {
		return {
			...q,
			shN: q.shF,
			elN: q.elF,
			abN: q.abF,
			wrN: q.wrF,
			shF: 34 + 12 * e,
			elF: 62 - 46 * e,
			abF: 16,
			wrF: 24 - 60 * e,
		};
	}
	return {
		...q,
		shN: 34 + 12 * e,
		elN: 62 - 46 * e,
		abN: 16,
		wrN: 24 - 60 * e,
	};
};

// His pose at a moment: the move's, with the dribbling hand on the bounce
// when he is dribbling, or - `target` of the way (0 to 1) - his hands up for
// a pass on its way to him, whatever his feet are doing.
export const posed = (
	anim: AnimName,
	phase: number,
	dribble?: number,
	hand?: Hand,
	target = 0,
): Pose => {
	const q = poseAt(anim, phase);
	if (dribble !== undefined) {
		return dribbleArm(q, dribble, hand);
	}
	if (target <= 0) {
		return q;
	}
	const out = { ...q };
	for (const key of Object.keys(TARGET) as (keyof Pose)[]) {
		out[key] = q[key] + (TARGET[key]! - q[key]) * Math.min(1, target);
	}
	return out;
};
