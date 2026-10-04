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
	abN: 9,
	abF: 9,
	wide: 0.08,
};
const pose = (o: Partial<Pose>): Pose => ({ ...BASE, ...o });

const lerpPose = (a: Pose, b: Pose, f: number): Pose => {
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
	ready: pose({}),
	stance: pose({
		hipN: 20,
		kneeN: 62,
		hipF: 34,
		kneeF: 66,
		shN: 66,
		elN: 20,
		shF: 48,
		elF: 28,
		lean: 22,
		abN: 38,
		abF: 38,
		wide: 0.55,
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
	passOut: pose({
		hipN: -10,
		kneeN: 24,
		hipF: 16,
		kneeF: 22,
		shN: 92,
		elN: 2,
		shF: 86,
		elF: 6,
		lean: 14,
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
};

type RunMode = "run" | "dribble" | "back" | "walk" | "carry";
const runPose = (ph: number, mode: RunMode): Pose => {
	const a = Math.sin(2 * Math.PI * ph);
	const c = Math.cos(2 * Math.PI * ph);
	if (mode === "walk" || mode === "carry") {
		const legs = {
			hipN: 20 * a,
			hipF: -20 * a,
			kneeN: 10 + 30 * Math.max(0, c),
			kneeF: 10 + 30 * Math.max(0, -c),
		};
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
	const legs = {
		hipN: 36 * a,
		hipF: -36 * a,
		kneeN: 16 + 66 * Math.max(0, c) ** 1.3,
		kneeF: 16 + 66 * Math.max(0, -c) ** 1.3,
		lean: 12,
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
		pose: (i) => (i ? pose({ kneeN: 20, kneeF: 22 }) : P.ready),
	},
	stance: {
		kind: "loop",
		n: 2,
		fps: 2.5,
		pose: (i) =>
			i ? { ...P.stance, hipN: 26, hipF: 28, shN: 72, shF: 40 } : P.stance,
	},
	hold: { kind: "loop", n: 1, fps: 1, pose: () => P.hold },
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
	dribble: {
		kind: "cycle",
		n: 6,
		stride: 8.2,
		pose: (i) => runPose(i / 6, "dribble"),
	},
	back: {
		kind: "cycle",
		n: 6,
		stride: 4.6,
		pose: (i) => runPose(i / 6, "back"),
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
					hipN: 75,
					kneeN: 95,
					hipF: -12,
					kneeF: 18,
					shN: 120,
					elN: 50,
					shF: 60,
					elF: 70,
					lean: 6,
				}),
			],
			[
				0.6,
				pose({
					hipN: 70,
					kneeN: 100,
					hipF: -6,
					kneeF: 22,
					shN: 168,
					elN: 6,
					shF: 70,
					elF: 60,
					lean: 0,
				}),
			],
			[1, P.land],
		],
	},
	dunk: {
		kind: "act",
		n: 10,
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
				0.3,
				pose({
					hipN: 60,
					kneeN: 100,
					hipF: 30,
					kneeF: 95,
					shN: 165,
					elN: 40,
					shF: 160,
					elF: 45,
					lean: 4,
				}),
			],
			[
				0.48,
				pose({
					hipN: 40,
					kneeN: 80,
					hipF: 20,
					kneeF: 85,
					shN: 125,
					elN: 8,
					shF: 120,
					elF: 12,
					lean: 14,
				}),
			],
			[
				0.58,
				pose({
					hipN: 4,
					kneeN: 14,
					hipF: -6,
					kneeF: 28,
					shN: 176,
					elN: 0,
					shF: 172,
					elF: 4,
					lean: 0,
				}),
			],
			[
				0.8,
				pose({
					hipN: 6,
					kneeN: 18,
					hipF: -10,
					kneeF: 34,
					shN: 176,
					elN: 0,
					shF: 170,
					elF: 6,
					lean: 0,
				}),
			],
			[1, P.land],
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
	pass: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.hold],
			[0.45, P.passOut],
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
	pickup: {
		kind: "act",
		n: 5,
		keys: [
			[0, P.ready],
			[
				0.5,
				pose({
					hipN: -34,
					kneeN: 76,
					hipF: 24,
					kneeF: 74,
					shN: 30,
					elN: 10,
					shF: 24,
					elF: 14,
					lean: 42,
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

// A player's build, in feet. Height drives everything; weight adds girth.
// Proportions are real ones, except the head, which is drawn a touch big so a
// face still reads from the broadcast camera.
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

export const bodyOf = (hgt = DEFAULT_HGT, weight = DEFAULT_WEIGHT): Body => {
	const H = hgt / 12;
	const g = Math.min(
		1.18,
		Math.max(0.9, 1 + ((weight - DEFAULT_WEIGHT) / DEFAULT_WEIGHT) * 0.7),
	);
	return {
		H,
		hipH: H * 0.525,
		ankleH: H * 0.045,
		thigh: H * 0.245,
		shin: H * 0.235,
		foot: H * 0.15,
		torso: H * 0.285,
		neck: H * 0.045,
		headR: H * 0.068,
		upper: H * 0.185,
		fore: H * 0.2,
		shoulderW: H * 0.118 * g,
		hipW: H * 0.066 * g,
		depth: H * 0.1 * g,
		thighR: H * 0.046 * g,
		kneeR: H * 0.034 * g,
		calfR: H * 0.035 * g,
		ankleR: H * 0.021,
		upperR: H * 0.03 * g,
		foreR: H * 0.025 * g,
		handR: H * 0.024,
	};
};

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
	const legR = leg(q.hipN, q.kneeN, -1);
	const legL = leg(q.hipF, q.kneeF, 1);
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
	const arm = (shDeg: number, elDeg: number, abDeg: number, side: 1 | -1) => {
		const ab = abDeg * rad;
		const along = (deg: number) => {
			const d = dir(deg);
			return {
				f: d.f * Math.cos(ab),
				s: side * Math.sin(ab),
				u: d.u * Math.cos(ab),
			};
		};
		const root = v3(chest.f, side * b.shoulderW, chest.u - b.H * 0.022);
		const a = along(shDeg);
		const mid = v3(
			root.f + a.f * b.upper,
			root.s + a.s * b.upper,
			root.u + a.u * b.upper,
		);
		const c = along(shDeg + elDeg);
		const end = v3(
			mid.f + c.f * b.fore,
			mid.s + c.s * b.fore,
			mid.u + c.u * b.fore,
		);
		return { root, mid, end };
	};
	return {
		pelvis,
		chest,
		head,
		legR,
		legL,
		armR: arm(q.shN, q.elN, q.abN, -1),
		armL: arm(q.shF, q.elF, q.abF, 1),
	};
};
