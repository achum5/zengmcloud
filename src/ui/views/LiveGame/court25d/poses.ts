import { PX_PER_FT } from "./geometry.ts";

// THE BODY AND HOW IT MOVES.
//
// A player is a small skeleton - hips, knees, shoulders, elbows - posed
// by a handful of joint angles and drawn as chunky pixels around it. Angles are
// in degrees for a player facing RIGHT: 0 points straight down, positive swings
// forward (toward where he faces). A knee bends backward from its thigh, an
// elbow forward from its upper arm. N is the near limb (toward the camera), F
// the far one.
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
	run: { kind: "cycle", n: 6, stride: 5.4, pose: (i) => runPose(i / 6, "run") },
	dribble: {
		kind: "cycle",
		n: 6,
		stride: 5.2,
		pose: (i) => runPose(i / 6, "dribble"),
	},
	back: {
		kind: "cycle",
		n: 6,
		stride: 3.4,
		pose: (i) => runPose(i / 6, "back"),
	},
	walk: {
		kind: "cycle",
		n: 6,
		stride: 3.2,
		pose: (i) => runPose(i / 6, "walk"),
	},
	carry: {
		kind: "cycle",
		n: 6,
		stride: 3.2,
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

// A player's build in sprite pixels. Height drives everything; weight adds
// width. Proportions are a little chunky (bigger head, wider torso) - that is
// what reads at thirty-odd pixels tall.
export type Body = {
	H: number;
	leg: number;
	thigh: number;
	shin: number;
	torso: number;
	headH: number;
	headW: number;
	upper: number;
	fore: number;
	torsoW: number;
	legT: number;
	armT: number;
};

export const DEFAULT_HGT = 78;
export const DEFAULT_WEIGHT = 215;

export const bodyOf = (hgt = DEFAULT_HGT, weight = DEFAULT_WEIGHT): Body => {
	const H = (hgt / 12) * PX_PER_FT;
	const girth = Math.min(
		1.15,
		Math.max(0.9, 1 + ((weight - DEFAULT_WEIGHT) / DEFAULT_WEIGHT) * 0.6),
	);
	return {
		H,
		leg: H * 0.44,
		thigh: H * 0.225,
		shin: H * 0.205,
		torso: H * 0.29,
		headH: Math.round(H * 0.22),
		headW: Math.round(H * 0.2),
		upper: H * 0.19,
		fore: H * 0.175,
		torsoW: Math.max(8, Math.round(H * 0.26 * girth)),
		legT: Math.max(3, Math.round(H * 0.08 * girth)),
		armT: H * girth > 42 ? 3 : 2,
	};
};

type V = { x: number; y: number };
export type Skeleton = {
	hip: V;
	shoulder: V;
	headC: V;
	legF: { knee: V; ankle: V };
	legN: { knee: V; ankle: V };
	armF: { s0: V; elbow: V; hand: V };
	armN: { s0: V; elbow: V; hand: V };
};

// Joint positions in sprite pixels: origin between the feet, x forward, y up.
// The body is set down so its lowest sole touches y = 0; a jump is z on top.
export const skeleton = (b: Body, q: Pose): Skeleton => {
	const dv = (deg: number): V => {
		const r = (deg * Math.PI) / 180;
		return { x: Math.sin(r), y: -Math.cos(r) };
	};
	const leg = (h: number, k: number) => {
		const a = dv(h);
		const s = dv(h - k);
		const knee = { x: a.x * b.thigh, y: b.leg + a.y * b.thigh };
		return {
			knee,
			ankle: { x: knee.x + s.x * b.shin, y: knee.y + s.y * b.shin },
		};
	};
	const legF = leg(q.hipF, q.kneeF);
	const legN = leg(q.hipN, q.kneeN);
	const off = -(Math.min(legF.ankle.y, legN.ankle.y) - 2);
	for (const l of [legF, legN]) {
		l.knee.y += off;
		l.ankle.y += off;
	}
	const hip = { x: 0, y: b.leg + off };
	const L = (q.lean * Math.PI) / 180;
	const shoulder = {
		x: hip.x + Math.sin(L) * b.torso,
		y: hip.y + Math.cos(L) * b.torso,
	};
	const headC = {
		x: shoulder.x + Math.sin(L) * (1 + b.headH / 2) + 0.5,
		y: shoulder.y + Math.cos(L) * (1 + b.headH / 2),
	};
	const arm = (sh: number, el: number, dx: number) => {
		const s0 = { x: shoulder.x + dx, y: shoulder.y - 1.5 };
		const u = dv(sh);
		const f = dv(sh + el);
		const elbow = { x: s0.x + u.x * b.upper, y: s0.y + u.y * b.upper };
		return {
			s0,
			elbow,
			hand: { x: elbow.x + f.x * b.fore, y: elbow.y + f.y * b.fore },
		};
	};
	return {
		hip,
		shoulder,
		headC,
		legF,
		legN,
		armF: arm(q.shF, q.elF, 1),
		armN: arm(q.shN, q.elN, -0.5),
	};
};
