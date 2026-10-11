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
	// His elbows tucked in under the ball (0 to 1): a jumper's arms come up
	// in front of his face, not out wide past his head.
	tuck: number;
	// On a jumper, how far his guide hand has come off the ball (0 to 1): on
	// its side as he brings it up, then still in the air beside it while the
	// shooting hand lets it go alone.
	free: number;
	// His toes pointed down (0 to 1): up off the floor in a jump.
	toe: number;
	// And each foot on its own (added to that): up on its toes, pushing off
	// a stride.
	toeN: number;
	toeF: number;
	// His hips and legs turned under him from the way he faces (degrees,
	// round to his left positive): going one way while he looks another.
	legs: number;
	// His shoulders turned on his hips about his spine (degrees, round to his
	// left positive), and his upper body tipped to his side (degrees, his left
	// shoulder down positive): a crossover's dip, an arm wrapped behind him.
	twist: number;
	tilt: number;
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
	tuck: 0,
	free: 0,
	toe: 0,
	toeN: 0,
	toeF: 0,
	legs: 0,
	twist: 0,
	tilt: 0,
};
const pose = (o: Partial<Pose>): Pose => ({ ...BASE, ...o });

// The same pose the other way round: his left doing what his right did.
const SIDES: [keyof Pose, keyof Pose][] = [
	["hipN", "hipF"],
	["kneeN", "kneeF"],
	["shN", "shF"],
	["elN", "elF"],
	["abN", "abF"],
	["wrN", "wrF"],
	["toeN", "toeF"],
];
export const mirror = (q: Pose): Pose => {
	const out = { ...q, twist: -q.twist, tilt: -q.tilt, legs: -q.legs };
	for (const [n, f] of SIDES) {
		out[n] = q[f];
		out[f] = q[n];
	}
	return out;
};

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
	// Standing easy but live, the way a man spaced out on the floor does:
	// a little sunk at the knees, feet a shade wider than his hips and one
	// a half step ahead, leaning into it, hands loose at his hips.
	ready: pose({
		hipN: 2,
		kneeN: 26,
		hipF: 14,
		kneeF: 28,
		lean: 10,
		shN: 14,
		elN: 30,
		shF: 16,
		elF: 34,
		abN: 14,
		abF: 16,
		wide: 0.36,
	}),
	// Down in a defensive stance: one hand up at the ball, chest high and
	// bent, the other low and out to the side in the lane - not both arms
	// held out flat like a scarecrow.
	stance: pose({
		hipN: 30,
		kneeN: 62,
		hipF: 36,
		kneeF: 66,
		shN: 22,
		elN: 40,
		abN: 40,
		wrN: -6,
		shF: 58,
		elF: 56,
		abF: 18,
		wrF: 16,
		lean: 18,
		wide: 0.85,
	}),
	// The ball in both hands, live: knees bent and weight forward, ready
	// to go with it - not stood up straight.
	hold: pose({
		hipN: -4,
		kneeN: 40,
		hipF: 22,
		kneeF: 42,
		shN: 36,
		elN: 92,
		shF: 30,
		elF: 98,
		lean: 14,
		wide: 0.3,
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
	// Dribbling where he stands, sizing his man up: down in his legs, the
	// other arm out, bent, between the ball and his man.
	sizeUp: pose({
		hipN: 2,
		kneeN: 58,
		hipF: 30,
		kneeF: 60,
		lean: 17,
		wide: 0.48,
		shN: 32,
		elN: 18,
		shF: 26,
		elF: 74,
		abF: 34,
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
	// A shot fake: the ball up on his shooting hand toward the set point, as
	// if to shoot - his legs straightening, but his heels down.
	fakeUp: pose({
		hipN: -2,
		kneeN: 16,
		hipF: 6,
		kneeF: 18,
		lean: 3,
		wide: 0.3,
		shN: 116,
		elN: 56,
		abN: -14,
		wrN: 70,
		shF: 108,
		elF: 64,
		abF: -10,
		wrF: 20,
		tuck: 1,
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
	| "jog"
	| "sprint"
	| "dribble"
	| "dribbleWalk"
	| "back"
	| "walk"
	| "carry"
	| "drift"
	| "shuffle"
	| "closeout";
// A defensive slide: how far he goes each step-and-close (feet), and how
// far apart his feet are, closed up (see "shuffle").
const SLIDE_STRIDE = 1.8;
const SLIDE_NARROW = 0.35;
// HOW HE RUNS - AND WALKS.
//
// The way a body really goes: each foot planted while it is down, going
// back under him exactly as fast as he goes forward, so it never skates -
// rolling up onto its toes to push off - then folding up behind him, the
// knee driven through and the foot reached out and pulled back to land. His
// hips sink over each planted foot and spring up off it (on a run, up off
// the floor between steps), and his arms swing against his legs. Worked out
// on a typical player's legs: anybody else's are the same in proportion.
type Gait = {
	// Feet a stride (a step of each foot): the anim's.
	stride: number;
	// How much of a stride each foot is down: over half a walk (a foot always
	// down), under half a run.
	duty: number;
	// How far ahead of his hips a foot lands, as a share of how far it goes
	// back under him while it is down.
	ahead: number;
	// How high his hips ride (feet - a typical player's stand 2.93 up), and
	// how far they rise and fall each step: lowest over the planted foot, or
	// - negative, walking - highest, vaulting over it.
	hips: number;
	bob: number;
	// How far through its time down a heel comes up, and how far up onto its
	// toes the foot has rolled as it leaves the floor (degrees).
	flat: number;
	roll: number;
	// Through the swing, keys of how far through it, hip and knee - the first
	// key's hip and knee from where they were as the foot left the floor.
	swing: [number, number, number][];
	// Forward lean; how far the arms swing forward and back (degrees, about
	// his side), the elbow's bend and how much more it bends in front; his
	// shoulders turned with them; and his feet's spread.
	lean: number;
	armFwd: number;
	armBack: number;
	elbow: number;
	pump: number;
	twist: number;
	wide: number;
};
type GaitMode =
	| "run"
	| "jog"
	| "sprint"
	| "dribble"
	| "dribbleWalk"
	| "walk"
	| "carry"
	| "drift";
const GAITS: Record<GaitMode, Gait> = {
	run: {
		stride: 8.6,
		duty: 0.3,
		ahead: 0.32,
		hips: 2.74,
		bob: 0.16,
		flat: 0.45,
		roll: 40,
		swing: [
			[0.12, -4, 32],
			[0.4, 6, 112],
			[0.62, 36, 104],
			[0.83, 48, 48],
		],
		lean: 13,
		armFwd: 44,
		armBack: 34,
		elbow: 84,
		pump: 18,
		twist: 6,
		wide: -0.04,
	},
	sprint: {
		stride: 12.4,
		duty: 0.25,
		ahead: 0.28,
		hips: 2.72,
		bob: 0.14,
		flat: 0.4,
		roll: 46,
		swing: [
			[0.12, -4, 40],
			[0.4, 12, 130],
			[0.62, 56, 116],
			[0.83, 66, 56],
		],
		lean: 17,
		armFwd: 56,
		armBack: 46,
		elbow: 84,
		pump: 16,
		twist: 8,
		wide: -0.06,
	},
	jog: {
		stride: 6.2,
		duty: 0.36,
		ahead: 0.36,
		hips: 2.78,
		bob: 0.14,
		flat: 0.5,
		roll: 32,
		swing: [
			[0.14, -3, 26],
			[0.42, 6, 82],
			[0.66, 30, 62],
			[0.86, 32, 24],
		],
		lean: 9,
		armFwd: 32,
		armBack: 24,
		elbow: 82,
		pump: 14,
		twist: 5,
		wide: -0.02,
	},
	dribble: {
		stride: 8.2,
		duty: 0.32,
		ahead: 0.34,
		hips: 2.62,
		bob: 0.14,
		flat: 0.45,
		roll: 36,
		swing: [
			[0.12, -4, 34],
			[0.4, 8, 104],
			[0.64, 40, 94],
			[0.85, 46, 42],
		],
		lean: 19,
		armFwd: 44,
		armBack: 34,
		elbow: 84,
		pump: 18,
		twist: 4,
		wide: 0.02,
	},
	dribbleWalk: {
		stride: 4.4,
		duty: 0.6,
		ahead: 0.42,
		hips: 2.6,
		bob: -0.06,
		flat: 0.55,
		roll: 26,
		swing: [
			[0.12, 4, 18],
			[0.36, 16, 66],
			[0.66, 32, 50],
			[0.88, 32, 30],
		],
		lean: 15,
		armFwd: 16,
		armBack: 12,
		elbow: 40,
		pump: 8,
		twist: 3,
		wide: 0.16,
	},
	walk: {
		stride: 4.8,
		duty: 0.6,
		ahead: 0.4,
		hips: 2.86,
		bob: -0.1,
		flat: 0.5,
		roll: 32,
		swing: [
			[0.12, 4, 20],
			[0.36, 12, 62],
			[0.66, 28, 34],
			[0.88, 27, 8],
		],
		lean: 4,
		armFwd: 18,
		armBack: 16,
		elbow: 22,
		pump: 10,
		twist: 3,
		wide: 0.04,
	},
	carry: {
		stride: 4.6,
		duty: 0.6,
		ahead: 0.4,
		hips: 2.84,
		bob: -0.08,
		flat: 0.5,
		roll: 30,
		swing: [
			[0.12, 4, 20],
			[0.36, 12, 62],
			[0.66, 28, 36],
			[0.88, 27, 10],
		],
		lean: 6,
		armFwd: 0,
		armBack: 0,
		elbow: 0,
		pump: 0,
		twist: 0,
		wide: 0.04,
	},
	drift: {
		stride: 3.8,
		duty: 0.6,
		ahead: 0.42,
		hips: 2.66,
		bob: -0.05,
		flat: 0.55,
		roll: 24,
		swing: [
			[0.12, 4, 18],
			[0.36, 16, 62],
			[0.66, 30, 48],
			[0.88, 30, 30],
		],
		lean: 9,
		armFwd: 0,
		armBack: 0,
		elbow: 0,
		pump: 0,
		twist: 0,
		wide: 0.1,
	},
};
export const isGait = (mode: string): mode is GaitMode => mode in GAITS;

// A leg through its stride (`phi`, 0 as the foot lands): hip and knee, how
// high the ankle is off the floor (feet), and the foot rolled onto its toes
// (0 to 1, as `toe`).
type Leg = { hip: number; knee: number; z: number; toe: number };
type GaitFit = {
	b: Body;
	// Where the leg is as the foot leaves the floor and as it lands, and how
	// fast its angles are going there (degrees through the whole swing).
	off: Leg;
	land: Leg;
	hipKeys: [number, number][];
	kneeKeys: [number, number][];
	hipOut: number;
	kneeOut: number;
	hipIn: number;
	kneeIn: number;
};
// The foot pointed down as `toe` 1 (see skeleton).
const POINT = 55;
// Hip and knee to put the ankle `f` ahead of the hip and `u` up from it
// (negative, below).
const legTo = (b: Body, f: number, u: number) => {
	const T = b.thigh;
	const S = b.shin;
	const d = Math.max(1e-6, Math.min(Math.hypot(f, u), (T + S) * 0.9995));
	const inner = Math.acos(
		Math.max(-1, Math.min(1, (T * T + S * S - d * d) / (2 * T * S))),
	);
	const beta = Math.acos(
		Math.max(-1, Math.min(1, (T * T + d * d - S * S) / (2 * T * d))),
	);
	return {
		hip: (Math.atan2(f, -u) + beta) / RAD,
		knee: 180 - inner / RAD,
	};
};
// The hips' height through a leg's stride (the same for both legs: a step
// is half a stride).
const hipsAt = (g: Gait, phi: number) =>
	g.hips - (g.bob / 2) * Math.cos(2 * Math.PI * (2 * phi - g.duty));
// A foot while it is down: going back under him, and late on up onto its
// toes - the toes staying put on the floor as the heel comes up.
const plantedAt = (g: Gait, b: Body, phi: number): Leg => {
	const toe = b.foot * 0.72;
	// (The heel comes up so far before the foot need point - see skeleton.)
	const lift = b.ankleR - b.ankleH * 0.45;
	const drop = toe - b.ankleH * 0.55;
	const r = Math.max(0, Math.min(1, (phi / g.duty - g.flat) / (1 - g.flat)));
	const z = (lift + drop * Math.sin(g.roll * RAD)) * r * r;
	const point = Math.asin(Math.min(1, Math.max(0, z - lift) / drop));
	const f =
		g.ahead * g.stride * g.duty - g.stride * phi + toe * (1 - Math.cos(point));
	return {
		...legTo(b, f, z + b.ankleH - hipsAt(g, phi)),
		z,
		toe: point / RAD / POINT,
	};
};
const fits = new Map<GaitMode, GaitFit>();
const gaitFit = (mode: GaitMode): GaitFit => {
	let fit = fits.get(mode);
	if (!fit) {
		const g = GAITS[mode];
		const b = bodyOf();
		const off = plantedAt(g, b, g.duty);
		const land = plantedAt(g, b, 0);
		const e = 1e-4;
		const before = plantedAt(g, b, g.duty - e);
		const after = plantedAt(g, b, e);
		const span = (1 - g.duty) / e;
		const [first, ...rest] = g.swing;
		fit = {
			b,
			off,
			land,
			hipKeys: [
				[0, off.hip],
				[first![0], off.hip + first![1]],
				...rest.map((k): [number, number] => [k[0], k[1]]),
				[1, land.hip],
			],
			kneeKeys: [
				[0, off.knee],
				[first![0], off.knee + first![2]],
				...rest.map((k): [number, number] => [k[0], k[2]]),
				[1, land.knee],
			],
			hipOut: (off.hip - before.hip) * span,
			kneeOut: (off.knee - before.knee) * span,
			hipIn: (after.hip - land.hip) * span,
			kneeIn: (after.knee - land.knee) * span,
		};
		fits.set(mode, fit);
	}
	return fit;
};
// Smoothly through keys (Catmull-Rom), going `out` at the first and `into`
// the last as fast as given.
const spline = (
	keys: [number, number][],
	s: number,
	out: number,
	into: number,
): number => {
	let i = 0;
	while (i < keys.length - 2 && s > keys[i + 1]![0]) {
		i++;
	}
	const [s0, v0] = keys[i]!;
	const [s1, v1] = keys[i + 1]!;
	const slope = (j: number) =>
		j === 0
			? out
			: j === keys.length - 1
				? into
				: (keys[j + 1]![1] - keys[j - 1]![1]) /
					(keys[j + 1]![0] - keys[j - 1]![0]);
	const h = s1 - s0;
	const t = (s - s0) / h;
	const t2 = t * t;
	const t3 = t2 * t;
	return (
		(2 * t3 - 3 * t2 + 1) * v0 +
		(t3 - 2 * t2 + t) * h * slope(i) +
		(-2 * t3 + 3 * t2) * v1 +
		(t3 - t2) * h * slope(i + 1)
	);
};
const legAt = (mode: GaitMode, phi: number): Leg => {
	const g = GAITS[mode];
	const fit = gaitFit(mode);
	if (phi < g.duty) {
		return plantedAt(g, fit.b, phi);
	}
	const s = (phi - g.duty) / (1 - g.duty);
	const hip = spline(fit.hipKeys, s, fit.hipOut, fit.hipIn);
	const knee = spline(fit.kneeKeys, s, fit.kneeOut, fit.kneeIn);
	const { b } = fit;
	const u =
		-b.thigh * Math.cos(hip * RAD) - b.shin * Math.cos((hip - knee) * RAD);
	// The foot pointed as it came off the floor, then square with his shin
	// - hanging toes down as his heel comes up behind him, level to land.
	const w = Math.min(1, s / 0.2);
	const square = Math.max(0, Math.min(1, (knee - hip + 10) / POINT));
	return {
		hip,
		knee,
		z: hipsAt(g, phi) + u - b.ankleH,
		toe: fit.off.toe + (square - fit.off.toe) * w,
	};
};
const frac = (x: number) => x - Math.floor(x);
// How high off the floor he is (feet), `ph` through a stride: his lower
// ankle's height (see skeleton, which puts it on the floor).
const gaitLift = (mode: GaitMode, ph: number): number =>
	Math.min(legAt(mode, frac(ph)).z, legAt(mode, frac(ph + 0.5)).z);
const gaitPose = (mode: GaitMode, ph: number): Pose => {
	const g = GAITS[mode];
	const r = legAt(mode, frac(ph));
	const l = legAt(mode, frac(ph + 0.5));
	// How far forward his right arm is (-1 to 1): back as his right foot
	// lands, forward as his left does - a beat behind his legs.
	const a = -Math.cos(2 * Math.PI * (ph - 0.03));
	const mid = (g.armFwd - g.armBack) / 2;
	const amp = (g.armFwd + g.armBack) / 2;
	return pose({
		hipN: r.hip,
		kneeN: r.knee,
		hipF: l.hip,
		kneeF: l.knee,
		toeN: r.toe,
		toeF: l.toe,
		lean: g.lean,
		wide: g.wide,
		shN: mid + amp * a,
		elN: g.elbow + g.pump * a,
		abN: 10 - 4 * a,
		shF: mid - amp * a,
		elF: g.elbow - g.pump * a,
		abF: 10 + 4 * a,
		twist: g.twist * a,
		plant: 0,
	});
};

const stride = (ph: number, mode: RunMode): Pose => {
	const a = Math.sin(2 * Math.PI * ph);
	const c = Math.cos(2 * Math.PI * ph);
	if (mode === "shuffle") {
		// A defensive slide, a push step: down in his stance, the lead foot
		// stepping out while the trail foot stays planted, then the trail foot
		// closing up behind it - never crossing, never touching. Over half a
		// stride his feet spread as far as he goes (SLIDE_STRIDE), so each
		// sits still on the floor while the other moves; his knees straighten
		// a little as they spread, so his hips stay level the whole way.
		const spread = ph < 0.5 ? ph * 2 : 2 - ph * 2;
		const knee = 74 - 21 * spread * spread;
		// His hands active: one up at the ball, the other low and out in the
		// lane, both working a little with each push.
		return pose({
			hipN: 30,
			hipF: 34,
			kneeN: knee,
			kneeF: knee + 2,
			lean: 18,
			shN: 22 + 6 * spread,
			elN: 38,
			abN: 40 + 6 * spread,
			wrN: -6,
			shF: 54 + 6 * spread,
			elF: 54,
			abF: 18 + 4 * spread,
			wrF: 14,
			wide: SLIDE_NARROW + (SLIDE_STRIDE / 2) * spread,
		});
	}
	if (mode === "closeout") {
		// Closing out on a shooter: short, choppy steps to break down, sat
		// down in his legs, his left hand high at the shot - face to face, the
		// hand on the ball's side, reaching with each chop - and the right
		// down at his hip and out to the side, a hand on the drive.
		return pose({
			hipN: 24 + 10 * a,
			hipF: 30 - 10 * a,
			kneeN: 54 + 12 * Math.max(0, c),
			kneeF: 56 + 12 * Math.max(0, -c),
			lean: 16,
			shN: 14 + 5 * a,
			elN: 30,
			abN: 50,
			wrN: -12,
			shF: 152 + 4 * c,
			elF: 18,
			abF: 16,
			wide: 0.6,
		});
	}
	if (isGait(mode)) {
		const q = gaitPose(mode, ph);
		// How far forward his left arm swings (-1 to 1).
		const left = Math.cos(2 * Math.PI * (ph - 0.03));
		if (mode === "dribble") {
			// Pushing it up the floor: the other arm still swinging with his
			// stride, bent and held out a little from him. (The dribbling arm
			// rides the ball - see dribbleArm.)
			return {
				...q,
				shN: 30,
				elN: 30,
				abN: 18,
				shF: -2 + 22 * left,
				elF: 86 + 8 * left,
				abF: 24,
			};
		}
		if (mode === "dribbleWalk") {
			// Working it, a step at a time: the other arm up, bent, between
			// the ball and his man.
			return {
				...q,
				shN: 30,
				elN: 30,
				abN: 18,
				shF: 30 + 4 * left,
				elF: 58,
				abF: 38,
			};
		}
		if (mode === "carry") {
			return { ...q, shN: 36, elN: 92, shF: 30, elF: 98 };
		}
		if (mode === "drift") {
			// Off the ball, sliding along the arc: hands up ready for it.
			return {
				...q,
				shN: 42,
				elN: 58,
				shF: 40,
				elF: 60,
				abN: 16,
				abF: 16,
				wrN: 20,
				wrF: 20,
			};
		}
		return q;
	}
	// A defensive slide / backpedal: low, short steps, hands active - one
	// up at the ball, the other low and out.
	return pose({
		hipN: 10 - 18 * a,
		hipF: 30 + 18 * a,
		kneeN: 50 + 16 * Math.max(0, -c),
		kneeF: 54 + 16 * Math.max(0, c),
		lean: 18,
		shN: 26 + 4 * a,
		elN: 40,
		abN: 36,
		shF: 56 - 4 * a,
		elF: 52,
		abF: 18,
		wide: 0.4,
	});
};

// Mid-stride, his feet are where his stride puts them - not planted under
// him.
const runPose = (ph: number, mode: RunMode): Pose => ({
	...stride(ph, mode),
	plant: 0,
});

// A PURE JUMPER.
//
// Down into his legs with the ball at his hip, then up through it all in one
// piece - legs and ball together, the ball up the middle of him on the
// shooting hand, the guide hand on its side - to a set point over his eyes:
// the elbow under the ball, the wrist cocked back. The guide hand comes off.
// He lets it go just before the top of his jump, the arm up and out at the
// rim, the wrist snapping through - and holds it there, a gooseneck, all the
// way down.
//
// When, through the act: off the floor, back down on it, and the ball gone.
// (Off the floor for 0.59 of it: in a one-second act, as long as gravity
// keeps a jump of about a foot and a half in the air.)
export const JUMPER = { off: 0.28, land: 0.87, release: 0.55 };
// His arms through it - anything a key leaves out goes on as it was.
const JUMPER_ARMS: [number, Partial<Pose>][] = [
	[0, { shN: 30, elN: 100, shF: 28, elF: 104 }],
	// The dip: the ball at his hip.
	[0.1, { shN: -6, elN: 112, shF: -8, elF: 116, abN: 8, abF: 8, wrN: 20 }],
	// Up the middle of him, the shooting hand getting under it.
	[0.25, { shN: 72, elN: 108, abN: -9, wrN: 52, shF: 62, elF: 112, abF: -4 }],
	[0.32, { shN: 89, elN: 78, abN: -18, wrN: 60, shF: 110, elF: 70, abF: -10 }],
	// The set point, the guide hand on the side of the ball.
	[
		0.4,
		{
			shN: 123,
			elN: 46,
			abN: -14,
			wrN: 72,
			shF: 118,
			elF: 56,
			abF: -10,
			wrF: 25,
			free: 0,
		},
	],
	[0.47, { shN: 126, elN: 42 }],
	// Up and out at the rim - the guide hand still on it until the last
	// instant - and gone.
	[0.51, { shN: 139, elN: 21, wrN: 50, free: 0 }],
	[0.55, { shN: 153, elN: 1, abN: -12, wrN: 12, free: 1 }],
	// The snap, and the gooseneck held.
	[0.6, { shN: 154, elN: 2, wrN: -85 }],
	[0.66, { elN: 3, wrN: -108, shF: 117, elF: 57, wrF: 22 }],
	[0.84, { shN: 151, elN: 5, shF: 96, elF: 64, abF: -2, wrF: 12 }],
	// Down: the guide hand drops to his side; the shooting hand stays up.
	[0.92, { shN: 148, elN: 6, wrN: -104, shF: 18, elF: 52, abF: 12, wrF: 0 }],
	[1, { shN: 146, elN: 7, shF: 6, elF: 30, abF: 12 }],
];
const ARM_FIELDS: (keyof Pose)[] = [
	"shN",
	"elN",
	"shF",
	"elF",
	"abN",
	"abF",
	"wrN",
	"wrF",
	"tuck",
	"free",
];
// Those arms over these legs, each on its own keys.
const jumper = (legs: [number, Partial<Pose>][]): [number, Pose][] => {
	let carry: Partial<Pose> = {};
	const arms = JUMPER_ARMS.map(([u, a]): [number, Pose] => {
		carry = {
			...carry,
			tuck: u < 0.2 ? 0 : u < 0.3 ? 0.65 : 1,
			...a,
		};
		return [u, pose(carry)];
	});
	const feet = legs.map(([u, l]): [number, Pose] => [u, pose(l)]);
	const times = [...new Set([...arms, ...feet].map(([u]) => u))].sort(
		(a, b) => a - b,
	);
	return times.map((u) => {
		const q = { ...keyed(feet, u) };
		const a = keyed(arms, u);
		for (const k of ARM_FIELDS) {
			q[k] = a[k];
		}
		return [u, q];
	});
};
// A jump shot: straight up and straight down, his toes pointed in the air,
// landing where he took off.
const AIR = { hipN: 2, hipF: -2, wide: 0.18, toe: 1 };
const JUMP_SHOT = jumper([
	[0, { hipN: -2, kneeN: 34, hipF: 4, kneeF: 36, lean: 8, wide: 0.2 }],
	[0.1, { hipN: 0, kneeN: 56, hipF: -2, kneeF: 58, lean: 13, wide: 0.22 }],
	[0.24, { hipN: 2, kneeN: 18, hipF: -2, kneeF: 20, lean: 4, wide: 0.2 }],
	[0.28, { ...AIR, kneeN: 7, kneeF: 9, lean: 2, toe: 0 }],
	[0.34, { ...AIR, kneeN: 6, kneeF: 8, lean: 1 }],
	[0.8, { ...AIR, kneeN: 8, kneeF: 11, lean: 1 }],
	[0.87, { ...AIR, hipN: 1, kneeN: 12, kneeF: 14, lean: 2, toe: 0 }],
	[0.93, { hipN: 0, kneeN: 32, hipF: -2, kneeF: 34, lean: 6, wide: 0.22 }],
	[1, { hipN: -2, kneeN: 26, hipF: 2, kneeF: 28, lean: 5, wide: 0.2 }],
]);
// A fadeaway: the same arms, but leaning back away from his man as he
// rises, his legs out in front of him, landing a step back.
const BACK = { plant: 0.3, toe: 1 };
const FADEAWAY = jumper([
	[0, { hipN: -2, kneeN: 34, hipF: 4, kneeF: 36, lean: 8, wide: 0.2 }],
	[0.1, { hipN: 0, kneeN: 56, hipF: -2, kneeF: 58, lean: 11, wide: 0.22 }],
	[0.24, { hipN: 6, kneeN: 18, hipF: 10, kneeF: 22, lean: -2, wide: 0.2 }],
	[0.28, { hipN: 10, kneeN: 12, hipF: 16, kneeF: 20, lean: -5 }],
	[0.36, { hipN: 20, kneeN: 18, hipF: 30, kneeF: 34, lean: -12, ...BACK }],
	[0.5, { hipN: 26, kneeN: 22, hipF: 38, kneeF: 44, lean: -16, ...BACK }],
	[0.7, { hipN: 24, kneeN: 23, hipF: 36, kneeF: 45, lean: -14, ...BACK }],
	[
		0.82,
		{
			hipN: 12,
			kneeN: 18,
			hipF: 20,
			kneeF: 30,
			lean: -7,
			plant: 0.5,
			toe: 0.6,
		},
	],
	[0.87, { hipN: 2, kneeN: 16, hipF: 8, kneeF: 22, lean: -2 }],
	[0.93, { hipN: -4, kneeN: 34, hipF: 10, kneeF: 38, lean: 2, wide: 0.24 }],
	[1, { hipN: -4, kneeN: 28, hipF: 6, kneeF: 30, lean: 4, wide: 0.22 }],
]);
// A free throw: the same stroke off the floor - a bend of the knees, up
// straight through them, the follow-through held.
const SET_SHOT = jumper([
	[0, { kneeN: 24, kneeF: 26, lean: 5, wide: 0.24 }],
	[0.12, { kneeN: 44, kneeF: 46, lean: 9, wide: 0.24 }],
	[0.3, { kneeN: 14, kneeF: 16, lean: 3, wide: 0.24 }],
	[0.42, { kneeN: 4, kneeF: 6, lean: 0, wide: 0.24 }],
	[0.75, { kneeN: 6, kneeF: 8, lean: 0, wide: 0.24 }],
	[1, { kneeN: 10, kneeF: 12, lean: 1, wide: 0.24 }],
]);

// DRIBBLE MOVES - one bounce each, keyed through it: 0 the ball in the hand
// it leaves, about 0.42 on the floor, 1 up in the other hand. Written for the
// ball leaving his right hand; the left-handed ones are their mirror images.
// Low and wide, the off arm up as a bar - and the whole of him in it, not
// just the ball changing hands. Each one's hands are placed so the ball's
// path (evaluate.ts) goes round his legs, never through them.
const MOVE_STANCE: Partial<Pose> = {
	hipN: 24,
	kneeN: 56,
	hipF: 28,
	kneeF: 58,
	lean: 20,
	wide: 0.55,
};
// A crossover: his right shoulder dipped to sell the right, the ball pushed
// hard across in front of his knees, low, everything shifting left as it
// comes up into his left hand - and his right arm up as the bar.
const CROSS_FRONT: [number, Pose][] = [
	[
		0,
		pose({
			...MOVE_STANCE,
			hipN: 18,
			kneeN: 66,
			tilt: -13,
			twist: -17,
			shN: 25,
			elN: 46,
			abN: 7,
			wrN: 22,
			shF: 50,
			elF: 72,
			abF: 30,
		}),
	],
	[
		0.12,
		pose({
			...MOVE_STANCE,
			hipN: 22,
			kneeN: 60,
			tilt: -6,
			twist: -7,
			shN: 38,
			elN: 11,
			abN: 7,
			wrN: -30,
			shF: 46,
			elF: 64,
			abF: 26,
		}),
	],
	[
		0.42,
		pose({
			...MOVE_STANCE,
			tilt: 3,
			twist: 5,
			shN: 32,
			elN: 2,
			abN: -7,
			wrN: -20,
			shF: 46,
			elF: 3,
			abF: 10,
			wrF: 10,
		}),
	],
	[
		0.75,
		pose({
			...MOVE_STANCE,
			hipF: 22,
			kneeF: 62,
			tilt: 8,
			twist: 10,
			shN: 32,
			elN: 46,
			abN: 6,
			shF: 41,
			elF: 0,
			abF: 13,
			wrF: 20,
		}),
	],
	[
		1,
		pose({
			...MOVE_STANCE,
			hipF: 18,
			kneeF: 66,
			tilt: 11,
			twist: 15,
			shN: 46,
			elN: 66,
			abN: 28,
			shF: 29,
			elF: 38,
			abF: 10,
			wrF: 24,
		}),
	],
];
// Between his legs: his left foot well out in front, down low, the ball
// pushed from beside his right knee through the gap between his legs and
// taken in his left hand behind that front leg.
const LEGS_STANCE: Partial<Pose> = {
	hipN: -16,
	kneeN: 58,
	hipF: 42,
	kneeF: 64,
	lean: 22,
	wide: 0.75,
};
const CROSS_LEGS: [number, Pose][] = [
	[
		0,
		pose({
			...LEGS_STANCE,
			hipF: 34,
			kneeF: 58,
			twist: 6,
			shN: -26,
			elN: 68,
			abN: 30,
			wrN: 22,
			shF: 50,
			elF: 72,
			abF: 30,
		}),
	],
	[
		0.14,
		pose({
			...LEGS_STANCE,
			twist: 9,
			shN: 5,
			elN: 18,
			abN: 26,
			wrN: -34,
			shF: 44,
			elF: 60,
			abF: 26,
		}),
	],
	[
		0.42,
		pose({
			...LEGS_STANCE,
			twist: 10,
			tilt: 2,
			shN: 2,
			elN: 2,
			abN: 15,
			wrN: -20,
			shF: -28,
			elF: 0,
			abF: 30,
			wrF: 8,
		}),
	],
	[
		0.72,
		pose({
			...LEGS_STANCE,
			twist: 8,
			tilt: 5,
			shN: 30,
			elN: 44,
			abN: 16,
			shF: -34,
			elF: 2,
			abF: 35,
			wrF: 20,
		}),
	],
	[
		1,
		pose({
			...LEGS_STANCE,
			twist: 6,
			tilt: 5,
			shN: 46,
			elN: 66,
			abN: 28,
			shF: -38,
			elF: 74,
			abF: 29,
			wrF: 24,
		}),
	],
];
// Behind his back: the ball drawn back along his right hip on the outside
// of his hand, his right shoulder turned back so the arm can wrap behind his
// seat and push it toward his left heel; it bounces behind him by that foot
// and comes up into his left hand at his side.
const BACK_STANCE: Partial<Pose> = { ...MOVE_STANCE, lean: 14, wide: 0.4 };
const CROSS_BACK: [number, Pose][] = [
	[
		0,
		pose({
			...BACK_STANCE,
			twist: -6,
			shN: -2,
			elN: 56,
			abN: 22,
			wrN: 14,
			shF: 50,
			elF: 72,
			abF: 30,
		}),
	],
	[
		0.1,
		pose({
			...BACK_STANCE,
			twist: -14,
			shN: -27,
			elN: 49,
			abN: 24,
			shF: 48,
			elF: 70,
			abF: 30,
		}),
	],
	[
		0.2,
		pose({
			...BACK_STANCE,
			twist: -22,
			tilt: -3,
			shN: -27,
			elN: 7,
			abN: 13,
			wrN: -20,
			shF: 44,
			elF: 64,
			abF: 28,
		}),
	],
	[
		0.42,
		pose({
			...BACK_STANCE,
			twist: -16,
			tilt: -2,
			shN: -35,
			elN: 1,
			abN: -5,
			wrN: -30,
			shF: -38,
			elF: 32,
			abF: 29,
			wrF: 10,
		}),
	],
	[
		0.62,
		pose({
			...BACK_STANCE,
			twist: -6,
			tilt: 2,
			shN: -38,
			elN: 78,
			abN: 14,
			shF: -26,
			elF: 0,
			abF: 29,
			wrF: 16,
		}),
	],
	[
		0.8,
		pose({
			...BACK_STANCE,
			twist: 2,
			tilt: 4,
			shN: 30,
			elN: 56,
			abN: 24,
			shF: -35,
			elF: 58,
			abF: 31,
			wrF: 20,
		}),
	],
	[
		1,
		pose({
			...BACK_STANCE,
			twist: 6,
			tilt: 6,
			shN: 46,
			elN: 66,
			abN: 28,
			shF: -17,
			elF: 77,
			abF: 24,
			wrF: 24,
		}),
	],
];
// Where each move lets go of the ball (how far through its bounce) and
// where the ball hits the floor - in his frame, as a share of his height, for
// the ball leaving his right hand (his left mirrors it): across in front of
// his knees, or on the move further out, past his stride; in the gap between
// his legs; behind him by the heel of the foot on the side it is going to.
// The keys above put his hands round these, so the ball's path between them
// (evaluate.ts) goes round his legs, never through them.
export const MOVE_BALL: Record<
	DribbleMove,
	{ letGo: number; f: number; s: number }
> = {
	front: { letGo: 0.12, f: 0.22, s: 0 },
	legs: { letGo: 0.14, f: 0.08, s: 0.064 },
	back: { letGo: 0.2, f: -0.143, s: 0.08 },
};
const ON_THE_MOVE_F = 0.32;
// Where a move's bounce hits the floor (feet, his frame): the ball going to
// `to`, him standing through it or not.
export const moveFloor = (
	b: Body,
	move: DribbleMove,
	to: Hand,
	still: boolean,
): { f: number; s: number } => {
	const m = MOVE_BALL[move];
	return {
		f: (move === "front" && !still ? ON_THE_MOVE_F : m.f) * b.H,
		s: (to === "L" ? 1 : -1) * m.s * b.H,
	};
};

const mirrorKeys = (keys: [number, Pose][]): [number, Pose][] =>
	keys.map(([u, q]) => [u, mirror(q)]);

// Every finish at the rim off one foot starts and ends the same way: the
// last stride in, and coming back down - the knee letting down, the arm
// following through past his face.
const LAYUP_START = pose({
	hipN: 10,
	kneeN: 40,
	hipF: -20,
	kneeF: 30,
	shN: 45,
	elN: 90,
	shF: 40,
	elF: 95,
	lean: 14,
});
const LAYUP_DOWN: [number, Pose][] = [
	[
		0.84,
		pose({
			hipN: 40,
			kneeN: 56,
			hipF: -4,
			kneeF: 22,
			shN: 132,
			elN: 40,
			shF: 44,
			elF: 54,
			lean: 3,
			wrN: -40,
			toe: 0.6,
		}),
	],
	[
		0.93,
		pose({
			hipN: 8,
			kneeN: 34,
			hipF: 6,
			kneeF: 36,
			shN: 66,
			elN: 62,
			shF: 24,
			elF: 40,
			lean: 8,
			wrN: -10,
		}),
	],
	[
		1,
		pose({
			hipN: -2,
			kneeN: 34,
			hipF: 10,
			kneeF: 38,
			shN: 30,
			elN: 40,
			shF: 16,
			elF: 32,
			lean: 8,
		}),
	],
];
// Up off the left foot, the right knee driving.
const LAYUP_KNEE = { hipN: 94, kneeN: 102, hipF: -16, kneeF: 24, toe: 1 };

// The finishes at the rim besides the plain layup (a layup's words cover
// all of them).
export const LAYUPS = new Set<string>([
	"layup",
	"fingerRoll",
	"powerLayup",
	"scoop",
]);

// "loop" anims play on the clock, "cycle" ones on distance covered (so feet
// never skate), "act" ones across their own span from 0 to 1.
type Anim =
	| { kind: "loop"; n: number; fps: number; pose: (i: number) => Pose }
	| { kind: "cycle"; n: number; stride: number; pose: (i: number) => Pose }
	| { kind: "act"; n: number; keys: [number, Pose][] };

export const ANIMS = {
	// Never still: he settles down into his knees and back up, his
	// shoulders swaying over his hips, his hands drifting - slowly.
	ready: {
		kind: "loop",
		n: 4,
		fps: 1.25,
		pose: (i) =>
			[
				P.ready,
				{
					...P.ready,
					hipN: 6,
					kneeN: 32,
					hipF: 18,
					kneeF: 34,
					lean: 12,
					tilt: 2.5,
					twist: 5,
					shN: 10,
					elN: 26,
					shF: 20,
					elF: 40,
				},
				{ ...P.ready, kneeN: 24, kneeF: 26, lean: 9, shN: 16, elN: 36 },
				{
					...P.ready,
					hipN: 5,
					kneeN: 31,
					hipF: 17,
					kneeF: 33,
					lean: 11,
					tilt: -2.5,
					twist: -5,
					shN: 18,
					elN: 38,
					shF: 12,
					elF: 28,
				},
			][i]!,
	},
	// Live in his stance: hands working - one up into the lane, then the
	// other - and his weight sinking into his heels and back up onto his toes.
	stance: {
		kind: "loop",
		n: 4,
		fps: 2,
		pose: (i) =>
			[
				P.stance,
				{
					...P.stance,
					hipN: 34,
					kneeN: 68,
					hipF: 40,
					kneeF: 72,
					shN: 40,
					elN: 52,
					abN: 30,
					shF: 46,
					elF: 50,
					twist: 4,
				},
				{ ...P.stance, hipN: 26, kneeN: 58, hipF: 32, kneeF: 62 },
				{
					...P.stance,
					hipN: 33,
					kneeN: 67,
					hipF: 39,
					kneeF: 71,
					shN: 18,
					elN: 34,
					abN: 44,
					shF: 68,
					elF: 60,
					twist: -4,
				},
			][i]!,
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
					? { shN: 30, elN: 40, abN: 30, shF: 150, elF: 22, abF: 20 }
					: { shN: 150, elN: 22, abN: 20, shF: 30, elF: 40, abF: 30 }),
			}),
	},
	// (Breathing with it: never quite still.)
	hold: {
		kind: "loop",
		n: 2,
		fps: 1.3,
		pose: (i) => (i ? { ...P.hold, kneeN: 44, kneeF: 46, lean: 15 } : P.hold),
	},
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
	// Run into a screen: knocked back a step by it, then the shoulder down
	// and fighting his way round it, back into his stance.
	bump: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.stance],
			[
				0.22,
				pose({
					hipN: 12,
					kneeN: 34,
					hipF: 24,
					kneeF: 38,
					lean: -8,
					shN: 78,
					elN: 72,
					abN: 30,
					shF: 64,
					elF: 84,
					abF: 26,
					wide: 0.45,
				}),
			],
			[
				0.62,
				pose({
					hipN: 22,
					kneeN: 48,
					hipF: 34,
					kneeF: 52,
					lean: 24,
					shN: 58,
					elN: 98,
					abN: 42,
					shF: 42,
					elF: 102,
					abF: 38,
					wide: 0.55,
				}),
			],
			[1, P.stance],
		],
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
	// Sizing his man up: down in his legs, rocking - weight forward as if
	// to go, back on his heels, a dip of the shoulder - the other arm out,
	// bent, between the ball and his man. (The dribbling arm rides the
	// ball - see dribbleArm.)
	dribbleIdle: {
		kind: "loop",
		n: 4,
		fps: 2.2,
		pose: (i) =>
			pose({
				...P.sizeUp,
				...[
					{},
					{
						hipN: 6,
						kneeN: 64,
						hipF: 34,
						kneeF: 66,
						lean: 21,
						twist: 7,
						shF: 36,
						elF: 66,
						abF: 40,
					},
					{ kneeN: 54, kneeF: 56, lean: 14 },
					{
						hipN: 0,
						kneeN: 62,
						hipF: 28,
						kneeF: 64,
						lean: 15,
						twist: -6,
						tilt: 3,
						shF: 22,
						elF: 82,
						abF: 30,
					},
				][i],
			}),
	},
	// A jab on the dribble: his foot stabbed at his man, his shoulder and
	// head going with it as if he is gone - and back.
	dribbleJab: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.sizeUp],
			[
				0.3,
				pose({
					...P.sizeUp,
					hipN: 58,
					kneeN: 74,
					hipF: -6,
					kneeF: 30,
					lean: 32,
					twist: 16,
					tilt: -4,
					shF: 44,
					elF: 62,
					abF: 46,
					wide: 0.52,
				}),
			],
			[
				0.55,
				pose({
					...P.sizeUp,
					hipN: 52,
					kneeN: 72,
					hipF: -2,
					kneeF: 32,
					lean: 29,
					twist: 12,
					shF: 40,
					elF: 64,
					abF: 44,
					wide: 0.5,
				}),
			],
			[1, P.sizeUp],
		],
	},
	// A hesitation: up out of his stance as if he is pulling up - chest up,
	// his eyes at the rim - and back down low.
	dribbleHesi: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.sizeUp],
			[
				0.35,
				pose({
					...P.sizeUp,
					hipN: 0,
					kneeN: 26,
					hipF: 14,
					kneeF: 28,
					lean: 6,
					shF: 42,
					elF: 84,
					abF: 24,
					wide: 0.38,
				}),
			],
			[
				0.55,
				pose({
					...P.sizeUp,
					hipN: 0,
					kneeN: 28,
					hipF: 14,
					kneeF: 30,
					lean: 8,
					shF: 40,
					elF: 82,
					abF: 26,
					wide: 0.38,
				}),
			],
			[1, P.sizeUp],
		],
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
	// Down on the floor, his legs out in front of him: both hands on the
	// knee, rocking with it - or folded over, holding his ankle.
	hurtKnee: {
		kind: "loop",
		n: 2,
		fps: 0.9,
		pose: (i) =>
			pose({
				hipN: 90,
				kneeN: 6,
				hipF: 88,
				kneeF: 2,
				lean: 30 + i * 8,
				shN: 60,
				elN: 34,
				abN: 8,
				shF: 56,
				elF: 38,
				abF: 8,
				wide: 0.3,
				plant: 0,
			}),
	},
	hurtAnkle: {
		kind: "loop",
		n: 2,
		fps: 0.8,
		pose: (i) =>
			pose({
				hipN: 92,
				kneeN: 4,
				hipF: 88,
				kneeF: 2,
				lean: 46 + i * 6,
				shN: 76,
				elN: 10,
				abN: 6,
				shF: 72,
				elF: 14,
				abF: 6,
				wide: 0.24,
				plant: 0,
			}),
	},
	// Bent over, both hands up to his face.
	hurtHead: {
		kind: "loop",
		n: 2,
		fps: 0.7,
		pose: (i) =>
			pose({
				hipN: 20,
				kneeN: 30,
				hipF: 16,
				kneeF: 28,
				lean: 32 + i * 4,
				shN: 118 + i * 4,
				elN: 130,
				abN: 16,
				shF: 114,
				elF: 132,
				abF: 16,
				wrN: 30,
				wrF: 30,
				wide: 0.3,
			}),
	},
	// Holding the hurt hand in the other, shaking it out.
	hurtHand: {
		kind: "loop",
		n: 2,
		fps: 2.4,
		pose: (i) =>
			pose({
				hipN: 6,
				kneeN: 18,
				hipF: 10,
				kneeF: 20,
				lean: 16,
				shN: 46,
				elN: 98,
				abN: 4,
				shF: 40 + i * 6,
				elF: 92,
				abF: 2,
				wrF: -30 + i * 40,
				wide: 0.25,
			}),
	},
	// The arm hanging, a hand clutching at the shoulder.
	hurtArm: {
		kind: "loop",
		n: 2,
		fps: 0.8,
		pose: (i) =>
			pose({
				hipN: 4,
				kneeN: 16,
				hipF: 8,
				kneeF: 18,
				lean: 12 + i * 3,
				shN: 76,
				elN: 142,
				abN: 4,
				shF: 4,
				elF: 8,
				abF: 6,
				tilt: -6,
				wide: 0.25,
			}),
	},
	run: { kind: "cycle", n: 6, stride: 8.6, pose: (i) => runPose(i / 6, "run") },
	jog: { kind: "cycle", n: 6, stride: 6.2, pose: (i) => runPose(i / 6, "jog") },
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
	dribbleWalk: {
		kind: "cycle",
		n: 6,
		stride: 4.4,
		pose: (i) => runPose(i / 6, "dribbleWalk"),
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
		stride: SLIDE_STRIDE,
		pose: (i) => runPose(i / 6, "shuffle"),
	},
	// Running at a shooter and breaking down in front of him, a hand up.
	closeout: {
		kind: "cycle",
		n: 6,
		stride: 2.6,
		pose: (i) => runPose(i / 6, "closeout"),
	},
	shoot: { kind: "act", n: 12, keys: JUMP_SHOT },
	setShot: { kind: "act", n: 12, keys: SET_SHOT },
	layup: {
		kind: "act",
		n: 10,
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
			// Up off his left foot, the right knee driving.
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
					toe: 1,
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
					toe: 1,
				}),
			],
			[
				0.72,
				pose({
					hipN: 84,
					kneeN: 104,
					hipF: -10,
					kneeF: 26,
					shN: 168,
					elN: 6,
					shF: 66,
					elF: 60,
					lean: 0,
					wrN: -70,
					toe: 1,
				}),
			],
			// Coming down: the knee lets down, the hand comes down past his
			// face.
			[
				0.84,
				pose({
					hipN: 40,
					kneeN: 56,
					hipF: -4,
					kneeF: 22,
					shN: 132,
					elN: 40,
					shF: 44,
					elF: 54,
					lean: 3,
					wrN: -40,
					toe: 0.6,
				}),
			],
			[
				0.93,
				pose({
					hipN: 8,
					kneeN: 34,
					hipF: 6,
					kneeF: 36,
					shN: 66,
					elN: 62,
					shF: 24,
					elF: 40,
					lean: 8,
					wrN: -10,
				}),
			],
			[
				1,
				pose({
					hipN: -2,
					kneeN: 34,
					hipF: 10,
					kneeF: 38,
					shN: 30,
					elN: 40,
					shF: 16,
					elF: 32,
					lean: 8,
				}),
			],
		],
	},
	// The finger roll: carried up under the ball, his arm reaching out long
	// for the rim with his palm up under it - and rolled off his fingertips.
	fingerRoll: {
		kind: "act",
		n: 10,
		keys: [
			[0, LAYUP_START],
			[
				0.3,
				pose({
					...LAYUP_KNEE,
					shN: 96,
					elN: 72,
					shF: 64,
					elF: 72,
					lean: 6,
					wrN: 50,
				}),
			],
			[
				0.6,
				pose({
					...LAYUP_KNEE,
					hipN: 88,
					kneeN: 106,
					shN: 146,
					elN: 6,
					shF: 52,
					elF: 60,
					lean: 0,
					wrN: 46,
				}),
			],
			[
				0.72,
				pose({
					...LAYUP_KNEE,
					hipN: 84,
					kneeN: 104,
					shN: 156,
					elN: 4,
					shF: 48,
					elF: 58,
					lean: 0,
					wrN: 4,
				}),
			],
			...LAYUP_DOWN,
		],
	},
	// The floater: up off one foot short of the big, the ball brought up in
	// front of his face and pushed up soft and high over him with one hand -
	// let go early, on the way up, the wrist flicked after it.
	floater: {
		kind: "act",
		n: 10,
		keys: [
			[0, LAYUP_START],
			[
				0.3,
				pose({
					...LAYUP_KNEE,
					hipN: 72,
					kneeN: 92,
					shN: 112,
					elN: 84,
					shF: 92,
					elF: 74,
					lean: 4,
					wrN: 44,
				}),
			],
			[
				0.5,
				pose({
					...LAYUP_KNEE,
					hipN: 80,
					kneeN: 98,
					shN: 148,
					elN: 30,
					shF: 66,
					elF: 62,
					lean: 0,
					wrN: 30,
				}),
			],
			[
				0.64,
				pose({
					...LAYUP_KNEE,
					hipN: 78,
					kneeN: 98,
					shN: 160,
					elN: 8,
					shF: 50,
					elF: 56,
					lean: -2,
					wrN: -34,
				}),
			],
			...LAYUP_DOWN,
		],
	},
	// Low and quick under a man coming over to block it: the ball swung up
	// from his hip, underhand, and lifted up off the glass.
	scoop: {
		kind: "act",
		n: 10,
		keys: [
			[0, { ...LAYUP_START, shN: 28, elN: 56, wrN: 30, lean: 18 }],
			[
				0.3,
				pose({
					...LAYUP_KNEE,
					hipN: 80,
					kneeN: 96,
					shN: 66,
					elN: 28,
					abN: 20,
					shF: 52,
					elF: 74,
					abF: 30,
					lean: 14,
					wrN: 56,
				}),
			],
			[
				0.6,
				pose({
					...LAYUP_KNEE,
					hipN: 84,
					kneeN: 100,
					shN: 124,
					elN: 12,
					shF: 72,
					elF: 50,
					abF: 34,
					lean: 6,
					wrN: 44,
				}),
			],
			[
				0.72,
				pose({
					...LAYUP_KNEE,
					hipN: 80,
					kneeN: 100,
					shN: 140,
					elN: 8,
					shF: 64,
					elF: 52,
					lean: 4,
					wrN: 6,
				}),
			],
			...LAYUP_DOWN,
		],
	},
	// Strong to the rim: a jump stop, gathered low on both feet, and up off
	// both with it in both hands - laid up over the front of the rim.
	powerLayup: {
		kind: "act",
		n: 10,
		keys: [
			[
				0,
				pose({
					hipN: 34,
					kneeN: 72,
					hipF: 34,
					kneeF: 72,
					shN: 46,
					elN: 92,
					shF: 46,
					elF: 92,
					lean: 20,
					wide: 0.38,
				}),
			],
			[
				0.3,
				pose({
					hipN: 8,
					kneeN: 18,
					hipF: 8,
					kneeF: 18,
					shN: 118,
					elN: 54,
					shF: 116,
					elF: 56,
					lean: 6,
					wide: 0.26,
					toe: 0.6,
				}),
			],
			[
				0.6,
				pose({
					hipN: 2,
					kneeN: 10,
					hipF: 2,
					kneeF: 12,
					shN: 160,
					elN: 14,
					shF: 156,
					elF: 16,
					lean: 0,
					wide: 0.2,
					wrN: -6,
					wrF: -6,
					toe: 1,
				}),
			],
			[
				0.72,
				pose({
					hipN: 4,
					kneeN: 12,
					hipF: 4,
					kneeF: 14,
					shN: 162,
					elN: 10,
					shF: 150,
					elF: 22,
					lean: 0,
					wide: 0.2,
					wrN: -36,
					wrF: -30,
					toe: 1,
				}),
			],
			[
				0.86,
				pose({
					hipN: 24,
					kneeN: 44,
					hipF: 24,
					kneeF: 46,
					shN: 104,
					elN: 40,
					shF: 96,
					elF: 44,
					lean: 4,
					wide: 0.3,
					toe: 0.4,
				}),
			],
			[
				1,
				pose({
					hipN: 14,
					kneeN: 38,
					hipF: 14,
					kneeF: 40,
					shN: 36,
					elN: 42,
					shF: 32,
					elF: 44,
					lean: 9,
					wide: 0.32,
				}),
			],
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
					toe: 0.8,
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
					toe: 1,
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
					toe: 1,
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
					toe: 1,
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
					toe: 1,
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
					toe: 0.5,
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
					toe: 0.8,
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
					toe: 1,
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
					toe: 1,
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
					toe: 1,
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
					toe: 1,
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
					toe: 0.5,
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
					toe: 0.8,
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
					toe: 1,
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
					toe: 1,
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
					toe: 1,
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
					toe: 1,
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
					toe: 0.5,
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
					toe: 0.8,
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
					toe: 1,
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
					toe: 0.5,
				}),
			],
			[1, P.land],
		],
	},
	// A turnaround fadeaway: up and drifting back, legs out in front.
	fade: { kind: "act", n: 12, keys: FADEAWAY },
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
					toe: 1,
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
					toe: 0.6,
				}),
			],
			[1, P.hold],
		],
	},
	block: {
		kind: "act",
		n: 8,
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
					toe: 1,
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
					toe: 1,
				}),
			],
			// Coming down, the arms with him.
			[
				0.82,
				pose({
					hipN: -2,
					kneeN: 18,
					hipF: 12,
					kneeF: 30,
					shN: 88,
					elN: 32,
					shF: 66,
					elF: 38,
					lean: 6,
					toe: 0.4,
				}),
			],
			[1, P.land],
		],
	},
	// A hand straight up at the shot as he goes up with it - and kept up
	// while it goes over him - the other hand down and out of the way. Down
	// in his legs to go, the hand already on its way up: off a closeout it
	// stays up the whole way, never down past his face and back.
	contest: {
		kind: "act",
		n: 7,
		keys: [
			[
				0,
				pose({
					...P.ready,
					kneeN: 40,
					kneeF: 42,
					hipN: 10,
					hipF: 20,
					lean: 12,
					shN: 146,
					elN: 22,
					abN: 6,
					shF: 14,
					elF: 30,
					abF: 34,
					tuck: 0.3,
				}),
			],
			[
				0.18,
				pose({
					hipN: -2,
					kneeN: 14,
					hipF: 10,
					kneeF: 24,
					shN: 158,
					elN: 12,
					abN: 4,
					shF: 10,
					elF: 24,
					abF: 30,
					lean: -2,
					tuck: 0.7,
					toe: 0.6,
				}),
			],
			[
				0.62,
				pose({
					hipN: -2,
					kneeN: 12,
					hipF: 10,
					kneeF: 22,
					shN: 166,
					elN: 6,
					abN: 4,
					shF: 8,
					elF: 22,
					abF: 30,
					lean: -4,
					tuck: 0.7,
					toe: 1,
				}),
			],
			[
				0.84,
				pose({
					hipN: -4,
					kneeN: 18,
					hipF: 12,
					kneeF: 28,
					shN: 112,
					elN: 38,
					shF: 14,
					elF: 30,
					abF: 26,
					lean: 2,
					tuck: 0.4,
					toe: 0.4,
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
	// A poke at the ball on a man's dribble: down low, a quick jab of the
	// right hand at it - forward, down and across - from his stance, the
	// other hand up, and back. (Mirrored for the left.)
	poke: {
		kind: "act",
		n: 6,
		keys: [
			[
				0,
				pose({
					hipN: 30,
					kneeN: 64,
					hipF: 36,
					kneeF: 66,
					lean: 20,
					wide: 0.8,
					shN: 46,
					elN: 44,
					abN: 40,
					shF: 120,
					elF: 30,
					abF: 22,
				}),
			],
			[
				0.38,
				pose({
					hipN: 42,
					kneeN: 66,
					hipF: 30,
					kneeF: 62,
					lean: 30,
					wide: 0.75,
					tilt: -8,
					twist: -10,
					shN: 64,
					elN: 4,
					abN: -4,
					wrN: -26,
					shF: 116,
					elF: 34,
					abF: 24,
				}),
			],
			[
				0.6,
				pose({
					hipN: 40,
					kneeN: 66,
					hipF: 30,
					kneeF: 62,
					lean: 28,
					wide: 0.75,
					tilt: -5,
					twist: -14,
					shN: 56,
					elN: 10,
					abN: -18,
					wrN: -10,
					shF: 112,
					elF: 36,
					abF: 24,
				}),
			],
			[
				1,
				pose({
					hipN: 30,
					kneeN: 64,
					hipF: 36,
					kneeF: 66,
					lean: 20,
					wide: 0.8,
					shN: 46,
					elN: 44,
					abN: 40,
					shF: 120,
					elF: 30,
					abF: 22,
				}),
			],
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
	// Crossed up: his feet go out from under him the wrong way - knees
	// buckling, arms flung out for balance, a hand nearly to the floor - and
	// he catches himself and turns to chase.
	stumble: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.stance],
			[
				0.35,
				pose({
					hipN: 40,
					kneeN: 78,
					hipF: 6,
					kneeF: 30,
					shN: 70,
					elN: 20,
					abN: 60,
					shF: 30,
					elF: 10,
					abF: 70,
					lean: 18,
					tilt: 16,
					wide: 0.75,
				}),
			],
			[
				0.6,
				pose({
					hipN: 46,
					kneeN: 88,
					hipF: 10,
					kneeF: 40,
					shN: 50,
					elN: 14,
					abN: 40,
					shF: 20,
					elF: 8,
					abF: 30,
					lean: 30,
					tilt: 20,
					wide: 0.7,
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
					toe: 1,
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
					toe: 0.6,
				}),
			],
			[0.66, P.chin],
			[1, P.chin],
		],
	},
	// A loose ball taken out of the air on the hop: down to it, reaching
	// out with both hands - and chinned.
	snatch: {
		kind: "act",
		n: 7,
		keys: [
			[
				0,
				pose({
					hipN: -6,
					kneeN: 30,
					hipF: 12,
					kneeF: 34,
					lean: 14,
					shN: 50,
					elN: 30,
					shF: 46,
					elF: 34,
				}),
			],
			[
				0.36,
				pose({
					hipN: 4,
					kneeN: 54,
					hipF: 22,
					kneeF: 58,
					lean: 26,
					shN: 44,
					elN: 14,
					shF: 40,
					elF: 16,
					wide: 0.4,
				}),
			],
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
					hipN: 44,
					kneeN: 124,
					hipF: 80,
					kneeF: 132,
					shN: 8,
					elN: 8,
					shF: 4,
					elF: 10,
					abN: 8,
					abF: 8,
					lean: 54,
					wide: 0.42,
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
			[0, SET_SHOT.at(-1)![1]],
			[0.7, { ...SET_SHOT.at(-1)![1], shN: 140, elN: 10, wrN: -96 }],
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
	// A low five: the hand swung down and out in front of him to meet the
	// other man's, palm to palm at the hip, leaning into it a little - and
	// through.
	lowFive: {
		kind: "act",
		n: 6,
		keys: [
			[0, P.ready],
			[
				0.3,
				pose({
					shN: 30,
					elN: 34,
					abN: -4,
					wrN: 10,
					kneeN: 14,
					kneeF: 12,
					lean: 6,
				}),
			],
			// Palm to palm, out in front of him at his hip, across his body to
			// meet the other man's.
			[
				0.5,
				pose({
					shN: 56,
					elN: 8,
					abN: -20,
					wrN: -14,
					kneeN: 16,
					kneeF: 14,
					lean: 9,
				}),
			],
			[
				0.68,
				pose({
					shN: 70,
					elN: 14,
					abN: -26,
					wrN: -4,
					kneeN: 12,
					kneeF: 10,
					lean: 6,
				}),
			],
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
	// THE EURO STEP: the dribble picked up, a long step one way with the
	// ball swung low and away to the hip, then a long step back across the
	// other way with it carried high, and gathered to go up.
	euroStep: {
		kind: "act",
		n: 8,
		keys: [
			[
				0,
				pose({
					shN: 40,
					elN: 92,
					shF: 40,
					elF: 92,
					hipN: 30,
					kneeN: 30,
					hipF: -12,
					kneeF: 40,
					lean: 12,
				}),
			],
			[
				0.4,
				pose({
					shN: 24,
					elN: 60,
					abN: 30,
					shF: 46,
					elF: 84,
					abF: -24,
					hipN: 58,
					kneeN: 42,
					hipF: -34,
					kneeF: 28,
					lean: 20,
					wide: 0.7,
				}),
			],
			[
				0.8,
				pose({
					shN: 70,
					elN: 70,
					abN: -18,
					shF: 58,
					elF: 66,
					abF: 30,
					hipN: -30,
					kneeN: 32,
					hipF: 60,
					kneeF: 46,
					lean: 18,
					wide: 0.7,
				}),
			],
			[
				1,
				pose({
					shN: 64,
					elN: 82,
					shF: 64,
					elF: 82,
					hipN: 34,
					kneeN: 54,
					hipF: 22,
					kneeF: 48,
					lean: 10,
				}),
			],
		],
	},
	// THE STEP-BACK: planted on the front foot, a hop back off it with the
	// ball pulled in to his chest, and down on both feet, balanced, to rise.
	stepBack: {
		kind: "act",
		n: 6,
		keys: [
			[
				0,
				pose({
					shN: 40,
					elN: 90,
					shF: 40,
					elF: 90,
					hipN: 46,
					kneeN: 52,
					hipF: -6,
					kneeF: 30,
					lean: 22,
				}),
			],
			[
				0.45,
				pose({
					shN: 46,
					elN: 96,
					shF: 46,
					elF: 96,
					hipN: 34,
					kneeN: 66,
					hipF: 22,
					kneeF: 62,
					lean: -4,
				}),
			],
			[
				1,
				pose({
					shN: 52,
					elN: 92,
					shF: 52,
					elF: 92,
					hipN: 30,
					kneeN: 46,
					hipF: 26,
					kneeF: 44,
					lean: 4,
					wide: 0.45,
				}),
			],
		],
	},
	// Face to face down the handshake line, waiting on the man before to
	// finish with him.
	waitFive: {
		kind: "act",
		n: 2,
		keys: [
			[0, P.ready],
			[1, P.ready],
		],
	},
	// A CHEST BUMP: a step in, up off both feet, chests together in the air
	// with the arms flung back, and down.
	chestBump: {
		kind: "act",
		n: 8,
		keys: [
			[0, P.ready],
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
			[1, P.ready],
		],
	},
	// Up off his knee at the table, arms crossed to the hem and the warm-up
	// top pulled off over his head, then dropped.
	strip: {
		kind: "act",
		n: 6,
		keys: [
			[
				0,
				pose({
					hipN: -4,
					kneeN: 96,
					hipF: 84,
					kneeF: 86,
					lean: 8,
					shN: 38,
					elN: 58,
					shF: 44,
					elF: 52,
					wide: 0.2,
				}),
			],
			[
				0.25,
				pose({
					shN: 14,
					elN: 34,
					abN: -34,
					shF: 14,
					elF: 34,
					abF: -34,
					lean: 6,
					kneeN: 10,
					kneeF: 12,
				}),
			],
			[
				0.5,
				pose({
					shN: 150,
					elN: 70,
					abN: -18,
					shF: 150,
					elF: 70,
					abF: -18,
					lean: -2,
				}),
			],
			[
				0.7,
				pose({
					shN: 172,
					elN: 18,
					abN: 8,
					shF: 172,
					elF: 18,
					abF: 8,
					lean: -4,
				}),
			],
			[
				0.85,
				pose({ shN: 70, elN: 20, abN: 34, shF: 20, elF: 30, abF: 10, lean: 2 }),
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
	// Dribble moves, one bounce each (see CROSS_FRONT): the ball leaving his
	// right hand, or his left.
	crossFrontR: { kind: "act", n: 8, keys: CROSS_FRONT },
	crossFrontL: { kind: "act", n: 8, keys: mirrorKeys(CROSS_FRONT) },
	crossLegsR: { kind: "act", n: 8, keys: CROSS_LEGS },
	crossLegsL: { kind: "act", n: 8, keys: mirrorKeys(CROSS_LEGS) },
	crossBackR: { kind: "act", n: 8, keys: CROSS_BACK },
	crossBackL: { kind: "act", n: 8, keys: mirrorKeys(CROSS_BACK) },
} satisfies Record<string, Anim>;

// The dribble moves, by move and the hand the ball leaves - each with his
// whole body in it, his hands included (no bounce of the dribbling arm laid
// over it).
export type DribbleMove = "front" | "legs" | "back";
export const moveAnim = (move: DribbleMove, from: Hand): AnimName =>
	move === "legs"
		? from === "R"
			? "crossLegsR"
			: "crossLegsL"
		: move === "back"
			? from === "R"
				? "crossBackR"
				: "crossBackL"
			: from === "R"
				? "crossFrontR"
				: "crossFrontL";
const MOVES = new Set<string>([
	"crossFrontR",
	"crossFrontL",
	"crossLegsR",
	"crossLegsL",
	"crossBackR",
	"crossBackL",
]);
export const isMove = (anim: string): boolean => MOVES.has(anim);

export type AnimName = keyof typeof ANIMS;

export const animFrames = (anim: AnimName): number => ANIMS[anim].n;

export const poseFor = (anim: AnimName, frame: number): Pose => {
	const a: Anim = ANIMS[anim];
	if (a.kind === "act") {
		return keyed(a.keys, a.n > 1 ? frame / (a.n - 1) : 0);
	}
	return a.pose(frame);
};

// Running, he leaves the floor between strides - walking, he rolls up onto
// his toes: how high his lower ankle is (feet) at a point through the
// stride.
export const bounceAt = (anim: AnimName, phase: number): number =>
	isGait(anim) ? gaitLift(anim, phase) : 0;

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

// A player's build, in feet: an athlete's, in true proportion - long legs, a
// lean torso only a little wider at the chest than at the waist, lean limbs -
// but for his head, a little big, so his face still reads from up in the
// stands. Height drives
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

// A cartoon athlete: a big head - a fifth of him - on a short neck, over a
// lean athlete's body, his arms in proportion to it (elbows at his waist,
// wrists at his hips, fingertips halfway down his thighs). His height and
// build still tell: a center towers over a point guard, a big man is broad.
// (His face is drawn a little bigger than headR: crown to chin is 2.45
// headR.)
export const bodyOf = (hgt = DEFAULT_HGT, weight = DEFAULT_WEIGHT): Body => {
	const H = (DEFAULT_HGT * (hgt / DEFAULT_HGT) ** 1.25) / 12;
	const g = girthOf(hgt, weight);
	return {
		H,
		hipH: H * 0.45,
		ankleH: H * 0.03,
		thigh: H * 0.215,
		shin: H * 0.205,
		foot: H * 0.15,
		torso: H * 0.27,
		neck: H * 0.05,
		headR: H * 0.1,
		upper: H * 0.14,
		fore: H * 0.115,
		shoulderW: H * 0.1 * g,
		hipW: H * 0.054 * g,
		depth: H * 0.104 * g,
		thighR: H * 0.043 * g,
		kneeR: H * 0.03 * g,
		calfR: H * 0.033 * g,
		ankleR: H * 0.025,
		upperR: H * 0.032 * g,
		foreR: H * 0.027 * g,
		handR: H * 0.036,
	};
};

// A raised arm's cartoon stretch, over its own length (see stretchOf).
const STRETCH = 0.16;

// How high he reaches standing, arms straight up - shoulders, arms at a
// raised arm's stretch, hand (feet).
export const standingReach = (b: Body): number =>
	b.ankleH +
	b.thigh +
	b.shin +
	b.torso -
	b.H * 0.022 +
	(b.upper + b.fore) * (1 + STRETCH) +
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

// His upper body turned on his hips (see Pose): a point of it carried round
// his spine by the twist, then tipped to his side by the tilt - `w` of the
// way (his chest all of it, his hips none). Or, `back`, undone.
export const turnUpper = (q: Pose, pelvis: V3, w = 1, back = false) => {
	const rad = Math.PI / 180;
	const L = q.lean * rad;
	const df = Math.sin(L);
	const du = Math.cos(L);
	const tw = q.twist * rad * w * (back ? -1 : 1);
	const tl = q.tilt * rad * w * (back ? -1 : 1);
	const ct = Math.cos(tw);
	const st = Math.sin(tw);
	const cl = Math.cos(tl);
	const sl = Math.sin(tl);
	const twist = (v: V3): V3 => {
		// Round the spine (Rodrigues), the spine leaning forward.
		const along = v.f * df + v.u * du;
		return v3(
			v.f * ct + -du * v.s * st + df * along * (1 - ct),
			v.s * ct + (du * v.f - df * v.u) * st,
			v.u * ct + df * v.s * st + du * along * (1 - ct),
		);
	};
	const tilt = (v: V3): V3 =>
		v3(v.f, v.s * cl + v.u * sl, -v.s * sl + v.u * cl);
	return (p: V3): V3 => {
		if (tw === 0 && tl === 0) {
			return p;
		}
		const v = v3(p.f - pelvis.f, p.s - pelvis.s, p.u - pelvis.u);
		const r = back ? twist(tilt(v)) : tilt(twist(v));
		return v3(pelvis.f + r.f, pelvis.s + r.s, pelvis.u + r.u);
	};
};

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
	// Spread wide, his legs angle out from his hips - each bone its own
	// length still, so a wide stance sits him lower instead of stretching him.
	const inPlane = (len: number) =>
		Math.sqrt(Math.max(len * len * 0.3, len * len - wide * wide * 0.25));
	const thigh = inPlane(b.thigh);
	const shin = inPlane(b.shin);
	const leg = (hipDeg: number, kneeDeg: number, side: 1 | -1) => {
		const a = dir(hipDeg);
		const c = dir(hipDeg - kneeDeg);
		const root = v3(0, side * b.hipW, b.hipH);
		const mid = v3(
			a.f * thigh,
			side * (b.hipW + wide * 0.5),
			b.hipH + a.u * thigh,
		);
		const end = v3(
			mid.f + c.f * shin,
			side * (b.hipW + wide),
			mid.u + c.u * shin,
		);
		return { root, mid, end };
	};
	// Balanced: a bend at the knees is a bend at the hips too, the way a
	// body crouches - so his feet stay under him instead of trailing behind.
	const ankleF = (h: number, k: number) =>
		thigh * Math.sin(h * rad) + shin * Math.sin((h - k) * rad);
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
	// Pointed, a foot hangs from the ankle (only ever up in the air: his
	// ankles still sit where they would on the floor) - or, up on its toes,
	// rests on them.
	const toe = b.foot * 0.72;
	for (const [l, own] of [
		[legR, q.toeN],
		[legL, q.toeF],
	] as const) {
		const point = Math.min(1, q.toe + own) * POINT * rad;
		l.root.u += off;
		l.mid.u += off;
		l.end.u += off;
		const flat = Math.max(b.ankleR, l.end.u - b.ankleH * 0.55);
		(l as Limb).tip = v3(
			l.end.f + toe * Math.cos(point),
			l.end.s,
			flat + (l.end.u - toe - flat) * Math.sin(point),
		);
	}
	// His legs turned under him, round his hips.
	if (q.legs !== 0) {
		const c = Math.cos(q.legs * rad);
		const sn = Math.sin(q.legs * rad);
		const spin = (p: V3) => {
			const f = p.f * c - p.s * sn;
			p.s = p.f * sn + p.s * c;
			p.f = f;
		};
		for (const l of [legR, legL] as Limb[]) {
			spin(l.root);
			spin(l.mid);
			spin(l.end);
			if (l.tip) {
				spin(l.tip);
			}
		}
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
	const armR = armLimb(b, chest, q.shN, q.elN, q.abN, q.wrN, -1, q.tuck);
	const armL = armLimb(b, chest, q.shF, q.elF, q.abF, q.wrF, 1, q.tuck);
	if (q.twist === 0 && q.tilt === 0) {
		return { pelvis, chest, head, legR, legL, armR, armL };
	}
	// His shoulders, arms and head turned on his hips.
	const turn = turnUpper(q, pelvis);
	const limb = (l: Limb): Limb => ({
		root: turn(l.root),
		mid: turn(l.mid),
		end: turn(l.end),
		...(l.tip ? { tip: turn(l.tip) } : {}),
	});
	return {
		pelvis,
		chest: turn(chest),
		head: turn(head),
		legR,
		legL,
		armR: limb(armR),
		armL: limb(armL),
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

// How far the arm stretches (1 its own length): a little (STRETCH), thrown up
// over his head - and a jumper's tucked arm from as soon as it is up past his
// shoulder, so the ball goes up over that big head on a bent elbow - never so
// far it stops looking his own.
const stretchOf = (shDeg: number, tuck: number): number => {
	const lift = Math.min(1, Math.max(0, (shDeg - 75) / 55));
	return (
		1 + STRETCH * Math.max(reachOf(shDeg), tuck * lift * lift * (3 - 2 * lift))
	);
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
	tuck = 0,
): Limb => {
	const up = reachOf(shDeg);
	return buildArm(
		b,
		chest,
		shDeg,
		elDeg,
		(abDeg + Math.max(0, 22 - abDeg) * up * (1 - tuck)) * RAD,
		stretchOf(shDeg, tuck),
		wrDeg,
		side,
	);
};

// The arm that puts his wrist at `target`, worked back from the arm's own
// geometry (the cartoon reach included): out from his side as far as the
// target is, then shoulder and elbow from the triangle the two bones make.
const armAim = (b: Body, chest: V3, target: V3, side: 1 | -1, tuck = 0) => {
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
		reach = stretchOf(sh / RAD, tuck);
	}
	return { sh, el, ab, reach };
};
export const armTo = (
	b: Body,
	chest: V3,
	target: V3,
	wrDeg: number,
	side: 1 | -1,
	tuck = 0,
): Limb => {
	const { sh, el, ab, reach } = armAim(b, chest, target, side, tuck);
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
	setShot: "shot",
	fade: "shot",
	shotFake: "shot",
	layup: "palm",
	fingerRoll: "palm",
	floater: "palm",
	scoop: "palm",
	dunk: "palm",
	dunk1: "palm",
	tomahawk: "palm",
	hook: "palm",
};
export const gripOf = (anim: AnimName): Grip => GRIPS[anim] ?? "two";

// A basketball's radius, feet.
const BALL_RADIUS = 0.39;

// The ball under a dribbling hand, in his frame: just below the middle of
// his palm.
export const underPalm = (arm: Limb): V3 => {
	const tip = arm.tip ?? arm.end;
	return v3(
		arm.end.f + (tip.f - arm.end.f) * 0.55,
		arm.end.s + (tip.s - arm.end.s) * 0.55,
		arm.end.u + (tip.u - arm.end.u) * 0.55 - BALL_RADIUS * 0.95,
	);
};
// On the palm of a hand: how far its middle is from the wrist, and how thick
// the hand is either side of it (of his height).
const PALM_AT = 0.044;
const PALM_THICK = 0.016;

// On the move with the ball in both hands - not dribbling it - he carries
// it at his chest, his arms still.
const CARRIES = new Set<AnimName>([
	"run",
	"sprint",
	"walk",
	"back",
	"slide",
	"dribble",
	"dribbleWalk",
	"post",
]);
export const carried = (q: Pose, anim: AnimName): Pose =>
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
	// (Not if his pose has him carrying it already - see poseOf.)
	carry = true,
): { sk: Skeleton; ball: V3 } => {
	const q = carry ? carried(q0, anim) : q0;
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
	// Up on the shooting hand: in front of his palm, his wrist cocked back
	// under it and his fingers spread up the back of it - so it rests on the
	// pads of his fingers and goes off his fingertips. (The palm faces the
	// way the hand is cocked away from.) Or, on a layup or a dunk, palmed
	// out past the fingers' roots.
	let one: V3;
	if (grip === "shot") {
		const n = Math.hypot(d.u, d.f) || 1;
		const pf = d.u / n;
		const pu = -d.f / n;
		const along = b.H * PALM_AT;
		const out = R + b.H * PALM_THICK;
		one = v3(
			r.end.f + d.f * along + pf * out,
			r.end.s + d.s * along,
			r.end.u + d.u * along + pu * out,
		);
	} else {
		one = v3(
			r.end.f + d.f * R * 1.15,
			r.end.s + d.s * R * 1.15,
			r.end.u + d.u * R * 1.15,
		);
	}
	const ball = mix(two, one, w);
	const armR =
		w >= 0.999
			? r
			: armTo(b, sk.chest, mix(side(two, -1), r.end, w), q.wrN, -1, q.tuck);
	// The other hand guides a jumper up - its palm flat on the side of the
	// ball, fingers up - and comes off it in the instant before the shooting
	// hand lets it go; on a layup or a dunk it lets go.
	const guide = v3(
		ball.f - R * 0.1,
		ball.s + R + b.H * PALM_THICK,
		ball.u - b.H * PALM_AT,
	);
	const armL =
		(grip === "palm" && w >= 0.999) || (grip === "shot" && q.free >= 0.999)
			? l
			: armTo(
					b,
					sk.chest,
					grip === "shot"
						? mix(mix(side(ball, 1), guide, w), l.end, q.free)
						: mix(side(ball, 1), l.end, w),
					q.wrF,
					1,
					q.tuck,
				);
	return { sk: { ...sk, armR, armL }, ball };
};

// Where the ball is on his fingers that far through a shot (`at`, 0 to 1) -
// a typical player's: in front of him, to his left, up from his feet.
export const releaseAt = (anim: AnimName, at: number, b = bodyOf()): V3 =>
	holdBall(b, poseAt(anim, at), anim).ball;

// The dribbling hand through one bounce: on the ball at the top (0),
// pushing it down until it leaves him, then back up to meet it (1). With the
// ball in his left hand the arms trade jobs: the right is the one held out
// to keep his man off.
export type Hand = "R" | "L";
// Where the hand works it (feet, from his hips: forward, out to that side,
// up) - at the top of a bounce and as it lets it go. Where he stands, at his
// hip, out to the side away from his man: his upper arm hanging, the
// forearm and wrist doing the work, the arm reaching all but straight as it
// pushes. On the move (`ahead`, 0 to 1), out in front of him, pushed on
// ahead as he goes.
const DRIBBLE_TOP = { f: 0.72, s: 1.12, u: 0.5 };
const DRIBBLE_LOW = { f: 0.82, s: 1.18, u: 0.02 };
const DRIBBLE_TOP_AHEAD = { f: 1.12, s: 1.02, u: 0.6 };
const DRIBBLE_LOW_AHEAD = { f: 1.38, s: 1.06, u: 0.2 };
// Through a bounce, how far down the hand has pushed (0 at the top, 1 at the
// bottom): down hard, the ball gone at the bottom, then back up to wait for
// it - riding it the last of the way up.
const pushOf = (ph: number): number => {
	const smooth = (x: number) => x * x * (3 - 2 * x);
	return ph < 0.2
		? smooth(ph / 0.2)
		: 1 - smooth(Math.min(1, (ph - 0.2) / 0.6));
};
// How far out in front of him he works it, going as he is (see dribbleArm).
export const dribbleAhead = (anim: AnimName): number =>
	anim === "dribble" || anim === "sprint" || anim === "run"
		? 1
		: anim === "dribbleWalk" || anim === "dribbleJab" || anim === "dribbleHesi"
			? 0.5
			: 0;
const DRIBBLER = (() => {
	let b: Body | undefined;
	return () => (b ??= bodyOf());
})();
export const dribbleArm = (
	q: Pose,
	ph: number,
	hand: Hand = "R",
	ahead = 0,
	// A bounce that takes it across to his other hand: the hand it leaves.
	leaves?: Hand,
): Pose => {
	const e = pushOf(ph);
	const at = (k: "f" | "s" | "u") => {
		const top =
			DRIBBLE_TOP[k] + (DRIBBLE_TOP_AHEAD[k] - DRIBBLE_TOP[k]) * ahead;
		const low =
			DRIBBLE_LOW[k] + (DRIBBLE_LOW_AHEAD[k] - DRIBBLE_LOW[k]) * ahead;
		return top + (low - top) * e;
	};
	const b = DRIBBLER();
	// (Worked out in his frame before his shoulders turn - as the skeleton
	// builds his arms.)
	const sk = skeleton(b, { ...q, twist: 0, tilt: 0 });
	// The hand over the top of it, fingers spread forward, snapped down
	// through the push and opening back up to take it.
	const snap = ph < 0.18 ? ph / 0.18 : 1 - Math.min(1, (ph - 0.18) / 0.5);
	const on = (h: Hand) => {
		const side = h === "L" ? 1 : -1;
		const arm = armAim(
			b,
			sk.chest,
			v3(at("f"), side * at("s"), sk.pelvis.u + at("u")),
			side,
		);
		const sh = arm.sh / RAD;
		const el = arm.el / RAD;
		return {
			sh,
			el,
			ab: arm.ab / RAD,
			wr: 74 - 62 * snap * snap * (3 - 2 * snap) - sh - el,
		};
	};
	// The other arm as the move has it.
	const free = { sh: q.shF, el: q.elF, ab: q.abF, wr: q.wrF };
	let right: typeof free;
	let left: typeof free;
	if (leaves) {
		// Across to the other hand: the one it leaves eases off it once it
		// has pushed it down and over, and the one it goes to drops to take
		// it on its way up - never a snap from one to the other.
		const smooth = (x: number) => {
			const u = Math.min(1, Math.max(0, x));
			return u * u * (3 - 2 * u);
		};
		const mix = (a: typeof free, c: typeof free, w: number) => ({
			sh: a.sh + (c.sh - a.sh) * w,
			el: a.el + (c.el - a.el) * w,
			ab: a.ab + (c.ab - a.ab) * w,
			wr: a.wr + (c.wr - a.wr) * w,
		});
		const goes = leaves === "R" ? "L" : "R";
		const off = mix(free, on(leaves), 1 - smooth((ph - 0.3) / 0.4));
		const to = mix(free, on(goes), smooth((ph - 0.22) / 0.4));
		[right, left] = leaves === "R" ? [off, to] : [to, off];
	} else {
		[right, left] = hand === "L" ? [free, on("L")] : [on("R"), free];
	}
	return {
		...q,
		shN: right.sh,
		elN: right.el,
		abN: right.ab,
		wrN: right.wr,
		shF: left.sh,
		elF: left.el,
		abF: left.ab,
		wrF: left.wr,
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
	leaves?: Hand,
): Pose => {
	const q = poseAt(anim, phase);
	if (dribble !== undefined && !MOVES.has(anim)) {
		return dribbleArm(q, dribble, hand, dribbleAhead(anim), leaves);
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
