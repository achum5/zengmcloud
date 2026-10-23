import { project, type Camera } from "../court3d/camera.ts";
import {
	skeleton,
	turnUpper,
	type Body,
	type Limb,
	type Pose,
	type V3,
} from "../court3d/poses.ts";
import type { Pt3 } from "./geometry.ts";
import type { FieldLook } from "./kit.ts";

// ONE FOOTBALL PLAYER, SCULPTED - the body of his sprite (see sprite.ts).
//
// Built the way the 3D court builds its players (see court3d/sculpt.ts, this
// is its football cousin): rounded masses laid along his skeleton, each pixel
// of the sprite finding the mass nearest the camera there, masses that meet
// at a joint melting into one another near it, the light falling on each
// pixel in two tones the cartoon way. What he wears is worked out on his
// body, pixel by pixel, from the point each pixel shows.
//
// What is his own to football: shoulder pads under the jersey, broad across
// the top of him with a cap over each shoulder; short sleeves; pants to below
// the knee with the stripes down their sides, pads in the thighs and knees,
// socks up to meet them, cleats. And no face drawn over him - a helmet, shell
// and facemask sculpted with the rest of him, the face only glimpsed through
// the bars, the team's logo on its sides. The ball, when he has it, a long
// brown ball with white laces, tucked away or in his hands.

// ---- colors -----------------------------------------------------------------

type RGB = [number, number, number];
const rgbs = new Map<string, RGB>();
const rgb = (c: string): RGB => {
	let out = rgbs.get(c);
	if (!out) {
		const m = /^#?([\da-f]{6})$/i.exec(c.trim());
		const r = /rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)/i.exec(c);
		if (m) {
			const n = Number.parseInt(m[1]!, 16);
			out = [(n >> 16) & 255, (n >> 8) & 255, n & 255];
		} else if (r) {
			out = [Number(r[1]), Number(r[2]), Number(r[3])];
		} else {
			out = [128, 128, 128];
		}
		rgbs.set(c, out);
	}
	return out;
};
const scaled = (c: RGB, f: number): RGB => [c[0] * f, c[1] * f, c[2] * f];
const toward = (c: RGB, to: RGB, f: number): RGB => [
	c[0] + (to[0] - c[0]) * f,
	c[1] + (to[1] - c[1]) * f,
	c[2] + (to[2] - c[2]) * f,
];

export const BALL_BROWN = "#7a3d1e";
const BALL_RGB = rgb(BALL_BROWN);
const LACE_RGB: RGB = [246, 243, 236];
const WHITE_RGB: RGB = [246, 245, 240];
const HEADSET_RGB: RGB = [30, 30, 34];

// ---- the masses -------------------------------------------------------------

// What a mass is, for painting it.
const TORSO = 0;
const UPPER = 1;
const FORE = 2;
const HAND = 3;
const THIGH = 4;
const SHIN = 5;
const SHOE = 6;
const NECK = 7;
const SHELL = 8;
const MASK = 9;
const CHIN = 10;
const BALL = 11;
const TOWEL = 12;
const HELD = 13;
const HEADSET = 14;

// What melts into what: masses in one group melt together everywhere, two
// groups only where a joint joins them.
const G_TORSO = 0;
const G_NECK = 1;
const G_ARM = 2; // + side (0 his right, 1 his left)
const G_LEG = 4; // + side
const G_SHOE = 6; // + side
const G_HELMET = 8;
const G_MASK = 9;
const G_BALL = 10;
const G_TOWEL = 11;
const G_HAND = 12; // + side
const G_FINGER = 14; // + side * 5 + finger (4 the thumb)
const GROUPS = 24;

type Mass = {
	// On the sprite (its pixels): from a, along d.
	ax: number;
	ay: number;
	dx: number;
	dy: number;
	l2: number;
	len: number;
	// How far from the camera each end is (feet), and the sprite's pixels to
	// a foot there.
	za: number;
	zb: number;
	k: number;
	// Its radius along it (sprite pixels), sampled.
	rad: Float32Array;
	rmax: number;
	// The same, on him (feet): from A to B.
	A: V3;
	B: V3;
	group: number;
	kind: number;
	// His right (0) or left (1).
	side: number;
	x0: number;
	y0: number;
	x1: number;
	y1: number;
};

const RS = 24;
type Station = [t: number, r: number];
const radiusAt = (prof: Station[], t: number): number => {
	if (t <= prof[0]![0]) {
		return prof[0]![1];
	}
	for (let i = 1; i < prof.length; i++) {
		const [t1, r1] = prof[i]!;
		const [t0, r0] = prof[i - 1]!;
		if (t <= t1) {
			const u = (t - t0) / Math.max(1e-6, t1 - t0);
			return r0 + (r1 - r0) * u * u * (3 - 2 * u);
		}
	}
	return prof.at(-1)![1];
};

const v3 = (f: number, s: number, u: number): V3 => ({ f, s, u });
const lerpV = (a: V3, b: V3, t: number): V3 =>
	v3(a.f + (b.f - a.f) * t, a.s + (b.s - a.s) * t, a.u + (b.u - a.u) * t);
const addV = (a: V3, b: V3, k = 1): V3 =>
	v3(a.f + b.f * k, a.s + b.s * k, a.u + b.u * k);
const subV = (a: V3, b: V3): V3 => v3(a.f - b.f, a.s - b.s, a.u - b.u);
const dotV = (a: V3, b: V3) => a.f * b.f + a.s * b.s + a.u * b.u;
const lenV = (a: V3) => Math.hypot(a.f, a.s, a.u);
const unit = (a: V3, or: V3 = v3(1, 0, 0)): V3 => {
	const l = lenV(a);
	return l > 1e-6 ? v3(a.f / l, a.s / l, a.u / l) : or;
};
// f, s, u turn the right way round: forward x left is up.
const cross = (a: V3, b: V3): V3 =>
	v3(a.s * b.u - a.u * b.s, a.u * b.f - a.f * b.u, a.f * b.s - a.s * b.f);
// The part of `a` square to the unit `d`.
const square = (a: V3, d: V3): V3 => addV(a, d, -dotV(a, d));

// A point on him (his frame, feet) on the field: forward is (cos, sin) of
// the way he faces, his left (sin, -cos) - the field's y runs toward the
// camera, as the court's does.
export type Placed = { x: number; y: number; z: number; yaw: number };
export const bodyPoint = (at: Placed, v: V3): Pt3 => {
	const c = Math.cos(at.yaw);
	const s = Math.sin(at.yaw);
	return {
		x: at.x + v.f * c + v.s * s,
		y: at.y + v.f * s - v.s * c,
		z: at.z + v.u,
	};
};

// ---- what he is doing, for sculpting ----------------------------------------

// How a hand is held: open (ready, or flat on the turf), relaxed, a fist
// (running), on the ball.
export type Grip = "open" | "relaxed" | "fist" | "ball";

// The ball with him: its middle, the way its points run, and which way its
// laces face - in his frame (feet).
export type HeldBall = { at: V3; axis: V3; laces: V3 };

export type Figure = Placed & {
	pose: Pose;
	// His right hand, his left.
	grips: [Grip, Grip];
	ball?: HeldBall;
};

// The football: eleven inches point to point, a little bigger than life so
// it reads, and how fat round its middle.
export const BALL_LEN = 1.04;
export const BALL_R = 0.31;

// ---- his uniform, on him ----------------------------------------------------

// Up his spine from his hips (feet): where his jersey tucks into his pants,
// and the tops of his shoulders (over his pads).
const tuckOf = (body: Body) => body.torso * 0.24;
const shouldersOf = (body: Body) => body.torso + body.depth * 0.55;

// How far down his upper arm his sleeve comes, and his pants down his shin.
const SLEEVE = 0.4;
const PANTS_HEM = 0.16;

// Where his lettering goes, up his torso (in torso lengths) and how big (in
// heights), and how wide it may be (in sheet half-widths): a big number on
// his chest, his name over a bigger one on his back.
export const FIELD_LETTERING = {
	number: { u: 0.5, size: 0.135, maxW: 1.25 },
	backNumber: { u: 0.47, size: 0.16, maxW: 1.35 },
	name: { u: 0.86, size: 0.036, maxW: 1.3 },
} as const;

// The jersey's sheet: his numbers and name laid out flat, in feet round his
// torso - across from his right side round to his left, the chest in the
// middle and the back at the ends.
type Sheet = {
	data: Uint8ClampedArray;
	w: number;
	h: number;
	ppf: number;
	aS: number;
	u0: number;
};
const sheets = new WeakMap<FieldLook, Map<number, Sheet | null>>();

const jerseySheet = (
	look: FieldLook,
	body: Body,
	ppf0: number,
	waist: number,
): Sheet | undefined => {
	if (!look.jerseyNumber || typeof document === "undefined") {
		return undefined;
	}
	const step = Math.round(Math.log(Math.max(6, ppf0)) / Math.log(1.25));
	let byStep = sheets.get(look);
	if (!byStep) {
		byStep = new Map();
		sheets.set(look, byStep);
	}
	const kept = byStep.get(step);
	if (kept !== undefined) {
		return kept ?? undefined;
	}
	const ppf = 1.25 ** step;
	const aS = body.shoulderW * 1.15;
	const T = body.torso;
	const u0 = shouldersOf(body) + body.H * 0.02;
	const w = Math.max(8, Math.round(2 * Math.PI * aS * ppf));
	const h = Math.max(8, Math.round((u0 - waist) * ppf) + 2);
	const cv = document.createElement("canvas");
	cv.width = w;
	cv.height = h;
	const g = cv.getContext("2d", { willReadFrequently: true });
	if (!g) {
		byStep.set(step, null);
		return undefined;
	}
	const kit = look.kit;
	// Block numbers, the football way: heavy, square, edged.
	const text = (
		str: string,
		x: number,
		u: number,
		size: number,
		maxW: number,
		block: boolean,
		color: string,
	) => {
		if (!str) {
			return;
		}
		const px = size * ppf;
		g.save();
		g.translate(x, (u0 - u) * ppf);
		g.scale(block ? 0.86 : 0.92, 1);
		g.font = block
			? `900 ${px}px "Arial Black", "Helvetica Neue", Arial, sans-serif`
			: `bold ${px}px "Arial Narrow", "Helvetica Neue", Arial, sans-serif`;
		g.textAlign = "center";
		g.textBaseline = "middle";
		g.lineJoin = "round";
		if (block) {
			g.lineWidth = Math.max(1, px * 0.12);
			g.strokeStyle = kit.numberEdge;
			g.strokeText(str, 0, 0, maxW * ppf);
		}
		g.fillStyle = color;
		g.fillText(str, 0, 0, maxW * ppf);
		g.restore();
	};
	const L = FIELD_LETTERING;
	// (An official's number on his back only.)
	text(
		look.official ? "" : look.jerseyNumber,
		w / 2,
		T * L.number.u,
		body.H * L.number.size,
		aS * L.number.maxW,
		true,
		kit.number,
	);
	for (const x of [0, w]) {
		text(
			look.jerseyNumber,
			x,
			T * L.backNumber.u,
			body.H * L.backNumber.size,
			aS * L.backNumber.maxW,
			true,
			kit.number,
		);
		text(
			look.lastName.toUpperCase(),
			x,
			T * L.name.u,
			body.H * L.name.size,
			aS * L.name.maxW,
			false,
			kit.name,
		);
	}
	const sheet = {
		data: g.getImageData(0, 0, w, h).data,
		w,
		h,
		ppf,
		aS,
		u0,
	};
	byStep.set(step, sheet);
	return sheet;
};

// The team's logo for the sides of his helmet, read back once.
type Decal = { data: Uint8ClampedArray; w: number; h: number };
const decals = new WeakMap<HTMLCanvasElement, Decal | null>();
const decalOf = (logo: HTMLCanvasElement | undefined): Decal | undefined => {
	if (!logo || logo.width === 0 || logo.height === 0) {
		return undefined;
	}
	const kept = decals.get(logo);
	if (kept !== undefined) {
		return kept ?? undefined;
	}
	let out: Decal | null = null;
	try {
		const g = logo.getContext("2d", { willReadFrequently: true });
		if (g) {
			out = {
				data: g.getImageData(0, 0, logo.width, logo.height).data,
				w: logo.width,
				h: logo.height,
			};
		}
	} catch {
		out = null;
	}
	decals.set(logo, out);
	return out ?? undefined;
};

// Where the parts of his uniform start and stop, in feet on his torso: U up
// his spine from his hips, S to his left, F forward.
type Cut = {
	waist: number;
	belt: number;
	top: number;
	// The neck: a V in front down to vAt below the top of his shoulders, a
	// shallow scoop behind, neckIn either side of the middle.
	neckIn: number;
	vAt: number;
	backAt: number;
	trim: number;
	hipStripe: number;
};

// ---- what the camera sees of him ---------------------------------------------

// His frame as the camera sees it: which way on him the sprite's right, down
// and toward the camera are - and, the field being seen from above, how much
// higher a point nearer the camera must be to stay on the same pixel.
type Frame = { rt: V3; dn: V3; tw: V3; zs: number; kap: number };

type Built = {
	masses: Mass[];
	joins: { a: number; b: number; x: number; y: number; reach: number }[];
	frame: Frame;
	cut: Cut;
	pel: V3;
	spine: V3;
	fwd: V3;
	sheet?: Sheet;
	decal?: Decal;
	// His helmet: its middle, which way it faces, its left, its up, and
	// how big it is.
	helm: { c: V3; f: V3; s: V3; u: V3; R: number };
	// Each arm and leg (his right, his left), for where his sleeves and
	// pants stop.
	arms: [Limb, Limb];
	legs: [Limb, Limb];
	ball?: HeldBall;
	// The middle of the helmet in his hand, if he has it off.
	held?: V3;
	// Which way each palm faces.
	pn: [V3, V3];
	untwist?: Float64Array;
	// Sprite pixels to a foot, at his middle.
	k: number;
};

// How much of his upper body's turn a point of his torso t up his spine
// (0 his hips, 1 his chest) takes.
const turnShare = (t: number): number => {
	const u = Math.min(1, Math.max(0, t));
	return u * u * (3 - 2 * u);
};

// The turn undone, a matrix about his hips for each of these steps up his
// torso (for painting what he wears where it was made to sit).
const TURN_STEPS = 16;
const untwistTable = (q: Pose, pel: V3): Float64Array => {
	const out = new Float64Array((TURN_STEPS + 1) * 9);
	for (let i = 0; i <= TURN_STEPS; i++) {
		const back = turnUpper(q, pel, turnShare(i / TURN_STEPS), true);
		const cols = [v3(1, 0, 0), v3(0, 1, 0), v3(0, 0, 1)].map((e) =>
			subV(back(addV(pel, e)), pel),
		);
		for (let c = 0; c < 3; c++) {
			out[i * 9 + c] = cols[c]!.f;
			out[i * 9 + 3 + c] = cols[c]!.s;
			out[i * 9 + 6 + c] = cols[c]!.u;
		}
	}
	return out;
};

// His helmet, as big round his head as it is (head radii): a cartoon's
// head, a cartoon's helmet. (An official's bare head, in helmet radii.)
const HELMET = 1.1;
const OFFICIAL_HEAD = 0.9;
// A coach's headset band: this far out over his cap (head radii), round
// this far either way of straight up, in so many pieces.
const BAND = 0.95;
const BAND_ARC = 1.45;
const BAND_STEPS = 6;
// A player's bare head, his helmet off - and the helmet in his hand, hung
// from it (helmet radii).
const BARE_HEAD = 0.86;
const HELD_DROP = 0.75;
const HELD_R = 0.9;
// An official's shirt: its stripes, how wide (feet).
const STRIPE_W = 0.15;

// The bars of each facemask, in helmet radii in its own frame: forward,
// to its left, up. Each bar runs from one side of the shell round the front
// to the other.
type Bar = [number, number, number][];
const across = (
	u: number,
	side: number,
	front: number,
	corner: number,
): Bar => [
	[side, -0.78, u + 0.06],
	[corner, -0.46, u],
	[front, 0, u - 0.02],
	[corner, 0.46, u],
	[side, 0.78, u + 0.06],
];
const MASKS: Record<FieldLook["gear"]["mask"], Bar[]> = {
	open: [
		across(-0.18, 0.7, 1.34, 1.2),
		across(-0.56, 0.62, 1.22, 1.08),
		// A strut down each side, from bar to bar.
		[
			[1.2, -0.46, -0.18],
			[1.08, -0.46, -0.56],
		],
		[
			[1.2, 0.46, -0.18],
			[1.08, 0.46, -0.56],
		],
	],
	bars: [
		across(-0.16, 0.7, 1.34, 1.2),
		across(-0.37, 0.67, 1.3, 1.16),
		across(-0.58, 0.62, 1.22, 1.08),
		[
			[1.2, -0.46, -0.16],
			[1.08, -0.46, -0.58],
		],
		[
			[1.2, 0.46, -0.16],
			[1.08, 0.46, -0.58],
		],
		// Up the middle, from the mouth to the nose.
		[
			[1.32, 0, -0.16],
			[1.21, 0, -0.6],
		],
	],
	cage: [
		across(-0.08, 0.72, 1.34, 1.22),
		across(-0.3, 0.7, 1.33, 1.19),
		across(-0.52, 0.64, 1.25, 1.11),
		[
			[1.22, -0.46, -0.08],
			[1.11, -0.46, -0.52],
		],
		[
			[1.22, 0.46, -0.08],
			[1.11, 0.46, -0.52],
		],
		// The lineman's bar up the middle, from his chin to his brow.
		[
			[1.08, 0, 0.3],
			[1.34, 0, -0.08],
			[1.24, 0, -0.62],
		],
	],
};

// Where his helmet sits: down on his pads - his neck is all but hidden in
// them - and, leaning into a stance, tipped back up off the line of his
// back, the way a man down in a stance holds his head up to see.
const NECK_UP = 0.45;
export const helmetAt = (body: Body, q: Pose): V3 => {
	const sk = skeleton(body, { ...q, twist: 0, tilt: 0 });
	const L = (q.lean * Math.PI) / 180;
	const Ln = L * (1 - NECK_UP);
	const reach = body.neck * 0.45 + body.headR * HELMET * 0.95;
	const c = v3(
		sk.chest.f + Math.sin(Ln) * reach + body.headR * 0.1,
		0,
		sk.chest.u + Math.cos(Ln) * reach,
	);
	return q.twist !== 0 || q.tilt !== 0 ? turnUpper(q, sk.pelvis)(c) : c;
};

const build = (
	cam: Camera,
	fig: Figure,
	body: Body,
	look: FieldLook,
	px: number,
	ox: number,
	oy: number,
): Built => {
	const q = fig.pose;
	const sk = skeleton(body, q);
	const P = (v: V3) => {
		const p = project(cam, bodyPoint(fig, v));
		return {
			x: (p.x - ox) / px,
			y: (p.y - oy) / px,
			depth: p.depth,
			k: p.k / px,
		};
	};
	const masses: Mass[] = [];
	const add = (
		A: V3,
		B: V3,
		prof: Station[],
		kind: number,
		group: number,
		side = 0,
		// The thinnest it may be drawn, in sprite pixels: a facemask's bars
		// still there from up in the stands.
		least = 0,
	) => {
		const a = P(A);
		const b = P(B);
		const k = (a.k + b.k) / 2;
		const rad = new Float32Array(RS + 1);
		let rmax = 0;
		for (let i = 0; i <= RS; i++) {
			rad[i] = Math.max(least, radiusAt(prof, i / RS) * k);
			rmax = Math.max(rmax, rad[i]!);
		}
		const dx = b.x - a.x;
		const dy = b.y - a.y;
		masses.push({
			ax: a.x,
			ay: a.y,
			dx,
			dy,
			l2: dx * dx + dy * dy,
			len: Math.hypot(dx, dy),
			za: a.depth,
			zb: b.depth,
			k,
			rad,
			rmax,
			A,
			B,
			group,
			kind,
			side,
			x0: Math.min(a.x, b.x) - rmax,
			x1: Math.max(a.x, b.x) + rmax,
			y0: Math.min(a.y, b.y) - rmax,
			y1: Math.max(a.y, b.y) + rmax,
		});
	};
	const sw = body.shoulderW;
	const dp = body.depth;
	const H = body.H;
	const pel = sk.pelvis;
	const T = body.torso;
	const lean = (q.lean * Math.PI) / 180;
	const spine = v3(Math.sin(lean), 0, Math.cos(lean));
	const fwdT = v3(spine.u, 0, -spine.f);
	const turned = q.twist !== 0 || q.tilt !== 0;
	// A point on his torso: t up the spine, s to his left, f forward - turned
	// on his hips as far up him as it is.
	const up = (t: number, s: number, f = 0): V3 => {
		const p = v3(
			pel.f + spine.f * T * t + fwdT.f * f,
			s,
			pel.u + spine.u * T * t + fwdT.u * f,
		);
		return turned ? turnUpper(q, pel, turnShare(t))(p) : p;
	};

	// The torso: belly and back, the chest out in front, the lats; across
	// the top of him, his shoulder pads under the jersey - broad, and a cap
	// over each shoulder; hips and seat.
	const torso = (A: V3, B: V3, prof: Station[]) =>
		add(A, B, prof, TORSO, G_TORSO);
	torso(up(0.04, 0), up(0.86, 0, -dp * 0.04), [
		[0, dp * 0.44],
		[0.45, dp * 0.45],
		[1, dp * 0.5],
	]);
	torso(up(0.64, -sw * 0.45, dp * 0.06), up(0.64, sw * 0.45, dp * 0.06), [
		[0, dp * 0.4],
		[0.5, dp * 0.42],
		[1, dp * 0.4],
	]);
	for (const sd of [-1, 1]) {
		torso(up(0.2, sd * sw * 0.42), up(0.72, sd * sw * 0.62), [
			[0, dp * 0.36],
			[1, dp * 0.4],
		]);
	}
	const official = look.official === true;
	if (official) {
		// (An official: no pads - just his shoulders under his shirt.)
		torso(up(0.88, -sw * 0.72), up(0.88, sw * 0.72), [
			[0, dp * 0.36],
			[0.5, dp * 0.44],
			[1, dp * 0.36],
		]);
	} else {
		torso(up(0.9, -sw * 0.86), up(0.9, sw * 0.86), [
			[0, dp * 0.42],
			[0.5, dp * 0.55],
			[1, dp * 0.42],
		]);
		// (The pads' plates stand out over his chest and his shoulder blades.)
		torso(up(0.76, -sw * 0.55, dp * 0.24), up(0.76, sw * 0.55, dp * 0.24), [
			[0, dp * 0.38],
			[0.5, dp * 0.44],
			[1, dp * 0.38],
		]);
		torso(up(0.8, -sw * 0.5, -dp * 0.2), up(0.8, sw * 0.5, -dp * 0.2), [
			[0, dp * 0.38],
			[0.5, dp * 0.42],
			[1, dp * 0.38],
		]);
		for (const sd of [-1, 1]) {
			const cap = up(0.94, sd * (sw + body.upperR * 0.4), -dp * 0.02);
			torso(cap, cap, [[0, body.upperR * 1.7]]);
		}
	}
	torso(up(0, -body.hipW), up(0, body.hipW), [
		[0, body.thighR * 1.15],
		[1, body.thighR * 1.15],
	]);
	torso(
		up(0.1, -body.hipW * 0.55, -dp * 0.1),
		up(0.1, body.hipW * 0.55, -dp * 0.1),
		[
			[0, body.thighR * 1.08],
			[1, body.thighR * 1.08],
		],
	);

	// His helmet: faced the way his shoulders are turned, but level - eyes
	// up, however far he leans.
	const twistR = turned ? ((q.twist * Math.PI) / 180) * 0.9 : 0;
	const hF = v3(Math.cos(twistR), Math.sin(twistR), 0);
	const hS = v3(-Math.sin(twistR), Math.cos(twistR), 0);
	const hU = v3(0, 0, 1);
	const R = body.headR * HELMET;
	const c = helmetAt(body, q);
	const at = (f: number, s: number, u: number): V3 =>
		addV(addV(addV(c, hF, f * R), hS, s * R), hU, u * R);
	// The neck, thick, up out of his pads into the helmet.
	add(
		up(0.9, 0, -dp * 0.02),
		at(-0.1, 0, -0.55),
		[
			[0, sw * (official ? 0.3 : 0.42)],
			[1, sw * (official ? 0.26 : 0.36)],
		],
		NECK,
		G_NECK,
	);
	let held: V3 | undefined;
	if (official) {
		// His bare head, a cap on it, and its brim out over his eyes.
		add(at(0, 0, 0), at(0, 0, 0), [[0, R * OFFICIAL_HEAD]], SHELL, G_HELMET);
		for (const sd of [-1, 1]) {
			add(
				at(0.6, sd * 0.26, 0.27),
				at(1.12, sd * 0.2, 0.2),
				[[0, R * 0.15]],
				SHELL,
				G_HELMET,
			);
		}
		if (look.headset) {
			// A coach's headset on over it: a cup over each ear, the band
			// over the top between them, the mike's boom round from his left
			// one to his mouth.
			for (const sd of [-1, 1]) {
				add(
					at(-0.02, sd * 0.84, -0.1),
					at(-0.02, sd * 0.98, -0.1),
					[[0, R * 0.25]],
					HEADSET,
					G_MASK,
				);
			}
			const band = (a: number) =>
				at(-0.02, Math.sin(a) * BAND, Math.cos(a) * BAND - 0.04);
			for (let i = 0; i < BAND_STEPS; i++) {
				const a0 = -BAND_ARC + (2 * BAND_ARC * i) / BAND_STEPS;
				const a1 = -BAND_ARC + (2 * BAND_ARC * (i + 1)) / BAND_STEPS;
				add(band(a0), band(a1), [[0, R * 0.07]], HEADSET, G_MASK, 0, 0.42);
			}
			add(
				at(0.12, -0.96, -0.2),
				at(0.8, -0.5, -0.5),
				[
					[0, R * 0.05],
					[0.92, R * 0.05],
					[1, R * 0.09],
				],
				HEADSET,
				G_MASK,
				0,
				0.42,
			);
		}
	} else if (look.bare) {
		// His bare head; his helmet hung from his left hand (his right free
		// to shake with).
		add(
			at(0, 0, -0.08),
			at(0, 0, -0.08),
			[[0, R * BARE_HEAD]],
			SHELL,
			G_HELMET,
		);
		const hand = sk.armL.tip ?? sk.armL.end;
		held = addV(hand, v3(0, 0, -1), R * HELD_DROP);
		add(held, held, [[0, R * HELD_R]], HELD, G_BALL);
	} else {
		// The shell: the dome, the back of it down over his neck, the jaw
		// guards forward either side of his face.
		add(at(-0.14, 0, 0.08), at(0.1, 0, 0.05), [[0, R]], SHELL, G_HELMET);
		add(
			at(-0.36, 0, -0.18),
			at(-0.28, 0, -0.58),
			[
				[0, R * 0.66],
				[1, R * 0.6],
			],
			SHELL,
			G_HELMET,
		);
		for (const sd of [-1, 1]) {
			add(
				at(0.0, sd * 0.56, -0.22),
				at(0.58, sd * 0.52, -0.6),
				[
					[0, R * 0.4],
					[1, R * 0.33],
				],
				SHELL,
				G_HELMET,
			);
		}
		// The facemask's bars, and the chin cup under them.
		const barR = R * 0.05;
		const barLeast = 0.42;
		for (const bar of MASKS[look.gear.mask]) {
			for (let i = 1; i < bar.length; i++) {
				const [f0, s0, u0] = bar[i - 1]!;
				const [f1, s1, u1] = bar[i]!;
				add(
					at(f0, s0, u0),
					at(f1, s1, u1),
					[[0, barR]],
					MASK,
					G_MASK,
					0,
					barLeast,
				);
			}
		}
		add(at(0.86, 0, -0.86), at(0.86, 0, -0.86), [[0, R * 0.17]], CHIN, G_MASK);
	}

	const fwd = v3(1, 0, 0);
	const upV = v3(0, 0, 1);
	const kMid = P(pel).k;
	const pn: [V3, V3] = [v3(0, 1, 0), v3(0, -1, 0)];
	const knuckleAt: V3[] = [];
	const thumbAt: V3[] = [];
	const arm = (limb: Limb, side: 0 | 1) => {
		const g = G_ARM + side;
		const a = unit(subV(limb.mid, limb.root));
		const cc = unit(subV(limb.end, limb.mid));
		const bend = square(cc, a);
		const inside = lenV(bend) > 0.08 ? unit(bend) : unit(square(fwd, a), upV);
		add(
			limb.root,
			limb.mid,
			[
				[0, body.upperR * 1.2],
				[0.2, body.upperR * 1.12],
				[0.5, body.upperR * 0.94],
				[1, body.foreR * 0.88],
			],
			UPPER,
			g,
			side,
		);
		add(
			addV(lerpV(limb.root, limb.mid, 0.36), inside, body.upperR * 0.32),
			addV(lerpV(limb.root, limb.mid, 0.78), inside, body.upperR * 0.28),
			[
				[0, body.upperR * 0.64],
				[0.5, body.upperR * 0.76],
				[1, body.upperR * 0.58],
			],
			UPPER,
			g,
			side,
		);
		add(
			addV(limb.mid, inside, -body.foreR * 0.36),
			addV(limb.mid, inside, -body.foreR * 0.36),
			[[0, body.foreR * 0.64]],
			FORE,
			g,
			side,
		);
		add(
			limb.mid,
			limb.end,
			[
				[0, body.foreR * 0.92],
				[0.22, body.foreR * 1.06],
				[0.7, body.foreR * 0.8],
				[1, body.foreR * 0.6],
			],
			FORE,
			g,
			side,
		);
		hand(limb, side, cc);
	};

	// The hand: a palm out of the wrist, four fingers and a thumb - a
	// cartoon's, a little bigger than life, so it reads.
	const hand = (limb: Limb, side: 0 | 1, foreDir: V3) => {
		const sgn = side === 0 ? -1 : 1;
		const grip = fig.grips[side];
		const wrist = limb.end;
		const tip = limb.tip ?? limb.end;
		let h = lenV(subV(tip, wrist)) > 1e-3 ? unit(subV(tip, wrist)) : foreDir;
		const inward = v3(0, -sgn, 0);
		let palmN = unit(
			square(addV(inward, fwd, -0.75 + 1.9 * Math.max(0, h.u)), h),
			inward,
		);
		const ball = fig.ball;
		if (ball && grip === "ball") {
			// Palm onto the ball, fingers along it.
			const onto = unit(subV(ball.at, addV(wrist, h, H * 0.04)));
			palmN = onto;
			h = unit(square(addV(h, ball.axis, 0.6), onto), h);
		}
		if (h.u < -0.75 && grip === "open") {
			// Flat on the turf: palm down.
			palmN = unit(square(v3(0, 0, -1), h), v3(0, 0, -1));
		}
		const thumbSide = unit(cross(palmN, h), fwd);
		const ts = v3(thumbSide.f * sgn, thumbSide.s * sgn, thumbSide.u * sgn);
		const g = G_HAND + side;
		pn[side] = palmN;
		const L = H * 0.124;
		const halfW = H * 0.017;
		const palmR = H * 0.019;
		const knuckles = addV(wrist, h, L * 0.5);
		for (const o of [-1, 1]) {
			add(
				addV(addV(wrist, h, L * 0.1), ts, o * halfW * 0.8),
				addV(knuckles, ts, o * halfW),
				[
					[0, palmR * 0.92],
					[1, palmR],
				],
				HAND,
				g,
				side,
			);
		}
		const curl =
			grip === "fist"
				? 1.95
				: grip === "relaxed"
					? 0.6
					: grip === "ball"
						? 0.5
						: 0.15;
		const fingerR = H * 0.0092;
		const lens = [0.4, 0.44, 0.41, 0.33];
		const bendTo = (d: V3, cv: number) =>
			unit(
				addV(
					v3(d.f * Math.cos(cv), d.s * Math.cos(cv), d.u * Math.cos(cv)),
					palmN,
					Math.sin(cv),
				),
			);
		if (fingerR * kMid < 1.4) {
			// Too small to tell apart: the four as one, flat like the palm.
			const dir = bendTo(h, curl);
			const len = L * 0.4 * (curl > 1.2 ? 0.55 : 1);
			for (const o of [-1, 1]) {
				const base = addV(knuckles, ts, o * halfW * 0.85);
				add(
					base,
					addV(base, dir, len),
					[
						[0, palmR * 0.95],
						[1, palmR * 0.8],
					],
					HAND,
					G_FINGER + side * 5,
					side,
				);
			}
		} else {
			for (let i = 0; i < 4; i++) {
				const a = (1.5 - i) / 1.5;
				const base = addV(knuckles, ts, a * halfW * 1.25);
				const spread = grip === "open" ? 0.08 : grip === "ball" ? 0.14 : 0.02;
				const dir = bendTo(unit(addV(h, ts, a * spread)), curl);
				const len = L * lens[i]! * (curl > 1.2 ? 0.6 : 1);
				add(
					base,
					addV(base, dir, len),
					[
						[0, fingerR],
						[0.7, fingerR * 0.96],
						[1, fingerR * 0.84],
					],
					HAND,
					G_FINGER + side * 5 + i,
					side,
				);
			}
		}
		const tBase = addV(addV(wrist, h, L * 0.16), ts, halfW * 1.05);
		knuckleAt[side] = knuckles;
		thumbAt[side] = tBase;
		const tTip =
			grip === "fist"
				? addV(addV(knuckles, palmN, H * 0.016), ts, halfW * 0.1)
				: addV(
						addV(
							addV(wrist, h, L * 0.5),
							ts,
							halfW * (grip === "ball" ? 2.2 : 1.9),
						),
						palmN,
						H * 0.012,
					);
		add(
			tBase,
			tTip,
			[
				[0, H * 0.011],
				[1, H * 0.009],
			],
			HAND,
			G_FINGER + side * 5 + 4,
			side,
		);
	};
	arm(sk.armR, 0);
	arm(sk.armL, 1);

	const leg = (limb: Limb, side: 0 | 1) => {
		const gl = G_LEG + side;
		const thighDir = unit(subV(limb.mid, limb.root));
		const thighFront = unit(square(fwd, thighDir), v3(0, 0, -1));
		// The thigh, in his pants from his hip down, the pad in the front of
		// it; the knee pad; the shin, with the calf swelling behind it.
		add(
			lerpV(limb.root, limb.mid, 0.3),
			limb.mid,
			[
				[0, body.thighR * 1.02],
				[0.4, body.thighR * 0.98],
				[1, body.kneeR * 1.04],
			],
			THIGH,
			gl,
			side,
		);
		add(
			addV(lerpV(limb.root, limb.mid, 0.38), thighFront, body.thighR * 0.32),
			addV(lerpV(limb.root, limb.mid, 0.74), thighFront, body.thighR * 0.3),
			[
				[0, body.thighR * 0.78],
				[1, body.thighR * 0.7],
			],
			THIGH,
			gl,
			side,
		);
		const shinDir = unit(subV(limb.end, limb.mid));
		const front = unit(square(fwd, shinDir), v3(1, 0, 0));
		add(
			addV(limb.mid, front, body.kneeR * 0.45),
			addV(limb.mid, front, body.kneeR * 0.45),
			[[0, body.kneeR * 0.82]],
			THIGH,
			gl,
			side,
		);
		add(
			limb.mid,
			limb.end,
			[
				[0, body.kneeR * 0.98],
				[0.3, body.calfR * 0.96],
				[0.8, body.ankleR * 1.12],
				[1, body.ankleR * 1.06],
			],
			SHIN,
			gl,
			side,
		);
		add(
			addV(lerpV(limb.mid, limb.end, 0.12), front, -body.calfR * 0.3),
			addV(lerpV(limb.mid, limb.end, 0.56), front, -body.calfR * 0.16),
			[
				[0, body.calfR * 0.82],
				[0.4, body.calfR * 0.9],
				[1, body.calfR * 0.64],
			],
			SHIN,
			gl,
			side,
		);
		// The cleat: heel to toe along the turf, lower than a sneaker, a
		// collar round the ankle.
		const tip = limb.tip ?? limb.end;
		const heel = lerpV(limb.end, tip, -0.32);
		const toe = lerpV(limb.end, tip, 1.06);
		add(
			v3(
				heel.f,
				heel.s,
				Math.max(body.ankleR * 1.0, heel.u - body.ankleR * 0.2),
			),
			v3(toe.f, toe.s, Math.max(body.ankleR * 0.85, toe.u - body.ankleR * 0.3)),
			[
				[0, body.ankleR * 1.24],
				[0.5, body.ankleR * 1.3],
				[1, body.ankleR * 1.05],
			],
			SHOE,
			G_SHOE + side,
			side,
		);
		const collar = lerpV(limb.end, tip, 0.25);
		add(
			lerpV(limb.end, tip, -0.12),
			v3(collar.f, collar.s, limb.end.u - body.ankleR * 0.2),
			[
				[0, body.ankleR * 1.1],
				[1, body.ankleR * 1.16],
			],
			SHOE,
			G_SHOE + side,
			side,
		);
	};
	leg(sk.legR, 0);
	leg(sk.legL, 1);

	if (look.gear.towel) {
		// A towel tucked in at the front of his pants, hanging down over his
		// right thigh.
		const top = up(0.2, -body.hipW * 0.7, dp * 0.42);
		add(
			top,
			addV(top, v3(0.04, 0, -1), body.thigh * 0.42),
			[
				[0, body.thighR * 0.42],
				[1, body.thighR * 0.5],
			],
			TOWEL,
			G_TOWEL,
		);
	}

	if (fig.ball) {
		const b = fig.ball;
		add(
			addV(b.at, b.axis, -BALL_LEN / 2),
			addV(b.at, b.axis, BALL_LEN / 2),
			[
				[0, BALL_R * 0.18],
				[0.5, BALL_R],
				[1, BALL_R * 0.18],
			],
			BALL,
			G_BALL,
		);
	}

	const joins: Built["joins"] = [];
	const join = (a: number, b: number, at: V3, reachFt: number) => {
		const p = P(at);
		joins.push({ a, b, x: p.x, y: p.y, reach: reachFt * p.k });
	};
	join(G_TORSO, G_ARM, sk.armR.root, body.upperR * 3.2);
	join(G_TORSO, G_ARM + 1, sk.armL.root, body.upperR * 3.2);
	join(G_TORSO, G_LEG, sk.legR.root, body.thighR * 2.8);
	join(G_TORSO, G_LEG + 1, sk.legL.root, body.thighR * 2.8);
	join(G_LEG, G_LEG + 1, up(-0.1, 0), body.thighR * 2.2);
	join(G_TORSO, G_NECK, up(0.95, 0), sw * 0.8);
	for (const side of [0, 1]) {
		for (let i = 0; i < 4; i++) {
			join(G_HAND + side, G_FINGER + side * 5 + i, knuckleAt[side]!, H * 0.04);
		}
		join(G_HAND + side, G_FINGER + side * 5 + 4, thumbAt[side]!, H * 0.035);
		join(
			G_ARM + side,
			G_HAND + side,
			side === 0 ? sk.armR.end : sk.armL.end,
			H * 0.03,
		);
	}

	// The camera's frame, in his.
	const cy = Math.cos(fig.yaw);
	const sy = Math.sin(fig.yaw);
	const toBody = (wx: number, wy: number): V3 =>
		v3(wx * cy + wy * sy, wx * sy - wy * cy, 0);
	const fh = Math.hypot(cam.fwd.x, cam.fwd.y) || 1;
	const frame: Frame = {
		rt: toBody(cam.right.x, cam.right.y),
		dn: v3(0, 0, -1),
		tw: toBody(-cam.fwd.x / fh, -cam.fwd.y / fh),
		zs: fh,
		kap: (cam.fwd.x * cam.up.x + cam.fwd.y * cam.up.y) / fh,
	};
	const cut: Cut = {
		waist: tuckOf(body),
		belt: H * 0.014,
		top: shouldersOf(body),
		neckIn: sw * 0.4,
		vAt: H * 0.05,
		backAt: H * 0.02,
		trim: Math.max(H * 0.012, 1.2 / kMid),
		hipStripe: body.hipW + body.thighR * 0.8,
	};
	return {
		masses,
		joins,
		frame,
		cut,
		pel,
		spine,
		fwd: fwdT,
		sheet: jerseySheet(look, body, kMid * 1.2, cut.waist),
		decal: decalOf(look.logo),
		helm: { c, f: hF, s: hS, u: hU, R },
		arms: [sk.armR, sk.armL],
		legs: [sk.legR, sk.legL],
		...(fig.ball ? { ball: fig.ball } : {}),
		...(held ? { held } : {}),
		pn,
		...(turned ? { untwist: untwistTable(q, pel) } : {}),
		k: kMid,
	};
};

// ---- painting him -------------------------------------------------------------

type Palette = {
	helmet: RGB;
	stripe?: RGB;
	stripeEdge?: RGB;
	mask: RGB;
	jersey: RGB;
	collar: RGB;
	sleeveStripes: RGB[];
	pants: RGB;
	belt: RGB;
	pantsStripes: RGB[];
	sock: RGB;
	skin: RGB;
	cleat: RGB;
	sole: RGB;
	accent: RGB;
	// His hands: gloves (the palms their own shade), tape, or skin.
	hands: [RGB, RGB];
	palms: [RGB, RGB];
	sleeve: [RGB | undefined, RGB | undefined];
	wrist: [RGB | undefined, RGB | undefined];
	visor?: RGB;
	// An official's: the black of his striped shirt, and his hair under
	// his cap.
	stripes?: RGB;
	// The chain crew's vest over the shirt.
	vest?: RGB;
	hair: RGB;
};

const palettes = new WeakMap<FieldLook, Palette>();
const TAPE: RGB = [238, 236, 230];
const paletteOf = (look: FieldLook): Palette => {
	let p = palettes.get(look);
	if (p) {
		return p;
	}
	const kit = look.kit;
	const gear = look.gear;
	const skin = rgb(look.skin);
	const both = <T>(f: (w: "R" | "L") => T): [T, T] => [f("R"), f("L")];
	const gloved = (w: "R" | "L") => gear.glove?.hands.includes(w) === true;
	p = {
		helmet: rgb(kit.helmet),
		...(kit.helmetStripe ? { stripe: rgb(kit.helmetStripe) } : {}),
		...(kit.helmetStripeEdge ? { stripeEdge: rgb(kit.helmetStripeEdge) } : {}),
		mask: rgb(kit.facemask),
		jersey: rgb(kit.jersey),
		collar: rgb(kit.collar),
		sleeveStripes: kit.sleeveStripes.map(rgb),
		pants: rgb(kit.pants),
		belt: scaled(rgb(kit.pants), 0.8),
		pantsStripes: kit.pantsStripes.map(rgb),
		sock: rgb(kit.sock),
		skin,
		cleat: rgb(gear.cleat),
		sole: rgb(gear.cleatSole),
		accent: rgb(gear.cleatAccent),
		hands: both((w) =>
			gloved(w) ? rgb(gear.glove!.color) : gear.tape ? TAPE : skin,
		),
		palms: both((w) =>
			gloved(w)
				? rgb(gear.glove!.palm)
				: gear.tape
					? TAPE
					: toward(skin, [232, 186, 166], 0.42),
		),
		sleeve: both((w) =>
			gear.sleeve?.arms.includes(w) ? rgb(gear.sleeve.color) : undefined,
		),
		wrist: both((w) =>
			gear.wrist?.arms.includes(w)
				? rgb(gear.wrist.color)
				: gear.tape
					? TAPE
					: undefined,
		),
		...(gear.visor === "dark" ? { visor: [24, 26, 32] as RGB } : {}),
		...(kit.stripes ? { stripes: rgb(kit.stripes) } : {}),
		...(kit.vest ? { vest: rgb(kit.vest) } : {}),
		hair: look.hair ? rgb(look.hair) : scaled(skin, 0.38),
	};
	palettes.set(look, p);
	return p;
};

// The light, from up and to the left and toward the camera (on the sprite:
// right, down, out of the picture) - the court's.
const LIGHT = (() => {
	const l = [-0.45, -0.7, 0.55];
	const n = Math.hypot(l[0]!, l[1]!, l[2]!);
	return [l[0]! / n, l[1]! / n, l[2]! / n] as const;
})();

const len2 = (a: number, b: number) => Math.sqrt(a * a + b * b);
const len3 = (a: number, b: number, c: number) =>
	Math.sqrt(a * a + b * b + c * c);

const smin = (a: number, b: number, k: number) => {
	const h = Math.max(k - Math.abs(a - b), 0) / k;
	return Math.min(a, b) - (h * h * k) / 4;
};

export type Sculpted = {
	img: ImageData;
	// The box round everything drawn (sprite pixels, ends included); empty
	// (x0 > x1) if nothing was.
	box: { x0: number; y0: number; x1: number; y1: number };
};

const RUN = 8;

// His sprite, w x h of its pixels, each px screen pixels, its top left at
// (ox, oy) on the screen.
export const sculptFigure = (
	cam: Camera,
	fig: Figure,
	body: Body,
	look: FieldLook,
	px: number,
	ox: number,
	oy: number,
	w: number,
	h: number,
): Sculpted => {
	const b = build(cam, fig, body, look, px, ox, oy);
	const pal = paletteOf(look);
	const official = look.official === true;
	const masses = b.masses;
	const n = masses.length;
	const img = new ImageData(w, h);
	const d = img.data;
	const lineW = Math.max(0.7, 0.028 * b.k);
	const margin = lineW * 3.5 + 1;
	const joinOf = new Int16Array(GROUPS * GROUPS).fill(-1);
	b.joins.forEach((j, i) => {
		joinOf[j.a * GROUPS + j.b] = i;
		joinOf[j.b * GROUPS + j.a] = i;
	});

	const hs = new Float64Array(n);
	const hz = new Float64Array(n);
	const hx = new Float64Array(n);
	const hy = new Float64Array(n);
	const hn = new Float64Array(n);
	const ht = new Float64Array(n);
	const hr = new Float64Array(n);
	const hi = new Int32Array(n);
	const ZT = 0.9;
	const { rt, dn, tw: tw3, zs, kap } = b.frame;
	const cut = b.cut;
	const pel = b.pel;
	const spine = b.spine;
	const fw = b.fwd;
	const sheet = b.sheet;
	const helm = b.helm;
	const decal = b.decal;
	let cr = 0;
	let cg = 0;
	let cb = 0;
	let cloth = false;
	// Shiny: the helmet, its mask.
	let gloss = 0;
	let bx0 = w;
	let by0 = h;
	let bx1 = -1;
	let by1 = -1;
	const set = (c: RGB) => {
		cr = c[0];
		cg = c[1];
		cb = c[2];
	};

	// His jersey, or his pants under it, at a point on his torso (U, S, F).
	const torsoAt = (U: number, S: number, F: number) => {
		cloth = true;
		if (U < cut.waist) {
			let c = pal.pants;
			if (Math.abs(S) > cut.hipStripe && Math.abs(F) < 0.11) {
				c = pal.pantsStripes[0] ?? c;
			}
			set(c);
			return;
		}
		if (U < cut.waist + cut.belt) {
			set(pal.belt);
			return;
		}
		const aS = Math.abs(S);
		const wN = Math.min(1, aS / cut.neckIn);
		const front = F >= 0;
		const neckDrop = front ? cut.vAt * (1 - wN) : cut.backAt * (1 - wN * wN);
		const slope = front
			? cut.vAt / cut.neckIn
			: (2 * cut.backAt * wN) / cut.neckIn;
		const below = (cut.top - neckDrop - U) / len2(1, slope);
		const dNeck =
			aS >= cut.neckIn
				? Math.max(aS - cut.neckIn, below)
				: below > 0
					? below
					: Math.max(below, aS - cut.neckIn);
		if (dNeck < 0) {
			cloth = false;
			set(pal.skin);
			return;
		}
		if (dNeck < cut.trim) {
			set(pal.collar);
			return;
		}
		let c = pal.vest ?? pal.jersey;
		if (pal.stripes) {
			// An official's shirt, striped up and down.
			const arc = Math.atan2(S, F) * body.shoulderW * 1.15;
			if ((Math.floor(arc / STRIPE_W) & 1) === 1) {
				c = pal.stripes;
			}
		}
		if (sheet) {
			// His numbers and name: round his torso by how far across it the
			// point is, so their uprights stay upright over his chest.
			const acr = Math.max(-1, Math.min(1, S / sheet.aS));
			const th =
				F >= 0
					? Math.asin(acr)
					: (acr < 0 ? -Math.PI : Math.PI) - Math.asin(acr);
			let x = sheet.w / 2 + th * sheet.aS * sheet.ppf;
			x = ((x % sheet.w) + sheet.w) % sheet.w;
			const y = (sheet.u0 - U) * sheet.ppf;
			if (y >= 0 && y < sheet.h - 1) {
				const x0 = Math.floor(x);
				const y0 = Math.floor(y);
				const fx = x - x0;
				const fy = y - y0;
				const x1 = (x0 + 1) % sheet.w;
				const sd = sheet.data;
				const sw4 = sheet.w * 4;
				const o00 = y0 * sw4 + x0 * 4;
				const o10 = y0 * sw4 + x1 * 4;
				const o01 = o00 + sw4;
				const o11 = o10 + sw4;
				const w00 = ((1 - fx) * (1 - fy) * sd[o00 + 3]!) / 255;
				const w10 = (fx * (1 - fy) * sd[o10 + 3]!) / 255;
				const w01 = ((1 - fx) * fy * sd[o01 + 3]!) / 255;
				const w11 = (fx * fy * sd[o11 + 3]!) / 255;
				const a = w00 + w10 + w01 + w11;
				if (a > 0.004) {
					const r =
						sd[o00]! * w00 + sd[o10]! * w10 + sd[o01]! * w01 + sd[o11]! * w11;
					const gg =
						sd[o00 + 1]! * w00 +
						sd[o10 + 1]! * w10 +
						sd[o01 + 1]! * w01 +
						sd[o11 + 1]! * w11;
					const bb =
						sd[o00 + 2]! * w00 +
						sd[o10 + 2]! * w10 +
						sd[o01 + 2]! * w01 +
						sd[o11 + 2]! * w11;
					cr = c[0] + (r / a - c[0]) * a;
					cg = c[1] + (gg / a - c[1]) * a;
					cb = c[2] + (bb / a - c[2]) * a;
					return;
				}
			}
		}
		set(c);
	};

	// How far along a limb (0 its root, 1 its middle joint) a point is.
	const alongOf = (l: Limb, pf: number, ps: number, pu: number) => {
		const lf = l.mid.f - l.root.f;
		const ls = l.mid.s - l.root.s;
		const lu = l.mid.u - l.root.u;
		return (
			((pf - l.root.f) * lf + (ps - l.root.s) * ls + (pu - l.root.u) * lu) /
			(lf * lf + ls * ls + lu * lu || 1)
		);
	};
	const belowOf = (l: Limb, pf: number, ps: number, pu: number) => {
		const lf = l.end.f - l.mid.f;
		const ls = l.end.s - l.mid.s;
		const lu = l.end.u - l.mid.u;
		return (
			((pf - l.mid.f) * lf + (ps - l.mid.s) * ls + (pu - l.mid.u) * lu) /
			(lf * lf + ls * ls + lu * lu || 1)
		);
	};
	// Which of an official's stripes a point on his sleeve is in, by how far
	// round his arm it is.
	const roundArm = (l: Limb, pf: number, ps: number, pu: number, a: number) => {
		const lf = l.mid.f - l.root.f;
		const ls = l.mid.s - l.root.s;
		const lu = l.mid.u - l.root.u;
		const ll = len3(lf, ls, lu) || 1;
		const rf = pf - l.root.f - lf * a;
		const rs = ps - l.root.s - ls * a;
		const ru = pu - l.root.u - lu * a;
		// Across the arm, level, and the other way round it.
		let e1f = -ls / ll;
		let e1s = lf / ll;
		const e1l = Math.hypot(e1f, e1s);
		if (e1l < 0.2) {
			e1f = 0;
			e1s = 1;
		} else {
			e1f /= e1l;
			e1s /= e1l;
		}
		const e2f = -(lu / ll) * e1s;
		const e2s = (lu / ll) * e1f;
		const e2u = (lf / ll) * e1s - (ls / ll) * e1f;
		const x = rf * e1f + rs * e1s;
		const y = rf * e2f + rs * e2s + ru * e2u;
		const arc = Math.atan2(y, x) * body.upperR;
		return Math.floor(arc / (STRIPE_W * 0.8)) & 1;
	};
	// Bands round a sleeve or a sock: which, if any, at `at` (0 to 1 down
	// the band's run), each band a share of it with a gap after.
	const bandOf = (bands: RGB[], at: number): RGB | undefined => {
		if (bands.length === 0 || at < 0 || at >= 1) {
			return undefined;
		}
		const i = Math.floor(at * bands.length);
		const f = at * bands.length - i;
		return f < 0.62 ? bands[i] : undefined;
	};

	const rowMass = new Int32Array(n);
	const rowLo = new Float64Array(n);
	const rowHi = new Float64Array(n);
	const rowDot = new Float64Array(n);
	const runMass = new Int32Array(n);
	const runLo = new Float64Array(n);
	const runHi = new Float64Array(n);
	const runDot = new Float64Array(n);
	for (let y = 0; y < h; y++) {
		const py = y + 0.5;
		let na = 0;
		let rowL = Infinity;
		let rowR = -Infinity;
		for (let i = 0; i < n; i++) {
			const c = masses[i]!;
			if (py < c.y0 - margin || py > c.y1 + margin) {
				continue;
			}
			let lo = c.x0 - margin;
			let hi = c.x1 + margin;
			if (c.dy > 1e-6 || c.dy < -1e-6) {
				const mid = c.ax + ((py - c.ay) * c.dx) / c.dy;
				const half = ((c.rmax + margin) * c.len) / Math.abs(c.dy) + 0.01;
				lo = Math.max(lo, mid - half);
				hi = Math.min(hi, mid + half);
			}
			if (lo > w || hi < 0 || lo > hi) {
				continue;
			}
			rowMass[na] = i;
			rowLo[na] = lo;
			rowHi[na] = hi;
			rowDot[na] = (py - c.ay) * c.dy;
			na++;
			rowL = Math.min(rowL, lo);
			rowR = Math.max(rowR, hi);
		}
		if (na === 0) {
			continue;
		}
		const rowTo = Math.min(w - 1, Math.floor(rowR - 0.5));
		for (
			let xStart = Math.max(0, Math.ceil(rowL - 0.5));
			xStart <= rowTo;
			xStart += RUN
		) {
			const xEnd = Math.min(rowTo + 1, xStart + RUN);
			let nb = 0;
			let runL = Infinity;
			let runR = -Infinity;
			for (let k = 0; k < na; k++) {
				const lo = rowLo[k]!;
				const hi = rowHi[k]!;
				if (lo > xEnd || hi < xStart) {
					continue;
				}
				runMass[nb] = rowMass[k]!;
				runLo[nb] = lo;
				runHi[nb] = hi;
				runDot[nb] = rowDot[k]!;
				nb++;
				runL = Math.min(runL, lo);
				runR = Math.max(runR, hi);
			}
			if (nb === 0) {
				continue;
			}
			const xTo = Math.min(xEnd - 1, Math.floor(runR - 0.5));
			for (let x = Math.max(xStart, Math.ceil(runL - 0.5)); x <= xTo; x++) {
				const pxx = x + 0.5;
				let m = 0;
				let best = -1;
				let bestZ = Infinity;
				let least = -1;
				let leastS = Infinity;
				for (let k = 0; k < nb; k++) {
					if (pxx < runLo[k]! || pxx > runHi[k]!) {
						continue;
					}
					const i = runMass[k]!;
					const c = masses[i]!;
					const raw =
						c.l2 > 1e-6 ? ((pxx - c.ax) * c.dx + runDot[k]!) / c.l2 : 0;
					const t = raw < 0 ? 0 : raw > 1 ? 1 : raw;
					const ex = pxx - (c.ax + c.dx * t);
					const ey = py - (c.ay + c.dy * t);
					const dist = Math.sqrt(ex * ex + ey * ey);
					const fi = t * RS;
					const i0 = fi | 0;
					const r =
						i0 >= RS
							? c.rad[RS]!
							: c.rad[i0]! + (c.rad[i0 + 1]! - c.rad[i0]!) * (fi - i0);
					const s = dist - r;
					if (s > margin) {
						continue;
					}
					const qn = dist < r ? dist / r : 1;
					const nz = Math.sqrt(1 - qn * qn);
					const z = c.za + (c.zb - c.za) * t - (nz * r * zs) / c.k;
					const inv = dist > 1e-6 ? qn / dist : 0;
					hs[m] = s;
					hz[m] = z;
					hx[m] = ex * inv;
					hy[m] = ey * inv;
					hn[m] = nz;
					ht[m] = t;
					hr[m] = r;
					hi[m] = i;
					if (s <= 0 && z < bestZ) {
						bestZ = z;
						best = m;
					}
					if (s < leastS) {
						leastS = s;
						least = m;
					}
					m++;
				}
				if (m === 0 || leastS > 0.5 + margin) {
					continue;
				}
				const f = best >= 0 ? best : least;
				const fc = masses[hi[f]!]!;
				let S = hs[f]!;
				let nx = hx[f]!;
				let ny = hy[f]!;
				let nzs = hn[f]!;
				let behind = false;
				let shadow = 0;
				for (let j = 0; j < m; j++) {
					if (j === f) {
						continue;
					}
					const c = masses[hi[j]!]!;
					const dz = hz[j]! - hz[f]!;
					let K = 0;
					if (c.group === fc.group) {
						K = Math.min(hr[j]!, hr[f]!) * 0.8;
					} else {
						const ji = joinOf[fc.group * GROUPS + c.group]!;
						if (ji >= 0) {
							const jn = b.joins[ji]!;
							const dj = len2(pxx - jn.x, py - jn.y);
							K = jn.reach * 0.45 * Math.max(0, 1 - dj / jn.reach);
						}
					}
					if (K > 0 && dz < ZT && dz > -ZT) {
						K *= 1 - Math.abs(dz) / ZT;
						const before = S;
						S = smin(S, hs[j]!, K);
						const wgt = 1 - Math.abs(hs[j]! - before) / K;
						if (wgt > 0) {
							nx += hx[j]! * wgt;
							ny += hy[j]! * wgt;
							nzs += hn[j]! * wgt;
						}
					} else if (hs[j]! < 0 && dz > 0.05) {
						behind = true;
					} else if (dz < -0.05 && hs[j]! > 0) {
						const sw = lineW * 3.5;
						if (hs[j]! < sw) {
							shadow = Math.max(shadow, 1 - hs[j]! / sw);
						}
					}
				}
				const cover = 0.5 - S;
				if (cover <= 0) {
					continue;
				}
				const nl = len3(nx, ny, nzs) || 1;
				let sx = nx / nl;
				let sy = ny / nl;
				let sz = nzs / nl;
				// The point on him this pixel shows (feet, his frame).
				const t = ht[f]!;
				const rf = hr[f]! / fc.k;
				const ox3 = hx[f]! * rf;
				const oy3 = hy[f]! * rf;
				const oz3 = hn[f]! * rf;
				const Pf =
					fc.A.f +
					(fc.B.f - fc.A.f) * t +
					ox3 * rt.f +
					oy3 * dn.f +
					oz3 * tw3.f;
				const Ps =
					fc.A.s +
					(fc.B.s - fc.A.s) * t +
					ox3 * rt.s +
					oy3 * dn.s +
					oz3 * tw3.s;
				const Pu =
					fc.A.u +
					(fc.B.u - fc.A.u) * t +
					ox3 * rt.u +
					oy3 * dn.u +
					oz3 * (tw3.u + kap);
				cloth = false;
				gloss = 0;
				const side = fc.side;
				switch (fc.kind) {
					case TORSO: {
						let rf0 = Pf - pel.f;
						let rs0 = Ps - pel.s;
						let ru0 = Pu - pel.u;
						const ut = b.untwist;
						if (ut) {
							const up0 = (rf0 * spine.f + ru0 * spine.u) / body.torso;
							const k =
								Math.min(
									TURN_STEPS,
									Math.max(0, Math.round(up0 * TURN_STEPS)),
								) * 9;
							const nf = ut[k]! * rf0 + ut[k + 1]! * rs0 + ut[k + 2]! * ru0;
							const ns = ut[k + 3]! * rf0 + ut[k + 4]! * rs0 + ut[k + 5]! * ru0;
							const nu = ut[k + 6]! * rf0 + ut[k + 7]! * rs0 + ut[k + 8]! * ru0;
							rf0 = nf;
							rs0 = ns;
							ru0 = nu;
						}
						torsoAt(
							rf0 * spine.f + ru0 * spine.u,
							rs0,
							rf0 * fw.f + ru0 * fw.u,
						);
						break;
					}
					case UPPER:
					case FORE: {
						const limb = b.arms[side]!;
						const a = fc.kind === UPPER ? alongOf(limb, Pf, Ps, Pu) : 1;
						let c: RGB;
						if (a < SLEEVE) {
							// His jersey's sleeve, banded near its hem - or, an
							// official's, striped down the arm.
							c =
								bandOf(pal.sleeveStripes, (a - (SLEEVE - 0.17)) / 0.13) ??
								pal.jersey;
							if (pal.stripes && roundArm(limb, Pf, Ps, Pu, a) === 1) {
								c = pal.stripes;
							}
							cloth = true;
						} else {
							c = pal.sleeve[side] ?? pal.skin;
							const bl = fc.kind === FORE ? belowOf(limb, Pf, Ps, Pu) : 0;
							const band = pal.wrist[side];
							if (band && bl > 0.8) {
								c = band;
							}
							cloth = c !== pal.skin;
						}
						set(c);
						break;
					}
					case THIGH: {
						cloth = true;
						const limb = b.legs[side]!;
						// The stripes down the outside of the leg.
						const a = alongOf(limb, Pf, Ps, Pu);
						const of = Pf - limb.root.f - (limb.mid.f - limb.root.f) * a;
						const os = Ps - limb.root.s - (limb.mid.s - limb.root.s) * a;
						const ou = Pu - limb.root.u - (limb.mid.u - limb.root.u) * a;
						const outward =
							(os * (side === 0 ? -1 : 1)) / (len3(of, os, ou) || 1);
						let c = pal.pants;
						const st = pal.pantsStripes;
						if (st.length > 0 && outward > 0.92) {
							c = st.length > 1 && outward < 0.975 ? st[1]! : st[0]!;
						}
						set(c);
						break;
					}
					case SHIN: {
						cloth = true;
						const limb = b.legs[side]!;
						const bl = belowOf(limb, Pf, Ps, Pu);
						set(bl < PANTS_HEM ? pal.pants : pal.sock);
						break;
					}
					case SHOE: {
						cloth = true;
						let c = Pu < body.ankleR * 0.5 ? pal.sole : pal.cleat;
						if (c === pal.cleat) {
							const lf = fc.B.f - fc.A.f;
							const ls = fc.B.s - fc.A.s;
							const lu = fc.B.u - fc.A.u;
							const ll = len3(lf, ls, lu) || 1;
							if (ll > body.ankleR * 4) {
								const of = Pf - fc.A.f - lf * t;
								const os = Ps - fc.A.s - ls * t;
								const ou = Pu - fc.A.u - lu * t;
								const ol = len3(of, os, ou) || 1;
								const upn = ou / ol;
								const out = Math.abs(of * ls - os * lf) / (ll * ol);
								const mid = 0.38 - (0.5 * (t - 0.15)) / 0.7;
								const wide = 0.26 * (1 - Math.abs(t - 0.5) * 1.6);
								if (
									out > 0.5 &&
									t > 0.15 &&
									t < 0.85 &&
									Math.abs(upn - mid) < wide
								) {
									c = pal.accent;
								}
							}
						}
						set(c);
						break;
					}
					case NECK: {
						set(pal.skin);
						break;
					}
					case SHELL: {
						// On the helmet, in its own frame (helmet radii).
						const qf = Pf - helm.c.f;
						const qs = Ps - helm.c.s;
						const qu = Pu - helm.c.u;
						const R = helm.R;
						const lf = (qf * helm.f.f + qs * helm.f.s + qu * helm.f.u) / R;
						const ls = (qf * helm.s.f + qs * helm.s.s + qu * helm.s.u) / R;
						const lu = (qf * helm.u.f + qs * helm.u.s + qu * helm.u.u) / R;
						gloss = 1;
						let c = pal.helmet;
						if (look.bare) {
							// His face, his eyes; his hair over the top and round
							// the back.
							gloss = 0;
							if (lf > 0.3 && lu < 0.42) {
								const eye =
									lu > -0.02 &&
									lu < 0.16 &&
									Math.abs(ls) > 0.12 &&
									Math.abs(ls) < 0.42;
								c = scaled(pal.skin, eye ? 0.34 : 0.96);
							} else {
								c = lu > 0.3 || lf < -0.25 ? pal.hair : pal.skin;
							}
							set(c);
							break;
						}
						if (official) {
							// His cap, and its brim; under it his face, his
							// eyes in its shadow; his hair round the back.
							gloss = 0;
							cloth = true;
							if (lu > 0.22 || lf > 0.96) {
								set(c);
								break;
							}
							cloth = false;
							if (lf > 0.3) {
								const eye =
									lu > -0.08 &&
									lu < 0.1 &&
									Math.abs(ls) > 0.12 &&
									Math.abs(ls) < 0.42;
								c = scaled(pal.skin, eye ? 0.34 : lu > 0.06 ? 0.8 : 0.96);
							} else {
								c = lf < -0.2 || lu > 0.05 ? pal.hair : pal.skin;
							}
							set(c);
							break;
						}
						if (lf > 0.42 && Math.abs(ls) < 0.62 && lu < 0.2 && lu > -0.9) {
							// His face, in the shadow of the helmet - his eyes
							// darker still under the brow, behind a dark visor if
							// he wears one.
							gloss = 0;
							const eyes = lu > -0.12 && lu < 0.12;
							if (eyes && pal.visor) {
								c = pal.visor;
								gloss = 1;
							} else {
								c = scaled(pal.skin, eyes ? 0.42 : 0.72);
							}
						} else if (
							pal.stripe &&
							Math.abs(ls) < 0.14 &&
							lu > -0.15 &&
							lf < 0.5
						) {
							c =
								Math.abs(ls) > 0.1 && pal.stripeEdge
									? pal.stripeEdge
									: pal.stripe;
						} else if (decal && Math.abs(ls) > 0.45) {
							// The logo on each side, facing forward on both.
							// (Seen from his right, forward is to the right of the
							// picture; from his left, to its left.)
							const half = 0.5;
							const xx = ((lf + 0.06) / half + 1) / 2;
							const yy = (1 - (lu - 0.02) / half) / 2;
							const fx = ls < 0 ? xx : 1 - xx;
							if (fx >= 0 && fx < 1 && yy >= 0 && yy < 1) {
								const ix = Math.floor(fx * decal.w);
								const iy = Math.floor(yy * decal.h);
								const o = (iy * decal.w + ix) * 4;
								const al = decal.data[o + 3]! / 255;
								if (al > 0.02) {
									c = toward(
										c,
										[decal.data[o]!, decal.data[o + 1]!, decal.data[o + 2]!],
										al,
									);
								}
							}
						}
						set(c);
						break;
					}
					case MASK: {
						gloss = 0.7;
						set(pal.mask);
						break;
					}
					case HEADSET: {
						gloss = 0.4;
						set(HEADSET_RGB);
						break;
					}
					case CHIN:
					case TOWEL: {
						cloth = true;
						set(WHITE_RGB);
						break;
					}
					case HELD: {
						// The helmet in his hand, hung by its facemask: the
						// stripe down over its crown, the bars of the mask up
						// round his fingers, the dark of it inside.
						const h = b.held!;
						const r = helm.R * HELD_R;
						const hs = (Ps - h.s) / r;
						const hu = (Pu - h.u) / r;
						let c = pal.helmet;
						gloss = 1;
						if (hu > 0.4) {
							const bar =
								Math.abs(hu - 0.55) < 0.08 || Math.abs(hu - 0.82) < 0.08;
							c = bar ? pal.mask : scaled(pal.helmet, 0.3);
							gloss = bar ? 0.6 : 0;
						} else if (pal.stripe && Math.abs(hs) < 0.13) {
							c = pal.stripe;
						}
						set(c);
						break;
					}
					case BALL: {
						const ball = b.ball!;
						const of = Pf - ball.at.f;
						const os = Ps - ball.at.s;
						const ou = Pu - ball.at.u;
						const al = of * ball.axis.f + os * ball.axis.s + ou * ball.axis.u;
						const rf2 = of - ball.axis.f * al;
						const rs2 = os - ball.axis.s * al;
						const ru2 = ou - ball.axis.u * al;
						const rl = len3(rf2, rs2, ru2) || 1;
						const facing =
							(rf2 * ball.laces.f + rs2 * ball.laces.s + ru2 * ball.laces.u) /
							rl;
						const along = al / BALL_LEN + 0.5;
						let c = BALL_RGB;
						if (along > 0.3 && along < 0.7) {
							if (facing > 0.985) {
								c = LACE_RGB;
							} else if (facing > 0.93 && (along * 22) % 1 < 0.5) {
								c = LACE_RGB;
							}
						}
						gloss = 0.35;
						set(c);
						break;
					}
					case HAND: {
						// The back of his glove, its palm a shade of its own.
						const p = b.pn[side]!;
						const nbf = hx[f]! * rt.f + hy[f]! * dn.f + hn[f]! * tw3.f;
						const nbs = hx[f]! * rt.s + hy[f]! * dn.s + hn[f]! * tw3.s;
						const nbu = hx[f]! * rt.u + hy[f]! * dn.u + hn[f]! * tw3.u;
						const inPalm = nbf * p.f + nbs * p.s + nbu * p.u;
						const c = pal.hands[side]!;
						const e = inPalm > 0.15 ? Math.min(1, (inPalm - 0.15) / 0.25) : 0;
						const pc = pal.palms[side]!;
						cr = c[0] + (pc[0] - c[0]) * e;
						cg = c[1] + (pc[1] - c[1]) * e;
						cb = c[2] + (pc[2] - c[2]) * e;
						cloth = c !== pal.skin;
						break;
					}
					default: {
						set(pal.skin);
					}
				}
				// Cloth hangs smoother over him than skin does.
				if (cloth) {
					sz += 0.35;
					const mm = len3(sx, sy, sz);
					sx /= mm;
					sy /= mm;
					sz /= mm;
				}
				const lam = sx * LIGHT[0] + sy * LIGHT[1] + sz * LIGHT[2];
				const e = Math.min(1, Math.max(0, (lam - 0.34) / 0.16));
				const lit = e * e * (3 - 2 * e);
				const dark = 1 - 0.22 * shadow;
				let r: number;
				let g: number;
				let bb: number;
				if (cloth || gloss > 0) {
					const k = (0.79 + 0.21 * lit) * dark;
					r = cr * k;
					g = cg * k;
					bb = cb * k;
				} else {
					const k = dark;
					r = cr * (0.84 + 0.16 * lit) * k;
					g = cg * (0.74 + 0.26 * lit) * k;
					bb = cb * (0.72 + 0.28 * lit) * k;
				}
				if (gloss > 0 && lam > 0.86) {
					// The shine on a helmet: a hard white glint where it
					// faces the lights.
					const s = Math.min(1, (lam - 0.86) / 0.06) * 0.55 * gloss;
					r += (255 - r) * s;
					g += (255 - g) * s;
					bb += (255 - bb) * s;
				}
				if ((behind || fc.kind === HAND || fc.kind === MASK) && S > -lineW) {
					const e2 = Math.min(1, (S + lineW) / lineW);
					r += (cr * 0.38 - r) * e2;
					g += (cg * 0.38 - g) * e2;
					bb += (cb * 0.38 - bb) * e2;
				}
				const o = (y * w + x) * 4;
				if (x < bx0) {
					bx0 = x;
				}
				if (x > bx1) {
					bx1 = x;
				}
				if (y < by0) {
					by0 = y;
				}
				if (y > by1) {
					by1 = y;
				}
				d[o] = r;
				d[o + 1] = g;
				d[o + 2] = bb;
				d[o + 3] = cover >= 1 ? 255 : 255 * cover;
			}
		}
	}
	return { img, box: { x0: bx0, y0: by0, x1: bx1, y1: by1 } };
};
