import { project, type Camera } from "./camera.ts";
import {
	ART_H,
	ART_JERSEY,
	ART_SHORTS,
	ART_W,
	artAt,
	artColor,
	artWrap,
	type ArtWrap,
} from "./kitArt.ts";
import { bodyPoint, onRim, poseOf, type PlayerState } from "./evaluate.ts";
import {
	BALL_ORANGE,
	BALL_SEAM,
	CAMERA_BODY,
	CAMERA_LENS,
	type Look,
} from "./figure.ts";
import {
	gripOf,
	holdBall,
	skeleton,
	turnUpper,
	type Body,
	type Limb,
	type Pose,
	type V3,
} from "./poses.ts";

// ONE PLAYER, SCULPTED - the body of his sprite (see sprite.ts).
//
// He is built out of rounded masses laid along his skeleton: a chest and a
// back, lats, a neck, the deltoid, biceps and forearm of each arm, a palm with
// its fingers and thumb, thighs, kneecaps, calves, sneakers. Every pixel of
// his sprite finds the mass nearest the camera there, and masses that meet at
// a joint melt into one another near it - shoulder into arm, wrist into hand,
// hip into shorts - so he is one body, not parts laid on parts. The light
// falls on each pixel by the slope of the surface there: two tones, the
// cartoon way.
//
// What he wears is painted on that surface, not on whole parts. Each pixel is
// taken back to the point on his body it shows, in feet in his own frame, and
// his clothes are worked out there: the tank top's armholes and V-neck, the
// trim round them, the waistband, his number wrapped round his chest, the
// stripe down his shorts, socks to mid-calf, the soles of his shoes.

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

// ---- the masses -------------------------------------------------------------

// What a mass is, for painting it.
const TORSO = 0;
const SHORTS = 1;
const UPPER = 2;
const FORE = 3;
const HAND = 4;
const THIGH = 5;
const SHIN = 6;
const SHOE = 7;
const NECK = 8;
const BALL = 9;
const CAM_BODY = 10;
const CAM_LENS = 11;

// What melts into what: masses in one group melt together everywhere, two
// groups only where a joint joins them.
const G_TORSO = 0;
const G_NECK = 1;
const G_ARM = 2; // + side (0 his right, 1 his left)
const G_SHORTS = 4;
const G_LEG = 6;
const G_SHOE = 8;
const G_BALL = 10;
const G_CAMERA = 11;
const G_FINGER = 12; // + side * 5 + finger (4 the thumb)
const G_HAND = 22; // + side
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
	// Cut square at B, rather than rounded: a hem.
	flat: boolean;
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

// ---- his clothes, on him ----------------------------------------------------

// Where his lettering goes, up his torso (in torso lengths) and how big (in
// heights), and how wide it may be (in sheet half-widths): his number and
// the team's name on his chest, his name and a bigger number on his back.
export const LETTERING = {
	number: { u: 0.48, size: 0.12, maxW: 1.15 },
	wordmark: { u: 0.77, size: 0.036, maxW: 1.2 },
	backNumber: { u: 0.47, size: 0.14, maxW: 1.25 },
	name: { u: 0.85, size: 0.036, maxW: 1.25 },
} as const;

// Up his spine from his hips (feet): where his jersey tucks into his shorts,
// and the tops of his shoulders.
const tuckOf = (body: Body) => body.torso * 0.2 + body.H * 0.016;
const shouldersOf = (body: Body) => body.torso + body.depth * 0.4;

// How a uniform's picture wraps round his jersey and his shorts (see
// kitArt.ts) - the shorts down to their hems, a little above his knees.
export const wrapsFor = (body: Body): { jersey: ArtWrap; shorts: ArtWrap } => ({
	jersey: artWrap(
		body.shoulderW * 1.1,
		body.depth * 0.5,
		shouldersOf(body),
		tuckOf(body),
		ART_JERSEY,
	),
	shorts: artWrap(
		body.hipW + body.thighR * 1.15,
		body.depth * 0.5,
		tuckOf(body),
		-body.thigh * 0.84,
		ART_SHORTS,
	),
});

// The jersey's sheet: his number and lettering laid out flat, in feet round
// his torso - across from his right side round to his left, the chest in the
// middle and the back at the ends - for the jersey to be painted from.
type Sheet = {
	data: Uint8ClampedArray;
	w: number;
	h: number;
	// Sheet pixels to a foot; the half-width of the torso it wraps round; how
	// far up his spine its top edge is.
	ppf: number;
	aS: number;
	u0: number;
};
const sheets = new WeakMap<Look, Map<number, Sheet | null>>();

const jerseySheet = (
	look: Look,
	body: Body,
	ppf0: number,
	waist: number,
): Sheet | undefined => {
	if (!look.jerseyNumber || look.outfit || typeof document === "undefined") {
		return undefined;
	}
	// Made at about the size he is drawn, in steps.
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
	const aS = body.shoulderW * 1.1;
	const T = body.torso;
	const u0 = T + body.H * 0.08;
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
	const text = (
		str: string,
		x: number,
		u: number,
		size: number,
		edge: boolean,
		maxW: number,
		color = kit.number,
	) => {
		if (!str) {
			return;
		}
		const px = size * ppf;
		g.save();
		g.translate(x, (u0 - u) * ppf);
		g.scale(0.92, 1);
		g.font = `bold ${px}px "Arial Narrow", "Helvetica Neue", Arial, sans-serif`;
		g.textAlign = "center";
		g.textBaseline = "middle";
		g.lineJoin = "round";
		if (edge) {
			g.lineWidth = Math.max(1, px * 0.1);
			g.strokeStyle = kit.numberEdge;
			g.strokeText(str, 0, 0, maxW * ppf);
		}
		g.fillStyle = color;
		g.fillText(str, 0, 0, maxW * ppf);
		g.restore();
	};
	// The chest: the team's name over the number - unless his uniform is a
	// picture, which has its own.
	const L = LETTERING;
	text(
		look.jerseyNumber,
		w / 2,
		T * L.number.u,
		body.H * L.number.size,
		true,
		aS * L.number.maxW,
	);
	if (!look.kitArt) {
		text(
			look.wordmark.toUpperCase(),
			w / 2,
			T * L.wordmark.u,
			body.H * L.wordmark.size,
			false,
			aS * L.wordmark.maxW,
			kit.chest,
		);
	}
	// The back, across the ends of the sheet: his name over a bigger number.
	for (const x of [0, w]) {
		text(
			look.jerseyNumber,
			x,
			T * L.backNumber.u,
			body.H * L.backNumber.size,
			true,
			aS * L.backNumber.maxW,
		);
		text(
			look.lastName.toUpperCase(),
			x,
			T * L.name.u,
			body.H * L.name.size,
			false,
			aS * L.name.maxW,
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

// Where the parts of his uniform start and stop, in feet on his torso: U up
// his spine from his hips, S to his left, F forward.
type Cut = {
	waist: number;
	band: number;
	// The armholes: an ellipse round the top of each side, from the strap's
	// outer edge (strap) down to the armpit (pit), out past his side (side).
	strap: number;
	side: number;
	top: number;
	pit: number;
	// The neck: a V in front down to vAt below the top of his shoulders, a
	// shallow scoop (backAt) behind, neckIn either side of the middle.
	neckIn: number;
	vAt: number;
	backAt: number;
	trim: number;
	sockTop: number;
	sole: number;
	hipStripe: number;
	// A suit's open front, his tie down the middle of it.
	open: number;
};

// ---- what the camera sees of him ---------------------------------------------

// His frame as the camera sees it: which way on him the sprite's right, down
// and toward the camera are - and, the floor being seen from above, how much
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
	// A uniform drawn from a picture: how it wraps round his jersey and
	// shorts.
	wraps?: { jersey: ArtWrap; shorts: ArtWrap };
	// Up in front of his face: these arms (and the ball), drawn over his head.
	late: boolean[];
	headDepth: number;
	head: { x: number; y: number; r: number };
	// Which way each palm faces.
	pn: [V3, V3];
	// Undoing his upper body's turn (see turnUpper), by how far up his
	// torso: TURN_STEPS + 1 matrices about his hips, if he is turned.
	untwist?: Float64Array;
	// Sprite pixels to a foot, at his middle.
	k: number;
};

// How his hands are held.
type Grip = "open" | "relaxed" | "fist" | "point" | "ball";

// Running, his hands are loose fists; with the ball, spread on it; pointing,
// a finger out; strolling or standing about, relaxed, fingers a little
// curled. Otherwise open: ready for the ball, or for him.
const RELAXED = new Set<string>([
	"jog",
	"walk",
	"back",
	"drift",
	"talk",
	"hips",
	"crossed",
	"sit",
	"kneel",
	"hurt",
	"hurtKnee",
	"hurtAnkle",
	"hurtHead",
	"hurtHand",
	"hurtArm",
	"fall",
	"follow",
]);
const gripFor = (st: PlayerState, which: "R" | "L", holding: boolean): Grip => {
	if (
		(st.arm?.point && st.arm.hand === which && st.arm.w > 0.5) ||
		(which === "R" && (st.anim === "point" || st.anim === "stancePoint"))
	) {
		return "point";
	}
	if (holding) {
		return "ball";
	}
	if (st.anim === "run" || st.anim === "sprint" || st.anim === "flex") {
		return "fist";
	}
	return RELAXED.has(st.anim) ? "relaxed" : "open";
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

const HELD_BALL_R = 0.39;
// How far down from the top of his shoulders a quarter-zip's zip runs, as a
// share of the torso.
const T_ZIP = 0.22;
// Where his face is drawn, a little above the true middle of his head (see
// figure.ts).
const FACE_LIFT = 0.15;

const build = (
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	px: number,
	ox: number,
	oy: number,
): Built => {
	const q = poseOf(st);
	const held = st.holding ? holdBall(body, q, st.anim) : undefined;
	const sk = held ? held.sk : onRim(skeleton(body, q), st, body);
	const P = (v: V3) => {
		const p = project(cam, bodyPoint(st, v));
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
		flat = false,
	) => {
		const a = P(A);
		const b = P(B);
		const k = (a.k + b.k) / 2;
		const rad = new Float32Array(RS + 1);
		let rmax = 0;
		for (let i = 0; i <= RS; i++) {
			rad[i] = radiusAt(prof, i / RS) * k;
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
			flat,
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
	// His torso as it would stand untwisted (see turnUpper): up his spine
	// from his hips at his lean.
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

	// The torso: belly and back, the chest out in front, the lats widening a
	// little up to his armpits, across the tops of his shoulders; hips and
	// seat.
	const torso = (A: V3, B: V3, prof: Station[]) =>
		add(A, B, prof, TORSO, G_TORSO);
	torso(up(0.04, 0), up(0.9, 0, -dp * 0.04), [
		[0, dp * 0.42],
		[0.45, dp * 0.42],
		[1, dp * 0.47],
	]);
	torso(up(0.66, -sw * 0.4, dp * 0.05), up(0.66, sw * 0.4, dp * 0.05), [
		[0, dp * 0.36],
		[0.5, dp * 0.38],
		[1, dp * 0.36],
	]);
	for (const sd of [-1, 1]) {
		torso(up(0.2, sd * sw * 0.4), up(0.74, sd * sw * 0.55), [
			[0, dp * 0.34],
			[1, dp * 0.36],
		]);
	}
	torso(up(0.86, -sw * 0.6), up(0.86, sw * 0.6), [
		[0, dp * 0.38],
		[0.5, dp * 0.42],
		[1, dp * 0.38],
	]);
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
	// The neck, thick at the base, up into his head; the traps sloping up
	// either side of it from his shoulders.
	add(
		up(0.9, 0, -dp * 0.05),
		v3(sk.head.f, 0, sk.head.u - body.headR * 0.6),
		[
			[0, sw * 0.45],
			[0.5, sw * 0.39],
			[1, sw * 0.37],
		],
		NECK,
		G_NECK,
	);
	for (const sd of [-1, 1]) {
		torso(
			up(1.02, sd * sw * 0.14, -dp * 0.12),
			up(0.88, sd * sw * 0.64, -dp * 0.04),
			[
				[0, dp * 0.3],
				[1, dp * 0.4],
			],
		);
	}

	const fwd = v3(1, 0, 0);
	const upV = v3(0, 0, 1);
	const kMid = P(pel).k;
	// Which way each palm faces (his right, his left), and where its fingers
	// and thumb grow from.
	const pn: [V3, V3] = [v3(0, 1, 0), v3(0, -1, 0)];
	const knuckleAt: V3[] = [];
	const thumbAt: V3[] = [];
	const arm = (limb: Limb, side: 0 | 1) => {
		const g = G_ARM + side;
		const a = unit(subV(limb.mid, limb.root));
		const c = unit(subV(limb.end, limb.mid));
		// The inside of the elbow: the way the forearm bends from the upper
		// arm (forward, for an arm hanging straight).
		const bend = square(c, a);
		const inside = lenV(bend) > 0.08 ? unit(bend) : unit(square(fwd, a), upV);
		// The deltoid, narrowing to the elbow; the biceps in front of it, the
		// point of the elbow behind.
		add(
			limb.root,
			limb.mid,
			[
				[0, body.upperR * 1.2],
				[0.2, body.upperR * 1.1],
				[0.5, body.upperR * 0.9],
				[1, body.foreR * 0.86],
			],
			UPPER,
			g,
			side,
		);
		add(
			addV(lerpV(limb.root, limb.mid, 0.36), inside, body.upperR * 0.32),
			addV(lerpV(limb.root, limb.mid, 0.78), inside, body.upperR * 0.28),
			[
				[0, body.upperR * 0.62],
				[0.5, body.upperR * 0.74],
				[1, body.upperR * 0.56],
			],
			UPPER,
			g,
			side,
		);
		add(
			addV(limb.mid, inside, -body.foreR * 0.36),
			addV(limb.mid, inside, -body.foreR * 0.36),
			[[0, body.foreR * 0.62]],
			FORE,
			g,
			side,
		);
		// The forearm, swelling below the elbow and slimming to the wrist.
		add(
			limb.mid,
			limb.end,
			[
				[0, body.foreR * 0.9],
				[0.22, body.foreR * 1.04],
				[0.7, body.foreR * 0.76],
				[1, body.foreR * 0.54],
			],
			FORE,
			g,
			side,
		);
		hand(limb, side, c);
	};

	// The hand: a palm out of the wrist, four fingers and a thumb, held the
	// way the moment calls for - a cartoon's, a little bigger than life and
	// broader than the wrist, so it reads.
	const hand = (limb: Limb, side: 0 | 1, foreDir: V3) => {
		const which = side === 0 ? "R" : "L";
		const sgn = side === 0 ? -1 : 1;
		// (A jumper's guide hand, once it has come off the ball, is open.)
		const off = which === "L" && gripOf(st.anim) === "shot" && q.free > 0.5;
		const grip = gripFor(st, which, held !== undefined && !off);
		const wrist = limb.end;
		const tip = limb.tip ?? limb.end;
		let h = lenV(subV(tip, wrist)) > 1e-3 ? unit(subV(tip, wrist)) : foreDir;
		// Which way the palm faces: hanging, in toward his middle and a little
		// back - the back of the hand turned out to the camera, the way a
		// cartoon shows it; raised, toward what is in front of him; on the
		// ball, onto the ball.
		const inward = v3(0, -sgn, 0);
		let palmN = unit(
			square(addV(inward, fwd, -0.75 + 1.9 * Math.max(0, h.u)), h),
			inward,
		);
		if (held && grip === "ball") {
			// Palm onto the ball: a jumper's shooting hand cocked back behind
			// it, fingers up its back; any other, fingers up its side.
			const onto = unit(subV(held.ball, addV(wrist, h, H * 0.04)));
			palmN = onto;
			const shooting = which === "R" && gripOf(st.anim) === "shot";
			h = unit(square(shooting ? h : addV(h, upV, 1.4), onto), h);
		}
		// A pass let go: palms out, thumbs down.
		if (
			!held &&
			(st.anim === "pass" || st.anim === "passBounce") &&
			st.phase > 0.38
		) {
			palmN = unit(square(v3(0.2, sgn, -0.45), h), palmN);
		}
		const thumbSide = unit(cross(palmN, h), fwd);
		const ts = v3(thumbSide.f * sgn, thumbSide.s * sgn, thumbSide.u * sgn);
		const g = G_HAND + side;
		pn[side] = palmN;
		const L = H * 0.124;
		const halfW = H * 0.016;
		const palmR = H * 0.018;
		// The palm: broad across, thin through - two masses side by side.
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
		// Fingers, side by side: straight out (open), curled a little
		// (relaxed), round the ball, or folded into the palm (a fist).
		const curl =
			grip === "fist" || grip === "point"
				? 1.95
				: grip === "relaxed"
					? 0.6
					: grip === "ball"
						? 0.55
						: 0.15;
		const fingerR = H * 0.0088;
		const lens = [0.4, 0.44, 0.41, 0.33];
		const bend = (d: V3, c: number) =>
			unit(
				addV(
					v3(d.f * Math.cos(c), d.s * Math.cos(c), d.u * Math.cos(c)),
					palmN,
					Math.sin(c),
				),
			);
		if (fingerR * kMid < 1.4 && grip !== "point") {
			// Too small to tell apart: the four as one, flat like the palm.
			const dir = bend(h, curl);
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
				const across = (1.5 - i) / 1.5;
				const base = addV(knuckles, ts, across * halfW * 1.25);
				const c = i === 0 && grip === "point" ? 0.04 : curl;
				const spread = grip === "open" ? 0.08 : grip === "ball" ? 0.12 : 0.02;
				const dir = bend(unit(addV(h, ts, across * spread)), c);
				const len = L * lens[i]! * (c > 1.2 ? 0.6 : 1);
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
		// The thumb: out from the heel of the palm on its side - across the
		// fingers in a fist.
		const tBase = addV(addV(wrist, h, L * 0.16), ts, halfW * 1.05);
		knuckleAt[side] = knuckles;
		thumbAt[side] = tBase;
		const tTip =
			grip === "fist" || grip === "point"
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
				[0, H * 0.0105],
				[1, H * 0.0085],
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
		// The thigh, from inside his shorts down to the knee; the kneecap in
		// front of the knee; the shin, with the calf swelling behind it.
		add(
			lerpV(limb.root, limb.mid, 0.35),
			limb.mid,
			[
				[0, body.thighR * 0.96],
				[0.4, body.thighR * 0.9],
				[1, body.kneeR * 0.96],
			],
			THIGH,
			gl,
			side,
		);
		const shinDir = unit(subV(limb.end, limb.mid));
		const front = unit(square(fwd, shinDir), v3(1, 0, 0));
		add(
			addV(limb.mid, front, body.kneeR * 0.42),
			addV(limb.mid, front, body.kneeR * 0.42),
			[[0, body.kneeR * 0.6]],
			SHIN,
			gl,
			side,
		);
		add(
			limb.mid,
			limb.end,
			[
				[0, body.kneeR * 0.94],
				[0.3, body.calfR * 0.92],
				[0.8, body.ankleR * 1.08],
				[1, body.ankleR * 1.02],
			],
			SHIN,
			gl,
			side,
		);
		add(
			addV(lerpV(limb.mid, limb.end, 0.12), front, -body.calfR * 0.3),
			addV(lerpV(limb.mid, limb.end, 0.56), front, -body.calfR * 0.16),
			[
				[0, body.calfR * 0.8],
				[0.4, body.calfR * 0.88],
				[1, body.calfR * 0.62],
			],
			SHIN,
			gl,
			side,
		);
		// His shorts' leg: loose, flaring a little, cut square above the knee.
		add(
			lerpV(limb.root, limb.mid, 0.16),
			lerpV(limb.root, limb.mid, 0.84),
			[
				[0, body.thighR * 1.2],
				[1, body.thighR * 1.28],
			],
			SHORTS,
			G_SHORTS + side,
			side,
			true,
		);
		// The sneaker: heel to toe along the floor, a collar round the ankle.
		const tip = limb.tip ?? limb.end;
		const heel = lerpV(limb.end, tip, -0.32);
		const toe = lerpV(limb.end, tip, 1.04);
		add(
			v3(
				heel.f,
				heel.s,
				Math.max(body.ankleR * 1.05, heel.u - body.ankleR * 0.15),
			),
			v3(toe.f, toe.s, Math.max(body.ankleR * 0.9, toe.u - body.ankleR * 0.25)),
			[
				[0, body.ankleR * 1.3],
				[0.5, body.ankleR * 1.38],
				[1, body.ankleR * 1.12],
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
				[0, body.ankleR * 1.12],
				[1, body.ankleR * 1.2],
			],
			SHOE,
			G_SHOE + side,
			side,
		);
	};
	leg(sk.legR, 0);
	leg(sk.legL, 1);

	if (held) {
		add(held.ball, held.ball, [[0, HELD_BALL_R]], BALL, G_BALL);
	}
	if (look.outfit?.camera) {
		// His camera, in both hands: the body between them, the long lens out
		// the way he faces.
		const mid = v3(
			(sk.armR.end.f + sk.armL.end.f) / 2 + 0.14,
			(sk.armR.end.s + sk.armL.end.s) / 2,
			(sk.armR.end.u + sk.armL.end.u) / 2 + 0.06,
		);
		add(
			addV(mid, v3(0, 1, 0), -0.18),
			addV(mid, v3(0, 1, 0), 0.18),
			[[0, 0.21]],
			CAM_BODY,
			G_CAMERA,
		);
		add(
			mid,
			addV(mid, fwd, 0.8),
			[
				[0, 0.17],
				[1, 0.15],
			],
			CAM_LENS,
			G_CAMERA,
		);
	}

	const joins: Built["joins"] = [];
	const join = (a: number, b: number, at: V3, reachFt: number) => {
		const p = P(at);
		joins.push({ a, b, x: p.x, y: p.y, reach: reachFt * p.k });
	};
	join(G_TORSO, G_ARM, sk.armR.root, body.upperR * 3);
	join(G_TORSO, G_ARM + 1, sk.armL.root, body.upperR * 3);
	join(G_TORSO, G_SHORTS, sk.legR.root, body.thighR * 2.6);
	join(G_TORSO, G_SHORTS + 1, sk.legL.root, body.thighR * 2.6);
	join(G_SHORTS, G_SHORTS + 1, up(-0.1, 0), body.thighR * 2.2);
	join(G_TORSO, G_NECK, up(0.95, 0), sw * 0.9);
	for (const side of [0, 1]) {
		for (let i = 0; i < 4; i++) {
			join(G_HAND + side, G_FINGER + side * 5 + i, knuckleAt[side]!, H * 0.04);
		}
		join(G_HAND + side, G_FINGER + side * 5 + 4, thumbAt[side]!, H * 0.035);
	}

	// The camera's frame, in his.
	const cy = Math.cos(st.yaw);
	const sy = Math.sin(st.yaw);
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
		waist: T * 0.2,
		band: H * 0.016,
		strap: sw * 0.62,
		side: sw * 1.32,
		top: shouldersOf(body),
		pit: T - H * 0.075,
		neckIn: sw * 0.42,
		vAt: H * 0.068,
		backAt: H * 0.03,
		trim: Math.max(H * 0.0085, 1.2 / kMid),
		sockTop: H * 0.115,
		sole: body.ankleR * 0.6,
		hipStripe: body.hipW + body.thighR * 0.8,
		open: T * 0.46,
	};

	const headC = P(sk.head);
	// Up in front of his face: an arm raised past his shoulder and nearer the
	// camera than his head, and the ball held up there.
	const late = [sk.armR, sk.armL].map((l) => {
		const mid = P(lerpV(l.mid, l.end, 0.5));
		return l.end.u > l.root.u + H * 0.08 && mid.depth < headC.depth;
	});
	if (held) {
		const c = P(held.ball);
		late.push(
			held.ball.u > sk.armR.root.u &&
				(c.depth < headC.depth || held.ball.u > sk.head.u),
		);
	} else {
		late.push(false);
	}
	return {
		masses,
		joins,
		frame,
		cut,
		pel,
		spine,
		fwd: fwdT,
		sheet: jerseySheet(look, body, kMid * 1.2, cut.waist),
		...(look.kitArt && !look.outfit ? { wraps: wrapsFor(body) } : {}),
		late,
		headDepth: headC.depth - body.headR * 0.4,
		pn,
		...(turned ? { untwist: untwistTable(q, pel) } : {}),
		head: {
			x: headC.x,
			y: headC.y - body.headR * headC.k * FACE_LIFT,
			r: body.headR * headC.k,
		},
		k: kMid,
	};
};

// ---- painting him -------------------------------------------------------------

// What he is made of, by part, in this light.
type Palette = {
	jersey: RGB;
	trim: RGB;
	shorts: RGB;
	band: RGB;
	stripe: RGB;
	skin: RGB;
	sock: RGB;
	shoe: RGB;
	sole: RGB;
	legs: [RGB, RGB];
	upper: [RGB, RGB];
	fore: [RGB, RGB];
	wrist: [RGB | undefined, RGB | undefined];
	knee: [RGB | undefined, RGB | undefined];
	shirt?: RGB;
	tie?: RGB;
	stripes?: RGB;
	lapel?: RGB;
};

const palettes = new WeakMap<Look, Palette>();
const paletteOf = (look: Look): Palette => {
	let p = palettes.get(look);
	if (p) {
		return p;
	}
	const kit = look.kit;
	const gear = look.gear;
	const outfit = look.outfit;
	const skin = rgb(look.skin);
	const jersey = rgb(kit.jersey);
	const both = <T>(f: (w: "R" | "L") => T): [T, T] => [f("R"), f("L")];
	const sleeve = (w: "R" | "L") =>
		gear?.sleeve?.arms.includes(w) ? rgb(gear.sleeve.color) : undefined;
	p = {
		jersey,
		trim: rgb(kit.trim),
		shorts: rgb(kit.shorts),
		band: outfit
			? scaled(rgb(kit.shorts), 0.45)
			: scaled(rgb(kit.shorts), 0.84),
		stripe: rgb(kit.stripe),
		skin,
		sock: rgb(gear?.sock ?? kit.sock),
		shoe: rgb(gear?.shoe ?? kit.shoe),
		sole: rgb(gear?.sole ?? kit.sole),
		legs: both((w) =>
			gear?.tights?.legs.includes(w) ? rgb(gear.tights.color) : skin,
		),
		upper: both((w) =>
			outfit?.sleeves
				? jersey
				: gear?.sleeve?.arms.includes(w) && !gear.sleeve.elbow
					? sleeve(w)!
					: skin,
		),
		fore: both((w) =>
			outfit?.sleeves === "long" ? jersey : (sleeve(w) ?? skin),
		),
		wrist: both((w) =>
			gear?.wrist?.arms.includes(w) ? rgb(gear.wrist.color) : undefined,
		),
		knee: both((w) =>
			gear?.knee?.legs === w ? rgb(gear.knee.color) : undefined,
		),
		...(outfit?.shirt
			? { shirt: rgb(outfit.shirt), lapel: scaled(jersey, 0.7) }
			: {}),
		...(outfit?.tie ? { tie: rgb(outfit.tie) } : {}),
		...(outfit?.stripes ? { stripes: rgb(outfit.stripes) } : {}),
	};
	palettes.set(look, p);
	return p;
};

// The palms of his hands, whatever his skin.
const PALM: RGB = [232, 186, 166];
const BALL_RGB = rgb(BALL_ORANGE);
const SEAM_RGB = rgb(BALL_SEAM);
const CAM_BODY_RGB = rgb(CAMERA_BODY);
const CAM_LENS_RGB = rgb(CAMERA_LENS);

// The light, from up and to the left and toward the camera (on the sprite:
// right, down, out of the picture).
const LIGHT = (() => {
	const l = [-0.45, -0.7, 0.55];
	const n = Math.hypot(l[0]!, l[1]!, l[2]!);
	return [l[0]! / n, l[1]! / n, l[2]! / n] as const;
})();

const smin = (a: number, b: number, k: number) => {
	const h = Math.max(k - Math.abs(a - b), 0) / k;
	return Math.min(a, b) - (h * h * k) / 4;
};

export type Sculpted = {
	img: ImageData;
	// What goes over his head once it is drawn: an arm raised in front of
	// his face, the ball held up there.
	over?: ImageData;
	head: { x: number; y: number; r: number };
};

const TILE = 8;

// His sprite, w x h of its pixels, each px screen pixels, its top left at
// (ox, oy) on the screen. Given `seen` (ART_W x ART_H), a uniform drawn
// from a picture also marks which of the picture's pixels show on him: 1
// where his jersey or shorts are, 2 where the trim goes over it.
export const sculpt = (
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	px: number,
	ox: number,
	oy: number,
	w: number,
	h: number,
	seen?: Uint8Array,
): Sculpted => {
	const b = build(cam, st, body, look, px, ox, oy);
	const pal = paletteOf(look);
	const masses = b.masses;
	const n = masses.length;
	const img = new ImageData(w, h);
	const d = img.data;
	const lineW = Math.max(0.7, 0.028 * b.k);
	const margin = lineW * 3.5 + 1;
	// Which masses each tile of the sprite need ask about.
	const tw = Math.ceil(w / TILE);
	const th = Math.ceil(h / TILE);
	const lists: number[][] = Array.from({ length: tw * th }, () => []);
	for (let i = 0; i < n; i++) {
		const m = masses[i]!;
		const reach = m.rmax + margin + TILE * 0.71;
		const tx0 = Math.max(
			0,
			Math.floor((Math.min(m.ax, m.ax + m.dx) - reach) / TILE),
		);
		const tx1 = Math.min(
			tw - 1,
			Math.floor((Math.max(m.ax, m.ax + m.dx) + reach) / TILE),
		);
		const ty0 = Math.max(
			0,
			Math.floor((Math.min(m.ay, m.ay + m.dy) - reach) / TILE),
		);
		const ty1 = Math.min(
			th - 1,
			Math.floor((Math.max(m.ay, m.ay + m.dy) + reach) / TILE),
		);
		for (let ty = ty0; ty <= ty1; ty++) {
			for (let tx = tx0; tx <= tx1; tx++) {
				const cx = (tx + 0.5) * TILE;
				const cy = (ty + 0.5) * TILE;
				const raw =
					m.l2 > 1e-6 ? ((cx - m.ax) * m.dx + (cy - m.ay) * m.dy) / m.l2 : 0;
				const t = raw < 0 ? 0 : raw > 1 ? 1 : raw;
				if (Math.hypot(cx - m.ax - m.dx * t, cy - m.ay - m.dy * t) < reach) {
					lists[ty * tw + tx]!.push(i);
				}
			}
		}
	}
	// Which joint, if any, melts two groups together.
	const joinOf = new Int16Array(GROUPS * GROUPS).fill(-1);
	b.joins.forEach((j, i) => {
		joinOf[j.a * GROUPS + j.b] = i;
		joinOf[j.b * GROUPS + j.a] = i;
	});
	const lateAny = b.late.some(Boolean);
	const owner = lateAny ? new Int8Array(w * h).fill(-1) : undefined;

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
	const wraps = b.wraps;
	const art = look.kitArt;
	const artXY = new Float64Array(2);
	const artPx = new Float64Array(3);
	const artRGB: RGB = [0, 0, 0];
	const mark = (v: number) => {
		const i = Math.round(artXY[1]!) * ART_W + Math.round(artXY[0]!);
		if (seen && i >= 0 && i < ART_W * ART_H && seen[i]! < v) {
			seen[i] = v;
		}
	};
	// The team's picture at a point on his jersey or shorts, over the plain
	// color c where the picture is clear.
	const artOver = (
		wrap: ArtWrap,
		U: number,
		S: number,
		F: number,
		notSide: boolean,
		c: RGB,
	): RGB => {
		artAt(wrap, U, S, F, notSide, artXY);
		mark(1);
		const a = artColor(art!, artXY[0]!, artXY[1]!, artPx);
		if (a <= 0) {
			return c;
		}
		artRGB[0] = c[0] + (artPx[0]! - c[0]) * a;
		artRGB[1] = c[1] + (artPx[1]! - c[1]) * a;
		artRGB[2] = c[2] + (artPx[2]! - c[2]) * a;
		return artRGB;
	};
	let cr = 0;
	let cg = 0;
	let cb = 0;
	let cloth = false;

	// The jersey, shirt or jacket at a point on his torso (U, S, F): sets
	// cr, cg, cb.
	const torsoAt = (U: number, S: number, F: number) => {
		cloth = true;
		const outfit = look.outfit;
		let c: RGB;
		// A suit's jacket hangs on over his hips; anything else is tucked in.
		const hem = pal.shirt ? -cut.waist : cut.waist;
		if (U < hem) {
			c = pal.shorts;
			if (wraps) {
				c = artOver(wraps.shorts, U, S, F, false, c);
			} else if (!outfit && Math.abs(F) < 0.08 && Math.abs(S) > cut.hipStripe) {
				c = pal.stripe;
			}
		} else if (U < cut.waist + cut.band && !pal.shirt) {
			c = wraps ? artOver(wraps.shorts, U, S, F, false, pal.band) : pal.band;
		} else {
			const aS = Math.abs(S);
			// The neck: a V in front, a scoop behind.
			const wN = Math.min(1, aS / cut.neckIn);
			const front = F >= 0;
			const neckDrop = outfit
				? (front ? cut.backAt * 1.2 : cut.backAt * 0.6) * (1 - wN * wN)
				: front
					? cut.vAt * (1 - wN)
					: cut.backAt * (1 - wN * wN);
			const slope =
				front && !outfit
					? cut.vAt / cut.neckIn
					: (2 * cut.backAt * wN) / cut.neckIn;
			const below = (cut.top - neckDrop - U) / Math.hypot(1, slope);
			const dNeck =
				aS >= cut.neckIn
					? Math.max(aS - cut.neckIn, below)
					: below > 0
						? below
						: Math.max(below, aS - cut.neckIn);
			let dArm = Infinity;
			if (!outfit) {
				// The armholes.
				const sx = Math.min(aS, cut.side);
				const uy = Math.min(U, cut.top);
				const ea = cut.side - cut.strap;
				const eb = cut.top - cut.pit;
				const gx = (sx - cut.side) / ea;
				const gy = (uy - cut.top) / eb;
				const g = gx * gx + gy * gy - 1;
				dArm = g / (2 * Math.hypot(gx / ea, gy / eb) || 1);
			}
			const edge = Math.min(dArm, dNeck);
			if (edge < 0) {
				c = pal.skin;
				cloth = false;
			} else if (edge < cut.trim && !outfit) {
				c = pal.trim;
				if (seen && wraps) {
					artAt(wraps.jersey, U, S, F, false, artXY);
					mark(2);
				}
			} else {
				c = pal.jersey;
				if (outfit?.stripes && pal.stripes) {
					const th = Math.atan2(S / (cut.side * 0.8), F / (cut.side * 0.4));
					if (Math.floor((th * cut.side * 0.8) / 0.17 + 100) % 2 === 0) {
						c = pal.stripes;
					}
				}
				if (pal.shirt && front) {
					// A suit: his shirt in the open front, his tie down it.
					const depth = cut.top - U;
					const halfOpen = cut.neckIn * Math.max(0, 1 - depth / cut.open);
					if (aS < halfOpen) {
						c =
							aS < cut.neckIn * 0.16 + depth * 0.06 && pal.tie
								? pal.tie
								: pal.shirt;
					} else if (aS < halfOpen + cut.trim * 1.6) {
						c = pal.lapel!;
					}
				} else if (
					outfit &&
					!outfit.shirt &&
					outfit.sleeves === "long" &&
					front &&
					aS < cut.trim * 0.6 &&
					U > cut.top - T_ZIP * cut.top
				) {
					// A quarter-zip's zip.
					c = pal.trim;
				}
				if (wraps && c === pal.jersey) {
					c = artOver(wraps.jersey, U, S, F, false, c);
				}
				if (sheet && (c === pal.jersey || c === artRGB)) {
					// His number and lettering.
					// Round his torso by how far across it the point is, not by how
					// far forward his chest stands there - which would bend the
					// uprights of his lettering over the curve of his chest.
					const across = Math.max(-1, Math.min(1, S / sheet.aS));
					const th =
						F >= 0
							? Math.asin(across)
							: (across < 0 ? -Math.PI : Math.PI) - Math.asin(across);
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
								sd[o00]! * w00 +
								sd[o10]! * w10 +
								sd[o01]! * w01 +
								sd[o11]! * w11;
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
			}
		}
		cr = c[0];
		cg = c[1];
		cb = c[2];
	};

	for (let ty = 0; ty < th; ty++) {
		for (let tx = 0; tx < tw; tx++) {
			const list = lists[ty * tw + tx]!;
			if (list.length === 0) {
				continue;
			}
			const yEnd = Math.min(h, (ty + 1) * TILE);
			const xEnd = Math.min(w, (tx + 1) * TILE);
			for (let y = ty * TILE; y < yEnd; y++) {
				const py = y + 0.5;
				for (let x = tx * TILE; x < xEnd; x++) {
					const pxx = x + 0.5;
					let m = 0;
					let best = -1;
					let bestZ = Infinity;
					let least = -1;
					let leastS = Infinity;
					for (let li = 0; li < list.length; li++) {
						const i = list[li]!;
						const c = masses[i]!;
						if (
							pxx < c.x0 - margin ||
							pxx > c.x1 + margin ||
							py < c.y0 - margin ||
							py > c.y1 + margin
						) {
							continue;
						}
						const raw =
							c.l2 > 1e-6
								? ((pxx - c.ax) * c.dx + (py - c.ay) * c.dy) / c.l2
								: 0;
						const t = raw < 0 ? 0 : raw > 1 ? 1 : raw;
						const ex = pxx - (c.ax + c.dx * t);
						const ey = py - (c.ay + c.dy * t);
						let dist = Math.sqrt(ex * ex + ey * ey);
						const fi = t * RS;
						const i0 = fi | 0;
						const r =
							i0 >= RS
								? c.rad[RS]!
								: c.rad[i0]! + (c.rad[i0 + 1]! - c.rad[i0]!) * (fi - i0);
						let s = dist - r;
						if (c.flat && raw > 1) {
							s = Math.max(s, (raw - 1) * c.len);
							dist = Math.min(dist, r);
						}
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
								const dj = Math.hypot(pxx - jn.x, py - jn.y);
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
							// Something just in front, beside: a little of its
							// shadow.
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
					const nl = Math.hypot(nx, ny, nzs) || 1;
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
					const side = fc.side;
					switch (fc.kind) {
						case TORSO: {
							let rf0 = Pf - pel.f;
							let rs0 = Ps - pel.s;
							let ru0 = Pu - pel.u;
							const ut = b.untwist;
							if (ut) {
								// Where it sat before he turned.
								const up0 = (rf0 * spine.f + ru0 * spine.u) / body.torso;
								const k =
									Math.min(
										TURN_STEPS,
										Math.max(0, Math.round(up0 * TURN_STEPS)),
									) * 9;
								const nf = ut[k]! * rf0 + ut[k + 1]! * rs0 + ut[k + 2]! * ru0;
								const ns =
									ut[k + 3]! * rf0 + ut[k + 4]! * rs0 + ut[k + 5]! * ru0;
								const nu =
									ut[k + 6]! * rf0 + ut[k + 7]! * rs0 + ut[k + 8]! * ru0;
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
						case SHORTS: {
							cloth = true;
							// The stripe down the outside of the leg: where the
							// surface faces out from his side.
							const lf = fc.B.f - fc.A.f;
							const ls = fc.B.s - fc.A.s;
							const lu = fc.B.u - fc.A.u;
							const ll = Math.hypot(lf, ls, lu) || 1;
							let of = Pf - fc.A.f - lf * t;
							let os = Ps - fc.A.s - ls * t;
							let ou = Pu - fc.A.u - lu * t;
							const along = (of * lf + os * ls + ou * lu) / (ll * ll);
							of -= lf * along;
							os -= ls * along;
							ou -= lu * along;
							const ol = Math.hypot(of, os, ou) || 1;
							let c =
								!look.outfit && (os * (side === 0 ? -1 : 1)) / ol > 0.95
									? pal.stripe
									: pal.shorts;
							if (wraps) {
								// Where this would be on him standing straight: how
								// far across him (square to his thigh) and ahead.
								const ax = lf / ll;
								const ay = ls / ll;
								const az = lu / ll;
								let xf = -ax * ay;
								let xs = 1 - ay * ay;
								let xu = -az * ay;
								const xl = Math.hypot(xf, xs, xu) || 1;
								xf /= xl;
								xs /= xl;
								xu /= xl;
								const across = of * xf + os * xs + ou * xu;
								const ahead =
									of * (ay * xu - az * xs) +
									os * (az * xf - ax * xu) +
									ou * (ax * xs - ay * xf);
								const out = side === 0 ? -1 : 1;
								c = artOver(
									wraps.shorts,
									-(0.16 + 0.68 * (t + along)) * body.thigh,
									out * body.hipW + across,
									ahead,
									// The inside of his leg faces the other one, not
									// out to his side.
									across * out < 0,
									pal.shorts,
								);
							}
							cr = c[0];
							cg = c[1];
							cb = c[2];
							break;
						}
						case UPPER:
						case FORE: {
							let c = fc.kind === UPPER ? pal.upper[side]! : pal.fore[side]!;
							const band = pal.wrist[side];
							if (
								fc.kind === FORE &&
								band &&
								t > 0.76 &&
								t < 0.97 &&
								fc.len > 0
							) {
								c = band;
							}
							if (
								look.outfit?.sleeves === "short" &&
								fc.kind === UPPER &&
								t > 0.55
							) {
								c = pal.skin;
							}
							cloth = c !== pal.skin;
							cr = c[0];
							cg = c[1];
							cb = c[2];
							break;
						}
						case THIGH:
						case SHIN: {
							let c = pal.legs[side]!;
							if (fc.kind === SHIN && Pu < cut.sockTop) {
								c = pal.sock;
							}
							// A pad round the knee.
							const pad = pal.knee[side];
							if (
								pad &&
								((fc.kind === THIGH && t > 0.82) ||
									(fc.kind === SHIN && t < 0.2))
							) {
								c = pad;
							}
							cloth = c !== pal.skin;
							cr = c[0];
							cg = c[1];
							cb = c[2];
							break;
						}
						case SHOE: {
							const c = Pu < cut.sole ? pal.sole : pal.shoe;
							cloth = true;
							cr = c[0];
							cg = c[1];
							cb = c[2];
							break;
						}
						case BALL: {
							// Its seams: round its middle, and a great circle
							// across.
							const nfx = hx[f]!;
							const nfy = hy[f]!;
							const nfz = hn[f]!;
							const seam = Math.min(
								Math.abs(nfy * 0.98 + nfx * 0.17),
								Math.abs(nfx * 0.95 - nfz * 0.3),
							);
							const c = seam < 0.055 ? SEAM_RGB : BALL_RGB;
							cr = c[0];
							cg = c[1];
							cb = c[2];
							break;
						}
						case CAM_BODY:
						case CAM_LENS: {
							const c = fc.kind === CAM_BODY ? CAM_BODY_RGB : CAM_LENS_RGB;
							cr = c[0];
							cg = c[1];
							cb = c[2];
							break;
						}
						case HAND: {
							// His palm, and the inside of his fingers, paler
							// than the back of his hand.
							const p = b.pn[side]!;
							const nbf = hx[f]! * rt.f + hy[f]! * dn.f + hn[f]! * tw3.f;
							const nbs = hx[f]! * rt.s + hy[f]! * dn.s + hn[f]! * tw3.s;
							const nbu = hx[f]! * rt.u + hy[f]! * dn.u + hn[f]! * tw3.u;
							const inPalm = nbf * p.f + nbs * p.s + nbu * p.u;
							const c = pal.skin;
							const e =
								inPalm > 0.15 ? Math.min(1, (inPalm - 0.15) / 0.25) * 0.42 : 0;
							cr = c[0] + (PALM[0] - c[0]) * e;
							cg = c[1] + (PALM[1] - c[1]) * e;
							cb = c[2] + (PALM[2] - c[2]) * e;
							break;
						}
						default: {
							cr = pal.skin[0];
							cg = pal.skin[1];
							cb = pal.skin[2];
						}
					}
					// Cloth hangs smoother over him than skin does.
					if (cloth) {
						sz += 0.35;
						const mm = Math.hypot(sx, sy, sz);
						sx /= mm;
						sy /= mm;
						sz /= mm;
					}
					const lam = sx * LIGHT[0] + sy * LIGHT[1] + sz * LIGHT[2];
					// Two tones, the step between them soft.
					const e = Math.min(1, Math.max(0, (lam - 0.34) / 0.16));
					const lit = e * e * (3 - 2 * e);
					const dark = 1 - 0.22 * shadow;
					let r: number;
					let g: number;
					let bb: number;
					if (cloth) {
						const k = (0.79 + 0.21 * lit) * dark;
						r = cr * k;
						g = cg * k;
						bb = cb * k;
					} else {
						// Skin in shadow warms.
						const k = dark;
						r = cr * (0.84 + 0.16 * lit) * k;
						g = cg * (0.74 + 0.26 * lit) * k;
						bb = cb * (0.72 + 0.28 * lit) * k;
					}
					// A fine line where he passes in front of himself - and
					// all round his hands, so they read.
					if ((behind || fc.kind === HAND) && S > -lineW) {
						const e = Math.min(1, (S + lineW) / lineW);
						r += (cr * 0.38 - r) * e;
						g += (cg * 0.38 - g) * e;
						bb += (cb * 0.38 - bb) * e;
					}
					const o = (y * w + x) * 4;
					d[o] = r;
					d[o + 1] = g;
					d[o + 2] = bb;
					d[o + 3] = cover >= 1 ? 255 : 255 * cover;
					if (owner) {
						owner[y * w + x] = fc.group;
					}
				}
			}
		}
	}

	let over: ImageData | undefined;
	if (owner) {
		over = new ImageData(w, h);
		const od = over.data;
		let any = false;
		for (let i = 0; i < w * h; i++) {
			const g = owner[i]!;
			if (g < 0) {
				continue;
			}
			const side =
				g === G_ARM || g === G_HAND || (g >= G_FINGER && g < G_FINGER + 5)
					? 0
					: g === G_ARM + 1 ||
						  g === G_HAND + 1 ||
						  (g >= G_FINGER + 5 && g < G_FINGER + 10)
						? 1
						: -1;
			if ((side >= 0 && b.late[side]) || (g === G_BALL && b.late[2])) {
				od[i * 4] = d[i * 4]!;
				od[i * 4 + 1] = d[i * 4 + 1]!;
				od[i * 4 + 2] = d[i * 4 + 2]!;
				od[i * 4 + 3] = d[i * 4 + 3]!;
				any = true;
			}
		}
		if (!any) {
			over = undefined;
		}
	}
	return { img, ...(over ? { over } : {}), head: b.head };
};
