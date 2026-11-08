import type { Camera } from "../court3d/camera.ts";
import { bodyPoint, type Placed } from "../court3d/evaluate.ts";
import {
	addV,
	armMasses,
	BEND_HI,
	BEND_LO,
	cross,
	dotV,
	frameOf,
	handMasses,
	massesFor,
	seerOf,
	torsoOf,
	len2,
	len3,
	lenV,
	lerpV,
	PRESS_MOST,
	rgb,
	scaled,
	shade,
	square,
	subV,
	TURN_STEPS,
	unit,
	untwistTable,
	v3,
	type ArmBuild,
	type Frame,
	type HandBuild,
	type Ink,
	type Mass,
	type RGB,
	type Station,
} from "../court3d/sculpt.ts";
import {
	skeleton,
	turnUpper,
	type Body,
	type Limb,
	type Pose,
	type V3,
} from "../court3d/poses.ts";
import {
	ART_H,
	ART_JERSEY,
	ART_SHORTS,
	ART_W,
	artAt,
	artColor,
	artWrap,
	DEFAULT_FIT,
	type ArtWrap,
	type KitArt,
} from "../court3d/kitArt.ts";
import type { BareHead, FieldLook } from "./kit.ts";

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

const clamp = (v: number, lo: number, hi: number) =>
	Math.min(hi, Math.max(lo, v));
const toward = (c: RGB, to: RGB, f: number): RGB => [
	c[0] + (to[0] - c[0]) * f,
	c[1] + (to[1] - c[1]) * f,
	c[2] + (to[2] - c[2]) * f,
];

export const BALL_BROWN = "#7a3d1e";
const BALL_RGB = rgb(BALL_BROWN);
const LACE_RGB: RGB = [246, 243, 236];
const EYE_BLACK_RGB: RGB = [22, 20, 20];
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
const POM = 15;
const HAIR = 16;

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

// A point on him (his frame, feet) on the field: the court's (see
// court3d's bodyPoint) - the field's y runs toward the camera, as the
// court's does.
export { bodyPoint, type Placed };

// ---- what he is doing, for sculpting ----------------------------------------

// How a hand is held: open (ready, or flat on the turf), relaxed, a fist
// (running), on the ball.
export type Grip = "open" | "relaxed" | "fist" | "ball";

// The ball with him: its middle, the way its points run, and which way its
// laces face - in his frame (feet). (Unseen: on its way from his hands to
// another man's, drawn on its own - his hands on it all the same.)
export type HeldBall = { at: V3; axis: V3; laces: V3; unseen?: true };

export type Figure = Placed & {
	pose: Pose;
	// His right hand, his left.
	grips: [Grip, Grip];
	ball?: HeldBall;
	// An official's whistle in his mouth, blowing it.
	whistle?: true;
	// His mouth open (0 to 1), talking.
	mouth?: number;
};

// The football: eleven inches point to point, a little bigger than life so
// it reads, and how fat round its middle.
export const BALL_LEN = 1.04;
export const BALL_R = 0.31;

// A finger on the ball (the court's wrapBend, for a ball that is long, not
// round): the bend at a joint - at (x, y) in the finger's plane, heading phi,
// `at` taking a point there into his frame - that first brings the end of
// the next bone (l long, k from the knuckle) round onto the ball's surface
// (its own thickness, r, off it), and no further. Already into it without
// bending, bent back, a fingertip pressed flat on it, as far as keeps it on
// it (PRESS_MOST at the most); out of its reach, as it rests.
const BEND_STEP = 0.12;
const bendOnto = (
	at: (t: number, v: number) => V3,
	ball: HeldBall,
	r: number,
	x: number,
	y: number,
	phi: number,
	l: number,
	k: number,
	rest: number,
): number => {
	const A = BALL_LEN / 2 + r;
	const B = BALL_R + r;
	const inside = (a: number) => {
		const p = subV(
			at(x + l * Math.cos(phi + a), y + l * Math.sin(phi + a)),
			ball.at,
		);
		const al = dotV(p, ball.axis);
		const rr = dotV(p, p) - al * al;
		return (al * al) / (A * A) + rr / (B * B) < 1;
	};
	const lo = BEND_LO[k]!;
	const hi = BEND_HI[k]!;
	// (Between a and b, one in and one out: where it touches.)
	const edge = (a: number, b: number) => {
		const inA = inside(a);
		for (let n = 0; n < 5; n++) {
			const m = (a + b) / 2;
			if (inside(m) === inA) {
				a = m;
			} else {
				b = m;
			}
		}
		return (a + b) / 2;
	};
	if (inside(lo)) {
		const back = lo - PRESS_MOST;
		return inside(back) ? back : edge(lo, back);
	}
	for (let a = lo + BEND_STEP; a < hi + BEND_STEP; a += BEND_STEP) {
		const b = Math.min(a, hi);
		if (inside(b)) {
			return edge(b - BEND_STEP, b);
		}
	}
	return Math.min(hi, Math.max(lo, rest));
};

// ---- his uniform, on him ----------------------------------------------------

// Up his spine from his hips (feet): where his jersey tucks into his pants,
// and the tops of his shoulders (over his pads).
const tuckOf = (body: Body) => body.torso * 0.24;
const shouldersOf = (body: Body) => body.torso + body.depth * 0.55;

// How far down his upper arm his sleeve comes, and his pants down his shin.
const SLEEVE = 0.4;
const PANTS_HEM = 0.16;
// Round a sleeve (feet), about.
const SLEEVE_R = 0.3;

// How a uniform's picture wraps round him (see court3d/kitArt.ts): its
// jersey from the tops of his pads down to where it tucks in, its shorts
// as his pants, from there to their hems under his knees. A point of his
// thigh is taken as it would be with him standing straight - THIGH_TOP to
// THIGH_TOP + THIGH_DOWN thighs below his hips, hip to knee - and of his
// shin, on down from there.
const THIGH_TOP = 0.16;
const THIGH_DOWN = 0.68;
export const wrapsFor = (body: Body): { jersey: ArtWrap; pants: ArtWrap } => ({
	jersey: artWrap(
		body.shoulderW * 1.15,
		body.depth * 0.55,
		shouldersOf(body),
		tuckOf(body),
		ART_JERSEY,
	),
	pants: artWrap(
		body.hipW + body.thighR * 1.15,
		body.depth * 0.5,
		tuckOf(body),
		-(body.thigh * (THIGH_TOP + THIGH_DOWN) + body.shin * PANTS_HEM),
		ART_SHORTS,
	),
});
// A cheerleader's skirt, down her thigh this far; her socks up her shin to
// this far down it.
const SKIRT = 0.34;
const SOCK_TOP = 0.84;

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
// A canvas to lay it out on: the page's own, or one off the page (in a
// worker - see farm.ts).
const sheetCanvas = (w: number, h: number): HTMLCanvasElement | undefined => {
	if (typeof document !== "undefined") {
		const cv = document.createElement("canvas");
		cv.width = w;
		cv.height = h;
		return cv;
	}
	return typeof OffscreenCanvas === "undefined"
		? undefined
		: (new OffscreenCanvas(w, h) as unknown as HTMLCanvasElement);
};

// A lettered picture's lettering (the part of it under the swatches), as a
// picture to set characters from (as court3d's).
const glyphPictures = new WeakMap<KitArt, HTMLCanvasElement | null>();
const glyphPicture = (art: KitArt): HTMLCanvasElement | undefined => {
	let cv = glyphPictures.get(art);
	if (cv === undefined) {
		cv = null;
		const W = ART_W * art.scale;
		const top = ART_H * art.scale;
		const h = art.data.length / 4 / W - top;
		const made =
			h > 0 && typeof ImageData !== "undefined" ? sheetCanvas(W, h) : undefined;
		const g = made?.getContext("2d");
		if (made && g) {
			g.putImageData(new ImageData(art.data.slice(top * W * 4), W, h), 0, 0);
			cv = made;
		}
		glyphPictures.set(art, cv);
	}
	return cv ?? undefined;
};

// How tall a digit or a capital stands, in its type's size; and how far an
// arched name rises in its middle at full arch, in its width (court3d's).
const CAP = 0.72;
const ARCH = 0.12;

const jerseySheet = (
	look: FieldLook,
	body: Body,
	ppf0: number,
	waist: number,
): Sheet | undefined => {
	if (!look.jerseyNumber) {
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
	const cv = sheetCanvas(w, h);
	const g = cv?.getContext("2d", { willReadFrequently: true });
	if (!g) {
		byStep.set(step, null);
		return undefined;
	}
	const kit = look.kit;
	// A uniform from a picture: its lettering where and as big as it says,
	// and in its own type, if it has every character wanted.
	const art = look.kitArt;
	const fit = art?.fit ?? DEFAULT_FIT;
	const glyphs = art?.glyphs;
	const glyphPic = glyphs && art ? glyphPicture(art) : undefined;
	g.imageSmoothingEnabled = true;
	g.imageSmoothingQuality = "high";
	// A line set in the team's own lettering: each character as wide as it
	// was painted, the line squeezed into `maxW` and bent round the arch.
	const setLine = (
		str: string,
		x: number,
		cy: number,
		size: number,
		maxW: number,
		arch: number,
	) => {
		if (!glyphs || !glyphPic) {
			return false;
		}
		const top = ART_H * art!.scale;
		const boxes: (readonly [number, number, number, number] | undefined)[] = [];
		let rowH = 0;
		for (const ch of str.normalize("NFD").replaceAll(/[\u0300-\u036F]/g, "")) {
			if (ch === " ") {
				boxes.push(undefined);
				continue;
			}
			const box = glyphs[ch];
			if (!box) {
				return false;
			}
			boxes.push(box);
			rowH = Math.max(rowH, box[3]);
		}
		if (rowH === 0) {
			return false;
		}
		const k = (size * ppf * CAP) / rowH;
		const gap = rowH * 0.06;
		const space = rowH * 0.4;
		let total = -gap;
		for (const box of boxes) {
			total += (box ? box[2] : space) + gap;
		}
		const kx = k * Math.min(1, (maxW * ppf) / (total * k));
		const half = (total * kx) / 2;
		const A = arch * total * kx * ARCH;
		let at = x - half;
		for (const box of boxes) {
			const bw = (box ? box[2] : space) * kx;
			if (box) {
				const t = (at + bw / 2 - x) / half;
				g.save();
				g.translate(at + bw / 2, cy - A * (1 - t * t));
				g.rotate(Math.atan((2 * A * t) / half));
				g.drawImage(
					glyphPic,
					box[0],
					box[1] - top,
					box[2],
					box[3],
					-bw / 2,
					(-box[3] * k) / 2,
					bw,
					box[3] * k,
				);
				g.restore();
			}
			at += bw + gap * kx;
		}
		return true;
	};
	// Block numbers, the football way: heavy, square, edged.
	const text = (
		str: string,
		x: number,
		u: number,
		size: number,
		maxW: number,
		block: boolean,
		color: string,
		arch = 0,
	) => {
		if (!str) {
			return;
		}
		const cy = (u0 - u) * ppf;
		if (setLine(str, x, cy, size, maxW, arch)) {
			return;
		}
		const px = size * ppf;
		const squeeze = block ? 0.86 : 0.92;
		g.save();
		g.translate(x, cy);
		g.font = block
			? `900 ${px}px "Arial Black", "Helvetica Neue", Arial, sans-serif`
			: `bold ${px}px "Arial Narrow", "Helvetica Neue", Arial, sans-serif`;
		g.textAlign = "center";
		g.textBaseline = "middle";
		g.lineJoin = "round";
		g.lineWidth = Math.max(1, px * 0.12);
		g.strokeStyle = kit.numberEdge;
		g.fillStyle = color;
		if (arch === 0) {
			g.scale(squeeze, 1);
			if (block) {
				g.strokeText(str, 0, 0, maxW * ppf);
			}
			g.fillText(str, 0, 0, maxW * ppf);
		} else {
			// A letter at a time, round the arch.
			const chars = [...str];
			const widths = chars.map((ch) => g.measureText(ch).width * squeeze);
			const sum = widths.reduce((all, cw) => all + cw, 0);
			const fits = Math.min(1, (maxW * ppf) / sum);
			const half = (sum * fits) / 2;
			const A = arch * sum * fits * ARCH;
			let at = -half;
			for (const [i, ch] of chars.entries()) {
				const cw = widths[i]! * fits;
				const t = (at + cw / 2) / half;
				g.save();
				g.translate(at + cw / 2, -A * (1 - t * t));
				g.rotate(Math.atan((2 * A * t) / half));
				g.scale(squeeze * fits, 1);
				if (block) {
					g.strokeText(ch, 0, 0);
				}
				g.fillText(ch, 0, 0);
				g.restore();
				at += cw;
			}
		}
		g.restore();
	};
	const L = FIELD_LETTERING;
	// (An official's number on his back only.)
	text(
		look.official ? "" : look.jerseyNumber,
		w / 2,
		T * (L.number.u + fit.numberUp),
		body.H * L.number.size * fit.numberSize,
		aS * L.number.maxW * Math.max(1, fit.numberSize),
		true,
		kit.number,
	);
	for (const x of [0, w]) {
		text(
			look.jerseyNumber,
			x,
			T * (L.backNumber.u + fit.backUp),
			body.H * L.backNumber.size * fit.backSize,
			aS * L.backNumber.maxW * Math.max(1, fit.backSize),
			true,
			kit.number,
		);
		text(
			look.lastName.toUpperCase(),
			x,
			T * (L.name.u + fit.nameUp),
			body.H * L.name.size * fit.nameSize,
			aS * L.name.maxW,
			false,
			kit.name,
			fit.nameArch,
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
	// A crop top: bare from the waist up to here.
	crop?: number;
};

// ---- what the camera sees of him ---------------------------------------------

// His frame as the camera sees it: which way on him the sprite's right, down
// and toward the camera are - and, the field being seen from above, how much
// higher a point nearer the camera must be to stay on the same pixel.

type Built = {
	masses: Mass[];
	joins: { a: number; b: number; x: number; y: number; reach: number }[];
	frame: Frame;
	cut: Cut;
	pel: V3;
	spine: V3;
	fwd: V3;
	sheet?: Sheet;
	// A uniform drawn from a picture: how it wraps round him.
	wraps?: { jersey: ArtWrap; pants: ArtWrap };
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

// His helmet, as big round his head as it is (head radii) - a little over
// life size, to be made out at the broadcast's distance. (An official's
// bare head, in helmet radii.)
const HELMET = 1.1;
const OFFICIAL_HEAD = 0.9;
// (Its brim: a strip of masses side by side out over his eyes, across it
// this far out each side, head radii.)
const BRIM = [-0.36, -0.12, 0.12, 0.36];
// A coach's headset band: this far out over his cap (head radii), round
// this far either way of straight up, in so many pieces.
const BAND = 0.95;
const BAND_ARC = 1.45;
const BAND_STEPS = 6;
// A player's bare head, his helmet off - and the helmet in his hand, hung
// from it (helmet radii).
const BARE_HEAD = 0.86;
// The mascot's head (helmet radii - of a head that much bigger, see
// mascotBody).
const MASCOT_R = 0.9;
// (His nose as big as most; his hair standing up off his head from this
// high (head radii) - full round it, this big round past it.)
const NOSE_MID = 0.85;
const HAIR_UP = 0.15;
const HAIR_BIG = 0.5;
// A cheerleader's pom-pom: how big round (of her height).
const POM_R = 0.075;
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

// His hand: the court's (see handMasses), a glove on it - a touch fuller,
// no nails, his fingers a little closer together on the ball.
const HAND_BUILD: HandBuild = {
	halfW: 0.0136,
	palmR: 0.0157,
	fingerR: 0.0067,
	thumb: [0.0078, 0.0086],
	ballSpread: 0.16,
};
// His arm: the court's (see armMasses), a touch thicker all down it.
const ARM: ArmBuild = {
	delt: [1.12, 0.94, 0.88],
	biceps: [0.64, 0.76, 0.58],
	elbow: 0.64,
	fore: [0.92, 1.06, 0.8, 0.6],
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
	const P = seerOf(cam, fig, px, ox, oy);
	const { masses, add } = massesFor(P);
	const sw = body.shoulderW;
	const dp = body.depth;
	const H = body.H;
	const pel = sk.pelvis;
	// (See torsoOf.)
	const { spine, fwdT, turned, up } = torsoOf(body, q, pel);

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
	const cheer = look.cheer !== undefined;
	if (official || cheer || look.reporter) {
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
	if (cheer) {
		// Her skirt, flared out from her hips.
		torso(up(0.08, 0), up(-0.2, 0), [
			[0, body.hipW * 1.12],
			[1, body.hipW * 1.5],
		]);
	}

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
			[0, sw * (cheer || look.reporter ? 0.26 : official ? 0.3 : 0.42)],
			[1, sw * (cheer || look.reporter ? 0.22 : official ? 0.26 : 0.36)],
		],
		NECK,
		G_NECK,
	);
	let held: V3 | undefined;
	// A bare head's ears, either side of it, and nose, out from the face -
	// the head this big round (helmet radii), its middle this far up; the
	// ears and nose bigger or smaller as his own are (see BareHead).
	const features = (r: number, u0: number, head?: BareHead) => {
		const ear = 0.8 + 0.2 * clamp(head?.ear ?? 1, 0.5, 1.5);
		const nose =
			0.8 + 0.2 * clamp((head?.nose ?? NOSE_MID) / NOSE_MID, 0.6, 1.7);
		for (const sd of [-1, 1]) {
			add(
				at(-0.03, sd * r * 0.95, u0 + 0.03),
				at(-0.06, sd * r * 0.95, u0 - 0.17),
				[[0, R * 0.11 * ear]],
				SHELL,
				G_MASK,
			);
		}
		add(
			at(r * 0.88, 0, u0 + 0.02),
			at(r * (0.88 + 0.15 * nose), 0, u0 - 0.15),
			[
				[0, R * 0.08 * nose],
				[1, R * 0.11 * nose],
			],
			SHELL,
			G_MASK,
		);
	};
	// And his hair where it stands off it (see BareHead): up off the top of
	// it, full round it, down past his neck, tied up at the back of it.
	const hairOn = (r: number, u0: number, head?: BareHead) => {
		if (!head || head.cut === "bald") {
			return;
		}
		const st = head.style;
		const crown = u0 + r;
		if (head.cut === "big") {
			const big = HAIR_BIG + st.height;
			add(
				at(-0.3, 0, u0 + 0.36),
				at(-0.3, 0, u0 + 0.36),
				[[0, R * big]],
				HAIR,
				G_HELMET,
			);
		} else if (st.height >= HAIR_UP) {
			const up = st.height * r;
			if (st.strip) {
				add(
					at(0.3, 0, crown - 0.2 + up * 0.5),
					at(-0.55, 0, crown - 0.3 + up * 0.5),
					[[0, R * 0.2]],
					HAIR,
					G_HELMET,
				);
			} else {
				const w = st.top === "flat" ? 0.6 : 0.52;
				add(
					at(-0.08, 0, crown - w + 0.05),
					at(-0.08, 0, crown - w + 0.05 + up),
					[[0, R * w]],
					HAIR,
					G_HELMET,
				);
			}
		}
		if (head.cut === "long") {
			add(
				at(-0.55, 0, u0 + 0.2),
				at(-0.72, 0, u0 - 1.05),
				[
					[0, R * 0.62],
					[1, R * 0.42],
				],
				HAIR,
				G_HELMET,
			);
		}
		if (st.bun) {
			add(
				at(-0.78, 0, u0 + 0.5),
				at(-0.78, 0, u0 + 0.5),
				[[0, R * 0.3]],
				HAIR,
				G_HELMET,
			);
		}
	};
	if (official) {
		// His bare head, a cap on it, and its brim out over his eyes.
		add(at(0, 0, 0), at(0, 0, 0), [[0, R * OFFICIAL_HEAD]], SHELL, G_HELMET);
		features(OFFICIAL_HEAD, 0);
		for (const s of BRIM) {
			add(
				at(0.62, s, 0.25),
				at(1.3, s * 0.8, 0.13),
				[[0, R * 0.13]],
				SHELL,
				G_HELMET,
			);
		}
		if (fig.whistle) {
			// His whistle, out from between his lips.
			add(
				at(0.76, 0, -0.33),
				at(1.06, 0.03, -0.37),
				[[0, R * 0.075]],
				HEADSET,
				G_MASK,
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
				add(
					band(a0),
					band(a1),
					[[0, R * 0.07]],
					HEADSET,
					G_MASK,
					0,
					false,
					0.42,
				);
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
				false,
				0.42,
			);
		}
	} else if (cheer || look.reporter) {
		// Her head, and her ponytail down from the back of it.
		add(
			at(0, 0, -0.08),
			at(0, 0, -0.08),
			[[0, R * BARE_HEAD]],
			SHELL,
			G_HELMET,
		);
		features(BARE_HEAD, -0.08);
		add(
			at(-0.7, 0, 0.42),
			at(-1.08, 0, -0.62),
			[
				[0, R * 0.3],
				[0.4, R * 0.27],
				[1, R * 0.12],
			],
			SHELL,
			G_HELMET,
		);
	} else if (look.mascot) {
		// The mascot's head: big and round, a round ear up either side of
		// the top of it, its muzzle out in front, a nose on the end of that.
		add(at(0, 0, 0), at(0, 0, 0), [[0, R * MASCOT_R]], SHELL, G_HELMET);
		for (const sd of [-1, 1]) {
			add(
				at(-0.08, sd * 0.6, 0.66),
				at(-0.02, sd * 0.66, 0.74),
				[[0, R * 0.24]],
				SHELL,
				G_MASK,
			);
		}
		add(
			at(0.5, 0, -0.34),
			at(0.68, 0, -0.36),
			[[0, R * 0.36]],
			SHELL,
			G_HELMET,
		);
		add(at(1.02, 0, -0.2), at(1.02, 0, -0.2), [[0, R * 0.12]], SHELL, G_MASK);
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
		features(BARE_HEAD, -0.08, look.head);
		hairOn(BARE_HEAD, -0.08, look.head);
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
					false,
					barLeast,
				);
			}
		}
		add(at(0.86, 0, -0.86), at(0.86, 0, -0.86), [[0, R * 0.17]], CHIN, G_MASK);
	}

	const fwd = v3(1, 0, 0);
	const kMid = P(pel).k;
	const pn: [V3, V3] = [v3(0, 1, 0), v3(0, -1, 0)];
	const knuckleAt: V3[] = [];
	const thumbAt: V3[] = [];
	const arm = (limb: Limb, side: 0 | 1) => {
		hand(
			limb,
			side,
			armMasses(add, body, limb, side, G_ARM + side, UPPER, FORE, ARM),
		);
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
		if (cheer) {
			// A pom-pom, all round her hand.
			const L = H * 0.118;
			const knuckles = addV(wrist, h, L * 0.5);
			knuckleAt[side] = knuckles;
			thumbAt[side] = knuckles;
			add(
				addV(wrist, h, L * 0.1),
				addV(wrist, h, L * 0.9),
				[
					[0, H * POM_R * 0.8],
					[0.5, H * POM_R],
					[1, H * POM_R * 0.8],
				],
				POM,
				G_BALL,
				side,
			);
			return;
		}
		// (See handMasses: on the ball, each finger laid along it - see
		// bendOnto.)
		const onBall = ball && grip === "ball" ? ball : undefined;
		const made = handMasses({
			add,
			H,
			kMid,
			wrist,
			h,
			palmN,
			ts,
			side,
			group: g,
			fingers: G_FINGER + side * 5,
			kind: HAND,
			nail: undefined,
			grip,
			b: HAND_BUILD,
			onto: onBall
				? (base, d, r) => (x, y, phi, l, k, rest) =>
						bendOnto(
							(t, v) => addV(addV(base, d, t), palmN, v),
							onBall,
							r,
							x,
							y,
							phi,
							l,
							k,
							rest,
						)
				: undefined,
		});
		knuckleAt[side] = made.knuckles;
		thumbAt[side] = made.thumb;
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
		const shinDir = unit(subV(limb.end, limb.mid));
		const front = unit(square(fwd, shinDir), v3(1, 0, 0));
		if (!cheer) {
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
			add(
				addV(limb.mid, front, body.kneeR * 0.45),
				addV(limb.mid, front, body.kneeR * 0.45),
				[[0, body.kneeR * 0.82]],
				THIGH,
				gl,
				side,
			);
		}
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

	if (fig.ball && !fig.ball.unseen) {
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
	const frame = frameOf(cam, fig.yaw);
	const cut: Cut = {
		waist: tuckOf(body),
		belt: H * 0.014,
		top: shouldersOf(body),
		neckIn: sw * 0.4,
		vAt: H * 0.05,
		backAt: H * 0.02,
		trim: Math.max(H * 0.012, 1.2 / kMid),
		hipStripe: body.hipW + body.thighR * 0.8,
		...(cheer
			? { crop: tuckOf(body) + (shouldersOf(body) - tuckOf(body)) * 0.36 }
			: {}),
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
		...(look.kitArt && !look.official && !cheer && !look.mascot
			? { wraps: wrapsFor(body) }
			: {}),
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
	eyeBlack?: true;
	// An official's: the black of his striped shirt, and his hair under
	// his cap.
	stripes?: RGB;
	// The chain crew's vest over the shirt.
	vest?: RGB;
	hair: RGB;
	// His beard and brows (his hair's, but for a bald head).
	whiskers: RGB;
	// A cheerleader's pom-poms, their strands in two colors.
	poms?: [RGB, RGB];
	// The mascot's pale and dark shades of his fur.
	mascot?: { pale: RGB; dark: RGB };
};

// A bare head's beard, where his face has one (in the head's frame - see
// SHELL - helmet radii, across it either way): over his lip, on his chin,
// along his jaw, his sideburns.
// His mouth on his face, its top `top` up it (helmet radii): shut, a line
// across under his nose; open (see Figure.mouth), his jaw dropping it - dark
// inside, his top teeth along the top of it, his lips round it. Which of
// those is at (as across from the middle, lu up): 0 none of it, 1 the line
// or his lips, 2 inside, 3 his teeth.
const MOUTH_LINE = 0.06;
const MOUTH_W = 0.2;
const mouthOn = (lu: number, as: number, top: number, open: number) => {
	if (open <= 0) {
		return lu > top - MOUTH_LINE && lu < top && as < MOUTH_W ? 1 : 0;
	}
	const hh = MOUTH_LINE / 2 + 0.085 * open;
	const mid = top - hh + 0.01;
	const hw = MOUTH_W - 0.03 * open;
	const e = ((lu - mid) / hh) ** 2 + (as / hw) ** 2;
	return e > 1.3 ? 0 : e > 1 ? 1 : open > 0.3 && lu > mid + hh * 0.42 ? 3 : 2;
};
const MOUTH_RGB: RGB = [58, 20, 22];
// His eye, near enough to see it (EYE_SEEN_K sprite pixels a foot, and
// more): an almond round (as, lu) - half so wide, half so high (helmet
// radii) - the white of it, the dark of his iris in the middle, his upper
// lid's line over it. Which of those is at (as, lu): 0 none of it, 1 his
// lid, 2 the white, 3 his iris.
const EYE_SEEN_K = 16;
const eyeOn = (
	as: number,
	lu: number,
	eye: { as: number; lu: number; w: number; h: number },
) => {
	const x = (as - eye.as) / eye.w;
	const y = (lu - eye.lu) / eye.h;
	// (Pointed at the corners.)
	if (y * y + x * x > 1 || Math.abs(y) > 1 - x * x * 0.55) {
		return 0;
	}
	if (y > 0.5 - x * x * 0.4) {
		return 1;
	}
	return x * x * (eye.w / eye.h) ** 2 + (y + 0.08) ** 2 < 0.62 * 0.62 ? 3 : 2;
};
const BARE_EYE = { as: 0.27, lu: 0.07, w: 0.13, h: 0.075 };
const OFFICIAL_EYE = { as: 0.26, lu: -0.005, w: 0.1, h: 0.055 };
const EYE_WHITE: RGB = [226, 220, 210];
// (An official's, in the shade of his cap's brim.)
const SHADED_WHITE = 0.78;
const IRIS_RGB: RGB = [44, 30, 24];
const TEETH_RGB: RGB = [236, 230, 218];

const beardAt = (h: BareHead, lf: number, as: number, lu: number): boolean =>
	(h.lip === true && lf > 0.6 && lu > -0.41 && lu < -0.33 && as < 0.24) ||
	(h.chin === true && lf > 0.35 && lu < -0.52 && as < 0.4) ||
	(h.jaw === true && lf > 0.12 && lu < -0.36 && as > 0.28) ||
	(h.burns === true &&
		lf > 0.06 &&
		lf < 0.3 &&
		as > 0.62 &&
		lu > -0.36 &&
		lu < 0.12);

// The top, sides and back of a bare head: his hair over them as it is cut
// (see HairStyle) - none bald, or on top; faded at the sides; thin all over;
// in rows; down over his ears, long - his skin under it, his ears a shade
// darker.
const scalpAt = (
	pal: Palette,
	h: BareHead | undefined,
	lf: number,
	ls: number,
	lu: number,
): RGB => {
	const as = Math.abs(ls);
	const skin = scaled(pal.skin, as > 0.88 ? 0.86 : 1);
	const st = h?.style;
	if (h?.cut === "bald" || (st?.crown && lu > 0.5)) {
		return skin;
	}
	if (h?.cut === "long" && as > 0.7 && lu > -0.5) {
		return pal.hair;
	}
	if (!(lu > 0.3 || lf < -0.25)) {
		return skin;
	}
	if (st?.fade && !(lu > 0.42 || (lf < -0.25 && lu > 0.12))) {
		return toward(pal.skin, pal.hair, 0.4);
	}
	if (st?.thin) {
		return toward(pal.skin, pal.hair, 0.6);
	}
	if (st?.rows) {
		return Math.floor((ls + 1) * 5) % 2 ? scaled(pal.hair, 0.72) : pal.hair;
	}
	return pal.hair;
};

// The mascot's face (in the head's frame - see SHELL - helmet radii): a
// black nose, the inside of his ears pale, big white eyes with black ones
// in them under dark brows, a grin across his pale muzzle - the rest fur.
const NOSE_RGB: RGB = [42, 29, 23];
const GRIN_RGB: RGB = [120, 30, 36];
const mascotFace = (
	pal: Palette,
	m: { pale: RGB; dark: RGB },
	lf: number,
	ls: number,
	lu: number,
): RGB => {
	const as = Math.abs(ls);
	if (Math.hypot(lf - 1.02, as, lu + 0.2) < 0.15) {
		return NOSE_RGB;
	}
	if (Math.hypot(lf + 0.05, as - 0.63, lu - 0.7) < 0.27) {
		return lf > 0.04 ? m.pale : pal.skin;
	}
	if (lf > 0.5 && lu > -0.02 && lu < 0.36) {
		const ex = (as - 0.26) / 0.15;
		const ey = (lu - 0.17) / 0.18;
		if (ex * ex + ey * ey < 1) {
			const px = (as - 0.23) / 0.07;
			const py = (lu - 0.13) / 0.09;
			return px * px + py * py < 1 ? NOSE_RGB : WHITE_RGB;
		}
	}
	if (lf > 0.5 && lu > 0.38 && lu < 0.46 && as > 0.1 && as < 0.42) {
		return m.dark;
	}
	const muzzle = Math.hypot(
		Math.max(0, Math.abs(lf - 0.59) - 0.09),
		as,
		lu + 0.35,
	);
	if (muzzle < 0.39) {
		const grin = -0.5 + 0.9 * as * as;
		return as < 0.3 && Math.abs(lu - grin) < 0.04 ? GRIN_RGB : m.pale;
	}
	return pal.skin;
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
		// (A cheerleader's skirt: its waistband in her top's trim.)
		belt: look.cheer ? rgb(kit.collar) : scaled(rgb(kit.pants), 0.8),
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
		...(gear.eyeBlack ? { eyeBlack: true as const } : {}),
		...(kit.stripes ? { stripes: rgb(kit.stripes) } : {}),
		...(kit.vest ? { vest: rgb(kit.vest) } : {}),
		hair: look.hair ? rgb(look.hair) : scaled(skin, 0.38),
		whiskers: look.head
			? rgb(look.head.whiskers)
			: look.hair
				? rgb(look.hair)
				: scaled(skin, 0.38),
		...(look.cheer
			? { poms: [rgb(look.cheer.poms[0]), rgb(look.cheer.poms[1])] }
			: {}),
		...(look.mascot
			? {
					mascot: {
						pale: rgb(look.mascot.pale),
						dark: rgb(look.mascot.dark),
					},
				}
			: {}),
	};
	palettes.set(look, p);
	return p;
};

export type Sculpted = {
	img: ImageData;
	// The box round everything drawn (sprite pixels, ends included); empty
	// (x0 > x1) if nothing was.
	box: { x0: number; y0: number; x1: number; y1: number };
};

// His sprite, w x h of its pixels, each px screen pixels, its top left at
// (ox, oy) on the screen. (And, for the template a uniform's picture is
// painted on, which of the picture's pixels showed on him (1) and which
// the collar covered (2), marked in `seen`.)
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
	seen?: Uint8Array,
): Sculpted => {
	const b = build(cam, fig, body, look, px, ox, oy);
	const pal = paletteOf(look);
	const official = look.official === true;
	const open = fig.mouth ?? 0;
	const eyesSeen = b.k >= EYE_SEEN_K;
	const img = new ImageData(w, h);
	const d = img.data;
	const { rt, dn, tw: tw3 } = b.frame;
	const cut = b.cut;
	const pel = b.pel;
	const spine = b.spine;
	const fw = b.fwd;
	const sheet = b.sheet;
	const helm = b.helm;
	const decal = b.decal;
	const wraps = b.wraps;
	const art = look.kitArt;
	// (The collar the picture's own, not painted on.)
	const trimFromArt = art?.fit?.trim === "picture" && wraps !== undefined;
	const artXY = new Float64Array(2);
	const artPx = new Float64Array(3);
	const artRGB: RGB = [0, 0, 0];
	const mark = (v: number) => {
		const i = Math.round(artXY[1]!) * ART_W + Math.round(artXY[0]!);
		if (seen && i >= 0 && i < ART_W * ART_H && seen[i]! < v) {
			seen[i] = v;
		}
	};
	// The team's picture at a point on his jersey or pants, over the plain
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
	// The picture on his pants at a point (Pf, Ps, Pu) of his leg, its axis
	// from A to B, U up him as the point would be with him standing: how far
	// across him it is (square to the leg) and how far ahead.
	const pantsOver = (
		A: V3,
		B: V3,
		Pf: number,
		Ps: number,
		Pu: number,
		U: number,
		side: number,
		c: RGB,
	): RGB => {
		const lf = B.f - A.f;
		const ls = B.s - A.s;
		const lu = B.u - A.u;
		const ll = len3(lf, ls, lu) || 1;
		const ax = lf / ll;
		const ay = ls / ll;
		const az = lu / ll;
		let of = Pf - A.f;
		let os = Ps - A.s;
		let ou = Pu - A.u;
		const along = of * ax + os * ay + ou * az;
		of -= ax * along;
		os -= ay * along;
		ou -= az * along;
		let xf = -ax * ay;
		let xs = 1 - ay * ay;
		let xu = -az * ay;
		const xl = len3(xf, xs, xu) || 1;
		xf /= xl;
		xs /= xl;
		xu /= xl;
		const across = of * xf + os * xs + ou * xu;
		const ahead =
			of * (ay * xu - az * xs) +
			os * (az * xf - ax * xu) +
			ou * (ax * xs - ay * xf);
		const out = side === 0 ? -1 : 1;
		return artOver(
			wraps!.pants,
			U,
			out * body.hipW + across,
			ahead,
			// The inside of his leg faces the other one, not out to his side.
			across * out < 0,
			c,
		);
	};
	let cr = 0;
	let cg = 0;
	let cb = 0;
	let cloth = false;
	// Shiny: the helmet, its mask.
	let gloss = 0;
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
			if (wraps) {
				c = artOver(wraps.pants, U, S, F, false, c);
			} else if (Math.abs(S) > cut.hipStripe && Math.abs(F) < 0.11) {
				c = pal.pantsStripes[0] ?? c;
			}
			set(c);
			return;
		}
		if (U < cut.waist + cut.belt) {
			set(wraps ? artOver(wraps.pants, U, S, F, false, pal.belt) : pal.belt);
			return;
		}
		if (cut.crop !== undefined && U < cut.crop) {
			cloth = false;
			set(pal.skin);
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
		if (dNeck < cut.trim && !trimFromArt) {
			if (seen && wraps) {
				artAt(wraps.jersey, U, S, F, false, artXY);
				mark(2);
			}
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
		if (wraps && c === pal.jersey) {
			c = artOver(wraps.jersey, U, S, F, false, c);
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

	// What he wears at a point of him (see court3d/sculpt.ts shade): the
	// mass `fc`, t along it; the surface's way there (toward the camera's
	// right and down, and toward it) and the mass's radius there (sprite
	// pixels); the point (feet, his frame).
	const ink: Ink = { r: 0, g: 0, b: 0, cloth: false, gloss: 0 };
	const paint = (
		fc: Mass,
		t: number,
		hxf: number,
		hyf: number,
		hnf: number,
		hrf: number,
		Pf: number,
		Ps: number,
		Pu: number,
	) => {
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
						Math.min(TURN_STEPS, Math.max(0, Math.round(up0 * TURN_STEPS))) * 9;
					const nf = ut[k]! * rf0 + ut[k + 1]! * rs0 + ut[k + 2]! * ru0;
					const ns = ut[k + 3]! * rf0 + ut[k + 4]! * rs0 + ut[k + 5]! * ru0;
					const nu = ut[k + 6]! * rf0 + ut[k + 7]! * rs0 + ut[k + 8]! * ru0;
					rf0 = nf;
					rs0 = ns;
					ru0 = nu;
				}
				torsoAt(rf0 * spine.f + ru0 * spine.u, rs0, rf0 * fw.f + ru0 * fw.u);
				break;
			}
			case UPPER:
			case FORE: {
				const limb = b.arms[side]!;
				const a = fc.kind === UPPER ? alongOf(limb, Pf, Ps, Pu) : 1;
				let c: RGB;
				if (a < SLEEVE && !pal.poms) {
					// His jersey's sleeve, banded near its hem - or, an
					// official's, striped down the arm.
					c =
						bandOf(pal.sleeveStripes, (a - (SLEEVE - 0.17)) / 0.13) ??
						pal.jersey;
					if (pal.stripes && roundArm(limb, Pf, Ps, Pu, a) === 1) {
						c = pal.stripes;
					}
					if (wraps && c === pal.jersey) {
						// The picture's side of the jersey, down from his
						// shoulder - as if over his side, his arm down: its
						// front edge on the front of the sleeve.
						const j = wraps.jersey;
						const lf = limb.mid.f - limb.root.f;
						const ls = limb.mid.s - limb.root.s;
						const lu = limb.mid.u - limb.root.u;
						const ahead =
							(Pf - limb.root.f - lf * a) * fw.f +
							(Pu - limb.root.u - lu * a) * fw.u;
						c = artOver(
							j,
							body.torso - a * len3(lf, ls, lu),
							(side === 0 ? -1 : 1) * j.a,
							Math.max(-1, Math.min(1, ahead / SLEEVE_R)) * j.sideF,
							false,
							c,
						);
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
				if (pal.poms) {
					// (Under her skirt, bare.)
					cloth = a < SKIRT;
					set(cloth ? pal.pants : pal.skin);
					break;
				}
				const of = Pf - limb.root.f - (limb.mid.f - limb.root.f) * a;
				const os = Ps - limb.root.s - (limb.mid.s - limb.root.s) * a;
				const ou = Pu - limb.root.u - (limb.mid.u - limb.root.u) * a;
				const outward = (os * (side === 0 ? -1 : 1)) / (len3(of, os, ou) || 1);
				let c = pal.pants;
				const st = pal.pantsStripes;
				if (wraps) {
					c = pantsOver(
						limb.root,
						limb.mid,
						Pf,
						Ps,
						Pu,
						-(THIGH_TOP + THIGH_DOWN * a) * body.thigh,
						side,
						c,
					);
				} else if (st.length > 0 && outward > 0.92) {
					c = st.length > 1 && outward < 0.975 ? st[1]! : st[0]!;
				}
				set(c);
				break;
			}
			case SHIN: {
				cloth = true;
				const limb = b.legs[side]!;
				const bl = belowOf(limb, Pf, Ps, Pu);
				if (pal.poms) {
					// (Bare down to her socks.)
					cloth = bl > SOCK_TOP;
					set(cloth ? pal.sock : pal.skin);
					break;
				}
				if (bl >= PANTS_HEM) {
					set(pal.sock);
				} else if (wraps) {
					set(
						pantsOver(
							limb.mid,
							limb.end,
							Pf,
							Ps,
							Pu,
							-(
								(THIGH_TOP + THIGH_DOWN) * body.thigh +
								Math.max(0, bl) * body.shin
							),
							side,
							pal.pants,
						),
					);
				} else {
					set(pal.pants);
				}
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
				if (pal.mascot) {
					gloss = 0;
					set(mascotFace(pal, pal.mascot, lf, ls, lu));
					break;
				}
				if (look.bare || pal.poms || look.reporter) {
					// His face - his eyes, his brows over them, his
					// mouth; his hair over the top and round the back,
					// his ears either side a shade darker.
					gloss = 0;
					const as = Math.abs(ls);
					const hd = look.head;
					const said = lf > 0.5 ? mouthOn(lu, as, -0.41, open) : 0;
					if (said >= 2) {
						c = said === 3 ? TEETH_RGB : MOUTH_RGB;
					} else if (hd && beardAt(hd, lf, as, lu)) {
						c = pal.whiskers;
					} else if (lf > 0.3 && lu < 0.42) {
						const eyed = eyesSeen ? eyeOn(as, lu, BARE_EYE) : 0;
						const eye =
							!eyesSeen && lu > -0.02 && lu < 0.16 && as > 0.12 && as < 0.42;
						const smear =
							pal.eyeBlack &&
							lf > 0.5 &&
							lu > -0.12 &&
							lu < -0.06 &&
							as > 0.16 &&
							as < 0.36;
						const brow =
							lf > 0.5 && lu > 0.21 && lu < 0.28 && as > 0.1 && as < 0.44;
						const mouth = said === 1;
						c = brow
							? pal.whiskers
							: smear
								? EYE_BLACK_RGB
								: eyed === 3
									? IRIS_RGB
									: eyed === 2
										? EYE_WHITE
										: scaled(
												pal.skin,
												eye || eyed === 1 ? 0.34 : mouth ? 0.66 : 0.96,
											);
					} else {
						c = scalpAt(pal, hd, lf, ls, lu);
					}
					set(c);
					break;
				}
				if (official) {
					// His cap, and its brim; under it his face, his
					// eyes in its shadow; his hair round the back.
					gloss = 0;
					cloth = true;
					if (lu > 0.22 || (lf > 0.96 && lu > 0.05)) {
						set(c);
						break;
					}
					cloth = false;
					if (lf > 0.3) {
						const eyed = eyesSeen ? eyeOn(Math.abs(ls), lu, OFFICIAL_EYE) : 0;
						const eye =
							eyed === 1 ||
							(!eyesSeen &&
								lu > -0.06 &&
								lu < 0.05 &&
								Math.abs(ls) > 0.16 &&
								Math.abs(ls) < 0.36);
						const said = lf > 0.5 ? mouthOn(lu, Math.abs(ls), -0.33, open) : 0;
						c =
							said === 3
								? TEETH_RGB
								: said === 2
									? MOUTH_RGB
									: eyed === 3
										? IRIS_RGB
										: eyed === 2
											? scaled(EYE_WHITE, SHADED_WHITE)
											: scaled(
													pal.skin,
													eye
														? 0.34
														: lu > 0.06
															? 0.8
															: said === 1
																? 0.66
																: 0.96,
												);
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
					const smear =
						pal.eyeBlack &&
						lu > -0.26 &&
						lu < -0.16 &&
						Math.abs(ls) > 0.16 &&
						Math.abs(ls) < 0.34;
					if (eyes && pal.visor) {
						c = pal.visor;
						gloss = 1;
					} else {
						c = smear ? EYE_BLACK_RGB : scaled(pal.skin, eyes ? 0.42 : 0.72);
					}
				} else if (
					pal.stripe &&
					Math.abs(ls) < 0.14 &&
					lu > -0.15 &&
					lf < 0.5
				) {
					c =
						Math.abs(ls) > 0.1 && pal.stripeEdge ? pal.stripeEdge : pal.stripe;
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
			case POM: {
				// Her pom-pom: its strands, in the two colors.
				const [c0, c1] = pal.poms!;
				const n =
					Math.sin(Pf * 31 + Ps * 19) * Math.sin(Ps * 27 - Pu * 35) +
					0.6 * Math.sin(Pu * 23 + Pf * 13);
				cloth = true;
				gloss = 0.5;
				set(scaled(n > 0 ? c0 : c1, 0.86 + 0.14 * Math.abs(Math.sin(n * 5))));
				break;
			}
			case HEADSET: {
				gloss = 0.4;
				set(HEADSET_RGB);
				break;
			}
			case HAIR: {
				set(scaled(pal.hair, 0.94));
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
					const bar = Math.abs(hu - 0.55) < 0.08 || Math.abs(hu - 0.82) < 0.08;
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
					(rf2 * ball.laces.f + rs2 * ball.laces.s + ru2 * ball.laces.u) / rl;
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
				const nbf = hxf * rt.f + hyf * dn.f + hnf * tw3.f;
				const nbs = hxf * rt.s + hyf * dn.s + hnf * tw3.s;
				const nbu = hxf * rt.u + hyf * dn.u + hnf * tw3.u;
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
		ink.r = cr;
		ink.g = cg;
		ink.b = cb;
		ink.cloth = cloth;
		ink.gloss = gloss;
	};

	const box = shade(
		b,
		GROUPS,
		d,
		w,
		h,
		paint,
		ink,
		(kind) => kind === HAND || kind === MASK,
	);
	return { img, box };
};
