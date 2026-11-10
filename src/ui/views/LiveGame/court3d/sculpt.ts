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
	DEFAULT_FIT,
	type KitArt,
} from "./kitArt.ts";
import {
	bodyPoint,
	type Placed,
	gripped,
	holdOf,
	onRim,
	poseOf,
	type PlayerState,
} from "./evaluate.ts";
import { CAMERA_BODY, CAMERA_LENS, type Look } from "./figure.ts";
import {
	ballSurface,
	type BallSurface,
	facingCamera,
	HELD,
	surfaceFor,
} from "./gameBall.ts";
import {
	gripOf,
	skeleton,
	underPalm,
	turnUpper,
	type Body,
	type Limb,
	type Pose,
	type Skeleton,
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

export type RGB = [number, number, number];
const rgbs = new Map<string, RGB>();
export const rgb = (c: string): RGB => {
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
export const scaled = (c: RGB, f: number): RGB => [
	c[0] * f,
	c[1] * f,
	c[2] * f,
];

// ---- the masses -------------------------------------------------------------

// What a mass is, for painting it.
export const TORSO = 0;
export const SHORTS = 1;
export const UPPER = 2;
export const FORE = 3;
export const HAND = 4;
export const THIGH = 5;
export const SHIN = 6;
export const SHOE = 7;
export const NECK = 8;
export const BALL = 9;
export const CAM_BODY = 10;
export const CAM_LENS = 11;
export const TOWEL = 12;
export const NAIL = 13;
export const TONGUE = 14;

// What melts into what: masses in one group melt together everywhere, two
// groups only where a joint joins them.
const G_TORSO = 0;
const G_NECK = 1;
export const G_ARM = 2; // + side (0 his right, 1 his left)
const G_SHORTS = 4;
const G_LEG = 6;
const G_SHOE = 8;
export const G_BALL = 10;
const G_CAMERA = 11;
export const G_FINGER = 12; // + side * 5 + finger (4 the thumb)
export const G_HAND = 22; // + side
export const GROUPS = 24;

export type Mass = {
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
	flat?: boolean;
	x0: number;
	y0: number;
	x1: number;
	y1: number;
};

export const RS = 24;
export type Station = [t: number, r: number];
export const radiusAt = (prof: Station[], t: number): number => {
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

export const v3 = (f: number, s: number, u: number): V3 => ({ f, s, u });
export const lerpV = (a: V3, b: V3, t: number): V3 =>
	v3(a.f + (b.f - a.f) * t, a.s + (b.s - a.s) * t, a.u + (b.u - a.u) * t);
export const addV = (a: V3, b: V3, k = 1): V3 =>
	v3(a.f + b.f * k, a.s + b.s * k, a.u + b.u * k);
export const subV = (a: V3, b: V3): V3 => v3(a.f - b.f, a.s - b.s, a.u - b.u);
export const dotV = (a: V3, b: V3) => a.f * b.f + a.s * b.s + a.u * b.u;
export const lenV = (a: V3) => Math.hypot(a.f, a.s, a.u);
export const unit = (a: V3, or: V3 = v3(1, 0, 0)): V3 => {
	const l = lenV(a);
	return l > 1e-6 ? v3(a.f / l, a.s / l, a.u / l) : or;
};
// f, s, u turn the right way round: forward x left is up.
export const cross = (a: V3, b: V3): V3 =>
	v3(a.s * b.u - a.u * b.s, a.u * b.f - a.f * b.u, a.f * b.s - a.s * b.f);
// The part of `a` square to the unit `d`.
export const square = (a: V3, d: V3): V3 => addV(a, d, -dotV(a, d));

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
// The team's logo on the front of a warm-up top: its middle up his torso,
// and the most it may stand tall (in heights) and wide (in half-widths).
const CREST = { u: 0.64, size: 0.11, maxW: 1.15 };

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
// Each look's sheets, a size a step: the lettering laid out once, big (see
// SHEET_BIG), and each sheet shrunk from that.
const sheets = new WeakMap<
	Look,
	{
		big?: HTMLCanvasElement;
		byStep: Map<number, Sheet | null>;
	}
>();
// Laying the lettering out - a font looked up, every glyph drawn at a size
// not drawn before - is most of what a new sheet cost, and a new one was
// wanted for every man each time the camera took him a step bigger or
// smaller: a dozen of them at a cut, or as the warm-ups started, in one
// frame. So it is laid out once a look, at this size (sheet pixels to a
// foot: 1.25 ** 19, about 69 - a replay close-up's), and each smaller sheet
// shrunk from it, smoothly; only a camera in closer still has his laid out
// at its own size. (The same sheet whatever size was wanted first - so a
// hand off the page's thread paints him exactly as the page would.)
const SHEET_BIG = 19;
// A canvas to lay a sheet out on (in a worker making pictures for the page
// - see farm.ts - an offscreen one).
const sheetCanvas = (w: number, h: number) => {
	const cv =
		typeof document === "undefined"
			? (new OffscreenCanvas(w, h) as unknown as HTMLCanvasElement)
			: document.createElement("canvas");
	cv.width = w;
	cv.height = h;
	return cv;
};

// A lettered picture's lettering (the part of it under the swatches), as a
// picture to set characters from.
const glyphPictures = new WeakMap<KitArt, HTMLCanvasElement | null>();
const glyphPicture = (art: KitArt): HTMLCanvasElement | undefined => {
	let cv = glyphPictures.get(art);
	if (cv === undefined) {
		cv = null;
		const W = ART_W * art.scale;
		const top = ART_H * art.scale;
		const h = art.data.length / 4 / W - top;
		if (h > 0 && typeof ImageData !== "undefined") {
			const made = sheetCanvas(W, h);
			const g = made.getContext("2d");
			if (g) {
				g.putImageData(new ImageData(art.data.slice(top * W * 4), W, h), 0, 0);
				cv = made;
			}
		}
		glyphPictures.set(art, cv);
	}
	return cv ?? undefined;
};

// How tall a digit or a capital stands, in its type's size; and how far an
// arched name rises in its middle at full arch, in its width.
const CAP = 0.72;
const ARCH = 0.12;

const jerseySheet = (
	look: Look,
	body: Body,
	ppf0: number,
	waist: number,
): Sheet | undefined => {
	// (A warm-up top has the team's logo or name on its front, his name and
	// number on its back.)
	const warmup = look.outfit?.plain === true;
	if (
		(!look.jerseyNumber && !(warmup && (look.wordmark || look.crest))) ||
		(look.outfit && !warmup) ||
		(typeof document === "undefined" && typeof OffscreenCanvas === "undefined")
	) {
		return undefined;
	}
	// Made at about the size he is drawn, in steps.
	const step = Math.round(Math.log(Math.max(6, ppf0)) / Math.log(1.25));
	let mine = sheets.get(look);
	if (!mine) {
		mine = { byStep: new Map() };
		sheets.set(look, mine);
	}
	const kept = mine.byStep.get(step);
	if (kept !== undefined) {
		return kept ?? undefined;
	}
	const aS = body.shoulderW * 1.1;
	const u0 = body.torso + body.H * 0.08;
	const sizeOf = (s: number) => {
		const ppf = 1.25 ** s;
		return {
			ppf,
			w: Math.max(8, Math.round(2 * Math.PI * aS * ppf)),
			h: Math.max(8, Math.round((u0 - waist) * ppf) + 2),
		};
	};
	const { ppf, w, h } = sizeOf(step);
	let g: CanvasRenderingContext2D | null;
	if (step >= SHEET_BIG) {
		// (Its own, at this size - the one the rest are shrunk from, or one
		// for a camera in closer still.)
		const cv =
			step === SHEET_BIG && mine.big
				? mine.big
				: letterSheet(look, body, sizeOf(step), aS, u0, warmup);
		g = cv ? cv.getContext("2d", { willReadFrequently: true }) : null;
		if (cv && step === SHEET_BIG) {
			mine.big = cv;
		}
	} else {
		if (!mine.big) {
			const cv = letterSheet(look, body, sizeOf(SHEET_BIG), aS, u0, warmup);
			if (!cv) {
				mine.byStep.set(step, null);
				return undefined;
			}
			mine.big = cv;
		}
		g = sheetCanvas(w, h).getContext("2d", { willReadFrequently: true });
		if (g) {
			// (All the way round his torso the same: across, to this sheet's
			// width; down, at its size.)
			g.imageSmoothingEnabled = true;
			g.imageSmoothingQuality = "high";
			const from = mine.big;
			g.drawImage(from, 0, 0, w, from.height * (ppf / 1.25 ** SHEET_BIG));
		}
	}
	if (!g) {
		mine.byStep.set(step, null);
		return undefined;
	}
	const sheet = {
		data: g.getImageData(0, 0, w, h).data,
		w,
		h,
		ppf,
		aS,
		u0,
	};
	mine.byStep.set(step, sheet);
	return sheet;
};

// THE BIG SHEET ITSELF (see SHEET_BIG) and what a sheet made for a man
// drawn `ppf0` sheet pixels to a foot would be shrunk from it to - for the
// graphics card, which shrinks it itself (see sculptGl.ts). Nothing, where
// jerseySheet has none.
export type BigSheet = {
	big: HTMLCanvasElement;
	// The sheet jerseySheet would make: its size, its pixels to a foot.
	w: number;
	h: number;
	ppf: number;
	bigPpf: number;
	aS: number;
	u0: number;
};
export const bigSheet = (
	look: Look,
	body: Body,
	ppf0: number,
	waist: number,
): BigSheet | undefined => {
	const warmup = look.outfit?.plain === true;
	if (
		(!look.jerseyNumber && !(warmup && (look.wordmark || look.crest))) ||
		(look.outfit && !warmup) ||
		(typeof document === "undefined" && typeof OffscreenCanvas === "undefined")
	) {
		return undefined;
	}
	let mine = sheets.get(look);
	if (!mine) {
		mine = { byStep: new Map() };
		sheets.set(look, mine);
	}
	const aS = body.shoulderW * 1.1;
	const u0 = body.torso + body.H * 0.08;
	const sizeOf = (s: number) => {
		const ppf = 1.25 ** s;
		return {
			ppf,
			w: Math.max(8, Math.round(2 * Math.PI * aS * ppf)),
			h: Math.max(8, Math.round((u0 - waist) * ppf) + 2),
		};
	};
	if (!mine.big) {
		const cv = letterSheet(look, body, sizeOf(SHEET_BIG), aS, u0, warmup);
		if (!cv) {
			return undefined;
		}
		mine.big = cv;
	}
	const step = Math.round(Math.log(Math.max(6, ppf0)) / Math.log(1.25));
	return {
		big: mine.big,
		...sizeOf(step),
		bigPpf: 1.25 ** SHEET_BIG,
		aS,
		u0,
	};
};

// His number and lettering laid out on a new sheet this size.
const letterSheet = (
	look: Look,
	body: Body,
	{ ppf, w, h }: { ppf: number; w: number; h: number },
	aS: number,
	u0: number,
	warmup: boolean,
): HTMLCanvasElement | undefined => {
	const T = body.torso;
	const cv = sheetCanvas(w, h);
	const g = cv.getContext("2d", { willReadFrequently: true });
	if (!g) {
		return undefined;
	}
	const kit = look.kit;
	const art = look.kitArt;
	// (A warm-up top is lettered the usual way.)
	const fit = (!warmup && art?.fit) || DEFAULT_FIT;
	const glyphs = warmup ? undefined : art?.glyphs;
	const glyphPic = glyphs && art ? glyphPicture(art) : undefined;
	g.imageSmoothingEnabled = true;
	g.imageSmoothingQuality = "high";
	// A line set in the team's own lettering, if it has every character of
	// it: each as wide as it was painted, the line squeezed into `maxW` and
	// bent round the arch.
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
			const b = glyphs[ch];
			if (!b) {
				return false;
			}
			boxes.push(b);
			rowH = Math.max(rowH, b[3]);
		}
		if (rowH === 0) {
			return false;
		}
		const k = (size * ppf * CAP) / rowH;
		const gap = rowH * 0.06;
		const space = rowH * 0.4;
		let total = -gap;
		for (const b of boxes) {
			total += (b ? b[2] : space) + gap;
		}
		const kx = k * Math.min(1, (maxW * ppf) / (total * k));
		const half = (total * kx) / 2;
		const A = arch * total * kx * ARCH;
		let at = x - half;
		for (const b of boxes) {
			const bw = (b ? b[2] : space) * kx;
			if (b) {
				const t = (at + bw / 2 - x) / half;
				g.save();
				g.translate(at + bw / 2, cy - A * (1 - t * t));
				g.rotate(Math.atan((2 * A * t) / half));
				g.drawImage(
					glyphPic,
					b[0],
					b[1] - top,
					b[2],
					b[3],
					-bw / 2,
					(-b[3] * k) / 2,
					bw,
					b[3] * k,
				);
				g.restore();
			}
			at += bw + gap * kx;
		}
		return true;
	};
	const text = (
		str: string,
		x: number,
		u: number,
		size: number,
		edge: boolean,
		maxW: number,
		color = kit.number,
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
		g.save();
		g.translate(x, cy);
		g.scale(0.92, 1);
		g.font = `bold ${px}px "Arial Narrow", "Helvetica Neue", Arial, sans-serif`;
		g.textAlign = "center";
		g.textBaseline = "middle";
		g.lineJoin = "round";
		g.lineWidth = Math.max(1, px * 0.1);
		g.strokeStyle = kit.numberEdge;
		g.fillStyle = color;
		if (arch === 0) {
			if (edge) {
				g.strokeText(str, 0, 0, maxW * ppf);
			}
			g.fillText(str, 0, 0, maxW * ppf);
		} else {
			// A letter at a time, round the arch.
			const chars = [...str];
			const widths = chars.map((ch) => g.measureText(ch).width);
			const total = widths.reduce((sum, w) => sum + w, 0);
			const squeeze = Math.min(1, (maxW * ppf) / total);
			const half = (total * squeeze) / 2;
			const A = arch * total * squeeze * ARCH;
			let at = -half;
			for (const [i, ch] of chars.entries()) {
				const cw = widths[i]! * squeeze;
				const t = (at + cw / 2) / half;
				g.save();
				g.translate(at + cw / 2, -A * (1 - t * t));
				g.rotate(Math.atan((2 * A * t) / half));
				g.scale(squeeze, 1);
				if (edge) {
					g.strokeText(ch, 0, 0);
				}
				g.fillText(ch, 0, 0);
				g.restore();
				at += cw;
			}
		}
		g.restore();
	};
	// The chest: the team's name over the number - unless his uniform is a
	// picture, which has its own. A warm-up top: the team's logo alone, or
	// failing that its name.
	const L = LETTERING;
	const crest = warmup ? look.crest : undefined;
	if (!warmup) {
		text(
			look.jerseyNumber,
			w / 2,
			T * (L.number.u + fit.numberUp),
			body.H * L.number.size * fit.numberSize,
			true,
			aS * L.number.maxW * Math.max(1, fit.numberSize),
		);
	}
	if (crest && crest.width > 0 && crest.height > 0) {
		const k = Math.min(
			(aS * CREST.maxW * ppf) / crest.width,
			(body.H * CREST.size * ppf) / crest.height,
		);
		const cw = crest.width * k;
		const ch = crest.height * k;
		g.drawImage(
			crest,
			w / 2 - cw / 2,
			(u0 - T * CREST.u) * ppf - ch / 2,
			cw,
			ch,
		);
	} else if (!look.kitArt) {
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
	// The back, across the ends of the sheet: his name over a bigger number
	// (on a warm-up top, plain, in the color its front is lettered in).
	for (const x of [0, w]) {
		text(
			look.jerseyNumber,
			x,
			T * (L.backNumber.u + fit.backUp),
			body.H * L.backNumber.size * fit.backSize,
			!warmup,
			aS * L.backNumber.maxW * Math.max(1, fit.backSize),
			warmup ? kit.chest : kit.number,
		);
		text(
			look.lastName.toUpperCase(),
			x,
			T * (L.name.u + fit.nameUp),
			body.H * L.name.size * fit.nameSize,
			false,
			aS * L.name.maxW,
			warmup ? kit.chest : kit.name,
			fit.nameArch,
		);
	}
	return cv;
};

// Where the parts of his uniform start and stop, in feet on his torso: U up
// his spine from his hips, S to his left, F forward.
export type Cut = {
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
export type Frame = { rt: V3; dn: V3; tw: V3; zs: number; kap: number };

export type Built = {
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

// How his hands are held (see handMasses).
type Grip = HandGrip;

// Running, his hands are loose fists; with the ball, spread on it; pointing,
// a finger out; strolling or standing about, relaxed, fingers a little
// curled. Otherwise open: ready for the ball, or for him.
const RELAXED = new Set<string>([
	"jog",
	"walk",
	"back",
	"drift",
	"talk",
	"easy",
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
// How far a thigh is from upright (0, upright, to 1, level) before the leg
// of his shorts starts to hang off it, and how far (thigh widths) it hangs
// with the thigh level.
const SAG_FROM = 0.35;
const SAG = 0.9;
// A hand up at a shot or a block, or slapping a teammate's: its fingers
// spread wide, palm to it. (A low five's swung down to it.)
const SPLAY = new Set<string>([
	"contest",
	"contestTwo",
	"contestFace",
	"block",
	"blockTwo",
	"highFive",
]);
const SPLAY_UP = 0.5;
const LOW_FIVE = [0.12, 0.62] as const;
const splayed = (st: PlayerState, which: "R" | "L", h: V3) =>
	(SPLAY.has(st.anim) && h.u > SPLAY_UP) ||
	(st.anim === "lowFive" &&
		which === "R" &&
		st.phase > LOW_FIVE[0] &&
		st.phase < LOW_FIVE[1]);
// Up for a board: both hands spread and cupped for the ball.
const REACH = new Set<string>([
	"board",
	"boardTuck",
	"boardSplit",
	"boardHigh",
	"boardOne",
	"rebound",
]);
const REACH_UP = 0.3;
// AN OFFICIAL'S HANDS, as the signal has them: the whistle a fist straight
// up; a charge a fist punched out; a travel the fists rolled; a three
// three fingers up; a push both palms out.
const SIGNAL_UP = 0.5;
const signalOf = (
	anim: string,
	up: number,
	which: "R" | "L",
): Grip | undefined =>
	// (And a microphone, a camera, in his hand: his fingers round it - the
	// lens cupped in the other.)
	anim === "mic" && which === "R"
		? "fist"
		: (anim === "kneelShoot" || anim === "shootStand") && which === "R"
			? "fist"
			: anim === "kneelShoot" || anim === "shootStand"
				? "target"
				: anim === "signalCharge" || anim === "travel"
					? "fist"
					: anim === "signalUp" && up > SIGNAL_UP
						? "fist"
						: anim === "threeUp" && up > SIGNAL_UP
							? "three"
							: anim === "signalPush"
								? "splay"
								: undefined;
// Hung on the rim this far (see onRim): his hands hooked over it.
const HOOK_ON = 0.5;
// Hands clasped with another man's, from when to when, and which: a dap's
// right hand; both, pulling a man up off the floor or being pulled up.
const CLASP = new Map<string, readonly [number, number, "R" | "RL"]>([
	["dap", [0.28, 0.78, "R"]],
	["dapPat", [0.28, 0.78, "R"]],
	["dapHug", [0.14, 0.86, "R"]],
	["helpUp", [0.2, 0.82, "RL"]],
	["pulledUp", [0.18, 0.8, "RL"]],
]);
const clasped = (st: PlayerState, which: "R" | "L") => {
	const at = CLASP.get(st.anim);
	return (
		at !== undefined &&
		at[2].includes(which) &&
		st.phase > at[0] &&
		st.phase < at[1]
	);
};
const gripFor = (st: PlayerState, which: "R" | "L", holding: boolean): Grip => {
	if (
		(st.arm?.point && st.arm.hand === which && st.arm.w > 0.5) ||
		(which === "R" &&
			(st.anim === "point" ||
				st.anim === "stancePoint" ||
				(st.anim === "thanks" && st.phase > 0.15 && st.phase < 0.85)))
	) {
		return "point";
	}
	if (holding) {
		return "ball";
	}
	// (Celebrating on the run: a fist, three fingers.)
	if (st.arm?.grip && st.arm.hand === which && st.arm.w > 0.5) {
		return st.arm.grip;
	}
	// Up for a pass on its way to him: spread wide and cupped for it.
	if ((st.target ?? 0) > 0.4) {
		return "target";
	}
	if (
		st.anim === "run" ||
		st.anim === "sprint" ||
		st.anim === "flex" ||
		st.anim === "fistPump" ||
		st.anim === "roar" ||
		st.anim === "chestPound"
	) {
		return "fist";
	}
	return RELAXED.has(st.anim) ? "relaxed" : "open";
};

// How much of his upper body's turn a point of his torso t up his spine
// (0 his hips, 1 his chest) takes.
export const turnShare = (t: number): number => {
	const u = Math.min(1, Math.max(0, t));
	return u * u * (3 - 2 * u);
};

// The turn undone, a matrix about his hips for each of these steps up his
// torso (for painting what he wears where it was made to sit).
export const TURN_STEPS = 16;
export const untwistTable = (q: Pose, pel: V3): Float64Array => {
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
// How far each joint of a finger bends (radians) - the knuckle, the middle
// joint, the end one - back (a little past straight) and forward.
export const BEND_LO = [-0.3, 0, -0.15];
// The share of a bounce, at its top, the ball is in his hand on his dribble
// - from as it comes up into it to as it leaves.
const DRIBBLE_TOUCH = 0.12;
// A ball not his, this near (of his height, past its own surface) the middle
// of his hand: his hand on it.
const MEET_REACH = 0.05;
// Fingertips this wide (pixels, the radius) and more show their nails.
const NAIL_PX = 2.2;
export const BEND_HI = [1.6, 1.9, 1.3];

// A finger on a ball: the bend at a joint - at (x, y) in the finger's plane,
// heading phi - that first brings the end of the next bone (l long) round
// to the ball, cut through in that plane (round (cu, cv), rho across), and
// no further: the bone laid on it. Already into it without bending, bent
// back - a fingertip pressed flat on the ball - as far as keeps it on the
// surface (PRESS_MOST past its stop at the most); out of its reach, as it
// rests.
export const PRESS_MOST = 0.35;
export const wrapBend = (
	x: number,
	y: number,
	phi: number,
	l: number,
	cu: number,
	cv: number,
	rho: number,
	lo: number,
	hi: number,
	rest: number,
): number => {
	const qx = cu - x;
	const qy = cv - y;
	const D = Math.hypot(qx, qy);
	const K = (l * l + D * D - rho * rho) / (2 * l);
	const meets = D > 1e-6 && Math.abs(K) <= D;
	const tq = Math.atan2(qy, qx);
	const ac = meets ? Math.acos(K / D) : 0;
	const into =
		Math.hypot(
			x + l * Math.cos(phi + lo) - cu,
			y + l * Math.sin(phi + lo) - cv,
		) <= rho;
	if (into) {
		if (!meets) {
			return lo;
		}
		let back = -Infinity;
		for (const psi of [tq - ac, tq + ac]) {
			let a = psi - phi;
			a -= 2 * Math.PI * Math.ceil((a - lo) / (2 * Math.PI));
			back = Math.max(back, a);
		}
		return Math.max(lo - PRESS_MOST, back);
	}
	if (!meets) {
		return Math.min(hi, Math.max(lo, rest));
	}
	let best = Infinity;
	for (const psi of [tq - ac, tq + ac]) {
		let a = psi - phi;
		a -= 2 * Math.PI * Math.floor((a - lo) / (2 * Math.PI));
		if (a <= hi && a < best) {
			best = a;
		}
	}
	return best < Infinity ? best : Math.min(hi, Math.max(lo, rest));
};
// How far down from the top of his shoulders a quarter-zip's zip runs, as a
// share of the torso.
export const T_ZIP = 0.22;
// How far up the surface of his shoe's instep faces (0 level, 1 straight
// up) where its laces run.
export const TONGUE_UP = 0.62;
// Where his face is drawn, a little above the true middle of his head (see
// figure.ts).
const FACE_LIFT = 0.15;

// THE GROUNDWORK OF A FIGURE, any sport's (see each sport's build).

// Where a point on him (his own frame, feet - see bodyPoint) lands in his
// sprite, seen by `cam` from where he stands: in the sprite's pixels (`px`
// screen pixels each, from `ox`, `oy`), how far off, and how many of the
// sprite's pixels a foot is there.
export type Seen = { x: number; y: number; depth: number; k: number };
export const seerOf =
	(cam: Camera, at: Placed, px: number, ox: number, oy: number) =>
	(v: V3): Seen => {
		const p = project(cam, bodyPoint(at, v));
		return {
			x: (p.x - ox) / px,
			y: (p.y - oy) / px,
			depth: p.depth,
			k: p.k / px,
		};
	};

// The masses he is sculpted from, laid one at a time (`add`): from A to B,
// as thick along it as `prof` has it (feet), of a `kind` and in a `group`
// (each sport's own), on a `side` (0 his right, 1 his left); `flat`, cloth
// lying flat on him (see shade); `least`, the thinnest it may be drawn
// (sprite pixels) - a facemask's bars still there from up in the stands.
// (In the order laid: the first of two as near wins a pixel.)
export type AddMass = (
	A: V3,
	B: V3,
	prof: Station[],
	kind: number,
	group: number,
	side?: number,
	flat?: boolean,
	least?: number,
) => void;
export const massesFor = (
	P: (v: V3) => Seen,
): { masses: Mass[]; add: AddMass } => {
	const masses: Mass[] = [];
	const add: AddMass = (
		A,
		B,
		prof,
		kind,
		group,
		side = 0,
		flat = false,
		least = 0,
	) => {
		const a = P(A);
		const b = P(B);
		const k = (a.k + b.k) / 2;
		const rad = new Float32Array(RS + 1);
		let rmax = 0;
		for (let i = 0; i <= RS; i++) {
			const r = radiusAt(prof, i / RS) * k;
			rad[i] = least > 0 ? Math.max(least, r) : r;
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
	return { masses, add };
};

// His torso's own frame: up his spine from his hips (`pel`) at his lean,
// and forward of it - and `up`, a point on his torso, t up the spine, s to
// his left, f forward, turned on his hips as far up him as it is (see
// turnUpper).
export const torsoOf = (body: Body, q: Pose, pel: V3) => {
	const T = body.torso;
	const lean = (q.lean * Math.PI) / 180;
	const spine = v3(Math.sin(lean), 0, Math.cos(lean));
	const fwdT = v3(spine.u, 0, -spine.f);
	const turned = q.twist !== 0 || q.tilt !== 0;
	const up = (t: number, s: number, f = 0): V3 => {
		const p = v3(
			pel.f + spine.f * T * t + fwdT.f * f,
			s,
			pel.u + spine.u * T * t + fwdT.u * f,
		);
		return turned ? turnUpper(q, pel, turnShare(t))(p) : p;
	};
	return { T, spine, fwdT, turned, up };
};

// The camera's frame, in his - for a man facing `yaw`.
export const frameOf = (cam: Camera, yaw: number): Frame => {
	const cy = Math.cos(yaw);
	const sy = Math.sin(yaw);
	const toBody = (wx: number, wy: number): V3 =>
		v3(wx * cy + wy * sy, wx * sy - wy * cy, 0);
	const fh = Math.hypot(cam.fwd.x, cam.fwd.y) || 1;
	return {
		rt: toBody(cam.right.x, cam.right.y),
		dn: v3(0, 0, -1),
		tw: toBody(-cam.fwd.x / fh, -cam.fwd.y / fh),
		zs: fh,
		kap: (cam.fwd.x * cam.up.x + cam.fwd.y * cam.up.y) / fh,
	};
};

// HIS ARM, any sport's: the deltoid, narrowing to the elbow; the biceps in
// front of it, bunching up as the elbow bends; the point of the elbow
// behind; the forearm, swelling below the elbow and slimming to the wrist -
// as thick along it as a sport builds them (`b`), of its `upper` and `fore`
// kinds, in `group`. The way the forearm points, for the hand.
export type ArmBuild = {
	// The deltoid, of the upper arm's radius, a fifth and half way down it -
	// and of the forearm's at the elbow.
	delt: [number, number, number];
	// The biceps at its ends and middle, straight (of the upper arm's).
	biceps: [number, number, number];
	// The point of the elbow, and the forearm down from it (of its own).
	elbow: number;
	fore: [number, number, number, number];
};
export const ARM: ArmBuild = {
	delt: [1.16, 0.96, 0.9],
	biceps: [0.62, 0.76, 0.56],
	elbow: 0.62,
	// (Down to a wrist as wide as a wrist is: his hand is on the end of
	// his arm, not a mitten on a stick.)
	fore: [0.92, 1.08, 0.78, 0.68],
};
// (A lean man's, and a strong one's - see buildsOf.)
const ARM_LEAN: ArmBuild = {
	delt: [1.06, 0.9, 0.88],
	biceps: [0.58, 0.68, 0.54],
	elbow: 0.62,
	fore: [0.9, 1, 0.78, 0.68],
};
const ARM_STRONG: ArmBuild = {
	delt: [1.26, 1.02, 0.92],
	biceps: [0.66, 0.86, 0.58],
	elbow: 0.62,
	fore: [0.94, 1.16, 0.78, 0.68],
};
const FWD = v3(1, 0, 0);
const UP_V = v3(0, 0, 1);
export const armMasses = (
	add: AddMass,
	body: Body,
	limb: Limb,
	side: 0 | 1,
	group: number,
	upper: number,
	fore: number,
	b: ArmBuild,
): V3 => {
	const a = unit(subV(limb.mid, limb.root));
	const c = unit(subV(limb.end, limb.mid));
	// The inside of the elbow: the way the forearm bends from the upper arm
	// (forward, for an arm hanging straight).
	const bend = square(c, a);
	const inside = lenV(bend) > 0.08 ? unit(bend) : unit(square(FWD, a), UP_V);
	add(
		limb.root,
		limb.mid,
		[
			[0, body.upperR * 1.2],
			[0.2, body.upperR * b.delt[0]],
			[0.5, body.upperR * b.delt[1]],
			[1, body.foreR * b.delt[2]],
		],
		upper,
		group,
		side,
	);
	// (As the elbow bends, the biceps bunch up: thicker, shorter, gathered
	// toward the middle of the upper arm - straight, long and flat to it.)
	const flexed = Math.acos(Math.max(-1, Math.min(1, dotV(a, c))));
	const bunch = Math.max(0, Math.min(1, (flexed - 0.5) / 1.4));
	const bulge = bunch * bunch * (3 - 2 * bunch);
	add(
		addV(
			lerpV(limb.root, limb.mid, 0.36 + 0.06 * bulge),
			inside,
			body.upperR * (0.32 + 0.1 * bulge),
		),
		addV(
			lerpV(limb.root, limb.mid, 0.78 - 0.1 * bulge),
			inside,
			body.upperR * (0.28 + 0.08 * bulge),
		),
		[
			[0, body.upperR * (b.biceps[0] + 0.06 * bulge)],
			[0.5, body.upperR * (b.biceps[1] + 0.22 * bulge)],
			[1, body.upperR * (b.biceps[2] + 0.04 * bulge)],
		],
		upper,
		group,
		side,
	);
	add(
		addV(limb.mid, inside, -body.foreR * 0.36),
		addV(limb.mid, inside, -body.foreR * 0.36),
		[[0, body.foreR * b.elbow]],
		fore,
		group,
		side,
	);
	add(
		limb.mid,
		limb.end,
		[
			[0, body.foreR * b.fore[0]],
			[0.22, body.foreR * b.fore[1]],
			[0.7, body.foreR * b.fore[2]],
			[1, body.foreR * b.fore[3]],
		],
		fore,
		group,
		side,
	);
	return c;
};

// HIS HAND, any sport's: the palm out of the wrist (along `h`, facing
// `palmN`, his thumb's side `ts`), broad across and thin through; four
// fingers of three bones each, each joint bent the way the grip calls for -
// nearly straight (open), curled a little, the little finger most
// (relaxed), folded into the palm (a fist) - or, `onto` a ball, each joint
// bent just as far as keeps the next bone on its surface, the pads of his
// fingers on it, spread wide; the thumb, out from the heel of the palm on
// its side, two bones - across the fingers in a fist, round the side of
// the ball on it; and the ball of his thumb. As a real hand is, a touch over
// life so it reads: as long as a ninth of him, its palm about as wide as
// half its length, the fingers slim enough to stand apart - as each sport
// builds it (`b`, shares of his height). `nail`: the kind of the nails on
// the backs of his fingertips, where they are big enough to see (none, a
// glove on). Where his knuckles and his thumb grow from.
export type HandGrip =
	| "open"
	| "relaxed"
	| "fist"
	| "point"
	| "ball"
	| "target"
	| "dribble"
	| "splay"
	| "clasp"
	| "hook"
	| "three";
export type HandBuild = {
	halfW: number;
	palmR: number;
	fingerR: number;
	// The thumb's, as laid and as drawn.
	thumb: [number, number];
	// How far apart his fingers are on the ball.
	ballSpread: number;
};
export const HAND_BUILD: HandBuild = {
	halfW: 0.0125,
	palmR: 0.0148,
	fingerR: 0.006,
	thumb: [0.0076, 0.0084],
	ballSpread: 0.22,
};
// A joint's bend (k, the bone out from it), the finger so far `x` along and
// `y` in toward the palm at `phi`, the next bone `l` long - instead of
// `rest`, its bend at rest.
export type Bend = (
	x: number,
	y: number,
	phi: number,
	l: number,
	k: number,
	rest: number,
) => number;
export const handMasses = ({
	add,
	H,
	kMid,
	wrist,
	h,
	palmN,
	ts,
	side,
	group,
	fingers,
	kind,
	nail,
	grip,
	b,
	onto,
}: {
	add: AddMass;
	H: number;
	// Sprite pixels to a foot, about him.
	kMid: number;
	wrist: V3;
	h: V3;
	palmN: V3;
	ts: V3;
	side: 0 | 1;
	// The hand's group, and the first of its fingers' five.
	group: number;
	fingers: number;
	kind: number;
	nail: number | undefined;
	grip: HandGrip;
	b: HandBuild;
	// Laid on a ball: the bend that keeps a finger from `base`, along `d`,
	// `r` thick, on it (none, it does not reach).
	onto?: (base: V3, d: V3, r: number) => Bend | undefined;
}): { knuckles: V3; thumb: V3 } => {
	const L = H * 0.118;
	const halfW = H * b.halfW;
	const palmR = H * b.palmR;
	// The palm: two masses side by side.
	const knuckles = addV(wrist, h, L * 0.5);
	for (const o of [-1, 1]) {
		add(
			addV(addV(wrist, h, L * 0.1), ts, o * halfW * 0.8),
			addV(knuckles, ts, o * halfW),
			[
				[0, palmR * 0.92],
				[1, palmR],
			],
			kind,
			group,
			side,
		);
	}
	const fingerR = H * b.fingerR;
	const lens = [0.4, 0.44, 0.41, 0.33];
	// The joints of a finger (or the thumb) from base, set off along d: each
	// bone's length, its bend at rest, and how far it bends back and forward
	// - on the ball, as far as lays it on the ball instead.
	const lay = (
		base: V3,
		d: V3,
		bones: number[],
		rest: number[],
		r: number,
	): V3[] => {
		const pts = [base];
		const bend = onto?.(base, d, r);
		let x = 0;
		let y = 0;
		let phi = 0;
		bones.forEach((l, k) => {
			const a = bend ? bend(x, y, phi, l, k, rest[k]!) : rest[k]!;
			phi += a;
			x += l * Math.cos(phi);
			y += l * Math.sin(phi);
			pts.push(addV(addV(base, d, x), palmN, y));
		});
		return pts;
	};
	// Bone by bone, each a little slimmer than the last, melted into one.
	const bones = (pts: V3[], r: number, g: number) => {
		for (let k = 1; k < pts.length; k++) {
			const r0 = r * (1 - 0.07 * (k - 1));
			add(
				pts[k - 1]!,
				pts[k]!,
				[
					[0, r0],
					[1, r0 * (k === pts.length - 1 ? 0.84 : 0.95)],
				],
				kind,
				g,
				side,
			);
			// Big enough to see: the nail on the back of its tip.
			if (k === pts.length - 1 && r0 * kMid >= NAIL_PX && nail !== undefined) {
				const u = unit(subV(pts[k]!, pts[k - 1]!), h);
				const back = unit(square(palmN, u), palmN);
				const out = (t: number, o: number) =>
					addV(lerpV(pts[k - 1]!, pts[k]!, t), back, -r0 * o);
				add(
					out(0.48, 0.42),
					out(0.9, 0.36),
					[
						[0, r0 * 0.52],
						[1, r0 * 0.5],
					],
					nail,
					g,
					side,
				);
			}
		}
	};
	const restOf = (i: number): number[] =>
		grip === "fist" ||
		(grip === "point" && i > 0) ||
		(grip === "three" && i > 2)
			? [1.45, 1.75, 1.05]
			: grip === "point" || grip === "three"
				? [0.02, 0.04, 0.02]
				: grip === "relaxed"
					? [0.24 + 0.06 * i, 0.5 + 0.08 * i, 0.32 + 0.05 * i]
					: grip === "ball"
						? [0.2, 0.35, 0.25]
						: grip === "target"
							? [0.14, 0.26, 0.16]
							: grip === "dribble"
								? [0.18, 0.3, 0.2]
								: grip === "splay"
									? [-0.08, 0.03, 0.02]
									: grip === "clasp"
										? [0.5 + 0.05 * i, 0.8, 0.55]
										: grip === "hook"
											? [0.75, 1.05, 0.7]
											: [0.04 + 0.02 * i, 0.08, 0.05];
	const spread =
		grip === "ball"
			? b.ballSpread
			: grip === "target"
				? 0.22
				: grip === "dribble"
					? 0.17
					: grip === "splay"
						? 0.5
						: grip === "open"
							? 0.1
							: grip === "relaxed"
								? 0.05
								: 0.02;
	if (fingerR * kMid < 0.9 && grip !== "point" && grip !== "three" && !onto) {
		// Too small to tell apart: the four as one, flat like the palm, bent
		// as the middle two are.
		for (const o of [-1, 1]) {
			const pts = lay(
				addV(knuckles, ts, o * halfW * 0.85),
				h,
				[0.47, 0.3, 0.23].map((k) => k * L * 0.42),
				restOf(1),
				palmR * 0.8,
			);
			bones(pts, palmR * 0.88, fingers);
		}
	} else {
		for (let i = 0; i < 4; i++) {
			const across = (1.5 - i) / 1.5;
			// (The knuckles in an arc: the middle one furthest out.)
			const base = addV(
				addV(knuckles, ts, across * halfW * 1.2),
				h,
				L * [0.0, 0.025, 0.005, -0.04][i]!,
			);
			const len = L * lens[i]!;
			const pts = lay(
				base,
				unit(addV(h, ts, across * spread)),
				[0.47, 0.3, 0.23].map((k) => k * len),
				restOf(i),
				fingerR,
			);
			bones(pts, fingerR, fingers + i);
		}
	}
	const tBase = addV(addV(wrist, h, L * 0.16), ts, halfW * 1.05);
	const tLen = L * 0.36;
	const thumb =
		grip === "fist" ||
		grip === "point" ||
		grip === "three" ||
		grip === "clasp" ||
		grip === "hook"
			? (() => {
					const tip = addV(addV(knuckles, palmN, H * 0.016), ts, halfW * 0.1);
					const mid = addV(lerpV(tBase, tip, 0.5), palmN, H * 0.006);
					return [tBase, mid, tip];
				})()
			: lay(
					tBase,
					unit(
						addV(
							h,
							ts,
							grip === "splay"
								? 1.35
								: grip === "ball" || grip === "target"
									? 1.1
									: grip === "open"
										? 0.85
										: 0.6,
						),
					),
					[tLen * 0.55, tLen * 0.45],
					grip === "relaxed"
						? [0.3, 0.35]
						: grip === "ball"
							? [0.2, 0.2]
							: [0.1, 0.12],
					H * b.thumb[0],
				);
	bones(thumb, H * b.thumb[1], fingers + 4);
	// The ball of his thumb: the muscle of the palm on its side, swelling out
	// to where the thumb leaves it.
	add(
		addV(addV(wrist, h, L * 0.1), ts, halfW * 0.55),
		addV(addV(wrist, h, L * 0.3), ts, halfW * 0.95),
		[
			[0, palmR * 0.72],
			[1, palmR * 0.66],
		],
		kind,
		group,
		side,
	);
	return { knuckles, thumb: tBase };
};

// HIS LEG, any sport's: the thigh down to the knee; the shin, the calf
// swelling behind it; the shoe, heel to toe along the ground, and a collar
// round the ankle - as thick along it as a sport builds them (`b`, of the
// body's own radii there), of its kinds and groups. Whatever a sport has
// on it besides, laid in their places: at the knee (`knee`, with which way
// is in front of it) and over the thigh after the calf (`over`).
export type LegBuild = {
	// How far down from his hip the thigh starts, and its profile.
	thighFrom: number;
	thigh: [number, number, number];
	shin: [number, number, number, number];
	calf: [number, number, number];
	// How far out past the toe of his foot the shoe goes; the heel's and the
	// toe's height off the ground (of the ankle's radius) - at least, and
	// below the bone; and how thick it is along it, and its collar.
	toe: number;
	heel: [number, number];
	toeUp: [number, number];
	shoe: [number, number, number];
	collar: [number, number];
	// A HIGH-TOP, where a sport's shoe is one: the instep, sloping from the
	// front of his ankle down to the toe box - where along his foot it
	// starts and stops, how high over the toe it ends (of the ankle's
	// radius) and how thick it is at each end - laced; and the collar on up
	// his ankle - how far (of the ankle's radius), how thick at the bottom
	// and the top.
	instep?: [number, number, number, number, number];
	high?: [number, number, number];
};
export const LEG: LegBuild = {
	thighFrom: 0.35,
	// (Into the knee as wide as the knee is, and the calf out wider than it
	// below: a leg, not two rods and a ball between them.)
	thigh: [1, 1.02, 1.12],
	shin: [1.02, 1.08, 1.1, 1],
	calf: [0.86, 1, 0.6],
	// (A basketball shoe: low and narrow at the toe, built up over the
	// instep, and on up round the ankle.)
	toe: 1.02,
	heel: [1, 0.15],
	toeUp: [0.8, 0.25],
	shoe: [1.2, 1.3, 0.98],
	collar: [1.12, 1.2],
	instep: [0.06, 0.6, 0.45, 1.18, 0.82],
	high: [2, 1.3, 1.2],
};
const LEG_LEAN: LegBuild = {
	...LEG,
	thigh: [0.96, 0.96, 1.1],
	shin: [1.02, 1.02, 1.1, 1],
	calf: [0.82, 0.92, 0.6],
};
const LEG_STRONG: LegBuild = {
	...LEG,
	thigh: [1.04, 1.08, 1.14],
	shin: [1.02, 1.14, 1.1, 1],
	calf: [0.9, 1.08, 0.6],
};
export const legMasses = (
	add: AddMass,
	body: Body,
	limb: Limb,
	side: 0 | 1,
	{
		thigh,
		shin,
		shoe,
		tongue = shoe,
	}: { thigh: number; shin: number; shoe: number; tongue?: number },
	group: number,
	shoeGroup: number,
	b: LegBuild,
	knee?: (front: V3) => void,
	over?: () => void,
) => {
	add(
		lerpV(limb.root, limb.mid, b.thighFrom),
		limb.mid,
		[
			[0, body.thighR * b.thigh[0]],
			[0.4, body.thighR * b.thigh[1]],
			[1, body.kneeR * b.thigh[2]],
		],
		thigh,
		group,
		side,
	);
	const shinDir = unit(subV(limb.end, limb.mid));
	const front = unit(square(FWD, shinDir), v3(1, 0, 0));
	knee?.(front);
	add(
		limb.mid,
		limb.end,
		[
			[0, body.kneeR * b.shin[0]],
			[0.3, body.calfR * b.shin[1]],
			[0.8, body.ankleR * b.shin[2]],
			[1, body.ankleR * b.shin[3]],
		],
		shin,
		group,
		side,
	);
	add(
		addV(lerpV(limb.mid, limb.end, 0.12), front, -body.calfR * 0.3),
		addV(lerpV(limb.mid, limb.end, 0.56), front, -body.calfR * 0.16),
		[
			[0, body.calfR * b.calf[0]],
			[0.4, body.calfR * b.calf[1]],
			[1, body.calfR * b.calf[2]],
		],
		shin,
		group,
		side,
	);
	over?.();
	const tip = limb.tip ?? limb.end;
	const heel = lerpV(limb.end, tip, -0.32);
	const toe = lerpV(limb.end, tip, b.toe);
	add(
		v3(
			heel.f,
			heel.s,
			Math.max(body.ankleR * b.heel[0], heel.u - body.ankleR * b.heel[1]),
		),
		v3(
			toe.f,
			toe.s,
			Math.max(body.ankleR * b.toeUp[0], toe.u - body.ankleR * b.toeUp[1]),
		),
		[
			[0, body.ankleR * b.shoe[0]],
			[0.5, body.ankleR * b.shoe[1]],
			[1, body.ankleR * b.shoe[2]],
		],
		shoe,
		shoeGroup,
		side,
	);
	const collar = lerpV(limb.end, tip, 0.25);
	add(
		lerpV(limb.end, tip, -0.12),
		v3(collar.f, collar.s, limb.end.u - body.ankleR * 0.2),
		[
			[0, body.ankleR * b.collar[0]],
			[1, body.ankleR * b.collar[1]],
		],
		shoe,
		shoeGroup,
		side,
	);
	if (b.instep) {
		const [from, to, over, r0, r1] = b.instep;
		const A = lerpV(limb.end, tip, from);
		const B = lerpV(limb.end, tip, to);
		add(
			v3(A.f, A.s, limb.end.u + body.ankleR * 0.1),
			v3(
				B.f,
				B.s,
				Math.max(body.ankleR * b.toeUp[0], toe.u - body.ankleR * b.toeUp[1]) +
					body.ankleR * over,
			),
			[
				[0, body.ankleR * r0],
				[1, body.ankleR * r1],
			],
			tongue,
			shoeGroup,
			side,
		);
	}
	if (b.high) {
		const [up, r0, r1] = b.high;
		const shinLen = lenV(subV(limb.mid, limb.end)) || 1;
		add(
			limb.end,
			lerpV(limb.end, limb.mid, Math.min(0.45, (body.ankleR * up) / shinLen)),
			[
				[0, body.ankleR * r0],
				[1, body.ankleR * r1],
			],
			shoe,
			shoeGroup,
			side,
			true,
		);
	}
};

// WHERE HIS MASSES MELT INTO ONE ANOTHER, any sport's: two groups (`a`,
// `b`) at `at`, as far round it as `reachFt` (see shade) - his own at his
// shoulders, his hips, between his legs, his neck, and each finger and his
// thumb into his palm, as each sport has his groups and their reach; and,
// `wrists`, his hands onto his forearms.
export type BodyJoins = {
	torso: number;
	neck: number;
	// The first of each side's two (his right, his left).
	arm: number;
	leg: number;
	hand: number;
	// The first of his right hand's five (four fingers, then his thumb), his
	// left's after them.
	finger: number;
	// The reach at his shoulders (of his upper arm's radius), his hips (of
	// his thigh's) and his neck (of his shoulders' width).
	shoulder: number;
	hip: number;
	neckReach: number;
	wrists: boolean;
};
export const joinsFor = (P: (v: V3) => Seen) => {
	const joins: Join[] = [];
	const join = (a: number, b: number, at: V3, reachFt: number) => {
		const p = P(at);
		joins.push({ a, b, x: p.x, y: p.y, reach: reachFt * p.k });
	};
	return { joins, join };
};
export const joinBody = (
	join: (a: number, b: number, at: V3, reachFt: number) => void,
	g: BodyJoins,
	sk: Skeleton,
	body: Body,
	up: (t: number, s: number, f?: number) => V3,
	knuckleAt: V3[],
	thumbAt: V3[],
) => {
	const H = body.H;
	join(g.torso, g.arm, sk.armR.root, body.upperR * g.shoulder);
	join(g.torso, g.arm + 1, sk.armL.root, body.upperR * g.shoulder);
	join(g.torso, g.leg, sk.legR.root, body.thighR * g.hip);
	join(g.torso, g.leg + 1, sk.legL.root, body.thighR * g.hip);
	join(g.leg, g.leg + 1, up(-0.1, 0), body.thighR * 2.2);
	join(g.torso, g.neck, up(0.95, 0), body.shoulderW * g.neckReach);
	for (const side of [0, 1]) {
		for (let i = 0; i < 4; i++) {
			join(g.hand + side, g.finger + side * 5 + i, knuckleAt[side]!, H * 0.04);
		}
		join(g.hand + side, g.finger + side * 5 + 4, thumbAt[side]!, H * 0.035);
		if (g.wrists) {
			join(
				g.arm + side,
				g.hand + side,
				side === 0 ? sk.armR.end : sk.armL.end,
				H * 0.03,
			);
		}
	}
};

// HIS TORSO, any sport's: belly and back; the chest out in front; the lats,
// widening a little up to his armpits; across the top of him, his
// shoulders (`shoulders`: each sport's - its pads, say); hips and seat - as
// a sport builds them (`b`: how far up him, how wide - of his shoulders' -
// and how far forward, and how thick, of his depth), on `up` (see torsoOf).
export type TorsoBuild = {
	// The belly's top, and how thick it is up it.
	belly: number;
	bellyR: [number, number, number];
	// How far up his chest is, how wide, how far forward, how thick.
	chest: [number, number, number];
	chestR: [number, number, number];
	// The lats: how wide at his waist, how far up to his armpits and how
	// wide there, and how thick.
	lats: [number, number, number];
	latsR: [number, number];
};
export const TORSO_BUILD: TorsoBuild = {
	belly: 0.9,
	bellyR: [0.41, 0.4, 0.46],
	chest: [0.68, 0.42, 0.062],
	chestR: [0.37, 0.4, 0.37],
	lats: [0.38, 0.76, 0.58],
	latsR: [0.34, 0.37],
};
const TORSO_LEAN: TorsoBuild = {
	...TORSO_BUILD,
	chest: [0.68, 0.42, 0.05],
	chestR: [0.355, 0.38, 0.355],
	lats: [0.38, 0.76, 0.54],
};
const TORSO_STRONG: TorsoBuild = {
	...TORSO_BUILD,
	chest: [0.68, 0.42, 0.075],
	chestR: [0.385, 0.42, 0.385],
	lats: [0.38, 0.76, 0.62],
};

// HIS OWN ARMS, LEGS AND TORSO: built for the muscle on him (see Body's
// `muscle`) - between a lean man's and a strong one's, the ordinary man's
// half way - where it is known; the ordinary man's where it is not.
type Builds = { arm: ArmBuild; leg: LegBuild; torso: TorsoBuild };
const ORDINARY: Builds = { arm: ARM, leg: LEG, torso: TORSO_BUILD };
const between = <T>(lo: T, hi: T, t: number): T => {
	if (typeof lo === "number" && typeof hi === "number") {
		return (lo + (hi - lo) * t) as T;
	}
	if (Array.isArray(lo) && Array.isArray(hi)) {
		return lo.map((x, i) => between(x, hi[i], t)) as T;
	}
	const out: Record<string, unknown> = {};
	for (const k of Object.keys(lo as object)) {
		out[k] = between(
			(lo as Record<string, unknown>)[k],
			(hi as Record<string, unknown>)[k],
			t,
		);
	}
	return out as T;
};
const builds = new WeakMap<Body, Builds>();
const buildsOf = (body: Body): Builds => {
	if (body.muscle === undefined) {
		return ORDINARY;
	}
	let b = builds.get(body);
	if (!b) {
		const t = Math.min(1, Math.max(0, body.muscle));
		b = {
			arm: between(ARM_LEAN, ARM_STRONG, t),
			leg: between(LEG_LEAN, LEG_STRONG, t),
			torso: between(TORSO_LEAN, TORSO_STRONG, t),
		};
		builds.set(body, b);
	}
	return b;
};
export const torsoMasses = (
	torso: (A: V3, B: V3, prof: Station[]) => void,
	up: (t: number, s: number, f?: number) => V3,
	body: Body,
	b: TorsoBuild,
	shoulders: () => void,
) => {
	const sw = body.shoulderW;
	const dp = body.depth;
	// (His middle as thick for his chest as his own build has it.)
	const mid = body.belly ?? 1;
	torso(up(0.04, 0), up(b.belly, 0, -dp * 0.04), [
		[0, dp * b.bellyR[0] * mid],
		[0.45, dp * b.bellyR[1] * mid],
		[1, dp * b.bellyR[2] * Math.sqrt(mid)],
	]);
	torso(
		up(b.chest[0], -sw * b.chest[1], dp * b.chest[2]),
		up(b.chest[0], sw * b.chest[1], dp * b.chest[2]),
		[
			[0, dp * b.chestR[0]],
			[0.5, dp * b.chestR[1]],
			[1, dp * b.chestR[2]],
		],
	);
	for (const sd of [-1, 1]) {
		torso(up(0.2, sd * sw * b.lats[0]), up(b.lats[1], sd * sw * b.lats[2]), [
			[0, dp * b.latsR[0]],
			[1, dp * b.latsR[1]],
		]);
	}
	shoulders();
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
};

// A microphone this far from his own mouth (feet) is held out to somebody
// else's.
const MIC_OWN = 1.1;
// (`noSheet`: his lettering not laid out as pixels - whoever asked paints it
// from the big sheet itself, see bigSheet.)
export const build = (
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	px: number,
	ox: number,
	oy: number,
	noSheet = false,
): Built => {
	const q = poseOf(st);
	// (Just let go of it, his hands come off where it was.)
	const held =
		st.holding || st.grip !== undefined ? holdOf(st, body, q) : undefined;
	const sk = held
		? gripped(held.sk, st, body, q)
		: onRim(skeleton(body, q), st, body);
	const P = seerOf(cam, st, px, ox, oy);
	const { masses, add } = massesFor(P);
	const sw = body.shoulderW;
	const dp = body.depth;
	const H = body.H;
	const pel = sk.pelvis;
	// His torso as it would stand untwisted, up his spine from his hips (see
	// torsoOf).
	const { T, spine, fwdT, turned, up } = torsoOf(body, q, pel);

	// The torso: belly and back, the chest out in front, the lats widening a
	// little up to his armpits, across the tops of his shoulders; hips and
	// seat.
	const torso = (A: V3, B: V3, prof: Station[]) =>
		add(A, B, prof, TORSO, G_TORSO);
	const mine = buildsOf(body);
	torsoMasses(torso, up, body, mine.torso, () => {
		torso(up(0.86, -sw * 0.6), up(0.86, sw * 0.6), [
			[0, dp * 0.38],
			[0.5, dp * 0.42],
			[1, dp * 0.38],
		]);
	});
	// The neck, thick at the base, up into his head; the traps sloping up
	// either side of it from his shoulders.
	add(
		up(0.9, 0, -dp * 0.05),
		v3(sk.head.f, 0, sk.head.u - body.headR * 0.6),
		[
			[0, sw * 0.43],
			[0.5, sw * 0.35],
			[1, sw * 0.33],
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
		hand(
			limb,
			side,
			armMasses(add, body, limb, side, G_ARM + side, UPPER, FORE, mine.arm),
		);
	};

	// The hand: a palm out of the wrist, four fingers and a thumb, held the
	// way the moment calls for - a cartoon's, a little bigger than life and
	// broader than the wrist, so it reads.
	const hand = (limb: Limb, side: 0 | 1, foreDir: V3) => {
		const which = side === 0 ? "R" : "L";
		const sgn = side === 0 ? -1 : 1;
		// (A jumper's guide hand, once it has come off the ball, is open.)
		const off = which === "L" && gripOf(st.anim) === "shot" && q.free > 0.5;
		// Dribbling, the ball up in this hand at the top of its bounce: his
		// palm down on it, his fingers spread over it.
		const ph = st.dribble;
		const dribbling = !held && ph !== undefined && st.dribbleHand === which;
		const touch = dribbling
			? Math.max(0, 1 - Math.min(ph, 1 - ph) / DRIBBLE_TOUCH)
			: 0;
		// (Between, his hand loose and spread over where it comes back up
		// to - and a shooter's, the ball gone off it, loose off his snapped
		// wrist.)
		const flick = which === "R" && gripOf(st.anim) === "shot" && q.free > 0.5;
		const wrist = limb.end;
		const tip = limb.tip ?? limb.end;
		let h = lenV(subV(tip, wrist)) > 1e-3 ? unit(subV(tip, wrist)) : foreDir;
		// A ball he isn't holding, at this hand - a rebound, a tip, a block,
		// a pass come into his hands, another man's ball he has a hand on: his
		// palm onto it, his fingers laid on it where they reach it (open and
		// flat on it, another man's: a reach at it, not a hold of it).
		const by = st.ballBy;
		const loose =
			!held && !dribbling && by
				? v3(
						by.x * Math.cos(st.yaw) + by.y * Math.sin(st.yaw),
						by.x * Math.sin(st.yaw) - by.y * Math.cos(st.yaw),
						by.z,
					)
				: undefined;
		const meets =
			loose !== undefined &&
			lenV(subV(loose, addV(wrist, h, H * 0.062))) <
				HELD_BALL_R + H * MEET_REACH;
		const grip: Grip =
			touch > 0.3 || (meets && !by?.held)
				? "ball"
				: meets
					? "open"
					: dribbling
						? "dribble"
						: flick
							? "relaxed"
							: !held && st.palmOn?.[which]
								? "open"
								: held
									? gripFor(st, which, !off)
									: // Hung on the rim: fingers hooked over the iron.
										(st.dunk?.grip ?? 0) > HOOK_ON
										? "hook"
										: splayed(st, which, h)
											? "splay"
											: clasped(st, which)
												? "clasp"
												: REACH.has(st.anim) && h.u > REACH_UP
													? "target"
													: (signalOf(st.anim, h.u, which) ??
														gripFor(st, which, false));
		// The ball his hand is on, if it is.
		const cup =
			held && grip === "ball"
				? held.ball
				: touch > 0.3
					? underPalm(limb)
					: meets
						? loose
						: undefined;
		// Which way the palm faces: hanging, in toward his middle and a little
		// back - the back of the hand turned out to the camera, the way a
		// cartoon shows it; raised, toward what is in front of him; on the
		// ball, onto the ball.
		const inward = v3(0, -sgn, 0);
		let palmN = unit(
			square(addV(inward, fwd, -0.75 + 1.9 * Math.max(0, h.u)), h),
			inward,
		);
		if (cup) {
			// Palm onto the ball: a jumper's shooting hand cocked back behind
			// it, fingers up its back; on his dribble, over the top of it; any
			// other, fingers up its side.
			const onto = unit(subV(cup, addV(wrist, h, H * 0.04)));
			palmN = onto;
			const shooting = which === "R" && gripOf(st.anim) === "shot";
			h = unit(square(shooting || !held ? h : addV(h, upV, 1.4), onto), h);
		} else if (dribbling) {
			// Coming onto the ball or off it, turned onto it as far as it is
			// in his hand - not flipped over the moment it is.
			const onto = unit(subV(underPalm(limb), addV(wrist, h, H * 0.04)));
			const w = Math.min(1, touch / 0.3);
			const k = w * w * (3 - 2 * w);
			palmN = unit(square(lerpV(palmN, onto, k), h), palmN);
		}
		// Up at a shot: palm to it. In a clasp, thumb up, palm to the
		// other man's.
		if (grip === "splay") {
			palmN = unit(square(fwd, h), palmN);
		} else if (grip === "clasp") {
			palmN = unit(square(addV(inward, fwd, 0.3), h), palmN);
		} else if (grip === "hook") {
			palmN = unit(square(addV(fwd, upV, -0.4), h), palmN);
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
		// (See handMasses: on the ball, each finger laid round it.)
		const ball = cup;
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
			nail: look.silhouette ? undefined : NAIL,
			grip,
			b: HAND_BUILD,
			onto: ball
				? (base, d, r) => {
						const c = subV(ball, base);
						const off = dotV(c, unit(cross(d, palmN)));
						const R = HELD_BALL_R + r;
						const rho = R * R > off * off ? Math.sqrt(R * R - off * off) : 0;
						const cu = dotV(c, d);
						const cv = dotV(c, palmN);
						return rho > 0
							? (x, y, phi, l, k, rest) =>
									wrapBend(
										x,
										y,
										phi,
										l,
										cu,
										cv,
										rho,
										BEND_LO[k]!,
										BEND_HI[k]!,
										rest,
									)
							: undefined;
					}
				: undefined,
		});
		knuckleAt[side] = made.knuckles;
		thumbAt[side] = made.thumb;
	};
	arm(sk.armR, 0);
	arm(sk.armL, 1);

	// His legs (see legMasses): the kneecap in front of the knee, and over
	// the thigh his shorts' leg - loose, flaring a little, cut square above
	// the knee.
	const leg = (limb: Limb, side: 0 | 1) => {
		const gl = G_LEG + side;
		legMasses(
			add,
			body,
			limb,
			side,
			{ thigh: THIGH, shin: SHIN, shoe: SHOE, tongue: TONGUE },
			gl,
			G_SHOE + side,
			mine.leg,
			(front) => {
				add(
					addV(limb.mid, front, body.kneeR * 0.56),
					addV(limb.mid, front, body.kneeR * 0.56),
					[[0, body.kneeR * 0.5]],
					SHIN,
					gl,
					side,
				);
			},
			() => {
				add(
					lerpV(limb.root, limb.mid, 0.16),
					lerpV(limb.root, limb.mid, 0.84),
					[
						[0, body.thighR * 1.22],
						[1, body.thighR * 1.4],
					],
					SHORTS,
					G_SHORTS + side,
					side,
					true,
				);
				// The thigh up (a knee driven up, sat down): the loose leg of
				// his shorts hangs down off it, not wrapped round it.
				const thighDir = unit(subV(limb.mid, limb.root));
				const level = 1 - Math.abs(thighDir.u);
				if (level > SAG_FROM) {
					const sag = v3(0, 0, -body.thighR * SAG * (level - SAG_FROM));
					add(
						addV(lerpV(limb.root, limb.mid, 0.4), sag, 0.5),
						addV(lerpV(limb.root, limb.mid, 0.82), sag, 1),
						[
							[0, body.thighR * 1.1],
							[1, body.thighR * 1.26],
						],
						SHORTS,
						G_SHORTS + side,
						side,
						true,
					);
				}
			},
		);
	};
	leg(sk.legR, 0);
	leg(sk.legL, 1);

	// (Letting go of it, his hands are only coming off where it was: it is
	// gone from them, drawn where it is.)
	if (held && st.holding) {
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

	if (look.outfit?.towel) {
		// His towel: spread under his hands as he wipes with it, down low;
		// or hung from his right hand.
		const r = sk.armR.end;
		const l = sk.armL.end;
		const low = Math.min(r.u, l.u) < 1.7;
		if (low) {
			const mid = v3(
				(r.f + l.f) / 2 + 0.1,
				(r.s + l.s) / 2,
				Math.max(0.08, Math.min(r.u, l.u) - 0.12),
			);
			add(
				addV(mid, v3(0, 1, 0), -0.38),
				addV(mid, v3(0, 1, 0), 0.38),
				[[0, 0.2]],
				TOWEL,
				G_CAMERA,
			);
		} else {
			add(r, addV(r, v3(0, 0, 1), -0.75), [[0, 0.13]], TOWEL, G_CAMERA);
		}
	}

	if (look.outfit?.mic) {
		// His microphone, up in his right hand toward his mouth: the handle,
		// and the grille on top of it.
		const hand = sk.armR.end;
		// (Just in front of his mouth.)
		const mouth = v3(
			sk.head.f + body.headR * 1.25,
			sk.head.s,
			sk.head.u - body.headR * 0.45,
		);
		// (Held out to somebody else, far from his own: pointed out and up,
		// at the other man's mouth.)
		const far = lenV(subV(mouth, hand)) > MIC_OWN;
		const toward = far
			? unit(v3(0.8, 0, 0.6), v3(0, 0, 1))
			: unit(subV(mouth, hand), v3(0, 0, 1));
		const top = addV(
			hand,
			toward,
			far ? 0.42 : Math.min(0.5, lenV(subV(mouth, hand)) - 0.14),
		);
		add(
			addV(hand, toward, -0.08),
			top,
			[
				[0, 0.06],
				[1, 0.08],
			],
			CAM_BODY,
			G_CAMERA,
		);
		add(top, addV(top, toward, 0.12), [[0, 0.13]], CAM_LENS, G_CAMERA);
	}

	// (See joinBody: his torso into his shorts at his hips.)
	const { joins, join } = joinsFor(P);
	joinBody(
		join,
		{
			torso: G_TORSO,
			neck: G_NECK,
			arm: G_ARM,
			leg: G_SHORTS,
			hand: G_HAND,
			finger: G_FINGER,
			shoulder: 3,
			hip: 2.6,
			neckReach: 0.9,
			wrists: false,
		},
		sk,
		body,
		up,
		knuckleAt,
		thumbAt,
	);

	// The camera's frame, in his.
	const frame = frameOf(cam, st.yaw);
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
	if (held && st.holding) {
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
		...(noSheet
			? {}
			: { sheet: jerseySheet(look, body, kMid * 1.2, cut.waist) }),
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
export type Palette = {
	jersey: RGB;
	trim: RGB;
	shorts: RGB;
	band: RGB;
	stripe: RGB;
	skin: RGB;
	palm: RGB;
	sock: RGB;
	shoe: RGB;
	sole: RGB;
	// Under the sole, the stripe down the side, the laces.
	tread: RGB;
	accent: RGB;
	laces: RGB;
	legs: [RGB, RGB];
	upper: [RGB, RGB];
	fore: [RGB, RGB];
	wrist: [RGB | undefined, RGB | undefined];
	knee: [RGB | undefined, RGB | undefined];
	shirt?: RGB;
	tie?: RGB;
	stripes?: RGB;
	sides?: RGB;
	lapel?: RGB;
};

// Laces a shade off the shoe: darker on a light one, lighter on a dark one.
const lacesOn = (shoe: RGB): RGB =>
	0.2126 * shoe[0] + 0.7152 * shoe[1] + 0.0722 * shoe[2] > 128
		? scaled(shoe, 0.78)
		: [
				shoe[0] + (255 - shoe[0]) * 0.32,
				shoe[1] + (255 - shoe[1]) * 0.32,
				shoe[2] + (255 - shoe[2]) * 0.32,
			];

const palettes = new WeakMap<Look, Palette>();
export const paletteOf = (look: Look): Palette => {
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
		palm: look.silhouette ? skin : PALM,
		sock: rgb(gear?.sock ?? kit.sock),
		shoe: rgb(gear?.shoe ?? kit.shoe),
		sole: rgb(gear?.sole ?? kit.sole),
		tread: scaled(rgb(gear?.sole ?? kit.sole), 0.55),
		accent: rgb(gear?.accent ?? kit.shoeStripe ?? gear?.sole ?? kit.sole),
		laces: lacesOn(rgb(gear?.shoe ?? kit.shoe)),
		legs: both((w) =>
			gear?.tights?.legs.includes(w) ? rgb(gear.tights.color) : skin,
		),
		upper: both((w) =>
			outfit?.sleeves
				? outfit.sleeveColor
					? rgb(outfit.sleeveColor)
					: jersey
				: gear?.sleeve?.arms.includes(w) && !gear.sleeve.elbow
					? sleeve(w)!
					: skin,
		),
		fore: both((w) =>
			outfit?.sleeves === "long"
				? outfit.sleeveColor
					? rgb(outfit.sleeveColor)
					: jersey
				: (sleeve(w) ?? skin),
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
		...(outfit?.sides ? { sides: rgb(outfit.sides) } : {}),
	};
	palettes.set(look, p);
	return p;
};

// The palms of his hands, whatever his skin.
const PALM: RGB = [232, 186, 166];
// The ball in his hands (see gameBall.ts), at the size it is drawn.
const heldSurfaces = new Map<number, BallSurface>();
export const heldSurface = (radius: number): BallSurface => {
	const key = Math.round(radius * 2) / 2;
	let s = heldSurfaces.get(key);
	if (!s) {
		s = surfaceFor(key);
		heldSurfaces.set(key, s);
	}
	return s;
};
const heldPx = new Float64Array(3);
export const CAM_BODY_RGB = rgb(CAMERA_BODY);
export const CAM_LENS_RGB = rgb(CAMERA_LENS);
export const TOWEL_RGB: RGB = [238, 238, 232];

// The light, from up and to the left and toward the camera (on the sprite:
// right, down, out of the picture).
export const LIGHT = (() => {
	const l = [-0.45, -0.7, 0.55];
	const n = Math.hypot(l[0]!, l[1]!, l[2]!);
	return [l[0]! / n, l[1]! / n, l[2]! / n] as const;
})();

// THE REST OF THE BUILDING'S LIGHT (a sport that asks for it - see shade's
// `rich`): a second bank of lights behind him and over the camera's right,
// catching the edge of him that is turned from the first; and, half way
// between the first and the camera, where his skin shines back at it.
const norm3 = (l: readonly number[]) => {
	const n = Math.hypot(l[0]!, l[1]!, l[2]!);
	return [l[0]! / n, l[1]! / n, l[2]! / n] as const;
};
export const BACK_LIGHT = norm3([0.75, -0.45, -0.2]);
export const SHINE_AT = norm3([LIGHT[0], LIGHT[1], LIGHT[2] + 1]);
// What it does (see shade): how far a surface turned well away from the
// lights darkens (cloth, then skin's red, green and blue); how much the
// light from behind lifts cloth and skin toward white; and the shine on
// skin.
export const RICH = {
	deepFrom: -0.3,
	deepOver: 0.32,
	deep: [0.09, 0.08, 0.1, 0.1],
	backFrom: 0.55,
	backOver: 0.4,
	back: [0.16, 0.22],
	shineFrom: 0.9,
	shineOver: 0.085,
	shine: 0.14,
	// (And the masses of one part of him - his arm's, his leg's - run
	// further into each other: one limb, not its muscles laid on it.)
	melt: 1.1,
	// Where two of his masses meet at an angle - round a muscle, in at a
	// joint - the crease between them is that much darker (cloth, skin), the
	// sharper the angle the more (`bend`: how sharp is all of it).
	groove: [0.1, 0.2],
	bend: 1.5,
} as const;

// HOW WHAT HE WEARS HANGS, AND HIS SKIN'S OWN TONES (a sport's `paint` says,
// in its ink's `tone`: so much lighter or darker there). His jersey falls
// in long folds from his chest and shoulder blades, more of them and deeper
// toward his waist and under his arms, and gathers where it tucks in; his
// shorts hang in folds down each leg, deeper toward the hem. His elbows and
// knees are a shade darker than the rest of him, and his neck in the shadow
// of his jaw.
export const CLOTH = {
	jersey: 0.15,
	// (So many folds round him, and how they wander down him.)
	drape: [6, 2.2, 2.3],
	// (Gathered into his waistband: so many round him, up so much of his
	// torso from it.)
	gather: [14, 9, 0.22],
	shorts: 0.14,
	legs: [5, 3, 2.1],
	seat: [7, 5],
	joint: 0.09,
	// (Over the last of a thigh or an upper arm into the joint, and the
	// first of the shin or forearm out of it.)
	jointSpan: 0.1,
	jaw: [0.14, 0.3, 0.95],
} as const;

// Lengths, for every pixel: Math.hypot's care over huge and tiny numbers
// is slow, and nothing here is either.
export const len2 = (a: number, b: number) => Math.sqrt(a * a + b * b);
export const len3 = (a: number, b: number, c: number) =>
	Math.sqrt(a * a + b * b + c * c);

export const smin = (a: number, b: number, k: number) => {
	const h = Math.max(k - Math.abs(a - b), 0) / k;
	return Math.min(a, b) - (h * h * k) / 4;
};

export type Sculpted = {
	img: ImageData;
	// What goes over his head once it is drawn: an arm raised in front of
	// his face, the ball held up there.
	over?: ImageData;
	head: { x: number; y: number; r: number };
	// The box round everything drawn (sprite pixels, ends included); empty
	// (x0 > x1) if nothing was.
	box: { x0: number; y0: number; x1: number; y1: number };
};

export const RUN = 8;

// What a sport paints at a point of him (see shade): the color, whether it
// is cloth (it hangs smoother over him than skin does, and takes the light
// flatter), and how glossy it is (a helmet's shell: a hard glint where it
// faces the lights).
export type Ink = {
	r: number;
	g: number;
	b: number;
	cloth: boolean;
	gloss: number;
	// So much lighter (or, below 0, darker) there: a fold of his clothes, a
	// darker patch of skin (see CLOTH) - where the sport says.
	tone?: number;
};
// Two of his parts (groups) that melt into each other round where they meet
// (sprite pixels), this far round it.
export type Join = {
	a: number;
	b: number;
	x: number;
	y: number;
	reach: number;
};

// HIM, SHADED - any sport's (see sculpt, and field3d/sculpt.ts): every pixel
// of the w x h sheet `d` finds the mass nearest the camera there, masses
// that meet at a joint melting into one another near it; the point on him
// the pixel shows is worked out (feet, his frame) and `paint` says what he
// wears there, into `ink`; and it is lit, two tones the cartoon way, with a
// fine dark line where he passes in front of himself and round what
// `outlined` says. `owner`, if given, takes each pixel's group. The box
// round everything drawn (empty, x0 > x1, if nothing was).
export const shade = (
	b: { masses: Mass[]; joins: Join[]; frame: Frame; k: number },
	groups: number,
	d: Uint8ClampedArray,
	w: number,
	h: number,
	paint: (
		fc: Mass,
		t: number,
		nx: number,
		ny: number,
		nz: number,
		r: number,
		Pf: number,
		Ps: number,
		Pu: number,
	) => void,
	ink: Ink,
	outlined: (kind: number) => boolean,
	owner?: Int8Array,
	// The rest of the building's light on him too (see RICH).
	rich = false,
): { x0: number; y0: number; x1: number; y1: number } => {
	const masses = b.masses;
	const n = masses.length;
	const lineW = Math.max(0.7, 0.028 * b.k);
	const margin = lineW * 3.5 + 1;
	// How far the masses of one part of him run into each other.
	const melt = rich ? RICH.melt : 0.8;
	// Which joint, if any, melts two groups together.
	const joinOf = new Int16Array(groups * groups).fill(-1);
	b.joins.forEach((j, i) => {
		joinOf[j.a * groups + j.b] = i;
		joinOf[j.b * groups + j.a] = i;
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
	let bx0 = w;
	let by0 = h;
	let bx1 = -1;
	let by1 = -1;

	// Row by row, each mass is asked about only along the stretch of the
	// row it can reach - no further off its axis than its widest and the
	// margin, and inside its box - and a pixel only if some mass can reach
	// it: most of the sprite is open floor round his limbs. (Taken a short
	// run of the row at a time, so each pixel asks just the few there.)
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
				// (A hair more, for rounding.)
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
				let groove = 0;
				for (let j = 0; j < m; j++) {
					if (j === f) {
						continue;
					}
					const c = masses[hi[j]!]!;
					const dz = hz[j]! - hz[f]!;
					let K = 0;
					if (c.group === fc.group) {
						K = Math.min(hr[j]!, hr[f]!) * melt;
					} else {
						const ji = joinOf[fc.group * groups + c.group]!;
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
							// (The crease where the two meet at an angle.)
							const bend =
								1 - (hx[j]! * hx[f]! + hy[j]! * hy[f]! + hn[j]! * hn[f]!);
							const gr = wgt * Math.min(1, bend * RICH.bend);
							if (gr > groove) {
								groove = gr;
							}
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
				ink.cloth = false;
				ink.gloss = 0;
				ink.tone = 0;
				paint(fc, t, hx[f]!, hy[f]!, hn[f]!, hr[f]!, Pf, Ps, Pu);
				const cloth = ink.cloth;
				const gloss = ink.gloss;
				const cr = ink.r;
				const cg = ink.g;
				const cb = ink.b;
				// Cloth hangs smoother over him than skin does.
				if (cloth) {
					sz += 0.35;
					const mm = len3(sx, sy, sz);
					sx /= mm;
					sy /= mm;
					sz /= mm;
				}
				const lam = sx * LIGHT[0] + sy * LIGHT[1] + sz * LIGHT[2];
				// Two tones, the step between them soft.
				const e = Math.min(1, Math.max(0, (lam - 0.34) / 0.16));
				const lit = e * e * (3 - 2 * e);
				const dark = 1 - 0.22 * shadow;
				// Turned well away from the lights, darker still; the edge of
				// him the lights behind catch; his skin shining.
				let deep = 0;
				let lift = 0;
				if (rich) {
					const e2 = Math.min(
						1,
						Math.max(0, (lam - RICH.deepFrom) / RICH.deepOver),
					);
					deep = 1 - e2 * e2 * (3 - 2 * e2);
					const e3 = Math.min(
						1,
						Math.max(
							0,
							(sx * BACK_LIGHT[0] +
								sy * BACK_LIGHT[1] +
								sz * BACK_LIGHT[2] -
								RICH.backFrom) /
								RICH.backOver,
						),
					);
					lift = e3 * e3 * (3 - 2 * e3) * (1 - lit) * RICH.back[cloth ? 0 : 1];
					if (!cloth) {
						const e4 = Math.min(
							1,
							Math.max(
								0,
								(sx * SHINE_AT[0] +
									sy * SHINE_AT[1] +
									sz * SHINE_AT[2] -
									RICH.shineFrom) /
									RICH.shineOver,
							),
						);
						lift += e4 * e4 * (3 - 2 * e4) * RICH.shine;
					}
				}
				let r: number;
				let g: number;
				let bb: number;
				if (cloth || gloss > 0) {
					const k = (0.79 + 0.21 * lit - RICH.deep[0] * deep) * dark;
					r = cr * k;
					g = cg * k;
					bb = cb * k;
				} else {
					// Skin in shadow warms.
					const k = dark;
					r = cr * (0.84 + 0.16 * lit - RICH.deep[1] * deep) * k;
					g = cg * (0.74 + 0.26 * lit - RICH.deep[2] * deep) * k;
					bb = cb * (0.72 + 0.28 * lit - RICH.deep[3] * deep) * k;
				}
				if (rich) {
					// The fold or the patch of skin there, and the crease between
					// two of his masses.
					const tone =
						1 + (ink.tone ?? 0) - groove * RICH.groove[cloth ? 0 : 1];
					r *= tone;
					g *= tone;
					bb *= tone;
				}
				if (lift > 0) {
					r += (255 - r) * lift;
					g += (255 - g) * lift;
					bb += (255 - bb) * lift;
				}
				if (gloss > 0 && lam > 0.86) {
					// A shine (a helmet's): a hard white glint where it faces the
					// lights.
					const s = Math.min(1, (lam - 0.86) / 0.06) * 0.55 * gloss;
					r += (255 - r) * s;
					g += (255 - g) * s;
					bb += (255 - bb) * s;
				}
				// A fine line where he passes in front of himself - and
				// all round his hands, so they read.
				if ((behind || outlined(fc.kind)) && S > -lineW) {
					const e = Math.min(1, (S + lineW) / lineW);
					r += (cr * 0.38 - r) * e;
					g += (cg * 0.38 - g) * e;
					bb += (cb * 0.38 - bb) * e;
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
				if (owner) {
					owner[y * w + x] = fc.group;
				}
			}
		}
	}

	return { x0: bx0, y0: by0, x1: bx1, y1: by1 };
};

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
	// The ball in his hands faces round from him as it came to him (see
	// facesTo in evaluate.ts): turned so from the way HELD has it, square
	// on to the camera.
	const heldTurn = st.yaw + (st.ballFace ?? 0) - facingCamera(cam.right);
	const heldCos = Math.cos(heldTurn);
	const heldSin = Math.sin(heldTurn);
	const img = new ImageData(w, h);
	const d = img.data;
	const lateAny = b.late.some(Boolean);
	const owner = lateAny ? new Int8Array(w * h).fill(-1) : undefined;

	const { rt, dn, tw: tw3 } = b.frame;
	const cut = b.cut;
	const pel = b.pel;
	const spine = b.spine;
	const fw = b.fwd;
	const sheet = b.sheet;
	const wraps = b.wraps;
	const art = look.kitArt;
	// (The trim round his neck and armholes the picture's own, not painted on.)
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
	const ink: Ink = { r: 0, g: 0, b: 0, cloth: false, gloss: 0 };

	// The jersey, shirt or jacket at a point on his torso (U, S, F): sets
	// the ink.
	const torsoAt = (U: number, S: number, F: number) => {
		ink.cloth = true;
		const outfit = look.outfit;
		let c: RGB;
		// A suit's jacket hangs on over his hips; anything else is tucked in.
		const hem = pal.shirt ? -cut.waist : cut.waist;
		// (Round him from the middle of his chest.)
		const round = Math.atan2(S, F);
		if (U < hem) {
			// The seat of his shorts, in folds.
			ink.tone =
				CLOTH.shorts *
				0.6 *
				Math.sin(round * CLOTH.seat[0] + U * CLOTH.seat[1]);
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
			const below = (cut.top - neckDrop - U) / len2(1, slope);
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
				dArm = g / (2 * len2(gx / ea, gy / eb) || 1);
			}
			const edge = Math.min(dArm, dNeck);
			if (edge >= 0) {
				// His top, hanging: long folds, and gathered at his waist.
				const low = Math.min(
					1,
					Math.max(0, (cut.top - U) / (cut.top - cut.waist)),
				);
				const drape =
					Math.sin(
						round * CLOTH.drape[0] +
							CLOTH.drape[1] * Math.sin(U * CLOTH.drape[2] + 0.7),
					) *
					(0.25 + 0.75 * low) *
					(0.45 + 0.55 * Math.abs(Math.sin(round)));
				const tuck = Math.min(
					1,
					Math.max(0, 1 - (U - cut.waist) / (CLOTH.gather[2] * body.torso)),
				);
				ink.tone =
					CLOTH.jersey *
					(0.7 * drape +
						0.5 *
							tuck *
							Math.sin(round * CLOTH.gather[0] + U * CLOTH.gather[1]));
			}
			if (edge < 0) {
				c = pal.skin;
				ink.cloth = false;
			} else if (edge < cut.trim && !outfit && !trimFromArt) {
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
					// (A panel down each side, under the arm.)
					if (pal.sides && Math.abs(Math.abs(th) - Math.PI / 2) < 0.32) {
						c = pal.sides;
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
					!outfit.plain &&
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
							ink.r = c[0] + (r / a - c[0]) * a;
							ink.g = c[1] + (gg / a - c[1]) * a;
							ink.b = c[2] + (bb / a - c[2]) * a;
							return;
						}
					}
				}
			}
		}
		ink.r = c[0];
		ink.g = c[1];
		ink.b = c[2];
	};

	// What he wears at a point of him (see shade): the mass `fc`, t along it;
	// the surface's way there (nx, ny toward the camera's right and down, nz
	// toward it) and the mass's radius there (sprite pixels); the point (feet,
	// his frame).
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
			case SHORTS: {
				ink.cloth = true;
				// The stripe down the outside of the leg: where the
				// surface faces out from his side.
				const lf = fc.B.f - fc.A.f;
				const ls = fc.B.s - fc.A.s;
				const lu = fc.B.u - fc.A.u;
				const ll = len3(lf, ls, lu) || 1;
				let of = Pf - fc.A.f - lf * t;
				let os = Ps - fc.A.s - ls * t;
				let ou = Pu - fc.A.u - lu * t;
				const along = (of * lf + os * ls + ou * lu) / (ll * ll);
				of -= lf * along;
				os -= ls * along;
				ou -= lu * along;
				const ol = len3(of, os, ou) || 1;
				// (Hanging in folds down his leg, deeper toward the hem.)
				ink.tone =
					CLOTH.shorts *
					Math.sin(
						Math.atan2(os, of) * CLOTH.legs[0] +
							t * CLOTH.legs[1] +
							side * CLOTH.legs[2],
					) *
					(0.35 + 0.65 * t);
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
				ink.r = c[0];
				ink.g = c[1];
				ink.b = c[2];
				break;
			}
			case UPPER:
			case FORE: {
				let c = fc.kind === UPPER ? pal.upper[side]! : pal.fore[side]!;
				const band = pal.wrist[side];
				if (fc.kind === FORE && band && t > 0.76 && t < 0.97 && fc.len > 0) {
					c = band;
				}
				if (look.outfit?.sleeves === "short" && fc.kind === UPPER && t > 0.55) {
					c = pal.skin;
				}
				ink.cloth = c !== pal.skin;
				if (!ink.cloth) {
					// (His elbow, a shade darker.)
					const into = fc.kind === UPPER ? 1 - t : t;
					if (into < CLOTH.jointSpan) {
						ink.tone = -CLOTH.joint * (1 - into / CLOTH.jointSpan);
					}
				}
				ink.r = c[0];
				ink.g = c[1];
				ink.b = c[2];
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
					((fc.kind === THIGH && t > 0.82) || (fc.kind === SHIN && t < 0.2))
				) {
					c = pad;
				}
				ink.cloth = c !== pal.skin;
				if (!ink.cloth) {
					// (His knee, a shade darker.)
					const into = fc.kind === THIGH ? 1 - t : t;
					if (into < CLOTH.jointSpan) {
						ink.tone = -CLOTH.joint * (1 - into / CLOTH.jointSpan);
					}
				}
				ink.r = c[0];
				ink.g = c[1];
				ink.b = c[2];
				break;
			}
			case SHOE: {
				let c = Pu < cut.sole ? pal.sole : pal.shoe;
				if (Pu < cut.sole * 0.35) {
					c = pal.tread;
				} else if (c === pal.shoe) {
					// Round the shoe from its axis: up its side, or
					// over the top of it - on the shoe itself, heel to
					// toe, not the collar round his ankle.
					const lf = fc.B.f - fc.A.f;
					const ls = fc.B.s - fc.A.s;
					const lu = fc.B.u - fc.A.u;
					const ll = len3(lf, ls, lu) || 1;
					if (ll > body.ankleR * 4) {
						const of = Pf - fc.A.f - lf * t;
						const os = Ps - fc.A.s - ls * t;
						const ou = Pu - fc.A.u - lu * t;
						const ol = len3(of, os, ou) || 1;
						const up = ou / ol;
						const out = Math.abs(of * ls - os * lf) / (ll * ol);
						// The stripe sweeps up from the toe to the heel,
						// widest in the middle.
						const mid = 0.42 - (0.5 * (t - 0.15)) / 0.7;
						const wide = 0.28 * (1 - Math.abs(t - 0.5) * 1.6);
						if (
							out > 0.5 &&
							t > 0.15 &&
							t < 0.85 &&
							Math.abs(up - mid) < wide
						) {
							c = pal.accent;
						} else if (up > 0.78 && t > 0.38 && t < 0.72) {
							c = pal.laces;
						}
					}
				}
				ink.cloth = true;
				ink.r = c[0];
				ink.g = c[1];
				ink.b = c[2];
				break;
			}
			case TONGUE: {
				// The instep of his shoe: laced up the top of it.
				const lf = fc.B.f - fc.A.f;
				const ls = fc.B.s - fc.A.s;
				const lu = fc.B.u - fc.A.u;
				const of = Pf - fc.A.f - lf * t;
				const os = Ps - fc.A.s - ls * t;
				const ou = Pu - fc.A.u - lu * t;
				const ol = len3(of, os, ou) || 1;
				const c =
					ou / ol > TONGUE_UP && t > 0.12 && t < 0.92 ? pal.laces : pal.shoe;
				ink.cloth = true;
				ink.r = c[0];
				ink.g = c[1];
				ink.b = c[2];
				break;
			}
			case BALL: {
				// The game ball, as it sits in his hands - its
				// channels and its print (see gameBall.ts).
				// (Turned round with him, the way it faces.)
				const nz = hnf;
				const nx = hxf * heldCos - nz * heldSin;
				const ny = -hyf;
				const nt = nz * heldCos + hxf * heldSin;
				const rad = hrf;
				ballSurface(
					heldSurface(rad),
					nx * HELD[0][0] + ny * HELD[0][1] + nt * HELD[0][2],
					nx * HELD[1][0] + ny * HELD[1][1] + nt * HELD[1][2],
					nx * HELD[2][0] + ny * HELD[2][1] + nt * HELD[2][2],
					1 / (rad * Math.max(0.3, Math.sqrt(nz))),
					heldPx,
				);
				ink.r = heldPx[0]!;
				ink.g = heldPx[1]!;
				ink.b = heldPx[2]!;
				break;
			}
			case CAM_BODY:
			case CAM_LENS:
			case TOWEL: {
				const c =
					fc.kind === CAM_BODY
						? CAM_BODY_RGB
						: fc.kind === TOWEL
							? TOWEL_RGB
							: CAM_LENS_RGB;
				ink.r = c[0];
				ink.g = c[1];
				ink.b = c[2];
				break;
			}
			case NAIL: {
				// Paler than his skin, a little pink.
				const c = pal.skin;
				ink.r = c[0] + (pal.palm[0] - c[0]) * 0.62 + 6;
				ink.g = c[1] + (pal.palm[1] - c[1]) * 0.62;
				ink.b = c[2] + (pal.palm[2] - c[2]) * 0.62;
				break;
			}
			case HAND: {
				// His palm, and the inside of his fingers, paler
				// than the back of his hand.
				const p = b.pn[side]!;
				const nbf = hxf * rt.f + hyf * dn.f + hnf * tw3.f;
				const nbs = hxf * rt.s + hyf * dn.s + hnf * tw3.s;
				const nbu = hxf * rt.u + hyf * dn.u + hnf * tw3.u;
				const inPalm = nbf * p.f + nbs * p.s + nbu * p.u;
				const c = pal.skin;
				const e =
					inPalm > 0.15 ? Math.min(1, (inPalm - 0.15) / 0.25) * 0.42 : 0;
				ink.r = c[0] + (pal.palm[0] - c[0]) * e;
				ink.g = c[1] + (pal.palm[1] - c[1]) * e;
				ink.b = c[2] + (pal.palm[2] - c[2]) * e;
				break;
			}
			default: {
				if (fc.kind === NECK) {
					// (His neck, in the shadow of his jaw.)
					const e = Math.min(
						1,
						Math.max(0, (t - CLOTH.jaw[1]) / (CLOTH.jaw[2] - CLOTH.jaw[1])),
					);
					ink.tone = -CLOTH.jaw[0] * e * e * (3 - 2 * e);
				}
				ink.r = pal.skin[0];
				ink.g = pal.skin[1];
				ink.b = pal.skin[2];
			}
		}
	};

	const box = shade(
		b,
		GROUPS,
		d,
		w,
		h,
		paint,
		ink,
		(kind) => kind === HAND || kind === NAIL,
		owner,
		true,
	);

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
	return {
		img,
		...(over ? { over } : {}),
		head: b.head,
		box,
	};
};
