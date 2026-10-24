import type { Kit } from "./figure.ts";

// A TEAM'S OWN UNIFORM, FROM A PICTURE: its jersey and shorts laid out flat,
// the way a Minecraft skin is, and wrapped round every player who wears it.
//
// The picture is ART_W x ART_H (or two, three or four times that). The
// jersey is a band across the top and the shorts a band under it; each band
// is four panels side by side - his right side, his front, his left side,
// his back (seen from behind) - from the tops of his shoulders (the shorts:
// the top of the waistband) down to the hem. Every player wears the same
// picture, stretched to his build. His number and name are put on over it,
// in the colors of the swatches along the bottom - the team's own colors
// where a swatch is left clear - and so is the trim round the neck and
// armholes. Clear parts of the picture are the team's plain color.
//
// The template (kitTemplate.ts) draws its guides in ART_GUIDE magenta. Any
// of that left on a picture is painted over with the colors round it when
// the picture is read - so a guide line vanishes into whatever was painted
// either side of it - or left clear where nothing was.
//
// A picture ART_H_LETTERED tall (or two, three or four times that) has its
// own lettering too, under the swatches: a strip of settings the game keeps
// in it (where his number and name go, how big, the name's arch, whether
// the trim is the picture's own - see KitFit), then the team's number and
// name type, a character a cell, painted in its own colors and outlines.
// What it has a character for is set in it; a name with any character it
// hasn't, and a number or name on a picture with none, in the game's type.

export const ART_W = 256;
export const ART_H = 304;

// The panels across a band: his right side, his front, his left side, his
// back.
const SIDE_W = 32;
const FRONT_W = 96;
export const ART_PANELS = {
	right: { x: 0, w: SIDE_W },
	front: { x: SIDE_W, w: FRONT_W },
	left: { x: SIDE_W + FRONT_W, w: SIDE_W },
	back: { x: SIDE_W * 2 + FRONT_W, w: FRONT_W },
} as const;

export type ArtBand = { y: number; h: number };
export const ART_JERSEY: ArtBand = { y: 0, h: 128 };
export const ART_SHORTS: ArtBand = { y: 136, h: 136 };

// The swatches along the bottom, left to right: his number, the edge round
// it, his name, the trim.
export const ART_SWATCHES = {
	y: 280,
	size: 24,
	number: 8,
	numberEdge: 72,
	name: 136,
	trim: 200,
} as const;

// The lettering, under the swatches of a picture that has it: the settings
// strip, and the rows of cells - the number's digits, the name's letters.
export const ART_H_LETTERED = 424;
export const ART_SETTINGS = { x: 8, y: 308, size: 8 } as const;
export type GlyphRow = {
	x: number;
	y: number;
	w: number;
	h: number;
	chars: string;
};
export const ART_DIGITS: GlyphRow = {
	x: 8,
	y: 328,
	w: 24,
	h: 32,
	chars: "0123456789",
};
export const ART_LETTERS: readonly GlyphRow[] = [
	{ x: 8, y: 372, w: 16, h: 20, chars: "ABCDEFGHIJKLMNO" },
	{ x: 8, y: 396, w: 16, h: 20, chars: "PQRSTUVWXYZ-'." },
];

// Where his number and name go and how big, against the usual (see
// LETTERING in sculpt.ts): sizes times the usual, heights in torso lengths
// up from the usual, the name's arch from -1 (sagging) to 1 (an arch); and
// whether the trim round his neck and armholes is the game's (the trim
// swatch's color) or the picture's own.
export type KitFit = {
	numberSize: number;
	numberUp: number;
	backSize: number;
	backUp: number;
	nameSize: number;
	nameUp: number;
	nameArch: number;
	trim: "game" | "picture";
};
export const DEFAULT_FIT: KitFit = {
	numberSize: 1,
	numberUp: 0,
	backSize: 1,
	backUp: 0,
	nameSize: 1,
	nameUp: 0,
	nameArch: 0,
	trim: "game",
};

export const ART_GUIDE = "#ff00ff";
const isGuide = (r: number, g: number, b: number) =>
	r >= 232 && g <= 40 && b >= 232;

export type KitArt = {
	// Tells pictures apart.
	id: string;
	// Its pixels, RGBA, `scale` times ART_W x ART_H.
	data: Uint8ClampedArray;
	scale: number;
	// The swatches' colors, where they're colored in.
	number?: string;
	numberEdge?: string;
	name?: string;
	trim?: string;
	// Its lettering: each character's box on the picture (in `data`'s own
	// pixels), and where his number and name go.
	glyphs?: Record<string, readonly [number, number, number, number]>;
	fit?: KitFit;
};

// How a band wraps round him: the garment's half-width (a) and half-depth
// (b), where up him (U, feet) its band of the picture starts and stops, and
// the angle round him where his front and back panels give way to his sides
// - set so a foot is as many pixels on his sides as on his front.
export type ArtWrap = {
	a: number;
	b: number;
	top: number;
	bottom: number;
	band: ArtBand;
	turn: number;
	frontS: number;
	sideF: number;
};

export const artWrap = (
	a: number,
	b: number,
	top: number,
	bottom: number,
	band: ArtBand,
): ArtWrap => {
	const turn = Math.atan(((FRONT_W / SIDE_W) * b) / a);
	return {
		a,
		b,
		top,
		bottom,
		band,
		turn,
		frontS: a * Math.sin(turn),
		sideF: b * Math.cos(turn),
	};
};

const clampTo = (x: number, lo: number, hi: number) =>
	x < lo ? lo : x > hi ? hi : x;

// Where on the picture (in ART_W x ART_H pixels) a point on the garment is:
// U up him, S to his left and F forward of its middle, in feet. Its front
// and back are laid flat - how far across him, not how far round - so what
// is drawn there stands up straight however his chest curves. `notSide`
// keeps a point on the front or back (the inside of a leg of his shorts,
// which faces the other leg, not out to his side). Sets out[0], out[1].
export const artAt = (
	w: ArtWrap,
	U: number,
	S: number,
	F: number,
	notSide: boolean,
	out: Float64Array,
) => {
	const down = clampTo((w.top - U) / (w.top - w.bottom), 0, 1);
	out[1] = w.band.y + down * (w.band.h - 1);
	const phi = Math.abs(Math.atan2(S / w.a, F / w.b));
	let p: { x: number; w: number };
	let across: number;
	if (notSide || phi <= w.turn || phi >= Math.PI - w.turn) {
		const front = notSide ? F >= 0 : phi <= w.turn;
		p = front ? ART_PANELS.front : ART_PANELS.back;
		across = (front ? S : -S) / w.frontS;
	} else {
		// His right side seen from his right: his back to the left, his front
		// to the right. His left side the other way round.
		p = S < 0 ? ART_PANELS.right : ART_PANELS.left;
		across = (S < 0 ? F : -F) / w.sideF;
	}
	out[0] = clampTo(p.x + (across * 0.5 + 0.5) * p.w, p.x, p.x + p.w - 1);
};

// The picture's color at (x, y) - in ART_W x ART_H pixels, blended between
// them - into out (0-255). Returns how much of it there is: 0 where the
// picture is clear.
export const artColor = (
	art: KitArt,
	x: number,
	y: number,
	out: Float64Array,
): number => {
	const s = art.scale;
	const W = ART_W * s;
	const px = clampTo(x * s + (s - 1) / 2, 0, W - 1);
	const py = clampTo(y * s + (s - 1) / 2, 0, ART_H * s - 1);
	const x0 = Math.floor(px);
	const y0 = Math.floor(py);
	const x1 = Math.min(W - 1, x0 + 1);
	const y1 = Math.min(ART_H * s - 1, y0 + 1);
	const fx = px - x0;
	const fy = py - y0;
	const d = art.data;
	const o00 = (y0 * W + x0) * 4;
	const o10 = (y0 * W + x1) * 4;
	const o01 = (y1 * W + x0) * 4;
	const o11 = (y1 * W + x1) * 4;
	const w00 = ((1 - fx) * (1 - fy) * d[o00 + 3]!) / 255;
	const w10 = (fx * (1 - fy) * d[o10 + 3]!) / 255;
	const w01 = ((1 - fx) * fy * d[o01 + 3]!) / 255;
	const w11 = (fx * fy * d[o11 + 3]!) / 255;
	const a = w00 + w10 + w01 + w11;
	if (a < 0.004) {
		return 0;
	}
	for (let c = 0; c < 3; c++) {
		out[c] =
			(d[o00 + c]! * w00 +
				d[o10 + c]! * w10 +
				d[o01 + c]! * w01 +
				d[o11 + c]! * w11) /
			a;
	}
	return a;
};

const hex = (r: number, g: number, b: number) =>
	`#${[r, g, b]
		.map((v) => Math.round(v).toString(16).padStart(2, "0"))
		.join("")}`;

// Whether a picture is the template's shape: ART_W x ART_H (or, lettered,
// ART_H_LETTERED), or two, three or four times it.
export const artScaleOf = (width: number, height: number) => {
	const scale = width / ART_W;
	return Number.isInteger(scale) &&
		scale >= 1 &&
		scale <= 4 &&
		(height === ART_H * scale || height === ART_H_LETTERED * scale)
		? scale
		: undefined;
};

// THE SETTINGS STRIP: each setting a square of one color - its value in the
// red, the rest of 255 in the green (a check), a mark in the blue - after a
// first square that says the strip is there.
const FIT_MARK = 0x5a;
const FIT_MAGIC = 0xa5;
const FIT_KEYS = [
	"numberSize",
	"numberUp",
	"backSize",
	"backUp",
	"nameSize",
	"nameUp",
	"nameArch",
] as const;
const sizeByte = (v: number) => Math.round((v - 0.5) * 170);
const upByte = (v: number) => Math.round(128 + v * 512);
const archByte = (v: number) => Math.round(128 + v * 127);
const toByte = (key: (typeof FIT_KEYS)[number], v: number) =>
	clampTo(
		key.endsWith("Size")
			? sizeByte(v)
			: key === "nameArch"
				? archByte(v)
				: upByte(v),
		0,
		255,
	);
const fromByte = (key: (typeof FIT_KEYS)[number], b: number) =>
	key.endsWith("Size")
		? 0.5 + b / 170
		: key === "nameArch"
			? clampTo((b - 128) / 127, -1, 1)
			: (b - 128) / 512;

// The squares' bytes, in order.
const fitBytes = (fit: KitFit) => [
	FIT_MAGIC,
	...FIT_KEYS.map((key) => toByte(key, fit[key])),
	fit.trim === "picture" ? 1 : 0,
];

// A KitFit written into a lettered picture's pixels (RGBA, ART_W * scale
// wide).
export const writeFit = (
	data: Uint8ClampedArray,
	width: number,
	scale: number,
	fit: KitFit,
) => {
	const S = ART_SETTINGS.size * scale;
	fitBytes(fit).forEach((b, i) => {
		const x0 = (ART_SETTINGS.x + i * ART_SETTINGS.size) * scale;
		const y0 = ART_SETTINGS.y * scale;
		for (let y = y0; y < y0 + S; y++) {
			for (let x = x0; x < x0 + S; x++) {
				const o = (y * width + x) * 4;
				data[o] = b;
				data[o + 1] = 255 - b;
				data[o + 2] = FIT_MARK;
				data[o + 3] = 255;
			}
		}
	});
};

// The KitFit in a lettered picture's pixels, if it has one.
export const readFit = (
	data: Uint8ClampedArray,
	width: number,
	scale: number,
): KitFit | undefined => {
	const S = ART_SETTINGS.size * scale;
	const byteAt = (i: number) => {
		const x0 = (ART_SETTINGS.x + i * ART_SETTINGS.size) * scale + S / 4;
		const y0 = ART_SETTINGS.y * scale + S / 4;
		let r = 0;
		let g = 0;
		let b = 0;
		let n = 0;
		for (let y = Math.floor(y0); y < y0 + S / 2; y++) {
			for (let x = Math.floor(x0); x < x0 + S / 2; x++) {
				const o = (y * width + x) * 4;
				if (data[o + 3]! < 200) {
					return undefined;
				}
				r += data[o]!;
				g += data[o + 1]!;
				b += data[o + 2]!;
				n++;
			}
		}
		r /= n;
		g /= n;
		b /= n;
		return Math.abs(r + g - 255) <= 8 && Math.abs(b - FIT_MARK) <= 12
			? Math.round((r + 255 - g) / 2)
			: undefined;
	};
	if (byteAt(0) !== FIT_MAGIC) {
		return undefined;
	}
	const fit: KitFit = { ...DEFAULT_FIT };
	for (const [i, key] of FIT_KEYS.entries()) {
		const b = byteAt(i + 1);
		if (b === undefined) {
			return undefined;
		}
		fit[key] = fromByte(key, b);
	}
	const flags = byteAt(FIT_KEYS.length + 1);
	if (flags === undefined) {
		return undefined;
	}
	fit.trim = flags & 1 ? "picture" : "game";
	return fit;
};

// The characters a lettered picture has, each one's box (in its own pixels):
// as wide as what is painted in its cell, as tall as all the characters of
// its kind (so they stand on one line). A cell's background, if it was
// painted in (its corners all one color), is keyed out of it.
const glyphsOf = (
	data: Uint8ClampedArray,
	width: number,
	scale: number,
): Record<string, readonly [number, number, number, number]> | undefined => {
	const out: Record<string, readonly [number, number, number, number]> = {};
	const dist = (o: number, c: readonly number[]) =>
		Math.abs(data[o]! - c[0]!) +
		Math.abs(data[o + 1]! - c[1]!) +
		Math.abs(data[o + 2]! - c[2]!);
	for (const rows of [[ART_DIGITS], ART_LETTERS]) {
		const found: { ch: string; x0: number; x1: number; top: number }[] = [];
		let relTop = Infinity;
		let relBottom = -Infinity;
		for (const row of rows) {
			for (const [i, ch] of [...row.chars].entries()) {
				const cx0 = (row.x + i * row.w) * scale + scale;
				const cy0 = row.y * scale + scale;
				const cx1 = (row.x + (i + 1) * row.w) * scale - scale;
				const cy1 = (row.y + row.h) * scale - scale;
				const corner = (x: number, y: number) => (y * width + x) * 4;
				const corners = [
					corner(cx0, cy0),
					corner(cx1 - 1, cy0),
					corner(cx0, cy1 - 1),
					corner(cx1 - 1, cy1 - 1),
				];
				let bg: number[] | undefined;
				if (
					corners.every((o) => data[o + 3]! > 200) &&
					corners.every((o) => !isGuide(data[o]!, data[o + 1]!, data[o + 2]!))
				) {
					const c0 = [
						data[corners[0]!]!,
						data[corners[0]! + 1]!,
						data[corners[0]! + 2]!,
					];
					if (corners.every((o) => dist(o, c0) < 40)) {
						bg = c0;
					}
				}
				let x0 = Infinity;
				let x1 = -Infinity;
				let y0 = Infinity;
				let y1 = -Infinity;
				for (let y = cy0; y < cy1; y++) {
					for (let x = cx0; x < cx1; x++) {
						const o = (y * width + x) * 4;
						if (bg && dist(o, bg) < 40) {
							data[o + 3] = 0;
							continue;
						}
						if (
							data[o + 3]! > 127 &&
							!isGuide(data[o]!, data[o + 1]!, data[o + 2]!)
						) {
							x0 = Math.min(x0, x);
							x1 = Math.max(x1, x);
							y0 = Math.min(y0, y);
							y1 = Math.max(y1, y);
						}
					}
				}
				if (x1 >= x0) {
					found.push({ ch, x0, x1, top: row.y * scale });
					relTop = Math.min(relTop, y0 - row.y * scale);
					relBottom = Math.max(relBottom, y1 - row.y * scale);
				}
			}
		}
		for (const f of found) {
			out[f.ch] = [
				f.x0,
				f.top + relTop,
				f.x1 - f.x0 + 1,
				relBottom - relTop + 1,
			];
		}
	}
	return Object.keys(out).length > 0 ? out : undefined;
};

// The template's guides, where they're left on a picture (RGBA, width x
// height, `scale` times the template): the paint round them over them, or
// clear where there's none.
export const clearGuides = (
	data: Uint8ClampedArray,
	width: number,
	height: number,
	scale: number,
) => {
	const guide = new Uint8Array(width * height);
	for (let i = 0; i < guide.length; i++) {
		const o = i * 4;
		if (data[o + 3]! > 0 && isGuide(data[o]!, data[o + 1]!, data[o + 2]!)) {
			guide[i] = 1;
		}
	}
	const R = 2 * scale;
	for (let y = 0; y < height; y++) {
		for (let x = 0; x < width; x++) {
			const i = y * width + x;
			if (!guide[i]) {
				continue;
			}
			if (y >= ART_H * scale) {
				// (In the lettering, clear - never the color of a character.)
				data[i * 4 + 3] = 0;
				continue;
			}
			let r = 0;
			let g = 0;
			let b = 0;
			let n = 0;
			for (
				let yy = Math.max(0, y - R);
				yy <= Math.min(height - 1, y + R);
				yy++
			) {
				for (
					let xx = Math.max(0, x - R);
					xx <= Math.min(width - 1, x + R);
					xx++
				) {
					const j = yy * width + xx;
					const o = j * 4;
					if (!guide[j] && data[o + 3]! > 127) {
						r += data[o]!;
						g += data[o + 1]!;
						b += data[o + 2]!;
						n++;
					}
				}
			}
			const o = i * 4;
			if (n > 0) {
				data[o] = r / n;
				data[o + 1] = g / n;
				data[o + 2] = b / n;
				data[o + 3] = 255;
			} else {
				data[o + 3] = 0;
			}
		}
	}
};

// A picture's pixels made into a KitArt - undefined if it isn't the
// template's shape.
export const kitArtFromPixels = (
	data: Uint8ClampedArray,
	width: number,
	height: number,
	id: string,
): KitArt | undefined => {
	const scale = artScaleOf(width, height);
	if (scale === undefined) {
		return undefined;
	}
	// A swatch's color: the middle of it, if that's colored in.
	const swatch = (x: number): string | undefined => {
		const S = ART_SWATCHES.size;
		let r = 0;
		let g = 0;
		let b = 0;
		let n = 0;
		for (let y = S * 0.25 * scale; y < S * 0.75 * scale; y++) {
			for (let xx = S * 0.25 * scale; xx < S * 0.75 * scale; xx++) {
				const o = ((ART_SWATCHES.y * scale + y) * width + x * scale + xx) * 4;
				if (data[o + 3]! > 127) {
					r += data[o]!;
					g += data[o + 1]!;
					b += data[o + 2]!;
					n++;
				}
			}
		}
		return n > (S * S * scale * scale) / 8
			? hex(r / n, g / n, b / n)
			: undefined;
	};
	const number = swatch(ART_SWATCHES.number);
	const numberEdge = swatch(ART_SWATCHES.numberEdge);
	const name = swatch(ART_SWATCHES.name);
	const trim = swatch(ART_SWATCHES.trim);
	const lettered = height === ART_H_LETTERED * scale;
	const fit = lettered ? readFit(data, width, scale) : undefined;
	const glyphs = lettered ? glyphsOf(data, width, scale) : undefined;
	clearGuides(data, width, height, scale);
	return {
		id,
		data,
		scale,
		...(number ? { number } : {}),
		...(numberEdge ? { numberEdge } : {}),
		...(name ? { name } : {}),
		...(trim ? { trim } : {}),
		...(glyphs ? { glyphs } : {}),
		...(fit ? { fit } : {}),
	};
};

// A uniform's picture, loaded: one hosted elsewhere asked for as a picture
// we may read back (its host must allow it - CORS), since every pixel of it
// is read to dress the players.
export const loadKitPicture = async (
	url: string,
): Promise<HTMLImageElement> => {
	const img = new Image();
	if (/^https?:\/\//.test(url)) {
		img.crossOrigin = "anonymous";
	}
	img.src = url;
	await img.decode();
	return img;
};

// A loaded picture made into a KitArt (see kitArtFromPixels).
export const kitArtOf = (
	img: HTMLImageElement | HTMLCanvasElement,
	id: string,
): KitArt | undefined => {
	if (
		artScaleOf(img.width, img.height) === undefined ||
		typeof document === "undefined"
	) {
		return undefined;
	}
	const cv = document.createElement("canvas");
	cv.width = img.width;
	cv.height = img.height;
	const g = cv.getContext("2d", { willReadFrequently: true });
	if (!g) {
		return undefined;
	}
	g.drawImage(img, 0, 0);
	let pixels: Uint8ClampedArray;
	try {
		pixels = g.getImageData(0, 0, cv.width, cv.height).data;
	} catch {
		// (A picture from a host that won't let it be read back.)
		return undefined;
	}
	return kitArtFromPixels(pixels, cv.width, cv.height, id);
};

// A team's kit with its picture's lettering and trim.
export const dressKit = (kit: Kit, art: KitArt | undefined): Kit =>
	art
		? {
				...kit,
				trim: art.trim ?? kit.trim,
				number: art.number ?? kit.number,
				numberEdge: art.numberEdge ?? kit.numberEdge,
				...(art.name ? { name: art.name } : {}),
			}
		: kit;
