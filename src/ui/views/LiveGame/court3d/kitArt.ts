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

// Whether a picture is the template's shape: ART_W x ART_H, or two, three or
// four times it.
export const artScaleOf = (width: number, height: number) => {
	const scale = width / ART_W;
	return Number.isInteger(scale) &&
		scale >= 1 &&
		scale <= 4 &&
		height === ART_H * scale
		? scale
		: undefined;
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
	clearGuides(data, width, height, scale);
	return {
		id,
		data,
		scale,
		...(number ? { number } : {}),
		...(numberEdge ? { numberEdge } : {}),
		...(name ? { name } : {}),
		...(trim ? { trim } : {}),
	};
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
	const pixels = g.getImageData(0, 0, cv.width, cv.height).data;
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
