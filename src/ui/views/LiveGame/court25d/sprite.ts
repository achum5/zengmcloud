import { project, type Camera } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import {
	BALL_SEAM,
	BALL_SHADE,
	BALL_ORANGE,
	CAMERA_BODY,
	CAMERA_LENS,
	drawFigure,
	drawHeadAt,
	shade,
	type FigureAnchors,
	type Look,
} from "./figure.ts";
import { ANIMS, type Body } from "./poses.ts";

// THE PLAYERS AS PIXEL ART.
//
// Each player is drawn small - a sprite pixel is several screen pixels - on a
// canvas of his own, then made into pixel art: every edge snapped hard (no
// soft, half-there pixels), every color pulled to his own little palette
// (his skin, his kit, his shoes, each in its light and its shadow), and a
// one-pixel outline round the outside. His face is his BBGM face, shrunk to
// the sprite's scale. Then he is blown back up without smoothing, so the
// pixels stay square.

type RGB = [number, number, number];

const OUTLINE: RGB = [22, 15, 13];

// 3 x 5 digits, for the numbers on the jerseys.
const DIGITS: Record<string, string[]> = {
	"0": ["111", "101", "101", "101", "111"],
	"1": ["010", "110", "010", "010", "111"],
	"2": ["111", "001", "111", "100", "111"],
	"3": ["111", "001", "011", "001", "111"],
	"4": ["101", "101", "111", "001", "001"],
	"5": ["111", "100", "111", "001", "111"],
	"6": ["111", "100", "111", "101", "111"],
	"7": ["111", "001", "010", "010", "010"],
	"8": ["111", "101", "111", "101", "111"],
	"9": ["111", "101", "111", "001", "111"],
};

// 3 x 5 capitals, for a name across his back (or the team's across his
// chest) when he is close enough to have room for one.
const LETTERS: Record<string, string> = {
	A: "010 101 111 101 101",
	B: "110 101 110 101 110",
	C: "011 100 100 100 011",
	D: "110 101 101 101 110",
	E: "111 100 110 100 111",
	F: "111 100 110 100 100",
	G: "011 100 101 101 011",
	H: "101 101 111 101 101",
	I: "111 010 010 010 111",
	J: "001 001 001 101 010",
	K: "101 101 110 101 101",
	L: "100 100 100 100 111",
	M: "101 111 111 101 101",
	N: "101 111 111 111 101",
	O: "010 101 101 101 010",
	P: "110 101 110 100 100",
	Q: "010 101 101 111 011",
	R: "110 101 110 101 101",
	S: "011 100 010 001 110",
	T: "111 010 010 010 010",
	U: "101 101 101 101 111",
	V: "101 101 101 101 010",
	W: "101 101 111 111 101",
	X: "101 101 010 101 101",
	Y: "101 101 010 010 010",
	Z: "111 001 010 100 111",
	"-": "000 000 111 000 000",
	".": "000 000 000 000 010",
	"'": "010 010 000 000 000",
	" ": "000 000 000 000 000",
};
const GLYPH5 = new Map<string, string[]>([
	...Object.entries(DIGITS),
	...Object.entries(LETTERS).map(([ch, rows]): [string, string[]] => [
		ch,
		rows.split(" "),
	]),
]);

const parseColor = (c: string): RGB => {
	const m = /^#?([\da-f]{6})$/i.exec(c.trim());
	if (m) {
		const n = Number.parseInt(m[1]!, 16);
		return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
	}
	const r = /rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)/i.exec(c);
	if (r) {
		return [Number(r[1]), Number(r[2]), Number(r[3])];
	}
	return [128, 128, 128];
};

// Everything his body is drawn in: each color, dimmed for the far side of
// him, and in shadow.
type Palette = { colors: RGB[]; memo: Map<number, number> };
const palettes = new WeakMap<Look, Palette>();
const ballPalettes = new WeakMap<Look, Palette>();
const paletteOf = (look: Look, ball = false): Palette => {
	let pal = (ball ? ballPalettes : palettes).get(look);
	if (!pal) {
		const k = look.kit;
		const seen = new Set<string>();
		const colors: RGB[] = [];
		const add = (c: string) => {
			for (const v of [
				c,
				shade(c, -0.14),
				shade(c, -0.12),
				shade(c, -0.18),
				shade(c, -0.2),
				shade(shade(c, -0.14), -0.18),
				shade(shade(c, -0.14), -0.2),
			]) {
				const rgb = parseColor(v);
				const key = rgb.join(",");
				if (!seen.has(key)) {
					seen.add(key);
					colors.push(rgb);
				}
			}
		};
		for (const c of [
			look.skin,
			k.jersey,
			k.trim,
			k.shorts,
			k.stripe,
			k.sock,
			k.shoe,
			k.sole,
			...(ball ? [BALL_ORANGE, BALL_SHADE, BALL_SEAM] : []),
			...(look.gear
				? [
						look.gear.shoe,
						look.gear.sole,
						look.gear.sock,
						look.gear.sleeve?.color,
						look.gear.tights?.color,
						look.gear.wrist?.color,
						look.gear.knee?.color,
					].filter((c): c is string => c !== undefined)
				: []),
			...(look.outfit
				? [
						look.outfit.stripes,
						look.outfit.shirt,
						look.outfit.tie,
						...(look.outfit.camera ? [CAMERA_BODY, CAMERA_LENS] : []),
					].filter((c): c is string => c !== undefined)
				: []),
		]) {
			add(c);
		}
		colors.push(OUTLINE);
		pal = { colors, memo: new Map() };
		(ball ? ballPalettes : palettes).set(look, pal);
	}
	return pal;
};

const nearest = (pal: Palette, r: number, g: number, b: number): number => {
	const key = (r << 16) | (g << 8) | b;
	let i = pal.memo.get(key);
	if (i === undefined) {
		let best = Infinity;
		i = 0;
		for (let j = 0; j < pal.colors.length; j++) {
			const c = pal.colors[j]!;
			const d = (c[0] - r) ** 2 + (c[1] - g) ** 2 + (c[2] - b) ** 2;
			if (d < best) {
				best = d;
				i = j;
			}
		}
		pal.memo.set(key, i);
	}
	return i;
};

export type Scratch = {
	canvas: HTMLCanvasElement;
	ctx: CanvasRenderingContext2D;
	// A second sheet, for what goes over his head.
	top?: Scratch;
};
export const makeScratch = (): Scratch => {
	const canvas = document.createElement("canvas");
	return {
		canvas,
		ctx: canvas.getContext("2d", { willReadFrequently: true })!,
	};
};

// Every pixel either there or not.
const snapAlpha = (d: Uint8ClampedArray) => {
	for (let i = 3; i < d.length; i += 4) {
		d[i] = d[i]! < 120 ? 0 : 255;
	}
};

// A one-pixel outline round everything that is there.
const outline = (d: Uint8ClampedArray, w: number, h: number) => {
	const solid = new Uint8Array(w * h);
	for (let i = 0; i < w * h; i++) {
		solid[i] = d[i * 4 + 3]! > 0 ? 1 : 0;
	}
	for (let y = 0; y < h; y++) {
		for (let x = 0; x < w; x++) {
			const i = y * w + x;
			if (solid[i]) {
				continue;
			}
			if (
				(x > 0 && solid[i - 1]) ||
				(x < w - 1 && solid[i + 1]) ||
				(y > 0 && solid[i - w]) ||
				(y < h - 1 && solid[i + w])
			) {
				d[i * 4] = OUTLINE[0];
				d[i * 4 + 1] = OUTLINE[1];
				d[i * 4 + 2] = OUTLINE[2];
				d[i * 4 + 3] = 255;
			}
		}
	}
};

// Text in the 3 x 5 font, centered on (cx, cy), only where he is.
const drawGlyphs = (
	d: Uint8ClampedArray,
	w: number,
	h: number,
	text: string,
	cx: number,
	cy: number,
	color: RGB,
	scale = 1,
) => {
	const glyphs = [...text.toUpperCase()]
		.map((ch) => GLYPH5.get(ch))
		.filter(Boolean);
	if (glyphs.length === 0) {
		return;
	}
	const total = (glyphs.length * 4 - 1) * scale;
	const x0 = Math.round(cx - total / 2);
	const y0 = Math.round(cy - 2.5 * scale);
	glyphs.forEach((g, n) => {
		for (let row = 0; row < 5 * scale; row++) {
			for (let col = 0; col < 3 * scale; col++) {
				if (g![Math.floor(row / scale)]![Math.floor(col / scale)] !== "1") {
					continue;
				}
				const x = x0 + n * 4 * scale + col;
				const y = y0 + row;
				if (x < 0 || y < 0 || x >= w || y >= h) {
					continue;
				}
				const i = (y * w + x) * 4;
				// Only on the jersey, never off his body.
				if (d[i + 3] === 0) {
					continue;
				}
				d[i] = color[0];
				d[i + 1] = color[1];
				d[i + 2] = color[2];
			}
		}
	});
};

// SPRITES ARE DRAWN ONCE AND KEPT.
//
// Like a sprite game's, his animation runs in frames - eight to a stride or
// a loop, a dozen across a jump shot - and he turns in sixteen directions, so
// the same few pictures come round again and again. Each is made the first
// time it is needed and kept; after that a player costs one image copy. He
// still glides across the floor smoothly - only his pose steps, the way
// pixel art does.
type Kept = { canvas: HTMLCanvasElement; dx: number; dy: number };
export type SpriteCache = {
	kept: Map<string, Kept>;
	ids: WeakMap<Look, number>;
	next: number;
};
export const makeSpriteCache = (): SpriteCache => ({
	kept: new Map(),
	ids: new WeakMap(),
	next: 0,
});
const KEEP = 700;
const CYCLE_FRAMES = 8;
// Steps through a bounce for the dribbling hand.
const DRIBBLE_FRAMES = 8;
const ACT_FRAMES = 12;
const TURNS = 16;

// The pose he is drawn in: his own, stepped to the sprite's frames and turns.
const stepped = (st: PlayerState) => {
	const a = ANIMS[st.anim];
	const phase =
		a.kind === "act"
			? Math.round(Math.min(1, Math.max(0, st.phase)) * (ACT_FRAMES - 1)) /
				(ACT_FRAMES - 1)
			: Math.floor((((st.phase % 1) + 1) % 1) * CYCLE_FRAMES) / CYCLE_FRAMES;
	const turn = Math.round(st.yaw / ((Math.PI * 2) / TURNS));
	return {
		phase,
		turn: ((turn % TURNS) + TURNS) % TURNS,
		yaw: (turn * Math.PI * 2) / TURNS,
		dribble:
			st.dribble === undefined
				? undefined
				: Math.floor(st.dribble * DRIBBLE_FRAMES) / DRIBBLE_FRAMES,
	};
};

export const drawSprite = (
	ctx: CanvasRenderingContext2D,
	scratch: Scratch,
	cam: Camera,
	st: PlayerState,
	body: Body,
	look: Look,
	px: number,
	cache?: SpriteCache,
) => {
	const base = project(cam, { x: st.x, y: st.y, z: 0 });
	const k = base.k;
	// Room for him wherever his arms and feet go: a box round where he stands
	// (and, up in the air, that much higher).
	const half = (body.H * 0.62 + 0.6) * k;
	const lift = Math.max(0, st.z) * k;
	const left = base.x - half;
	const right = base.x + half;
	const upper = base.y - (body.H * 1.08 + 0.9) * k - lift;
	const lower = base.y + 0.9 * k - lift;
	// Nothing of him on the picture: nothing to draw.
	if (right < 0 || left > cam.viewW || lower < 0 || upper > cam.viewH) {
		return;
	}
	const pose = stepped(st);
	let key: string | undefined;
	if (cache) {
		let id = cache.ids.get(look);
		if (id === undefined) {
			id = cache.next++;
			cache.ids.set(look, id);
		}
		key = `${id}|${st.anim}|${pose.phase}|${pose.turn}|${Math.round(
			Math.log(k) / Math.log(1.04),
		)}|${px}|${st.holding ? 1 : 0}|${pose.dribble ?? ""}`;
		const kept = cache.kept.get(key);
		if (kept) {
			const smoothing = ctx.imageSmoothingEnabled;
			ctx.imageSmoothingEnabled = false;
			ctx.drawImage(
				kept.canvas,
				Math.round(base.x + kept.dx),
				Math.round(base.y + kept.dy - lift),
				kept.canvas.width * px,
				kept.canvas.height * px,
			);
			ctx.imageSmoothingEnabled = smoothing;
			return;
		}
	}
	// Drawn standing on the floor; a jump only moves the picture up.
	const posed: PlayerState = {
		...st,
		z: 0,
		phase: pose.phase,
		yaw: pose.yaw,
		dribble: pose.dribble,
	};
	const w = Math.max(4, Math.ceil((right - left) / px) + 2);
	const h = Math.max(4, Math.ceil((lower - upper) / px) + 2);
	const { canvas, ctx: s } = scratch;
	if (canvas.width < w || canvas.height < h) {
		canvas.width = Math.max(canvas.width, w);
		canvas.height = Math.max(canvas.height, h);
	}
	s.setTransform(1, 0, 0, 1, 0, 0);
	s.clearRect(0, 0, w, h);
	// Screen coordinates, shrunk to sprite pixels, with a pixel of margin.
	const ox = left - px;
	const oy = upper + lift - px;
	s.setTransform(1 / px, 0, 0, 1 / px, -ox / px, -oy / px);
	const anchors: FigureAnchors = drawFigure(s, cam, posed, body, look, px);
	s.setTransform(1, 0, 0, 1, 0, 0);
	const img = s.getImageData(0, 0, w, h);
	const d = img.data;
	snapAlpha(d);
	// Every color pulled to his palette.
	const pal = paletteOf(look, anchors.holding);
	for (let i = 0; i < d.length; i += 4) {
		if (d[i + 3] === 0) {
			continue;
		}
		const c = pal.colors[nearest(pal, d[i]!, d[i + 1]!, d[i + 2]!)]!;
		d[i] = c[0];
		d[i + 1] = c[1];
		d[i + 2] = c[2];
	}
	// A ball held in front of his jersey hides the lettering behind it.
	const b = anchors.ball;
	const hidden =
		b !== undefined &&
		b.front &&
		Math.abs(b.x - anchors.number.x) < b.r + anchors.number.h * 0.5 &&
		Math.abs(b.y - anchors.number.y) < b.r + anchors.number.h * 0.9;
	if (anchors.number.side !== 0 && !hidden) {
		const color = parseColor(look.kit.number);
		if (look.jerseyNumber) {
			drawGlyphs(
				d,
				w,
				h,
				look.jerseyNumber,
				(anchors.number.x - ox) / px,
				(anchors.number.y - oy) / px,
				color,
				// Bigger when he is close enough for it.
				Math.max(1, Math.floor(anchors.number.h / px / 6)),
			);
		}
		// His name across his back, the team's across his chest - when there
		// is room for it.
		const word = (anchors.number.side < 0 ? look.lastName : look.wordmark)
			.normalize("NFD")
			.replace(/[\u0300-\u036f]/g, "")
			.toUpperCase()
			.replace(/[^ '.A-Z-]/g, "");
		if (word && word.length * 4 - 1 <= anchors.letters.w / px - 2) {
			drawGlyphs(
				d,
				w,
				h,
				word,
				(anchors.letters.x - ox) / px,
				(anchors.letters.y - oy) / px,
				color,
			);
		}
	}
	s.putImageData(img, 0, 0);
	// His face, shrunk to the sprite's scale.
	s.setTransform(1 / px, 0, 0, 1 / px, -ox / px, -oy / px);
	drawHeadAt(s, cam, posed, body, look);
	s.setTransform(1, 0, 0, 1, 0, 0);
	if (anchors.over) {
		// An arm up in front of his face goes over it: drawn on its own, in his
		// palette, outlined so it reads against his face.
		scratch.top ??= makeScratch();
		const t = scratch.top;
		if (t.canvas.width < w || t.canvas.height < h) {
			t.canvas.width = Math.max(t.canvas.width, w);
			t.canvas.height = Math.max(t.canvas.height, h);
		}
		t.ctx.setTransform(1, 0, 0, 1, 0, 0);
		t.ctx.clearRect(0, 0, w, h);
		t.ctx.setTransform(1 / px, 0, 0, 1 / px, -ox / px, -oy / px);
		anchors.over(t.ctx);
		t.ctx.setTransform(1, 0, 0, 1, 0, 0);
		const top = t.ctx.getImageData(0, 0, w, h);
		const td = top.data;
		snapAlpha(td);
		for (let i = 0; i < td.length; i += 4) {
			if (td[i + 3] === 0) {
				continue;
			}
			const c = pal.colors[nearest(pal, td[i]!, td[i + 1]!, td[i + 2]!)]!;
			td[i] = c[0];
			td[i + 1] = c[1];
			td[i + 2] = c[2];
		}
		outline(td, w, h);
		t.ctx.putImageData(top, 0, 0);
		s.drawImage(t.canvas, 0, 0, w, h, 0, 0, w, h);
	}
	const img2 = s.getImageData(0, 0, w, h);
	snapAlpha(img2.data);
	outline(img2.data, w, h);
	s.putImageData(img2, 0, 0);

	// Kept for next time, as its own little canvas.
	let src: HTMLCanvasElement = canvas;
	if (cache && key) {
		const keep = document.createElement("canvas");
		keep.width = w;
		keep.height = h;
		keep.getContext("2d")!.drawImage(canvas, 0, 0, w, h, 0, 0, w, h);
		cache.kept.set(key, { canvas: keep, dx: ox - base.x, dy: oy - base.y });
		if (cache.kept.size > KEEP) {
			// The oldest go first.
			let n = cache.kept.size - KEEP + 100;
			for (const old of cache.kept.keys()) {
				cache.kept.delete(old);
				if (--n <= 0) {
					break;
				}
			}
		}
		src = keep;
	}
	const smoothing = ctx.imageSmoothingEnabled;
	ctx.imageSmoothingEnabled = false;
	ctx.drawImage(
		src,
		0,
		0,
		w,
		h,
		Math.round(ox),
		Math.round(oy - lift),
		w * px,
		h * px,
	);
	ctx.imageSmoothingEnabled = smoothing;
};
