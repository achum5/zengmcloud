import type { Camera } from "./camera.ts";

// THE GAME BALL.
//
// A real game ball's look: tan, pebbled leather; eight panels between black
// channels - one channel round its middle, one at right angles to that, and
// the two curved ones either side, each crossing only the first; and on one
// side, inside a curved channel's ring, the maker's word in a fat black script
// edged in pale gold above the middle channel, and below it the league's
// shield, OFFICIAL GAME BALL / GENUINE LEATHER and the commissioner's
// signature. (A made-up maker and league - nobody's real marks.) The same ball
// loose (arena.ts) and in his hands (sculpt.ts), turning with its spin.
//
// The ball's own frame: x out through the middle of the printed side (each
// curved channel rings x or -x), y square to the channel round its middle
// (up, on the print), z = x cross y.

export type RGB = [number, number, number];

export const LEATHER: RGB = [188, 100, 55];
export const CHANNEL: RGB = [27, 21, 18];
const PRINT_INK: RGB = [33, 26, 23];
const PRINT_EDGE: RGB = [201, 186, 160];

// The curved channel's angle off the x axis, by how far round the axis it is
// from the middle channel: nearest it (48 degrees) over the middle of the
// print, out to within 12 degrees of the channel at right angles where it
// crosses the middle one.
const CURVE_NEAR = 0.84;
const CURVE_OUT = 0.52;

// How far, in radians over the ball, a point on it is from the nearest
// channel.
export const channelDistance = (x: number, y: number, z: number): number => {
	const ax = Math.abs(x);
	const rho = Math.acos(Math.min(1, ax));
	const phi = Math.atan2(Math.abs(y), Math.abs(z));
	const c = Math.cos(phi);
	const want = CURVE_NEAR + CURVE_OUT * c * c;
	const slope = (CURVE_OUT * Math.sin(2 * phi)) / Math.max(0.2, Math.sin(rho));
	const curved = Math.abs(rho - want) / Math.sqrt(1 + slope * slope);
	return Math.min(Math.abs(y), ax, curved);
};

// The channels as lines on the ball (its frame), for a ball too small to
// paint a pixel at a time.
export const CHANNEL_LINES: [number, number, number][][] = (() => {
	const n = 64;
	const loop = (
		f: (a: number) => [number, number, number],
	): [number, number, number][] =>
		Array.from({ length: n + 1 }, (_, i) => f((i / n) * Math.PI * 2));
	const curved = (s: number) =>
		loop((a) => {
			const c = Math.cos(a);
			const rho = CURVE_NEAR + CURVE_OUT * c * c;
			return [
				s * Math.cos(rho),
				Math.sin(rho) * Math.sin(a),
				Math.sin(rho) * c,
			];
		});
	return [
		loop((a) => [Math.cos(a), 0, Math.sin(a)]),
		loop((a) => [0, Math.cos(a), Math.sin(a)]),
		curved(1),
		curved(-1),
	];
})();

// THE PRINT, laid out flat as it is seen square on: u across (to the right),
// v up, both in ball radii from the middle of the printed side - laid on the
// ball straight down onto it (so a point on the ball shows the print at its
// own u and v). Painted once into two channels of coverage - all of the print,
// and its black alone (the rest is its pale edging) - and kept at a few sizes,
// halving, so a small ball reads the print at about its own size.

type PrintLevel = { n: number; all: Uint8Array; ink: Uint8Array };

const PRINT_N = 512;

const canvasOf = (n: number) => {
	if (typeof OffscreenCanvas !== "undefined") {
		return new OffscreenCanvas(n, n) as unknown as HTMLCanvasElement;
	}
	if (typeof document !== "undefined") {
		const cv = document.createElement("canvas");
		cv.width = n;
		cv.height = n;
		return cv;
	}
	return undefined;
};

// A path in SVG's absolute M, L, C, Q and Z, onto a canvas.
const trace = (g: CanvasRenderingContext2D, d: string) => {
	const t = d.trim().split(/[\s,]+/);
	let i = 0;
	const num = () => Number(t[i++]);
	while (i < t.length) {
		const op = t[i++];
		if (op === "M") {
			g.moveTo(num(), num());
		} else if (op === "L") {
			g.lineTo(num(), num());
		} else if (op === "C") {
			g.bezierCurveTo(num(), num(), num(), num(), num(), num());
		} else if (op === "Q") {
			g.quadraticCurveTo(num(), num(), num(), num());
		} else if (op === "Z") {
			g.closePath();
		}
	}
};

// The maker's word, a fat round script, along a baseline at 0 with its
// small letters 64 high: the strokes' middles.
const WORD_W = 318;
const WORD_STROKE = 24;
const WORD_EDGE = 5;
const WORD = [
	// S
	"M 66 -84 C 58 -98, 20 -100, 15 -78 C 10 -58, 62 -58, 62 -32 C 62 -6, 20 -4, 6 -22",
	// w
	"M 84 -50 C 82 -28, 88 -12, 101 -12 C 114 -12, 119 -26, 121 -42 C 123 -26, 128 -12, 141 -12 C 156 -12, 162 -30, 162 -52",
	// i
	"M 186 -50 C 184 -30, 182 -12, 196 -12 C 203 -12, 207 -16, 210 -22",
	// s
	"M 251 -48 C 244 -56, 220 -56, 222 -42 C 224 -30, 252 -32, 252 -22 C 252 -10, 226 -6, 214 -18",
	// h: up into its loop, straight down, and over
	"M 258 -20 C 270 -42, 290 -78, 289 -98 C 288 -114, 268 -112, 270 -90 C 271 -62, 271 -34, 270 -12",
	"M 271 -36 C 280 -54, 306 -60, 308 -40 C 309 -28, 306 -18, 316 -12",
];
const WORD_DOT: [number, number] = [190, -86];

// The league's shield: a player on the drive, low, the ball in his outside
// hand, cut out of it (in the shield's own 64 x 126).
const SHIELD_W = 64;
const SHIELD_H = 126;
const SHIELD_MAN = [
	// Body: shoulders to hips, leaning into it.
	"M 26 31 L 43 29 L 41 60 L 28 62 Z",
	// Back leg, down and away; front leg, bent and out ahead.
	"M 37 58 L 48 84 L 49 107",
	"M 31 60 L 21 82 L 27 104",
	// Dribbling arm down to the ball; the other out for balance.
	"M 28 34 L 19 52 L 17 66",
	"M 42 33 L 50 47 L 54 58",
];

const SIGNATURE = [
	"M 304 372 C 312 350, 326 322, 336 322 C 346 322, 330 352, 322 366 C 336 352, 348 340, 352 346 C 356 352, 348 360, 358 356 C 368 352, 372 344, 378 348 C 382 352, 378 358, 386 356",
	"M 314 350 C 326 346, 340 342, 352 338",
	"M 392 364 C 400 342, 412 324, 420 326 C 428 328, 414 350, 410 360 C 420 350, 428 342, 432 346 C 436 350, 430 358, 438 356 C 446 352, 452 344, 458 334",
];

const paintPrint = (g: CanvasRenderingContext2D) => {
	const INK = "#ff0000";
	const EDGE = "#00ff00";
	g.lineCap = "round";
	g.lineJoin = "round";

	// The word: its pale edging everywhere first, then the black over it.
	g.save();
	g.translate(80, 214);
	g.scale(364 / WORD_W, 1.14);
	for (const [color, extra] of [
		[EDGE, WORD_EDGE * 2],
		[INK, 0],
	] as const) {
		g.strokeStyle = color;
		g.fillStyle = color;
		g.lineWidth = WORD_STROKE + extra;
		for (const d of WORD) {
			g.beginPath();
			trace(g, d);
			g.stroke();
		}
		g.beginPath();
		g.arc(
			WORD_DOT[0],
			WORD_DOT[1],
			(WORD_STROKE + extra) * 0.55,
			0,
			Math.PI * 2,
		);
		g.fill();
	}
	g.restore();

	// The shield, edged; the player and the league's letters cut out of it.
	g.save();
	g.translate(224, 290);
	const round = (x: number, y: number, w: number, h: number, r: number) => {
		g.beginPath();
		g.moveTo(x + r, y);
		g.arcTo(x + w, y, x + w, y + h, r);
		g.arcTo(x + w, y + h, x, y + h, r);
		g.arcTo(x, y + h, x, y, r);
		g.arcTo(x, y, x + w, y, r);
		g.closePath();
	};
	round(-2, -2, SHIELD_W + 4, SHIELD_H + 4, 12);
	g.fillStyle = EDGE;
	g.fill();
	round(0, 0, SHIELD_W, SHIELD_H, 10);
	g.fillStyle = INK;
	g.fill();
	g.globalCompositeOperation = "destination-out";
	g.fillStyle = "#000";
	g.strokeStyle = "#000";
	g.beginPath();
	g.arc(37, 20, 7, 0, Math.PI * 2);
	g.fill();
	g.beginPath();
	g.arc(16, 74, 6, 0, Math.PI * 2);
	g.fill();
	g.lineWidth = 8;
	for (const d of SHIELD_MAN) {
		g.beginPath();
		trace(g, d);
		if (d.endsWith("Z")) {
			g.fill();
		}
		g.stroke();
	}
	g.font = `bold 15px "Arial Narrow", Arial, sans-serif`;
	g.textAlign = "center";
	g.textBaseline = "alphabetic";
	g.fillText("PRO", SHIELD_W / 2, SHIELD_H - 7, SHIELD_W - 16);
	g.globalCompositeOperation = "source-over";
	g.restore();

	// The small print, narrow capitals: left of the shield, and under the
	// signature on its right.
	g.fillStyle = INK;
	g.strokeStyle = INK;
	g.textAlign = "center";
	g.textBaseline = "alphabetic";
	const line = (
		text: string,
		x: number,
		y: number,
		size: number,
		w: number,
	) => {
		g.font = `bold ${size}px "Arial Narrow", Arial, sans-serif`;
		g.fillText(text, x, y, w);
		// (Heavier than a bold.)
		g.lineWidth = size * 0.08;
		g.strokeText(text, x, y, w);
	};
	line("OFFICIAL GAME BALL", 148, 350, 24, 112);
	line("GENUINE LEATHER", 147, 377, 17, 72);
	line("COMMISSIONER", 380, 386, 17, 66);
	g.strokeStyle = INK;
	g.lineWidth = 2.8;
	for (const d of SIGNATURE) {
		g.beginPath();
		trace(g, d);
		g.stroke();
	}
};

let print: PrintLevel[] | null | undefined;
const printLevels = (): PrintLevel[] | null => {
	if (print !== undefined) {
		return print;
	}
	print = null;
	try {
		let cv = canvasOf(PRINT_N);
		const g = cv?.getContext("2d") as CanvasRenderingContext2D | null;
		if (!cv || !g || typeof g.getImageData !== "function") {
			return print;
		}
		paintPrint(g);
		const levels: PrintLevel[] = [];
		for (let n = PRINT_N; n >= 16; n /= 2) {
			let at = g;
			if (n < PRINT_N) {
				const next = canvasOf(n)!;
				at = next.getContext("2d") as CanvasRenderingContext2D;
				at.imageSmoothingEnabled = true;
				at.imageSmoothingQuality = "high";
				at.drawImage(cv, 0, 0, n, n);
				cv = next;
			}
			const d = at.getImageData(0, 0, n, n).data;
			const all = new Uint8Array(n * n);
			const ink = new Uint8Array(n * n);
			for (let i = 0; i < n * n; i++) {
				const a = d[i * 4 + 3]!;
				const red = d[i * 4]!;
				const green = d[i * 4 + 1]!;
				all[i] = a;
				ink[i] = red + green > 0 ? Math.round((a * red) / (red + green)) : 0;
			}
			levels.push({ n, all, ink });
		}
		print = levels;
	} catch {
		print = null;
	}
	return print;
};

// The print at (u, v), smoothly: how much of it is there, and how much of
// that is black (into out[0], out[1], 0 to 1).
const sampleAt = (lv: PrintLevel, u: number, v: number, out: Float64Array) => {
	const n = lv.n;
	const fx = ((u + 1) / 2) * n - 0.5;
	const fy = ((1 - v) / 2) * n - 0.5;
	const x0 = Math.floor(fx);
	const y0 = Math.floor(fy);
	if (x0 < 0 || y0 < 0 || x0 >= n - 1 || y0 >= n - 1) {
		out[0] = 0;
		out[1] = 0;
		return;
	}
	const tx = fx - x0;
	const ty = fy - y0;
	const i = y0 * n + x0;
	const w00 = (1 - tx) * (1 - ty);
	const w10 = tx * (1 - ty);
	const w01 = (1 - tx) * ty;
	const w11 = tx * ty;
	const { all, ink } = lv;
	out[0] =
		(all[i]! * w00 +
			all[i + 1]! * w10 +
			all[i + n]! * w01 +
			all[i + n + 1]! * w11) /
		255;
	out[1] =
		(ink[i]! * w00 +
			ink[i + 1]! * w10 +
			ink[i + n]! * w01 +
			ink[i + n + 1]! * w11) /
		255;
};

const levelFor = (levels: PrintLevel[], radius: number): PrintLevel => {
	let best = levels[0]!;
	for (const lv of levels) {
		if (lv.n >= radius * 2) {
			best = lv;
		}
	}
	return best;
};

// Fine pebbling: a speck per cell of a grid over the ball, light or dark -
// never finer than a pixel and a half, or it would crawl as the ball turns.
const grainAt = (x: number, y: number, z: number, k: number): number => {
	let h =
		Math.imul(Math.floor(x * k), 0x27d4eb2d) ^
		Math.imul(Math.floor(y * k), 0x165667b1) ^
		Math.imul(Math.floor(z * k), 0x5bd1e995);
	h = Math.imul(h ^ (h >>> 15), 0x2c1b3c6d);
	h ^= h >>> 13;
	return ((h >>> 0) & 0xff) / 127.5 - 1;
};

// THE SURFACE at a point on the ball (its frame): its color before the light
// falls on it, into out. radius: the ball's in pixels; pixel: the angle over
// the ball one pixel spans there.
export type BallSurface = {
	levels: PrintLevel[] | null;
	level: PrintLevel | undefined;
	// The print's strength (it fades out on a ball too small to show it).
	printK: number;
	grainK: number;
	grainA: number;
	// The channels: half their width, in radians, and how dark (a ball a few
	// pixels across shows only a hint of them).
	half: number;
	seamK: number;
	cov: Float64Array;
};

export const surfaceFor = (radius: number): BallSurface => {
	const levels = printLevels();
	return {
		levels,
		level: levels ? levelFor(levels, radius) : undefined,
		printK: Math.min(1, Math.max(0, (radius - 4) / 6)),
		grainK: Math.min(70, radius / 1.5),
		grainA: radius > 5 ? 0.06 : 0,
		half: Math.max(0.016, 0.55 / radius),
		seamK: Math.min(1, Math.max(0.4, radius / 7)),
		cov: new Float64Array(2),
	};
};

export const ballSurface = (
	s: BallSurface,
	x: number,
	y: number,
	z: number,
	pixel: number,
	out: Float64Array,
) => {
	let r = LEATHER[0];
	let g = LEATHER[1];
	let b = LEATHER[2];
	if (s.level && s.printK > 0 && (x > 0.12 || x < -0.12)) {
		// The printed side square on, the word alone on the other side.
		const front = x > 0;
		const u = front ? -z : z;
		if (front || y > 0.08) {
			sampleAt(s.level, u, y, s.cov);
			const all = s.cov[0]! * s.printK;
			if (all > 0.001) {
				const ink = s.cov[1]! * s.printK;
				const edge = Math.max(0, all - ink);
				const left = 1 - all;
				r = r * left + PRINT_INK[0] * ink + PRINT_EDGE[0] * edge;
				g = g * left + PRINT_INK[1] * ink + PRINT_EDGE[1] * edge;
				b = b * left + PRINT_INK[2] * ink + PRINT_EDGE[2] * edge;
			}
		}
	}
	if (s.grainA > 0) {
		const k = 1 + grainAt(x, y, z, s.grainK) * s.grainA;
		r *= k;
		g *= k;
		b *= k;
	}
	const d = channelDistance(x, y, z);
	const reach = s.half + pixel * 0.5;
	if (d < reach) {
		const e = Math.min(1, (reach - d) / Math.max(pixel, 1e-3)) * s.seamK;
		r += (CHANNEL[0] - r) * e;
		g += (CHANNEL[1] - g) * e;
		b += (CHANNEL[2] - b) * e;
	}
	out[0] = r;
	out[1] = g;
	out[2] = b;
};

// HOW IT SITS. Its frame against the picture's (x right, y up, z out of the
// picture toward the camera): as it was in his hands - the printed side a
// little to the right of facing the camera, and tipped forward - and turned from
// there by its spin, about the level axis square to the way it goes (spin > 0
// rolls it on that way, the top going first; with no way given, toward the
// far basket). Columns: where the ball's x, y and z point in the picture.

type V3 = [number, number, number];

const turnAbout = (v: V3, a: V3, angle: number): V3 => {
	// Rodrigues.
	const c = Math.cos(angle);
	const s = Math.sin(angle);
	const d = v[0] * a[0] + v[1] * a[1] + v[2] * a[2];
	return [
		v[0] * c + (a[1] * v[2] - a[2] * v[1]) * s + a[0] * d * (1 - c),
		v[1] * c + (a[2] * v[0] - a[0] * v[2]) * s + a[1] * d * (1 - c),
		v[2] * c + (a[0] * v[1] - a[1] * v[0]) * s + a[2] * d * (1 - c),
	];
};

export const HELD: [V3, V3, V3] = (() => {
	// Square on: its x toward the camera, y up, z to the left.
	let cols: V3[] = [
		[0, 0, 1],
		[0, 1, 0],
		[-1, 0, 0],
	];
	for (const [axis, angle] of [
		[[0, 1, 0], 0.22],
		[[1, 0, 0], 0.12],
		[[0, 0, 1], -0.1],
	] as [V3, number][]) {
		cols = cols.map((c) => turnAbout(c, axis, angle));
	}
	return cols as [V3, V3, V3];
})();

// The picture's frame in the world, for this camera: right, up, toward it.
export const viewFrame = (cam: Camera): [V3, V3, V3] => {
	const R: V3 = [cam.right.x, cam.right.y, cam.right.z];
	const U: V3 = cam.upright ? [0, 0, 1] : [cam.up.x, cam.up.y, cam.up.z];
	let T: V3 = [-cam.fwd.x, -cam.fwd.y, -cam.fwd.z];
	const dr = T[0] * R[0] + T[1] * R[1] + T[2] * R[2];
	const du = T[0] * U[0] + T[1] * U[1] + T[2] * U[2];
	T = [
		T[0] - dr * R[0] - du * U[0],
		T[1] - dr * R[1] - du * U[1],
		T[2] - dr * R[2] - du * U[2],
	];
	const l = Math.hypot(T[0], T[1], T[2]) || 1;
	return [R, U, [T[0] / l, T[1] / l, T[2] / l]];
};

// Where its frame points in the picture with this much spin on it, going
// `way` (radians, across the floor from toward the far basket).
export const ballTurn = (
	frame: [V3, V3, V3],
	spin: number,
	way = 0,
): [V3, V3, V3] => {
	const [R, U, T] = frame;
	// The level axis square to the way it goes, in the picture.
	const wx = -Math.sin(way);
	const wy = Math.cos(way);
	const a: V3 = [
		R[0] * wx + R[1] * wy,
		U[0] * wx + U[1] * wy,
		T[0] * wx + T[1] * wy,
	];
	const l = Math.hypot(a[0], a[1], a[2]) || 1;
	const axis: V3 = [a[0] / l, a[1] / l, a[2] / l];
	return HELD.map((c) => turnAbout(c, axis, -spin)) as [V3, V3, V3];
};

// The mirror of the picture's frame in the floor: what a point on the ball's
// reflection shows of the ball itself.
export const floorMirror = (frame: [V3, V3, V3]): [V3, V3, V3] =>
	frame.map((a) =>
		frame.map((b) => a[0] * b[0] + a[1] * b[1] - a[2] * b[2]),
	) as [V3, V3, V3];

// THE LOOSE BALL, painted a pixel at a time onto a canvas of its own (one
// kept for each size) and laid into the picture: lit from up and to the left
// like everybody, darker toward its edge, with a soft shine off the leather.

const LIGHT: V3 = (() => {
	const l: V3 = [-0.45, 0.7, 0.55];
	const n = Math.hypot(l[0], l[1], l[2]);
	return [l[0] / n, l[1] / n, l[2] / n];
})();
const SHINE: V3 = (() => {
	const h: V3 = [LIGHT[0], LIGHT[1], LIGHT[2] + 1];
	const n = Math.hypot(h[0], h[1], h[2]);
	return [h[0] / n, h[1] / n, h[2] / n];
})();

type Scratch = {
	cv: HTMLCanvasElement;
	g: CanvasRenderingContext2D;
	img: ImageData;
};
const scratches = new Map<number, Scratch | null>();
const scratchOf = (n: number): Scratch | null => {
	let s = scratches.get(n);
	if (s !== undefined) {
		return s;
	}
	s = null;
	try {
		const cv = canvasOf(n);
		const g = cv?.getContext("2d") as CanvasRenderingContext2D | null;
		if (cv && g && typeof g.createImageData === "function") {
			s = { cv, g, img: g.createImageData(n, n) };
		}
	} catch {
		s = null;
	}
	scratches.set(n, s);
	return s;
};

const px = new Float64Array(3);

// (A ball right up against the camera painted no finer than this, and
// stretched.)
const PAINT_R_MOST = 90;

export const paintBall = (
	ctx: CanvasRenderingContext2D,
	cx0: number,
	cy0: number,
	r0: number,
	turn: [V3, V3, V3],
	mirror?: [V3, V3, V3],
): boolean => {
	if (typeof ctx.drawImage !== "function") {
		return false;
	}
	const scale = Math.min(1, PAINT_R_MOST / r0);
	const r = r0 * scale;
	const cx = cx0 * scale;
	const cy = cy0 * scale;
	const n = Math.ceil(r * 2) + 2;
	const s = scratchOf(n);
	if (!s) {
		return false;
	}
	const surface = surfaceFor(r);
	const x0 = Math.round(cx - n / 2);
	const y0 = Math.round(cy - n / 2);
	const d = s.img.data;
	const [X, Y, Z] = turn;
	for (let j = 0; j < n; j++) {
		const sy = (cy - (y0 + j + 0.5)) / r;
		for (let i = 0; i < n; i++) {
			const o = (j * n + i) * 4;
			const sx = (x0 + i + 0.5 - cx) / r;
			const q = sx * sx + sy * sy;
			const dist = Math.sqrt(q);
			const cover = Math.min(1, (1 - dist) * r + 0.5);
			if (cover <= 0) {
				d[o + 3] = 0;
				continue;
			}
			let nx: number;
			let ny: number;
			let nz: number;
			if (q < 1) {
				nx = sx;
				ny = sy;
				nz = Math.sqrt(1 - q);
			} else {
				nx = sx / dist;
				ny = sy / dist;
				nz = 0;
			}
			// (Seen in the floor: the ball's own point there.)
			let mx = nx;
			let my = ny;
			let mz = nz;
			if (mirror) {
				mx = mirror[0][0] * nx + mirror[0][1] * ny + mirror[0][2] * nz;
				my = mirror[1][0] * nx + mirror[1][1] * ny + mirror[1][2] * nz;
				mz = mirror[2][0] * nx + mirror[2][1] * ny + mirror[2][2] * nz;
			}
			const bx = mx * X[0] + my * X[1] + mz * X[2];
			const by = mx * Y[0] + my * Y[1] + mz * Y[2];
			const bz = mx * Z[0] + my * Z[1] + mz * Z[2];
			ballSurface(
				surface,
				bx,
				by,
				bz,
				1 / (r * Math.max(0.3, Math.sqrt(nz))),
				px,
			);
			const lam = Math.max(0, mx * LIGHT[0] + my * LIGHT[1] + mz * LIGHT[2]);
			const k = (0.55 + 0.55 * lam) * (0.8 + 0.2 * nz);
			const h = Math.max(0, mx * SHINE[0] + my * SHINE[1] + mz * SHINE[2]);
			const h2 = h * h;
			const h4 = h2 * h2;
			const shine = h4 * h4 * h4 * 0.22 * 255;
			d[o] = px[0]! * k + shine;
			d[o + 1] = px[1]! * k + shine * 0.92;
			d[o + 2] = px[2]! * k + shine * 0.82;
			d[o + 3] = 255 * cover;
		}
	}
	s.g.putImageData(s.img, 0, 0);
	if (scale < 1) {
		ctx.drawImage(s.cv, x0 / scale, y0 / scale, n / scale, n / scale);
	} else {
		ctx.drawImage(s.cv, x0, y0);
	}
	return true;
};
