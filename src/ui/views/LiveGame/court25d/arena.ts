import { makeCourtRng } from "../courtRng.ts";
import { project, type Camera } from "./camera.ts";
import { BALL_R } from "./evaluate.ts";
import { shade } from "./figure.ts";
import {
	BENCH_SEATS,
	BENCH_Y,
	benchStart,
	COURT_W,
	RIM_R,
	RIM_Z,
	SEAT_GAP,
	type Pt3,
	type Side,
} from "./geometry.ts";

// THE BUILDING: the stands, the LED boards along their front, the scorer's
// table and the benches - flat pictures painted once per game and stood up in
// the world where the camera sees them (see Court25D) - and the baskets, the
// one part of the building that moves, drawn every frame.

export type ArenaTeam = {
	abbrev?: string;
	name?: string;
	region?: string;
	colors?: [string, string, string];
};

// A flat picture in the world: its top-left corner, how far one of its px
// goes along its width and down its height, and its size in px.
export type Plane = {
	key: string;
	origin: Pt3;
	alongX: Pt3;
	alongY: Pt3;
	w: number;
	h: number;
};

const X0 = -40;
const X1 = COURT_W + 40;
// The front row of the stands, behind the far sideline, and their rake.
const STANDS_Y = -12;
const WALL_H = 3.6;
const RAKE = (28 * Math.PI) / 180;
const SLOPE = 66;

const plane = (
	key: string,
	origin: Pt3,
	across: Pt3,
	down: Pt3,
	lengthX: number,
	lengthY: number,
	px: number,
): Plane => ({
	key,
	origin,
	alongX: { x: across.x / px, y: across.y / px, z: across.z / px },
	alongY: { x: down.x / px, y: down.y / px, z: down.z / px },
	w: Math.round(lengthX * px),
	h: Math.round(lengthY * px),
});

export const STANDS = plane(
	"stands",
	{
		x: X0,
		y: STANDS_Y - SLOPE * Math.cos(RAKE),
		z: WALL_H + SLOPE * Math.sin(RAKE),
	},
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: Math.cos(RAKE), z: -Math.sin(RAKE) },
	X1 - X0,
	SLOPE,
	8,
);
export const LED_WALL = plane(
	"wall",
	{ x: X0, y: STANDS_Y, z: WALL_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	X1 - X0,
	WALL_H,
	12,
);
export const FLOOR = plane(
	"floor",
	{ x: X0, y: STANDS_Y, z: 0 },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 1, z: 0 },
	X1 - X0,
	62 - STANDS_Y,
	2,
);
export const TABLE_X0 = 37;
export const TABLE_X1 = 57;
const TABLE_Y = -5;
const TABLE_D = 2.2;
const TABLE_H = 2.7;
export const TABLE_TOP = plane(
	"tableTop",
	{ x: TABLE_X0, y: TABLE_Y - TABLE_D, z: TABLE_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 1, z: 0 },
	TABLE_X1 - TABLE_X0,
	TABLE_D,
	12,
);
export const TABLE_FRONT = plane(
	"tableFront",
	{ x: TABLE_X0, y: TABLE_Y, z: TABLE_H },
	{ x: 1, y: 0, z: 0 },
	{ x: 0, y: 0, z: -1 },
	TABLE_X1 - TABLE_X0,
	TABLE_H,
	12,
);
// Each team's bench: a row of chairs beside the table.
const BENCH_LEN = BENCH_SEATS * SEAT_GAP + 0.6;
export const benchPlane = (t: Side) =>
	plane(
		`bench${t}`,
		{ x: benchStart(t), y: BENCH_Y, z: 3.1 },
		{ x: 1, y: 0, z: 0 },
		{ x: 0, y: 0, z: -1 },
		BENCH_LEN,
		3.1,
		12,
	);

const PALETTE = [
	"#f2f2f2",
	"#1c1c1c",
	"#3b3f46",
	"#7c8592",
	"#24365c",
	"#8b2331",
	"#2f5d3a",
	"#c9b79c",
	"#d9d9d9",
	"#5a4636",
];

const teamColor = (team: ArenaTeam | undefined, i: number, fallback: string) =>
	team?.colors?.[i] || fallback;

// A point on the stands: x along the court, `up` feet up the rake from the
// front row.
export const standsPoint = (x: number, up: number): Pt3 => ({
	x,
	y: STANDS_Y - up * Math.cos(RAKE),
	z: WALL_H + up * Math.sin(RAKE),
});

// The crowd, painted once - and twice more on its feet (`up` 1 and 2: arms
// high, then waving wide), the same people in the same seats, for when the
// building erupts.
export const paintStands = (
	home: ArenaTeam | undefined,
	away: ArenaTeam | undefined,
	seed: string,
	up: 0 | 1 | 2 = 0,
): HTMLCanvasElement => {
	const { w, h } = STANDS;
	const px = w / (X1 - X0);
	const canvas = document.createElement("canvas");
	canvas.width = w;
	canvas.height = h;
	const ctx = canvas.getContext("2d")!;
	const rng = makeCourtRng(`stands|${seed}`);
	// Who gets up is decided apart from who is there, so both pictures have
	// the same crowd.
	const rise = makeCourtRng(`stands-up|${seed}`);
	const g = ctx.createLinearGradient(0, 0, 0, h);
	g.addColorStop(0, "#07080b");
	g.addColorStop(1, "#1a1c22");
	ctx.fillStyle = g;
	ctx.fillRect(0, 0, w, h);

	const homeC = [teamColor(home, 0, "#8c1d40"), teamColor(home, 1, "#f2c14e")];
	const awayC = [teamColor(away, 0, "#1d3461"), teamColor(away, 1, "#f28c28")];
	const shirt = () => {
		const r = rng();
		if (r < 0.32) {
			return homeC[0]!;
		}
		if (r < 0.44) {
			return homeC[1]!;
		}
		if (r < 0.5) {
			return awayC[0]!;
		}
		return PALETTE[Math.floor(rng() * PALETTE.length)]!;
	};
	const skins = ["#f1c7a5", "#d9a77f", "#b07a52", "#8a5a3b", "#5e3b26"];
	const rowFt = 2.75;
	const rows = Math.floor(SLOPE / rowFt);
	const aisle = 31;
	// A ribbon board between the lower and upper deck.
	const ribbonRow = 13;
	for (let r = 0; r < rows; r++) {
		// Rows from the front (bottom of the picture) up.
		const yb = h - r * rowFt * px;
		if (r === ribbonRow) {
			ctx.fillStyle = "#050507";
			ctx.fillRect(0, yb - rowFt * px, w, rowFt * px);
			ctx.fillStyle = homeC[0]!;
			ctx.fillRect(0, yb - rowFt * px * 0.78, w, rowFt * px * 0.5);
			ctx.fillStyle = "rgba(255,255,255,0.85)";
			ctx.font = `800 ${Math.round(rowFt * px * 0.42)}px Arial, sans-serif`;
			ctx.textBaseline = "middle";
			const label = (home?.name || home?.abbrev || "").toUpperCase();
			for (let x = 20; x < w; x += 26 * px) {
				ctx.fillText(label, x, yb - rowFt * px * 0.53);
			}
			continue;
		}
		// Seat backs.
		ctx.fillStyle = r % 2 ? "#22252c" : "#1d2026";
		ctx.fillRect(0, yb - 0.9 * px, w, 0.9 * px);
		for (let xf = X0 + 0.6; xf < X1 - 0.6; xf += 1.85) {
			const mod = (((xf - X0) % aisle) + aisle) % aisle;
			if (mod < 3) {
				continue;
			}
			if (rng() > 0.9) {
				continue;
			}
			const x = (xf - X0) * px + (rng() - 0.5) * 2;
			const bodyW = (1.25 + rng() * 0.35) * px;
			const color = shirt();
			const skin = skins[Math.floor(rng() * skins.length)]!;
			const homeFan = color === homeC[0] || color === homeC[1];
			const standing = up > 0 && rise() < (homeFan ? 0.88 : 0.45);
			// On his feet he is taller, and his arms are up.
			const bodyH = (standing ? 2 : 1.45) * px;
			const lift = standing ? 0.55 * px : 0;
			ctx.fillStyle = color;
			ctx.beginPath();
			ctx.roundRect(
				x - bodyW / 2,
				yb - bodyH - 0.5 * px - lift,
				bodyW,
				bodyH,
				0.35 * px,
			);
			ctx.fill();
			const headY = yb - bodyH - 0.75 * px - lift;
			if (standing) {
				// Arms up in a V - some waving a towel.
				const reach = (1.25 + rise() * 0.45) * px;
				const spread = (up === 1 ? 0.3 : 0.95) * px;
				const shoulderY = headY + 0.55 * px;
				const towel = homeFan && rise() < 0.35;
				ctx.strokeStyle = skin;
				ctx.lineWidth = 0.46 * px;
				ctx.lineCap = "round";
				ctx.beginPath();
				const hands: [number, number][] = [];
				for (const side of [-1, 1]) {
					const sx = x + (side * bodyW) / 2.4;
					const hx = sx + side * spread;
					const hy = shoulderY - reach + (up === 2 ? 0.3 * px : 0);
					ctx.moveTo(sx, shoulderY);
					ctx.lineTo(hx, hy);
					hands.push([hx, hy]);
				}
				ctx.stroke();
				if (towel) {
					const [hx, hy] = hands[up === 1 ? 1 : 0]!;
					ctx.fillStyle = rise() < 0.5 ? "#f4f4f4" : homeC[0]!;
					ctx.fillRect(hx - 0.45 * px, hy - 0.95 * px, 0.9 * px, 0.75 * px);
				}
			}
			ctx.fillStyle = skin;
			ctx.beginPath();
			ctx.arc(x, headY, 0.36 * px, 0, Math.PI * 2);
			ctx.fill();
			if (rng() < 0.05) {
				// Somebody's on his feet.
				ctx.fillRect(
					x - bodyW / 2 - 0.15 * px,
					yb - bodyH - 1.6 * px,
					0.25 * px,
					1.1 * px,
				);
			}
		}
		// The aisles.
		ctx.fillStyle = "#2b2e35";
		for (let xf = X0; xf < X1; xf += aisle) {
			ctx.fillRect((xf - X0) * px, yb - rowFt * px, 2.4 * px, rowFt * px);
		}
	}
	// The light falls on the court: the upper deck fades into the dark.
	const dark = ctx.createLinearGradient(0, 0, 0, h);
	dark.addColorStop(0, "rgba(3,3,6,0.88)");
	dark.addColorStop(0.45, "rgba(3,3,6,0.5)");
	dark.addColorStop(1, "rgba(3,3,6,0.12)");
	ctx.fillStyle = dark;
	ctx.fillRect(0, 0, w, h);
	return canvas;
};

export const paintWall = (home: ArenaTeam | undefined): HTMLCanvasElement => {
	const { w, h } = LED_WALL;
	const px = w / (X1 - X0);
	const canvas = document.createElement("canvas");
	canvas.width = w;
	canvas.height = h;
	const ctx = canvas.getContext("2d")!;
	ctx.fillStyle = "#06070a";
	ctx.fillRect(0, 0, w, h);
	const c0 = teamColor(home, 0, "#8c1d40");
	const c1 = teamColor(home, 1, "#f2c14e");
	const name = (home?.name || home?.abbrev || "").toUpperCase();
	const region = (home?.region || "").toUpperCase();
	const seg = 22 * px;
	ctx.textAlign = "center";
	ctx.textBaseline = "middle";
	ctx.font = `800 ${Math.round(h * 0.5)}px Arial, sans-serif`;
	for (let i = 0, x = 0; x < w; i++, x += seg) {
		ctx.fillStyle = i % 2 ? c0 : "#0b0c10";
		ctx.fillRect(x + 2, h * 0.12, seg - 4, h * 0.76);
		ctx.fillStyle = i % 2 ? "#ffffff" : c1;
		ctx.fillText(i % 2 || !region ? name : region, x + seg / 2, h * 0.52);
	}
	return canvas;
};

export const paintTable = (
	home: ArenaTeam | undefined,
	away: ArenaTeam | undefined,
): { front: HTMLCanvasElement; top: HTMLCanvasElement } => {
	const front = document.createElement("canvas");
	front.width = TABLE_FRONT.w;
	front.height = TABLE_FRONT.h;
	const ctx = front.getContext("2d")!;
	const w = front.width;
	const h = front.height;
	ctx.fillStyle = "#0c0d11";
	ctx.fillRect(0, 0, w, h);
	ctx.fillStyle = "#050507";
	ctx.fillRect(0, h * 0.2, w, h * 0.55);
	ctx.textAlign = "center";
	ctx.textBaseline = "middle";
	ctx.font = `800 ${Math.round(h * 0.34)}px Arial, sans-serif`;
	ctx.fillStyle = teamColor(away, 0, "#1d3461");
	ctx.fillRect(w * 0.04, h * 0.26, w * 0.28, h * 0.43);
	ctx.fillStyle = teamColor(home, 0, "#8c1d40");
	ctx.fillRect(w * 0.68, h * 0.26, w * 0.28, h * 0.43);
	ctx.fillStyle = "#ffffff";
	ctx.fillText((away?.abbrev ?? "").toUpperCase(), w * 0.18, h * 0.48);
	ctx.fillText((home?.abbrev ?? "").toUpperCase(), w * 0.82, h * 0.48);
	ctx.fillStyle = "#ffb547";
	ctx.fillText("VS", w * 0.5, h * 0.48);
	const top = document.createElement("canvas");
	top.width = TABLE_TOP.w;
	top.height = TABLE_TOP.h;
	const tctx = top.getContext("2d")!;
	tctx.fillStyle = "#2a2c33";
	tctx.fillRect(0, 0, top.width, top.height);
	tctx.fillStyle = "#3a3d46";
	tctx.fillRect(0, top.height * 0.7, top.width, top.height * 0.3);
	return { front, top };
};

export const paintBench = (team: ArenaTeam | undefined): HTMLCanvasElement => {
	const p = benchPlane(0);
	const canvas = document.createElement("canvas");
	canvas.width = p.w;
	canvas.height = p.h;
	const ctx = canvas.getContext("2d")!;
	const px = p.w / BENCH_LEN;
	const c = teamColor(team, 0, "#333a44");
	for (let i = 0; i < BENCH_SEATS; i++) {
		const x = (0.4 + i * SEAT_GAP) * px;
		// Legs, seat, back.
		ctx.fillStyle = "#111";
		ctx.fillRect(x + 0.1 * px, p.h - 1.5 * px, 0.12 * px, 1.5 * px);
		ctx.fillRect(x + 1.3 * px, p.h - 1.5 * px, 0.12 * px, 1.5 * px);
		ctx.fillStyle = shade(c, -0.25);
		ctx.fillRect(x, p.h - 1.75 * px, 1.55 * px, 0.35 * px);
		ctx.fillStyle = c;
		ctx.beginPath();
		ctx.roundRect(x, 0, 1.55 * px, 1.6 * px, 0.25 * px);
		ctx.fill();
	}
	return canvas;
};

// ---- the baskets --------------------------------------------------------------

const line = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	a: Pt3,
	b: Pt3,
	width: number,
) => {
	const p = project(cam, a);
	const q = project(cam, b);
	ctx.lineWidth = Math.max(0.6, width * (p.k + q.k) * 0.5);
	ctx.beginPath();
	ctx.moveTo(p.x, p.y);
	ctx.lineTo(q.x, q.y);
	ctx.stroke();
};

// The same, inked: a dark edge drawn first, the color down its middle.
const INK = "rgba(22, 15, 13, 0.92)";
const inkLine = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	a: Pt3,
	b: Pt3,
	width: number,
	color: string,
) => {
	const p = project(cam, a);
	const q = project(cam, b);
	const k = (p.k + q.k) * 0.5;
	const w = Math.max(0.8, width * k);
	ctx.lineCap = "round";
	ctx.beginPath();
	ctx.moveTo(p.x, p.y);
	ctx.lineTo(q.x, q.y);
	ctx.strokeStyle = INK;
	ctx.lineWidth = w + 2 * Math.max(1, 0.05 * k);
	ctx.stroke();
	ctx.strokeStyle = color;
	ctx.lineWidth = w;
	ctx.stroke();
};

const poly = (ctx: CanvasRenderingContext2D, cam: Camera, pts: Pt3[]) => {
	ctx.beginPath();
	pts.forEach((pt, i) => {
		const p = project(cam, pt);
		if (i === 0) {
			ctx.moveTo(p.x, p.y);
		} else {
			ctx.lineTo(p.x, p.y);
		}
	});
	ctx.closePath();
};

export type HoopFx = {
	// 0..1, fading: the net snapping up after a make, the iron ringing after a
	// miss, the rim bent by a dunk.
	swish: number;
	clank: number;
	dunk: number;
	// A dunk worth a replay rattles the whole basket harder.
	big?: boolean;
	t: number;
};

// The basket at one end: stanchion, backboard and its shot clock, then the
// rim and net - with `between` drawn inside them (the ball going through).
export const drawHoop = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	side: Side,
	padColor: string,
	shotClock: string,
	fx: HoopFx,
	between?: () => void,
) => {
	const sgn = side === 0 ? 1 : -1;
	const base = side === 0 ? 0 : COURT_W;
	const X = (d: number) => base + sgn * d;
	// After a dunk the whole basket rings: the board and rim shake up and
	// down and rock side to side, dying away over a second; while he hangs
	// on it the rim is bent down.
	const ringing = fx.dunk * fx.dunk * (fx.big ? 1.5 : 1);
	const shake =
		fx.clank * 0.06 * Math.sin(fx.t / 22) +
		ringing * 0.42 * Math.sin(fx.t / 22);
	const rock = ringing * 0.16 * Math.sin(fx.t / 31);
	const bend = Math.max(0, (fx.dunk - 0.55) / 0.45) * 0.32;

	// The stanchion: padded base behind the baseline, post, arm to the board.
	const b0 = X(-9.5);
	const b1 = X(-5);
	const inkW = Math.max(1, 0.05 * project(cam, { x: X(4), y: 25, z: 0 }).k);
	const box = (y0: number, y1: number, z1: number) => {
		const pts = (x: number, y: number, z: number): Pt3 => ({ x, y, z });
		// Top, the face toward the court, the face toward the camera.
		ctx.lineJoin = "round";
		ctx.lineWidth = inkW * 2;
		ctx.strokeStyle = INK;
		for (const [fill, face] of [
			[
				shade(padColor, -0.15),
				[pts(b0, y0, z1), pts(b1, y0, z1), pts(b1, y1, z1), pts(b0, y1, z1)],
			],
			[
				padColor,
				[pts(b1, y0, 0), pts(b1, y1, 0), pts(b1, y1, z1), pts(b1, y0, z1)],
			],
			[
				shade(padColor, -0.32),
				[pts(b0, y1, 0), pts(b1, y1, 0), pts(b1, y1, z1), pts(b0, y1, z1)],
			],
		] as const) {
			poly(ctx, cam, [...face]);
			ctx.stroke();
			ctx.fillStyle = fill;
			ctx.fill();
		}
	};
	box(22.4, 27.6, 3.4);
	inkLine(
		ctx,
		cam,
		{ x: X(-6.6), y: 25, z: 3.4 },
		{ x: X(-6.2), y: 25, z: 12.4 },
		0.75,
		"#3d4048",
	);
	inkLine(
		ctx,
		cam,
		{ x: X(-6.2), y: 25, z: 12.4 },
		{ x: X(3.7), y: 25 + rock, z: 11.9 + shake },
		0.42,
		"#4a4e57",
	);
	inkLine(
		ctx,
		cam,
		{ x: X(-6.4), y: 25, z: 8.6 },
		{ x: X(3.7), y: 25 + rock, z: 10.2 + shake },
		0.28,
		"#4a4e57",
	);
	// Padding round the bottom of the post.
	inkLine(
		ctx,
		cam,
		{ x: X(-6.6), y: 25, z: 3.4 },
		{ x: X(-6.5), y: 25, z: 7.4 },
		1.15,
		padColor,
	);

	// The backboard: glass in a white frame, the shooter's square, and the
	// shot clock on top.
	const bx = X(4);
	const bz = shake;
	const glass = [
		{ x: bx, y: 22 + rock, z: 9.5 + bz },
		{ x: bx, y: 28 + rock, z: 9.5 + bz },
		{ x: bx, y: 28 + rock, z: 13 + bz },
		{ x: bx, y: 22 + rock, z: 13 + bz },
	];
	const gk = project(cam, glass[0]!).k;
	ctx.fillStyle = "rgba(214, 232, 244, 0.34)";
	poly(ctx, cam, glass);
	ctx.fill();
	ctx.lineJoin = "round";
	ctx.strokeStyle = INK;
	ctx.lineWidth = Math.max(0.8, 0.22 * gk) + 2 * inkW;
	ctx.stroke();
	ctx.strokeStyle = "#ffffff";
	ctx.lineWidth = Math.max(0.8, 0.22 * gk);
	ctx.stroke();
	// The padding along the bottom of the board.
	inkLine(
		ctx,
		cam,
		{ x: bx, y: 22 + rock, z: 9.45 + bz },
		{ x: bx, y: 28 + rock, z: 9.45 + bz },
		0.3,
		padColor,
	);
	ctx.strokeStyle = "rgba(255,255,255,0.92)";
	ctx.lineWidth = Math.max(0.6, 0.12 * gk);
	poly(ctx, cam, [
		{ x: bx, y: 24 + rock, z: 10.05 + bz },
		{ x: bx, y: 26 + rock, z: 10.05 + bz },
		{ x: bx, y: 26 + rock, z: 11.5 + bz },
		{ x: bx, y: 24 + rock, z: 11.5 + bz },
	]);
	ctx.stroke();
	const sc = [
		{ x: X(3.85), y: 24.15 + rock, z: 13.1 + bz },
		{ x: X(3.85), y: 25.85 + rock, z: 13.1 + bz },
		{ x: X(3.85), y: 25.85 + rock, z: 14.05 + bz },
		{ x: X(3.85), y: 24.15 + rock, z: 14.05 + bz },
	];
	ctx.fillStyle = "#0d0d10";
	poly(ctx, cam, sc);
	ctx.fill();
	if (shotClock) {
		const c = project(cam, { x: X(3.8), y: 25 + rock, z: 13.58 + bz });
		ctx.fillStyle = "#ff3b2f";
		ctx.font = `700 ${Math.max(5, 0.72 * c.k)}px "Courier New", monospace`;
		ctx.textAlign = "center";
		ctx.textBaseline = "middle";
		ctx.fillText(shotClock, c.x, c.y);
	}

	// The rim and its net.
	const rx = X(5.25);
	const rz = RIM_Z - bend + shake * 0.5;
	const N = 16;
	const ring = (r: number, z: number, sway = 0): Pt3[] =>
		Array.from({ length: N }, (_, i) => {
			const a = (i / N) * Math.PI * 2;
			return {
				x: rx + Math.cos(a) * r + sway,
				y: 25 + rock + Math.sin(a) * r,
				z,
			};
		});
	const rim = ring(RIM_R, rz);
	// After a make the net whips up and sways; a dunk yanks it down first.
	const lift = fx.swish * 0.75 - ringing * 0.5 * Math.cos(fx.t / 40);
	const sway = (fx.swish * 0.12 + ringing * 0.1) * Math.sin(fx.t / 45);
	const mid = ring(RIM_R * 0.78, rz - 0.75 + lift * 0.5, sway * 0.5);
	const bottom = ring(RIM_R * 0.6, rz - 1.55 + lift, sway);
	// Back half: the far side of the ring (smaller y is farther away).
	const isBack = (i: number) => Math.sin((i / N) * Math.PI * 2) < 0;
	const netLines = (back: boolean) => {
		ctx.strokeStyle = back ? "rgba(225,225,225,0.7)" : "rgba(252,252,252,0.95)";
		ctx.lineCap = "round";
		for (let i = 0; i < N; i++) {
			if (isBack(i) !== back) {
				continue;
			}
			const j = (i + 1) % N;
			const k = (i + N - 1) % N;
			line(ctx, cam, rim[i]!, mid[j]!, 0.055);
			line(ctx, cam, rim[i]!, mid[k]!, 0.055);
			line(ctx, cam, mid[i]!, bottom[j]!, 0.05);
			line(ctx, cam, mid[i]!, bottom[k]!, 0.05);
		}
	};
	const rimArc = (back: boolean) => {
		for (let i = 0; i < N; i++) {
			if (isBack(i) !== back) {
				continue;
			}
			inkLine(
				ctx,
				cam,
				rim[i]!,
				rim[(i + 1) % N]!,
				0.17,
				back ? "#c4501f" : "#f06a2a",
			);
		}
	};
	// The bracket from the board to the rim.
	inkLine(
		ctx,
		cam,
		{ x: X(4.05), y: 25 + rock, z: rz - 0.25 },
		{ x: X(4.55), y: 25 + rock, z: rz },
		0.17,
		"#c4501f",
	);
	rimArc(true);
	netLines(true);
	between?.();
	netLines(false);
	rimArc(false);
};

// How close the ball is to going through a rim: drawn between the rim's back
// and front halves when it is.
export const ballAtRim = (ball: Pt3): Side | undefined => {
	for (const side of [0, 1] as const) {
		const rx = side === 0 ? 5.25 : COURT_W - 5.25;
		if (
			Math.hypot(ball.x - rx, ball.y - 25) < RIM_R + BALL_R + 0.5 &&
			ball.z > RIM_Z - 2.2 &&
			ball.z < RIM_Z + 1.4
		) {
			return side;
		}
	}
	return undefined;
};

export const drawBall = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	ball: Pt3,
	spin: number,
) => {
	const c = project(cam, ball);
	const r = Math.max(1.6, BALL_R * c.k);
	const g = ctx.createRadialGradient(
		c.x - r * 0.35,
		c.y - r * 0.4,
		r * 0.1,
		c.x,
		c.y,
		r,
	);
	g.addColorStop(0, "#f9a964");
	g.addColorStop(0.55, "#e2702a");
	g.addColorStop(1, "#a44716");
	ctx.beginPath();
	ctx.arc(c.x, c.y, r, 0, Math.PI * 2);
	ctx.strokeStyle = INK;
	ctx.lineWidth = Math.max(1, 0.1 * c.k) * 2;
	ctx.stroke();
	ctx.fillStyle = g;
	ctx.fill();
	if (r > 3) {
		ctx.save();
		ctx.beginPath();
		ctx.arc(c.x, c.y, r, 0, Math.PI * 2);
		ctx.clip();
		ctx.strokeStyle = "rgba(40, 16, 6, 0.85)";
		ctx.lineWidth = Math.max(0.6, r * 0.11);
		ctx.translate(c.x, c.y);
		ctx.rotate(spin);
		ctx.beginPath();
		ctx.moveTo(-r, 0);
		ctx.lineTo(r, 0);
		ctx.stroke();
		ctx.beginPath();
		ctx.ellipse(0, 0, r * Math.abs(Math.cos(spin * 1.7)), r, 0, 0, Math.PI * 2);
		ctx.stroke();
		ctx.restore();
	}
};

// A soft dark patch on the floor under something: a player's feet, the ball.
export const drawShadow = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	x: number,
	y: number,
	radius: number,
	strength: number,
) => {
	if (strength <= 0.01) {
		return;
	}
	const c = project(cam, { x, y, z: 0 });
	const near = project(cam, { x, y: y + radius, z: 0 });
	const far = project(cam, { x, y: y - radius, z: 0 });
	const rx = radius * c.k;
	const ry = Math.max(0.5, Math.abs(near.y - far.y) / 2);
	ctx.save();
	ctx.translate(c.x, c.y);
	ctx.scale(1, ry / rx);
	const g = ctx.createRadialGradient(0, 0, 0, 0, 0, rx);
	g.addColorStop(0, `rgba(0,0,0,${0.5 * strength})`);
	g.addColorStop(0.6, `rgba(0,0,0,${0.3 * strength})`);
	g.addColorStop(1, "rgba(0,0,0,0)");
	ctx.fillStyle = g;
	ctx.beginPath();
	ctx.arc(0, 0, rx, 0, Math.PI * 2);
	ctx.fill();
	ctx.restore();
};
