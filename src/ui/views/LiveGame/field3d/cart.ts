import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { FIGURE_SCALE } from "./body.ts";
import { shade } from "./color.ts";
import type { Play } from "./director.ts";
import type { Pt, Pt3, Side } from "./geometry.ts";

// THE CART, for a man hurt badly (see hurt.ts): out from beyond his
// sideline to him, round to park alongside him, him helped up onto the
// back of it - sat on its back end, looking back - and away to the
// sideline with him, a trainer driving. White, a band of his team's color
// down each side, black seats, a roof over the front on four posts.

// Its way, piece by piece: from `from`, bent toward `ctrl`, to `to` - off
// from a stop and into one at the end (see CART_RAMP_MS), steady between.
export type CartLeg = { t0: number; t1: number; from: Pt; ctrl: Pt; to: Pt };
export type Cart = { team: Side; legs: CartLeg[] };

// How long it is and how wide (feet); its deck as high as a man sits (see
// bench.ts); where along it (feet ahead of its middle) the driver sits, and
// the man on the back - and the driver this far over to his left.
export const CART_L = 9;
export const CART_W = 4.4;
const DECK_Z = 1.6 * FIGURE_SCALE;
export const DRIVER_AT = 0.9;
export const DRIVER_LEFT = 1;
export const BACK_AT = -(CART_L / 2 - 0.8);
// Up to speed from a stop in this long, ms, and down to one.
export const CART_RAMP_MS = 1500;

const BODY = "#f1f1ee";
const DARK = "#26282c";
const TIRE = "#151618";
const HOOD_L = 1.9;
const HOOD_Z = 2.9 * FIGURE_SCALE;
const SEAT_BACK = [-0.35, 0] as const;
const SEAT_BACK_Z = 3.4 * FIGURE_SCALE;
const ROOF = [-0.7, CART_L / 2 - 0.25] as const;
const ROOF_Z = 6.5 * FIGURE_SCALE;
const ROOF_T = 0.25;
const POST = 0.18;
const WHEEL_R = 0.85;
const WHEEL_W = 0.6;
const WHEELS_AT = CART_L / 2 - 1.4;
const BAND_Z = [1.05, 1.45] as const;

// (Along a leg, sampled this many times for how far it goes.)
const SAMPLES = 16;
const bend = (l: Pick<CartLeg, "from" | "ctrl" | "to">, u: number): Pt => ({
	x: (1 - u) * (1 - u) * l.from.x + 2 * (1 - u) * u * l.ctrl.x + u * u * l.to.x,
	y: (1 - u) * (1 - u) * l.from.y + 2 * (1 - u) * u * l.ctrl.y + u * u * l.to.y,
});
// (How far along it each sample is, feet.)
const along = (l: Pick<CartLeg, "from" | "ctrl" | "to">) => {
	const out = [0];
	let q = l.from;
	for (let k = 1; k <= SAMPLES; k++) {
		const p = bend(l, k / SAMPLES);
		out.push(out.at(-1)! + Math.hypot(p.x - q.x, p.y - q.y));
		q = p;
	}
	return out;
};
// How long a leg takes, ms, at `fps` feet a second between the stops.
export const cartLegMs = (
	l: Pick<CartLeg, "from" | "ctrl" | "to">,
	fps: number,
) => (along(l).at(-1)! / fps) * 1000 + CART_RAMP_MS;

// Where it is at t, and which way it points (the way its path goes there).
export const cartAt = (
	cart: Cart,
	t: number,
): { x: number; y: number; yaw: number } => {
	const legs = cart.legs;
	const l = legs.find((x) => t < x.t1) ?? legs.at(-1)!;
	const span = l.t1 - l.t0;
	const tau = Math.min(span, Math.max(0, t - l.t0));
	// (How much of the way it has gone: up to speed, steady, and down.)
	const ramp = Math.min(CART_RAMP_MS, span / 2);
	const v = span > ramp ? 1 / (span - ramp) : 0;
	const f =
		span <= 0
			? 1
			: tau < ramp
				? (0.5 * v * tau * tau) / ramp
				: tau <= span - ramp
					? v * (tau - ramp / 2)
					: 1 - (0.5 * v * (span - tau) * (span - tau)) / ramp;
	const len = along(l);
	const want = f * len.at(-1)!;
	let k = 1;
	while (k < SAMPLES && len[k]! < want) {
		k += 1;
	}
	const a = len[k - 1]!;
	const b = len[k]!;
	const u = (k - 1 + (b > a ? (want - a) / (b - a) : 0)) / SAMPLES;
	const p = bend(l, Math.min(1, Math.max(0, u)));
	const dx = 2 * (1 - u) * (l.ctrl.x - l.from.x) + 2 * u * (l.to.x - l.ctrl.x);
	const dy = 2 * (1 - u) * (l.ctrl.y - l.from.y) + 2 * u * (l.to.y - l.ctrl.y);
	return { x: p.x, y: p.y, yaw: Math.atan2(dy, dx) };
};

// A place on it (feet ahead of its middle, feet over to its left), where
// it is at t.
export const onCart = (cart: Cart, t: number, ahead: number, left = 0) => {
	const c = cartAt(cart, t);
	return {
		x: c.x + Math.cos(c.yaw) * ahead - Math.sin(c.yaw) * left,
		y: c.y + Math.sin(c.yaw) * ahead + Math.cos(c.yaw) * left,
		yaw: c.yaw,
	};
};

type Item = { depth: number; draw: () => void };
// (A box's faces: its front, back, left side, right side, top.)
const FRONT = 1;
const BACK = 2;
const LEFT = 4;
const RIGHT = 8;
const TOP = 16;
const ALL = FRONT | BACK | LEFT | RIGHT | TOP;
type Box = { f: [number, number]; s: [number, number]; z: [number, number] };

export const cartGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	play: Play | undefined,
	t: number,
	colors: [string, string],
): Item[] => {
	const cart = play?.cart;
	// (Not there until it is called for.)
	if (!cart || t < cart.legs[0]!.t0 || t > play.t1) {
		return [];
	}
	const at = cartAt(cart, t);
	const cos = Math.cos(at.yaw);
	const sin = Math.sin(at.yaw);
	const w = (f: number, s: number, z: number): Pt3 => ({
		x: at.x + cos * f - sin * s,
		y: at.y + sin * f + cos * s,
		z,
	});
	const mid = w(0, 0, 0);
	const depth = depthOf(cam, mid);
	if (depth <= 1) {
		return [];
	}
	const team = colors[cart.team];
	const quad = (pts: Pt3[], fill: string) => {
		const q = pts.map((p) => project(cam, p));
		ctx.fillStyle = fill;
		ctx.beginPath();
		ctx.moveTo(q[0]!.x, q[0]!.y);
		for (const p of q.slice(1)) {
			ctx.lineTo(p.x, p.y);
		}
		ctx.closePath();
		ctx.fill();
	};
	// (A box on it: the faces turned to the camera - of those asked for -
	// the top lit, the sides darker the more they face away from the sky's
	// light.)
	const box = ({ f, s, z }: Box, fill: string, faces = ALL) => {
		const [f0, f1] = f;
		const [s0, s1] = s;
		const [z0, z1] = z;
		const sides: [[Pt3, Pt3], number, number, number][] = [
			[[w(f1, s0, 0), w(f1, s1, 0)], cos, sin, FRONT],
			[[w(f0, s1, 0), w(f0, s0, 0)], -cos, -sin, BACK],
			[[w(f0, s1, 0), w(f1, s1, 0)], -sin, cos, LEFT],
			[[w(f1, s0, 0), w(f0, s0, 0)], sin, -cos, RIGHT],
		];
		for (const [[a, b], nx, ny, face] of sides) {
			const cx = (a.x + b.x) / 2;
			const cy = (a.y + b.y) / 2;
			if (
				!(faces & face) ||
				(cam.pos.x - cx) * nx + (cam.pos.y - cy) * ny <= 0
			) {
				continue;
			}
			quad(
				[
					{ ...a, z: z0 },
					{ ...b, z: z0 },
					{ ...b, z: z1 },
					{ ...a, z: z1 },
				],
				shade(fill, -0.12 - 0.14 * Math.abs(nx)),
			);
		}
		if (faces & TOP) {
			quad(
				[w(f0, s0, z1), w(f1, s0, z1), w(f1, s1, z1), w(f0, s1, z1)],
				shade(fill, 0.06),
			);
		}
	};
	const by = (b: Box) =>
		depthOf(cam, w((b.f[0] + b.f[1]) / 2, (b.s[0] + b.s[1]) / 2, 0));
	const half = CART_W / 2;
	// (Which of its long sides is to the camera: +1, its left.)
	const toCam =
		(cam.pos.y - at.y) * cos - (cam.pos.x - at.x) * sin > 0 ? 1 : -1;
	const nearSide = (b: Box) => (b.s[0] + b.s[1]) * toCam > 0;
	const wheels: Box[] = [-WHEELS_AT, WHEELS_AT].flatMap((f) =>
		[-half, half - WHEEL_W].map((s): Box => ({
			f: [f - WHEEL_R, f + WHEEL_R],
			s: [s, s + WHEEL_W],
			z: [0, WHEEL_R * 1.7],
		})),
	);
	const chassis: Box = {
		f: [-CART_L / 2, CART_L / 2],
		s: [-half + 0.1, half - 0.1],
		z: [0.7, DECK_Z],
	};
	const hood: Box = {
		f: [CART_L / 2 - HOOD_L, CART_L / 2],
		s: [-half + 0.1, half - 0.1],
		z: [DECK_Z, HOOD_Z],
	};
	const seatBack: Box = {
		f: [SEAT_BACK[0], SEAT_BACK[1]],
		s: [-half + 0.2, half - 0.2],
		z: [DECK_Z, SEAT_BACK_Z],
	};
	const seat: Box = {
		f: [SEAT_BACK[1], CART_L / 2 - HOOD_L],
		s: [-half + 0.2, half - 0.2],
		z: [DECK_Z, DECK_Z + 0.25],
	};
	const bands: Box[] = [
		{
			f: [-CART_L / 2, CART_L / 2],
			s: [-half + 0.05, -half + 0.1],
			z: [BAND_Z[0], BAND_Z[1]],
		},
		{
			f: [-CART_L / 2, CART_L / 2],
			s: [half - 0.1, half - 0.05],
			z: [BAND_Z[0], BAND_Z[1]],
		},
	];
	const posts: Box[] = [ROOF[0] + 0.1, ROOF[1] - 0.3].flatMap((f) =>
		[-half + 0.15, half - 0.15 - POST].map((s): Box => ({
			f: [f, f + POST],
			s: [s, s + POST],
			z: [DECK_Z, ROOF_Z],
		})),
	);
	const roof: Box = {
		f: [ROOF[0], ROOF[1]],
		s: [-half, half],
		z: [ROOF_Z, ROOF_Z + ROOF_T],
	};
	// (Under the men on it - the chassis drawn as from its far end - and the
	// roof over them, drawn as from its near end; and, each where it is
	// among them, the seat's back and the hood - and its side to the
	// camera, in front of the driver.)
	const far = Math.max(
		...[-1, 1].flatMap((f) =>
			[-1, 1].map((s) => depthOf(cam, w((f * CART_L) / 2, s * half, 0))),
		),
	);
	const near = Math.min(
		...[ROOF[0], ROOF[1]].flatMap((f) =>
			[-1, 1].map((s) => depthOf(cam, w(f, s * half, 0))),
		),
	);
	const sorted = (parts: [Box, string, number?][]) => {
		parts.sort((a, b) => by(b[0]) - by(a[0]));
		for (const [b, fill, faces] of parts) {
			box(b, fill, faces);
		}
	};
	const nearFace = toCam > 0 ? LEFT : RIGHT;
	return [
		{
			depth: far,
			draw: () => {
				// (Its shadow on the turf under it.)
				ctx.globalAlpha = 0.28;
				quad(
					[
						w(-CART_L / 2 - 0.3, -half - 0.3, 0),
						w(CART_L / 2 + 0.3, -half - 0.3, 0),
						w(CART_L / 2 + 0.3, half + 0.3, 0),
						w(-CART_L / 2 - 0.3, half + 0.3, 0),
					],
					"#000",
				);
				ctx.globalAlpha = 1;
				sorted([
					...wheels
						.filter((b) => !nearSide(b))
						.map((b): [Box, string] => [b, TIRE]),
					[chassis, BODY, ALL & ~nearFace],
					[seat, DARK],
					...bands
						.filter((b) => !nearSide(b))
						.map((b): [Box, string] => [b, team]),
					...posts
						.filter((b) => !nearSide(b))
						.map((b): [Box, string] => [b, DARK]),
				]);
			},
		},
		{ depth: by(seatBack), draw: () => box(seatBack, DARK) },
		{ depth: by(hood), draw: () => box(hood, BODY) },
		{
			depth: depthOf(cam, w(0, toCam * half, 0)) - 0.05,
			draw: () => {
				box(chassis, BODY, nearFace);
				sorted([
					...bands
						.filter((b) => nearSide(b))
						.map((b): [Box, string] => [b, team]),
					...wheels
						.filter((b) => nearSide(b))
						.map((b): [Box, string] => [b, TIRE]),
					...posts
						.filter((b) => nearSide(b))
						.map((b): [Box, string] => [b, DARK]),
				]);
			},
		},
		{
			depth: near - 0.5,
			draw: () => box(roof, DARK),
		},
	];
};
