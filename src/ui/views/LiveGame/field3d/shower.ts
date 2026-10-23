import { depthOf, project, type Camera } from "../court3d/camera.ts";
import { skeleton, type Body } from "../court3d/poses.ts";
import { makeCourtRng } from "../courtRng.ts";
import { FIGURE_SCALE } from "./body.ts";
import type { CrewPart } from "./crew.ts";
import type { Leg } from "./director.ts";
import type { FieldMoment } from "./evaluate.ts";
import { FIELD_W, type Pt, type Pt3, type Side } from "./geometry.ts";
import { poseOfMove, type Move } from "./poses.ts";
import { bodyPoint } from "./sculpt.ts";
import { drawnPose, type FieldPlayerState } from "./sprite.ts";

// THE SHOWER. The game won, with no doubt about it by the end - and two of
// the winners' men off the field creep up behind their head coach with the
// big orange cooler between them, a handle each. The game over, up to
// their chests with it, over their heads, and over him it goes, all of it:
// him hunched under it, wiping it out of his eyes, laughing. Them up off
// their feet, and out onto the field after the rest.

// How often: the game won by this much at least, or knelt out.
export const SHOWER = 0.5;
export const SHOWER_MARGIN = 9;
// The cooler (feet, drawn to the size of the men - see body.ts): how big
// round and how tall; their hands on its handles this far up it.
const R = 0.68 * FIGURE_SCALE;
const H = 1.85 * FIGURE_SCALE;
const HANDLE_AT = 0.82;
// The two of them either side of it this far apart, the cooler this far
// behind him - on the near sideline, the camera behind them, wider and in
// closer either side of him, so he shows between them; from this far off
// the field, at a creep, there this long before the whistle at least.
const SPAN = 3.9;
const BEHIND = 2.6;
const SPAN_NEAR = 4.6;
const BEHIND_NEAR = 1.5;
const START_OFF = 23;
const SNEAK_FPS = 3.3;
const READY = 700;
// Up with it this long after the game is over (the picture on them by
// then), over this long - in it (shares of it), tipped from, to, and on
// over as it pours (radians); pouring from, to; let go.
const AT = 900;
const DUMP_MS = 1900;
const TIP_FROM = 0.5;
const TIP_TO = 0.64;
const TIPPED = 2.2;
const TIPPED_MOST = 2.5;
const POUR_FROM = 0.56;
const POUR_TO = 0.86;
const LET_GO = 0.86;
// Let go: down onto its side, coming this far on as it falls.
const DRIFT = 0.6;
const G = 32;
// Him under it from this far into the dump, this long - the picture on him
// until this share of the way through it.
const HIT = 0.6;
const DOUSE_MS = 2300;
const WATCHED = 0.85;
// Then each of them up off his feet or a fist pumped, and out onto the
// field this far, this far wide of the cooler, at a walk.
const OUT_IN: [number, number] = [16, 26];
const OUT_WIDE: [number, number] = [4, 9];
const OUT_FPS = 4.6;
// What is in it.
const FLAVORS = [
	"#f7931e",
	"#f7931e",
	"#f7931e",
	"#e6e23a",
	"#e6e23a",
	"#3ca5f0",
	"#3ca5f0",
	"#e3393f",
	"#8c4fc9",
];
// The pour: so many drops of it, the first of them this long to reach him
// (and up to this much longer), off him as fast as this (feet a second);
// so many of them ice.
const DROPS = 130;
// Its stream out of the mouth onto him: this thick (feet) at its heaviest,
// this long getting to him.
const STREAM = 0.7 * FIGURE_SCALE;
const STREAM_MS = 150;
const STREAM_STEPS = 10;
const ICE = 12;
const FALL_MS: [number, number] = [90, 230];
const SPLASH: [number, number] = [2, 6.5];
const MISS = 0.18;

export type Shower = {
	team: Side;
	coach: number;
	// The two of them, the first with it on his right, the second on his
	// left - where they stand for it, faced the way he is.
	men: [number, number];
	spots: [Pt, Pt];
	yaw: number;
	// Where he is; when they lift it; when the picture is done with them.
	at: Pt;
	dump: number;
	until: number;
	flavor: string;
};

const plus = (a: Pt, b: Pt, k: number): Pt => ({
	x: a.x + b.x * k,
	y: a.y + b.y * k,
});

// The shower for the head coach (his legs: the first, stood where he is
// from the start of the play, cut to it), with these two men: where they
// stand at the start of the play (`t0`), the game over at `from`.
export const stageShower = (input: {
	men: [number, number];
	team: Side;
	coach: number;
	legs: Leg[];
	at: Pt;
	yaw: number;
	t0: number;
	whistle: number;
	from: number;
	r: () => number;
}): { parts: CrewPart[]; shower: Shower; free: number } => {
	const { men, team, at, yaw, t0, r } = input;
	const fwd = { x: Math.cos(yaw), y: Math.sin(yaw) };
	const left = { x: Math.sin(yaw), y: -Math.cos(yaw) };
	const near = at.y > FIELD_W;
	const span = near ? SPAN_NEAR : SPAN;
	const cooler = plus(at, fwd, -(near ? BEHIND_NEAR : BEHIND));
	const off = near ? cooler.y - FIELD_W : -cooler.y;
	const sneak = Math.max(4, START_OFF - off);
	const spots: [Pt, Pt] = [
		plus(cooler, left, span / 2),
		plus(cooler, left, -span / 2),
	];
	const walkMs = (sneak / SNEAK_FPS) * 1000;
	const go = Math.max(t0 + 200, input.whistle - READY - walkMs);
	const there = go + walkMs;
	const dump = Math.max(input.from + AT, there + 300);
	const done = dump + DUMP_MS;
	const parts: CrewPart[] = men.map((pid, k) => {
		const spot = spots[k]!;
		const start = plus(spot, fwd, -sneak);
		const mirrored = k === 1;
		const legs: Leg[] = [];
		const push = (l: Omit<Leg, "mirrored">) => {
			if (l.t1 > l.t0) {
				legs.push({ ...l, ...(mirrored ? { mirrored: true } : {}) });
			}
		};
		push({ t0, t1: go, from: start, to: start, move: "coolerCarry", yaw });
		push({
			t0: go,
			t1: there,
			from: start,
			to: spot,
			move: "walk",
			yaw,
			arms: "coolerCarry",
		});
		push({
			t0: there,
			t1: dump,
			from: spot,
			to: spot,
			move: "coolerCarry",
			yaw,
		});
		push({ t0: dump, t1: done, from: spot, to: spot, move: "coolerDump", yaw });
		// (Up off his feet, or a fist pumped - and out onto the field.)
		const glad: Move = r() < 0.6 ? "leap" : "fistPump";
		const gladMs = glad === "leap" ? 800 : 1300;
		const wait = done + 150 + r() * 300;
		push({ t0: done, t1: wait, from: spot, to: spot, move: "stand", yaw });
		push({
			t0: wait,
			t1: wait + gladMs,
			from: spot,
			to: spot,
			move: glad,
			yaw,
		});
		const out = plus(
			plus(spot, fwd, OUT_IN[0] + r() * (OUT_IN[1] - OUT_IN[0])),
			left,
			(mirrored ? -1 : 1) * (OUT_WIDE[0] + r() * (OUT_WIDE[1] - OUT_WIDE[0])),
		);
		const leave = wait + gladMs;
		push({
			t0: leave,
			t1: leave + (Math.hypot(out.x - spot.x, out.y - spot.y) / OUT_FPS) * 1000,
			from: spot,
			to: out,
			move: "walk",
		});
		return { pid, team, legs };
	});
	// Him, under it.
	const hit = dump + DUMP_MS * HIT;
	const first = input.legs[0]!;
	first.t1 = Math.max(first.t0, hit);
	input.legs.push({
		t0: first.t1,
		t1: first.t1 + DOUSE_MS,
		from: at,
		to: at,
		move: "doused",
		yaw,
	});
	return {
		parts,
		shower: {
			team,
			coach: input.coach,
			men,
			spots,
			yaw,
			at,
			dump,
			until: hit + DOUSE_MS * WATCHED,
			flavor: FLAVORS[Math.floor(r() * FLAVORS.length)]!,
		},
		free: first.t1 + DOUSE_MS,
	};
};

// ---- as the camera sees it --------------------------------------------------

const ORANGE = "#f27b1c";
const ORANGE_DARK = "#b3520d";
const RIM = "#f2f0e8";
const EMPTY = "#4a2307";
const ICE_COLOR = "#eef6fb";
const MIN_PX = 1;
const RING = 16;

type Item = { depth: number; draw: () => void };
type Cooler = { bottom: Pt3; axis: Pt3; hands?: [Pt3, Pt3] };

const smooth = (u: number) => {
	const c = Math.min(1, Math.max(0, u));
	return c * c * (3 - 2 * c);
};
// How far over it is tipped, a share of the way into the dump.
const tipAt = (u: number) =>
	u < TIP_TO
		? TIPPED * smooth((u - TIP_FROM) / (TIP_TO - TIP_FROM))
		: TIPPED +
			(TIPPED_MOST - TIPPED) * smooth((u - TIP_TO) / (LET_GO - TIP_TO));

// A man's hand on the cooler as he is drawn - his right, or (the second
// man) his left.
const handOf = (st: FieldPlayerState, body: Body, second: boolean): Pt3 => {
	const { pose, yaw } = drawnPose(st);
	const sk = skeleton(body, pose);
	return bodyPoint(
		{ x: st.x, y: st.y, z: st.z, yaw },
		second ? sk.armL.end : sk.armR.end,
	);
};

// The cooler between their hands, tipped this far over (toward `fwd`).
const between = (a: Pt3, b: Pt3, tip: number, fwd: Pt): Cooler => {
	const axis = {
		x: fwd.x * Math.sin(tip),
		y: fwd.y * Math.sin(tip),
		z: Math.cos(tip),
	};
	const k = HANDLE_AT * H;
	return {
		bottom: {
			x: (a.x + b.x) / 2 - axis.x * k,
			y: (a.y + b.y) / 2 - axis.y * k,
			z: (a.z + b.z) / 2 - axis.z * k,
		},
		axis,
		hands: [a, b],
	};
};

// The two of them at their places, partway into the dump, as drawn.
const inDump = (sh: Shower, u: number, k: 0 | 1): FieldPlayerState => ({
	id: sh.men[k],
	team: sh.team,
	x: sh.spots[k].x,
	y: sh.spots[k].y,
	z: 0,
	yaw: sh.yaw,
	move: "coolerDump",
	phase: u,
	...(k === 1 ? { mirrored: true as const } : {}),
});

const dumpCooler = (sh: Shower, u: number, bodies: [Body, Body]): Cooler => {
	const fwd = { x: Math.cos(sh.yaw), y: Math.sin(sh.yaw) };
	return between(
		handOf(inDump(sh, u, 0), bodies[0], false),
		handOf(inDump(sh, u, 1), bodies[1], true),
		tipAt(u),
		fwd,
	);
};

const centerOf = (c: Cooler): Pt3 => ({
	x: c.bottom.x + (c.axis.x * H) / 2,
	y: c.bottom.y + (c.axis.y * H) / 2,
	z: c.bottom.z + (c.axis.z * H) / 2,
});

// The cooler at t: carried, lifted and tipped, let go and down on its side.
const coolerAt = (
	sh: Shower,
	t: number,
	a: FieldPlayerState | undefined,
	b: FieldPlayerState | undefined,
	bodies: [Body, Body],
): Cooler | null => {
	const fwd = { x: Math.cos(sh.yaw), y: Math.sin(sh.yaw) };
	const u = (t - sh.dump) / DUMP_MS;
	if (u < LET_GO) {
		if (!a || !b) {
			return null;
		}
		return between(
			handOf(a, bodies[0], false),
			handOf(b, bodies[1], true),
			u > 0 ? tipAt(u) : 0,
			fwd,
		);
	}
	// (Let go: down, turning as it falls, onto its side - along the
	// sideline, its mouth toward the first of them.)
	const was = dumpCooler(sh, LET_GO, bodies);
	const c = centerOf(was);
	const fall = Math.sqrt((2 * Math.max(0, c.z - R)) / G);
	const s = (t - sh.dump - DUMP_MS * LET_GO) / 1000;
	const v = smooth(fall > 0 ? s / fall : 1);
	const lie = { x: Math.sin(sh.yaw), y: -Math.cos(sh.yaw), z: 0 };
	const turned = {
		x: was.axis.x + (lie.x - was.axis.x) * v,
		y: was.axis.y + (lie.y - was.axis.y) * v,
		z: was.axis.z + (lie.z - was.axis.z) * v,
	};
	const l = Math.hypot(turned.x, turned.y, turned.z) || 1;
	const axis = { x: turned.x / l, y: turned.y / l, z: turned.z / l };
	const mid = {
		x: c.x + fwd.x * DRIFT * v,
		y: c.y + fwd.y * DRIFT * v,
		z: Math.max(R, c.z - 0.5 * G * s * s),
	};
	return {
		bottom: {
			x: mid.x - (axis.x * H) / 2,
			y: mid.y - (axis.y * H) / 2,
			z: mid.z - (axis.z * H) / 2,
		},
		axis,
	};
};

// The pour, worked out once: each drop out of its mouth when, from where,
// onto him where and when - and off him how (or onto the turf).
type Drop = {
	t0: number;
	from: Pt3;
	v: Pt3;
	t1: number;
	hit: Pt3;
	off?: Pt3;
	ice: boolean;
};
const pours = new WeakMap<Shower, Drop[]>();
const pourOf = (sh: Shower, bodies: [Body, Body], coach: Body): Drop[] => {
	let drops = pours.get(sh);
	if (drops) {
		return drops;
	}
	drops = [];
	const r = makeCourtRng(`shower|${sh.coach}|${sh.dump}`);
	const head = skeleton(coach, poseOfMove("stand", 0)).head;
	const placed = { x: sh.at.x, y: sh.at.y, z: 0, yaw: sh.yaw };
	for (let i = 0; i < DROPS; i++) {
		const u = POUR_FROM + ((POUR_TO - POUR_FROM) * (i + r())) / DROPS;
		const c = dumpCooler(sh, u, bodies);
		const mouth = (k: number, a: number) => {
			// (A point across its mouth.)
			const e = across(c.axis);
			return {
				x:
					c.bottom.x +
					c.axis.x * H +
					(e[0].x * Math.cos(a) + e[1].x * Math.sin(a)) * k,
				y:
					c.bottom.y +
					c.axis.y * H +
					(e[0].y * Math.cos(a) + e[1].y * Math.sin(a)) * k,
				z:
					c.bottom.z +
					c.axis.z * H +
					(e[0].z * Math.cos(a) + e[1].z * Math.sin(a)) * k,
			};
		};
		const from = mouth(R * 0.8 * Math.sqrt(r()), r() * Math.PI * 2);
		const t0 = sh.dump + DUMP_MS * u;
		const miss = r() < MISS;
		// (Onto his head and his back - or past him onto the turf.)
		const hit = miss
			? bodyPoint(placed, {
					f: -0.9 + r() * 0.6,
					s: (r() - 0.5) * 2.4,
					u: 0,
				})
			: bodyPoint(placed, {
					f: head.f - 0.6 + r() * 1.1,
					s: (r() - 0.5) * 1.8,
					u: head.u - 1.9 + r() * 2.2,
				});
		const ms = miss
			? Math.sqrt((2 * Math.max(0.1, from.z)) / G) * 1000
			: FALL_MS[0] + r() * (FALL_MS[1] - FALL_MS[0]);
		const T = ms / 1000;
		const v = {
			x: (hit.x - from.x) / T,
			y: (hit.y - from.y) / T,
			z: (hit.z - from.z + 0.5 * G * T * T) / T,
		};
		let off: Pt3 | undefined;
		if (!miss) {
			const dx = hit.x - sh.at.x;
			const dy = hit.y - sh.at.y;
			const l = Math.hypot(dx, dy) || 1;
			const k = SPLASH[0] + r() * (SPLASH[1] - SPLASH[0]);
			off = {
				x: (dx / l) * k + (r() - 0.5) * 2,
				y: (dy / l) * k + (r() - 0.5) * 2,
				z: r() * 2.5,
			};
		}
		drops.push({
			t0,
			from,
			v,
			t1: t0 + ms,
			hit,
			...(off ? { off } : {}),
			ice: i % Math.round(DROPS / ICE) === 0,
		});
	}
	pours.set(sh, drops);
	return drops;
};

// Where a drop is at t (or nowhere: not out yet, or down on the turf).
const dropAt = (d: Drop, t: number): Pt3 | null => {
	if (t < d.t0) {
		return null;
	}
	if (t < d.t1) {
		const s = (t - d.t0) / 1000;
		return {
			x: d.from.x + d.v.x * s,
			y: d.from.y + d.v.y * s,
			z: d.from.z + d.v.z * s - 0.5 * G * s * s,
		};
	}
	if (!d.off) {
		return null;
	}
	const s = (t - d.t1) / 1000;
	const z = d.hit.z + d.off.z * s - 0.5 * G * s * s;
	return z > 0
		? { x: d.hit.x + d.off.x * s, y: d.hit.y + d.off.y * s, z }
		: null;
};

// Two lines square to `a`, and to each other.
const across = (a: Pt3): [Pt3, Pt3] => {
	const ref =
		Math.abs(a.z) > 0.95 ? { x: 1, y: 0, z: 0 } : { x: 0, y: 0, z: 1 };
	let e = {
		x: a.y * ref.z - a.z * ref.y,
		y: a.z * ref.x - a.x * ref.z,
		z: a.x * ref.y - a.y * ref.x,
	};
	const l = Math.hypot(e.x, e.y, e.z) || 1;
	e = { x: e.x / l, y: e.y / l, z: e.z / l };
	return [
		e,
		{
			x: a.y * e.z - a.z * e.y,
			y: a.z * e.x - a.x * e.z,
			z: a.x * e.y - a.y * e.x,
		},
	];
};

type P2 = { x: number; y: number };
const areaOf = (ps: P2[]) => {
	let s = 0;
	for (let i = 0; i < ps.length; i++) {
		const a = ps[i]!;
		const b = ps[(i + 1) % ps.length]!;
		s += a.x * b.y - b.x * a.y;
	}
	return s;
};
const hullOf = (ps: P2[]): P2[] => {
	const pts = [...ps].sort((a, b) => a.x - b.x || a.y - b.y);
	const cross = (o: P2, a: P2, b: P2) =>
		(a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
	const lower: P2[] = [];
	for (const p of pts) {
		while (lower.length >= 2 && cross(lower.at(-2)!, lower.at(-1)!, p) <= 0) {
			lower.pop();
		}
		lower.push(p);
	}
	const upper: P2[] = [];
	for (const p of pts.reverse()) {
		while (upper.length >= 2 && cross(upper.at(-2)!, upper.at(-1)!, p) <= 0) {
			upper.pop();
		}
		upper.push(p);
	}
	return [...lower.slice(0, -1), ...upper.slice(0, -1)];
};
const fill = (
	ctx: CanvasRenderingContext2D,
	ps: P2[],
	color: string,
	edge?: string,
	width = 1,
) => {
	ctx.beginPath();
	ps.forEach((p, i) => (i === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y)));
	ctx.closePath();
	ctx.fillStyle = color;
	ctx.fill();
	if (edge) {
		ctx.strokeStyle = edge;
		ctx.lineWidth = width;
		ctx.stroke();
	}
};

const drawCooler = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	c: Cooler,
	inside: string,
) => {
	const [e1, e2] = across(c.axis);
	const ring = (o: Pt3) =>
		Array.from({ length: RING }, (_, i) => {
			const a = (i / RING) * Math.PI * 2;
			return project(cam, {
				x: o.x + (e1.x * Math.cos(a) + e2.x * Math.sin(a)) * R,
				y: o.y + (e1.y * Math.cos(a) + e2.y * Math.sin(a)) * R,
				z: o.z + (e1.z * Math.cos(a) + e2.z * Math.sin(a)) * R,
			});
		});
	const top = {
		x: c.bottom.x + c.axis.x * H,
		y: c.bottom.y + c.axis.y * H,
		z: c.bottom.z + c.axis.z * H,
	};
	const lo = ring(c.bottom);
	const hi = ring(top);
	const k = project(cam, top).k;
	// (Which end of it faces the camera: as a ring round a cooler stood up
	// goes, its mouth.)
	const [u1, u2] = across({ x: 0, y: 0, z: 1 });
	const up = Array.from({ length: 4 }, (_, i) => {
		const a = (i / 4) * Math.PI * 2;
		return project(cam, {
			x: top.x + (u1.x * Math.cos(a) + u2.x * Math.sin(a)) * R,
			y: top.y + (u1.y * Math.cos(a) + u2.y * Math.sin(a)) * R,
			z: top.z + (u1.z * Math.cos(a) + u2.z * Math.sin(a)) * R,
		});
	});
	const mouthShows = Math.sign(areaOf(hi)) === Math.sign(areaOf(up));
	ctx.save();
	ctx.lineJoin = "round";
	fill(ctx, hullOf([...lo, ...hi]), ORANGE, ORANGE_DARK, Math.max(1, 0.08 * k));
	if (mouthShows) {
		fill(ctx, hi, inside, RIM, Math.max(MIN_PX, 0.14 * k));
	} else {
		fill(ctx, lo, ORANGE_DARK);
	}
	// (Its handles, up to their hands.)
	if (c.hands) {
		const k2 = HANDLE_AT * H;
		const at = {
			x: c.bottom.x + c.axis.x * k2,
			y: c.bottom.y + c.axis.y * k2,
			z: c.bottom.z + c.axis.z * k2,
		};
		ctx.strokeStyle = RIM;
		ctx.lineWidth = Math.max(MIN_PX, 0.1 * k);
		ctx.lineCap = "round";
		for (const h of c.hands) {
			const d = { x: h.x - at.x, y: h.y - at.y, z: h.z - at.z };
			const l = Math.hypot(d.x, d.y, d.z) || 1;
			const side = project(cam, {
				x: at.x + (d.x / l) * R,
				y: at.y + (d.y / l) * R,
				z: at.z + (d.z / l) * R,
			});
			const hand = project(cam, h);
			ctx.beginPath();
			ctx.moveTo(side.x, side.y);
			ctx.lineTo(hand.x, hand.y);
			ctx.stroke();
		}
	}
	ctx.restore();
};

// One stood up where it is kept (its bottom at `bottom`), the lid on.
export const drawStandingCooler = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	bottom: Pt3,
) => drawCooler(ctx, cam, { bottom, axis: { x: 0, y: 0, z: 1 } }, RIM);

// The stream from its mouth onto the back of his head: each bit of it out
// of the mouth a moment ago, as thick as the pour was then.
const streamOf = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	sh: Shower,
	t: number,
	c: Cooler,
	coach: Body,
	pourFrom: number,
	pourTo: number,
): Item => {
	const head = skeleton(coach, poseOfMove("stand", 0)).head;
	const land = bodyPoint(
		{ x: sh.at.x, y: sh.at.y, z: 0, yaw: sh.yaw },
		{ f: head.f - 0.35, s: 0, u: head.u + 0.15 },
	);
	const mouth = {
		x: c.bottom.x + c.axis.x * H,
		y: c.bottom.y + c.axis.y * H,
		z: c.bottom.z + c.axis.z * H,
	};
	const out = {
		x: mouth.x + c.axis.x * 0.6,
		y: mouth.y + c.axis.y * 0.6,
		z: mouth.z + c.axis.z * 0.6,
	};
	const pts: { p: Pt3; w: number }[] = [];
	for (let j = 0; j <= STREAM_STEPS; j++) {
		const v = j / STREAM_STEPS;
		const when = t - v * STREAM_MS;
		const flow =
			when < pourFrom || when > pourTo
				? 0
				: Math.sin((Math.PI * (when - pourFrom)) / (pourTo - pourFrom)) ** 0.6;
		const a = (1 - v) * (1 - v);
		const b = 2 * v * (1 - v);
		const d = v * v;
		pts.push({
			p: {
				x: a * mouth.x + b * out.x + d * land.x,
				y: a * mouth.y + b * out.y + d * land.y,
				z: a * mouth.z + b * out.z + d * land.z,
			},
			w: STREAM * flow * (1 + 0.5 * v),
		});
	}
	const mid = pts[Math.floor(STREAM_STEPS / 2)]!.p;
	return {
		depth: depthOf(cam, { ...mid, z: 0 }),
		draw: () => {
			const at = pts.map((q) => project(cam, q.p));
			ctx.save();
			ctx.lineCap = "round";
			for (const [alpha, share, color] of [
				[0.6, 1, sh.flavor],
				[0.7, 0.4, lighter(sh.flavor)],
			] as const) {
				ctx.globalAlpha = alpha;
				ctx.strokeStyle = color;
				for (let j = 0; j < STREAM_STEPS; j++) {
					const w = (pts[j]!.w + pts[j + 1]!.w) / 2;
					if (w <= 0) {
						continue;
					}
					ctx.lineWidth = Math.max(MIN_PX, w * share * at[j]!.k);
					ctx.beginPath();
					ctx.moveTo(at[j]!.x, at[j]!.y);
					ctx.lineTo(at[j + 1]!.x, at[j + 1]!.y);
					ctx.stroke();
				}
			}
			ctx.restore();
		},
	};
};

// A color halfway to white.
const lighter = (hex: string) => {
	const n = Number.parseInt(hex.slice(1), 16);
	const ch = (k: number) => Math.round(((n >> k) & 255) * 0.5 + 127.5);
	return `rgb(${ch(16)}, ${ch(8)}, ${ch(0)})`;
};

export const showerGear = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	m: FieldMoment,
	bodyOf: (pid: number) => Body,
): Item[] => {
	const sh = m.play?.postgame?.shower;
	if (!sh) {
		return [];
	}
	const t = m.t;
	const bodies: [Body, Body] = [bodyOf(sh.men[0]), bodyOf(sh.men[1])];
	const a = m.players.find((p) => p.id === sh.men[0]);
	const b = m.players.find((p) => p.id === sh.men[1]);
	const items: Item[] = [];
	const c = coolerAt(sh, t, a, b, bodies);
	if (c) {
		const poured = t > sh.dump + DUMP_MS * POUR_FROM;

		items.push({
			depth: depthOf(cam, { ...centerOf(c), z: 0 }),
			draw: () => {
				drawCooler(ctx, cam, c, poured ? EMPTY : sh.flavor);
			},
		});
	}
	const pourFrom = sh.dump + DUMP_MS * POUR_FROM;
	const pourTo = sh.dump + DUMP_MS * POUR_TO;
	if (t < pourFrom) {
		return items;
	}
	const coach = bodyOf(sh.coach);
	if (c && t < pourTo + STREAM_MS) {
		items.push(streamOf(ctx, cam, sh, t, c, coach, pourFrom, pourTo));
	}
	for (const d of pourOf(sh, bodies, coach)) {
		const p = dropAt(d, t);
		if (!p) {
			continue;
		}
		const q = dropAt(d, t - 30) ?? p;
		items.push({
			depth: depthOf(cam, { ...p, z: 0 }),
			draw: () => {
				const a2 = project(cam, p);
				const b2 = project(cam, q);
				ctx.save();
				ctx.lineCap = d.ice ? "square" : "round";
				ctx.globalAlpha = d.ice ? 0.95 : 0.8;
				ctx.strokeStyle = d.ice ? ICE_COLOR : sh.flavor;
				ctx.lineWidth = Math.max(MIN_PX, (d.ice ? 0.22 : 0.17) * a2.k);
				ctx.beginPath();
				ctx.moveTo(b2.x, b2.y);
				ctx.lineTo(a2.x + 0.01, a2.y);
				ctx.stroke();
				ctx.restore();
			},
		});
	}
	return items;
};
