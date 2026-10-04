import type {
	Act,
	BallSeg,
	FxKind,
	Fx,
	CourtTimeline,
	Track,
} from "./director.ts";
import { rimX, type Pt, type Pt3, type Side } from "./geometry.ts";
import {
	ANIMS,
	poseAt,
	skeleton,
	type AnimName,
	type Body,
	type V3,
} from "./poses.ts";

// WHERE EVERYTHING IS AT A MOMENT.
//
// The director wrote down intentions over time; this reads them back at any
// instant t. Pure, so the renderer can ask about any frame (and a seek can jump
// anywhere) and the tests can sample a whole game.

const clamp01 = (u: number) => Math.min(1, Math.max(0, u));
const ease = (u: number) => 0.5 - 0.5 * Math.cos(Math.PI * clamp01(u));

// Index of the last item with key <= t (items sorted by key), or -1.
const lastIndex = <T>(list: T[], t: number, key: (x: T) => number): number => {
	let lo = 0;
	let hi = list.length - 1;
	let ans = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (key(list[mid]!) <= t) {
			ans = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return ans;
};

export type PlayerState = {
	pid: number;
	team: Side;
	shown: boolean;
	x: number;
	y: number;
	z: number;
	// Which way he faces on the floor, radians (0 = toward the right rim).
	yaw: number;
	anim: AnimName;
	// How far through the animation (0 to 1).
	phase: number;
	moving: boolean;
};

export const offenseAt = (tl: CourtTimeline, t: number): Side => {
	const i = lastIndex(tl.poss, t, (p) => p[0]);
	return i >= 0 ? tl.poss[i]![1] : 1;
};

const ballSegAt = (tl: CourtTimeline, t: number): BallSeg | undefined => {
	const i = lastIndex(tl.ball, t, (s) => s.t0);
	return i >= 0 ? tl.ball[i] : tl.ball[0];
};

const jumpZ = (act: Act, u: number): number => {
	if (act.zKeys) {
		const k = act.zKeys;
		for (let i = 1; i < k.length; i++) {
			const [u1, z1] = k[i]!;
			if (u <= u1) {
				const [u0, z0] = k[i - 1]!;
				const f = u1 === u0 ? 1 : (u - u0) / (u1 - u0);
				// Up fast and decelerating, down accelerating - a jump, not a lift.
				const g = z1 >= z0 ? Math.sin((f * Math.PI) / 2) : f * f;
				return z0 + (z1 - z0) * g;
			}
		}
		return 0;
	}
	if (!act.jump) {
		return 0;
	}
	const [a, b, peak] = act.jump;
	const v = (u - a) / (b - a);
	return v <= 0 || v >= 1 ? 0 : 4 * peak * v * (1 - v);
};

// Where he is on the floor and what his feet are doing, without asking which
// way he faces (which depends on where the ball is - see yawAt).
type Spot = {
	x: number;
	y: number;
	moveIndex: number;
	moving: boolean;
	traveled: number;
};

const spotAt = (tr: Track, t: number): Spot => {
	const mi = lastIndex(tr.moves, t, (m) => m.t0);
	const mv = mi >= 0 ? tr.moves[mi] : undefined;
	if (!mv) {
		return {
			x: tr.start.x,
			y: tr.start.y,
			moveIndex: -1,
			moving: false,
			traveled: 0,
		};
	}
	if (t < mv.t1) {
		const e = ease((t - mv.t0) / (mv.t1 - mv.t0));
		return {
			x: mv.from.x + (mv.to.x - mv.from.x) * e,
			y: mv.from.y + (mv.to.y - mv.from.y) * e,
			moveIndex: mi,
			moving: true,
			traveled: Math.hypot(mv.to.x - mv.from.x, mv.to.y - mv.from.y) * e,
		};
	}
	return {
		x: mv.to.x,
		y: mv.to.y,
		moveIndex: mi,
		moving: false,
		traveled: 0,
	};
};

// The act running now, if any (they rarely overlap; the later one wins).
const actAt = (tr: Track, t: number): Act | undefined => {
	const ai = lastIndex(tr.acts, t, (a) => a.t0);
	for (let k = ai; k >= 0 && k >= ai - 3; k--) {
		const a = tr.acts[k]!;
		if (t < a.t1) {
			return a;
		}
	}
	return undefined;
};

// Roughly where the ball is - a holder's spot rather than his hands - for
// deciding which way people look. (The hands depend on which way he looks.)
const ballNear = (tl: CourtTimeline, t: number): Pt => {
	const seg = ballSegAt(tl, t);
	const at = (p: Pt3 | { pid: number }, when: number): Pt => {
		if ("pid" in p) {
			const tr = tl.tracks.get(p.pid);
			return tr ? spotAt(tr, when) : { x: 47, y: 25 };
		}
		return p;
	};
	if (!seg) {
		return { x: 47, y: 25 };
	}
	if (seg.kind === "hold") {
		return at({ pid: seg.pid }, t);
	}
	if (seg.kind === "fly") {
		const a = at(seg.from, seg.t0);
		const b = at(seg.to, seg.t1);
		const u = clamp01((t - seg.t0) / (seg.t1 - seg.t0));
		return { x: a.x + (b.x - a.x) * u, y: a.y + (b.y - a.y) * u };
	}
	if (seg.kind === "bounce") {
		const u = clamp01((t - seg.t0) / (seg.t1 - seg.t0));
		return {
			x: seg.from.x + (seg.to.x - seg.from.x) * u,
			y: seg.from.y + (seg.to.y - seg.from.y) * u,
		};
	}
	return seg.at;
};

const angleTo = (from: Pt, to: Pt, fallback: number): number => {
	const dx = to.x - from.x;
	const dy = to.y - from.y;
	return dx * dx + dy * dy < 0.01 ? fallback : Math.atan2(dy, dx);
};

const unit = (from: Pt, to: Pt): Pt => {
	const dx = to.x - from.x;
	const dy = to.y - from.y;
	const l = Math.hypot(dx, dy) || 1;
	return { x: dx / l, y: dy / l };
};

// Which way he means to face at t: at what he is doing (a shot faces the rim),
// where he is running, or - standing - at the ball.
const yawTarget = (tl: CourtTimeline, tr: Track, t: number): number => {
	const here = spotAt(tr, t);
	const fi = lastIndex(tr.faces, t, (f) => f[0]);
	const face = fi >= 0 ? tr.faces[fi]![1] : 1;
	const fallback = face === 1 ? 0 : Math.PI;
	const act = actAt(tr, t);
	if (act?.look) {
		return angleTo(here, act.look, fallback);
	}
	const mv = here.moveIndex >= 0 ? tr.moves[here.moveIndex] : undefined;
	if (mv && here.moving) {
		const dx = mv.to.x - mv.from.x;
		const dy = mv.to.y - mv.from.y;
		const d = Math.hypot(dx, dy);
		if (d >= 3) {
			const heading = Math.atan2(dy, dx);
			if (mv.anim === "back") {
				return heading + Math.PI;
			}
			// Told to face against where he is going: a slide with his man, or a
			// backpedal - he keeps his eyes on the ball.
			if (mv.face !== undefined && mv.face * dx < 0 && d < 14) {
				return angleTo(here, ballNear(tl, t), fallback);
			}
			return heading;
		}
	}
	const li = lastIndex(tr.looks, t, (l) => l[0]);
	if (li >= 0 && (!mv || tr.looks[li]![0] >= mv.t0)) {
		return angleTo(here, tr.looks[li]![1], fallback);
	}
	const ball = ballNear(tl, t);
	const seg = ballSegAt(tl, t);
	const rim = { x: rimX(tr.team), y: 25 };
	if (seg?.kind === "hold" && seg.pid === tr.pid) {
		return angleTo(here, rim, fallback);
	}
	if (offenseAt(tl, t) === tr.team) {
		const a = unit(here, ball);
		const b = unit(here, rim);
		return Math.atan2(a.y * 0.65 + b.y * 0.35, a.x * 0.65 + b.x * 0.35);
	}
	return angleTo(here, ball, fallback);
};

// Turning takes a moment: the facing is the recent intentions, averaged.
const YAW_SAMPLES = [0, 45, 90, 135, 180, 225];
const yawAt = (tl: CourtTimeline, tr: Track, t: number): number => {
	let sx = 0;
	let sy = 0;
	for (let k = 0; k < YAW_SAMPLES.length; k++) {
		const a = yawTarget(tl, tr, t - YAW_SAMPLES[k]!);
		const w = YAW_SAMPLES.length - k;
		sx += Math.cos(a) * w;
		sy += Math.sin(a) * w;
	}
	return Math.atan2(sy, sx);
};

export const evalPlayer = (
	tl: CourtTimeline,
	pid: number,
	t: number,
): PlayerState => {
	const tr = tl.tracks.get(pid);
	if (!tr) {
		return {
			pid,
			team: 0,
			shown: false,
			x: 0,
			y: 0,
			z: 0,
			yaw: 0,
			anim: "ready",
			phase: 0,
			moving: false,
		};
	}
	const si = lastIndex(tr.shown, t, (s) => s[0]);
	const shown = si >= 0 ? tr.shown[si]![1] : false;
	const here = spotAt(tr, t);
	const act = actAt(tr, t);
	const mv = here.moveIndex >= 0 ? tr.moves[here.moveIndex] : undefined;

	let anim: AnimName;
	let phase: number;
	let z = 0;
	if (act) {
		const u = (t - act.t0) / (act.t1 - act.t0);
		anim = act.anim;
		const a = ANIMS[anim];
		phase =
			a.kind === "act"
				? clamp01(u)
				: ((t - act.t0) / 1000) * (a.kind === "loop" ? a.fps / a.n : 1);
		z = jumpZ(act, u);
	} else if (here.moving && mv) {
		anim = mv.anim;
		const a = ANIMS[anim];
		phase = here.traveled / (a.kind === "cycle" ? a.stride : 5);
	} else {
		const seg = ballSegAt(tl, t);
		if (seg && seg.kind === "hold" && seg.pid === pid) {
			anim = seg.style === "dribble" ? "dribbleIdle" : "hold";
		} else {
			anim = offenseAt(tl, t) === tr.team ? "ready" : "stance";
		}
		const a = ANIMS[anim];
		const fps = a.kind === "loop" ? a.fps : 2;
		phase = (t / 1000) * (fps / a.n) + pid * 0.37;
	}
	return {
		pid,
		team: tr.team,
		shown,
		x: here.x,
		y: here.y,
		z,
		yaw: yawAt(tl, tr, t),
		anim,
		phase,
		moving: here.moving,
	};
};

// A point on his body, in world feet.
export const bodyPoint = (st: PlayerState, v: V3): Pt3 => {
	const c = Math.cos(st.yaw);
	const s = Math.sin(st.yaw);
	// Forward is (c, s); his left is (s, -c) - the floor's y runs toward the
	// camera, so turning left from the right rim is turning away from it.
	return {
		x: st.x + v.f * c + v.s * s,
		y: st.y + v.f * s - v.s * c,
		z: st.z + v.u,
	};
};

// A hand, in world feet, from his own skeleton - so the ball sits in the hands
// the renderer draws.
export const handWorld = (
	st: PlayerState,
	body: Body,
	which: "near" | "both" = "both",
): Pt3 => {
	const sk = skeleton(body, poseAt(st.anim, st.phase));
	const r = sk.armR.end;
	const l = sk.armL.end;
	const h =
		which === "near"
			? r
			: { f: (r.f + l.f) / 2, s: (r.s + l.s) / 2, u: (r.u + l.u) / 2 };
	// The ball rests just in front of the hands, not inside them.
	return bodyPoint(st, { f: h.f + 0.28, s: h.s, u: h.u });
};

export type BallState = { x: number; y: number; z: number; holder?: number };

// A basketball is 9.4 inches across.
export const BALL_R = 0.39;

export const evalBall = (
	tl: CourtTimeline,
	t: number,
	bodyFor: (pid: number) => Body,
): BallState => {
	const seg = ballSegAt(tl, t);
	if (!seg) {
		return { x: 47, y: 25, z: 0 };
	}
	const handOf = (pid: number, at: number, which: "near" | "both") =>
		handWorld(evalPlayer(tl, pid, at), bodyFor(pid), which);
	const resolve = (
		p: Pt3 | { pid: number; hand?: "near" | "both" },
		at: number,
	): Pt3 => ("pid" in p ? handOf(p.pid, at, p.hand ?? "both") : p);

	if (seg.kind === "hold") {
		const st = evalPlayer(tl, seg.pid, t);
		if (seg.style === "dribble") {
			const body = bodyFor(seg.pid);
			const h = handWorld(st, body, "near");
			const ph = (((t - seg.t0) / 1000) * (st.moving ? 2.4 : 1.9)) % 1;
			const tri = 1 - Math.abs(2 * ph - 1);
			// It hits the floor ahead of him and off his right foot.
			const floor = bodyPoint(
				{ ...st, z: 0 },
				{ f: st.moving ? 1.6 : 0.9, s: -0.75, u: BALL_R },
			);
			return {
				x: h.x + (floor.x - h.x) * tri,
				y: h.y + (floor.y - h.y) * tri,
				z: h.z + (floor.z - h.z) * tri,
				holder: seg.pid,
			};
		}
		return { ...handWorld(st, bodyFor(seg.pid), "both"), holder: seg.pid };
	}
	if (seg.kind === "fly") {
		const a = resolve(seg.from, seg.t0);
		const b = resolve(seg.to, seg.t1);
		const u = clamp01((t - seg.t0) / (seg.t1 - seg.t0));
		const arc = Math.max(0, seg.peak - (a.z + b.z) / 2);
		return {
			x: a.x + (b.x - a.x) * u,
			y: a.y + (b.y - a.y) * u,
			z: a.z + (b.z - a.z) * u + 4 * arc * u * (1 - u),
		};
	}
	if (seg.kind === "bounce") {
		// A drop from where it was, then shrinking hops, then a roll. Heights
		// are of the ball's middle, which sits a radius off the floor.
		const u = clamp01((t - seg.t0) / (seg.t1 - seg.t0));
		const drop = Math.max(0, seg.from.z - BALL_R);
		const hops = [Math.sqrt(Math.max(0.02, drop))];
		for (let i = 0; i < seg.hops; i++) {
			hops.push(2 * Math.sqrt(seg.h0 * 0.42 ** i));
		}
		const total = hops.reduce((s, h) => s + h, 0) / 0.85;
		let w = u * total;
		let z = 0;
		for (let i = 0; i < hops.length; i++) {
			if (w <= hops[i]!) {
				const v = w / hops[i]!;
				z =
					i === 0
						? drop * (1 - v * v)
						: 4 * seg.h0 * 0.42 ** (i - 1) * v * (1 - v);
				break;
			}
			w -= hops[i]!;
		}
		const roll = 1 - (1 - Math.min(1, u / 0.85)) ** 1.5;
		return {
			x: seg.from.x + (seg.to.x - seg.from.x) * roll,
			y: seg.from.y + (seg.to.y - seg.from.y) * roll,
			z: BALL_R + z,
		};
	}
	return { ...seg.at };
};

// The most recent effect of a kind (optionally on one rim) within `windowMs`.
export const recentFx = (
	tl: CourtTimeline,
	t: number,
	kinds: FxKind[],
	windowMs: number,
	rim?: Side,
): Fx | undefined => {
	const i = lastIndex(tl.fx, t, (f) => f.t);
	for (let k = i; k >= 0; k--) {
		const f = tl.fx[k]!;
		if (t - f.t > windowMs) {
			break;
		}
		if (kinds.includes(f.kind) && (rim === undefined || f.rim === rim)) {
			return f;
		}
	}
	return undefined;
};
