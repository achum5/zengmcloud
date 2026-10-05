import type {
	Act,
	ArenaShot,
	BallSeg,
	FxKind,
	Fx,
	CourtTimeline,
	Track,
} from "./director.ts";
import { rimX, type Pt, type Pt3, type Side } from "./geometry.ts";
import {
	ANIMS,
	holdBall,
	posed,
	skeleton,
	type AnimName,
	type Body,
	type Hand,
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
	// The ball in his hands - not bouncing on a dribble - so it is drawn with
	// him, his hands on it.
	holding?: boolean;
	// How far through a bounce of his dribble (0 the ball in his hand at the
	// top), when he is dribbling: his hand rides it - this one.
	dribble?: number;
	dribbleHand?: Hand;
};

// Bounces a second on a dribble: one steady beat, walking or driving, so the
// ball never skips a bounce when he starts or stops.
export const DRIBBLE_RATE = 2.1;
// A crossover's bounces, quicker: low and hand to hand.
export const CROSS_RATE = 3.2;
const otherHand = (h: Hand): Hand => (h === "R" ? "L" : "R");

// Through a crossover: which bounce, how far through it, and the hand it
// left - each bounce the other hand from the one before.
const crossAt = (seg: { t0: number; hand?: Hand }, t: number) => {
	const b = ((t - seg.t0) / 1000) * CROSS_RATE;
	const k = Math.floor(b);
	const from = k % 2 ? otherHand(seg.hand ?? "R") : (seg.hand ?? "R");
	return { ph: b - k, from, to: otherHand(from) };
};
const dribblePhase = (t0: number, t: number): number =>
	(((((t - t0) / 1000) * DRIBBLE_RATE) % 1) + 1) % 1;

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
	// A defensive slide keeps his eyes on the ball whichever way he goes.
	if (mv && here.moving && mv.anim === "slide") {
		return angleTo(here, ballNear(tl, t), fallback);
	}
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

// Turning takes a moment: from where he meant to face a little while ago,
// he turns toward each newer intention no faster than a player can pivot.
// Worked out afresh for every t (no state), so any frame can be asked for.
const TURN_STEP = 40;
const TURN_WINDOW = 360;
const TURN_MAX = (Math.PI * 3 * TURN_STEP) / 1000;
const yawAt = (tl: CourtTimeline, tr: Track, t: number): number => {
	let yaw = yawTarget(tl, tr, t - TURN_WINDOW);
	for (let k = TURN_WINDOW - TURN_STEP; k >= 0; k -= TURN_STEP) {
		let d = yawTarget(tl, tr, t - k) - yaw;
		d -= Math.round(d / (Math.PI * 2)) * Math.PI * 2;
		yaw += Math.max(-TURN_MAX, Math.min(TURN_MAX, d));
	}
	return yaw;
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
			anim = seg.style === "hold" ? "hold" : "dribbleIdle";
		} else {
			anim = offenseAt(tl, t) === tr.team ? "ready" : "stance";
		}
		const a = ANIMS[anim];
		const fps = a.kind === "loop" ? a.fps : 2;
		phase = (t / 1000) * (fps / a.n) + pid * 0.37;
	}
	const seg = ballSegAt(tl, t);
	const has = seg?.kind === "hold" && seg.pid === pid ? seg : undefined;
	// The hand on the ball: on a crossover, the one it left for the first
	// half of the bounce, the one it goes to for the second.
	const cross = has?.style === "cross" ? crossAt(has, t) : undefined;
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
		holding: has?.style === "hold",
		dribble: cross
			? cross.ph
			: has?.style === "dribble"
				? dribblePhase(has.t0, t)
				: undefined,
		dribbleHand: cross
			? cross.ph < 0.5
				? cross.from
				: cross.to
			: has?.style === "dribble"
				? (has.hand ?? "R")
				: undefined,
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
	which: "near" | "far" | "both" = "both",
): Pt3 => {
	const sk = skeleton(
		body,
		posed(st.anim, st.phase, st.dribble, st.dribbleHand),
	);
	const r = sk.armR.end;
	const l = sk.armL.end;
	const h =
		which === "near"
			? r
			: which === "far"
				? l
				: { f: (r.f + l.f) / 2, s: (r.s + l.s) / 2, u: (r.u + l.u) / 2 };
	// The ball rests just in front of the hands, not inside them.
	return bodyPoint(st, { f: h.f + 0.28, s: h.s, u: h.u });
};

// The ball in his hands, held the way his move holds it.
export const heldBall = (st: PlayerState, body: Body): Pt3 =>
	bodyPoint(
		st,
		holdBall(
			body,
			posed(st.anim, st.phase, st.dribble, st.dribbleHand),
			st.anim,
		).ball,
	);

export type BallState = {
	x: number;
	y: number;
	z: number;
	holder?: number;
	// How far it has turned (radians, positive rolling toward the right rim):
	// backspin off a shooter's fingers or a passer's, a roll along the floor.
	roll?: number;
};

// Turns a second of backspin on a ball thrown or shot.
const BACKSPIN = 2;

// A basketball is 9.4 inches across.
export const BALL_R = 0.39;
// Feet per second, per second.
const GRAVITY = 32.2;

// How far through one bounce of a dribble the ball is (0 at the hand, 1 at
// the floor), for a point `ph` through the dribble: pushed down hard and
// falling faster all the way to the floor, then rising off it and slowing
// into the hand.
const DOWN = 0.42;
const dribbleDepth = (ph: number): number =>
	ph < DOWN ? (ph / DOWN) ** 1.55 : (1 - (ph - DOWN) / (1 - DOWN)) ** 1.8;

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
		which === "both"
			? heldBall(evalPlayer(tl, pid, at), bodyFor(pid))
			: handWorld(evalPlayer(tl, pid, at), bodyFor(pid), which);
	const resolve = (
		p: Pt3 | { pid: number; hand?: "near" | "both" },
		at: number,
	): Pt3 => ("pid" in p ? handOf(p.pid, at, p.hand ?? "both") : p);

	if (seg.kind === "hold") {
		const st = evalPlayer(tl, seg.pid, t);
		if (seg.style === "cross") {
			// Low and quick, hand to hand across in front of him - or through
			// his legs.
			const body = bodyFor(seg.pid);
			const { ph, from: a, to: b } = crossAt(seg, t);
			const from = handWorld(st, body, a === "R" ? "near" : "far");
			const to = handWorld(st, body, b === "R" ? "near" : "far");
			const floor = bodyPoint(
				{ ...st, z: 0 },
				{ f: seg.move === "legs" ? 0.15 : 1.1, s: 0, u: BALL_R },
			);
			const tri = dribbleDepth(ph);
			const h = ph < DOWN ? from : to;
			return {
				x: h.x + (floor.x - h.x) * tri,
				y: h.y + (floor.y - h.y) * tri,
				z: h.z + (floor.z - h.z) * tri,
				holder: seg.pid,
			};
		}
		if (seg.style === "dribble") {
			const body = bodyFor(seg.pid);
			const left = seg.hand === "L";
			const h = handWorld(st, body, left ? "far" : "near");
			const ph = st.dribble ?? dribblePhase(seg.t0, t);
			const tri = dribbleDepth(ph);
			// It hits the floor ahead of him and off the foot on that side.
			const floor = bodyPoint(
				{ ...st, z: 0 },
				{ f: st.moving ? 1.6 : 0.9, s: left ? 0.75 : -0.75, u: BALL_R },
			);
			return {
				x: h.x + (floor.x - h.x) * tri,
				y: h.y + (floor.y - h.y) * tri,
				z: h.z + (floor.z - h.z) * tri,
				holder: seg.pid,
			};
		}
		return { ...heldBall(st, bodyFor(seg.pid)), holder: seg.pid };
	}
	if (seg.kind === "fly") {
		// In flight it is a thrown ball: steady across the floor, and up and
		// down under gravity - so a three climbs to fifteen feet in the second
		// it takes, a chest pass barely rises, and a lob hangs for the dunker.
		const a = resolve(seg.from, seg.t0);
		const b = resolve(seg.to, seg.t1);
		const u = clamp01((t - seg.t0) / (seg.t1 - seg.t0));
		const T = Math.max(0, seg.t1 - seg.t0) / 1000;
		const tau = u * T;
		const vz = T > 0 ? (b.z - a.z) / T + 0.5 * GRAVITY * T : 0;
		return {
			x: a.x + (b.x - a.x) * u,
			y: a.y + (b.y - a.y) * u,
			z: a.z + vz * tau - 0.5 * GRAVITY * tau * tau,
			roll: -Math.PI * 2 * BACKSPIN * tau * (Math.sign(b.x - a.x) || 1),
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
		const along =
			Math.hypot(seg.to.x - seg.from.x, seg.to.y - seg.from.y) * roll;
		return {
			x: seg.from.x + (seg.to.x - seg.from.x) * roll,
			y: seg.from.y + (seg.to.y - seg.from.y) * roll,
			z: BALL_R + z,
			roll: (along / BALL_R) * (Math.sign(seg.to.x - seg.from.x) || 1),
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

// The look round the building (see ArenaShot) showing at t, if any.
export const arenaShotAt = (
	tl: CourtTimeline,
	t: number,
): ArenaShot | undefined => {
	const i = lastIndex(tl.shots, t, (s) => s.t0);
	const s = i >= 0 ? tl.shots[i] : undefined;
	return s && t < s.t1 ? s : undefined;
};

// Every moment the picture cuts: the director's cuts, and into and out of
// each look round the building.
const camCuts = new WeakMap<CourtTimeline, number[]>();
export const cameraCuts = (tl: CourtTimeline): number[] => {
	let out = camCuts.get(tl);
	if (!out) {
		out = [
			...new Set([...tl.cuts, ...tl.shots.flatMap((s) => [s.t0, s.t1])]),
		].sort((a, b) => a - b);
		camCuts.set(tl, out);
	}
	return out;
};

// The last moment the picture cut, at or before t.
export const lastCut = (tl: CourtTimeline, t: number): number => {
	const cuts = cameraCuts(tl);
	const i = lastIndex(cuts, t, (c) => c);
	return i >= 0 ? cuts[i]! : -Infinity;
};
