// HOW A MAN GETS ABOUT THE FLOOR, a step (STEP_S) at a time (see README.md).
//
// Where he means to go becomes how he goes there, inside what a body can do
// (body.ts): a push that builds rather than jolts, the less of it the nearer
// he is to flat out; braking in time to stop where he means to; and the
// three - speeding up, braking, turning - sharing one grip on the floor, so
// a man going hard cannot also turn hard. Cutting back the way he came, he
// slows and rounds it; real men go through a cut of over 90 degrees at about
// 5 ft/s, not stopped dead and not at full tilt (tools/court3d-tape/nba).
//
// Nothing here decides anything: the goal comes from whoever does (the
// planner, his role), and the same step serves all ten.

import type { Pt } from "../geometry.ts";
import type { Body } from "./body.ts";

export const STEP_S = 0.05;

export type Mover = {
	x: number;
	y: number;
	vx: number;
	vy: number;
	// How hard he is pushing right now (feet a second, a second).
	ax: number;
	ay: number;
	// Which way he faces (radians).
	yaw: number;
};

export type Goal = {
	to: Pt;
	// How that spot is moving (feet a second) - the man he goes with.
	toV?: Pt;
	// How fast he means to be going as he gets there (0: there and stood).
	arrive?: number;
	// How hard he goes about it, from 0 (strolling) to 1 (all he has).
	effort: number;
	// Which way he means to face (radians), if not the way he is going.
	face?: number;
};

// A stroll (feet a second): the least a man going somewhere goes.
const STROLL = 4.5;
// How quickly he answers a change in what he wants (s): sharper going hard.
const ANSWER_EASY = 0.34;
const ANSWER_HARD = 0.2;
// Coming up on his spot, he eases the last of it in over about this long
// (s), not braking flat out into it.
const SETTLE = 0.35;
// Within this (feet) and this slow (feet a second), he is there.
const THERE = 0.05;
const STOOD = 0.25;
// Under this pace (feet a second) he has no way he is going yet: pushing
// off any way is the same push.
const ANY_WAY = 2;
// (How much of the way he would carry on before he answers, he allows for.)
const LAG_SHARE = 0.7;
// Turning further off the way he is going than this (its cosine), he takes
// the turn at a man's cutting pace: this much of his sideways grip (feet a
// second) - about 5 ft/s, as real men cut.
const CUT_COS = -0.25;
const CORNER = 0.32;

const clamp01 = (x: number) => (x < 0 ? 0 : x > 1 ? 1 : x);
const wrapAngle = (a: number) => {
	let x = a;
	while (x > Math.PI) {
		x -= 2 * Math.PI;
	}
	while (x < -Math.PI) {
		x += 2 * Math.PI;
	}
	return x;
};

// The velocity he wants: going with his spot, and closing on it as fast as
// he can still stop at it (or arrive at it as fast as he means to) - and,
// wanting to go a good way off the way he is going, slowed into the turn
// first, as a man plants and cuts rather than swinging wide round it.
const wanted = (m: Mover, g: Goal, b: Body, answer: number): Pt => {
	const effort = clamp01(g.effort);
	const cap = STROLL + (b.top - STROLL) * effort;
	const tv = g.toV ?? { x: 0, y: 0 };
	const dx = g.to.x - m.x;
	const dy = g.to.y - m.y;
	const d = Math.hypot(dx, dy);
	if (d < 1e-9) {
		return { x: tv.x, y: tv.y };
	}
	const pace = Math.hypot(m.vx, m.vy);
	const arrive = Math.min(g.arrive ?? 0, cap);
	// (Braking on the way in with the most of his brakes he would use going
	// that hard - never flat out - and starting to as soon as he must, given
	// how long he takes to answer: by how fast he is closing on the spot,
	// which may itself be going.)
	const brakeUse = b.brake * (0.35 + 0.4 * effort);
	const closing = Math.max(0, ((m.vx - tv.x) * dx + (m.vy - tv.y) * dy) / d);
	const ahead = Math.max(0, d - closing * answer * LAG_SHARE);
	const close = Math.min(
		Math.sqrt(arrive * arrive + 2 * brakeUse * ahead),
		Math.max(arrive, ahead / SETTLE),
		cap,
	);
	let wx = tv.x + (dx / d) * close;
	let wy = tv.y + (dy / d) * close;
	let most = cap;
	const w = Math.hypot(wx, wy);
	if (pace > ANY_WAY && w > 1e-9) {
		const cos = (wx * m.vx + wy * m.vy) / (w * pace);
		const f = clamp01((cos - CUT_COS) / (1 - CUT_COS));
		const corner = CORNER * b.side;
		most = Math.min(cap, corner + (cap - corner) * f);
	}
	if (w > most) {
		wx *= most / w;
		wy *= most / w;
	}
	return { x: wx, y: wy };
};

// The push he can put down, `want` (feet a second, a second) cut to what
// his body has at this pace and effort: forward and back along the way he
// is going, sideways across it, the three sharing one grip.
const pushable = (m: Mover, want: Pt, b: Body, effort: number): Pt => {
	const v = Math.hypot(m.vx, m.vy);
	const fwd = b.go * (0.4 + 0.6 * effort) * Math.max(0.04, 1 - v / b.top);
	const brk = b.brake * (0.45 + 0.55 * effort);
	const sid = b.side * (0.45 + 0.55 * effort);
	const wn = Math.hypot(want.x, want.y);
	if (wn < 1e-9) {
		return { x: 0, y: 0 };
	}
	// (Stood or nearly: every way is forward - blended in, so nothing jumps
	// as he gets going.)
	const k = clamp01(v / ANY_WAY);
	const ux = k > 0 ? m.vx / v : want.x / wn;
	const uy = k > 0 ? m.vy / v : want.y / wn;
	const along = want.x * ux + want.y * uy;
	const across = -want.x * uy + want.y * ux;
	const lim = along >= 0 ? fwd : brk;
	const alongMax = lim;
	const acrossMax = sid * k + fwd * (1 - k);
	const e = (along / alongMax) ** 2 + (across / acrossMax) ** 2;
	const s = e > 1 ? 1 / Math.sqrt(e) : 1;
	const a1 = along * s;
	const c1 = across * s;
	return { x: ux * a1 - uy * c1, y: uy * a1 + ux * c1 };
};

// One step on: where he is, how fast he is going and pushing, which way he
// faces.
export const step = (m: Mover, g: Goal, b: Body, dt = STEP_S): Mover => {
	const effort = clamp01(g.effort);
	const answer = ANSWER_EASY + (ANSWER_HARD - ANSWER_EASY) * effort;
	const w = wanted(m, g, b, answer);
	const want = { x: (w.x - m.vx) / answer, y: (w.y - m.vy) / answer };
	const can = pushable(m, want, b, effort);
	// (The push builds and lets go no quicker than a body does.)
	let dax = can.x - m.ax;
	let day = can.y - m.ay;
	const da = Math.hypot(dax, day);
	const most = b.jerk * dt;
	if (da > most) {
		dax *= most / da;
		day *= most / da;
	}
	let ax = m.ax + dax;
	let ay = m.ay + day;
	let vx = m.vx + ax * dt;
	let vy = m.vy + ay * dt;
	const v = Math.hypot(vx, vy);
	if (v > b.top) {
		vx *= b.top / v;
		vy *= b.top / v;
	}
	let x = m.x + ((m.vx + vx) / 2) * dt;
	let y = m.y + ((m.vy + vy) / 2) * dt;
	// (There and all but stood, his spot not going anywhere: stood there.)
	const tv = g.toV;
	if (
		(g.arrive ?? 0) === 0 &&
		(!tv || Math.hypot(tv.x, tv.y) < STOOD) &&
		Math.hypot(g.to.x - x, g.to.y - y) < THERE &&
		Math.hypot(vx, vy) < STOOD
	) {
		x = g.to.x;
		y = g.to.y;
		vx = 0;
		vy = 0;
		ax = 0;
		ay = 0;
	}
	// Facing: where he means to, or the way he is going - turned no quicker
	// than he can, and less quickly at a run.
	const pace = Math.hypot(vx, vy);
	const face = g.face ?? (pace > ANY_WAY ? Math.atan2(vy, vx) : m.yaw);
	const spin = b.spin * (1 - 0.65 * Math.min(1, pace / b.top)) * dt;
	const turn = wrapAngle(face - m.yaw);
	const yaw = wrapAngle(
		m.yaw + (Math.abs(turn) <= spin ? turn : Math.sign(turn) * spin),
	);
	return { x, y, vx, vy, ax, ay, yaw };
};

export const stood = (p: Pt, yaw = 0): Mover => ({
	x: p.x,
	y: p.y,
	vx: 0,
	vy: 0,
	ax: 0,
	ay: 0,
	yaw,
});
