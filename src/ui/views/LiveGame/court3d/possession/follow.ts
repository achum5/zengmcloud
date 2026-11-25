// THE NEW MOVEMENT, FOLLOWING TODAY'S PLAY (see README.md, step 3).
//
// Until the ten decide for themselves, each man goes where today's stepping
// wants him (stepper.ts: the director's runs, with each man's own way of
// getting open, probing, drifting and the rest laid over them) - but moved
// by his own body (body.ts, movement.ts) rather than today's: a push that
// builds, braking in time, a cut taken at a cutting pace, and only as much
// of all that as his ratings give him. What he does - a shot, a pass, a
// catch, a screen - is where and when the play has it, as before.
//
// Today's play sets a man off and pulls him up quicker than any body can
// (flat out in half a second), so chased as it comes he falls feet behind
// it. He goes as a man who knows where he is going does instead: into a run
// a little before the play has him in it, out of it a little after - the
// play's runs smoothed, as much either way, over about as long as he takes
// to get going (TAKE_IN). Nearer something of his, less so, and on the play
// itself by the time it starts.
//
// Swapped in for stepper.ts's own `follow`, one man at a time: same in,
// same out.

import { COURT_H, COURT_W, type Pt } from "../geometry.ts";
import type { Sample, Spans } from "../stepper.ts";
import type { Body } from "./body.ts";
import { type Goal, type Mover, STEP_S, step } from "./movement.ts";

const STEP_MS = STEP_S * 1000;

export type Follow = (
	want: (t: number) => Pt | undefined,
	t0: number,
	t1: number,
	held: Spans,
	cut?: Spans,
	lagAt?: (t: number) => number,
	seeAt?: (t: number) => number,
	sharp?: Spans,
	before?: Sample,
) => Sample[];

// How far ahead (s) he reads where the play is taking him: the time it
// takes him to pull up from the pace he is going, within these.
const LOOK_LEAST = 0.2;
const LOOK_MOST = 1.2;
// Further off where the play wants him than this (feet), something has
// gone wrong (a cut, a man brought on): he is just there.
const JUMP = 6;
// Behind the play, he goes up to this much quicker (feet a second) to make
// it up, over about this long (s).
const CATCH_UP = 5;
const CATCH_OVER = 0.5;
// Stood, the play setting him off: going by this pace or faster (feet a
// second) - seen this far (ms) ahead - a beat to react first, shorter the
// faster it sends him (this pace or more: none to speak of).
const SET_OFF = 0.5;
const REACT_SEE = 150;
const REACT_PACE = 8;
// ... unless something of his is due sooner than this (ms) after it.
const REACT_ROOM = 1500;
// Where the play asks more of him than his body has - flat out quicker
// than any man gets going, faster than he goes - he falls behind it, this
// far (feet) and no further: past that he is pulled along, up to this much
// quicker (feet a second) than he would go - if no quicker than the play -
// the pull building and letting go over most of a second (feet a second, a
// second), not jolting him.
// (Until the ten decide for themselves, and ask only what a body has.)
const SLACK = 2.5;
const PULL = 5;
const PULL_OVER = 0.3;
const PULL_BUILD = 7;
// Something of his coming: over this long (ms) before it, less and less
// behind the play; over the last of that, whatever gap is left closed -
// time enough that he eases onto it, not snatched there in a few steps.
const COMMIT_MS = 1300;
const ONTO_MS = 800;
// Over a line the play has him inside, drawn back in no faster than this
// (feet a second).
const LINE_PULL = 4;
// What he goes about the least hard (0 to 1): even strolling, a man means it.
const LEAST_EFFORT = 0.2;
// How long either side (s) of a change in the play's pace he takes it
// over: this over his burst (feet a second, a second) - a man the middle of
// the league's about 0.45 s, the quickest a third less, the slowest a third
// more. (A man of the play's flat out from stood, so: at 12 feet a second
// in about 1.7 s, as real men are at the median.)
const TAKE_IN = 7.9;
// (Taken in steps of this (s), so each width is worked out once.)
const TAKE_GRAIN = 0.025;

const zero = { hit: () => false, next: () => Infinity, last: () => -Infinity };

const clamp01 = (x: number) => (x < 0 ? 0 : x > 1 ? 1 : x);

// A bell-shaped weighting of the steps either side, `w` grains wide either
// way (a normal curve, cut at three widths), summing to one. (Kept in
// `bells`, one lot to a man, worked out as the game is staged - the same
// everywhere: see sameMath.)
type Bells = Map<number, Float64Array>;
const bellOf = (bells: Bells, w: number): Float64Array => {
	let b = bells.get(w);
	if (!b) {
		const s = (w * TAKE_GRAIN) / STEP_S;
		const h = Math.ceil(3 * s);
		b = new Float64Array(2 * h + 1);
		let sum = 0;
		for (let k = -h; k <= h; k++) {
			const v = Math.exp(-(k * k) / (2 * s * s));
			b[k + h] = v;
			sum += v;
		}
		for (let k = 0; k < b.length; k++) {
			b[k]! /= sum;
		}
		bells.set(w, b);
	}
	return b;
};

// Where the play has him from `from` to `to` (ms) a step at a time, its
// runs taken in over `take` (s) either side - not across anything of his,
// a cut, or where the play puts him somewhere else, and less so the nearer
// one of those is: on the play itself at each.
const takenIn = (
	want: (t: number) => Pt | undefined,
	from: number,
	to: number,
	stopAt: (t: number) => boolean,
	take: number,
	bells: Bells,
): ((t: number) => Pt | undefined) => {
	const n = Math.max(1, Math.round((to - from) / STEP_MS) + 1);
	const xs = new Float64Array(n);
	const ys = new Float64Array(n);
	const ok = new Uint8Array(n);
	const stops = new Uint8Array(n);
	// (How many steps to the nearest of those, either way.)
	const near = new Int32Array(n);
	let last = -1;
	for (let k = 0; k < n; k++) {
		const t = from + k * STEP_MS;
		const p = want(t);
		let stop = !p || stopAt(t);
		if (p) {
			xs[k] = p.x;
			ys[k] = p.y;
			ok[k] = 1;
			if (
				k > 0 &&
				ok[k - 1] &&
				Math.hypot(p.x - xs[k - 1]!, p.y - ys[k - 1]!) > JUMP
			) {
				stop = true;
			}
		}
		if (stop) {
			stops[k] = 1;
			last = k;
		}
		near[k] = last < 0 ? k + 1 : k - last;
	}
	last = -1;
	for (let k = n - 1; k >= 0; k--) {
		if (stops[k]) {
			last = k;
		}
		near[k] = Math.min(near[k]!, last < 0 ? n - k : last - k);
	}
	const sx = new Float64Array(n);
	const sy = new Float64Array(n);
	const most = Math.floor(take / TAKE_GRAIN);
	for (let k = 0; k < n; k++) {
		if (!ok[k]) {
			continue;
		}
		// (Its width no more than a third of the way to the nearest.)
		const w = Math.min(
			most,
			Math.floor(((near[k]! - 1) * STEP_S) / 3 / TAKE_GRAIN),
		);
		if (w < 1) {
			sx[k] = xs[k]!;
			sy[k] = ys[k]!;
			continue;
		}
		const b = bellOf(bells, w);
		const h = (b.length - 1) / 2;
		let x = 0;
		let y = 0;
		for (let j = 0; j < b.length; j++) {
			const q = k - h + j;
			x += b[j]! * xs[q]!;
			y += b[j]! * ys[q]!;
		}
		sx[k] = x;
		sy[k] = y;
	}
	return (t) => {
		const f = (t - from) / STEP_MS;
		const k = Math.floor(f);
		if (k < 0 || k >= n || !ok[k]) {
			return want(t);
		}
		const u = f - k;
		if (u < 1e-9 || k + 1 >= n || !ok[k + 1]) {
			return { x: sx[k]!, y: sy[k]! };
		}
		return {
			x: sx[k]! + (sx[k + 1]! - sx[k]!) * u,
			y: sy[k]! + (sy[k + 1]! - sy[k]!) * u,
		};
	};
};

export const followWith = (body: Body): Follow => {
	const take = TAKE_IN / body.go;
	const bells: Bells = new Map();
	return (
		want,
		t0,
		t1,
		held,
		cut = zero,
		lagAt = () => 0,
		seeAt = () => LOOK_MOST,
		sharp = zero,
		before,
	) => {
		const dt = STEP_S;
		const out: Sample[] = [];
		// The play as he takes it: from far enough back to take in what he
		// comes in on, to as far on as he reads.
		const pad = Math.ceil((3 * take * 1000) / STEP_MS) * STEP_MS;
		const go = takenIn(
			want,
			t0 - pad,
			t1 + pad + LOOK_MOST * 1000 + STEP_MS,
			(t) => held.hit(t - STEP_MS, t) || cut.hit(t - STEP_MS, t),
			take,
			bells,
		);
		let m: Mover | undefined = before && {
			x: before.x,
			y: before.y,
			vx: before.vx,
			vy: before.vy,
			ax: 0,
			ay: 0,
			yaw: Math.atan2(before.vy, before.vx),
		};
		let was: Pt | undefined = before && want(t0 - STEP_MS);
		// (When the play set him off from standing, and how long he takes.)
		let off: { t: number; lag: number } | undefined;
		// (How hard he is being pulled along, feet a second.)
		let pull = 0;
		for (let t = t0; t <= t1; t += STEP_MS) {
			const W = want(t);
			if (!W) {
				m = undefined;
				was = undefined;
				continue;
			}
			const W1 = want(t + STEP_MS) ?? W;
			const wx = (W1.x - W.x) / dt;
			const wy = (W1.y - W.y) / dt;
			if (
				!m ||
				!was ||
				held.hit(t - STEP_MS, t) ||
				cut.hit(t - STEP_MS, t) ||
				Math.hypot(W.x - was.x, W.y - was.y) > JUMP
			) {
				// Doing something, the picture cutting, or just put on the
				// floor: where the play has him, going as it has him going.
				was = W;
				m = {
					x: W.x,
					y: W.y,
					vx: wx,
					vy: wy,
					ax: 0,
					ay: 0,
					yaw: m?.yaw ?? Math.atan2(wy, wx),
				};
				pull = 0;
				out.push({ t, x: W.x, y: W.y, vx: wx, vy: wy });
				continue;
			}
			was = W;
			// Where he means to be: the play as he takes it.
			const G = go(t) ?? W;
			const G1 = go(t + STEP_MS) ?? G;
			const gx = (G1.x - G.x) / dt;
			const gy = (G1.y - G.y) / dt;
			const pace = Math.hypot(m.vx, m.vy);
			// Where that is taking him, read as far ahead as it takes him to
			// pull up from the pace he is going: the way it goes over that is
			// the way he goes, so a turn coming is one he starts into.
			const see = seeAt(t);
			const look =
				see <= 0
					? 0
					: Math.max(
							LOOK_LEAST,
							Math.min(LOOK_MOST, see, (pace / body.brake) * 0.9 + 0.15),
						);
			const L = look > 0 ? (go(t + look * 1000) ?? G) : G;
			const vx = look > 0 ? (L.x - G.x) / look : gx;
			const vy = look > 0 ? (L.y - G.y) / look : gy;
			// Stood, and it sets him off: a beat to react first - his own -
			// unless something of his is coming up he has to be in time for,
			// or everybody is going for a shot.
			const soon = go(t + REACT_SEE) ?? G;
			const going = Math.max(
				Math.hypot(gx, gy),
				(Math.hypot(soon.x - G.x, soon.y - G.y) * 1000) / REACT_SEE,
			);
			if (pace < 1 && going > SET_OFF) {
				if (!off) {
					const outside = m.x < 0 || m.x > COURT_W || m.y < 0 || m.y > COURT_H;
					const lag =
						outside || sharp.hit(t, t + 1)
							? 0
							: lagAt(t) * Math.min(1, REACT_PACE / Math.max(going, 2));
					const from = Math.hypot(gx, gy) > SET_OFF ? t : t + REACT_SEE;
					off =
						held.next(t) - from > lag + REACT_ROOM
							? { t: from, lag }
							: { t: from, lag: 0 };
				}
			} else if (going < SET_OFF) {
				off = undefined;
			}
			const gap = Math.hypot(G.x - m.x, G.y - m.y);
			const need = Math.hypot(vx, vy) + Math.min(CATCH_UP, gap / CATCH_OVER);
			const effort = Math.min(
				1,
				Math.max(LEAST_EFFORT, (need - 4.5) / (body.top - 4.5) + 0.1),
			);
			const goal: Goal =
				off && t - off.t < off.lag
					? { to: { x: m.x, y: m.y }, effort: LEAST_EFFORT }
					: { to: G, toV: { x: vx, y: vy }, effort };
			m = step(m, goal, body, dt);
			// Too far behind the play: pulled along - and less and less
			// behind it as something of his comes. (His own going kept as it
			// was: the pull is on top of it, and shown in how fast he goes.)
			const due = held.next(t);
			const slack = SLACK * clamp01((due - t - ONTO_MS) / COMMIT_MS);
			let px = 0;
			let py = 0;
			{
				const ex = W.x - m.x;
				const ey = W.y - m.y;
				const e = Math.hypot(ex, ey);
				// (Something of his coming, he has to be there: never mind how
				// fast that has him going.)
				const most =
					slack < SLACK
						? PULL
						: Math.max(
								0,
								Math.max(body.top, Math.hypot(wx, wy)) - Math.hypot(m.vx, m.vy),
							);
				const want = Math.min(PULL, most, Math.max(0, e - slack) / PULL_OVER);
				pull += Math.max(
					-PULL_BUILD * dt,
					Math.min(PULL_BUILD * dt, want - pull),
				);
				if (pull > 0 && e > 1e-9) {
					const by = Math.min(e, pull * dt);
					px = (ex / e) * by;
					py = (ey / e) * by;
					m = { ...m, x: m.x + px, y: m.y + py };
				}
			}
			// Something of his about to start: on the play for it by then,
			// whatever is left of the gap closed over the last moment.
			// (The gap left closed along an easing in and out over that moment
			// - each step taking the share of what is left that keeps it on
			// that curve - not most of it in the last few steps.)
			if (due - t <= ONTO_MS) {
				const u = clamp01(1 - (due - t - STEP_MS) / ONTO_MS);
				const left = (x: number) => {
					const c = clamp01(x);
					return 1 - c * c * (3 - 2 * c);
				};
				const was = left(u - STEP_MS / ONTO_MS);
				const k = was > 1e-9 ? 1 - left(u) / was : 1;
				px += (W.x - m.x) * k;
				py += (W.y - m.y) * k;
				m = {
					...m,
					x: m.x + (W.x - m.x) * k,
					y: m.y + (W.y - m.y) * k,
					vx: m.vx + (wx - m.vx) * k,
					vy: m.vy + (wy - m.vy) * k,
				};
			}
			// Wanted inside the lines and over one: drawn back in, a step at a
			// time - never put there.
			if (W.x >= 0 && W.x <= COURT_W && W.y >= 0 && W.y <= COURT_H) {
				const ix = Math.min(COURT_W, Math.max(0, m.x));
				const iy = Math.min(COURT_H, Math.max(0, m.y));
				const over = Math.hypot(ix - m.x, iy - m.y);
				if (over > 0) {
					const k = Math.min(1, (LINE_PULL * dt) / over);
					m = { ...m, x: m.x + (ix - m.x) * k, y: m.y + (iy - m.y) * k };
				}
			}
			out.push({ t, x: m.x, y: m.y, vx: m.vx + px / dt, vy: m.vy + py / dt });
		}
		return out;
	};
};
