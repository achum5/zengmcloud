// THE TEN ON THE FLOOR, STEPPED (see tools/court3d-tape/SIM.md).
//
// The director lays out the game - every line acted out, every man's runs
// and what he does on them. This takes that, and moves every man the way a
// man moves: a step at a time (STEP_MS), no quicker to get going, turn or
// pull up than a man can (see motion.ts), following where the play wants
// him - and exactly where it has him whenever he is doing something there
// (an act: a shot, a pass, a catch, a screen...), so every moment of the
// play is where and when it was. Out come the same tracks the court reads.
//
// This first step only follows the director's own runs: the picture is the
// one it laid out, less what no man could do (a lurch, a corner turned on
// the spot at a run). What each man wants comes next, a part at a time.

import { compileCourt, type CourtTimeline, type Move } from "./director.ts";
import { floorSpotOf } from "./evaluate.ts";
import type { Pt } from "./geometry.ts";
import { pushFrom } from "./motion.ts";

export const STEP_MS = 50;

// A number from 0 to 1 from two others, the same every time (every device
// stepping the same game the same way).
const hash01 = (a: number, b: number): number => {
	let h = Math.imul(a | 0, 0x9e3779b1) ^ Math.imul(b | 0, 0x85ebca77);
	h = Math.imul(h ^ (h >>> 15), 0x2c1b3c6d);
	h = Math.imul(h ^ (h >>> 12), 0x297a2d39);
	return ((h ^ (h >>> 15)) >>> 0) / 4294967296;
};
// How hard he can pull up or turn (feet a second, a second) - the same
// braking motion.ts has him pulling up with, a touch over.
const BRAKE = 24;
const TURN = 26;
// How far ahead (s) he steers at where the play will have him - the time it
// takes him to pull up, within these - and how much faster (feet a second)
// than the play is going he goes to catch up with it.
const LOOK_LEAST = 0.2;
const LOOK_MOST = 1.2;
const CATCH_UP = 5;
// Doing something where the play has him (an act), he is eased onto it
// over this long before (ms).
const ONTO_MS = 250;
// ... and easing up into it over as long as this before (ms), pulling up
// no harder than this (feet a second, a second).
const ARRIVE_MS = 1500;
const EASE_UP = 14;
// Waiting a beat to react, only with this long (ms) still to spare before
// something of his.
const REACT_ROOM = 1500;
// How long a man takes to react, setting off (ms): from one end to the
// other, his own each time.
const REACT_MS: [number, number] = [60, 320];
const REACT_PACE = 8;
// Fastest anybody goes (feet a second).
const TOP = 30;
// Further off where the play wants him than this (feet), something has
// gone wrong (a cut, a man brought on): he is just there.
const JUMP = 6;
// A path laid down as straight runs where it bends no more than this
// (feet).
const KNOT_TOL = 0.12;
const KNOT_PINNED = 0.02;
// ... each no longer than this (ms), its pace not changing back and forth
// by more than this (feet a second) along it.
const RUN_MOST = 1500;
const PACE_NOISE = 0.25;
// Under this pace (feet a second) he is stood.
const STILL = 0.2;

type Sample = { t: number; x: number; y: number; vx: number; vy: number };

// When something of his is going on - his acts, the picture's cuts - as
// stretches of time.
type Spans = {
	// Whether any of them is going on between t0 and t1.
	hit: (t0: number, t1: number) => boolean;
	// When the next one begins, from t on.
	next: (t: number) => number;
};
const spansAt = (list: (readonly [number, number])[]): Spans => {
	// (One list of stretches, run together where they meet - each found by
	// halving.)
	const spans: [number, number][] = [];
	for (const [a, b] of [...list].sort((x, y) => x[0] - y[0])) {
		const last = spans.at(-1);
		if (last && a <= last[1]) {
			last[1] = Math.max(last[1], b);
		} else {
			spans.push([a, b]);
		}
	}
	// (The first beginning at t or after.)
	const from = (t: number) => {
		let lo = 0;
		let hi = spans.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (spans[mid]![0] < t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		return lo;
	};
	return {
		hit: (t0, t1) => {
			const k = from(t1) - 1;
			return k >= 0 && spans[k]![1] > t0;
		},
		next: (t) => spans[from(t)]?.[0] ?? Infinity,
	};
};

// One man, stepped: where he is every STEP_MS from t0 to t1, following
// `want`: as near on it as he can be whenever `held`, and on it outright
// whenever the picture `cut`s.
export const follow = (
	want: (t: number) => Pt | undefined,
	t0: number,
	t1: number,
	// Doing something there: on it, eased onto it over the moment before.
	held: Spans,
	// The picture cutting: wherever the play has him, then and there.
	cut: Spans = spansAt([]),
	// How long he takes to react (ms), setting off from standing at t: each
	// man in his own time, not all of them at once.
	lagAt: (t: number) => number = () => 0,
): Sample[] => {
	const out: Sample[] = [];
	let p: Pt | undefined;
	let vx = 0;
	let vy = 0;
	// (When the play set him off from standing, and how long he takes.)
	let off: { t: number; lag: number } | undefined;
	// (Where the play had him the step before.)
	let was: Pt | undefined;
	const dt = STEP_MS / 1000;
	// (Where he is wanted next step, kept for the step after.)
	let ahead: Pt | undefined = want(t0);
	for (let t = t0; t <= t1; t += STEP_MS) {
		const W = ahead;
		ahead = want(t + STEP_MS);
		if (!W) {
			p = undefined;
			was = undefined;
			continue;
		}
		const W1 = ahead ?? W;
		const wx = (W1.x - W.x) / dt;
		const wy = (W1.y - W.y) / dt;
		if (
			!p ||
			!was ||
			held.hit(t - STEP_MS, t) ||
			cut.hit(t - STEP_MS, t) ||
			Math.hypot(W.x - was.x, W.y - was.y) > JUMP
		) {
			was = W;
			// (Where the play has him, as fast as it has him going.)
			p = { ...W };
			vx = wx;
			vy = wy;
			out.push({ t, x: p.x, y: p.y, vx, vy });
			continue;
		}
		// Where he wants to be going: at where the play will have him a
		// moment from now - as far ahead as it takes him to pull up at the
		// pace he is going, so a turn or a stop coming is one he sees coming
		// - and no faster than a little quicker than the play has him going.
		const v0 = Math.hypot(vx, vy);
		// (Stood, and the play not yet sending him anywhere, he waits for it:
		// no setting off ahead of it.)
		const look =
			v0 < 1 && Math.hypot(wx, wy) < 0.5
				? 0
				: Math.max(LOOK_LEAST, Math.min(LOOK_MOST, (v0 / BRAKE) * 0.9));
		const L = look > 0 ? (want(t + look * 1000) ?? W) : W;
		const L1 = look > 0 ? (want(t + look * 1000 + STEP_MS) ?? L) : (ahead ?? W);
		const reach = Math.max(look, LOOK_LEAST);
		let cx = (L.x - p.x) / reach + (look > 0 ? 0 : wx);
		let cy = (L.y - p.y) / reach + (look > 0 ? 0 : wy);
		{
			const lv = Math.hypot(L1.x - L.x, L1.y - L.y) / dt;
			const most = Math.max(lv, Math.hypot(wx, wy)) + CATCH_UP;
			const cv = Math.hypot(cx, cy);
			if (cv > most) {
				cx *= most / cv;
				cy *= most / cv;
			}
		}
		// Stood, and the play sets him off: a beat to react first - his own
		// - unless something of his is coming up soon that he has to be in
		// time for.
		const due0 = held.next(t);
		if (Math.hypot(vx, vy) < 1 && Math.hypot(wx, wy) > 2) {
			if (!off) {
				// (Sent off flat out, he goes the quicker: his beat to react
				// shorter the faster the play has him going.)
				const lag = lagAt(t) * Math.min(1, REACT_PACE / Math.hypot(wx, wy));
				off = due0 - t > lag + REACT_ROOM ? { t, lag } : { t, lag: 0 };
			}
		} else if (Math.hypot(wx, wy) < 0.5) {
			off = undefined;
		}
		if (off && t - off.t < off.lag) {
			cx = 0;
			cy = 0;
		}
		// Something of his coming up where the play has him: he sees it
		// coming and is easing up into it in time - no faster, with the room
		// left, than he can comfortably pull up to the pace he does it at.
		let cap = TOP;
		const due = held.next(t);
		if (due - t < ARRIVE_MS) {
			const Q = want(due);
			const Q1 = want(due + STEP_MS);
			if (Q && Q1) {
				const qv = Math.hypot(Q1.x - Q.x, Q1.y - Q.y) / dt;
				cap = Math.min(
					cap,
					qv + Math.sqrt(2 * EASE_UP * Math.hypot(Q.x - p.x, Q.y - p.y)),
				);
			}
		}
		const c = Math.hypot(cx, cy);
		if (c > cap) {
			cx *= cap / c;
			cy *= cap / c;
		}
		// No quicker than his legs: speeding up as hard as he still can at
		// the pace he is going, pulling up and turning as hard as a man can.
		let ax = (cx - vx) / dt;
		let ay = (cy - vy) / dt;
		const v = Math.hypot(vx, vy);
		const ux = v > 1e-6 ? vx / v : 0;
		const uy = v > 1e-6 ? vy / v : 0;
		let along = v > 1e-6 ? ax * ux + ay * uy : Math.hypot(ax, ay);
		let side = v > 1e-6 ? -ax * uy + ay * ux : 0;
		const fx =
			v > 1e-6 ? ux : Math.hypot(ax, ay) > 1e-9 ? ax / Math.hypot(ax, ay) : 0;
		const fy =
			v > 1e-6 ? uy : Math.hypot(ax, ay) > 1e-9 ? ay / Math.hypot(ax, ay) : 0;
		along = Math.max(-BRAKE, Math.min(pushFrom(v), along));
		side = Math.max(-TURN, Math.min(TURN, side));
		ax = fx * along - fy * side;
		ay = fy * along + fx * side;
		vx += ax * dt;
		vy += ay * dt;
		p = { x: p.x + vx * dt, y: p.y + vy * dt };
		// Something of his coming up: eased onto where the play has him for
		// it, there as it starts.
		if (due - t < ONTO_MS) {
			const u = 1 - Math.max(0, due - t) / ONTO_MS;
			const k = u * u * (3 - 2 * u);
			p = { x: p.x + (W.x - p.x) * k, y: p.y + (W.y - p.y) * k };
			vx += (wx - vx) * k;
			vy += (wy - vy) * k;
		}
		was = W;
		out.push({ t, x: p.x, y: p.y, vx, vy });
	}
	return out;
};

// The samples as runs: a knot wherever the way bends, or his pace changes,
// more than a hair - each run with his pace at either end of it.
export const toMoves = (
	s: Sample[],
	animAt: (t: number) => Pick<Move, "anim" | "face"> | undefined,
	// (Where the play has him doing something: laid down to the step.)
	pinned: Spans = spansAt([]),
): Move[] => {
	const moves: Move[] = [];
	const pace = (q: Sample) => Math.hypot(q.vx, q.vy);
	// (Stood somewhere else than his last run left him - back on the floor,
	// the picture cut: put there, a run that goes nowhere.)
	const place = (q: Sample) => {
		const last = moves.at(-1);
		if (!last || Math.hypot(last.to.x - q.x, last.to.y - q.y) > 0.05) {
			moves.push({
				t0: q.t,
				t1: q.t + 1,
				from: { x: q.x, y: q.y },
				to: { x: q.x, y: q.y },
				anim: animAt(q.t)?.anim ?? "walk",
				v0: 0,
				v1: 0,
			});
		}
	};
	let i = 0;
	while (i < s.length - 1) {
		// (Stood: no run.)
		if (
			pace(s[i]!) < STILL &&
			Math.hypot(s[i + 1]!.x - s[i]!.x, s[i + 1]!.y - s[i]!.y) < 0.01
		) {
			place(s[i]!);
			i += 1;
			continue;
		}
		// As far on as one straight run covers it: the way bending no more
		// than a hair off it, his pace only getting quicker or only slower
		// along it (how the court lays a run out - see runShape), and none
		// too long.
		// (Not across a jump - the picture cutting: he is just there.)
		if (
			Math.hypot(s[i + 1]!.x - s[i]!.x, s[i + 1]!.y - s[i]!.y) /
				((s[i + 1]!.t - s[i]!.t) / 1000) >
			TOP * 1.2
		) {
			i += 1;
			place(s[i]!);
			continue;
		}
		let j = i + 1;
		let trend = 0;
		while (
			j + 1 < s.length &&
			s[j + 1]!.t - s[j]!.t <= STEP_MS + 1 &&
			s[j + 1]!.t - s[i]!.t <= RUN_MOST
		) {
			const a = s[i]!;
			const b = s[j + 1]!;
			const L = Math.hypot(b.x - a.x, b.y - a.y);
			let ok =
				pace(b) >= STILL &&
				Math.hypot(b.x - s[j]!.x, b.y - s[j]!.y) / ((b.t - s[j]!.t) / 1000) <=
					TOP * 1.2;
			const tol = pinned.hit(s[i]!.t, b.t) ? KNOT_PINNED : KNOT_TOL;
			// (Off the line, or along it ahead of or behind where a pace
			// changing evenly from one end's to the other's would have him.)
			const T = (b.t - a.t) / 1000;
			const v0 = pace(a);
			const v1 = pace(b);
			const mean = ((v0 + v1) / 2) * T;
			for (let k = i + 1; k <= j && ok && L > 1e-6; k++) {
				const q = s[k]!;
				const tau = (q.t - a.t) / 1000;
				const along =
					((q.x - a.x) * (b.x - a.x) + (q.y - a.y) * (b.y - a.y)) / L;
				const model =
					mean > 1e-6
						? (L * (v0 * tau + ((v1 - v0) / T) * tau * tau * 0.5)) / mean
						: L * (tau / T);
				ok =
					Math.abs(
						((q.x - a.x) * (b.y - a.y) - (q.y - a.y) * (b.x - a.x)) / L,
					) < tol && Math.abs(along - model) < tol;
			}
			const dv = pace(b) - pace(s[j]!);
			if (Math.abs(dv) > PACE_NOISE) {
				const sign = Math.sign(dv);
				ok &&= trend === 0 || trend === sign;
				trend = trend || sign;
			}
			if (!ok) {
				break;
			}
			j += 1;
		}
		const a = s[i]!;
		const b = s[j]!;
		if (b.t - a.t > STEP_MS + 1 || Math.hypot(b.x - a.x, b.y - a.y) >= 0.01) {
			const mid = animAt((a.t + b.t) / 2);
			const v = Math.hypot(b.x - a.x, b.y - a.y) / ((b.t - a.t) / 1000);
			moves.push({
				t0: a.t,
				t1: b.t,
				from: { x: a.x, y: a.y },
				to: { x: b.x, y: b.y },
				anim: mid?.anim ?? (v < 5 ? "walk" : v < 10 ? "jog" : "run"),
				...(mid?.face === undefined ? {} : { face: mid.face }),
				// (From standing, or pulled up to stand: said so - the court
				// would otherwise run the pause between two runs into one.)
				v0: pace(a) < STILL ? 0 : pace(a),
				v1: pace(b) < STILL ? 0 : pace(b),
			});
		}
		i = j;
	}
	return moves;
};

// The whole game stepped: every man's tracks re-laid as he would really
// move them. (In place.)
export const stepTimeline = (tl: CourtTimeline): CourtTimeline => {
	for (const tr of tl.tracks.values()) {
		if (tr.pid < 0 || tr.moves.length === 0) {
			continue;
		}
		const held = spansAt(tr.acts.map((x) => [x.t0, x.t1] as const));
		const cut = spansAt(tl.cuts.map((c) => [c - STEP_MS, c] as const));
		// (Only while he is out on the floor.)
		const samples: Sample[] = [];
		const begin = Math.max(0, tr.moves[0]!.t0 - 1000);
		const end = tr.moves.at(-1)!.t1 + 1000;
		tr.shown.forEach(([at, on], k) => {
			const from = Math.max(begin, at);
			const until = Math.min(end, tr.shown[k + 1]?.[0] ?? end);
			if (!on || !(until > from)) {
				return;
			}
			// (Where the play has him, worked out once a step - and between
			// two steps, a straight line.)
			const n = Math.ceil(
				(until - from + LOOK_MOST * 1000 + 2 * STEP_MS) / STEP_MS,
			);
			const spots: (Pt | undefined)[] = [];
			for (let k = 0; k <= n; k++) {
				spots.push(floorSpotOf(tr, from + k * STEP_MS));
			}
			const wantAt = (t: number): Pt | undefined => {
				const f = (t - from) / STEP_MS;
				const k = Math.floor(f);
				const a = spots[k];
				const b = spots[k + 1];
				if (!a || !b) {
					return a ?? (k >= 0 ? floorSpotOf(tr, t) : undefined);
				}
				const u = f - k;
				return { x: a.x + (b.x - a.x) * u, y: a.y + (b.y - a.y) * u };
			};
			for (const q of follow(
				wantAt,
				from,
				until - 1,
				held,
				cut,
				(t) =>
					REACT_MS[0] +
					(REACT_MS[1] - REACT_MS[0]) *
						hash01(tr.pid * 17 + 3, Math.round(t / 100)),
			)) {
				samples.push(q);
			}
		});
		const old = tr.moves;
		const animAt = (t: number) => {
			let lo = 0;
			let hi = old.length;
			while (lo < hi) {
				const mid = (lo + hi) >> 1;
				if (old[mid]!.t0 <= t) {
					lo = mid + 1;
				} else {
					hi = mid;
				}
			}
			const m = old[lo - 1];
			return m && t <= m.t1 + STEP_MS ? m : undefined;
		};
		// (Off the floor, where it had him: the runs it laid there stay.)
		const off = old.filter((m) => !floorSpotOf(tr, (m.t0 + m.t1) / 2));
		tr.moves = [...toMoves(samples, animAt, held), ...off].sort(
			(a, b) => a.t0 - b.t0,
		);
		delete tr.nudges;
		delete tr.sep;
	}
	return tl;
};

// A game staged, then stepped.
export const compileStepped = (
	...args: Parameters<typeof compileCourt>
): CourtTimeline => stepTimeline(compileCourt(...args));
