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

import type { BallSeg, CourtTimeline, Move } from "./director.ts";
import { beatsAt, beatTime, dribbleStart, floorSpotOf } from "./evaluate.ts";
import type { Hand } from "./poses.ts";
import { COURT_H, COURT_W, inPlay, rimX, type Pt } from "./geometry.ts";
import { RIM_INSET } from "../courtSpots.ts";
import { alongShape, pushFrom, runShape } from "./motion.ts";

export const STEP_MS = 50;

const unitVec = (a: Pt, b: Pt): Pt => {
	const d = Math.hypot(b.x - a.x, b.y - a.y) || 1;
	return { x: (b.x - a.x) / d, y: (b.y - a.y) / d };
};

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
// over this long before (ms) ...
const ONTO_MS = 500;
// (... the gap to it closed this long (ms) before it starts: on it, going
// as it goes, for the last of it.)
const ONTO_SETTLE = 100;
// (Further off it than that would take, sooner: over as long as it takes
// at this much quicker than he is going (feet a second), up to this long.)
const ONTO_PACE = 2;
const ONTO_MOST = 1200;
// (Pulling up into it no harder than this, feet a second a second.)
const ONTO_BRAKE = 40;
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
// (Seeing it this far (ms) ahead.)
const REACT_SEE = 150;
// (From this long before a shot leaves his hand (ms) to this long after it
// comes down off the rim, no beat to it: see stepTimeline.)
const SHOT_BEFORE = 300;
const SHOT_AFTER = 1500;
// Fastest anybody goes (feet a second).
const TOP = 30;
// Further off where the play wants him than this (feet), something has
// gone wrong (a cut, a man brought on): he is just there.
const JUMP = 6;
// A path laid down as straight runs where it bends no more than this
// (feet).
const KNOT_TOL = 0.4;
const KNOT_PINNED = 0.02;
// ... each no longer than this (ms), its pace not changing back and forth
// by more than this (feet a second) along it.
const RUN_MOST = 1500;
const PACE_NOISE = 0.25;
// Under this pace (feet a second) he is stood.
const STILL = 0.2;
// Seeing nothing ahead, he still sees where he will be stood this long
// (ms) from now - and pulls up for it at this share of all he has.
const STOP_SEE = 500;
const STOP_SHARE = 1;
// (Running at him, that is - going faster than this, feet a second: not
// sliding with his man.)
const STOP_FAST = 8;
// Over a line the play has him inside, drawn back in no faster than this
// (feet a second).
const LINE_PULL = 4;
// Pulling up to stand somewhere: as evenly as this (feet a second, a
// second), and set there within this far of it (feet) going no faster than
// this (feet a second).
const SET_BRAKE = 12;
const SET_AT = 0.04;
const SET_PACE = 0.8;
// A corner in his way: the way in and the way out over this long (ms)
// either side, each at least this far (feet), turned through more than
// this (the cosine between them) - a cut, not a bend: he runs it to the
// corner, plants, and goes, no faster through it than CORNER_PACE (feet a
// second) times how little it turns (see cornersOf).
const CORNER_MS = 300;
const CORNER_LEAST = 1.5;
const CORNER_COS = 0.5;
const CORNER_PACE = 12;
// (At the corner, within this (feet), or this long (ms) after the play
// was there, he is round it.)
const CORNER_AT = 0.6;
const CORNER_LATE = 400;

type Sample = { t: number; x: number; y: number; vx: number; vy: number };

// (Of the game, what stepping it goes by.)
type Stepped = Pick<
	CourtTimeline,
	"tracks" | "ball" | "poss" | "cuts" | "beats" | "end"
>;

// When something of his is going on - his acts, the picture's cuts - as
// stretches of time.
type Spans = {
	// Whether any of them is going on between t0 and t1.
	hit: (t0: number, t1: number) => boolean;
	// When the next one begins, from t on.
	next: (t: number) => number;
	// When the last one before t was over (t, in one).
	last: (t: number) => number;
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
		last: (t) => {
			const k = from(t) - 1;
			return k < 0 ? -Infinity : Math.min(t, spans[k]![1]);
		},
	};
};

// Where a way turns hard - a cut - between t0 and t1: when the play has
// him at the corner, where it is, and how hard it turns (the cosine).
type Corner = { t: number; at: Pt; cos: number };
const cornersOf = (
	want: (t: number) => Pt | undefined,
	t0: number,
	t1: number,
): Corner[] => {
	const k = Math.round(CORNER_MS / STEP_MS);
	const from = t0 - CORNER_MS;
	const spots: (Pt | undefined)[] = [];
	for (let t = from; t <= t1 + CORNER_MS; t += STEP_MS) {
		spots.push(want(t));
	}
	const out: Corner[] = [];
	let best: Corner | undefined;
	for (let i = k; i + k < spots.length; i++) {
		const A = spots[i - k];
		const P = spots[i];
		const B = spots[i + k];
		let cos = 1;
		if (A && P && B) {
			const ix = P.x - A.x;
			const iy = P.y - A.y;
			const ox = B.x - P.x;
			const oy = B.y - P.y;
			const li = Math.hypot(ix, iy);
			const lo = Math.hypot(ox, oy);
			if (li >= CORNER_LEAST && lo >= CORNER_LEAST) {
				cos = (ix * ox + iy * oy) / (li * lo);
			}
		}
		if (cos < CORNER_COS) {
			if (!best || cos < best.cos) {
				best = { t: from + i * STEP_MS, at: P!, cos };
			}
		} else if (best) {
			out.push(best);
			best = undefined;
		}
	}
	if (best) {
		out.push(best);
	}
	return out;
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
	// How far ahead (s) he can see where the play has him at t: none, for
	// a man reacting to another (he goes where the play has him now - it has
	// him reacting already: see mark in director.ts).
	seeAt: (t: number) => number = () => LOOK_MOST,
	// When there is no beat to it: everybody going for the shot as the play
	// has them going (see stepTimeline).
	sharp: Spans = spansAt([]),
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
	// (Being eased onto the play for something of his: when it starts, and
	// over how long.)
	let onto: { due: number; window: number } | undefined;
	// (The cuts in his way, and the next he is coming to.)
	const corners = cornersOf(want, t0, t1);
	let ci = 0;
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
		const see = seeAt(t);
		const look =
			see <= 0 || (v0 < 1 && Math.hypot(wx, wy) < 0.5)
				? 0
				: Math.max(LOOK_LEAST, Math.min(LOOK_MOST, see, (v0 / BRAKE) * 0.9));
		let L = look > 0 ? (want(t + look * 1000) ?? W) : W;
		let L1 = look > 0 ? (want(t + look * 1000 + STEP_MS) ?? L) : (ahead ?? W);
		let reach = Math.max(look, LOOK_LEAST);
		// A cut coming up in that: not round it - to the corner, and through
		// it no faster than he can turn it.
		while (
			ci < corners.length &&
			(Math.hypot(corners[ci]!.at.x - p.x, corners[ci]!.at.y - p.y) <
				CORNER_AT ||
				t > corners[ci]!.t + CORNER_LATE)
		) {
			ci += 1;
		}
		let plant = TOP;
		const C = corners[ci];
		if (C && look > 0 && C.t < t + look * 1000) {
			L = C.at;
			L1 = C.at;
			reach = Math.max(LOOK_LEAST, (C.t - t) / 1000);
			plant =
				Math.max(2, CORNER_PACE * (0.5 + C.cos)) +
				Math.sqrt(2 * BRAKE * Math.hypot(C.at.x - p.x, C.at.y - p.y));
		}
		let cx = (L.x - p.x) / reach + (look > 0 ? 0 : wx);
		let cy = (L.y - p.y) / reach + (look > 0 ? 0 : wy);
		const lv = Math.hypot(L1.x - L.x, L1.y - L.y) / dt;
		// Where he is going to stand: pulled up into it evenly, and set -
		// not crept up on.
		const stop = lv < STILL && Math.hypot(wx, wy) < 0.5;
		const toStop = Math.hypot(L.x - p.x, L.y - p.y);
		if (stop && toStop > 1e-6) {
			const sv = Math.min(
				Math.hypot(cx, cy) + 1,
				Math.sqrt(2 * SET_BRAKE * toStop),
			);
			cx = ((L.x - p.x) / toStop) * sv;
			cy = ((L.y - p.y) / toStop) * sv;
		}
		{
			const most = Math.min(plant, Math.max(lv, Math.hypot(wx, wy)) + CATCH_UP);
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
		// (How fast the play has him going over the moment to come: setting
		// off, it eases him in - the first step of it is not yet going.)
		const soon = want(t + REACT_SEE) ?? W;
		const going = Math.max(
			Math.hypot(wx, wy),
			(Math.hypot(soon.x - W.x, soon.y - W.y) * 1000) / REACT_SEE,
		);
		if (Math.hypot(vx, vy) < 1 && going > 0.5) {
			if (!off) {
				// (Sent off flat out, he goes the quicker: his beat to react
				// shorter the faster the play has him going.)
				// (Stood out of bounds - he has just taken it in, or is coming
				// on: nothing to react to - he goes with the play.)
				const out = p.x < 0 || p.x > COURT_W || p.y < 0 || p.y > COURT_H;
				const lag =
					out || sharp.hit(t, t + 1)
						? 0
						: lagAt(t) * Math.min(1, REACT_PACE / Math.max(going, 2));
				// (Counted from when the play really has him going - seen
				// coming, he is stood still till then all the same.)
				const from = Math.hypot(wx, wy) > 0.5 ? t : t + REACT_SEE;
				off =
					due0 - from > lag + REACT_ROOM
						? { t: from, lag }
						: { t: from, lag: 0 };
			}
		} else if (going < 0.5) {
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
		// Going where the play has him now, seeing none of it ahead (see
		// seeAt) - but where it stands him a moment from now, he sees that
		// coming: no faster than he can pull up there (a closeout, chopping
		// his feet - not run past it and back).
		if (see <= 0 && v0 > STOP_FAST) {
			const F = want(t + STOP_SEE);
			const F1 = want(t + STOP_SEE + STEP_MS);
			if (F && F1 && Math.hypot(F1.x - F.x, F1.y - F.y) / dt < STILL) {
				cap = Math.min(
					cap,
					Math.sqrt(2 * BRAKE * STOP_SHARE * Math.hypot(F.x - p.x, F.y - p.y)),
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
		// Something of his coming up: eased onto where the play has him for
		// it, there as it starts - his pace bent round to what gets him there
		// then, more and more of it as it comes. (As much sooner as he is
		// further off it - no more than ONTO_PACE quicker than he was going
		// - and once begun for it, kept to.)
		// (How far off it he is: where the play has him, against where he
		// would be going on as it goes - the step it takes this step no gap.)
		const gx = W.x - wx * dt - p.x;
		const gy = W.y - wy * dt - p.y;
		if (onto?.due !== due) {
			const window = Math.min(
				ONTO_MOST,
				Math.max(ONTO_MS, (Math.hypot(gx, gy) / ONTO_PACE) * 1000),
			);
			onto = due - t < window ? { due, window } : undefined;
		}
		if (onto) {
			const rem = Math.max(0, due - t - ONTO_SETTLE) / 1000 + dt;
			const u = 1 - Math.max(0, due - t) / onto.window;
			const k = u * u * (3 - 2 * u);
			// (Back onto the play's own way there - going as it goes, pulling
			// up as it does, the gap to it closed by then - not on at
			// whatever pace gets him to the spot and stopped dead as it
			// starts.)
			let tx = wx + (2 * gx) / rem;
			let ty = wy + (2 * gy) / rem;
			// (No quicker than he can pull up from to the pace the play has
			// him going there as it starts: the play pulling up to stand for
			// it, he is not hurried on past its pace to then stop dead.)
			const Q = want(due);
			const Q1 = want(due + STEP_MS);
			if (Q && Q1) {
				const qv = Math.hypot(Q1.x - Q.x, Q1.y - Q.y) / dt;
				const most = Math.sqrt(
					qv * qv + 2 * ONTO_BRAKE * Math.hypot(Q.x - p.x, Q.y - p.y),
				);
				const tv = Math.hypot(tx, ty);
				if (tv > most) {
					tx *= most / tv;
					ty *= most / tv;
				}
			}
			vx += (tx - vx) * k;
			vy += (ty - vy) * k;
		}
		p = { x: p.x + vx * dt, y: p.y + vy * dt };
		// Wanted inside the lines and over one: drawn back in, a step at a
		// time - never put there.
		if (W.x >= 0 && W.x <= COURT_W && W.y >= 0 && W.y <= COURT_H) {
			const ix = Math.min(COURT_W, Math.max(0, p.x));
			const iy = Math.min(COURT_H, Math.max(0, p.y));
			const over = Math.hypot(ix - p.x, iy - p.y);
			if (over > 0) {
				const k = Math.min(1, (LINE_PULL * dt) / over);
				p = { x: p.x + (ix - p.x) * k, y: p.y + (iy - p.y) * k };
			}
		}
		if (
			stop &&
			Math.hypot(L.x - p.x, L.y - p.y) < SET_AT &&
			Math.hypot(vx, vy) < SET_PACE
		) {
			p = { x: L.x, y: L.y };
			vx = 0;
			vy = 0;
		}
		was = W;
		out.push({ t, x: p.x, y: p.y, vx, vy });
	}
	return out;
};

// From sample i, on to where he next stands, as one run the way the court
// runs one (see runShape): where it stands him, if the way there is
// straight and the court's run has him within a hair of every step of it
// - otherwise i.
const WHOLE_ALONG = 0.4;
const WHOLE_MOST = 3000;
const wholeRun = (
	s: Sample[],
	i: number,
	pinned: Spans,
	// (For each sample, the next he stands at or is put at - see toMoves.)
	stand: Int32Array,
): number => {
	const a = s[i]!;
	const e = stand[i]!;
	if (
		e >= s.length ||
		e - i < 3 ||
		s[e]!.t - a.t > WHOLE_MOST ||
		s[e]!.t - s[e - 1]!.t > STEP_MS + 1
	) {
		return i;
	}
	const b = s[e]!;
	const L = Math.hypot(b.x - a.x, b.y - a.y);
	if (L < 0.05) {
		return i;
	}
	const tol = pinned.hit(a.t, b.t) ? KNOT_PINNED : KNOT_TOL;
	const tolAlong = pinned.hit(a.t, b.t) ? KNOT_PINNED : WHOLE_ALONG;
	const ux = (b.x - a.x) / L;
	const uy = (b.y - a.y) / L;
	// (Straight, first - cheap - then as the court would run it.)
	for (let k = i + 1; k < e; k++) {
		const q = s[k]!;
		if (Math.abs((q.x - a.x) * uy - (q.y - a.y) * ux) > tol) {
			return i;
		}
	}
	const v0 = Math.hypot(a.vx, a.vy);
	const shape = runShape(L, (b.t - a.t) / 1000, v0 < STILL ? 0 : v0, 0);
	for (let k = i + 1; k < e; k++) {
		const q = s[k]!;
		const along = (q.x - a.x) * ux + (q.y - a.y) * uy;
		if (Math.abs(along - alongShape(shape, (q.t - a.t) / 1000)) > tolAlong) {
			return i;
		}
	}
	return e;
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
	// (Each sample's next stood one, or one put down after a gap.)
	const stand = new Int32Array(s.length);
	for (let k = s.length - 1, next = s.length; k >= 0; k--) {
		stand[k] = next;
		if (pace(s[k]!) < STILL || (k > 0 && s[k]!.t - s[k - 1]!.t > STEP_MS + 1)) {
			next = k;
		}
	}
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
		// (Not across a jump - the picture cutting: he is just there - nor
		// a gap, off the floor between: he is back, there.)
		if (
			s[i + 1]!.t - s[i]!.t > STEP_MS + 1 ||
			Math.hypot(s[i + 1]!.x - s[i]!.x, s[i + 1]!.y - s[i]!.y) /
				((s[i + 1]!.t - s[i]!.t) / 1000) >
				TOP * 1.2
		) {
			i += 1;
			place(s[i]!);
			continue;
		}
		// Going somewhere and stopping there - a step, a jab, a dash -
		// straight: one run, the way the court runs it (getting going, on,
		// pulling up: see runShape), if that has him where the steps do.
		const whole = wholeRun(s, i, pinned, stand);
		if (whole > i) {
			// (Turned on the way to face somebody - a defender sliding in on
			// his man - not run over: cut where the play turns him, so he gets
			// there facing as it has him.)
			const face = animAt(s[i]!.t)?.face;
			let turn = i + 1;
			while (turn < whole && animAt(s[turn]!.t)?.face === face) {
				turn += 1;
			}
			for (const [a, b, last] of turn < whole
				? ([
						[s[i]!, s[turn]!, false],
						[s[turn]!, s[whole]!, true],
					] as const)
				: ([[s[i]!, s[whole]!, true]] as const)) {
				const mid = animAt((a.t + b.t) / 2);
				const v = Math.hypot(b.x - a.x, b.y - a.y) / ((b.t - a.t) / 1000);
				moves.push({
					t0: a.t,
					t1: b.t,
					from: { x: a.x, y: a.y },
					to: { x: b.x, y: b.y },
					anim: mid?.anim ?? (v < 5 ? "walk" : v < 10 ? "jog" : "run"),
					...(mid?.face === undefined ? {} : { face: mid.face }),
					v0: pace(a) < STILL ? 0 : pace(a),
					v1: last || pace(b) < STILL ? 0 : pace(b),
				});
			}
			i = whole;
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
		} else {
			// (Going nowhere yet: there, all the same.)
			place(a);
		}
		i = j;
	}
	return pullUpsIn(moves, pinned);
};

// A short run pulling up to all but stand, going on the way the run before
// it went: run together with that one - one run that pulls up at the end of
// it, his strides shortening into the stop over it (not short all at once
// where the second began: see shortSteps in evaluate.ts).
const PULL_UP_TURN = Math.cos((25 * Math.PI) / 180);
const PULL_UP_LONGEST = 4;
const pullUpsIn = (moves: Move[], pinned: Spans): Move[] => {
	const out: Move[] = [];
	for (const m of moves) {
		const a = out.at(-1);
		const v0 = m.v0 ?? 0;
		const v1 = m.v1 ?? 0;
		const L = Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);
		const La = a ? Math.hypot(a.to.x - a.from.x, a.to.y - a.from.y) : 0;
		if (
			a &&
			a.t1 === m.t0 &&
			L > 0.05 &&
			La > 0.05 &&
			L <= PULL_UP_LONGEST &&
			v0 >= 4 &&
			v1 < v0 * 0.6 &&
			(a.v1 ?? 0) > 0 &&
			a.face === m.face &&
			!pinned.hit(a.t0, m.t1) &&
			((a.to.x - a.from.x) * (m.to.x - m.from.x) +
				(a.to.y - a.from.y) * (m.to.y - m.from.y)) /
				(L * La) >
				PULL_UP_TURN
		) {
			out[out.length - 1] = { ...a, t1: m.t1, to: m.to, v1: m.v1 };
			continue;
		}
		out.push(m);
	}
	return out;
};

// Where a man is, step by step over a stretch of time: as a lookup between
// the steps.
type Path = {
	from: number;
	spots: (Pt | undefined)[];
	at: (t: number) => Pt | undefined;
};
const pathOf = (from: number, spots: (Pt | undefined)[]): Path => ({
	from,
	spots,
	at: (t) => {
		const f = (t - from) / STEP_MS;
		const k = Math.floor(f);
		const a = spots[k];
		const b = spots[k + 1];
		// (Coming on or going off between two steps: where he is on the
		// floor, either side.)
		if (!a || !b) {
			return a ?? b;
		}
		const u = f - k;
		return { x: a.x + (b.x - a.x) * u, y: a.y + (b.y - a.y) * u };
	},
});

// THE MAN WITH THE BALL, WORKING HIS MAN. On his dribble out on the floor
// a while with nothing of his to do, he is never just carried from one
// spot to the next. Stood - waiting on a screen, on the set - he sways
// across his man, jabs a step in at him and back, gives ground a step and
// comes again. Going somewhere, he gets there his own way: crosses over
// from one side of his line to the other, hesitates and goes. Each move
// his own, a beat each, eased in and out of, so he is back where the play
// wants him for whatever comes next. (His man goes with him, a beat late:
// see stepTimeline.)
const PROBE_LEAST = 1200;
const PROBE_EASE = 400;
const PROBE_BEAT: [number, number] = [700, 1100];
// (Across his man, in at him and back - feet, his own way of it.)
const PROBE_MOVES: [number, number][] = [
	[1.2, 0],
	[-1.2, 0],
	[0.3, 1.4],
	[-0.3, 1.4],
	[0, -1.2],
	[0.9, 0.7],
	[-0.9, 0.7],
];
// Going (faster than this, feet a second, no faster than ROUTE_FASTEST):
// across his line (feet, to one side or the other - crossing over from
// one to the other, as often as not) and along it (behind it a beat: a
// hesitation; ahead of it: the burst after) - a beat each.
const ROUTE_PACE = 1.5;
const ROUTE_FASTEST = 18;
const ROUTE_BEAT: [number, number] = [550, 900];
const ROUTE_ACROSS: [number, number] = [1, 2.2];
const ROUTE_ALONG = [0, 0, -1.1, 0.6];
const ROUTE_CROSS = 0.65;
// (All of it within this far of the rim, feet.)
const WORK_NEAR = 40;
// THE BALL GOES WITH HIM. Going one way and then the other across his
// line (see probesOf), a man takes the ball across into the hand on the
// side he heads for - a crossover in front, on the next top of his dribble
// as he sets off that way (CROSS_ON_CUT of the time: now and then he just
// keeps it where it is) - and has it back in the hand he started in by the
// time he is done (the last of them left out if not), whatever comes next
// of his finding it where it was.
// (Only through a stretch of nothing but his plain dribble, in the one
// hand.)
const CROSS_ON_CUT = 0.75;
// (A move that takes him this far across his line, at the least, feet.)
const CUT_ACROSS = 0.8;
// The crossover done this long (ms) before his next piece of the ball, and
// the stretch, at the least.
const CROSS_CLEAR = 150;
type Crossed = { at: number; pieces: BallSeg[]; was: [BallSeg, Hand][] };
type Cut = { t0: number; t1: number; to: Hand };
const crossOnCuts = (
	tl: Stepped,
	pid: number,
	a: number,
	b: number,
	cuts: Cut[],
) => {
	const ball = tl.ball;
	const indexAt = (t: number): number => {
		let lo = 0;
		let hi = ball.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (ball[mid]!.t0 <= t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		return lo - 1;
	};
	const plain = (s: BallSeg | undefined) =>
		s?.kind === "hold" && s.pid === pid && s.style === "dribble";
	const first = indexAt(a);
	if (first < 0) {
		return;
	}
	// (His dribble all the one hand through it, too: not taken across to
	// the other already somewhere in it.)
	const h0 = (ball[first] as { hand?: Hand }).hand ?? "R";
	for (let k = first; k < ball.length && ball[k]!.t0 < b; k++) {
		const s = ball[k];
		if (!plain(s) || ((s as { hand?: Hand }).hand ?? "R") !== h0) {
			return;
		}
	}
	let h = h0;
	const done: Crossed[] = [];
	// Across from `h`, the first top of his dribble from t on - done by
	// `until`, and clear of the next piece of the ball.
	const cross = (
		t: number,
		latest: number,
		until: number,
	): Crossed | undefined => {
		const j = indexAt(t);
		if (!plain(ball[j])) {
			return undefined;
		}
		const start = dribbleStart(ball, j);
		const n = Math.ceil(beatsAt(ball, start, t) - 1e-6);
		const T = beatTime(ball, start, n);
		if (T > latest) {
			return undefined;
		}
		const jj = indexAt(T);
		const seg = ball[jj];
		if (!plain(seg) || seg!.kind !== "hold" || T - seg!.t0 < 1) {
			return undefined;
		}
		// (A bounce of his dribble's beat - so it comes up on the beat it
		// was always going to, whatever was timed to it after.)
		const end = beatTime(ball, start, n + 1);
		if (end > until || (ball[jj + 1]?.t0 ?? Infinity) < end + CROSS_CLEAR) {
			return undefined;
		}
		const to: Hand = h === "R" ? "L" : "R";
		const pieces: BallSeg[] = [
			{
				kind: "hold",
				t0: T,
				pid,
				style: "cross",
				hand: h,
				move: "front",
				rate: 1000 / (end - T),
			},
			{
				kind: "hold",
				t0: end,
				pid,
				style: "dribble",
				hand: to,
				...(seg.rate !== undefined ? { rate: seg.rate } : {}),
			},
		];
		ball.splice(jj + 1, 0, ...pieces);
		// (The rest of his dribble through the stretch in that hand now.)
		const was: [BallSeg, Hand][] = [];
		for (let k = jj + 3; k < ball.length && ball[k]!.t0 < b; k++) {
			const s = ball[k]!;
			if (s.kind === "hold") {
				was.push([s, s.hand ?? "R"]);
				s.hand = to;
			}
		}
		return { at: T, pieces, was };
	};
	for (const cut of cuts) {
		if (
			cut.to === h ||
			hash01(pid * 13 + 7, Math.round(cut.t0 / 50)) >= CROSS_ON_CUT
		) {
			continue;
		}
		// (As he sets off that way: by halfway there.)
		const c = cross(cut.t0, (cut.t0 + cut.t1) / 2, b - CROSS_CLEAR);
		if (c) {
			done.push(c);
			h = cut.to;
		}
	}
	if (h !== h0) {
		// (Not back in the hand he started in: not taken across the last
		// time.)
		const c0 = done.pop()!;
		for (const [s, hand] of c0.was) {
			(s as { hand?: Hand }).hand = hand;
		}
		ball.splice(ball.indexOf(c0.pieces[0]!), 2);
	}
};

const probesOf = (
	tl: Stepped,
	pid: number,
	laid: Path,
	held: Spans,
	team: 0 | 1,
): Offsets => {
	const rim = { x: rimX(team), y: 25 };
	// When he has it on his dribble - not stood through a move.
	const dribbling: [number, number][] = [];
	tl.ball.forEach((s, k) => {
		if (s.kind === "hold" && s.pid === pid && s.style !== "hold" && !s.still) {
			dribbling.push([s.t0, tl.ball[k + 1]?.t0 ?? s.t0]);
		}
	});
	const on = spansAt(dribbling);
	// Each stretch of it with nothing of his on: his moves through it.
	const stretches: Keyed[] = [];
	const begin = laid.from;
	// (Where the play has him going at t, feet a second.)
	const goingAt = (t: number): Pt => {
		const P = laid.at(t - 150);
		const Q = laid.at(t + 150);
		return P && Q
			? { x: (Q.x - P.x) / 0.3, y: (Q.y - P.y) / 0.3 }
			: { x: 0, y: 0 };
	};
	let a = -1;
	const close = (b: number) => {
		if (a >= 0 && b - a >= PROBE_LEAST) {
			const keys: [number, Pt][] = [[a, { x: 0, y: 0 }]];
			let t = a + PROBE_EASE;
			let n = 0;
			let side = hash01(pid * 5 + 1, Math.round(a)) < 0.5 ? -1 : 1;
			while (t < b - PROBE_EASE - 500) {
				const r = hash01(pid * 31 + n, Math.round(t / 50));
				const pick = hash01(pid * 7 + n, Math.round(a));
				const v = goingAt(t);
				const pace = Math.hypot(v.x, v.y);
				if (pace < ROUTE_PACE) {
					// Stood: at his man.
					const u = unitVec(laid.at(t) ?? rim, rim);
					const [across, inAt] =
						PROBE_MOVES[Math.floor(pick * PROBE_MOVES.length)]!;
					keys.push([
						t,
						{
							x: -u.y * across + u.x * inAt,
							y: u.x * across + u.y * inAt,
						},
					]);
					t += PROBE_BEAT[0] + (PROBE_BEAT[1] - PROBE_BEAT[0]) * r;
				} else {
					// Going: his own way along his line.
					const u = { x: v.x / pace, y: v.y / pace };
					if (hash01(pid * 11 + n, Math.round(a)) < ROUTE_CROSS) {
						side = -side;
					}
					const across =
						side *
						(ROUTE_ACROSS[0] + (ROUTE_ACROSS[1] - ROUTE_ACROSS[0]) * pick);
					const along = ROUTE_ALONG[Math.floor(r * ROUTE_ALONG.length)]!;
					keys.push([
						t,
						{
							x: -u.y * across + u.x * along,
							y: u.x * across + u.y * along,
						},
					]);
					t += ROUTE_BEAT[0] + (ROUTE_BEAT[1] - ROUTE_BEAT[0]) * r;
				}
				n += 1;
			}
			keys.push([b, { x: 0, y: 0 }]);
			if (keys.length > 2) {
				stretches.push({ a, b, keys });
				// Each move from one key to the next that takes him a good way
				// across his line, going: which way, and from when to when.
				const cuts: Cut[] = [];
				for (let k = 1; k < keys.length; k++) {
					const [t0, A] = keys[k - 1]!;
					const [t1, B] = keys[k]!;
					const v = goingAt(t0 + 100);
					const pace = Math.hypot(v.x, v.y);
					if (pace < ROUTE_PACE) {
						continue;
					}
					// (His left, going the way he goes.)
					const d = ((B.x - A.x) * -v.y + (B.y - A.y) * v.x) / pace;
					if (Math.abs(d) >= CUT_ACROSS) {
						cuts.push({ t0, t1, to: d > 0 ? "L" : "R" });
					}
				}
				crossOnCuts(tl, pid, a, b, cuts);
			}
		}
		a = -1;
	};
	for (let k = 0; k + 1 < laid.spots.length; k++) {
		const t = begin + k * STEP_MS;
		const P = laid.spots[k];
		const Q = laid.spots[k + 1];
		const ok =
			P !== undefined &&
			Q !== undefined &&
			Math.hypot(Q.x - P.x, Q.y - P.y) / (STEP_MS / 1000) < ROUTE_FASTEST &&
			on.hit(t, t + STEP_MS) &&
			!held.hit(t - 300, t + STEP_MS + 300) &&
			Math.hypot(P.x - rim.x, P.y - rim.y) < WORK_NEAR;
		if (ok && a < 0) {
			a = t;
		} else if (!ok) {
			close(t);
		}
	}
	close(begin + laid.spots.length * STEP_MS);
	return keyed(stretches);
};

// BRINGING IT UP. Out past where he works his man (WORK_NEAR), on his way
// up the floor with it, a man doesn't carry it all the way in the one hand:
// every few seconds he takes it across to the other (see crossOnCuts) -
// and back, by the time he gets where he is going.
const SWITCH_EVERY: [number, number] = [2200, 4200];
// (On his way at this pace at least, feet a second; for this long at least,
// ms.)
const SWITCH_PACE = 3;
const SWITCH_LEAST = 2600;
const switchesOf = (
	tl: Stepped,
	pid: number,
	laid: Path,
	held: Spans,
	team: 0 | 1,
) => {
	const rim = { x: rimX(team), y: 25 };
	const dribbling: [number, number][] = [];
	tl.ball.forEach((s, k) => {
		if (s.kind === "hold" && s.pid === pid && s.style === "dribble") {
			dribbling.push([s.t0, tl.ball[k + 1]?.t0 ?? s.t0]);
		}
	});
	const on = spansAt(dribbling);
	const stretches: [number, number][] = [];
	let a = -1;
	for (let k = 0; k + 1 < laid.spots.length; k++) {
		const t = laid.from + k * STEP_MS;
		const P = laid.spots[k];
		const Q = laid.spots[k + 1];
		const ok =
			P !== undefined &&
			Q !== undefined &&
			Math.hypot(Q.x - P.x, Q.y - P.y) / (STEP_MS / 1000) >= SWITCH_PACE &&
			on.hit(t, t + STEP_MS) &&
			!held.hit(t - 300, t + STEP_MS + 300) &&
			Math.hypot(P.x - rim.x, P.y - rim.y) >= WORK_NEAR;
		if (ok && a < 0) {
			a = t;
		} else if (!ok && a >= 0) {
			if (t - a >= SWITCH_LEAST) {
				stretches.push([a, t]);
			}
			a = -1;
		}
	}
	for (const [a0, b0] of stretches) {
		const cuts: Cut[] = [];
		let at = a0 + 500 + 700 * hash01(pid * 3 + 1, Math.round(a0));
		let to: Hand =
			((tl.ball.findLast((s) => s.t0 <= a0) as { hand?: Hand } | undefined)
				?.hand ?? "R") === "R"
				? "L"
				: "R";
		for (let n = 0; at < b0 - 600; n++) {
			cuts.push({ t0: at, t1: at + 700, to });
			to = to === "R" ? "L" : "R";
			at +=
				SWITCH_EVERY[0] +
				(SWITCH_EVERY[1] - SWITCH_EVERY[0]) *
					hash01(pid * 7 + n, Math.round(a0));
		}
		crossOnCuts(tl, pid, a0, b0, cuts);
	}
};

// Moves a man makes off where the play has him, stretch by stretch: from
// each key (when, how far off it) to the next, eased.
type Keyed = { a: number; b: number; keys: [number, Pt][] };
// (And when he is making any, as stretches of time.)
type Offsets = ((t: number) => Pt) & { spans: (readonly [number, number])[] };
const keyed = (stretches: Keyed[]): Offsets => {
	const none = { x: 0, y: 0 };
	const at = (t: number): Pt => {
		let lo = 0;
		let hi = stretches.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (stretches[mid]!.a <= t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		const st = stretches[lo - 1];
		if (!st || !(t < st.b)) {
			return none;
		}
		const k = st.keys.findIndex(([at]) => at > t);
		if (k < 1) {
			return none;
		}
		const [t0, A] = st.keys[k - 1]!;
		const [t1, B] = st.keys[k]!;
		const u = t1 > t0 ? (t - t0) / (t1 - t0) : 1;
		const e = u * u * (3 - 2 * u);
		return { x: A.x + (B.x - A.x) * e, y: A.y + (B.y - A.y) * e };
	};
	return Object.assign(at, {
		spans: stretches.map((x) => [x.a, x.b] as const),
	});
};

// How far behind the three-point line (feet) at the end a team attacks -
// and the spot this far behind it, straight out from the rim (straight
// out from the middle of the floor, in the corners).
const CORNER_DEEP = 14;
const beyondArc = (team: 0 | 1, p: Pt): number => {
	const depth = team === 0 ? p.x : COURT_W - p.x;
	return depth < CORNER_DEEP
		? Math.abs(p.y - 25) - 22
		: Math.hypot(depth - RIM_INSET, p.y - 25) - 23.75;
};
const ontoArc = (team: 0 | 1, p: Pt, behind: number): Pt => {
	const depth = team === 0 ? p.x : COURT_W - p.x;
	if (depth < CORNER_DEEP) {
		return { x: p.x, y: 25 + Math.sign(p.y - 25 || 1) * (22 + behind) };
	}
	const rim = { x: rimX(team), y: 25 };
	const u = unitVec(rim, p);
	return {
		x: rim.x + u.x * (23.75 + behind),
		y: rim.y + u.y * (23.75 + behind),
	};
};

// OFF THE BALL, ROUND THE ARC. Going from one spot behind the line to
// another, a man off the ball goes round the line, not straight across
// inside it: kept ARC_BEHIND behind it wherever his way would cut in (on
// it, more or less, once laid down as runs: see KNOT_TOL).
const ARC_BEHIND = 1;
const ARC_LEAST = 6;
// (Cutting in deeper than this (feet), it is a cut - through the lane,
// along the baseline - not a way round: left be.)
const ARC_DEEPEST = 4;
const arcsOf = (
	tr: { moves: Move[]; team: 0 | 1; pid: number },
	tl: Stepped,
	laid: Path,
	held: Spans,
	offenseAt: (t: number) => 0 | 1,
): Offsets => {
	const team = tr.team;
	const isRound = (b: number) => b < ARC_BEHIND && b > -ARC_DEEPEST;
	// (His own holds of the ball: not with it.)
	const mine = spansAt(
		tl.ball.flatMap((s, k) =>
			s.kind === "hold" && s.pid === tr.pid
				? [[s.t0, tl.ball[k + 1]?.t0 ?? s.t0] as const]
				: [],
		),
	);
	// The runs from behind the line to behind it again, cutting in.
	const runs = tr.moves.filter(
		(m) =>
			Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y) >= ARC_LEAST &&
			beyondArc(team, m.from) > -0.3 &&
			beyondArc(team, m.to) > -0.3 &&
			isRound(
				beyondArc(team, {
					x: (m.from.x + m.to.x) / 2,
					y: (m.from.y + m.to.y) / 2,
				}),
			) &&
			offenseAt((m.t0 + m.t1) / 2) === team,
	);
	const none = { x: 0, y: 0 };
	const at = (t: number): Pt => {
		let lo = 0;
		let hi = runs.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (runs[mid]!.t0 <= t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		const m = runs[lo - 1];
		if (!m || t >= m.t1) {
			return none;
		}
		const P = laid.at(t);
		if (!P || beyondArc(team, P) >= ARC_BEHIND || mine.hit(t, t + 1)) {
			return none;
		}
		// (Nothing of his on, or about to be.)
		const near = Math.min(t - held.last(t), held.next(t) - t);
		const w = Math.min(1, Math.max(0, near / 300));
		const Q = ontoArc(team, P, ARC_BEHIND);
		return { x: (Q.x - P.x) * w, y: (Q.y - P.y) * w };
	};
	return Object.assign(at, {
		spans: runs.map((m) => [m.t0, m.t1] as const),
	});
};

// OFF THE BALL, GETTING OPEN. Stood a while off the ball with nothing of
// his coming, a man does not wait where the play put him like a cone: now
// and then he relocates a step - along the arc, a step in, a step out -
// and sets his feet there a while (long enough to spot up, to call for it:
// see offBall in evaluate.ts). Back where the play wants him as he next
// goes. (His man goes with him, a beat late: see stepTimeline.)
const OPEN_LEAST = 3000;
const OPEN_FIRST: [number, number] = [600, 1800];
const OPEN_MOVE: [number, number] = [700, 1100];
const OPEN_HOLD: [number, number] = [2400, 4000];
const OPEN_NEAR = 30;
const OPEN_OUT = 500;
// (Along the arc, in toward the rim - feet.)
const OPEN_SPOTS: [number, number][] = [
	[1.6, 0],
	[-1.6, 0],
	[0.8, 1],
	[-0.8, 1],
	[0.9, -0.8],
	[-0.9, -0.8],
	[0, 1.3],
];
const openingsOf = (
	tl: Stepped,
	pid: number,
	laid: Path,
	held: Spans,
	team: 0 | 1,
	offenseAt: (t: number) => 0 | 1,
	// (When the ball is in play: in a man's hands on the floor, not at a
	// free throw.)
	live: Spans,
): Offsets => {
	const rim = { x: rimX(team), y: 25 };
	const beyond = (p: Pt) => beyondArc(team, p);
	const mine = spansAt(
		tl.ball.flatMap((s, k) =>
			s.kind === "hold" && s.pid === pid
				? [[s.t0, tl.ball[k + 1]?.t0 ?? s.t0] as const]
				: [],
		),
	);
	const stretches: Keyed[] = [];
	let a = -1;
	const close = (b: number) => {
		if (a < 0) {
			return;
		}
		// (Not till he is back from the last.)
		a = Math.max(a, stretches.at(-1)?.b ?? -Infinity);
		if (b - a >= OPEN_LEAST) {
			const keys: [number, Pt][] = [[a, { x: 0, y: 0 }]];
			let at = { x: 0, y: 0 };
			let n = 0;
			let t =
				a +
				OPEN_FIRST[0] +
				(OPEN_FIRST[1] - OPEN_FIRST[0]) * hash01(pid * 3 + 7, Math.round(a));
			for (;;) {
				const r = hash01(pid * 29 + n, Math.round(t / 50));
				const move = OPEN_MOVE[0] + (OPEN_MOVE[1] - OPEN_MOVE[0]) * r;
				if (t + move > b - 300) {
					break;
				}
				const P = laid.at(t) ?? rim;
				const u = unitVec(P, rim);
				const [across, inAt] =
					OPEN_SPOTS[
						Math.floor(hash01(pid * 19 + n, Math.round(a)) * OPEN_SPOTS.length)
					]!;
				let o = {
					x: -u.y * across + u.x * inAt,
					y: u.x * across + u.y * inAt,
				};
				const Q = { x: P.x + o.x, y: P.y + o.y };
				const kept = inPlay(Q);
				// (On the floor, and behind the line if he was.)
				if (
					Math.hypot(kept.x - Q.x, kept.y - Q.y) > 0.01 ||
					(beyond(P) > 0.3 && beyond(Q) < 0.5)
				) {
					o = { x: 0, y: 0 };
				}
				keys.push([t, at], [t + move, o]);
				at = o;
				t +=
					move +
					OPEN_HOLD[0] +
					(OPEN_HOLD[1] - OPEN_HOLD[0]) * hash01(pid * 23 + n, Math.round(a));
				n += 1;
			}
			keys.push([b, at], [b + OPEN_OUT, { x: 0, y: 0 }]);
			if (keys.length > 3) {
				stretches.push({ a, b: b + OPEN_OUT, keys });
			}
		}
		a = -1;
	};
	const begin = laid.from;
	for (let k = 0; k + 1 < laid.spots.length; k++) {
		const t = begin + k * STEP_MS;
		const P = laid.spots[k];
		const Q = laid.spots[k + 1];
		const ok =
			P !== undefined &&
			Q !== undefined &&
			Math.hypot(Q.x - P.x, Q.y - P.y) / (STEP_MS / 1000) < 0.7 &&
			offenseAt(t) === team &&
			live.hit(t, t + STEP_MS) &&
			!mine.hit(t - 300, t + STEP_MS + 300) &&
			!held.hit(t - 600, t + STEP_MS + 600) &&
			Math.hypot(P.x - rim.x, P.y - rim.y) < OPEN_NEAR;
		if (ok && a < 0) {
			a = t;
		} else if (!ok) {
			close(t);
		}
	}
	close(begin + laid.spots.length * STEP_MS);
	return keyed(stretches);
};

// HIS MAN WITH HIM. A defender up on a man goes where that man really goes
// - not only where the play laid him out - a beat behind him: the man
// he guards the nearest of the other side to where the play has him, all
// of it the nearer they are (a step back from it in help, none of it off
// him).
const GUARD_FULL = 7;
const GUARD_NONE = 12;
const GUARD_TIE = 1;
// The help: a man of theirs off the man with the ball goes this share of
// what that man does of his own - all of it within HELP_NEAR of him (feet),
// none of it past HELP_FAR.
const HELP_SHARE = 0.35;
const HELP_NEAR = 10;
const HELP_FAR = 22;
// (His own beat, ms - on top of the moment his legs take to answer it.)
const GUARD_LAG: [number, number] = [90, 200];
// On defense a man goes where the play has him now, seeing nothing of it
// ahead: the play has him reacting to his man already (see mark in
// director.ts) - here he doesn't get ahead of that. What his man does of
// his own here (see probesOf, openingsOf) he answers his own beat late
// (GUARD_LAG). Into anything he has to be there for on time (his acts, the
// picture's cuts, the ball changing hands) he sees it coming again and
// leaves his man's moves be - all of it this long (ms) off one.
const REACT_RAMP = 600;

// The whole game stepped: every man's tracks re-laid as he would really
// move them. (In place.)
export const stepTimeline = <T extends Stepped>(tl: T): T => {
	const cut = spansAt(tl.cuts.map((c) => [c - STEP_MS, c] as const));
	const men = [...tl.tracks.values()].filter(
		(tr) => tr.pid >= 0 && tr.moves.length > 0,
	);
	// Where the play has each man, step by step.
	const laid = new Map<number, Path>();
	const helds = new Map<number, Spans>();
	for (const tr of men) {
		const from = Math.max(0, tr.moves[0]!.t0 - 1000);
		const until = tr.moves.at(-1)!.t1 + 1000 + LOOK_MOST * 1000;
		const spots: (Pt | undefined)[] = [];
		for (let t = from; t <= until; t += STEP_MS) {
			spots.push(floorSpotOf(tr, t));
		}
		laid.set(tr.pid, pathOf(from, spots));
	}
	for (const tr of men) {
		// (His acts - and the moment he goes off the floor: there for it, as
		// for one, where whatever takes him from there has him.)
		helds.set(
			tr.pid,
			spansAt([
				...tr.acts.map((x) => [x.t0, x.t1] as const),
				...tr.shown.flatMap(([at, on], k) =>
					!on && k > 0 && tr.shown[k - 1]![1]
						? [[at - STEP_MS, at] as const]
						: [],
				),
			]),
		);
	}
	// Who has the ball, from when.
	const offenseAt = (t: number): 0 | 1 => {
		let lo = 0;
		let hi = tl.poss.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (tl.poss[mid]![0] <= t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		return lo > 0 ? tl.poss[lo - 1]![1] : 1;
	};
	// A shot up, till it is rebounded: every man going for it - to the glass,
	// boxing out - when the play has him go, no beat to it (who goes when is
	// the play's: see crash and boxOut in director.ts).
	const sharp = spansAt(
		tl.ball.flatMap((s) =>
			s.kind === "fly" && "pid" in s.from && !("pid" in s.to)
				? [[s.t0 - SHOT_BEFORE, s.t1 + SHOT_AFTER] as const]
				: [],
		),
	);
	// Each man stepped after what he wants, over his stretches on the floor.
	const step = (
		tr: (typeof men)[number],
		want: (t: number) => Pt | undefined,
		seeAt?: (t: number) => number,
	) => {
		const L = laid.get(tr.pid)!;
		const held = helds.get(tr.pid)!;
		const samples: Sample[] = [];
		const begin = L.from;
		const end = tr.moves.at(-1)!.t1 + 1000;
		tr.shown.forEach(([at, on], k) => {
			const from = Math.max(begin, at);
			const until = Math.min(end, tr.shown[k + 1]?.[0] ?? end);
			if (!on || !(until > from)) {
				return;
			}
			for (const q of follow(
				want,
				from,
				until - 1,
				held,
				cut,
				(t) =>
					REACT_MS[0] +
					(REACT_MS[1] - REACT_MS[0]) *
						hash01(tr.pid * 17 + 3, Math.round(t / 100)),
				seeAt,
				sharp,
			)) {
				samples.push(q);
			}
		});
		return samples;
	};
	// When the ball is in play: in a man's hands on the floor (not taking
	// it in from out of bounds), not at a free throw.
	const ft = spansAt(
		tl.beats.flatMap((b, k) =>
			b.type === "ft" || b.type === "missFt"
				? [[b.preStart, tl.beats[k + 1]?.preStart ?? tl.end] as const]
				: [],
		),
	);
	const live = spansAt(
		tl.ball.flatMap((s, k) => {
			if (s.kind !== "hold") {
				return [];
			}
			const end = tl.ball[k + 1]?.t0 ?? s.t0;
			const P = laid.get(s.pid)?.at(s.t0);
			return P &&
				P.x > 0 &&
				P.x < COURT_W &&
				P.y > 0 &&
				P.y < COURT_H &&
				!ft.hit(s.t0, end)
				? [[s.t0, end] as const]
				: [];
		}),
	);
	// Each with the ball working his own way (see probesOf), and each off
	// it getting open (openingsOf).
	const probes = new Map<number, (t: number) => Pt>();
	// (When anybody on each side is making any.)
	const making: [(readonly [number, number])[], (readonly [number, number])[]] =
		[[], []];
	for (const tr of men) {
		const L = laid.get(tr.pid)!;
		const held = helds.get(tr.pid)!;
		switchesOf(tl, tr.pid, L, held, tr.team);
		const probe = probesOf(tl, tr.pid, L, held, tr.team);
		const open = openingsOf(tl, tr.pid, L, held, tr.team, offenseAt, live);
		const arc = arcsOf(tr, tl, L, held, offenseAt);
		probes.set(tr.pid, (t) => {
			const a = probe(t);
			const b = open(t);
			const c = arc(t);
			return { x: a.x + b.x + c.x, y: a.y + b.y + c.y };
		});
		making[tr.team].push(...probe.spans, ...open.spans, ...arc.spans);
	}
	const busy = [spansAt(making[0]), spansAt(making[1])] as const;
	// Who has the ball at t, if anybody does.
	const holderAt = (t: number): number | undefined => {
		let lo = 0;
		let hi = tl.ball.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (tl.ball[mid]!.t0 <= t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		const seg = tl.ball[lo - 1];
		return seg?.kind === "hold" ? seg.pid : undefined;
	};
	// And on defense, each with his man as his man really goes, a beat
	// behind him.
	for (const tr of men) {
		const L = laid.get(tr.pid)!;
		const probe = probes.get(tr.pid)!;
		const lag =
			GUARD_LAG[0] + (GUARD_LAG[1] - GUARD_LAG[0]) * hash01(tr.pid * 13 + 5, 1);
		const onTime = spansAt([
			...tr.acts.map((x) => [x.t0, x.t1] as const),
			...tl.cuts.map((c) => [c - STEP_MS, c] as const),
			...tl.poss.map(([at]) => [at, at] as const),
			...tr.shown.map(([at]) => [at, at] as const),
		]);
		// (How much of his beat behind he is, at t.)
		const behind = (t: number) =>
			offenseAt(t) === tr.team
				? 0
				: Math.max(
						0,
						Math.min(
							1,
							(t - onTime.last(t)) / REACT_RAMP,
							(onTime.next(t) - t) / REACT_RAMP,
						),
					);
		const wantNow = (t: number) => {
			if (offenseAt(t) === tr.team) {
				const P = L.at(t);
				if (!P) {
					return P;
				}
				const o = probe(t);
				if (o.x === 0 && o.y === 0) {
					return P;
				}
				// (Never off the floor for it, if the play has him on it.)
				const Q = { x: P.x + o.x, y: P.y + o.y };
				const inP = inPlay(P);
				return inP.x === P.x && inP.y === P.y ? inPlay(Q) : Q;
			}
			const r = behind(t);
			const at = t - lag * r;
			const P = L.at(t);
			// (Nobody on the other side making a move of his own: nothing
			// to go with.)
			if (!P || r <= 0 || !busy[tr.team === 0 ? 1 : 0].hit(at, at + 1)) {
				return P;
			}
			let man: number | undefined;
			let d = GUARD_NONE;
			// (And the next nearest: as near as him, or all but, a share of
			// what that one does too - so his man changing from one to the
			// other, what he goes with doesn't jump in a step.)
			let next: number | undefined;
			let d2 = GUARD_NONE;
			for (const o of men) {
				if (o.team === tr.team) {
					continue;
				}
				const Q = laid.get(o.pid)!.at(t);
				const g = Q ? Math.hypot(Q.x - P.x, Q.y - P.y) : Infinity;
				if (g < d) {
					next = man;
					d2 = d;
					d = g;
					man = o.pid;
				} else if (g < d2) {
					next = o.pid;
					d2 = g;
				}
			}
			const tie =
				next === undefined ? 0 : Math.max(0, 1 - (d2 - d) / GUARD_TIE);
			const mine = man === undefined ? { x: 0, y: 0 } : probes.get(man)!(at);
			const theirs = tie > 0 ? probes.get(next!)!(at) : { x: 0, y: 0 };
			const went = {
				x: (mine.x + theirs.x * tie) / (1 + tie),
				y: (mine.y + theirs.y * tie) / (1 + tie),
			};
			// (None of it into anything of his own: on time, where the play
			// has him for it.)
			const w =
				r *
				Math.min(1, Math.max(0, (GUARD_NONE - d) / (GUARD_NONE - GUARD_FULL)));
			// And the man with the ball, if his man is somebody else: a share of
			// whatever he does of his own - the help going with the ball.
			let hx = 0;
			let hy = 0;
			const holder = holderAt(at);
			if (holder !== undefined && holder !== man && holder !== tr.pid) {
				const H = laid.get(holder)?.at(t);
				const ht = tl.tracks.get(holder);
				if (H && ht && ht.team !== tr.team) {
					const wh =
						r *
						HELP_SHARE *
						Math.min(
							1,
							Math.max(
								0,
								(HELP_FAR - Math.hypot(H.x - P.x, H.y - P.y)) /
									(HELP_FAR - HELP_NEAR),
							),
						);
					if (wh > 0) {
						const hp = probes.get(holder)!(at);
						hx = hp.x * wh;
						hy = hp.y * wh;
					}
				}
			}
			if (w === 0 && hx === 0 && hy === 0) {
				return P;
			}
			// (Never off the floor for it, if the play has him on it.)
			const Q = {
				x: P.x + went.x * w + hx,
				y: P.y + went.y * w + hy,
			};
			const inP = inPlay(P);
			return inP.x === P.x && inP.y === P.y ? inPlay(Q) : Q;
		};
		const want = pathOf(
			L.from,
			L.spots.map((_, k) => wantNow(L.from + k * STEP_MS)),
		).at;
		const samples: Sample[] = step(tr, want, (t) =>
			behind(t) > 0 ? 0 : LOOK_MOST,
		);
		// (On the floor of the place, out by the benches, wherever: see
		// LINE_PULL for the lines themselves.)
		for (const q of samples) {
			q.x = Math.min(COURT_W + 3.9, Math.max(-3.9, q.x));
			q.y = Math.min(COURT_H + 3.9, Math.max(-8.4, q.y));
		}
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
		tr.moves = [...toMoves(samples, animAt, helds.get(tr.pid)!), ...off].sort(
			(a, b) => a.t0 - b.t0,
		);
	}
	for (const tr of men) {
		delete tr.nudges;
		delete tr.sep;
	}
	return tl;
};
