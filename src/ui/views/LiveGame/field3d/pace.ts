import type { Leg } from "./director.ts";
import { moveKind, type Move } from "./poses.ts";

// HOW HE GETS FROM ONE MARK TO THE NEXT - not at one even speed from the
// first instant to the last, the way the play lays it out, but off from a
// stand the way a man gets going, and pulling up to one the way he stops.
// Each mark still his at the time it is laid out for, or near enough and a
// moment either side, so whatever the play has meet there still meets
// there.

// Off from a stand: up to speed over this long, its start a moment before
// his - a touch on from his mark when he is meant to go from it, at his
// pace a moment after. Off from nothing before (his first stretch): up to
// speed over this long at the most (or this share of the stretch), the
// ground lost made up over the rest of it. Pulling up to one: down from his
// pace over this long, the stretch's end in the middle of it - a touch
// short of his mark when it ends, on it a moment after.
const GO_MS = 240;
// (Off from a stand into a walk or a jog - in no hurry: up to it over this
// long, no more than this share of it, the ground lost made up over the
// rest of it.)
const GO_EASY_MS = 700;
const GO_EASY_MOST = 0.35;
const EASY = new Set<Move>(["walk", "jog"]);
const FIRST_MS = 260;
const FIRST_MOST = 0.3;
const STOP_MS = 240;
const EASE_LEAST_MS = 120;
// Off from his stance at the snap, never before it: a moment behind where
// the play has him getting going, and caught up with it over this long - or
// this share of his going on from it, if that is less (before he does
// anything but run on).
const SNAP_CATCH_MS = 500;
const SNAP_CATCH_MOST = 0.8;

// Set for the snap.
export const SET = new Set<Move>([
	"threePoint",
	"dlThree",
	"center",
	"underCenter",
	"shotgun",
	"backStance",
	"receiverStance",
	"backerStance",
	"cornerStance",
	"safetyStance",
	"holdKneel",
	"kickReady",
]);

export const along = (l: Leg, t: number) =>
	l.t1 > l.t0 ? Math.min(1, Math.max(0, (t - l.t0) / (l.t1 - l.t0))) : 1;

export const goes = (l: Leg) => l.from.x !== l.to.x || l.from.y !== l.to.y;

const eases = (l: Leg) =>
	goes(l) && l.t1 - l.t0 >= EASE_LEAST_MS && moveKind(l.move) === "cycle";
// (An act on the move - a dive, a handoff, a man brought down - only ever
// pulled up out of, into a stand.)
const acts = (l: Leg) =>
	goes(l) && l.t1 - l.t0 >= EASE_LEAST_MS && moveKind(l.move) === "act";
// (Acts he does where he pulls up to, still coming to a stop as he starts
// them.)
const PULL_INTO = new Set<Move>(["swat", "handOff"]);

// (The next leg taking him on from where the one before left him - not
// put somewhere else, the picture cut away: no further off it than this,
// feet.)
const JOIN_FT = 1;
export const joined = (l: Leg, next: Leg) =>
	Math.hypot(next.from.x - l.to.x, next.from.y - l.to.y) <= JOIN_FT;

// How long past a stretch's end he is still pulling up into the stand after
// it (none if he runs on, or into an act of some kind, or is put somewhere
// else after it) - no more than half of it, if he gets going again after
// it, for him to get going over the rest.
const stopOf = (
	l: Leg | undefined,
	next: Leg | undefined,
	then: Leg | undefined,
): number =>
	!l ||
	!(eases(l) || acts(l)) ||
	(next && !joined(l, next)) ||
	(next &&
		(goes(next) ||
			(moveKind(next.move) === "act" && !PULL_INTO.has(next.move))))
		? 0
		: Math.min(
				STOP_MS / 2,
				(l.t1 - l.t0) / 2,
				next
					? (next.t1 - next.t0) / (then && eases(then) ? 2 : 1)
					: STOP_MS / 2,
			);

// A stretch's pace: off from a stand `early` ms before it starts (`g` of it
// either side of its start, as a share of it) - or, off from his stance,
// up to speed over the first `a` of it, `top` the pace he makes it up at;
// and pulling up over `h` of it either side of its end, `stop` ms past it.
export type Pace = {
	g: number;
	early: number;
	a: number;
	top: number;
	h: number;
	stop: number;
};
const EVEN: Pace = { g: 0, early: 0, a: 0, top: 1, h: 0, stop: 0 };

export const paceOf = (legs: Leg[], k: number): Pace => {
	const l = legs[k];
	if (!l || !eases(l)) {
		const stop = l && acts(l) ? stopOf(l, legs[k + 1], legs[k + 2]) : 0;
		return stop > 0 ? { ...EVEN, h: stop / (l!.t1 - l!.t0), stop } : EVEN;
	}
	const ms = l.t1 - l.t0;
	// (Put where it starts - the picture cut to him there: off from nothing
	// before.)
	const prev = legs[k - 1] && joined(legs[k - 1]!, l) ? legs[k - 1] : undefined;
	const stop = stopOf(l, legs[k + 1], legs[k + 2]);
	if (prev && goes(prev)) {
		return stop > 0 ? { ...EVEN, h: stop / ms, stop } : EVEN;
	}
	if (!prev) {
		const a = Math.min(FIRST_MS / ms, FIRST_MOST);
		return { g: 0, early: 0, a, top: 1 / (1 - a / 2), h: stop / ms, stop };
	}
	if (SET.has(prev.move)) {
		return stop > 0 ? { ...EVEN, h: stop / ms, stop } : EVEN;
	}
	// (Not before the stretch before this one's was pulled up from.)
	const early = Math.max(
		0,
		Math.min(
			GO_MS / 2,
			ms / 2,
			prev.t1 - prev.t0 - stopOf(legs[k - 2], prev, l),
		),
	);
	const g = early / ms;
	const a = EASY.has(l.move)
		? Math.min(GO_EASY_MS / ms - g, GO_EASY_MOST, 1 - stop / ms - 0.05)
		: 0;
	return a > g
		? { g, early, a, top: 1 / (1 + (g - a) / 2), h: stop / ms, stop }
		: { g, early, a: 0, top: 1, h: stop / ms, stop };
};

// How far along a stretch he is at t (or just before its start, getting
// going; or just past its end, pulling up).
const easedAlong = (l: Leg, t: number, p: Pace): number => {
	if (p === EVEN) {
		return along(l, t);
	}
	const { g, a, top, h } = p;
	const u = (t - l.t0) / (l.t1 - l.t0);
	// (Getting going from g before its start, up to his pace by a into it -
	// or, a no later than g, by as long after it as before.)
	const easy = g > 0 && a > g;
	if (g > 0 && !easy && u < g) {
		const w = Math.max(0, u + g);
		return (w * w) / (4 * g);
	}
	const off = easy
		? (v: number) => {
				const w = v + g;
				const R = a + g;
				return w <= 0
					? 0
					: w < R
						? (top * w * w) / (2 * R)
						: top * (R / 2 + v - a);
			}
		: (v: number) =>
				v <= 0 ? 0 : v < a ? (top * v * v) / (2 * a) : top * (a / 2 + v - a);
	if (h > 0 && u > 1 - h) {
		const w = Math.min(u - (1 - h), 2 * h);
		return off(1 - h) + top * (w - (w * w) / (4 * h));
	}
	return off(Math.min(1, u));
};

// The times he goes off his stance at the snap, and how long he is behind
// the play from each (worked out once for his legs as they are).
const runsOn = (l: Leg) => goes(l) && moveKind(l.move) === "cycle";
const offs = new WeakMap<
	Leg[],
	{ n: number; off: { b: number; r: number }[] }
>();
export const offsOf = (legs: Leg[]) => {
	const had = offs.get(legs);
	if (had && had.n === legs.length) {
		return had.off;
	}
	const off: { b: number; r: number }[] = [];
	for (let j = 1; j < legs.length; j++) {
		if (!runsOn(legs[j]!) || !SET.has(legs[j - 1]!.move)) {
			continue;
		}
		const b = legs[j]!.t0;
		let m = j;
		// (Run on over as many legs as it takes: the one stretch, as
		// long as it is, whatever it is cut into.)
		while (
			m + 1 < legs.length &&
			runsOn(legs[m + 1]!) &&
			legs[m]!.t1 - b < SNAP_CATCH_MS / SNAP_CATCH_MOST
		) {
			m++;
		}
		const r = Math.min(SNAP_CATCH_MS, (legs[m]!.t1 - b) * SNAP_CATCH_MOST);
		if (r > 0) {
			off.push({ b, r });
		}
	}
	offs.set(legs, { n: legs.length, off });
	return off;
};

// The time the play has him where he is at t: a moment behind it just off
// his stance (see SNAP_CATCH_MS), or t. (The times he goes off it, if had
// already.)
export const lagged = (legs: Leg[], t: number, off = offsOf(legs)): number => {
	for (const { b, r } of off) {
		if (t > b && t < b + r) {
			// (Still at the first, and as fast as the play has him at the
			// last.)
			const u = (t - b) / r;
			return b + r * u * u * (2 - u);
		}
	}
	return t;
};

// Where his feet are at t, k his leg then: which stretch they are on (the
// one before, into a stand, still pulling up; the next, out of one, already
// getting going), and how far along it. (The paces of leg k and those
// either side, if had already.)
export const paceAt = (
	legs: Leg[],
	k: number,
	t: number,
	pace = paceOf(legs, k),
	before = paceOf(legs, k - 1),
	after = paceOf(legs, k + 1),
): { on: number; u: number } => {
	if (k > 0 && t < legs[k]!.t0 + before.stop) {
		return { on: k - 1, u: easedAlong(legs[k - 1]!, t, before) };
	}
	const next = legs[k + 1];
	if (next && t >= next.t0 - after.early) {
		return { on: k + 1, u: easedAlong(next, t, after) };
	}
	return { on: k, u: easedAlong(legs[k]!, t, pace) };
};

// From one stretch straight on into another - going on, not pulled up at
// it - not from the one's pace and way to the other's all at once, but over
// this long either side of where they meet (no more than half of either):
// the corner rounded, him just inside it at the time laid out for it.
export const CORNER_MS = 150;
// (And from one pace to a far different one, the way on the same: longer,
// up to twice as long for this much quicker or slower, ft/s.)
const CORNER_DV = 4;
const CORNER_MOST_MS = CORNER_MS * 2;

// Where his feet are at t, k his leg then (see paceAt): on his stretch, or
// round the corner from the one before it or into the next.
export const placeAt = (
	legs: Leg[],
	k: number,
	t: number,
	pace = paceOf(legs, k),
	before = paceOf(legs, k - 1),
	after = paceOf(legs, k + 1),
): { x: number; y: number; on: number; u: number } => {
	const { on, u } = paceAt(legs, k, t, pace, before, after);
	const l = legs[on]!;
	const at = {
		x: l.from.x + (l.to.x - l.from.x) * u,
		y: l.from.y + (l.to.y - l.from.y) * u,
		on,
		u,
	};
	if (on === k) {
		if (t - legs[k]!.t0 < CORNER_MOST_MS) {
			rounded(legs, k, before.top, t, at);
		}
		const next = legs[k + 1];
		if (next && next.t0 - t < CORNER_MOST_MS) {
			rounded(legs, k + 1, pace.top, t, at);
		}
	}
	return at;
};

// Round the corner between legs j - 1 and j (at the pace he came into it),
// if he is at it: from where he is on the one to where he would be,
// carried on from it at its pace, on the other.
const rounded = (
	legs: Leg[],
	j: number,
	top: number,
	t: number,
	at: { x: number; y: number },
) => {
	const a = legs[j - 1];
	const b = legs[j];
	if (!a || !b || !goes(a) || !goes(b) || Math.abs(a.t1 - b.t0) > 1) {
		return;
	}
	const ma = a.t1 - a.t0;
	const mb = b.t1 - b.t0;
	const dv = Math.abs(
		(Math.hypot(a.to.x - a.from.x, a.to.y - a.from.y) * top * 1000) / ma -
			(Math.hypot(b.to.x - b.from.x, b.to.y - b.from.y) * 1000) / mb,
	);
	const strides = moveKind(a.move) === "cycle" && moveKind(b.move) === "cycle";
	const w = Math.min(
		CORNER_MS * (strides ? Math.min(2, Math.max(1, dv / CORNER_DV)) : 1),
		ma / 2,
		mb / 2,
	);
	const d = t - b.t0;
	if (w <= 0 || Math.abs(d) >= w) {
		return;
	}
	// (From the one's pace and way to the other's evenly over it: where he
	// is on the one he is on, and the ground the change has made up or lost
	// on it by then.)
	const ia = top / (a.t1 - a.t0);
	const ib = 1 / (b.t1 - b.t0);
	const dx = (b.to.x - b.from.x) * ib - (a.to.x - a.from.x) * ia;
	const dy = (b.to.y - b.from.y) * ib - (a.to.y - a.from.y) * ia;
	const e = d < 0 ? d + w : w - d;
	const s = (e * e) / (4 * w);
	at.x += dx * s;
	at.y += dy * s;
};

// WHERE HE IS EVERY TRACK_MS through a play, as placeAt has him (n times,
// from t0): and the leg he is on then, by the play's time (`k`) and by the
// time the play has him at (`late` - see lagged), and the one his feet are
// on (`on`) - and whether he was put there since the time before, not come
// there (`cut`: see joined). Worked out once for his legs as they are.
export const TRACK_MS = 50;
export type Track = {
	t0: number;
	xs: Float32Array;
	ys: Float32Array;
	k: Int32Array;
	late: Int32Array;
	on: Int32Array;
	cut: Uint8Array;
};
export const trackLength = (t0: number, t1: number) =>
	Math.max(2, Math.ceil((t1 - t0) / TRACK_MS) + 1);
const tracks = new WeakMap<Leg[], { n: number; tr: Track }>();
export const trackOf = (legs: Leg[], t0: number, n: number): Track => {
	const had = tracks.get(legs);
	if (
		had &&
		had.n === legs.length &&
		had.tr.t0 === t0 &&
		had.tr.xs.length === n
	) {
		return had.tr;
	}
	const tr: Track = {
		t0,
		xs: new Float32Array(n),
		ys: new Float32Array(n),
		k: new Int32Array(n),
		late: new Int32Array(n),
		on: new Int32Array(n),
		cut: new Uint8Array(n),
	};
	let k = 0;
	let before = paceOf(legs, -1);
	let pace = paceOf(legs, 0);
	let after = paceOf(legs, 1);
	const off = offsOf(legs);
	for (let s = 0; s < n; s++) {
		const t = t0 + s * TRACK_MS;
		while (k + 1 < legs.length && legs[k + 1]!.t0 <= t) {
			k++;
			before = pace;
			pace = after;
			after = paceOf(legs, k + 1);
			if (s > 0 && !joined(legs[k - 1]!, legs[k]!)) {
				tr.cut[s] = 1;
			}
		}
		// (Just off his stance, a moment behind.)
		const late = lagged(legs, t, off);
		let kl = k;
		while (kl > 0 && legs[kl]!.t0 > late) {
			kl--;
		}
		tr.k[s] = k;
		tr.late[s] = kl;
		// (Stood where he is, not pulling up into it nor getting going from
		// it: there.)
		const l = legs[k]!;
		const next = legs[k + 1];
		if (
			kl === k &&
			!goes(l) &&
			late >= l.t0 + before.stop &&
			!(next && late >= next.t0 - after.early)
		) {
			tr.xs[s] = l.from.x;
			tr.ys[s] = l.from.y;
			tr.on[s] = k;
			continue;
		}
		const at =
			kl === k
				? placeAt(legs, k, late, pace, before, after)
				: placeAt(legs, kl, late);
		tr.xs[s] = at.x;
		tr.ys[s] = at.y;
		tr.on[s] = at.on;
	}
	tracks.set(legs, { n: legs.length, tr });
	return tr;
};

// NOT FROM FLAT OUT TO STOOD IN A FRAME: where his track has him, run
// through a short bell a TRACK_MS apart either side - a sudden start, stop
// or cut taken over a step or so, not in a frame; on a straight run at a
// steady pace, or stood still, just where he was - and from one time on it
// to the next along a smooth curve through them (see smoothAt). Never
// across a jump from one place to another (more than JUMP_FT from one time
// on it to the next, or put there - see Track): there he is where his legs
// have him.
const SMOOTH = [1, 4, 6, 4, 1];
const SMOOTH_SUM = 16;
const SMOOTH_HALF = (SMOOTH.length - 1) / 2;
const JUMP_FT = 3;
type Smooth = { xs: Float32Array; ys: Float32Array; ok: Uint8Array };
const smooths = new WeakMap<Track, Smooth>();
const smoothOf = (tr: Track): Smooth => {
	const had = smooths.get(tr);
	if (had) {
		return had;
	}
	const n = tr.xs.length;
	const at = (j: number) => Math.min(n - 1, Math.max(0, j));
	const jumps = new Uint8Array(n);
	for (let s = 1; s < n; s++) {
		const d = Math.hypot(tr.xs[s]! - tr.xs[s - 1]!, tr.ys[s]! - tr.ys[s - 1]!);
		jumps[s] = d > JUMP_FT || tr.cut[s] ? 1 : 0;
	}
	const sm: Smooth = {
		xs: new Float32Array(n),
		ys: new Float32Array(n),
		ok: new Uint8Array(n),
	};
	for (let s = 0; s < n; s++) {
		let x = 0;
		let y = 0;
		let ok = 1;
		for (let i = 0; i < SMOOTH.length; i++) {
			const j = at(s + i - SMOOTH_HALF);
			x += SMOOTH[i]! * tr.xs[j]!;
			y += SMOOTH[i]! * tr.ys[j]!;
			if (i > 0 && jumps[j]) {
				ok = 0;
			}
		}
		sm.xs[s] = x / SMOOTH_SUM;
		sm.ys[s] = y / SMOOTH_SUM;
		sm.ok[s] = ok;
	}
	smooths.set(tr, sm);
	return sm;
};
// (Where he is at t that way, if he can be had there: through the times
// either side of t and the next out either side, Catmull-Rom.)
export const smoothAt = (
	tr: Track,
	t: number,
): { x: number; y: number } | undefined => {
	const sm = smoothOf(tr);
	const n = sm.xs.length;
	const u = (t - tr.t0) / TRACK_MS;
	const s = Math.floor(u);
	if (s < 1 || s + 2 >= n) {
		return undefined;
	}
	const { xs, ys, ok } = sm;
	if (!ok[s - 1] || !ok[s] || !ok[s + 1] || !ok[s + 2]) {
		return undefined;
	}
	const w = u - s;
	const cr = (a: number, b: number, c: number, d: number) =>
		b +
		0.5 *
			w *
			(c - a + w * (2 * a - 5 * b + 4 * c - d + w * (3 * (b - c) + d - a)));
	return {
		x: cr(xs[s - 1]!, xs[s]!, xs[s + 1]!, xs[s + 2]!),
		y: cr(ys[s - 1]!, ys[s]!, ys[s + 1]!, ys[s + 2]!),
	};
};
