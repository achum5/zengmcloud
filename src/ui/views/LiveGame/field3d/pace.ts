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
// pace a moment after. Off from his stance at the snap, never before it:
// up to speed over this long at the most (or this share of the stretch),
// the ground lost made up over the rest of it. Pulling up to one: down from
// his pace over this long, the stretch's end in the middle of it - a touch
// short of his mark when it ends, on it a moment after.
const GO_MS = 240;
const SNAP_MS = 260;
const SNAP_MOST = 0.3;
const STOP_MS = 240;
const EASE_LEAST_MS = 120;

// Set for the snap.
const SET = new Set<Move>([
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

// How long past a stretch's end he is still pulling up into the stand after
// it (none if he runs on, or into an act of some kind).
const stopOf = (l: Leg | undefined, next: Leg | undefined): number =>
	!l || !eases(l) || (next && (goes(next) || moveKind(next.move) === "act"))
		? 0
		: Math.min(
				STOP_MS / 2,
				(l.t1 - l.t0) / 2,
				next ? next.t1 - next.t0 : STOP_MS / 2,
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
		return EVEN;
	}
	const ms = l.t1 - l.t0;
	const prev = legs[k - 1];
	const stop = stopOf(l, legs[k + 1]);
	if (prev && goes(prev)) {
		return stop > 0 ? { ...EVEN, h: stop / ms, stop } : EVEN;
	}
	if (!prev || SET.has(prev.move)) {
		const a = Math.min(SNAP_MS / ms, SNAP_MOST);
		return { g: 0, early: 0, a, top: 1 / (1 - a / 2), h: stop / ms, stop };
	}
	// (Not before the stretch before this one's was pulled up from.)
	const early = Math.max(
		0,
		Math.min(GO_MS / 2, ms / 2, prev.t1 - prev.t0 - stopOf(legs[k - 2], prev)),
	);
	return { g: early / ms, early, a: 0, top: 1, h: stop / ms, stop };
};

// How far along a stretch he is at t (or just before its start, getting
// going; or just past its end, pulling up).
const easedAlong = (l: Leg, t: number, p: Pace): number => {
	if (p === EVEN) {
		return along(l, t);
	}
	const { g, a, top, h } = p;
	const u = (t - l.t0) / (l.t1 - l.t0);
	if (g > 0 && u < g) {
		const w = Math.max(0, u + g);
		return (w * w) / (4 * g);
	}
	const off = (v: number) =>
		v <= 0 ? 0 : v < a ? (top * v * v) / (2 * a) : top * (a / 2 + v - a);
	if (h > 0 && u > 1 - h) {
		const w = Math.min(u - (1 - h), 2 * h);
		return off(1 - h) + top * (w - (w * w) / (4 * h));
	}
	return off(Math.min(1, u));
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
