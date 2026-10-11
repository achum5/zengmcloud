import type {
	Act,
	ArenaShot,
	BallSeg,
	FxKind,
	Fx,
	CourtTimeline,
	Gesture,
	Move,
	Sep,
	Track,
} from "./director.ts";
import { rimX, type Pt, type Pt3, type Side } from "./geometry.ts";
import { alongShape, keepThrough, runShape, type RunShape } from "./motion.ts";
import { playAt } from "./physics.ts";
import {
	ANIMS,
	armTo,
	bodyOf,
	bounceAt,
	dribbleAhead,
	isGait,
	holdBall,
	isMove,
	lerpPose,
	mirror,
	MOVE_BALL,
	moveAnim,
	moveFloor,
	poseAt,
	posed,
	skeleton,
	standingReach,
	underPalm,
	type AnimName,
	type Body,
	type DribbleMove,
	type Hand,
	type Limb,
	type Pose,
	type Skeleton,
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
	// Just after it came into his hands - caught, or picked up off his
	// dribble - how far his hands have closed on it (0 to 1): they meet it,
	// not snap to it.
	grip?: number;
	// How far through a bounce of his dribble (0 the ball in his hand at the
	// top), when he is dribbling: his hand rides it - this one.
	dribble?: number;
	dribbleHand?: Hand;
	// His hands up as a target for a pass on its way to him (0 to 1).
	target?: number;
	// Up for a dunk: the jump a typical player makes to throw it down (feet),
	// the rim, and how much his hands are on it (0 to 1). His own build
	// decides how high he really goes - see withBody.
	dunk?: { leap: number; rim: Pt3; grip: number };
	// Up for the ball where it will be: the jump a typical player makes to
	// get his hands there (feet) - his own reach decides how high he really
	// goes (see withBody).
	reach?: number;
	// His move done the other way round: his left doing what its right does.
	mirror?: boolean;
	// His legs turned under him from the way he faces (degrees - see Pose).
	legs?: number;
	// Just after a change of move: the last move as it was when it changed,
	// and how much of his pose is still that (1 all, 0 none) - and, if it
	// differs, how much of his arms and the turn of his shoulders - see
	// poseOf.
	from?: Blend;
	// One arm saying something while the rest of him goes on (see armAt):
	// which, its angles (as a pose has them), and how far into them it is
	// (0 to 1).
	arm?: ArmPose;
};
// A move he is easing out of (see blendInto).
export type Blend = {
	anim: AnimName;
	phase: number;
	dribble?: number;
	dribbleHand?: Hand;
	target?: number;
	mirror?: boolean;
	w: number;
	arms?: number;
	// His legs turned under him as it changed (see legs).
	legs?: number;
	// Changed again while still easing out of the move before: that one,
	// and how much of it was still in him then.
	from?: Blend;
};
export type ArmPose = {
	hand: Hand;
	sh: number;
	el: number;
	ab: number;
	wr: number;
	w: number;
	// The elbow tucked in under it, as a shooter's is (see Pose).
	tuck?: number;
	// A finger out: pointing.
	point?: boolean;
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
const CROSS_MS = 1000 / CROSS_RATE;
// A bounce of a dribble move done as the move it is - between his legs,
// behind his back - only if he stands through the whole of it. On the move,
// any of them is a crossover in front, his legs running, the ball pushed
// further out ahead of him.
const crossMoveAt = (
	tl: CourtTimeline,
	seg: { t0: number; pid: number; hand?: Hand; move?: DribbleMove },
	t: number,
) => {
	const c = crossAt(seg, t);
	const tr = tl.tracks.get(seg.pid);
	const b0 = t - c.ph * CROSS_MS;
	// Setting off on a drive as it comes up into his hand still counts.
	const still =
		!tr ||
		[0.02, 0.25, 0.5, 0.75, 0.95].every(
			(u) => !spotAt(tr, b0 + u * CROSS_MS).moving,
		);
	const move: DribbleMove = still ? (seg.move ?? "front") : "front";
	return { ...c, still, move };
};
// How far through one bounce of a dribble the ball is (0 at the hand, 1 at
// the floor), for a point `ph` through the dribble: pushed down hard and
// falling faster all the way to the floor, then rising off it and slowing
// into the hand.
const DOWN = 0.42;
const dribbleDepth = (ph: number): number =>
	ph < DOWN ? (ph / DOWN) ** 1.55 : (1 - (ph - DOWN) / (1 - DOWN)) ** 1.8;

export const offenseAt = (tl: CourtTimeline, t: number): Side => {
	const i = lastIndex(tl.poss, t, (p) => p[0]);
	return i >= 0 ? tl.poss[i]![1] : 1;
};

// A jump ball not yet tipped: nobody's ball.
const jumpBallAt = (tl: CourtTimeline, t: number): boolean =>
	tl.jumps?.some(([t0, tip]) => t >= t0 && t < tip) ?? false;

const ballIndexAt = (tl: CourtTimeline, t: number): number =>
	Math.max(
		0,
		lastIndex(tl.ball, t, (s) => s.t0),
	);
const ballSegAt = (tl: CourtTimeline, t: number): BallSeg | undefined =>
	tl.ball[ballIndexAt(tl, t)];

// A bounce of his dribble. One man's dribbles back to back keep one beat -
// a switch of hands or a new move does not start the bounce over - and a
// switch goes down from one hand and comes up into the other.
type Bounce = {
	// How far through this bounce (0 the ball at the top, in his hand).
	ph: number;
	// The hand it left, and the hand it comes up to.
	from: Hand;
	to: Hand;
	// The first bounce out of his hands, from holding it.
	first: boolean;
	// Where in the ball's path his dribble starts, and when.
	start: number;
	origin: number;
};
const DRIBBLE_MS = 1000 / DRIBBLE_RATE;
const bounceOf = (tl: CourtTimeline, i: number, t: number): Bounce => {
	const seg = tl.ball[i]!;
	const pid = seg.kind === "hold" ? seg.pid : -1;
	const dribbling = (s: BallSeg | undefined) =>
		s?.kind === "hold" && s.pid === pid && s.style === "dribble";
	let j = i;
	while (j > 0 && dribbling(tl.ball[j - 1])) {
		j--;
	}
	const origin = tl.ball[j]!.t0;
	const b = Math.max(0, t - origin) / DRIBBLE_MS;
	const k = Math.floor(b);
	const top = origin + k * DRIBBLE_MS;
	// The hand the ball is in at a top: a dribble's, or the one a crossover
	// off it starts in.
	const handAt = (when: number): Hand | undefined => {
		// (A dribble picked up on the beat starts a hair after it.)
		const s = tl.ball[ballIndexAt(tl, when + 1)];
		return s?.kind === "hold" &&
			s.pid === pid &&
			(s.style === "dribble" || s.style === "cross")
			? (s.hand ?? "R")
			: undefined;
	};
	// (At the top his dribble ends on, the hand the bounce before came up
	// into.)
	const from =
		handAt(top) ??
		handAt(top - DRIBBLE_MS) ??
		(seg.kind === "hold" ? seg.hand : "R") ??
		"R";
	const before = j > 0 ? tl.ball[j - 1] : undefined;
	return {
		ph: b - k,
		from,
		to: handAt(top + DRIBBLE_MS) ?? from,
		start: j,
		origin,
		// Held, or just caught.
		first:
			k === 0 &&
			((before?.kind === "hold" &&
				before.pid === pid &&
				before.style === "hold") ||
				(before?.kind === "fly" &&
					"pid" in before.to &&
					before.to.pid === pid)),
	};
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

// HOW HE RUNS.
//
// A run gets up to speed in a few strides, holds it, and eases off into
// wherever it stops, as hard as a man can and no harder (see motion.ts) -
// not the cosine glide of a thing on a rail. A run that follows on from
// the last one (a beat after it, or none) carries its speed through the
// join, as much as the turn allows - straight on at full tilt, a right
// angle at a third of it - and the corner is rounded off, so he curves
// through it. Only turning back the way he came does he plant and stop.
// A run starting within this long of the last one's end follows on from
// it: the pause between them is taken up in the running.
const FLOW_MS = 300;
// How long either side of a join he spends coming round the corner: the
// faster he goes through it, the wider he swings - up to this long.
const ROUND_MS = 420;
const ROUND_MS_PER_FTPS = 26;

const lenOf = (m: Move) => Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);

// How fast (feet a second) he goes through the join from run a into run
// b, if b follows on from a.
const joinOf = (
	a: Move | undefined,
	b: Move | undefined,
): number | undefined => {
	if (!a || !b) {
		return undefined;
	}
	const gap = b.t0 - a.t1;
	if (gap < -1 || gap > FLOW_MS) {
		return undefined;
	}
	const la = lenOf(a);
	const lb = lenOf(b);
	if (la < 0.3 || lb < 0.3 || dist2(a.to, b.from) > 0.25) {
		return undefined;
	}
	const cos =
		((a.to.x - a.from.x) * (b.to.x - b.from.x) +
			(a.to.y - a.from.y) * (b.to.y - b.from.y)) /
		(la * lb);
	const keep = keepThrough(cos);
	if (keep < 0.1) {
		return undefined;
	}
	const va = la / Math.max(0.05, (a.t1 - a.t0 + gap / 2) / 1000);
	const vb = lb / Math.max(0.05, (b.t1 - b.t0 + gap / 2) / 1000);
	return Math.min(va, vb) * keep;
};

// Set in a screen or a post-up in the pause between two runs, he stops for
// it - however short - rather than running on through it.
const PLANTED = new Set<string>(["screen", "postUp"]);
const plantedBetween = (
	tr: Track,
	a: Move | undefined,
	b: Move | undefined,
): boolean => {
	if (!a || !b) {
		return false;
	}
	const t = (a.t1 + b.t0) / 2;
	const i = lastIndex(tr.acts, t, (x) => x.t0);
	for (let k = i; k >= 0 && k >= i - 3; k--) {
		const x = tr.acts[k]!;
		if (t < x.t1 && PLANTED.has(x.anim)) {
			return true;
		}
	}
	return false;
};

// A run as he really runs it: when it starts and ends (sharing any pause
// with a run it joins), and how fast he is going at each end.
type Run = { mv: Move; s0: number; s1: number; v0: number; v1: number };
// (Worked out once a run - again only if the runs either side of it, or its
// times, are not what they were.)
const runs = new WeakMap<
	Move,
	{ prev?: Move; next?: Move; t0: number; t1: number; run: Run }
>();
const runOf = (tr: Track, k: number): Run => {
	const mv = tr.moves[k]!;
	const prev = tr.moves[k - 1];
	const next = tr.moves[k + 1];
	const got = runs.get(mv);
	if (
		got &&
		got.prev === prev &&
		got.next === next &&
		got.t0 === mv.t0 &&
		got.t1 === mv.t1
	) {
		return got.run;
	}
	const run = workRun(tr, mv, prev, next);
	runs.set(mv, { prev, next, t0: mv.t0, t1: mv.t1, run });
	return run;
};
const workRun = (
	tr: Track,
	mv: Move,
	prev: Move | undefined,
	next: Move | undefined,
): Run => {
	// Going as fast as his path was worked out to have him, where it was -
	// otherwise as fast as the turn into it from the last run allows.
	const vIn =
		mv.v0 !== undefined
			? undefined
			: plantedBetween(tr, prev, mv)
				? undefined
				: joinOf(prev, mv);
	const vOut =
		mv.v1 !== undefined
			? undefined
			: plantedBetween(tr, mv, next)
				? undefined
				: joinOf(mv, next);
	return {
		mv,
		s0: vIn === undefined ? mv.t0 : (prev!.t1 + mv.t0) / 2,
		s1: vOut === undefined ? mv.t1 : (mv.t1 + next!.t0) / 2,
		v0: mv.v0 ?? vIn ?? 0,
		v1: mv.v1 ?? vOut ?? 0,
	};
};

// How far along a run (feet) he is at t: getting going, on at his pace and
// pulling up the way a man does (see motion.ts) - worked out once a run.
const shapes = new WeakMap<
	Move,
	{ s0: number; s1: number; v0: number; v1: number; shape: RunShape }
>();
const alongRun = (run: Run, t: number): number =>
	alongShape(shapeOf(run), (t - run.s0) / 1000);

// Where a run has him at t - carried on past its end, or back before its
// start, at the speed he goes through the join, for rounding a corner.
const onRun = (run: Run, t: number): Pt => {
	const { mv, s0, s1 } = run;
	const L = lenOf(mv);
	const ux = L > 0 ? (mv.to.x - mv.from.x) / L : 0;
	const uy = L > 0 ? (mv.to.y - mv.from.y) / L : 0;
	const d =
		t > s1
			? L + (run.v1 * (t - s1)) / 1000
			: t < s0
				? (-run.v0 * (s0 - t)) / 1000
				: alongRun(run, t);
	return { x: mv.from.x + ux * d, y: mv.from.y + uy * d };
};

// Off one run and onto the next, round a corner: the two runs, each
// carried on through the join, blended from the one into the other - so he
// curves through it with no kink in where he is or how fast he is going.
// (Blended as they are, the two would swing him round hardest halfway, three
// times as hard as an even turn; the swing is taken back out, so the way he
// is going comes round smoothly from the one run's to the other's.)
const smooth = (u: number) => u * u * (3 - 2 * u);
const rounded = (a: Run, b: Run, t: number): Pt | undefined => {
	const tj = a.s1;
	const d = Math.min(
		ROUND_MS,
		Math.max(80, a.v1 * ROUND_MS_PER_FTPS),
		(a.s1 - a.s0) * 0.5,
		(b.s1 - b.s0) * 0.5,
	);
	if (d <= 10 || Math.abs(t - tj) >= d) {
		return undefined;
	}
	const u = (t - (tj - d)) / (2 * d);
	const w = smooth(u);
	const pa = onRun(a, t);
	const pb = onRun(b, t);
	const ua = unit(a.mv.from, a.mv.to);
	const ub = unit(b.mv.from, b.mv.to);
	const k = ((3 * d) / 1000) * u * u * (1 - u) * (1 - u);
	return {
		x: pa.x + (pb.x - pa.x) * w + (ub.x * b.v0 - ua.x * a.v1) * k,
		y: pa.y + (pb.y - pa.y) * w + (ub.y * b.v0 - ua.y * a.v1) * k,
	};
};

// How he goes, by how fast he really goes (feet a second, at his quickest
// along it): a sprint flat out, a run, a jog, a walk at a stroll - whatever
// he was told to do. A defensive slide or a backpedal is for a step or two
// at a man's side, not for covering ground: faster than a man can slide or
// backpedal, he opens up and runs. And drifting for the ball at a run is a
// run.
const SPRINT_FTPS = 19;
const JOG_FTPS = 11;
const WALK_FTPS = 5.5;
const DRIBBLE_WALK_FTPS = 7;
const SLIDE_FTPS = 11.5;
const BACK_FTPS = 12.5;
const DRIFT_FTPS = 9;
const shapeOf = (run: Run): RunShape => {
	const { mv, s0, s1, v0, v1 } = run;
	const L = lenOf(mv);
	let got = shapes.get(mv);
	// (Worked out again if the run has changed since: the staging asks
	// where men are while it is still moving them about.)
	if (
		!got ||
		got.s0 !== s0 ||
		got.s1 !== s1 ||
		got.v0 !== v0 ||
		got.v1 !== v1 ||
		got.shape.L !== L
	) {
		got = {
			s0,
			s1,
			v0,
			v1,
			shape: runShape(L, (s1 - s0) / 1000, v0, v1),
		};
		shapes.set(mv, got);
	}
	return got.shape;
};
const gaitFor = (v: number): AnimName =>
	v >= SPRINT_FTPS
		? "sprint"
		: v >= JOG_FTPS
			? "run"
			: v >= WALK_FTPS
				? "jog"
				: "walk";
const runAnim = (run: Run): AnimName => {
	const anim = run.mv.anim;
	const v = Math.max(shapeOf(run).vc, run.v0, run.v1);
	switch (anim) {
		case "run":
		case "jog":
			return gaitFor(v);
		case "walk":
			return v > WALK_FTPS + 2 ? gaitFor(v) : "walk";
		// Walking it, short steps; attacking, long ones.
		case "dribble":
			return v > DRIBBLE_WALK_FTPS ? "dribble" : "dribbleWalk";
		case "slide":
		case "shuffle":
			return v > SLIDE_FTPS ? gaitFor(v) : anim;
		case "back":
			return v > BACK_FTPS ? gaitFor(v) : anim;
		case "drift":
			return v > DRIFT_FTPS ? gaitFor(v) : anim;
		default:
			return anim;
	}
};
const strideOf = (anim: AnimName): number => {
	const a = ANIMS[anim];
	return a.kind === "cycle" ? a.stride : 5;
};

// Whether a run goes the way his back faces, slowly enough to backpedal:
// asked halfway along it, once a run.
const backwards = new WeakMap<Run["mv"], boolean>();
const backward = (tl: CourtTimeline, tr: Track, run: Run): boolean => {
	let b = backwards.get(run.mv);
	if (b === undefined) {
		const dx = run.mv.to.x - run.mv.from.x;
		const dy = run.mv.to.y - run.mv.from.y;
		const d = Math.hypot(dx, dy);
		const v = Math.max(shapeOf(run).vc, run.v0, run.v1);
		if (d < 0.5 || v > BACK_FTPS + 1) {
			b = false;
		} else {
			const yaw = yawAt(tl, tr, (run.s0 + run.s1) / 2);
			b = (dx * Math.cos(yaw) + dy * Math.sin(yaw)) / d < -0.4;
		}
		backwards.set(run.mv, b);
	}
	return b;
};

// Where he is on the floor and what his feet are doing, without asking which
// way he faces (which depends on where the ball is - see yawAt).
type Spot = {
	x: number;
	y: number;
	moveIndex: number;
	moving: boolean;
	// The run he is on, and which way he is heading along it (round a
	// corner, the way the curve goes).
	run?: Run;
	hx: number;
	hy: number;
	// Eased aside off a man (see spotAt): how fast that has him going
	// (feet a second), and how far it has taken him all told.
	nv?: number;
	nd?: number;
};

// Where he is, eased aside off anybody he would be standing on (see
// keepApart in director.ts) - and kept off anybody he would still be in (see
// keepOff).
const spotAt = (tr: Track, t: number): Spot => {
	const s = nudgedAt(tr, t);
	return tr.sep ? keptOff(tr.sep, s, t) : s;
};

// Kept off a man: the way over, sampled every SEP_MS, eased through the
// samples (Catmull-Rom) - where that has him, how fast it has him going, and
// how far it has taken him.
export const SEP_MS = 50;
let crAt = 0;
let crV = 0;
const catmullRom = (a: number[], i: number, u: number) => {
	const n = a.length;
	const p0 = a[Math.max(0, i - 1)]!;
	const p1 = a[i]!;
	const p2 = a[Math.min(n - 1, i + 1)]!;
	const p3 = a[Math.min(n - 1, i + 2)]!;
	const c1 = 0.5 * (p2 - p0);
	const c2 = p0 - 2.5 * p1 + 2 * p2 - 0.5 * p3;
	const c3 = 0.5 * (p3 - p0) + 1.5 * (p1 - p2);
	crAt = p1 + u * (c1 + u * (c2 + u * c3));
	crV = ((c1 + u * (2 * c2 + 3 * u * c3)) * 1000) / SEP_MS;
};
const keptOff = (list: Sep[], s: Spot, t: number): Spot => {
	const k = lastIndex(list, t, (x) => x.t0);
	if (k < 0) {
		return s;
	}
	const { t0, dx, dy, dl } = list[k]!;
	const f = (t - t0) / SEP_MS;
	if (f >= dx.length - 1) {
		return s;
	}
	const i = Math.floor(f);
	const u = f - i;
	catmullRom(dx, i, u);
	const x = crAt;
	const vx = crV;
	catmullRom(dy, i, u);
	const y = crAt;
	const vy = crV;
	return {
		...s,
		x: s.x + x,
		y: s.y + y,
		nv: (s.nv ?? 0) + Math.hypot(vx, vy),
		nd: (s.nd ?? 0) + dl[i]! + (dl[i + 1]! - dl[i]!) * u,
	};
};

const NUDGE_RAMP = 450;
const nudgedAt = (tr: Track, t: number): Spot => {
	const s = rawSpotAt(tr, t);
	const list = tr.nudges;
	if (!list || list.length === 0) {
		return s;
	}
	let dx = 0;
	let dy = 0;
	let vx = 0;
	let vy = 0;
	let nd = 0;
	for (let i = lastIndex(list, t, (n) => n.t0); i >= 0; i--) {
		const n = list[i]!;
		// (None lasts long: the ones begun long before are over.)
		if (t - n.t0 > NUDGE_LONGEST) {
			break;
		}
		if (t >= n.t1) {
			continue;
		}
		const r = Math.min(n.ramp ?? NUDGE_RAMP, (n.t1 - n.t0) / 2);
		const a = (t - n.t0) / r;
		const b = (n.t1 - t) / r;
		const u = Math.min(1, a, b);
		const w = u * u * (3 - 2 * u);
		dx += n.dx * w;
		dy += n.dy * w;
		// (Easing over, or back: how fast, and how far he has stepped.)
		const len = Math.hypot(n.dx, n.dy);
		if (u < 1) {
			const dw = (6 * u * (1 - u) * 1000) / r;
			const sign = a < b ? 1 : -1;
			vx += n.dx * dw * sign;
			vy += n.dy * dw * sign;
		}
		nd += a < b ? len * w : len * (2 - w);
	}
	return dx === 0 && dy === 0
		? s
		: { ...s, x: s.x + dx, y: s.y + dy, nv: Math.hypot(vx, vy), nd };
};
const NUDGE_LONGEST = 15000;
const rawSpotAt = (tr: Track, t: number): Spot => {
	let k = lastIndex(tr.moves, t, (m) => m.t0);
	if (k < 0) {
		return {
			x: tr.start.x,
			y: tr.start.y,
			moveIndex: -1,
			moving: false,
			hx: 0,
			hy: 0,
		};
	}
	let run = runOf(tr, k);
	// Already on his way into the next run, in the pause before it.
	if (t >= run.s1 && k + 1 < tr.moves.length) {
		const next = runOf(tr, k + 1);
		if (t >= next.s0) {
			run = next;
			k += 1;
		}
	}
	const { mv } = run;
	if (t >= run.s1) {
		return { ...mv.to, moveIndex: k, moving: false, hx: 0, hy: 0 };
	}
	let p = onRun(run, t);
	// An inch or two over a second or more - creeping slower than any walk,
	// not part of a run on through: he is standing there, not walking.
	if (
		!run.v0 &&
		!run.v1 &&
		lenOf(mv) < 1.5 &&
		lenOf(mv) < (0.7 * (run.s1 - run.s0)) / 1000
	) {
		return { ...p, moveIndex: k, moving: false, hx: 0, hy: 0 };
	}
	let hx = mv.to.x - mv.from.x;
	let hy = mv.to.y - mv.from.y;
	// Coming round a corner: off the end of the last run and onto this one,
	// or off this one onto the next - heading the way the curve goes. (A
	// short run can be all corners, one at either end.)
	let pair: [Run, Run] | undefined;
	let q: Pt | undefined;
	if (run.v0 > 0 && t - run.s0 < ROUND_MS) {
		pair = [runOf(tr, k - 1), run];
		q = rounded(pair[0], pair[1], t);
	}
	if (!q && run.v1 > 0 && run.s1 - t < ROUND_MS) {
		pair = [run, runOf(tr, k + 1)];
		q = rounded(pair[0], pair[1], t);
	}
	if (pair && q) {
		p = q;
		const ahead = rounded(pair[0], pair[1], t + 8) ?? q;
		const behind = rounded(pair[0], pair[1], t - 8) ?? q;
		if (dist2(ahead, behind) > 1e-6) {
			hx = ahead.x - behind.x;
			hy = ahead.y - behind.y;
		}
	}
	return { ...p, moveIndex: k, moving: true, run, hx, hy };
};

// How many strides into his run he is at t, counting the runs it follows
// on from - so his legs keep their rhythm through a join.
// (A run straight on from the last - planted, but with hardly a moment
// between - carries on his strides from where they were.)
const STRIDE_ON_MS = 220;
const stridesAt = (tr: Track, k: number, run: Run, t: number): number => {
	let strides = alongRun(run, t) / strideOf(runAnim(run));
	for (
		let j = k;
		j > 0 &&
		(runOf(tr, j).v0 > 0 ||
			tr.moves[j]!.t0 - tr.moves[j - 1]!.t1 <= STRIDE_ON_MS);
		j--
	) {
		const before = runOf(tr, j - 1);
		strides += lenOf(before.mv) / strideOf(runAnim(before));
	}
	return strides;
};

// Hands up for the ball: from a moment before it leaves the passer (or
// comes off the floor on its way) until it gets to him.
const TARGET_LEAD = 240;
const TARGET_RAMP = 180;
const targetAt = (tl: CourtTimeline, pid: number, t: number): number => {
	const i = Math.max(
		0,
		lastIndex(tl.ball, t, (s) => s.t0),
	);
	let from: number | undefined;
	for (let k = i; k < tl.ball.length && k <= i + 2; k++) {
		const s = tl.ball[k]!;
		// (Once it is on its way - a bounce pass's first leg - the leg that
		// gets to him counts from then, however late it starts.)
		if (from === undefined && s.t0 > t + TARGET_LEAD) {
			break;
		}
		if (s.kind !== "fly") {
			from = undefined;
			continue;
		}
		from ??= s.t0;
		if ("pid" in s.to) {
			if (s.to.pid !== pid || t >= s.t1) {
				return 0;
			}
			const u = clamp01((t - from + TARGET_LEAD) / TARGET_RAMP);
			return u * u * (3 - 2 * u);
		}
	}
	return 0;
};

// How tense the building is at t (0 to 1): a close game, late.
export const seatsAt = (tl: CourtTimeline, t: number): number => {
	const s = tl.seats ?? [];
	const i = lastIndex(s, t, (x) => x[0]);
	return i >= 0 ? s[i]![1] : 1;
};

export const tensionAt = (tl: CourtTimeline, t: number): number => {
	const i = lastIndex(tl.tension, t, (x) => x[0]);
	return i >= 0 ? tl.tension[i]![1] : 0;
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
	if (seg.kind === "path") {
		return playAt(seg.pts, t - seg.t0);
	}
	return seg.at;
};

const dist2 = (a: Pt, b: Pt) => (a.x - b.x) ** 2 + (a.y - b.y) ** 2;

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

// Moving like this, he faces the ball, not where he is going.
const EYES_ON_BALL = new Set<AnimName>([
	"slide",
	"shuffle",
	"closeout",
	"drift",
]);

const PASSES = new Set<AnimName>(["pass", "passBounce", "passOverhead"]);
export const PASS_TURN = 250;
export const PASS_PICK_UP = new Set<AnimName>([
	"dribble",
	"dribbleWalk",
	"back",
]);

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
	// About to pass it - standing, or picking up his dribble for it - he
	// squares up to where it goes first, not round on it as it leaves his
	// hands.
	if (!act) {
		const nx = tr.acts[lastIndex(tr.acts, t + PASS_TURN, (a) => a.t0)];
		if (
			nx &&
			nx.t0 > t &&
			nx.look &&
			PASSES.has(nx.anim) &&
			(!here.moving ||
				(mv !== undefined && PASS_PICK_UP.has(mv.anim) && mv.t1 <= nx.t0 + 40))
		) {
			return angleTo(here, nx.look, fallback);
		}
	}
	// How he is going: a slide too fast to be one is a run, say.
	const gait = here.run ? runAnim(here.run) : mv?.anim;
	// A defensive slide keeps his eyes on the ball whichever way he goes -
	// and so does a man drifting along the arc for it.
	if (mv && here.moving && gait && EYES_ON_BALL.has(gait)) {
		return angleTo(here, ballNear(tl, t), fallback);
	}
	if (mv && here.moving) {
		const dx = mv.to.x - mv.from.x;
		const dy = mv.to.y - mv.from.y;
		const d = Math.hypot(dx, dy);
		// Walking the ball a few steps, or backing out of an attack, a man
		// with the ball stays squared up to his man and the rim.
		if (
			d < 8 &&
			mv.face !== undefined &&
			(mv.anim === "dribble" || mv.anim === "back")
		) {
			const own = ballSegAt(tl, t);
			if (own?.kind === "hold" && own.pid === tr.pid && own.style !== "hold") {
				return angleTo(here, { x: rimX(tr.team), y: 25 }, fallback);
			}
		}
		if (d >= 3) {
			const heading =
				here.hx * here.hx + here.hy * here.hy > 1e-6
					? Math.atan2(here.hy, here.hx)
					: Math.atan2(dy, dx);
			if (gait === "back") {
				return heading + Math.PI;
			}
			// Told to face against where he is going: a slide with his man, or a
			// backpedal - he keeps his eyes on the ball, unless he has had to
			// open up and run.
			if (
				mv.face !== undefined &&
				mv.face * dx < 0 &&
				d < 14 &&
				gait !== "run" &&
				gait !== "sprint"
			) {
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

// Turning takes a moment: he turns toward wherever he means to face no
// faster than a player can pivot. Worked out step by step on a steady beat,
// from a moment well enough back that he had settled - the same moment for
// every frame near this one, so the next frame turns him the way this one
// did - and eased between beats. No state that depends on which frames were
// drawn before, so any frame can be asked for and every viewing agrees.
const TURN_STEP = 40;
// (Radians a ms.)
export const TURN_RATE = (Math.PI * 3) / 1000;
const TURN_MAX = TURN_RATE * TURN_STEP;
const TURN_SETTLE = 1000;
const TURN_ANCHOR = 2000;
const TAU = Math.PI * 2;
const wrapAngle = (d: number) => d - Math.round(d / TAU) * TAU;
// A step of turning, and which way he is turning (0 once he faces where he
// means to).
type Turning = { yaw: number; sense: number };
const turnStep = ({ yaw, sense }: Turning, target: number): Turning => {
	let d = wrapAngle(target - yaw);
	// Turning right round, he keeps on the way he was already turning; from
	// a standstill, he comes round facing the camera - not whichever way a
	// hair's difference happens to point.
	if (Math.abs(d) > Math.PI * 0.8) {
		const other = d - Math.sign(d) * TAU;
		if (
			sense !== 0
				? Math.sign(other) === sense
				: Math.sin(yaw + other / 2) > Math.sin(yaw + d / 2)
		) {
			d = other;
		}
	}
	const step = Math.max(-TURN_MAX, Math.min(TURN_MAX, d));
	return {
		yaw: yaw + step,
		sense: Math.abs(d) <= TURN_MAX ? 0 : Math.sign(step),
	};
};
// Worked-out beats, kept per player: keyed by the beat and which settling
// moment it was worked out from.
const beatYaws = new WeakMap<Track, Map<number, Turning>>();
const yawOnBeat = (tl: CourtTimeline, tr: Track, g: number): number => {
	const from = Math.floor((g - TURN_SETTLE) / TURN_ANCHOR) * TURN_ANCHOR;
	const tag = Math.abs(Math.round(from / TURN_ANCHOR)) % 2;
	let kept = beatYaws.get(tr);
	if (!kept) {
		kept = new Map();
		beatYaws.set(tr, kept);
	}
	const key = (k: number) => k * 2 + tag;
	let k = g;
	while (k > from && !kept.has(key(k))) {
		k -= TURN_STEP;
	}
	let turning: Turning =
		k > from ? kept.get(key(k))! : { yaw: yawTarget(tl, tr, from), sense: 0 };
	if (kept.size > 4096) {
		kept.clear();
	}
	for (k = Math.max(k, from) + TURN_STEP; k <= g; k += TURN_STEP) {
		turning = turnStep(turning, yawTarget(tl, tr, k));
		kept.set(key(k), turning);
	}
	return turning.yaw;
};
const yawAt = (tl: CourtTimeline, tr: Track, t: number): number => {
	const g = Math.floor(t / TURN_STEP) * TURN_STEP;
	const a = yawOnBeat(tl, tr, g);
	const u = (t - g) / TURN_STEP;
	return u <= 0 ? a : a + wrapAngle(yawOnBeat(tl, tr, g + TURN_STEP) - a) * u;
};

// OFF THE BALL.
//
// Standing his ground while the ball is in play somewhere else, a man is no
// statue: a shooter sinks into his stance and shows his hands, a man calls
// for it or claps for it; a defender works his hands into the lane, or
// points out his man. Which, and when, comes from who he is and when he
// stopped - the same every viewing.
const OFF_BALL: Record<"ready" | "stance", [AnimName, number][]> = {
	ready: [
		["spotUp", 1500],
		["callBall", 1100],
		["clapCall", 900],
		["spotUp", 1800],
	],
	stance: [
		["stanceHands", 1100],
		["stancePoint", 1000],
		["stanceHands", 1400],
	],
};
// A beat of standing (ms), with at most one of them in it.
const LIFE_EVERY = 2300;
const hash01 = (a: number, b: number): number => {
	const x = Math.sin(a * 12.9898 + b * 78.233) * 43758.5453;
	return x - Math.floor(x);
};
const offBall = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	base: "ready" | "stance",
): { anim: AnimName; phase: number } | undefined => {
	// Only with the ball in play: in a man's hands, or on its way between
	// two - not at a free throw.
	const seg = ballSegAt(tl, t);
	if (
		!(
			seg?.kind === "hold" ||
			(seg?.kind === "fly" && "pid" in seg.from && "pid" in seg.to)
		)
	) {
		return undefined;
	}
	const beat = tl.beats[lastIndex(tl.beats, t, (b) => b.preStart)];
	if (beat && (beat.type === "ft" || beat.type === "missFt")) {
		return undefined;
	}
	// Since when he has stood here, and until he next moves or acts.
	const mi = lastIndex(tr.moves, t, (m) => m.t0);
	let since = mi >= 0 ? tr.moves[mi]!.t1 : 0;
	let until = tr.moves[mi + 1]?.t0 ?? Infinity;
	const ai = lastIndex(tr.acts, t, (a) => a.t0);
	for (let k = ai; k >= 0 && k >= ai - 3; k--) {
		since = Math.max(since, tr.acts[k]!.t1);
	}
	until = Math.min(until, tr.acts[ai + 1]?.t0 ?? Infinity);
	const k = Math.floor((t - since - 300) / LIFE_EVERY);
	if (k < 0) {
		return undefined;
	}
	const h = hash01(tr.pid, since / 1000 + k);
	if (h < 0.4) {
		return undefined;
	}
	const list = OFF_BALL[base];
	const [anim, dur] = list[Math.floor(h * 1000) % list.length]!;
	const start =
		since +
		300 +
		k * LIFE_EVERY +
		hash01(tr.pid + 1, since / 1000 + k) * (LIFE_EVERY - dur - 200);
	if (t < start || t >= start + dur || start + dur > until) {
		return undefined;
	}
	return { anim, phase: (t - start) / dur };
};

// The hand the ball is in, with whoever has it at t: on its way down, the
// hand it left; on its way up, the hand it goes to - and in both hands, as
// good as his right.
const ballHand = (tl: CourtTimeline, pid: number, t: number): Hand => {
	const bi = ballIndexAt(tl, t);
	const seg = tl.ball[bi];
	if (seg?.kind !== "hold" || seg.pid !== pid || seg.style === "hold") {
		return "R";
	}
	const beat = seg.style === "cross" ? crossAt(seg, t) : bounceOf(tl, bi, t);
	return beat.ph < DOWN ? beat.from : beat.to;
};
// Up on the man with the ball, the hand on the ball's side is down at it
// and the other up - his right hand the ball on the defender's left - and
// they trade, a beat behind, when it goes across (see the guard loop: 0 his
// left down, 0.5 his right).
const GUARD_SAMPLES = 12;
const guardHands = (tl: CourtTimeline, man: number, t: number): number => {
	// How much of the last little while it has been in his left - eased, so
	// the hands go across together and smoothly, never in jumps.
	let left = 0;
	for (let i = 0; i < GUARD_SAMPLES; i++) {
		left += ballHand(tl, man, t - 110 - i * 20) === "L" ? 1 : 0;
	}
	return smooth01(left / GUARD_SAMPLES) * 0.5;
};

// How much of the way he is going is across the way he faces (0 straight
// ahead or back, 1 square to his side).
// Whether a slide goes across the way he faces (push steps) rather than
// toward or away from it (drop steps): decided for the whole run, by how he
// faces halfway along it, so his feet never switch from one to the other
// mid-stride.
const sidewaysRuns = new WeakMap<Run["mv"], boolean>();
const sideways = (tl: CourtTimeline, tr: Track, run: Run): boolean => {
	let s = sidewaysRuns.get(run.mv);
	if (s === undefined) {
		const dx = run.mv.to.x - run.mv.from.x;
		const dy = run.mv.to.y - run.mv.from.y;
		const d = Math.hypot(dx, dy);
		if (d < 0.05) {
			s = false;
		} else {
			const yaw = yawAt(tl, tr, (run.s0 + run.s1) / 2);
			s = Math.abs(Math.cos(yaw) * dy - Math.sin(yaw) * dx) / d > 0.6;
		}
		sidewaysRuns.set(run.mv, s);
	}
	return s;
};

// Up on the ball, now and then he pokes at it - a quick swipe that gets
// nothing - with the hand on its side as he starts it. Never the moment he
// has got there.
const SWIPE_EVERY = 2600;
const SWIPE_MS = 420;
const SWIPE_SHARE = 0.09;
const swipeAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	man: number,
): { anim: AnimName; phase: number; mirror?: boolean } | undefined => {
	const k = Math.floor(t / SWIPE_EVERY);
	if (hash01(tr.pid + 7, k) >= SWIPE_SHARE) {
		return undefined;
	}
	const start =
		k * SWIPE_EVERY + hash01(tr.pid + 3, k) * (SWIPE_EVERY - SWIPE_MS);
	if (t < start || t >= start + SWIPE_MS) {
		return undefined;
	}
	const mi = lastIndex(tr.moves, start, (m) => m.t0);
	if (mi >= 0 && tr.moves[mi]!.t1 > start - 400) {
		return undefined;
	}
	return {
		anim: "poke",
		phase: (t - start) / SWIPE_MS,
		mirror: ballHand(tl, man, start) === "R",
	};
};

// What his body is doing at t - the move he makes, how far through it, the
// ball in his hands and his dribble - apart from where he is and which way
// he faces.
type Doing = {
	anim: AnimName;
	phase: number;
	z: number;
	mirror?: boolean;
	dunk?: PlayerState["dunk"];
	reach?: number;
	holding: boolean;
	grip?: number;
	dribble?: number;
	dribbleHand?: Hand;
	target?: number;
};
// Stood watching - arms folded, hands on his hips - he doesn't glide off in
// it: once he is on his way somewhere, he walks there.
const STANDING = new Set<AnimName>([
	"crossed",
	"hips",
	"flex",
	"celebrate",
	"protest",
	"point",
	"clap",
	"talk",
]);
// How his feet go on a run at t: the gait for its pace, and how far through
// his strides he is.
const gaitOf = (
	tl: CourtTimeline,
	tr: Track,
	k: number,
	run: Run,
	t: number,
): { anim: AnimName; phase: number } => {
	let anim = runAnim(run);
	let phase = stridesAt(tr, k, run, t);
	// Sliding with his man: push steps when he goes across the way he
	// faces, drop steps when he gives ground or steps up.
	if (anim === "slide" && sideways(tl, tr, run)) {
		phase *= strideOf("slide") / strideOf("shuffle");
		anim = "shuffle";
	}
	// Going the way his back faces - easing off from the ball while he
	// watches it - at no more than a backpedal's pace: he backpedals, not
	// runs forward while he goes backward. (Decided for the whole run, by
	// how he faces halfway along it, so his legs don't switch mid-stride.)
	if (
		(anim === "walk" || anim === "jog" || anim === "run") &&
		backward(tl, tr, run)
	) {
		phase *= strideOf(anim) / strideOf("back");
		anim = "back";
	}
	return { anim, phase };
};

// Between two runs with hardly a moment between them, he plants on the last
// stride and goes - he does not stand up into his stance for a frame or two.
const BRIDGE_MS = 220;
const bridgeAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
): { anim: AnimName; phase: number } | undefined => {
	const k = lastIndex(tr.moves, t, (m) => m.t0);
	const m = k >= 0 ? tr.moves[k] : undefined;
	const next = tr.moves[k + 1];
	if (
		!m ||
		!next ||
		t < m.t1 ||
		t >= next.t0 ||
		next.t0 - m.t1 > BRIDGE_MS ||
		Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y) < 0.5
	) {
		return undefined;
	}
	// (Not through a move with the ball: that is all of him.)
	const seg = ballSegAt(tl, t);
	if (seg?.kind === "hold" && seg.pid === tr.pid && seg.style === "cross") {
		return undefined;
	}
	const run = runOf(tr, k);
	const g = gaitOf(tl, tr, k, run, Math.min(t, run.s1));
	return ANIMS[g.anim].kind === "cycle" ? g : undefined;
};

// How long his hands take to close on the ball once it is his.
const GRIP_MS = 110;
const doingAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here: Spot = spotAt(tr, t),
): Doing => {
	const pid = tr.pid;
	const standing = actAt(tr, t);
	const act =
		standing && STANDING.has(standing.anim) && here.moving && here.run
			? undefined
			: standing;
	let anim: AnimName;
	let phase: number;
	let z = 0;
	let dunk: PlayerState["dunk"];
	let reach: number | undefined;
	let mirrored = false;
	let bridged: { anim: AnimName; phase: number } | undefined;
	if (act) {
		mirrored = act.mirror === true;
		const u = (t - act.t0) / (act.t1 - act.t0);
		anim = act.anim;
		const a = ANIMS[anim];
		phase =
			a.kind === "act"
				? clamp01(u)
				: ((t - act.t0) / 1000) * (a.kind === "loop" ? a.fps / a.n : 1);
		z = jumpZ(act, u);
		if (act.reach && act.jump) {
			reach = act.jump[2];
		}
		if (act.rim && act.zKeys) {
			dunk = {
				leap: Math.max(...act.zKeys.map((k) => k[1])),
				rim: act.rim.at,
				grip: keyAt(act.rim.grip, clamp01(u)),
			};
		}
	} else if (here.moving && here.run) {
		({ anim, phase } = gaitOf(tl, tr, here.moveIndex, here.run, t));
		z = bounceAt(anim, phase);
	} else if ((bridged = bridgeAt(tl, tr, t))) {
		({ anim, phase } = bridged);
	} else {
		const seg = ballSegAt(tl, t);
		// A dribble move: all of him through each bounce of it.
		const move =
			seg?.kind === "hold" && seg.pid === pid && seg.style === "cross"
				? crossMoveAt(tl, seg, t)
				: undefined;
		if (move) {
			anim = moveAnim(move.move, move.from);
		} else if (seg && seg.kind === "hold" && seg.pid === pid) {
			anim = seg.style === "hold" ? "hold" : "dribbleIdle";
		} else if (jumpBallAt(tl, t) || offenseAt(tl, t) === tr.team) {
			anim = "ready";
		} else {
			// Up on the man with the ball, or set in his stance off it.
			const man = seg?.kind === "hold" ? tl.tracks.get(seg.pid) : undefined;
			const on =
				man !== undefined &&
				man.team !== tr.team &&
				dist2(spotAt(man, t), here) < 7 * 7;
			anim = on ? "guard" : "stance";
		}
		const a = ANIMS[anim];
		const fps = a.kind === "loop" ? a.fps : 2;
		phase = move
			? move.ph
			: anim === "guard" && seg?.kind === "hold"
				? guardHands(tl, seg.pid, t)
				: (t / 1000) * (fps / a.n) + pid * 0.37;
		const life:
			| { anim: AnimName; phase: number; mirror?: boolean }
			| undefined = jumpBallAt(tl, t)
			? undefined
			: anim === "ready" || anim === "stance"
				? offBall(tl, tr, t, anim)
				: anim === "guard" && seg?.kind === "hold"
					? swipeAt(tl, tr, t, seg.pid)
					: undefined;
		if (
			(here.nv ?? 0) > 1 &&
			(anim === "ready" || anim === "stance" || anim === "guard")
		) {
			// Eased aside off a man: he steps over, not slides.
			anim = "shuffle";
			phase = (here.nd ?? 0) / strideOf("shuffle");
		} else if (life) {
			anim = life.anim;
			phase = life.phase;
			mirrored = life.mirror === true;
		}
	}
	const bi = ballIndexAt(tl, t);
	const seg = tl.ball[bi];
	const has = seg?.kind === "hold" && seg.pid === pid ? seg : undefined;
	// Where his dribble is, and the hand on the ball: through a bounce that
	// changes hands, the one it left on the way down, the one it goes to on
	// the way up.
	const beat =
		has?.style === "cross"
			? crossAt(has, t)
			: has?.style === "dribble"
				? bounceOf(tl, bi, t)
				: undefined;
	// Since it came into his hands.
	let grip = 1;
	if (has?.style === "hold") {
		let since = has.t0;
		for (let k = bi - 1; k >= 0; k--) {
			const x = tl.ball[k]!;
			if (x.kind !== "hold" || x.pid !== pid || x.style !== "hold") {
				break;
			}
			since = x.t0;
		}
		grip = smooth01((t - since) / GRIP_MS);
	}
	return {
		anim,
		phase,
		z,
		...(mirrored ? { mirror: true } : {}),
		...(dunk ? { dunk } : {}),
		...(reach ? { reach } : {}),
		holding: has?.style === "hold",
		...(grip < 1 ? { grip } : {}),
		dribble: beat?.ph,
		dribbleHand: beat ? (beat.ph < DOWN ? beat.from : beat.to) : undefined,
		target: act || has ? undefined : targetAt(tl, pid, t) || undefined,
	};
};

// One move into the next eases in: for a moment after the change his body
// is part the way from how the last move had it to how this one wants it -
// not snapped there between one frame and the next. Into a dribble move his
// hands and shoulders get there quicker than his feet: a bounce is over in a
// third of a second, and the ball has to go where they take it.
const BLEND_MS = 160;
const MOVE_ARMS_MS = 60;
const BLEND_DEPTH = 3;
const blendInto = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	anim: AnimName,
	// How far back inside other blends this is looking (see BLEND_DEPTH).
	depth = 0,
): Blend | undefined => {
	let lo = t - BLEND_MS;
	let before = doingAt(tl, tr, lo);
	if (before.anim === anim) {
		// The same as a moment ago - but maybe something else in between,
		// and back: out of that, then.
		const flick = [40, 80, 120]
			.map((back) => ({ at: t - back, d: doingAt(tl, tr, t - back) }))
			.find((x) => x.d.anim !== anim);
		if (!flick) {
			return undefined;
		}
		lo = flick.at;
		before = flick.d;
	}
	// When it changed.
	let hi = t;
	for (let k = 0; k < 5; k++) {
		const mid = (lo + hi) / 2;
		const d = doingAt(tl, tr, mid);
		if (d.anim === anim) {
			hi = mid;
		} else {
			lo = mid;
			before = d;
		}
	}
	const ease = (ms: number) => {
		const u = Math.min(1, (t - hi) / ms);
		return 1 - u * u * (3 - 2 * u);
	};
	const w = ease(BLEND_MS);
	if (w <= 0.02) {
		return undefined;
	}
	// How he looked as it changed: the last move - itself, maybe, still
	// easing out of the one before it, and that out of the one before (a
	// few quick changes in a row must not drop any of them in a frame).
	const prior =
		depth + 1 < BLEND_DEPTH
			? blendInto(tl, tr, lo, before.anim, depth + 1)
			: undefined;
	// The turn of his legs then, eased out of too - not snapped round.
	const legs =
		depth > 0 ? 0 : legsOf(spotAt(tr, lo), yawAt(tl, tr, lo), before.anim, lo);
	return {
		anim: before.anim,
		phase: before.phase,
		dribble: before.dribble,
		dribbleHand: before.dribbleHand,
		target: before.target,
		...(before.mirror ? { mirror: true } : {}),
		w,
		...(legs !== 0 ? { legs } : {}),
		...(isMove(anim) ? { arms: ease(MOVE_ARMS_MS) } : {}),
		...(prior ? { from: prior } : {}),
	};
};

// WITH AN ARM, ON THE MOVE.
//
// Whatever his feet are doing - running, sliding, dribbling - a man can
// still say something with a hand: point out the man he has or the screen
// coming, put a hand up for the ball, wave a teammate on. With his free arm
// (not the one on the ball), and never while all of him is in something
// else (a shot, a catch, a screen) or his hands are up for a pass coming.
const ARM_IN = 0.22;
const ARM_OUT = 0.25;
// A jumper's follow-through as he lands, held on after.
const FOLLOW = poseAt("shoot", 1);
const smooth01 = (u: number) => {
	const v = clamp01(u);
	return v * v * (3 - 2 * v);
};
const armAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here: Pt,
	yaw: number,
	// His free hand: either, the one not on the ball, or neither.
	free: Hand | "both" | "none",
): ArmPose | undefined => {
	const list = tr.arms;
	const gi = lastIndex(list, t, (g) => g.t0);
	const g: Gesture | undefined =
		gi >= 0 && t < list[gi]!.t1 ? list[gi] : undefined;
	if (!g || free === "none") {
		return undefined;
	}
	const u = (t - g.t0) / (g.t1 - g.t0);
	if (g.kind === "follow") {
		// Held up from the moment he lands - his shooting hand, if it is
		// free - and let down slowly at the end.
		const w = smooth01((1 - u) / 0.45);
		return free === "L" || w <= 0.02
			? undefined
			: {
					hand: "R",
					sh: FOLLOW.shN,
					el: FOLLOW.elN,
					ab: FOLLOW.abN,
					wr: FOLLOW.wrN,
					tuck: FOLLOW.tuck,
					w,
				};
	}
	const w = smooth01(u / ARM_IN) * smooth01((1 - u) / ARM_OUT);
	if (w <= 0.02) {
		return undefined;
	}
	// Which way it is from the way he faces: to his right, positive.
	const bearingAt = (when: number, from: Pt, facing: number): number => {
		if (g.at === undefined) {
			return 0;
		}
		const who = typeof g.at === "number" ? tl.tracks.get(g.at) : undefined;
		const P = typeof g.at === "number" ? who && spotAt(who, when) : g.at;
		return P ? wrapAngle(Math.atan2(P.y - from.y, P.x - from.x) - facing) : 0;
	};
	const bearing = bearingAt(t, here, yaw);
	// The arm on that side as he started, if it is free - the same arm all
	// the way through.
	const t0 = Math.min(g.t1, g.t0 + 120);
	const side: Hand =
		bearingAt(t0, spotAt(tr, t0), yawAt(tl, tr, t0)) >= 0 ? "R" : "L";
	const hand: Hand = free === "both" ? side : free;
	// Out from that shoulder (degrees): across his body, a little at most.
	const out = Math.max(
		-25,
		Math.min(95, ((hand === "R" ? bearing : -bearing) * 180) / Math.PI),
	);
	if (g.kind === "point") {
		return { hand, sh: 98, el: 4, ab: out, wr: 8, w, point: true };
	}
	if (g.kind === "slap") {
		// Out to him, low, the palm open.
		return { hand, sh: 72, el: 18, ab: Math.max(0, out), wr: 24, w };
	}
	if (g.kind === "hand") {
		// Up high, open - pumped once or twice.
		const pump = Math.sin(u * Math.PI * 4) * 7;
		return { hand, sh: 160 + pump, el: 16, ab: 16, wr: 18, w };
	}
	// Come on: the forearm swept in at him and out again.
	const s = Math.sin(u * Math.PI * 6);
	return {
		hand,
		sh: 92,
		el: 48 + 34 * s,
		ab: Math.max(0, out),
		wr: 22,
		w,
	};
};

// ONE PASS AWAY.
//
// Up on his man one pass from the ball, a defender gets his arm out into the
// passing lane - the arm on his man's side, reaching toward a point in the
// lane between the ball and him. It comes in as he gets up on his man and
// goes as the ball gets farther away (or so near the lane is gone), so it
// eases rather than snaps - and never swaps arms while it is out.
const DENY_FROM = new Set<AnimName>([
	"stance",
	"stanceHands",
	"stancePoint",
	"shuffle",
	"slide",
]);
const denyArm = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here: Pt,
	yaw: number,
	anim: AnimName,
): ArmPose | undefined => {
	if (!DENY_FROM.has(anim)) {
		return undefined;
	}
	const bi = ballIndexAt(tl, t);
	const seg = tl.ball[bi];
	if (seg?.kind !== "hold") {
		return undefined;
	}
	const holder = tl.tracks.get(seg.pid);
	if (!holder || holder.team === tr.team) {
		return undefined;
	}
	// Since the ball came to this man (and his last dribble aside), and until
	// it leaves him.
	let since = seg.t0;
	for (let k = bi - 1; k >= 0; k--) {
		const x = tl.ball[k]!;
		if (x.kind !== "hold" || x.pid !== seg.pid) {
			break;
		}
		since = x.t0;
	}
	let until = Infinity;
	for (let k = bi + 1; k < tl.ball.length; k++) {
		const x = tl.ball[k]!;
		if (x.kind !== "hold" || x.pid !== seg.pid) {
			until = x.t0;
			break;
		}
	}
	const B = spotAt(holder, t);
	// His man: the nearest of theirs, the ball aside - and how sure that is
	// (how much nearer than the next).
	let man: Pt | undefined;
	let near = Infinity;
	let next = Infinity;
	for (const o of tl.tracks.values()) {
		if (o.team === tr.team || o === holder) {
			continue;
		}
		const si = lastIndex(o.shown, t, (x) => x[0]);
		if (si < 0 || !o.shown[si]![1]) {
			continue;
		}
		const p = spotAt(o, t);
		const d = Math.hypot(p.x - here.x, p.y - here.y);
		if (d < near) {
			next = near;
			near = d;
			man = p;
		} else if (d < next) {
			next = d;
		}
	}
	if (!man) {
		return undefined;
	}
	const far = Math.hypot(man.x - B.x, man.y - B.y);
	const lane = { x: B.x + (man.x - B.x) * 0.7, y: B.y + (man.y - B.y) * 0.7 };
	const bearing = wrapAngle(Math.atan2(lane.y - here.y, lane.x - here.x) - yaw);
	const deg = (Math.abs(bearing) * 180) / Math.PI;
	// (Down while he turns, or while it is not clear which man is his: it
	// changes sides only then.)
	const turning = Math.abs(wrapAngle(yaw - yawAt(tl, tr, t - 100)));
	const w =
		smooth01((7 - near) / 2.5) *
		smooth01((26 - far) / 4) *
		smooth01((far - 9) / 3) *
		smooth01((deg - 10) / 20) *
		smooth01((125 - deg) / 30) *
		smooth01((0.22 - turning) / 0.12) *
		smooth01((next - near) / 2) *
		smooth01((t - since) / 350) *
		smooth01((until - t) / 300);
	if (w <= 0.05) {
		return undefined;
	}
	return {
		hand: bearing >= 0 ? "R" : "L",
		sh: 90,
		el: 12,
		ab: Math.min(85, deg),
		wr: 36,
		w,
	};
};

// UP AGAINST HIS MAN.
//
// A man with the ball and the man on him are never two men who happen to be
// near each other - nor is a cutter and the man chasing him. With his man
// tight on him, a ball handler's free arm comes up as a bar between him and
// the ball, and a cutter's comes up to fight him off; and on the move, the
// man guarding either one rides him - a hand on his hip, feeling where he
// goes. All of it eases in as the two close and out as they part.
const ENGAGE_NEAR = 4.6;
// Off the ball, only this close.
const FIGHT_NEAR = 3.2;
const RIDE_FROM = new Set<AnimName>([
	"shuffle",
	"slide",
	"back",
	"run",
	"jog",
	"sprint",
	"stance",
	"stanceHands",
]);
// How fast a man goes at t (feet a second).
const speedAt = (tr: Track, t: number): number => {
	const a = spotAt(tr, t - 150);
	const b = spotAt(tr, t);
	return Math.hypot(b.x - a.x, b.y - a.y) / 0.15;
};
// The nearest man on the floor of the other side to one at P.
const nearestOf = (
	tl: CourtTimeline,
	team: Side,
	P: Pt,
	t: number,
	but?: number,
): { tr: Track; at: Pt; d: number } | undefined => {
	let best: { tr: Track; at: Pt; d: number } | undefined;
	for (const o of tl.tracks.values()) {
		if (o.team === team || o.pid === but) {
			continue;
		}
		const p = floorSpotOf(o, t);
		if (!p) {
			continue;
		}
		const d = Math.hypot(p.x - P.x, p.y - P.y);
		if (!best || d < best.d) {
			best = { tr: o, at: p, d };
		}
	}
	return best;
};
// A defender's hand on the man he rides: on his side - let go as he
// crosses in front, and only put back once he is past, on the other, so it
// never swaps while it is on him.
const rideArm = (
	tl: CourtTimeline,
	tr: Track,
	him: Track,
	t: number,
	here: Pt,
	yaw: number,
	w0: number,
): ArmPose | undefined => {
	const H = spotAt(him, t);
	const bearing = wrapAngle(Math.atan2(H.y - here.y, H.x - here.x) - yaw);
	const deg = (Math.abs(bearing) * 180) / Math.PI;
	const t0 = t - 250;
	const me0 = spotAt(tr, t0);
	const him0 = spotAt(him, t0);
	const before = wrapAngle(
		Math.atan2(him0.y - me0.y, him0.x - me0.x) - yawAt(tl, tr, t0),
	);
	if (Math.sign(before) !== Math.sign(bearing)) {
		return undefined;
	}
	const w =
		w0 *
		smooth01((speedAt(him, t) - 4) / 3) *
		smooth01((140 - deg) / 30) *
		smooth01((deg - 15) / 20);
	if (w <= 0.05) {
		return undefined;
	}
	return {
		hand: bearing >= 0 ? "R" : "L",
		sh: 62,
		el: 26,
		ab: Math.min(80, deg),
		wr: 12,
		w,
	};
};
// An attacker's free forearm up between him and the man on him: out toward
// him on that arm's side, from straight ahead round to beside him - never
// reaching back across his body.
const barArm = (
	free: Hand,
	here: Pt,
	yaw: number,
	them: Pt,
	w0: number,
): ArmPose | undefined => {
	const bearing = wrapAngle(Math.atan2(them.y - here.y, them.x - here.x) - yaw);
	const deg = ((free === "R" ? bearing : -bearing) * 180) / Math.PI;
	const w = w0 * smooth01((deg + 30) / 30) * smooth01((150 - deg) / 30);
	if (w <= 0.05) {
		return undefined;
	}
	return {
		hand: free,
		sh: 58,
		el: 92,
		ab: Math.max(18, Math.min(70, deg)),
		wr: 0,
		w,
	};
};
const engageArm = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here: Pt,
	yaw: number,
	now: { anim: AnimName; dribbleHand?: Hand; holding?: boolean },
): ArmPose | undefined => {
	const seg = ballSegAt(tl, t);
	if (seg?.kind !== "hold" || actAt(tr, t)) {
		return undefined;
	}
	const holder = tl.tracks.get(seg.pid);
	if (!holder) {
		return undefined;
	}
	const offense = holder.team;
	if (tr === holder) {
		// On his dribble, his man tight on him.
		const free =
			now.dribbleHand === "R" ? "L" : now.dribbleHand === "L" ? "R" : undefined;
		if (seg.style === "hold" || !free || isMove(now.anim)) {
			return undefined;
		}
		const D = nearestOf(tl, offense, here, t);
		if (!D || D.d > ENGAGE_NEAR) {
			return undefined;
		}
		return barArm(free, here, yaw, D.at, smooth01((ENGAGE_NEAR - D.d) / 1.4));
	}
	if (tr.team === offense) {
		// Cutting, with his man on his hip: fighting him off.
		if (speedAt(tr, t) < 8) {
			return undefined;
		}
		const D = nearestOf(tl, offense, here, t);
		if (!D || D.d > FIGHT_NEAR) {
			return undefined;
		}
		const bearing = wrapAngle(
			Math.atan2(D.at.y - here.y, D.at.x - here.x) - yaw,
		);
		const deg = (Math.abs(bearing) * 180) / Math.PI;
		const arm = barArm(
			bearing >= 0 ? "R" : "L",
			here,
			yaw,
			D.at,
			smooth01((FIGHT_NEAR - D.d) / 1) * smooth01((deg - 15) / 20),
		);
		return arm && { ...arm, sh: 70, el: 70 };
	}
	if (!RIDE_FROM.has(now.anim)) {
		return undefined;
	}
	// On the man with the ball, as he goes somewhere with it.
	if (seg.style !== "hold") {
		const H = spotAt(holder, t);
		const d = Math.hypot(H.x - here.x, H.y - here.y);
		const D = nearestOf(tl, offense, H, t);
		if (D?.tr === tr && d <= ENGAGE_NEAR) {
			return rideArm(
				tl,
				tr,
				holder,
				t,
				here,
				yaw,
				smooth01((ENGAGE_NEAR - d) / 1.4),
			);
		}
	}
	// On a cutter: the nearest of theirs to him, and him the nearest of
	// ours to that one.
	const A = nearestOf(tl, tr.team, here, t, holder.pid);
	if (!A || A.d > FIGHT_NEAR) {
		return undefined;
	}
	if (nearestOf(tl, offense, A.at, t)?.tr !== tr) {
		return undefined;
	}
	return rideArm(tl, tr, A.tr, t, here, yaw, smooth01((FIGHT_NEAR - A.d) / 1));
};

// Just where he is on the floor at t, if he is on it - cheap, for asking
// of every man at every step.
export const floorSpotOf = (tr: Track, t: number): Pt | undefined => {
	const si = lastIndex(tr.shown, t, (x) => x[0]);
	if (si < 0 || !tr.shown[si]![1]) {
		return undefined;
	}
	const s = spotAt(tr, t);
	return { x: s.x, y: s.y };
};

// An arm saying something, or up against his man, at t - as it is worked out
// at that very moment (see armAt, denyArm, engageArm).
const armNow = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
): ArmPose | undefined => {
	const si = lastIndex(tr.shown, t, (s) => s[0]);
	if (si < 0 || !tr.shown[si]![1]) {
		return undefined;
	}
	const here = spotAt(tr, t);
	const now = doingAt(tl, tr, t, here);
	const yaw = yawAt(tl, tr, t);
	const said =
		tr.arms.length > 0
			? armAt(
					tl,
					tr,
					t,
					here,
					yaw,
					actAt(tr, t) || now.holding || (now.target ?? 0) > 0
						? "none"
						: now.dribbleHand === "R"
							? "L"
							: now.dribbleHand === "L"
								? "R"
								: "both",
				)
			: undefined;
	return (
		said ??
		denyArm(tl, tr, t, here, yaw, now.anim) ??
		engageArm(tl, tr, t, here, yaw, now)
	);
};
// Each of those comes and goes on its own reasons - his man close enough,
// his pace, the ball in a man's hands - and never snaps up or drops in a
// frame for it: it is worked out every ARM_GRID ms (kept, so a moment is
// worked out once), and what shows is the last ARM_TAPS of those, run
// smoothly from one to the next - an arm a beat behind its reason, easing
// in and out, the way a man's are.
const ARM_GRID = 50;
const ARM_TAPS = 2;
const ARM_KEEP = 64;
const armGrids = new WeakMap<
	CourtTimeline,
	Map<number, Map<number, ArmPose | null>>
>();
const armOnGrid = (
	tl: CourtTimeline,
	tr: Track,
	k: number,
): ArmPose | undefined => {
	let byPid = armGrids.get(tl);
	if (!byPid) {
		byPid = new Map();
		armGrids.set(tl, byPid);
	}
	let kept = byPid.get(tr.pid);
	if (!kept) {
		kept = new Map();
		byPid.set(tr.pid, kept);
	}
	const had = kept.get(k);
	if (had !== undefined) {
		return had ?? undefined;
	}
	const a = armNow(tl, tr, k * ARM_GRID);
	if (kept.size >= ARM_KEEP) {
		kept.clear();
	}
	kept.set(k, a ?? null);
	return a;
};
const easedArm = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
): ArmPose | undefined => {
	const k = Math.floor(t / ARM_GRID);
	const f = t / ARM_GRID - k;
	// How much each grid moment counts: the last ARM_TAPS up to k, run on
	// toward the ARM_TAPS up to k + 1.
	const taps: { a: ArmPose; c: number }[] = [];
	for (let j = k - ARM_TAPS + 1; j <= k + 1; j++) {
		const c = (j === k - ARM_TAPS + 1 ? 1 - f : j === k + 1 ? f : 1) / ARM_TAPS;
		const a = c > 0 ? armOnGrid(tl, tr, j) : undefined;
		if (a && a.w > 0) {
			taps.push({ a, c: c * a.w });
		}
	}
	if (taps.length === 0) {
		return undefined;
	}
	// The hand most of it is in, and its angles as they run.
	let r = 0;
	let l = 0;
	for (const x of taps) {
		if (x.a.hand === "R") {
			r += x.c;
		} else {
			l += x.c;
		}
	}
	const hand: Hand = r >= l ? "R" : "L";
	const mine = taps.filter((x) => x.a.hand === hand);
	const w = hand === "R" ? r : l;
	if (w <= 0.02) {
		return undefined;
	}
	const mean = (get: (a: ArmPose) => number) =>
		mine.reduce((sum, x) => sum + get(x.a) * x.c, 0) / w;
	const tucked = mine.some((x) => x.a.tuck !== undefined);
	const last = mine.at(-1)!.a;
	return {
		hand,
		sh: mean((a) => a.sh),
		el: mean((a) => a.el),
		ab: mean((a) => a.ab),
		wr: mean((a) => a.wr),
		w: Math.min(1, w),
		...(tucked ? { tuck: mean((a) => a.tuck ?? 0) } : {}),
		...(last.point ? { point: true } : {}),
	};
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
	const now = doingAt(tl, tr, t, here);
	const from = shown ? blendInto(tl, tr, t, now.anim) : undefined;
	const yaw = yawAt(tl, tr, t);
	const legs = legsOf(here, yaw, now.anim, t);
	const arm = shown ? easedArm(tl, tr, t) : undefined;
	return {
		pid,
		team: tr.team,
		shown,
		x: here.x,
		y: here.y,
		yaw,
		moving: here.moving,
		...now,
		...(legs !== 0 ? { legs } : {}),
		...(from ? { from } : {}),
		...(arm ? { arm } : {}),
	};
};

// Going one way while he looks another - a few steps aside with his eyes on
// the ball, or not yet round from his last way - his legs go the way he
// goes, his hips turned under him as far as hips turn; past that, they
// give up on it, and right round (backing off) not at all. Eased in as he
// gets going and out as he pulls up, so he is square again when he stops.
const LEGS_TURN = 75;
const LEGS_EASE_MS = 220;
const LEGS_GIVE = 40;
const legsOf = (here: Spot, yaw: number, anim: AnimName, t: number): number => {
	const run = here.run;
	if (!here.moving || !run || !isGait(anim)) {
		return 0;
	}
	if (here.hx * here.hx + here.hy * here.hy < 1e-6) {
		return 0;
	}
	const off = (wrapAngle(Math.atan2(here.hy, here.hx) - yaw) * 180) / Math.PI;
	const a = Math.abs(off);
	const turn =
		a <= LEGS_TURN
			? a
			: LEGS_TURN * Math.max(0, 1 - (a - LEGS_TURN) / LEGS_GIVE);
	if (turn < 0.5) {
		return 0;
	}
	// Into it as he gets going from a standstill, out of it as he pulls up
	// to one - on the clock, not his pace, which drops away too fast at
	// the very end for his hips to follow.
	const ease = Math.min(
		run.v0 > 0 ? 1 : smooth01((t - run.s0) / LEGS_EASE_MS),
		run.v1 > 0 ? 1 : smooth01((run.s1 - t) / LEGS_EASE_MS),
	);
	// (His left is the floor's clockwise.)
	return -Math.sign(off) * turn * ease;
};

// The arms and the turn of his shoulders, which can ease into a move on
// their own time (see blendInto).
const UPPER: (keyof Pose)[] = [
	"shN",
	"elN",
	"abN",
	"wrN",
	"shF",
	"elF",
	"abF",
	"wrF",
	"flare",
	"tuck",
	"free",
	"twist",
	"tilt",
];
// The pose a blend shows: its move's, itself easing out of the one before.
const blendPose = (f: Blend): Pose => {
	const as = posed(f.anim, f.phase, f.dribble, f.dribbleHand, f.target);
	const own = f.mirror ? mirror(as) : as;
	const g = f.from;
	return g && g.w > 0 ? lerpPose(own, blendPose(g), g.w) : own;
};
const blendFrom = (q: Pose, f: Blend): Pose => {
	const was = blendPose(f);
	const p = lerpPose(q, was, f.w);
	if (f.arms !== undefined) {
		for (const key of UPPER) {
			p[key] = q[key] + (was[key] - q[key]) * f.arms;
		}
	}
	return p;
};

// His pose: his move's, eased in from the last one's just after a change -
// and an arm in whatever it is saying.
export const poseOf = (st: PlayerState): Pose => {
	const own = posed(st.anim, st.phase, st.dribble, st.dribbleHand, st.target);
	const q = st.mirror ? mirror(own) : own;
	const f = st.from;
	const b = f && f.w > 0 ? blendFrom(q, f) : q;
	const turned = st.legs ?? 0;
	const legs = f && f.w > 0 ? turned + ((f.legs ?? 0) - turned) * f.w : turned;
	const p = legs ? { ...b, legs } : b;
	const a = st.arm;
	if (!a || a.w <= 0) {
		return p;
	}
	const mix = (v: number, to: number) => v + (to - v) * a.w;
	const tuck = a.tuck === undefined ? p.tuck : mix(p.tuck, a.tuck);
	return a.hand === "R"
		? {
				...p,
				shN: mix(p.shN, a.sh),
				elN: mix(p.elN, a.el),
				abN: mix(p.abN, a.ab),
				wrN: mix(p.wrN, a.wr),
				tuck,
			}
		: {
				...p,
				shF: mix(p.shF, a.sh),
				elF: mix(p.elF, a.el),
				abF: mix(p.abF, a.ab),
				wrF: mix(p.wrF, a.wr),
				tuck,
			};
};

// A value through an act, from its keys (0 before the first, the last after
// the last), eased between them.
const keyAt = (keys: [number, number][], u: number): number => {
	if (keys.length === 0 || u <= keys[0]![0]) {
		return keys[0]?.[1] ?? 0;
	}
	for (let i = 1; i < keys.length; i++) {
		const [u1, v1] = keys[i]!;
		if (u <= u1) {
			const [u0, v0] = keys[i - 1]!;
			return v0 + (v1 - v0) * ease(u1 === u0 ? 1 : (u - u0) / (u1 - u0));
		}
	}
	return keys.at(-1)![1];
};

// Up for a dunk, as high as his own reach needs: a typical player's jump,
// less for a seven-footer, more for a guard - so every dunker's hands get
// over the rim. Everything that draws him or puts the ball in his hands
// asks this first; asking twice changes nothing.
const TYPICAL = bodyOf();
const TYPICAL_REACH = standingReach(TYPICAL);
export const withBody = (st: PlayerState, body: Body): PlayerState => {
	const r = st.reach;
	if (r !== undefined && r > 0 && st.z > 0) {
		// Up for a rebound: his hands where a typical player's would get to.
		const more =
			holdBall(body, poseAt(st.anim, st.phase), st.anim).ball.u -
			holdBall(TYPICAL, poseAt(st.anim, st.phase), st.anim).ball.u;
		return { ...st, z: st.z * Math.max(0.3, (r - more) / r), reach: 0 };
	}
	const d = st.dunk;
	if (!d || d.leap <= 0 || st.z <= 0) {
		return st;
	}
	const scale = Math.max(
		0.4,
		(d.leap + TYPICAL_REACH - standingReach(body)) / d.leap,
	);
	return { ...st, z: st.z * scale, dunk: { ...d, leap: 0 } };
};

// A world point in his own frame - forward, to his left, up from his feet.
const toBody = (st: PlayerState, p: Pt3): V3 => {
	const c = Math.cos(st.yaw);
	const s = Math.sin(st.yaw);
	const dx = p.x - st.x;
	const dy = p.y - st.y;
	return { f: dx * c + dy * s, s: dx * s - dy * c, u: p.z - st.z };
};

const mixLimb = (a: Limb, b: Limb, w: number): Limb => {
	const m = (p: V3, q: V3): V3 => ({
		f: p.f + (q.f - p.f) * w,
		s: p.s + (q.s - p.s) * w,
		u: p.u + (q.u - p.u) * w,
	});
	return {
		root: m(a.root, b.root),
		mid: m(a.mid, b.mid),
		end: m(a.end, b.end),
		tip: m(a.tip ?? a.end, b.tip ?? b.end),
	};
};

// The ball just come into his hands: his arms part the way from where they
// were to it (see grip).
export const gripped = (
	held: Skeleton,
	st: PlayerState,
	body: Body,
	q: Pose,
): Skeleton => {
	const g = st.grip;
	if (g === undefined || g >= 1) {
		return held;
	}
	const free = onRim(skeleton(body, q), st, body);
	return {
		...held,
		armR: mixLimb(free.armR, held.armR, g),
		armL: mixLimb(free.armL, held.armL, g),
	};
};

// Hanging on the rim after a dunk: both hands on the front of it, a
// shoulder's width apart.
export const onRim = (sk: Skeleton, st: PlayerState, body: Body): Skeleton => {
	const d = st.dunk;
	if (!d || d.grip <= 0) {
		return sk;
	}
	const at = toBody(st, d.rim);
	const hand = (side: 1 | -1) =>
		armTo(
			body,
			sk.chest,
			{ f: at.f, s: at.s + side * 0.42, u: at.u },
			-30,
			side,
		);
	return {
		...sk,
		armR: mixLimb(sk.armR, hand(-1), d.grip),
		armL: mixLimb(sk.armL, hand(1), d.grip),
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
	st0: PlayerState,
	body: Body,
	which: "near" | "far" | "both" = "both",
): Pt3 => {
	const st = withBody(st0, body);
	const sk = onRim(skeleton(body, poseOf(st)), st, body);
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
export const heldBall = (st0: PlayerState, body: Body): Pt3 => {
	const st = withBody(st0, body);
	return bodyPoint(st, holdBall(body, poseOf(st), st.anim).ball);
};

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
export const BACKSPIN = 2;

// A basketball is 9.4 inches across.
export const BALL_R = 0.39;
// Feet per second, per second.
export const GRAVITY = 32.2;

// The ball's hand-overs from one move to the next - a dribble picked up, a
// ball scooped off the floor - take this long, eased from where the last
// move left it, so it never jumps.
const HANDOVER_MS = 140;
const HANDOVER_REACH = 6;
const BEFORE = 0.5;

// A move's ball through its bounce, in his frame: in the hand it leaves
// (`under` - the ball under a hand at a point of the bounce) until it lets
// go, then straight down to the floor, falling faster, then up off it into
// the other hand, slowing as it gets there.
export const moveBallAt = (
	ph: number,
	letGo: number,
	floor: V3,
	under: (at: number, hand: Hand) => V3,
	from: Hand,
	to: Hand,
): V3 => {
	const mix = (a: V3, b: V3, u: number): V3 => ({
		f: a.f + (b.f - a.f) * u,
		s: a.s + (b.s - a.s) * u,
		u: a.u + (b.u - a.u) * u,
	});
	if (ph < letGo) {
		return under(ph, from);
	}
	if (ph < DOWN) {
		return mix(
			under(letGo, from),
			floor,
			((ph - letGo) / (DOWN - letGo)) ** 1.35,
		);
	}
	return mix(under(ph, to), floor, (1 - (ph - DOWN) / (1 - DOWN)) ** 1.8);
};

// The ball under a dribbling hand: just below the middle of his palm.
const underHand = (st0: PlayerState, body: Body, hand: Hand): Pt3 => {
	const st = withBody(st0, body);
	const sk = skeleton(body, poseOf(st));
	return bodyPoint(st, underPalm(hand === "R" ? sk.armR : sk.armL));
};

export const evalBall = (
	tl: CourtTimeline,
	t: number,
	bodyFor: (pid: number) => Body,
): BallState =>
	tl.ball.length === 0
		? { x: 47, y: 25, z: 0 }
		: ballOn(tl, ballIndexAt(tl, t), t, bodyFor, 0);

// Where the ball is at t, by the i-th piece of its path.
const ballOn = (
	tl: CourtTimeline,
	i: number,
	t: number,
	bodyFor: (pid: number) => Body,
	depth: number,
): BallState => {
	const seg = tl.ball[i]!;
	const prev = i > 0 && depth < 2 ? tl.ball[i - 1] : undefined;
	// Where the piece before left it, the moment before this one took over.
	const left = () => ballOn(tl, i - 1, seg.t0 - BEFORE, bodyFor, depth + 1);
	const handOf = (pid: number, at: number, which: "near" | "far" | "both") =>
		which === "both"
			? heldBall(evalPlayer(tl, pid, at), bodyFor(pid))
			: handWorld(evalPlayer(tl, pid, at), bodyFor(pid), which);
	const resolve = (
		p: Pt3 | { pid: number; hand?: "near" | "far" | "both" },
		at: number,
	): Pt3 => ("pid" in p ? handOf(p.pid, at, p.hand ?? "both") : p);

	if (seg.kind === "hold") {
		const st = evalPlayer(tl, seg.pid, t);
		const body = bodyFor(seg.pid);
		if (seg.style === "cross") {
			// A move, low and quick: ridden down in the hand it leaves, let go,
			// off the floor where the move puts it - across in front of him,
			// between his feet, behind him by his far foot - and up into the
			// other hand coming to meet it.
			const { ph, from, to, move, still } = crossMoveAt(tl, seg, t);
			const me = withBody(st, body);
			const under = (at: number, hand: Hand): V3 => {
				const sk = skeleton(body, poseOf({ ...me, phase: at }));
				const p = underPalm(hand === "R" ? sk.armR : sk.armL);
				return { ...p, u: p.u + me.z };
			};
			return {
				...bodyPoint(
					{ ...me, z: 0 },
					moveBallAt(
						ph,
						MOVE_BALL[move].letGo,
						{ ...moveFloor(body, move, to, still), u: BALL_R },
						under,
						from,
						to,
					),
				),
				holder: seg.pid,
			};
		}
		if (seg.style === "dribble") {
			const { ph, from, to, first, start, origin } = bounceOf(tl, i, t);
			const down = ph < DOWN;
			const hand = down ? from : to;
			// Out of both hands the first time, from where he had it - carried
			// along with him.
			const h =
				first && down
					? (() => {
							const was =
								start > 0 && depth < 2
									? ballOn(tl, start - 1, origin - BEFORE, bodyFor, depth + 1)
									: heldBall(
											{
												...st,
												anim: "hold",
												phase: 0,
												dribble: undefined,
												dribbleHand: undefined,
											},
											body,
										);
							const then = evalPlayer(tl, seg.pid, origin - BEFORE);
							return {
								x: was.x + st.x - then.x,
								y: was.y + st.y - then.y,
								z: was.z,
							};
						})()
					: underHand(st, body, hand);
			const tri = dribbleDepth(ph);
			// It hits the floor beside him, out past the foot on that side -
			// out ahead of him on the move - or, changing hands, between his
			// feet.
			const across = from !== to;
			const ahead = dribbleAhead(st.anim);
			const out = 1.22 - 0.12 * ahead;
			const floor = bodyPoint(
				{ ...st, z: 0 },
				{
					f: across ? 1.0 : 0.95 + ahead,
					s: across ? 0 : hand === "L" ? out : -out,
					u: BALL_R,
				},
			);
			return {
				x: h.x + (floor.x - h.x) * tri,
				y: h.y + (floor.y - h.y) * tri,
				z: h.z + (floor.z - h.z) * tri,
				holder: seg.pid,
			};
		}
		const held: BallState = { ...heldBall(st, body), holder: seg.pid };
		// Taken into both hands - up off his dribble, off the floor, out of
		// another move - it comes from where it was. (Not when the picture
		// cut to him with it: that is no hand-over.)
		const u = (t - seg.t0) / HANDOVER_MS;
		if (
			prev &&
			u < 1 &&
			((prev.kind === "hold" && prev.pid === seg.pid) ||
				prev.kind === "rest" ||
				prev.kind === "bounce" ||
				prev.kind === "path")
		) {
			const was = left();
			const then = evalPlayer(tl, seg.pid, seg.t0 - BEFORE);
			const at = heldBall(then, body);
			if (dist2(was, at) + (was.z - at.z) ** 2 < HANDOVER_REACH ** 2) {
				// Carried along with him if it was already his.
				const moved =
					prev.kind === "hold"
						? { x: st.x - then.x, y: st.y - then.y }
						: { x: 0, y: 0 };
				const e = ease(u);
				return {
					x: was.x + moved.x + (held.x - was.x - moved.x) * e,
					y: was.y + moved.y + (held.y - was.y - moved.y) * e,
					z: was.z + (held.z - was.z) * e,
					holder: seg.pid,
				};
			}
		}
		return held;
	}
	if (seg.kind === "fly") {
		// In flight it is a thrown ball: steady across the floor, and up and
		// down under gravity - so a three climbs to fifteen feet in the second
		// it takes, a chest pass barely rises, and a lob hangs for the dunker.
		// Out of a man's hands, it goes from wherever his last move had it.
		// Caught, it stays in his hands until the ball's next move.
		if (t >= seg.t1 && "pid" in seg.to) {
			return { ...resolve(seg.to, t), holder: seg.to.pid };
		}
		const a =
			prev?.kind === "hold" && "pid" in seg.from && prev.pid === seg.from.pid
				? left()
				: resolve(seg.from, seg.t0);
		const b = resolve(seg.to, seg.t1);
		const u = clamp01((t - seg.t0) / (seg.t1 - seg.t0));
		const T = Math.max(0, seg.t1 - seg.t0) / 1000;
		const tau = u * T;
		const vz = T > 0 ? (b.z - a.z) / T + 0.5 * GRAVITY * T : 0;
		// Into a play at the rim (see physics.ts), it comes in going just as
		// the play has it, however high his hands let it go from: bent by
		// as much as that takes - and not at all at either end.
		const next = tl.ball[i + 1];
		const bend =
			next?.kind === "path" && next.t0 === seg.t1 && T > 0
				? (u * u * u - u * u) * T
				: 0;
		const w = bend === 0 || next?.kind !== "path" ? undefined : next.v0;
		return {
			x: a.x + (b.x - a.x) * u + (w ? (w.x - (b.x - a.x) / T) * bend : 0),
			y: a.y + (b.y - a.y) * u + (w ? (w.y - (b.y - a.y) / T) * bend : 0),
			z:
				a.z +
				vz * tau -
				0.5 * GRAVITY * tau * tau +
				(w ? (w.z - (vz - GRAVITY * T)) * bend : 0),
			roll: -Math.PI * 2 * BACKSPIN * tau * (Math.sign(b.x - a.x) || 1),
		};
	}
	if (seg.kind === "bounce") {
		// A drop from where it was, then shrinking hops, then a roll. Heights
		// are of the ball's middle, which sits a radius off the floor. Lost
		// out of a man's hands, it drops from where he had it.
		const from = prev?.kind === "hold" ? left() : seg.from;
		const u = clamp01((t - seg.t0) / (seg.t1 - seg.t0));
		const drop = Math.max(0, from.z - BALL_R);
		// Each fall and hop takes the time gravity gives it - squeezed only
		// if the bounce has less (then it rolls the rest of the way).
		// (Off the floor already, it goes straight up into its first hop.)
		const fall = (h: number) => Math.sqrt((2 * Math.max(0.01, h)) / GRAVITY);
		const hops = drop > 0.005 ? [fall(drop)] : [];
		const first = hops.length;
		for (let i = 0; i < seg.hops; i++) {
			hops.push(2 * fall(seg.h0 * 0.42 ** i));
		}
		const real = hops.reduce((s, h) => s + h, 0);
		const span = Math.max(0.001, ((seg.t1 - seg.t0) / 1000) * 0.85);
		let w = (u * (seg.t1 - seg.t0) * Math.max(1, real / span)) / 1000;
		let z = 0;
		for (let i = 0; i < hops.length; i++) {
			if (w <= hops[i]!) {
				const v = w / hops[i]!;
				z =
					i < first
						? drop * (1 - v * v)
						: 4 * seg.h0 * 0.42 ** (i - first) * v * (1 - v);
				break;
			}
			w -= hops[i]!;
		}
		const roll = 1 - (1 - Math.min(1, u / 0.85)) ** 1.5;
		const along = Math.hypot(seg.to.x - from.x, seg.to.y - from.y) * roll;
		return {
			x: from.x + (seg.to.x - from.x) * roll,
			y: from.y + (seg.to.y - from.y) * roll,
			z: BALL_R + z,
			roll: (along / BALL_R) * (Math.sign(seg.to.x - from.x) || 1),
		};
	}
	if (seg.kind === "path") {
		// Off the iron and the glass and down through the net, the way it
		// was worked out (see physics.ts), still turning the way it left his
		// fingers.
		const ms = Math.min(t, seg.t1) - seg.t0;
		return {
			...playAt(seg.pts, ms),
			roll: seg.roll0 + (seg.spin * ms) / 1000,
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

// Through a fast stretch the picture runs this many times over: getting up
// to speed gently, so it never lurches away from what just happened, and
// back down crisply into what comes next (timeline ms). Up to speed in
// step with the timeline is a steady build in the time the viewer sees -
// the same few percent quicker every moment - about half a second of it at
// the usual speed.
export const FAST = 18;
const FAST_IN = 2800;
const FAST_OUT = 800;
export const fastAt = (tl: CourtTimeline, t: number): number => {
	const i = lastIndex(tl.fast, t, (f) => f[0]);
	const f = i >= 0 ? tl.fast[i] : undefined;
	if (!f || t >= f[1]) {
		return 1;
	}
	const len = f[1] - f[0];
	const up = Math.min(1, (t - f[0]) / Math.min(FAST_IN, len * 0.6));
	const v = Math.min(1, (f[1] - t) / Math.min(FAST_OUT, len * 0.4));
	return 1 + (FAST - 1) * Math.min(up, v * v * (3 - 2 * v));
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
