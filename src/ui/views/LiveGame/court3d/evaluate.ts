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
import { COURT_H, rimX, type Pt, type Pt3, type Side } from "./geometry.ts";
import {
	alongShape,
	keepThrough,
	pushAlong,
	runShape,
	type RunShape,
} from "./motion.ts";
import { playAt } from "./physics.ts";
import {
	ANIMS,
	armReaching,
	armTo,
	bodyOf,
	bounceAt,
	flexOf,
	footDown,
	gripOf,
	dribbleAhead,
	dribbleIn,
	IN_AHEAD,
	IN_BY,
	isGait,
	legPhases,
	carried,
	holdBall,
	isMove,
	lerpPose,
	mirror,
	MOVE_BALL,
	moveAnim,
	moveFloor,
	poseAt,
	posed,
	shortLift,
	shortSteps,
	shotForm,
	skeleton,
	SLIDE_NARROW,
	standingReach,
	stepBeat,
	stepLength,
	underPalm,
	type AnimName,
	type Body,
	type DribbleMove,
	type Grip,
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
	// Getting going or pulling up, how long each leg's stride is, as a share
	// of a full one - right, left (see shortSteps).
	stepLen?: [number, number];
	// How hard he is getting going (up to 1, all he has) or pulling up (down
	// to -1): he drives into it, or sits back against it (see driven).
	drive?: number;
	// Pulled up mid-stride: his foot up off the floor come down this far (0
	// to 1 - see footDown).
	down?: number;
	// The ball in his hands - not bouncing on a dribble - so it is drawn with
	// him, his hands on it.
	holding?: boolean;
	// Just after it came into his hands - caught, or picked up off his
	// dribble - how far his hands have closed on it (0 to 1): they meet it,
	// not snap to it. And just after he let it go - a shot, a pass - how
	// much they are still where it was: they come off it, not snap away.
	grip?: number;
	// How far through a bounce of his dribble (0 the ball in his hand at the
	// top), when he is dribbling: his hand rides it - this one.
	dribble?: number;
	dribbleHand?: Hand;
	// A bounce that takes it across to his other hand: the hand it leaves.
	dribbleFrom?: Hand;
	// How tight on him the nearest of theirs in front of him is (0 to 1):
	// up on him, his dribble is kept in by his hip, not out in front.
	tight?: number;
	// Doing something with his arms on the run - a catch, a pass, a hand at
	// the ball: the stride his legs keep on with under it (see STRIDE_ACTS).
	stride?: Stride;
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
	// Shuffling, the way he goes - ahead, and across to his left (a length
	// up to 1, as much of it as counts: none all but stood still): partly
	// ahead or behind as well as across, each foot steps out along it (see
	// slanted).
	way?: [number, number];
	// Turning where he stands: each leg's own turn (degrees - see Pose) and
	// how far each foot is up off the floor stepping round (0 to 1) - right,
	// left; turn, turn, up, up.
	pivot?: [number, number, number, number];
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
// His legs on a stride - its gait and how far through it - for so much of
// them (0 to 1) under whatever his arms are doing.
export type Stride = {
	anim: AnimName;
	phase: number;
	w: number;
	// (Short steps, getting going or pulling up - see stepLen.)
	len?: [number, number];
};
// A move he is easing out of (see blendInto).
export type Blend = {
	anim: AnimName;
	phase: number;
	stepLen?: [number, number];
	// The ball in his hands then: carried as that move carries it.
	held?: true;
	dribble?: number;
	dribbleHand?: Hand;
	dribbleFrom?: Hand;
	tight?: number;
	stride?: Stride;
	target?: number;
	mirror?: boolean;
	w: number;
	arms?: number;
	// Turning where he stood as it changed (see pivot); each leg's turn under
	// him as it showed then (see legs), right, left; and the way he faced.
	pivot?: [number, number, number, number];
	turns?: [number, number];
	facing?: number;
	down?: number;
	// Shuffling then, the way he went (see way).
	way?: [number, number];
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

// The ball in play at the i'th of its segments: in a man's hands, or on
// its way from one to another - straight there, or off the floor between
// them (a bounce pass is two flights, down to the floor and back up).
export const liveSeg = (list: readonly BallSeg[], i: number): boolean => {
	const s = list[i];
	if (s?.kind === "hold") {
		return true;
	}
	if (s?.kind !== "fly") {
		return false;
	}
	// (Down at the floor: where a bounce pass hits it.)
	const floor = (e: object): boolean => "z" in e && (e as Pt3).z < 1.5;
	const p = list[i - 1];
	const n = list[i + 1];
	const fromOk =
		"pid" in s.from ||
		(floor(s.from) &&
			p?.kind === "fly" &&
			"pid" in p.from &&
			Math.abs(p.t1 - s.t0) < 1);
	const toOk =
		"pid" in s.to ||
		(floor(s.to) &&
			n?.kind === "fly" &&
			"pid" in n.to &&
			Math.abs(n.t0 - s.t1) < 1);
	return fromOk && toOk;
};

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
export const FLOW_MS = 300;
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
	// (Not one his path has him pull up at the end of, or set off on from a
	// standstill.)
	if (!a || !b || a.v1 === 0 || b.v0 === 0) {
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
	// (Not into a pause the run either side has the measure of itself - it
	// keeps to its own time there, and the two would not meet.)
	const vIn =
		mv.v0 !== undefined || prev?.v1 !== undefined
			? undefined
			: plantedBetween(tr, prev, mv)
				? undefined
				: joinOf(prev, mv);
	const vOut =
		mv.v1 !== undefined || next?.v0 !== undefined
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
	// (Only where the one hands on to the other, going on through it - both
	// of them: rounded on one side of the join and not the other, he would
	// jump at it.)
	if (Math.abs(b.s0 - tj) > 1 || a.v1 <= 0 || b.v0 <= 0) {
		return undefined;
	}
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
// How fast he really covers the ground through the middle of a run - the
// run itself, and whatever eases him aside on top of it while he is on it
// (see keepOff, keepApart in director.ts): his legs go at that, not slower.
// (The middle third of it: a step aside eased back as he sets off is not
// the pace he goes at.)
const groundSpeeds = new WeakMap<Run["mv"], number>();
const groundSpeed = (tr: Track, run: Run): number => {
	let v = groundSpeeds.get(run.mv);
	if (v === undefined) {
		v = 0;
		const { s0, s1 } = run;
		const aside =
			(tr.sep ?? []).some(
				(x) => x.t0 < s1 && x.t0 + x.dx.length * SEP_MS > s0,
			) || (tr.nudges ?? []).some((x) => x.t0 < s1 && x.t1 > s0);
		if (aside) {
			for (let t = s0 + (s1 - s0) * 0.35; t <= s1 - (s1 - s0) * 0.35; t += 40) {
				const p = spotAt(tr, t - 10, false);
				const q = spotAt(tr, t + 10, false);
				v = Math.max(v, Math.hypot(q.x - p.x, q.y - p.y) / 0.02);
			}
		}
		groundSpeeds.set(run.mv, v);
	}
	return v;
};
const runAnim = (tr: Track, run: Run): AnimName => {
	const anim = run.mv.anim;
	// His legs go at the pace he holds most of the way: come in off a sprint
	// and settled into a run for the length of the floor, he runs it. (Only a
	// run that is mostly getting going or pulling up goes at the quicker end.)
	const shape = shapeOf(run);
	const v =
		shape.T - shape.ta - shape.td >= shape.T / 2
			? shape.vc
			: Math.max(shape.vc, run.v0, run.v1);
	switch (anim) {
		case "run":
		case "jog":
			return gaitFor(Math.max(v, groundSpeed(tr, run)));
		// Sent hard: no slower than a run - but a sprint only at a sprint's
		// pace, not a sprinter's stride over a few quick steps.
		case "sprint":
			return gaitFor(Math.max(v, groundSpeed(tr, run), JOG_FTPS));
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
	// How fast he is going round a corner, if he is (feet a second).
	round?: number;
};

// Where he is, eased aside off anybody he would be standing on (see
// keepApart in director.ts) - and kept off anybody he would still be in (see
// keepOff).
// (Without which way he is heading round a corner, where only where he is
// is wanted.)
const spotAt = (tr: Track, t: number, heading = true): Spot => {
	const s = nudgedAt(tr, t, heading);
	return tr.sep ? keptOff(tr.sep, s, t) : s;
};

// Kept off a man: the way over, sampled every SEP_MS, eased through the
// samples (Catmull-Rom).
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
	const { t0, dx, dy } = list[k]!;
	const f = (t - t0) / SEP_MS;
	if (f >= dx.length - 1) {
		return s;
	}
	const i = Math.floor(f);
	const u = f - i;
	catmullRom(dx, i, u);
	const x = crAt;
	catmullRom(dy, i, u);
	return { ...s, x: s.x + x, y: s.y + crAt };
};

const NUDGE_RAMP = 450;
const nudgedAt = (tr: Track, t: number, heading = true): Spot => {
	const s = rawSpotAt(tr, t, heading);
	const list = tr.nudges;
	if (!list || list.length === 0) {
		return s;
	}
	let dx = 0;
	let dy = 0;
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
		const u = Math.min(1, (t - n.t0) / r, (n.t1 - t) / r);
		const w = u * u * (3 - 2 * u);
		dx += n.dx * w;
		dy += n.dy * w;
	}
	return dx === 0 && dy === 0 ? s : { ...s, x: s.x + dx, y: s.y + dy };
};
const NUDGE_LONGEST = 15000;

// Eased aside off a man - nudged (see nudgedAt) or kept off him (see
// keptOff): how fast that has him going at t (feet a second), and how far
// it has taken him all told, over and back. (Written here, not handed back:
// it is asked for every man, every frame.)
let asideV = 0;
let asideD = 0;
const asideAt = (tr: Track, t: number): void => {
	let vx = 0;
	let vy = 0;
	let d = 0;
	const list = tr.nudges ?? [];
	for (let i = lastIndex(list, t, (n) => n.t0); i >= 0; i--) {
		const n = list[i]!;
		if (t - n.t0 > NUDGE_LONGEST) {
			break;
		}
		const len = Math.hypot(n.dx, n.dy);
		if (t >= n.t1) {
			d += 2 * len;
			continue;
		}
		const r = Math.min(n.ramp ?? NUDGE_RAMP, (n.t1 - n.t0) / 2);
		const a = (t - n.t0) / r;
		const b = (n.t1 - t) / r;
		const u = Math.min(1, a, b);
		const w = u * u * (3 - 2 * u);
		if (u < 1) {
			const dw = ((6 * u * (1 - u) * 1000) / r) * (a < b ? 1 : -1);
			vx += n.dx * dw;
			vy += n.dy * dw;
		}
		d += a < b ? len * w : len * (2 - w);
	}
	asideV = Math.hypot(vx, vy);
	const sep = tr.sep;
	const k = sep ? lastIndex(sep, t, (x) => x.t0) : -1;
	if (sep && k >= 0) {
		const base = sepBase(sep);
		const { t0, dx, dy, dl } = sep[k]!;
		const f = (t - t0) / SEP_MS;
		if (f >= dx.length - 1) {
			d += base[k + 1]!;
		} else {
			const i = Math.floor(f);
			const u = f - i;
			catmullRom(dx, i, u);
			const sx = crV;
			catmullRom(dy, i, u);
			asideV += Math.hypot(sx, crV);
			d += base[k]! + dl[i]! + (dl[i + 1]! - dl[i]!) * u;
		}
	}
	asideD = d;
};
// How far he had been kept off men before each time it starts.
const sepBases = new WeakMap<Sep[], number[]>();
const sepBase = (list: Sep[]): number[] => {
	let base = sepBases.get(list);
	if (base?.length !== list.length + 1) {
		base = [0];
		for (const x of list) {
			base.push(base.at(-1)! + (x.dl.at(-1) ?? 0));
		}
		sepBases.set(list, base);
	}
	return base;
};
const rawSpotAt = (tr: Track, t: number, heading = true): Spot => {
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
	// (Only between two runs he has: the first or last of his - one he was
	// set off on already going, a reel's cut to him, say - has nobody on
	// the other side of it to round into.)
	if (run.v0 > 0 && k > 0 && t - run.s0 < ROUND_MS) {
		pair = [runOf(tr, k - 1), run];
		q = rounded(pair[0], pair[1], t);
	}
	if (!q && run.v1 > 0 && k + 1 < tr.moves.length && run.s1 - t < ROUND_MS) {
		pair = [run, runOf(tr, k + 1)];
		q = rounded(pair[0], pair[1], t);
	}
	if (pair && q) {
		p = q;
		if (!heading) {
			return { ...p, moveIndex: k, moving: true, run, hx, hy };
		}
		const ahead = rounded(pair[0], pair[1], t + 8) ?? q;
		const behind = rounded(pair[0], pair[1], t - 8) ?? q;
		if (dist2(ahead, behind) > 1e-6) {
			hx = ahead.x - behind.x;
			hy = ahead.y - behind.y;
		}
		const round = Math.sqrt(dist2(ahead, behind)) / 0.016;
		return { ...p, moveIndex: k, moving: true, run, hx, hy, round };
	}
	return { ...p, moveIndex: k, moving: true, run, hx, hy };
};

// How many strides into his run he is at t, counting the runs it follows
// on from - so his legs keep their rhythm through a join. (Each run's at
// the stride of the steps he takes on it - a slide he pushes across in is
// a shuffle's - so where one hands on to the next they come out the same.)
// (A run straight on from the last - planted, but with hardly a moment
// between - carries on his strides from where they were.)
const STRIDE_ON_MS = 220;
// (And how long each leg's stride is then - see shortSteps.)
const stridesAt = (
	tl: CourtTimeline,
	tr: Track,
	k: number,
	run: Run,
	t: number,
): { ph: number; len: [number, number] } => {
	const st = steppingOf(tl, tr, k, run);
	const n = st.ph.length - 1;
	const f =
		st.s1 > st.s0
			? ((Math.min(st.s1, Math.max(st.s0, t)) - st.s0) * n) / (st.s1 - st.s0)
			: n;
	const i = Math.max(0, Math.min(n - 1, Math.floor(f)));
	const u = n > 0 ? f - i : 0;
	const at = (a: number[]) => (n > 0 ? a[i]! + (a[i + 1]! - a[i]!) * u : a[0]!);
	return { ph: at(st.ph), len: [at(st.r), at(st.l)] };
};

// How fast a run has him going at t (feet a second).
const paceOn = (run: Run, t: number): number => {
	const u = Math.min(run.s1, Math.max(run.s0, t));
	const a = Math.max(run.s0, u - 4);
	const b = Math.min(run.s1, u + 4);
	return b > a ? ((alongRun(run, b) - alongRun(run, a)) * 1000) / (b - a) : 0;
};
// HIS STRIDES THROUGH A RUN, worked out step by step along it (every
// STEP_MS), once a run: how far round them he is, and how long each leg's
// stride is (see shortSteps). A foot down keeps the stride it came down on,
// so it stays where it is on the floor as he picks up pace or slows; his
// strides go round as fast as that foot goes back under him; and a foot up
// goes over to a stride for the pace he will be going as it comes down -
// looking ahead to it, so pulling up his last steps come down short, not
// long and then stuck under him while he slows; and getting going, each
// step a little longer than the last. (Both feet off the floor, his strides
// go round as they did - or, slowing, at the beat.) Carried on from the run
// before, where he goes straight on from it.
const STEP_MS = 10;
// The shortest stride he takes (of a full one), and the quickest his
// strides go round (a second).
const LEN_MIN = 0.1;
const BEAT_MOST = 2.6;
// (Looking no further ahead than this, ms.)
const LAND_AHEAD_MS = 300;
// (Getting going, each step no more than this much longer than the other
// foot's - his strides lengthen a step at a time, quicker meanwhile.)
const STEP_GROW = 2.5;
// (A foot's stride changes no quicker than this - so many full ones a
// second.)
const LEN_RATE = 10;
type Stepping = {
	s0: number;
	s1: number;
	ph: number[];
	r: number[];
	l: number[];
	// Each leg at the end: its stride, and whether its foot is up.
	legR: StepLeg;
	legL: StepLeg;
};
// (Up, the stride it goes over from toward the next - the one it left the
// floor on - and from how far through its swing: from the start of it, or
// from where it was as the run before ended.)
type StepLeg = { len: number; up?: number; from?: number };
const steppings = new WeakMap<Track, Map<Move, Stepping>>();
const steppingOf = (
	tl: CourtTimeline,
	tr: Track,
	k: number,
	run: Run,
): Stepping => {
	let byMove = steppings.get(tr);
	if (!byMove) {
		byMove = new Map();
		steppings.set(tr, byMove);
	}
	const had = byMove.get(run.mv);
	if (had && had.s0 === run.s0 && had.s1 === run.s1) {
		return had;
	}
	const anim = stepsOf(tl, tr, run);
	const S = strideOf(anim);
	const L0 = lenOf(run.mv);
	// How far along he is at t - on past the end at the pace he ends on.
	const along = (t: number) =>
		t <= run.s1 ? alongRun(run, t) : L0 + (run.v1 * (t - run.s1)) / 1000;
	// The stride for a foot coming down at t: for his pace over the time it
	// will be down (at the beat) - so pulling up he steps short and his
	// strides keep the beat as he slows. (Setting off, the one he pushes off
	// on, for his pace while it is still down.)
	const beat = stepBeat(anim);
	const duty = legPhases(anim, 0)?.duty ?? 0.5;
	const DOWN_MS = beat > 0 ? (duty * 1000) / beat : 0;
	const over = (t: number, ms = DOWN_MS) =>
		ms > 0 ? ((along(t + ms) - along(t)) * 1000) / ms : paceOn(run, t);
	const lenAt = (t: number) => Math.max(LEN_MIN, stepLength(anim, over(t)));
	let ph = STRIDE_START;
	// (Setting off, the foot he pushes off on is only down a moment more.)
	const first = legPhases(anim, ph);
	const firstMs =
		first && beat > 0
			? (Math.max(
					0,
					...[first.r, first.l]
						.filter((phi) => phi < first.duty)
						.map((phi) => (first.back ? phi : first.duty - phi)),
				) *
					1000) /
				beat
			: 0;
	// (Steps that keep no beat - a shuffle, say - full ones throughout. A
	// foot up as he sets off goes over from there, from where it is through
	// its swing.)
	let R: StepLeg = {
		len: first ? Math.max(LEN_MIN, stepLength(anim, over(run.s0, firstMs))) : 1,
		from: -1,
	};
	let L: StepLeg = { ...R };
	if (
		k > 0 &&
		(run.v0 > 0 || tr.moves[k]!.t0 - tr.moves[k - 1]!.t1 <= STRIDE_ON_MS)
	) {
		// (Counted at the stride he was showing as it ended - backpedaling by
		// then, say - so he goes on from where his feet were: each foot's
		// stride as long in feet as it was.)
		const before = runOf(tr, k - 1);
		const was = steppingOf(tl, tr, k - 1, before);
		const m = was.ph.length - 1;
		const ended = endStepsOf(tl, tr, k - 1, before);
		const S0 = strideOf(ended);
		const took = stepsOf(tl, tr, before);
		ph = was.ph[m]! + lenOf(before.mv) * (1 / S0 - 1 / strideOf(took));
		if (first && legPhases(took, 0)) {
			const as = (len: number) =>
				Math.min(1, Math.max(LEN_MIN, (len * strideOf(took)) / S));
			// (A foot up goes on over from the stride it has now, from where
			// it is through its swing on these steps - which may count it
			// differently, turned round from running into a backpedal, say.)
			const carry = (leg: StepLeg): StepLeg => ({
				len: as(leg.len),
				...(leg.up === undefined ? {} : { up: as(leg.len), from: -1 }),
			});
			R = carry(was.legR);
			L = carry(was.legL);
		}
	}
	const n = Math.max(1, Math.ceil((run.s1 - run.s0) / STEP_MS));
	const dt = (run.s1 - run.s0) / n;
	const out: Stepping = {
		s0: run.s0,
		s1: run.s1,
		ph: [ph],
		r: [R.len],
		l: [L.len],
		legR: R,
		legL: L,
	};
	for (let i = 1; i <= n; i++) {
		const u = run.s0 + (i - 0.5) * dt;
		const v = paceOn(run, u);
		const was = legPhases(anim, ph);
		if (!was) {
			ph += ((v / S) * dt) / 1000;
		} else {
			// (Both feet up, at the stride of the one that left the floor last
			// - or, slowing, his pace's.)
			const down =
				R.up === undefined && L.up === undefined
					? (R.len + L.len) / 2
					: R.up === undefined
						? R.len
						: L.up === undefined
							? L.len
							: Math.min(
									(was.back ? was.r > was.l : was.r < was.l) ? R.up : L.up,
									lenAt(u),
								);
			const rate = Math.min(BEAT_MOST, v / (S * down));
			ph += (rate * dt) / 1000;
			const now = legPhases(anim, ph)!;
			// Each foot: up off the floor once it is through its time down;
			// then over from the stride it left the floor on to the one for
			// when it comes down - so far ahead as it has yet to swing, at the
			// rate it is swinging. (Never more than so much longer than the
			// other foot's stride, as it is now - not as it was as that foot
			// left the floor, or the cap would jump as it came down.)
			const step = (leg: StepLeg, phi: number, other: StepLeg): StepLeg => {
				if (phi < now.duty) {
					return leg.up === undefined ? leg : { len: leg.len };
				}
				const through = Math.min(
					1,
					(now.back ? 1 - phi : phi - now.duty) / Math.max(1e-6, 1 - now.duty),
				);
				const up = leg.up ?? leg.len;
				const from =
					leg.from === -1
						? through
						: leg.up === undefined
							? 0
							: (leg.from ?? 0);
				const ahead = Math.min(
					LAND_AHEAD_MS,
					((1 - through) * (1 - now.duty) * 1000) / Math.max(rate, 0.5),
				);
				const to = Math.min(lenAt(u + ahead), other.len * STEP_GROW);
				const s0 = smooth01(from);
				const share =
					s0 >= 1 ? 1 : clamp01((smooth01(through) - s0) / (1 - s0));
				// (And no quicker than LEN_RATE: pulling up late in a swing,
				// the step it comes down on shortened over the last of it, not
				// snapped short in a frame.)
				const most = (LEN_RATE * dt) / 1000;
				const len =
					leg.len +
					Math.max(-most, Math.min(most, up + (to - up) * share - leg.len));
				return { len, up, from };
			};
			const r = step(R, now.r, L);
			L = step(L, now.l, R);
			R = r;
		}
		out.ph.push(ph);
		out.r.push(R.len);
		out.l.push(L.len);
	}
	out.legR = R;
	out.legL = L;
	byMove.set(run.mv, out);
	return out;
};
// Setting off, he is a quarter of a stride in: over the foot he pushes off
// with, the other coming through under him - nearest how he stood, so his
// legs ease out of that into the stride with his feet staying where they
// are, not splayed apart across the floor in a frame or two.
const STRIDE_START = 0.25;

// Hands up for the ball: from a moment before it leaves the passer (or
// comes off the floor on its way) until it gets to him.
const TARGET_LEAD = 240;
const TARGET_RAMP = 180;
const CHAIN_GAP = 20;
const targetAt = (tl: CourtTimeline, pid: number, t: number): number => {
	const i = Math.max(
		0,
		lastIndex(tl.ball, t, (s) => s.t0),
	);
	let from: number | undefined;
	// (Far enough on to see a bounce pass off the dribble coming: the
	// dribble, the gather, and both legs of it.)
	for (let k = i; k < tl.ball.length && k <= i + 4; k++) {
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
			// From when it set off his way - and if that was knocked out of
			// the air on its way to somebody else, he never saw it coming.
			// (Not from before it stopped in somebody's hands on the way - an
			// official's, say, handing it on.)
			let j = k;
			for (; j > 0; j--) {
				const x = tl.ball[j - 1]!;
				if (
					x.kind !== "fly" ||
					"pid" in x.to ||
					x.t1 < tl.ball[j]!.t0 - CHAIN_GAP
				) {
					break;
				}
			}
			const was = tl.ball[j - 1];
			const lead =
				was?.kind === "fly" && was.t1 >= tl.ball[j]!.t0 - CHAIN_GAP
					? 0
					: TARGET_LEAD;
			const u = clamp01((t - tl.ball[j]!.t0 + lead) / TARGET_RAMP);
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

const PASSES = new Set<AnimName>([
	"pass",
	"passBounce",
	"passOverhead",
	"passPush",
	"passBouncePush",
	"passBehind",
]);
export const PASS_TURN = 250;
export const PASS_PICK_UP = new Set<AnimName>([
	"dribble",
	"dribbleWalk",
	"back",
]);
// On the move with a pass coming to him, he opens up to it - from as soon
// as he can see it coming (ms) - round toward the ball as far as running on
// lets him (radians off the way he goes).
const CATCH_OPEN_MS = 400;
const CATCH_OPEN_MAX = (95 * Math.PI) / 180;
const passComing = (tl: CourtTimeline, pid: number, t: number): boolean => {
	const i = ballIndexAt(tl, t);
	for (let k = i; k <= i + 2 && k < tl.ball.length; k++) {
		const s = tl.ball[k]!;
		// (Not yet thrown, and not about to be.)
		if (k > i && tl.ball[k - 1]!.kind !== "fly" && s.t0 - t > CATCH_OPEN_MS) {
			return false;
		}
		if (s.kind !== "fly") {
			if (k > i) {
				return false;
			}
			continue;
		}
		if ("pid" in s.to) {
			return s.to.pid === pid;
		}
	}
	return false;
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
		const to = angleTo(here, act.look, fallback);
		// Caught on the run, he opens up to it only as far as running on lets
		// him - not turned right round on it, carried on backward at a run.
		if (
			act.anim === "catch" &&
			here.moving &&
			here.hx * here.hx + here.hy * here.hy > 1e-6
		) {
			const was = spotAt(tr, t - TURN_STEP, false);
			if (
				Math.hypot(here.x - was.x, here.y - was.y) / (TURN_STEP / 1000) >=
				RUSH_FTPS
			) {
				const heading = Math.atan2(here.hy, here.hx);
				const d = wrapAngle(to - heading);
				return heading + Math.max(-CATCH_OPEN_MAX, Math.min(CATCH_OPEN_MAX, d));
			}
		}
		return to;
	}
	const mv = here.moveIndex >= 0 ? tr.moves[here.moveIndex] : undefined;
	// About to pass it - standing, or picking up his dribble for it - he
	// squares up to where it goes first, not round on it as it leaves his
	// hands. (Or to reach for it: round to it before his hand goes out.)
	if (!act) {
		const nx = tr.acts[lastIndex(tr.acts, t + PASS_TURN, (a) => a.t0)];
		if (
			nx &&
			nx.t0 > t &&
			nx.look &&
			(PASSES.has(nx.anim) || GRABS.has(nx.anim)) &&
			(!here.moving ||
				(mv !== undefined && PASS_PICK_UP.has(mv.anim) && mv.t1 <= nx.t0 + 40))
		) {
			return angleTo(here, nx.look, fallback);
		}
	}
	// How he is going: a slide too fast to be one is a run, say.
	const gait = here.run ? runAnim(tr, here.run) : mv?.anim;
	if (
		mv &&
		here.moving &&
		gait !== "back" &&
		!(gait && EYES_ON_BALL.has(gait)) &&
		passComing(tl, tr.pid, t)
	) {
		const heading =
			here.hx * here.hx + here.hy * here.hy > 1e-6
				? Math.atan2(here.hy, here.hx)
				: Math.atan2(mv.to.y - mv.from.y, mv.to.x - mv.from.x);
		const d = wrapAngle(angleTo(here, ballNear(tl, t), heading) - heading);
		return heading + Math.max(-CATCH_OPEN_MAX, Math.min(CATCH_OPEN_MAX, d));
	}
	// A defensive slide keeps his eyes on the ball whichever way he goes -
	// and so does a man drifting along the arc for it.
	if (mv && here.moving && gait && EYES_ON_BALL.has(gait)) {
		return angleTo(here, ballNear(tl, t), fallback);
	}
	// Backing his man down in the post: his back to the rim.
	if (mv && here.moving && gait === "post") {
		return angleTo(here, { x: rimX(tr.team), y: 25 }, fallback) + Math.PI;
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
const turnStep = (
	{ yaw, sense }: Turning,
	target: number,
	max = TURN_MAX,
): Turning => {
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
	const step = Math.max(-max, Math.min(max, d));
	return {
		yaw: yaw + step,
		sense: Math.abs(d) <= max ? 0 : Math.sign(step),
	};
};
// Going at a pace one way while still facing well round from it - turned
// back on himself - he plants and spins round onto it, quicker than a man
// turns where he stands: not run on with his legs going the wrong way.
const RUSH_TURN = 1.25;
const RUSH_FTPS = 5;
const rushed = (tr: Track, k: number, yaw: number, target: number): boolean => {
	if (Math.abs(wrapAngle(target - yaw)) < Math.PI / 2) {
		return false;
	}
	const b = spotAt(tr, k);
	if (!b.moving || b.hx * b.hx + b.hy * b.hy < 1e-6) {
		return false;
	}
	// (Only round onto the way he is going.)
	if (Math.abs(wrapAngle(target - Math.atan2(b.hy, b.hx))) > 0.5) {
		return false;
	}
	const a = spotAt(tr, k - TURN_STEP, false);
	return Math.hypot(b.x - a.x, b.y - a.y) / (TURN_STEP / 1000) >= RUSH_FTPS;
};
// Stood off the ball with nothing coming to him, a man on the offense
// follows it round at an easier pace - his eyes do the quick part - not
// whipped round on the spot every time it moves.
const CALM_TURN = 0.55;
const calm = (tl: CourtTimeline, tr: Track, k: number): boolean => {
	if (offenseAt(tl, k) !== tr.team || spotAt(tr, k, false).moving) {
		return false;
	}
	if (actAt(tr, k)) {
		return false;
	}
	const seg = ballSegAt(tl, k);
	if (seg?.kind === "hold" && seg.pid === tr.pid) {
		return false;
	}
	return !passComing(tl, tr.pid, k);
};
// Worked-out beats, kept per player: keyed by the beat and which settling
// moment it was worked out from.
const beatYaws = new WeakMap<Track, Map<number, Turning>>();
// (Forgotten when where they are is changed after the fact - see
// clearTheBall in director.ts.)
export const forgetTurns = (tracks: Iterable<Track>) => {
	for (const tr of tracks) {
		beatYaws.delete(tr);
		endSteps.delete(tr);
		steppings.delete(tr);
	}
};
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
		const target = yawTarget(tl, tr, k);
		turning = turnStep(
			turning,
			target,
			rushed(tr, k, turning.yaw, target)
				? TURN_MAX * RUSH_TURN
				: calm(tl, tr, k)
					? TURN_MAX * CALM_TURN
					: TURN_MAX,
		);
		kept.set(key(k), turning);
	}
	return turning.yaw;
};
// And a man gets going round and slows as he comes onto it - never from
// stood still to turning flat out in a frame, nor stopped dead in the next,
// like a turret. What shows is that turning run through a short bell, a
// beat apart either side (each beat taken from this one, so it never goes
// the long way round): a turn begins a touch before its reason and ends a
// touch after, halfway round at the same moment - so he is as far round
// by a pass or a shot as the director counted on (TURN_RATE).
const TURN_EASE = [1, 4, 6, 4, 1];
const TURN_EASE_SUM = 16;
const yawEased = (tl: CourtTimeline, tr: Track, g: number): number => {
	const c = yawOnBeat(tl, tr, g);
	const half = (TURN_EASE.length - 1) / 2;
	let off = 0;
	for (let i = 0; i < TURN_EASE.length; i++) {
		if (i !== half) {
			off +=
				TURN_EASE[i]! *
				wrapAngle(yawOnBeat(tl, tr, g + (i - half) * TURN_STEP) - c);
		}
	}
	return c + off / TURN_EASE_SUM;
};
const yawAt = (tl: CourtTimeline, tr: Track, t: number): number => {
	const g = Math.floor(t / TURN_STEP) * TURN_STEP;
	const a = yawEased(tl, tr, g);
	const u = (t - g) / TURN_STEP;
	return u <= 0 ? a : a + wrapAngle(yawEased(tl, tr, g + TURN_STEP) - a) * u;
};
// Round in a spin move: from the way he faces as he plants to the way he
// faces coming out of it - the long way, right round, the way his right
// hand takes him (turning to his right) or his left's (see spinBy in
// director.ts).
const SPIN_IN = 0.12;
const SPIN_OUT = 0.86;
const spinYawAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
): number | undefined => {
	const a = actAt(tr, t);
	if (a?.anim !== "spinMove") {
		return undefined;
	}
	const y0 = yawAt(tl, tr, a.t0);
	const turn = (a.mirror ? -TAU : TAU) + wrapAngle(yawAt(tl, tr, a.t1) - y0);
	const u = clamp01(
		((t - a.t0) / (a.t1 - a.t0) - SPIN_IN) / (SPIN_OUT - SPIN_IN),
	);
	return wrapAngle(y0 + turn * smooth01(u));
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
	if (!liveSeg(tl.ball, ballIndexAt(tl, t))) {
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

// THE BALL DEAD.
//
// After a whistle - a foul, a ball out of bounds, a sub, the trip to the
// line - nobody stays down in his stance, or crouched ready, for seconds on
// end: he stands up easy, puts his hands on his hips or on his knees a
// while, and sinks back into it a moment before the ball is back in play -
// in an inbounder's hands, or off the shooter's at the line. The ball's own
// path says when: out of anybody's hands and play (or in the shooter's,
// going through his routine), for long enough to be a stoppage - not just
// dropped through the net and straight back in.
const DEAD_MIN = 4000;
// He stands up once it has been dead a moment, and is back down in it this
// long before it is live.
const DEAD_LAG = 600;
const DEAD_LEAD = 1100;
// (At the line, the men out on the floor stay stood till the last one is
// on its way - then go.)
const LINE_LEAD = 250;
// (In play for less than this between two stoppages - a free throw with
// another to come, the ball tossed to the official - is no time to get
// down in it.)
const LIVE_GAP = 1500;
// A stoppage: from, to, and how long before its end he is back down in it.
type Stoppage = [number, number, number];
const deadSpans = new WeakMap<CourtTimeline, Stoppage[]>();
const AT_LINE = new Set(["ft", "missFt"]);
const HUDDLES = new Set(["timeout", "endOfPeriod"]);
// (Out of the huddle, he walks back out onto the floor: stood easy in it
// till then.)
const HUDDLE_LEAD = 300;
const BETWEEN = new Set(["sub", "timeout", "timeouts"]);
export const deadSpansOf = (tl: CourtTimeline): Stoppage[] => {
	const known = deadSpans.get(tl);
	if (known) {
		return known;
	}
	// At the line, the ball is dead all through a free throw with another
	// to come - and through the last till it leaves his hand.
	const lines: [number, number][] = [];
	const shots = new Set<number>();
	tl.beats.forEach((b, i) => {
		if (!AT_LINE.has(b.type)) {
			return;
		}
		let k = i + 1;
		while (k < tl.beats.length && BETWEEN.has(tl.beats[k]!.type)) {
			k++;
		}
		const next = tl.beats[k];
		if (next && AT_LINE.has(next.type)) {
			lines.push([b.preStart, b.end]);
			return;
		}
		// (Its shot: the last throw out of a man's hands before it gets
		// there.)
		const shot = tl.ball.findLast(
			(s) =>
				s.kind === "fly" &&
				"pid" in s.from &&
				s.t0 >= b.preStart &&
				s.t0 < b.actionStart,
		);
		const end = shot ? shot.t0 : b.actionStart;
		lines.push([b.preStart, end]);
		shots.add(end);
	});
	// Alternating stretches, dead or live, from the first: in play in a
	// man's hands, passed or shot out of them, or off the rim - not lying
	// where it was thrown, or bouncing loose.
	const runs: { t0: number; t1: number; live: boolean }[] = [];
	let li = 0;
	const add = (t0: number, t1: number, live: boolean) => {
		if (t1 <= t0) {
			return;
		}
		while (li < lines.length && lines[li]![1] <= t0) {
			li++;
		}
		const line = lines[li];
		if (live && line && line[0] < t1) {
			add(t0, line[0], true);
			add(Math.max(t0, line[0]), Math.min(t1, line[1]), false);
			add(line[1], t1, true);
			return;
		}
		const last = runs.at(-1);
		if (last && last.live === live) {
			last.t1 = t1;
		} else {
			runs.push({ t0, t1, live });
		}
	};
	tl.ball.forEach((s, i) => {
		const next = tl.ball[i + 1]?.t0 ?? tl.end;
		if (s.kind === "fly") {
			const end = Math.min(s.t1, next);
			add(s.t0, end, "pid" in s.from || liveSeg(tl.ball, i));
			add(end, next, "pid" in s.to);
		} else {
			add(s.t0, next, s.kind === "hold" || s.kind === "path");
		}
	});
	const out: Stoppage[] = [];
	// (A timeout, the break between periods: off in the huddles, the ball
	// with whoever took it there - dead, whoever has it.)
	for (const b of tl.beats) {
		if (HUDDLES.has(b.type) && b.end - b.actionStart >= DEAD_MIN) {
			out.push([b.actionStart, b.end, HUDDLE_LEAD]);
		}
	}
	for (let i = 0; i < runs.length; i++) {
		const r = runs[i]!;
		if (r.live) {
			continue;
		}
		let t1 = r.t1;
		// (Through a moment in play to the next stoppage.)
		while (
			i + 2 < runs.length &&
			runs[i + 1]!.t1 - runs[i + 1]!.t0 < LIVE_GAP
		) {
			i += 2;
			t1 = runs[i]!.t1;
		}
		if (t1 - r.t0 >= DEAD_MIN) {
			out.push([r.t0, t1, shots.has(t1) ? LINE_LEAD : DEAD_LEAD]);
		}
	}
	// (In time order, one where two run into each other.)
	out.sort((a, b) => a[0] - b[0]);
	const merged: Stoppage[] = [];
	for (const s of out) {
		const last = merged.at(-1);
		if (last && s[0] <= last[1]) {
			if (s[1] > last[1]) {
				last[1] = s[1];
				last[2] = s[2];
			}
		} else {
			merged.push([...s]);
		}
	}
	deadSpans.set(tl, merged);
	return merged;
};
// The stoppage he may stand easy in at t, if any.
const stoppageAt = (tl: CourtTimeline, t: number): Stoppage | undefined => {
	const list = deadSpansOf(tl);
	const s = list[lastIndex(list, t, (d) => d[0])];
	return s && t >= s[0] + DEAD_LAG && t < s[1] - s[2] ? s : undefined;
};
// How worn down he is (0 fresh, 1 run ragged): the trips up and down the
// floor - changes of possession - since he came on, or since the break
// between periods, from this many on to this many; a timeout gives him back
// this many of them.
const FRESH_TRIPS = 6;
const SPENT_TRIPS = 30;
const TIMEOUT_TRIPS = 6;
const breaksOf = new WeakMap<
	CourtTimeline,
	{ ends: number[]; outs: number[] }
>();
const tirednessAt = (tl: CourtTimeline, tr: Track, t: number): number => {
	let breaks = breaksOf.get(tl);
	if (!breaks) {
		const endsOf = (type: string) =>
			tl.beats
				.filter((b) => b.type === type)
				.map((b) => b.end)
				.sort((x, y) => x - y);
		breaks = { ends: endsOf("endOfPeriod"), outs: endsOf("timeout") };
		breaksOf.set(tl, breaks);
	}
	const { ends, outs } = breaks;
	const si = lastIndex(tr.shown, t, (s) => s[0]);
	let from = si >= 0 && tr.shown[si]![1] ? tr.shown[si]![0] : -Infinity;
	const ei = lastIndex(ends, t, (e) => e);
	if (ei >= 0) {
		from = Math.max(from, ends[ei]!);
	}
	const trips =
		lastIndex(tl.poss, t, (p) => p[0]) -
		lastIndex(tl.poss, from, (p) => p[0]) -
		TIMEOUT_TRIPS *
			(lastIndex(outs, t, (e) => e) - lastIndex(outs, from, (e) => e));
	return clamp01((trips - FRESH_TRIPS) / (SPENT_TRIPS - FRESH_TRIPS));
};
// Stood easy through a stoppage, now and then hands on his hips, or bent
// over with them on his knees (getting his breath) - the longer he has been
// out there, the more of it, and the more bent over - which, and when, from
// who he is and when he stopped, as off the ball.
const STOPPED_EVERY = 4200;
const stoppedLife = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	stop: Stoppage,
): AnimName | undefined => {
	const mi = lastIndex(tr.moves, t, (m) => m.t0);
	let since = Math.max(stop[0] + DEAD_LAG, mi >= 0 ? tr.moves[mi]!.t1 : 0);
	let until = Math.min(stop[1] - stop[2], tr.moves[mi + 1]?.t0 ?? Infinity);
	const ai = lastIndex(tr.acts, t, (a) => a.t0);
	for (let k = ai; k >= 0 && k >= ai - 3; k--) {
		since = Math.max(since, tr.acts[k]!.t1);
	}
	until = Math.min(until, tr.acts[ai + 1]?.t0 ?? Infinity);
	const k = Math.floor((t - since - 400) / STOPPED_EVERY);
	if (k < 0) {
		return undefined;
	}
	const tired = tirednessAt(tl, tr, t);
	const h = hash01(tr.pid + 0.5, since / 1000 + k);
	if (h < 0.55 - 0.45 * tired) {
		return undefined;
	}
	const bent = hash01(tr.pid + 2.5, since / 1000 + k) < 0.2 + 0.45 * tired;
	const anim: AnimName = bent ? "crouch" : "hips";
	const dur = (bent ? 1900 : 2600) + 1000 * tired + 800 * (h - 0.5);
	const start =
		since +
		400 +
		k * STOPPED_EVERY +
		hash01(tr.pid + 1.5, since / 1000 + k) *
			Math.max(0, STOPPED_EVERY - dur - 300);
	// (Done with, and stood up out of it, before he next moves.)
	return t < start || t >= start + dur || start + dur > until - STOOD_MS - 100
		? undefined
		: anim;
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
// Up on an inbounder, his arms waving (an act of its own): from where his
// hands were on the ball before it to where they are after, waving about as
// long as it lasts in between - never both arms swapped over in a frame as
// it starts or ends.
const guardWave = (tl: CourtTimeline, act: Act, u: number): number => {
	const a = ANIMS.guard;
	const cycles =
		((act.t1 - act.t0) / 1000) * (a.kind === "loop" ? a.fps / a.n : 1);
	const at = (t: number) => {
		const seg = ballSegAt(tl, t);
		return seg?.kind === "hold" ? guardHands(tl, seg.pid, t) : 0;
	};
	const from = at(act.t0);
	const to = at(act.t1);
	let way = to - from + Math.round(cycles - (to - from));
	if (way < 0) {
		way += 1;
	}
	return from + u * way;
};
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

// Whether a run takes him the way his back faces, by how he faces halfway
// along it: once a run.
const givingGrounds = new WeakMap<Run["mv"], boolean>();
const givingGround = (tl: CourtTimeline, tr: Track, run: Run): boolean => {
	let g = givingGrounds.get(run.mv);
	if (g === undefined) {
		const yaw = yawAt(tl, tr, (run.s0 + run.s1) / 2);
		g =
			(run.mv.to.x - run.mv.from.x) * Math.cos(yaw) +
				(run.mv.to.y - run.mv.from.y) * Math.sin(yaw) <
			0;
		givingGrounds.set(run.mv, g);
	}
	return g;
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
	dribbleFrom?: Hand;
	tight?: number;
	stride?: Stride;
	target?: number;
	// Stepping over off a man (not a shuffle he set off on).
	aside?: true;
	// Pulling up, or stood a moment between runs: his foot up off the floor
	// come down (see footDown).
	down?: number;
	stepLen?: [number, number];
	drive?: number;
};
// Eased aside off a man fast enough that he steps over (feet a second) - and
// once he is stepping, a moment's slowing between one push and the next
// doesn't stand him up for a frame or two. (Whether a slowing is that short
// is asked on a grid, so it comes out the same all through it - never on
// and off frame to frame.)
const STEP_FTPS = 1;
const STEP_GRID = 50;
const STEP_HOLD_MS = 150;
const fastAsideAt = (tr: Track, t: number): boolean => {
	asideAt(tr, t);
	return asideV > STEP_FTPS;
};
const steppingAt = (tr: Track, t: number): boolean => {
	if (!tr.nudges && !tr.sep) {
		return false;
	}
	if (fastAsideAt(tr, t)) {
		return true;
	}
	const g = Math.floor(t / STEP_GRID) * STEP_GRID;
	let last = g;
	while (!fastAsideAt(tr, last)) {
		last -= STEP_GRID;
		if (last <= g - STEP_HOLD_MS) {
			return false;
		}
	}
	for (
		let next = g + STEP_GRID;
		next <= last + STEP_HOLD_MS;
		next += STEP_GRID
	) {
		if (fastAsideAt(tr, next)) {
			return true;
		}
	}
	return false;
};
// Stood watching - arms folded, hands on his hips - he doesn't glide off in
// it: once he is on his way somewhere, he walks there.
const STANDING = new Set<AnimName>([
	"crossed",
	"hips",
	"crouch",
	"laneSet",
	"flex",
	"roar",
	"chestPound",
	"celebrate",
	"protest",
	"point",
	"clap",
	"talk",
]);
// How his feet go on a run at t: the gait for its pace, and how far through
// his strides he is.
// (Not yet round to the way he is going - opening up out of a backpedal,
// say - he backpedals on until his hips come round past this far off it,
// rather than run forward with his back to where he goes.)
const OPEN_UP = (110 * Math.PI) / 180;
// (And turning round into one - dropping back off a jog, say - he runs on
// the way he faces until his back comes round past this far off it, rather
// than backpedal while he goes the way he faces.)
const TURN_ROUND = (70 * Math.PI) / 180;
// (How long either side it has to hold, ms.)
const GAIT_HOLD = 45;
const FORWARD = new Set<AnimName>([
	"walk",
	"jog",
	"run",
	"sprint",
	"dribble",
	"dribbleWalk",
]);
// The steps he shows on a run at t: its own - or, turned far enough from
// the way he goes, a backpedal; or, round far enough, forward again.
const shownSteps = (
	tl: CourtTimeline,
	tr: Track,
	run: Run,
	t: number,
	anim: AnimName,
): AnimName => {
	if (!FORWARD.has(anim) && anim !== "back") {
		return anim;
	}
	const here = spotAt(tr, t);
	if (!here.moving || here.hx * here.hx + here.hy * here.hy <= 1e-6) {
		return anim;
	}
	// (Turned that far from the way he goes for a moment either side, not
	// just in passing - a wobble as one run gives way to the next is no
	// backpedal for a frame.)
	const heading = Math.atan2(here.hy, here.hx);
	const offAt = (when: number) =>
		Math.abs(wrapAngle(yawAt(tl, tr, when) - heading));
	const off0 = offAt(t);
	const off =
		(anim !== "back" && off0 > OPEN_UP) ||
		(anim === "back" && off0 < TURN_ROUND)
			? anim === "back"
				? Math.max(off0, offAt(t - GAIT_HOLD), offAt(t + GAIT_HOLD))
				: Math.min(off0, offAt(t - GAIT_HOLD), offAt(t + GAIT_HOLD))
			: off0;
	if (anim !== "back" && off > OPEN_UP) {
		return "back";
	}
	if (anim === "back" && off < TURN_ROUND) {
		const ahead = runAnim(tr, run);
		return FORWARD.has(ahead) ? ahead : "slide";
	}
	return anim;
};
// The steps a run ends on (see stridesAt), worked out once - and, like
// the turns, forgotten when where men are is changed after the fact.
const endSteps = new WeakMap<Track, AnimName[]>();
const endStepsOf = (
	tl: CourtTimeline,
	tr: Track,
	j: number,
	run: Run,
): AnimName => {
	let memo = endSteps.get(tr);
	if (!memo) {
		memo = [];
		endSteps.set(tr, memo);
	}
	return (memo[j] ??= shownSteps(
		tl,
		tr,
		run,
		run.mv.t1 - 1,
		stepsOf(tl, tr, run),
	));
};
const gaitOf = (
	tl: CourtTimeline,
	tr: Track,
	k: number,
	run: Run,
	t: number,
): { anim: AnimName; phase: number; len?: [number, number] } => {
	const anim = stepsOf(tl, tr, run);
	const { ph: phase, len: own } = stridesAt(tl, tr, k, run, t);
	const shown = shownSteps(tl, tr, run, t, anim);
	// (Short steps, slow - see shortSteps. On the other gait's, each as long
	// in feet.)
	const as = (len: number) =>
		Math.min(1, Math.max(LEN_MIN, (len * strideOf(anim)) / strideOf(shown)));
	const len: [number, number] = shown === anim ? own : [as(own[0]), as(own[1])];
	const short = len[0] < 1 || len[1] < 1;
	// (At the other gait's stride, so his feet stay planted - over this run
	// only: where it starts, his feet are where the one before left them.)
	return {
		anim: shown,
		phase:
			shown === anim
				? phase
				: phase + alongRun(run, t) * (1 / strideOf(shown) - 1 / strideOf(anim)),
		...(short ? { len } : {}),
	};
};
// The steps he takes on a run.
const stepsOf = (tl: CourtTimeline, tr: Track, run: Run): AnimName => {
	const anim = runAnim(tr, run);
	// Sliding with his man - or shuffling a step or two: push steps when he
	// goes across the way he faces, drop steps when he gives ground, short
	// steps up when he steps up.
	if (anim === "slide" || anim === "shuffle") {
		return sideways(tl, tr, run)
			? "shuffle"
			: givingGround(tl, tr, run)
				? "back"
				: "slide";
	}
	// Drifting along the arc square to the ball, the same: push steps, not a
	// run with his legs turned only so far under him and his feet sliding
	// the rest of the way.
	if (anim === "drift" && sideways(tl, tr, run)) {
		return "sideStep";
	}
	// Going the way his back faces - easing off from the ball while he
	// watches it, or backing out with it squared up to the rim - at no more
	// than a backpedal's pace: he backpedals, not runs forward while he goes
	// backward. (Decided for the whole run, by how he faces halfway along it,
	// so his legs don't switch mid-stride.)
	if (
		(anim === "walk" ||
			anim === "jog" ||
			anim === "run" ||
			anim === "dribble" ||
			anim === "dribbleWalk" ||
			anim === "drift") &&
		backward(tl, tr, run)
	) {
		return "back";
	}
	return anim;
};

// Between two runs with hardly a moment between them, he plants on the last
// stride and goes - he does not stand up into his stance for a frame or two.
const BRIDGE_MS = 220;
const bridgeAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
):
	| { anim: AnimName; phase: number; down: number; len?: [number, number] }
	| undefined => {
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
	// (On the steps he was taking as it ended - a backpedal, say, not
	// whatever stopped dead reads as - the ones the next run's strides
	// count on from: see stridesAt.)
	const at = Math.min(t, run.s1, run.mv.t1 - 1);
	const g = gaitOf(tl, tr, k, run, at);
	return ANIMS[g.anim].kind === "cycle"
		? {
				...g,
				down: footDownAt(run, at, k > 0 ? runOf(tr, k - 1) : undefined),
			}
		: undefined;
};
// Pulling up to a stop, over the last of it his foot up off the floor comes
// down - and setting off from one, it goes back up into his stride over the
// first of it - rather than hanging in the air while he stands there.
const STOP_STEP_MS = 140;
// (Where one run hands on to the next, as the two of them go through it
// together: not planted at the end of the one and up in his stride at the
// start of the other.)
const footDownAt = (run: Run, t: number, before?: Run, after?: Run): number => {
	const vIn =
		before && Math.abs(before.s1 - run.s0) <= 1
			? Math.max(run.v0, before.v1)
			: run.v0;
	const vOut =
		after && Math.abs(run.s1 - after.s0) <= 1
			? Math.max(run.v1, after.v0)
			: run.v1;
	return Math.max(
		vOut < LEGS_FTPS ? smooth01(1 - (run.s1 - t) / STOP_STEP_MS) : 0,
		vIn < LEGS_FTPS ? 1 - smooth01((t - run.s0) / STOP_STEP_MS) : 0,
	);
};

// A defender up on the man dribbling, in front of him (feet): from the
// farther, he starts to keep the ball in; by the nearer, it is by his hip.
const TIGHT_FAR = 3.4;
const TIGHT_NEAR = 2.2;
const tightAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here: Spot,
): number => {
	// In front of him: the way he is going, or standing, toward the rim.
	let fx = rimX(tr.team) - here.x;
	let fy = COURT_H / 2 - here.y;
	if (here.moving && Math.hypot(here.hx, here.hy) > 1e-6) {
		fx = here.hx;
		fy = here.hy;
	}
	const n = Math.hypot(fx, fy) || 1;
	fx /= n;
	fy /= n;
	let most = 0;
	for (const o of tl.tracks.values()) {
		if (o.team === tr.team) {
			continue;
		}
		const si = lastIndex(o.shown, t, (x) => x[0]);
		if (si < 0 || !o.shown[si]![1]) {
			continue;
		}
		const p = spotAt(o, t, false);
		const dx = p.x - here.x;
		const dy = p.y - here.y;
		const d = Math.hypot(dx, dy);
		if (d >= TIGHT_FAR || d < 0.01) {
			continue;
		}
		const front = clamp01(((dx * fx + dy * fy) / d - 0.1) / 0.5);
		most = Math.max(
			most,
			clamp01((TIGHT_FAR - d) / (TIGHT_FAR - TIGHT_NEAR)) * front,
		);
	}
	return most;
};
// How long his hands take to close on the ball once it is his.
const GRIP_MS = 110;
const GRIP_FLOOR_MS = 50;
const LET_GO_MS = 140;
const GRABS = new Set<AnimName>(["poke", "reach"]);
const ballIsHis = (tl: CourtTimeline, pid: number, t: number): boolean => {
	const seg = ballSegAt(tl, t);
	return seg?.kind === "hold" && seg.pid === pid;
};
// How far his hands have closed on the ball at t (1 all the way), held
// since it came to him (the hold at `bi`, and any before it straight on).
// (Off the floor, his hands are on it already as he takes it; thrown to him,
// they came to it as it got to him - see doingAt.)
const closedOn = (
	tl: CourtTimeline,
	pid: number,
	bi: number,
	t: number,
): number => {
	let since = tl.ball[bi]!.t0;
	let k = bi - 1;
	for (; k >= 0; k--) {
		const x = tl.ball[k]!;
		if (x.kind !== "hold" || x.pid !== pid || x.style !== "hold") {
			break;
		}
		since = x.t0;
	}
	const was = tl.ball[k];
	if (was?.kind === "fly" && "pid" in was.to && was.to.pid === pid) {
		return 1;
	}
	const off = was?.kind === "rest" ? GRIP_FLOOR_MS : GRIP_MS;
	return smooth01((t - since) / off);
};
const doingAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here: Spot = spotAt(tr, t),
): Doing => {
	const pid = tr.pid;
	const standing = actAt(tr, t);
	const act =
		standing &&
		((STANDING.has(standing.anim) && here.moving && here.run) ||
			// (A hand at the ball once it is his - the steal he poked at
			// come into his hands and away on his dribble: done reaching.)
			(GRABS.has(standing.anim) && ballIsHis(tl, pid, t)))
			? undefined
			: standing;
	let anim: AnimName;
	let phase: number;
	let z = 0;
	let dunk: PlayerState["dunk"];
	let reach: number | undefined;
	let mirrored = false;
	let aside = false;
	let bridged: ReturnType<typeof bridgeAt>;
	let down = 0;
	let stepLen: [number, number] | undefined;
	let drive = 0;
	if (act) {
		mirrored = act.mirror === true;
		const u = (t - act.t0) / (act.t1 - act.t0);
		// (A jumper, each man his own way of it.)
		anim = act.anim === "shoot" ? shotForm(pid) : act.anim;
		const a = ANIMS[anim];
		// (A loop on the same beat as when he does it standing of his own
		// accord - so one held for a while and then just stood in, a
		// defender's stance, say, goes on as it was, not to another frame.)
		phase =
			a.kind === "act"
				? clamp01(u)
				: anim === "guard"
					? guardWave(tl, act, u)
					: a.kind === "loop"
						? (t / 1000) * (a.fps / a.n) + pid * 0.37
						: (t - act.t0) / 1000;
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
		({
			anim,
			phase,
			len: stepLen,
		} = gaitOf(tl, tr, here.moveIndex, here.run, t));
		z = liftOf(anim, phase, stepLen);
		drive = driveOf(anim, here.run, t);
		const k = here.moveIndex;
		down = footDownAt(
			here.run,
			t,
			k > 0 ? runOf(tr, k - 1) : undefined,
			k + 1 < tr.moves.length ? runOf(tr, k + 1) : undefined,
		);
	} else if ((bridged = bridgeAt(tl, tr, t))) {
		({ anim, phase, down, len: stepLen } = bridged);
	} else {
		const seg = ballSegAt(tl, t);
		// A dribble move: all of him through each bounce of it.
		const move =
			seg?.kind === "hold" && seg.pid === pid && seg.style === "cross"
				? crossMoveAt(tl, seg, t)
				: undefined;
		const jump = jumpBallAt(tl, t);
		const stop = jump ? undefined : stoppageAt(tl, t);
		if (move) {
			anim = moveAnim(move.move, move.from);
		} else if (seg && seg.kind === "hold" && seg.pid === pid) {
			anim = seg.style === "hold" ? "hold" : "dribbleIdle";
		} else if (stop) {
			anim = stoppedLife(tl, tr, t, stop) ?? "easy";
		} else if (jump || offenseAt(tl, t) === tr.team) {
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
			(anim === "ready" ||
				anim === "stance" ||
				anim === "guard" ||
				STOOD.has(anim)) &&
			steppingAt(tr, t)
		) {
			// Eased aside off a man: he steps over, not slides.
			asideAt(tr, t);
			anim = "shuffle";
			phase = asideD / strideOf("shuffle");
			aside = true;
		} else if (life) {
			anim = life.anim;
			phase = life.phase;
			mirrored = life.mirror === true;
		}
	}
	// An act on the run with only his arms in it - a catch, a pass, a hand
	// at the ball, a man fought off - his legs keep on with his stride, not
	// stood still under him while he goes on at a run.
	// Shoved aside in it - a box-out, a fight for the ball - he steps over.
	// (Onto it or off it, he eases over - see blendInto.)
	let stride: Stride | undefined;
	if (act && STRIDE_ACTS.has(act.anim) && z <= 0) {
		const run = here.moving ? here.run : undefined;
		if (run && Math.max(shapeOf(run).vc, run.v0, run.v1) >= STRIDE_FTPS) {
			stride = { ...gaitOf(tl, tr, here.moveIndex, run, t), w: 1 };
		} else if (steppingAt(tr, t)) {
			asideAt(tr, t);
			stride = { anim: "shuffle", phase: asideD / strideOf("shuffle"), w: 1 };
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
	const tight = has?.style === "dribble" ? tightAt(tl, tr, t, here) : 0;
	// Since it came into his hands - or went out of them.
	let grip: number | undefined;
	if (has?.style === "hold") {
		const g = closedOn(tl, pid, bi, t);
		grip = g < 1 ? g : undefined;
	} else if (
		seg?.kind === "fly" &&
		"pid" in seg.to &&
		seg.to.pid === pid &&
		seg.t1 - t < GRIP_MS &&
		ballIsHis(tl, pid, seg.t1 + 1)
	) {
		// A pass coming to him: his hands come together to where it will be
		// as it gets there. (Not a shot he swats away, or a ball he tips on:
		// both hands on it for a frame and off it the next.)
		const from = Math.max(seg.t0, seg.t1 - GRIP_MS);
		grip = smooth01((t - from) / Math.max(1, seg.t1 - from));
	} else if (seg && t - seg.t0 < LET_GO_MS) {
		// (Gone from him - or put on the floor: his hands come off it as he
		// pushes it down, not out of the air.)
		// (From as far as they had closed on it: picked up and put straight
		// back down, not all the way.)
		// (Not off one hand - pulled round in a spin, laid up, say - as if
		// he had had it in both.)
		const was = tl.ball[bi - 1];
		const by = actAt(tr, seg.t0 - 1);
		if (
			was?.kind === "hold" &&
			was.pid === pid &&
			was.style === "hold" &&
			!(by && gripOf(by.anim) !== "two")
		) {
			grip =
				closedOn(tl, pid, bi - 1, seg.t0) *
				(1 - smooth01((t - seg.t0) / LET_GO_MS));
		}
	}
	return {
		anim,
		phase,
		z,
		...(mirrored ? { mirror: true } : {}),
		...(aside ? { aside: true } : {}),
		...(dunk ? { dunk } : {}),
		...(reach ? { reach } : {}),
		holding: has?.style === "hold",
		...(grip === undefined ? {} : { grip }),
		dribble: beat?.ph,
		dribbleHand: beat ? (beat.ph < DOWN ? beat.from : beat.to) : undefined,
		...(beat && beat.from !== beat.to ? { dribbleFrom: beat.from } : {}),
		...(tight > 0 ? { tight } : {}),
		...(stride ? { stride } : {}),
		...(down > 0 ? { down } : {}),
		...(stepLen ? { stepLen } : {}),
		...(drive !== 0 ? { drive } : {}),
		target: act || has ? undefined : targetAt(tl, pid, t) || undefined,
	};
};

// HOW HARD HE IS GOING AT IT on a run, getting going or pulling up (see
// PlayerState's drive): as hard as he is speeding up or slowing down,
// against all he has (feet a second, a second) - on his feet going forward,
// not sliding or backpedalling, and nothing to speak of left out.
const DRIVE_FULL = 28;
const DRIVE_MIN = 0.08;
const DRIVES = new Set<AnimName>(["run", "sprint", "jog", "dribble"]);
const driveOf = (anim: AnimName, run: Run, t: number): number => {
	if (!DRIVES.has(anim)) {
		return 0;
	}
	const d = Math.max(
		-1,
		Math.min(1, pushAlong(shapeOf(run), (t - run.s0) / 1000) / DRIVE_FULL),
	);
	return Math.abs(d) < DRIVE_MIN ? 0 : d;
};
// How far off the floor his stride takes him (short steps: see shortLift).
const liftOf = (
	anim: AnimName,
	ph: number,
	len: [number, number] | undefined,
): number => (len ? shortLift(anim, len, ph) : bounceAt(anim, ph));

// One move into the next eases in: for a moment after the change his body
// is part the way from how the last move had it to how this one wants it -
// not snapped there between one frame and the next. Into a dribble move his
// hands and shoulders get there quicker than his feet: a bounce is over in a
// third of a second, and the ball has to go where they take it.
const BLEND_MS = 160;
// (Pulled up out of a run into where he stands, a step with each foot:
// longer.)
const SETTLE_MS = 300;
// (Stood easy, standing up out of his stance or bending over onto his
// knees and back: in no hurry.)
const STOOD_MS = 500;
const MOVE_ARMS_MS = 60;
// (Behind his back, the move starts with his hand out in front where the
// ball comes up into it, and sweeps it back from there: no time to hold it
// where it was.)
const BACK_ARMS_MS = 25;
const BLEND_DEPTH = 8;
const FLICK_MS = 40;
// An arm thrown up - fighting for the ball, contesting, reaching - comes
// back down on its own time, not snapped to his side with the rest of him
// (unless what he goes into wants his hands: a catch, a shot).
const ARMS_UP = new Set<AnimName>([
	"fight",
	"contest",
	"block",
	"rebound",
	"reach",
	"hack",
	"threeUp",
	"goggles",
	"signalUp",
	"cheer",
	"celebrate",
]);
const ARMS_DOWN_MS = 320;
const SLIVER_MS = 80;
// (And under a frame of it, a blip.)
const BLIP_MS = 20;
const armsComeDown = (tr: Track, t: number, from: AnimName, to: AnimName) =>
	ARMS_UP.has(from) && !ARMS_UP.has(to) && !actAt(tr, t);
// Still bringing them down after the rest of him has finished easing over.
const armsDown = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	now: Doing,
): Blend | undefined => {
	const i = lastIndex(tr.acts, t, (a) => a.t0);
	for (let k = i; k >= 0 && k >= i - 3; k--) {
		const a = tr.acts[k]!;
		if (a.t1 > t || t - a.t1 >= ARMS_DOWN_MS) {
			continue;
		}
		if (!armsComeDown(tr, t, a.anim, now.anim)) {
			return undefined;
		}
		// When it ended: when he stopped doing it - cut short, set off at a
		// run in the middle of it, then, not when it was meant to end (or
		// the arms come down out of the run he was already on: not at all,
		// the arms up there one frame and gone the next).
		let end = a.t1;
		let before = doingAt(tl, tr, end - 1);
		if (before.anim !== a.anim) {
			const first = doingAt(tl, tr, a.t0 + 1);
			if (first.anim === a.anim) {
				let lo = a.t0 + 1;
				let hi = end - 1;
				before = first;
				for (let n = 0; n < 12 && hi - lo > 0.5; n++) {
					const mid = (lo + hi) / 2;
					const d = doingAt(tl, tr, mid);
					if (d.anim === a.anim) {
						lo = mid;
						before = d;
					} else {
						hi = mid;
					}
				}
				end = lo;
			}
		}
		// (As far up as they got: a moment's act hardly raised them.)
		const arms =
			(1 - smooth01((t - end) / ARMS_DOWN_MS)) *
			Math.min(1, (end - a.t0) / BLEND_MS);
		return arms <= 0.02
			? undefined
			: {
					anim: before.anim,
					phase: before.phase,
					target: before.target,
					...(before.mirror ? { mirror: true } : {}),
					w: 0,
					arms,
				};
	}
	return undefined;
};
const blendInto = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	now: Doing,
	// How far back inside other blends this is looking (see BLEND_DEPTH).
	depth = 0,
): Blend | undefined => {
	// (A change of move - or, the same move, from stepping over off a man
	// into strides he set off on, or from dribbling it into holding it: his
	// feet and hands are not where they were either.)
	const same = (d: Doing) =>
		d.anim === now.anim &&
		(d.aside === true) === (now.aside === true) &&
		(d.dribble === undefined) === (now.dribble === undefined) &&
		d.stride?.anim === now.stride?.anim;
	const anim = now.anim;
	let lo = t - BLEND_MS;
	let before = doingAt(tl, tr, lo);
	let settling = false;
	if (same(before)) {
		// The same as a moment ago - but maybe something else in between,
		// and back: out of that, then. (Looked for at the same moments of
		// the game whatever the frame, and all the way back to BLEND_MS ago
		// - a sliver of something found some frames and missed in others,
		// he flickers in and out of it.)
		const g = Math.floor(t / FLICK_MS) * FLICK_MS;
		let flick: { at: number; d: Doing } | undefined;
		for (let at = g; at > lo - FLICK_MS && !flick; at -= FLICK_MS) {
			const d = doingAt(tl, tr, at);
			if (!same(d)) {
				flick = { at, d };
			}
		}
		// (Looked for the whole settle ago and halfway back to it: a step
		// too short to span the gap between that and BLEND_MS ago is not
		// lost in it for a few frames, and found again after.)
		const back = STOOD.has(anim) ? STOOD_MS : SETTLE_MS;
		const settle =
			flick || !(PIVOTS.has(anim) || SETTLE_FROM.has(anim))
				? undefined
				: [t - back, t - (back + BLEND_MS) / 2]
						.map((at) => ({ at, d: doingAt(tl, tr, at) }))
						.find((x) => !same(x.d));
		if (flick) {
			lo = flick.at;
			before = flick.d;
		} else if (settle) {
			// Still settling, maybe - out of whatever came last before this,
			// found below: not whatever he was doing the whole settle ago (a
			// moment's shuffle out of standing, and then a point, settles out
			// of the shuffle all the way, not out of it and then all at once).
			lo = settle.at;
			before = settle.d;
			settling = true;
		} else {
			return armsDown(tl, tr, t, now);
		}
	}
	// When it changed - to the moment, and what he was doing the moment
	// before: never something before that. (A sliver of something between
	// two moves - the dribble picked up the instant before the pass goes -
	// found some frames and missed in others, he flickers between easing
	// out of it and out of what came before it.)
	let hi = t;
	// (Looked for back from now a blip at a time first, not just bisected
	// for: what he does can change and change back in between - a stride
	// under a contest ended by the jump, say - and a bisection finds the
	// one change some frames and the other in others. At the same moments
	// of the game whatever the frame, too: a sliver shorter than a blip.)
	for (
		let at = Math.floor((t - BLIP_MS / 2) / BLIP_MS) * BLIP_MS;
		at > lo;
		at -= BLIP_MS
	) {
		const d = doingAt(tl, tr, at);
		if (!same(d)) {
			lo = at;
			before = d;
			break;
		}
		hi = at;
	}
	for (let k = 0; k < 12 && hi - lo > 0.5; k++) {
		const mid = (lo + hi) / 2;
		const d = doingAt(tl, tr, mid);
		if (same(d)) {
			hi = mid;
		} else {
			lo = mid;
			before = d;
		}
	}
	// (Nor a sliver of this same move, only some way of it apart - the last
	// instant of a stride as a pass starts, say: out of what came before
	// that, or he eases out of the one some frames and the other in others.)
	if (before.anim === anim) {
		const back = doingAt(tl, tr, lo - BLIP_MS);
		if (back.anim !== anim) {
			lo -= BLIP_MS;
			before = back;
		}
	}
	if (settling && !slowOver(before.anim, anim)) {
		return armsDown(tl, tr, t, now);
	}
	// (A moment of something else between an arm thrown up and this - his
	// feet catching up with him - is no change of its own: out of that arm,
	// then, or it comes down in fits and starts.)
	if (!ARMS_UP.has(before.anim)) {
		const i = lastIndex(tr.acts, hi, (a) => a.t0);
		for (let k = i; k >= 0 && k >= i - 3; k--) {
			const a = tr.acts[k]!;
			if (a.t1 <= hi && hi - a.t1 < SLIVER_MS && ARMS_UP.has(a.anim)) {
				hi = a.t1;
				lo = hi - 1;
				before = doingAt(tl, tr, lo);
				break;
			}
		}
	}
	const ease = (ms: number) => {
		const u = Math.min(1, (t - hi) / ms);
		return 1 - u * u * (3 - 2 * u);
	};
	const w = ease(
		!slowOver(before.anim, anim)
			? BLEND_MS
			: STOOD.has(anim) && PIVOTS.has(before.anim)
				? STOOD_MS
				: SETTLE_MS,
	);
	const slowArms = armsComeDown(tr, t, before.anim, anim);
	if (w <= 0.02 && !(slowArms && ease(ARMS_DOWN_MS) > 0.02)) {
		return undefined;
	}
	// How he looked as it changed: the last move - itself, maybe, still
	// easing out of the one before it, and that out of the one before (a
	// few quick changes in a row must not drop any of them in a frame).
	const prior =
		depth + 1 < BLEND_DEPTH
			? blendInto(tl, tr, lo, before, depth + 1)
			: undefined;
	// The turn of his legs then, eased out of too - not snapped round (as
	// they showed, part the way out of the move before, if they were: each
	// foot still pointing where it pointed on the floor as that changed).
	const facing = yawAt(tl, tr, lo);
	const there = spotAt(tr, lo);
	const own = flippedLegs(
		tl,
		tr,
		lo,
		before.anim,
		legsOf(there, facing, before.anim, lo),
	);
	const way =
		PUSH_STEPS.has(before.anim) && there.moving
			? wayOf(tr, facing, lo)
			: undefined;
	const ownPv = pivotOf(tl, tr, facing, before, lo);
	const turns = ([0, 1] as const).map((i) => {
		const to = own + (ownPv?.[i] ?? 0);
		return prior && prior.w > 0
			? to + (legTurnFrom(prior, i, facing, before.anim) - to) * prior.w
			: to;
	}) as [number, number];
	const pv: [number, number, number, number] | undefined =
		prior?.pivot || ownPv
			? ([0, 1, 2, 3].map(
					(i) =>
						(ownPv?.[i] ?? 0) +
						((prior?.pivot?.[i] ?? 0) - (ownPv?.[i] ?? 0)) * (prior?.w ?? 0),
				) as [number, number, number, number])
			: undefined;
	return {
		anim: before.anim,
		phase: before.phase,
		dribble: before.dribble,
		dribbleHand: before.dribbleHand,
		...(before.dribbleFrom ? { dribbleFrom: before.dribbleFrom } : {}),
		...(before.tight ? { tight: before.tight } : {}),
		...(before.stride ? { stride: before.stride } : {}),
		...(before.down ? { down: before.down } : {}),
		...(before.stepLen ? { stepLen: before.stepLen } : {}),
		// (His hands most of the way onto it as it changed - picked up as he
		// went up into a floater, or a pass all but in them as he goes
		// straight into his dribble - count as on it: not his arms back
		// where they were before for a frame.)
		...((before.holding || before.grip !== undefined) &&
		(before.grip ?? 1) >= 0.5
			? { held: true }
			: {}),
		target: before.target,
		...(before.mirror ? { mirror: true } : {}),
		w,
		...(pv ? { pivot: pv } : {}),
		...(turns[0] || turns[1] ? { turns } : {}),
		...(way === undefined ? {} : { way }),
		facing,
		...(isMove(anim)
			? {
					arms: ease(
						anim === "crossBackR" || anim === "crossBackL"
							? BACK_ARMS_MS
							: MOVE_ARMS_MS,
					),
				}
			: slowArms
				? { arms: ease(ARMS_DOWN_MS) }
				: {}),
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
		const hand: Hand = g.mirror ? "L" : "R";
		return free === otherHand(hand) || w <= 0.02
			? undefined
			: {
					hand,
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
// His hand out at a man `d` feet away, `deg` degrees round from straight
// ahead to that hand's side, `up` feet over his hips: `them` short of the
// middle of the other man (feet), and never further out than `most` - on
// him, never into him, however close the two of them are.
const ARM_BODY = bodyOf();
const reachAt = (
	q: Pose,
	hand: Hand,
	d: number,
	deg: number,
	them: number,
	most: number,
	up: number,
): { sh: number; el: number; ab: number } => {
	const r = Math.max(0.7, Math.min(most, d - them));
	const a = (Math.max(0, Math.min(100, deg)) * Math.PI) / 180;
	return armReaching(ARM_BODY, q, hand, {
		f: r * Math.cos(a),
		s: Math.max(0.45, r * Math.sin(a)),
		u: up,
	});
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
	q: Pose,
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
	const hand = bearing >= 0 ? "R" : "L";
	const d = Math.hypot(H.x - here.x, H.y - here.y);
	return { hand, ...reachAt(q, hand, d, deg, 0.65, 2, 0.4), wr: 12, w };
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
	q: Pose,
	up = 1.45,
): ArmPose | undefined => {
	const bearing = wrapAngle(Math.atan2(them.y - here.y, them.x - here.x) - yaw);
	const deg = ((free === "R" ? bearing : -bearing) * 180) / Math.PI;
	const w = w0 * smooth01((deg + 30) / 30) * smooth01((150 - deg) / 30);
	if (w <= 0.05) {
		return undefined;
	}
	const d = Math.hypot(them.x - here.x, them.y - here.y);
	return { hand: free, ...reachAt(q, free, d, deg, 0.85, 1.6, up), wr: 0, w };
};
const engageArm = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here: Pt,
	yaw: number,
	now: { anim: AnimName; phase: number; dribbleHand?: Hand; holding?: boolean },
): ArmPose | undefined => {
	const seg = ballSegAt(tl, t);
	if (seg?.kind !== "hold" || actAt(tr, t)) {
		return undefined;
	}
	const holder = tl.tracks.get(seg.pid);
	if (!holder) {
		return undefined;
	}
	const q = poseAt(now.anim, now.phase);
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
		return barArm(
			free,
			here,
			yaw,
			D.at,
			smooth01((ENGAGE_NEAR - D.d) / 1.4),
			q,
		);
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
		return barArm(
			bearing >= 0 ? "R" : "L",
			here,
			yaw,
			D.at,
			smooth01((FIGHT_NEAR - D.d) / 1) * smooth01((deg - 15) / 20),
			q,
			1.6,
		);
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
				q,
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
	return rideArm(
		tl,
		tr,
		A.tr,
		t,
		here,
		yaw,
		smooth01((FIGHT_NEAR - A.d) / 1),
		q,
	);
};

// Just where he is on the floor at t, if he is on it - cheap, for asking
// of every man at every step.
export const floorSpotOf = (tr: Track, t: number): Pt | undefined => {
	const si = lastIndex(tr.shown, t, (x) => x[0]);
	if (si < 0 || !tr.shown[si]![1]) {
		return undefined;
	}
	const s = spotAt(tr, t, false);
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
const ARM_TAPS = 4;
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
	// (Going over from one arm to the other, the one comes down before the
	// other goes up.)
	const hand: Hand = r >= l ? "R" : "L";
	const mine = taps.filter((x) => x.a.hand === hand);
	const w = Math.abs(r - l);
	if (w <= 0.02) {
		return undefined;
	}
	const all = hand === "R" ? r : l;
	const mean = (get: (a: ArmPose) => number) =>
		mine.reduce((sum, x) => sum + get(x.a) * x.c, 0) / all;
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

// The hand his dribble is in at t (none, not dribbling it).
const dribbleHandAt = (
	tl: CourtTimeline,
	pid: number,
	t: number,
): Hand | undefined => {
	const bi = ballIndexAt(tl, t);
	const seg = tl.ball[bi];
	if (seg?.kind !== "hold" || seg.pid !== pid || seg.style === "hold") {
		return undefined;
	}
	const beat = seg.style === "cross" ? crossAt(seg, t) : bounceOf(tl, bi, t);
	return beat.ph < DOWN ? beat.from : beat.to;
};
// His dribble about to come over into the hand saying something: that arm
// down out of the way as it comes, not gone in a frame when it gets there.
const ARM_CLEAR_MS = 150;
const clearOf = (
	tl: CourtTimeline,
	pid: number,
	t: number,
	arm: ArmPose | undefined,
): ArmPose | undefined => {
	if (!arm || dribbleHandAt(tl, pid, t + ARM_CLEAR_MS) !== arm.hand) {
		return arm;
	}
	let lo = t;
	let hi = t + ARM_CLEAR_MS;
	for (let n = 0; n < 10; n++) {
		const mid = (lo + hi) / 2;
		if (dribbleHandAt(tl, pid, mid) === arm.hand) {
			hi = mid;
		} else {
			lo = mid;
		}
	}
	const w = arm.w * smooth01((hi - t) / ARM_CLEAR_MS);
	return w > 0 ? { ...arm, w } : undefined;
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
	const doing = doingAt(tl, tr, t, here);
	const from = shown ? blendInto(tl, tr, t, doing) : undefined;
	// (The bob of his stride comes in as his legs do - not all of him up
	// off the floor in a frame, setting off from where he stood.)
	const now =
		from && from.w > 0 && isGait(doing.anim)
			? {
					...doing,
					z:
						doing.z +
						((isGait(from.anim)
							? liftOf(from.anim, from.phase, from.stepLen)
							: 0) -
							doing.z) *
							from.w,
				}
			: doing;
	const yaw = spinYawAt(tl, tr, t) ?? yawAt(tl, tr, t);
	const legs = flippedLegs(tl, tr, t, now.anim, legsOf(here, yaw, now.anim, t));
	const way =
		PUSH_STEPS.has(now.anim) && here.moving ? wayOf(tr, yaw, t) : undefined;
	const pivot = shown ? pivotOf(tl, tr, yaw, now, t) : undefined;
	// (An arm a beat behind its reason never takes the hand his dribble has
	// just come over into.)
	const eased = shown ? easedArm(tl, tr, t) : undefined;
	const said =
		eased?.hand === now.dribbleHand
			? undefined
			: now.dribbleHand === undefined
				? eased
				: clearOf(tl, pid, t, eased);
	// (His hands on the ball, or closing on it, neither arm is out saying
	// anything: an arm up against his man as he picks up his dribble comes
	// in to the ball with the other - not still easing down as he goes up
	// with it.)
	const free = now.grip !== undefined ? 1 - now.grip : now.holding ? 0 : 1;
	const arm =
		said && free < 1
			? said.w * free > 0.02
				? { ...said, w: said.w * free }
				: undefined
			: said;
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
		...(way === undefined ? {} : { way }),
		...(pivot ? { pivot } : {}),
		...(from ? { from } : {}),
		...(arm ? { arm } : {}),
	};
};

// Push steps, a foot out and the other closed up after it (see slanted).
const PUSH_STEPS = new Set<AnimName>(["shuffle", "sideStep"]);
// The way he shuffles, in his frame: the way he gets from a moment before
// to a moment after - so it comes round a corner as he does, not in a frame
// or two - and as much of it as counts, none all but stood still (turning
// back the way he came, say: which foot leads changes over there, not in a
// frame).
const WAY_MS = 60;
const WAY_FTPS = 3;
const wayOf = (tr: Track, yaw: number, t: number): [number, number] => {
	const a = spotAt(tr, t - WAY_MS, false);
	const b = spotAt(tr, t + WAY_MS, false);
	const dx = b.x - a.x;
	const dy = b.y - a.y;
	const d = Math.hypot(dx, dy);
	if (d < 1e-6) {
		return [0, 0];
	}
	const on = smooth01((d * 1000) / (2 * WAY_MS) / WAY_FTPS) / d;
	const c = Math.cos(yaw);
	const sn = Math.sin(yaw);
	// (His left is the floor's clockwise.)
	return [(dx * c + dy * sn) * on, (dx * sn - dy * c) * on];
};

// SHUFFLING ON A SLANT.
//
// A shuffle steps straight out to his side - and going partly ahead or back
// as well, his feet would slide that way under him. So each foot steps out
// the way he goes instead: as far out from under him, but only so much of it
// across and the rest ahead (the foot he leads with) or behind (the other) -
// his hips square, his feet together at the close the same as ever. (Going
// so nearly straight ahead or back that which foot leads is no way to tell,
// not at all.)
// How far ahead a foot goes for a turn of his hip (feet a radian, a typical
// man down in his stance - his hips turned the two ways at once).
const SLANT_REACH = 2.15;
const slanted = (q: Pose, way: [number, number]): Pose => {
	const [f, s] = way;
	const m = Math.hypot(f, s);
	const d = q.wide - SLIDE_NARROW;
	if (m < 1e-3 || d <= 0) {
		return q;
	}
	const side = Math.abs(s) / m;
	const on = smooth01((side - 0.3) / 0.3) * Math.min(1, m);
	if (on <= 0) {
		return q;
	}
	const ahead = ((d * (f / m) * on) / SLANT_REACH) * (180 / Math.PI);
	const across = d * (1 - side) * on;
	// (His left leads going to his left.)
	return {
		...q,
		wideN: q.wideN - across,
		wideF: q.wideF - across,
		hipN: q.hipN + (s > 0 ? -ahead : ahead),
		hipF: q.hipF + (s > 0 ? ahead : -ahead),
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
// (Into or out of a run he joins at hardly more than a standstill, the same:
// at that pace which way it heads is no way to turn his hips.)
const LEGS_FTPS = 2;
const legsOf = (here: Spot, yaw: number, anim: AnimName, t: number): number => {
	const run = here.run;
	// (Backpedalling, his legs go back the way he goes; closing out, his
	// chop steps go the way he goes.)
	const back = anim === "back";
	if (
		!here.moving ||
		!run ||
		!(isGait(anim) || back || anim === "slide" || anim === "closeout")
	) {
		return 0;
	}
	if (here.hx * here.hx + here.hy * here.hy < 1e-6) {
		return 0;
	}
	const off =
		(wrapAngle(Math.atan2(here.hy, here.hx) - yaw + (back ? Math.PI : 0)) *
			180) /
		Math.PI;
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
	// (And round a corner, the pace he goes round it at: turned right back
	// on himself, all but stood still at the turn, not swung round with
	// it.)
	const ease =
		Math.min(
			run.v0 > LEGS_FTPS ? 1 : smooth01((t - run.s0) / LEGS_EASE_MS),
			run.v1 > LEGS_FTPS ? 1 : smooth01((run.s1 - t) / LEGS_EASE_MS),
		) * (here.round === undefined ? 1 : smooth01(here.round / LEGS_FTPS));
	// (His left is the floor's clockwise.)
	return -Math.sign(off) * turn * ease;
};
// Turned and away out of a backpedal, his legs come round under him over a
// couple of steps - eased over from the turn they had under him as he went
// back - not swung from back the way he went to out ahead of him in a frame
// or two as his hips go round. (The last moment he was backpedalling: looked
// for back a FLICK_MS at a time, at the same moments whatever the frame,
// past nothing but strides.)
const FLIP_MS = 320;
const FLIPS_TO = new Set<AnimName>([
	"run",
	"sprint",
	"jog",
	"walk",
	"dribble",
	"dribbleWalk",
	"carry",
]);
const flippedLegs = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	anim: AnimName,
	own: number,
): number => {
	if (!FLIPS_TO.has(anim)) {
		return own;
	}
	for (
		let at = Math.floor(t / FLICK_MS) * FLICK_MS;
		at > t - FLIP_MS;
		at -= FLICK_MS
	) {
		const d = doingAt(tl, tr, at);
		if (d.anim === "back") {
			// (Not a moment's backpedal in passing, his legs never round for
			// it: that is eased out of like any other change.)
			if (doingAt(tl, tr, at - FLICK_MS).anim !== "back") {
				return own;
			}
			const was = legsOf(spotAt(tr, at), yawAt(tl, tr, at), "back", at);
			return own + (was - own) * (1 - smooth01((t - at) / FLIP_MS));
		}
		if (!FLIPS_TO.has(d.anim)) {
			return own;
		}
	}
	return own;
};

// TURNING WHERE HE STANDS.
//
// Turned round on the spot, his feet don't spin under him with the rest of
// him: one stays where it was, turned on, while the other steps round after
// him - every PIVOT_MS the other way about, a PIVOT_STEP_MS step up off the
// floor to where he will be facing by the end of it. (Only as far as hips
// turn: past that, the planted foot goes round with him.)
const PIVOT_MS = 160;
const PIVOT_STEP_MS = 110;
// A step's lift (degrees at the hip and knee, a full step of the turn), and
// how far round a step is a full one (radians).
const PIVOT_HIP = 16;
const PIVOT_KNEE = 34;
const PIVOT_FULL = 0.35;
const PIVOTS = new Set<AnimName>([
	"ready",
	"stance",
	"easy",
	"crouch",
	"laneSet",
	"guard",
	"hold",
	"triple",
	"dribbleIdle",
	"boxOut",
	"callPlay",
	"spotUp",
	"callBall",
	"clapCall",
	"stanceHands",
	"stancePoint",
	"point",
	"crossed",
	"clap",
	"hips",
	"talk",
	"protest",
	// (Hands at work, feet where they are.)
	"contest",
	"reach",
	"hack",
	"poke",
	"fight",
	"bump",
	"catch",
	"pass",
	"passBounce",
	"passOverhead",
	"passPush",
	"passBouncePush",
	"shotFake",
	"jab",
	"strip",
	"screen",
	"highFive",
	"lowFive",
	"waitFive",
	"signalUp",
	"signalSide",
	"threeUp",
	"goggles",
	"shimmy",
	"cheer",
	"crouch",
	"flex",
	"roar",
	"chestPound",
]);
// A leg turned under him from the way he faces (degrees): as far as hips
// turn - and further round than that, back with him by the time he is
// facing the other way.
const hipsTurn = (d: number) => {
	const w = ((((d + 180) % 360) + 360) % 360) - 180;
	const a = Math.abs(w);
	return (
		Math.sign(w) *
		(a <= LEGS_TURN ? a : (LEGS_TURN * (180 - a)) / (180 - LEGS_TURN))
	);
};
// A leg's turn under him (degrees, right or left) as a move he is easing
// out of had it, now that he faces `yaw`. Pulled up into where he stands,
// where it pointed on the floor: turning round as he settles, he turns on
// the foot he is down on, not with it slid round under him. (On the move,
// with him.)
export const legTurnFrom = (
	f: Blend,
	i: 0 | 1,
	yaw: number,
	to: AnimName,
): number =>
	hipsTurn(
		(f.turns?.[i] ?? 0) +
			(f.facing === undefined || !PIVOTS.has(to)
				? 0
				: (wrapAngle(yaw - f.facing) * 180) / Math.PI),
	);
const pivotOf = (
	tl: CourtTimeline,
	tr: Track,
	yaw: number,
	now: Doing,
	t: number,
): [number, number, number, number] | undefined => {
	if (now.z > 0 || !PIVOTS.has(now.anim)) {
		return undefined;
	}
	const k = Math.floor(t / PIVOT_MS);
	const g = k * PIVOT_MS;
	// (Right steps round on even beats, left on odd.)
	const leg = (even: boolean): [number, number] => {
		if ((k % 2 === 0) !== even) {
			return [yawAt(tl, tr, g), 0];
		}
		const a = yawAt(tl, tr, g - PIVOT_MS);
		const d = wrapAngle(yawAt(tl, tr, g + PIVOT_MS) - a);
		const u = clamp01((t - (g + PIVOT_MS - PIVOT_STEP_MS)) / PIVOT_STEP_MS);
		return [
			a + d * smooth01(u),
			Math.sin(Math.PI * u) * Math.min(1, Math.abs(d) / PIVOT_FULL),
		];
	};
	const [rf, rl] = leg(true);
	const [lf, ll] = leg(false);
	// (As far as hips turn - and further round than that, back with him by
	// the time he is facing the other way. His left is the floor's
	// clockwise.)
	const spin = (feet: number) => {
		const d = (wrapAngle(feet - yaw) * 180) / Math.PI;
		const a = Math.abs(d);
		return (
			-Math.sign(d) *
			(a <= LEGS_TURN ? a : (LEGS_TURN * (180 - a)) / (180 - LEGS_TURN))
		);
	};
	const r = spin(rf);
	const l = spin(lf);
	return Math.abs(r) < 0.5 && Math.abs(l) < 0.5 && rl < 0.01 && ll < 0.01
		? undefined
		: [r, l, rl, ll];
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
	"onHipN",
	"onHipF",
	"foldN",
	"foldF",
	"onKneeN",
	"onKneeF",
	"flexN",
	"flexF",
];
// Acts that are all arms - done on the run (from this fast, feet a second),
// his legs keep on striding.
const STRIDE_FTPS = 3;
const STRIDE_ACTS = new Set<AnimName>([
	"catch",
	"pass",
	"passBounce",
	"passOverhead",
	"passPush",
	"passBouncePush",
	"passBehind",
	"reach",
	"poke",
	"callBall",
	"contest",
	"boxOut",
	"fight",
	"bump",
]);
const STRIDE_LEGS: (keyof Pose)[] = [
	"hipN",
	"kneeN",
	"hipF",
	"kneeF",
	"wide",
	"plant",
	"toe",
	"toeN",
	"toeF",
];
// A pose with its legs on a stride (see Stride).
const strode = (q: Pose, s: Stride | undefined): Pose => {
	if (!s || s.w <= 0) {
		return q;
	}
	const legs = shortSteps(poseAt(s.anim, s.phase), s.anim, s.len, s.phase);
	const out = { ...q };
	for (const k of STRIDE_LEGS) {
		out[k] = q[k] + (legs[k] - q[k]) * s.w;
	}
	return out;
};
// The pose a blend shows: its move's, itself easing out of the one before.
const blendPose = (f: Blend): Pose => {
	const as = posed(
		f.anim,
		f.phase,
		f.dribble,
		f.dribbleHand,
		f.target,
		f.dribbleFrom,
		f.tight,
	);
	const short = shortSteps(as, f.anim, f.stepLen, f.phase);
	const m = strode(f.mirror ? mirror(short) : short, f.stride);
	const own0 = footDown(f.held ? carried(m, f.anim) : m, f.down ?? 0);
	const own =
		PUSH_STEPS.has(f.anim) && f.way !== undefined ? slanted(own0, f.way) : own0;
	const g = f.from;
	if (!g || (g.w <= 0 && (g.arms ?? 0) <= 0)) {
		return own;
	}
	const eased = blendFrom(own, g);
	// (Itself still settling out of a run, his feet stepping into it, as he
	// went on into something else: from there - as he was - not from the two
	// eased together, both feet slid over to it in a frame.)
	return g.w > 0 && g.w < 1 && stepsOver(g.anim, f.anim)
		? settleStep(eased, own, blendPose(g), 1 - g.w, standRef(f))
		: eased;
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

// Where he stands, as it starts: its pose at the top of its cycle.
const standRef = (x: {
	anim: AnimName;
	dribble?: number;
	dribbleHand?: Hand;
	target?: number;
	dribbleFrom?: Hand;
	tight?: number;
	mirror?: boolean;
}): Pose => {
	const q = posed(
		x.anim,
		0,
		x.dribble,
		x.dribbleHand,
		x.target,
		x.dribbleFrom,
		x.tight,
	);
	return x.mirror ? mirror(q) : q;
};

// Pulled up out of a run into where he stands, his feet step there - the
// one with furthest to go first, then the other, each up off the floor and
// back down - rather than sliding over to it as the one pose eases into the
// other: `u` of the way (0 to 1), from `was` to `to`.
const SETTLE_FROM = new Set<AnimName>([
	"run",
	"jog",
	"sprint",
	"walk",
	"dribble",
	"dribbleWalk",
	"back",
	"slide",
	"shuffle",
	"sideStep",
	"closeout",
	"drift",
	"carry",
]);
// (And from one way of moving his feet in his stance into another - a
// slide into a drop step, say - each foot steps over, not both slid.)
const FOOTWORK = new Set<AnimName>(["shuffle", "back", "slide"]);
// (And standing up out of his stance to stand easy, or back down into it:
// his feet stepped in under him and out again, not slid.)
const STOOD = new Set<AnimName>(["easy", "hips", "crouch"]);
const stepsOver = (from: AnimName, to: AnimName) =>
	(PIVOTS.has(to) && SETTLE_FROM.has(from)) ||
	(FOOTWORK.has(from) && FOOTWORK.has(to)) ||
	(PIVOTS.has(from) && PIVOTS.has(to) && STOOD.has(from) !== STOOD.has(to));
// (Stood easy, his hands go to his hips or his knees, and back, in no
// hurry either: as long as a step over.)
const slowOver = (from: AnimName, to: AnimName) =>
	stepsOver(from, to) || (STOOD.has(from) && STOOD.has(to));
const SETTLE_HIP = 14;
const SETTLE_KNEE = 30;
// (A full step for a leg this far from where it is going - degrees at the
// hip, half that again of its knee, and his feet's spread at so many a
// foot.)
const SETTLE_FULL = 30;
const SETTLE_SPREAD = 40;
// Each step's share of the way, overlapping.
const SETTLE_STEP = 0.6;
// Two legs this near to as far to go (degrees, see settleLegs) go over
// from the one first to the other first gradually.
const SETTLE_TIE = 6;
// (One foot this much higher than the other - in legs' lengths, about - is
// up off the floor.)
const SETTLE_UP = 0.12;
// (Stepping over only on his feet: not off them, up in the air - though
// rolled up onto his toes is on them.)
const SETTLE_AIR = 0.25;
type SettleLeg = { on: number; up: number };
const settleLegs = (
	to: Pose,
	was: Pose,
	u: number,
	// (Which goes first, reckoned against where he stands at the start of
	// it - not the stand's own breathing, which would have them swap over
	// halfway.)
	toRef: Pose = to,
): { n: SettleLeg; f: SettleLeg } => {
	const away = (q: Pose) => {
		const spread = Math.abs(was.wide - q.wide) * SETTLE_SPREAD;
		return [
			Math.abs(was.hipN - q.hipN) +
				0.5 * Math.abs(was.kneeN - q.kneeN) +
				spread,
			Math.abs(was.hipF - q.hipF) +
				0.5 * Math.abs(was.kneeF - q.kneeF) +
				spread,
		] as const;
	};
	const [dn, df] = away(to);
	const [rn, rf] = toRef === to ? [dn, df] : away(toRef);
	// (Each leg gets there only while it is up: not slid the rest of the
	// way once it is down again.)
	const leg = (d: number, from: number) => {
		const v = clamp01((u - from) / SETTLE_STEP);
		return {
			on: smooth01(v),
			up: Math.sin(Math.PI * v) * Math.min(1, d / SETTLE_FULL),
		};
	};
	// The foot already up off the floor first - then the one he stands on,
	// or (both down) the one with furthest to go.
	const wasFlex = flexOf(was);
	const RAD = Math.PI / 180;
	const lift = (hip: number, knee: number) =>
		-Math.cos((hip + wasFlex) * RAD) - Math.cos((hip + wasFlex - knee) * RAD);
	const up = lift(was.hipN, was.kneeN) - lift(was.hipF, was.kneeF);
	// (How surely the near one goes first: the foot that is up, the more
	// clearly up it is - eased over into the one with furthest to go as
	// both come down, not switched from one frame to the next.)
	// (Near enough the same way to go, neither leg first outright - eased
	// from the one to the other, not switched as the two pass by a hair.)
	const byFar = smooth01((rn - rf) / SETTLE_TIE + 0.5);
	const lifted = Math.min(1, Math.abs(up) / SETTLE_UP);
	const first = byFar + ((up > 0 ? 1 : 0) - byFar) * lifted;
	return {
		n: leg(dn, (1 - first) * (1 - SETTLE_STEP)),
		f: leg(df, first * (1 - SETTLE_STEP)),
	};
};
const settleStep = (
	p: Pose,
	to: Pose,
	was: Pose,
	u: number,
	toRef: Pose = to,
): Pose => {
	const { n, f } = settleLegs(to, was, u, toRef);
	const wasFlex = flexOf(was);
	const toFlex = flexOf(to);
	const at = (a: number, b: number, w: number) => a + (b - a) * w;
	// (His hips' bend over his feet goes with each leg as it steps, not
	// with both at once - which would slide the one still down.)
	return {
		...p,
		plant: 0,
		hipN: at(was.hipN + wasFlex, to.hipN + toFlex, n.on) + SETTLE_HIP * n.up,
		kneeN: at(was.kneeN, to.kneeN, n.on) + SETTLE_KNEE * n.up,
		toeN: at(was.toeN, to.toeN, n.on),
		wideN: at(was.wide + was.wideN, to.wide + to.wideN, n.on) - p.wide,
		hipF: at(was.hipF + wasFlex, to.hipF + toFlex, f.on) + SETTLE_HIP * f.up,
		kneeF: at(was.kneeF, to.kneeF, f.on) + SETTLE_KNEE * f.up,
		toeF: at(was.toeF, to.toeF, f.on),
		wideF: at(was.wide + was.wideF, to.wide + to.wideF, f.on) - p.wide,
	};
};

// Getting going hard he drives into it - leaning on into his first steps,
// his arms pumping - and pulling up hard he sits back against it. (His arms
// left be with the ball in them, or on it.)
const DRIVE_LEAN = 14;
const BRACE_LEAN = 9;
const DRIVE_PUMP = 0.45;
const DRIVE_ELBOW = 12;
const driven = (p: Pose, d: number, arms: boolean): Pose => {
	if (d < 0) {
		return { ...p, lean: p.lean + BRACE_LEAN * d };
	}
	if (!arms) {
		return { ...p, lean: p.lean + DRIVE_LEAN * d };
	}
	const mid = (p.shN + p.shF) / 2;
	const k = 1 + DRIVE_PUMP * d;
	return {
		...p,
		lean: p.lean + DRIVE_LEAN * d,
		shN: mid + (p.shN - mid) * k,
		shF: mid + (p.shF - mid) * k,
		elN: p.elN + DRIVE_ELBOW * d,
		elF: p.elF + DRIVE_ELBOW * d,
	};
};

// His pose: his move's, eased in from the last one's just after a change -
// and an arm in whatever it is saying.
export const poseOf = (st: PlayerState): Pose => {
	const own = posed(
		st.anim,
		st.phase,
		st.dribble,
		st.dribbleHand,
		st.target,
		st.dribbleFrom,
		st.tight,
	);
	const short = shortSteps(own, st.anim, st.stepLen, st.phase);
	const m = strode(st.mirror ? mirror(short) : short, st.stride);
	// The ball in his hands - not just now coming into them - his arms
	// carry it as the move does (and as the one before did, if he had it
	// then), so going from the one to the other they ease over too.
	const carry = st.holding === true && st.grip === undefined;
	const q0 = footDown(carry ? carried(m, st.anim) : m, st.down ?? 0);
	const q =
		PUSH_STEPS.has(st.anim) && st.way !== undefined ? slanted(q0, st.way) : q0;
	const f = st.from;
	const eased = f && (f.w > 0 || (f.arms ?? 0) > 0) ? blendFrom(q, f) : q;
	const fw = f && f.w > 0 ? f.w : 0;
	const was =
		fw > 0 && fw < 1 && st.z <= SETTLE_AIR && stepsOver(f!.anim, st.anim)
			? blendPose(f!)
			: undefined;
	const steps = was ? settleLegs(q, was, 1 - fw, standRef(st)) : undefined;
	let p = steps ? settleStep(eased, q, was!, 1 - fw, standRef(st)) : eased;
	if (st.drive) {
		p = driven(
			p,
			st.drive,
			st.dribble === undefined && !st.holding && st.grip === undefined,
		);
	}
	// His legs turned under him - and turning on the spot, a leg each (see
	// pivotOf). Easing out of the last move, each foot points where it
	// pointed on the floor as it changed - not swung round with him as he
	// turns - till it steps (or eases) round to where this one has it.
	const pv = st.pivot;
	const fp = fw > 0 ? f!.pivot : undefined;
	const turned = st.legs ?? 0;
	const fromAt = (i: 0 | 1) =>
		fw > 0 ? legTurnFrom(f!, i, st.yaw, st.anim) : 0;
	if (turned || pv || fp || fromAt(0) || fromAt(1)) {
		const legAt = (i: 0 | 1, on: number) => {
			const to = turned + (pv?.[i] ?? 0);
			return to + (fromAt(i) - to) * on;
		};
		const lift = (i: 2 | 3) =>
			(pv?.[i] ?? 0) + ((fp?.[i] ?? 0) - (pv?.[i] ?? 0)) * fw;
		p = {
			...p,
			legs: 0,
			spinN: legAt(0, steps ? 1 - steps.n.on : fw),
			spinF: legAt(1, steps ? 1 - steps.f.on : fw),
			hipN: p.hipN + PIVOT_HIP * lift(2),
			kneeN: p.kneeN + PIVOT_KNEE * lift(2),
			hipF: p.hipF + PIVOT_HIP * lift(3),
			kneeF: p.kneeF + PIVOT_KNEE * lift(3),
		};
	}
	const a = st.arm;
	if (!a || a.w <= 0) {
		return p;
	}
	const mix = (v: number, to: number) => v + (to - v) * a.w;
	const tuck = a.tuck === undefined ? p.tuck : mix(p.tuck, a.tuck);
	// (Off his hip, or out of his folded arms, as it does.)
	return a.hand === "R"
		? {
				...p,
				shN: mix(p.shN, a.sh),
				elN: mix(p.elN, a.el),
				abN: mix(p.abN, a.ab),
				wrN: mix(p.wrN, a.wr),
				onHipN: mix(p.onHipN, 0),
				foldN: mix(p.foldN, 0),
				onKneeN: mix(p.onKneeN, 0),
				flexN: mix(p.flexN, 0),
				tuck,
			}
		: {
				...p,
				shF: mix(p.shF, a.sh),
				elF: mix(p.elF, a.el),
				abF: mix(p.abF, a.ab),
				wrF: mix(p.wrF, a.wr),
				onHipF: mix(p.onHipF, 0),
				foldF: mix(p.foldF, 0),
				onKneeF: mix(p.onKneeF, 0),
				flexF: mix(p.flexF, 0),
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

// The ball in his hands and his arms on it, the way his move holds it - and,
// easing out of a move that held it another way, into one that has it all
// in one hand from the start (cupped for a windmill out of a rebound,
// pushed out for a pass, pulled round in a spin), where that one had it as
// it changed, as much as his arms are still in it: it goes over from the
// one hold to the other, not across in a frame. (Up on a jumper or a
// finish, the hand takes it over as it comes up anyway - see holdBall.)
// Whatever draws him and whatever puts the ball in his hands both ask
// this, so the two agree.
const ONE_HAND = new Set<Grip>(["push", "pull", "cup"]);
export const holdOf = (
	st: PlayerState,
	body: Body,
	q: Pose,
): { sk: Skeleton; ball: V3 } => {
	const carry = st.grip !== undefined;
	const now = holdBall(body, q, st.anim, carry, st.mirror === true);
	const f = st.from;
	const w = f ? (f.arms ?? f.w) : 0;
	const a = f ? gripOf(f.anim) : undefined;
	const b = gripOf(st.anim);
	if (
		!f ||
		w <= 0 ||
		!f.held ||
		!(ONE_HAND.has(a!) || ONE_HAND.has(b)) ||
		(a === b && (f.mirror === true) === (st.mirror === true))
	) {
		return now;
	}
	const was = holdBall(body, blendPose(f), f.anim, false, f.mirror === true);
	return {
		sk: {
			...now.sk,
			armR: mixLimb(now.sk.armR, was.sk.armR, w),
			armL: mixLimb(now.sk.armL, was.sk.armL, w),
		},
		ball: {
			f: now.ball.f + (was.ball.f - now.ball.f) * w,
			s: now.ball.s + (was.ball.s - now.ball.s) * w,
			u: now.ball.u + (was.ball.u - now.ball.u) * w,
		},
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
	return bodyPoint(st, holdOf(st, body, poseOf(st)).ball);
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
const HANDOVER_FLOOR_MS = 40;
const HANDOVER_REACH = 6;
const BEFORE = 0.5;

// A move's ball through its bounce, in his frame: in the hand it leaves
// (`under` - the ball under a hand at a point of the bounce) until it lets
// go, then straight down to the floor, falling faster, then up off it into
// the other hand, slowing as it gets there.
export const moveBallAt = (
	ph: number,
	letGo: number,
	down: number,
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
	if (ph < down) {
		return mix(
			under(letGo, from),
			floor,
			((ph - letGo) / (down - letGo)) ** 1.35,
		);
	}
	return mix(under(ph, to), floor, (1 - (ph - down) / (1 - down)) ** 1.8);
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
			const ball = bodyPoint(
				{ ...me, z: 0 },
				moveBallAt(
					ph,
					MOVE_BALL[move].letGo,
					MOVE_BALL[move].down,
					{ ...moveFloor(body, move, to, still), u: BALL_R },
					under,
					from,
					to,
				),
			);
			// Straight out of both hands, the first bounce goes down from
			// where he had it - carried along with him - not over into the one
			// hand in a frame.
			const down = MOVE_BALL[move].down;
			if (
				prev?.kind === "hold" &&
				prev.pid === seg.pid &&
				prev.style === "hold" &&
				t - seg.t0 < CROSS_MS &&
				ph < down
			) {
				const was = left();
				const then = evalPlayer(tl, seg.pid, seg.t0 - BEFORE);
				const e = ease(ph / down);
				const x = was.x + st.x - then.x;
				const y = was.y + st.y - then.y;
				return {
					x: x + (ball.x - x) * e,
					y: y + (ball.y - y) * e,
					z: was.z + (ball.z - was.z) * e,
					holder: seg.pid,
				};
			}
			return { ...ball, holder: seg.pid };
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
							const held = {
								x: was.x + st.x - then.x,
								y: was.y + st.y - then.y,
								z: was.z,
							};
							// (His hand on it as his arms come down off holding
							// it: pushed down out of where he had it, not let
							// drop away from under his hands.)
							const on = st.from && st.from.w > 0 ? st.from.w : 0;
							if (on >= 1) {
								return held;
							}
							const p = underHand(st, body, hand);
							return {
								x: p.x + (held.x - p.x) * on,
								y: p.y + (held.y - p.y) * on,
								z: p.z + (held.z - p.z) * on,
							};
						})()
					: underHand(st, body, hand);
			const tri = dribbleDepth(ph);
			// It hits the floor beside him, out past the foot on that side -
			// out ahead of him on the move - or, changing hands, between his
			// feet.
			const across = from !== to;
			// (Easing from one dribble into another - setting off from where he
			// stood, say - the spot goes out ahead of him as his hand does,
			// not a foot on in a frame.)
			const was = st.from && st.from.w > 0 ? st.from : undefined;
			const ahead =
				(was
					? dribbleAhead(st.anim) +
						(dribbleAhead(was.anim) - dribbleAhead(st.anim)) * was.w
					: dribbleAhead(st.anim)) *
				(1 - (st.tight ?? 0));
			// (Worked in toward the middle of him on an in-and-out.)
			const inward = dribbleIn(st.anim, st.phase);
			const out = (1.22 - 0.12 * ahead) * (1 - IN_BY * inward);
			const floor = bodyPoint(
				{ ...st, z: 0 },
				{
					f: across ? 1.0 : 0.95 + ahead + IN_AHEAD * inward,
					s: across ? 0 : hand === "L" ? out : -out,
					u: BALL_R,
				},
			);
			// On the move up or down the floor it turns over once a bounce,
			// pushed on ahead from its top - and so comes back up to his hand
			// the way it left it; standing, it barely turns at all.
			const top = evalPlayer(tl, seg.pid, t - ph * DRIBBLE_MS);
			const c = Math.cos(top.yaw);
			const roll =
				dribbleAhead(top.anim) === 1 && Math.abs(c) > 0.5
					? Math.PI * 2 * ph * Math.sign(c)
					: 0;
			return {
				x: h.x + (floor.x - h.x) * tri,
				y: h.y + (floor.y - h.y) * tri,
				z: h.z + (floor.z - h.z) * tri,
				holder: seg.pid,
				...(roll !== 0 ? { roll } : {}),
			};
		}
		const held: BallState = { ...heldBall(st, body), holder: seg.pid };
		// Taken into both hands - up off his dribble, off the floor, out of
		// another move - it comes from where it was. (Not when the picture
		// cut to him with it: that is no hand-over.)
		// (Off the floor, quicker: his hands come down to it where it lies.)
		const u =
			(t - seg.t0) / (prev?.kind === "rest" ? HANDOVER_FLOOR_MS : HANDOVER_MS);
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
			// (Caught, it has turned whole times over, so it comes into his
			// hands as it left the passer's; into a play at the rim, as the play
			// takes it on.)
			roll:
				-Math.PI *
				2 *
				("pid" in seg.to && T > 0
					? (Math.max(1, Math.round(BACKSPIN * T)) * tau) / T
					: BACKSPIN * tau) *
				(Math.sign(b.x - a.x) || 1),
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
		const span = Math.max(0.001, ((seg.t1 - seg.t0) / 1000) * 0.85);
		// With less time than its hops take, it hops lower - it never falls
		// faster than it would (only, with no time for even the drop, is the
		// whole of it hurried).
		const dropT = drop > 0.005 ? fall(drop) : 0;
		let hopsT = 0;
		for (let i = 0; i < seg.hops; i++) {
			hopsT += 2 * fall(seg.h0 * 0.42 ** i);
		}
		const room = span - dropT;
		const low =
			dropT + hopsT <= span || hopsT <= 0
				? 1
				: room > 0.05
					? (room / hopsT) ** 2
					: 1;
		const h0 = seg.h0 * low;
		const hops = drop > 0.005 ? [dropT] : [];
		const first = hops.length;
		for (let i = 0; i < seg.hops; i++) {
			hops.push(2 * fall(h0 * 0.42 ** i));
		}
		const real = hops.reduce((s, h) => s + h, 0);
		let w = (u * (seg.t1 - seg.t0) * Math.max(1, real / span)) / 1000;
		let z = 0;
		for (let i = 0; i < hops.length; i++) {
			if (w <= hops[i]!) {
				const v = w / hops[i]!;
				z =
					i < first
						? drop * (1 - v * v)
						: 4 * h0 * 0.42 ** (i - first) * v * (1 - v);
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
