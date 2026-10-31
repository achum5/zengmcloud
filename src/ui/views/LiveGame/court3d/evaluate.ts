import type {
	Act,
	ArenaShot,
	BallEnd,
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
import { alongShape, keepThrough, runShape, type RunShape } from "./motion.ts";
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
	runStyleOf,
	type RunStyle,
	shotWith,
	styledRun,
	styledStance,
	skeleton,
	SLIDE_NARROW,
	standingReach,
	stepBeat,
	stepLength,
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
	// How much room there is in front of him (feet), with anybody that near
	// the way he faces: he leans in no further than that leaves (see
	// leanRoom).
	room?: number;
	// How far each arm (right, left) is drawn in toward his chest, off a man
	// it would be in (0 to 1: see tuckAt).
	armsIn?: [number, number];
	// How low he keeps his dribble, his own way (-1 to 1: see dribbleLow).
	low?: number;
	// How easy he is about it (0 to 1): nobody near him, way out from the
	// rim, walking it up - stood up tall, the ball up round his waist, the
	// other arm loose (see calmAt).
	calm?: number;
	// His own way of dribbling it (0 to 1, the same every time: see easy in
	// poses.ts) - how far down in his stance, his other arm, how high.
	flair?: number;
	// Doing something with his arms on the run - a catch, a pass, a hand at
	// the ball: the stride his legs keep on with under it (see STRIDE_ACTS).
	stride?: Stride;
	// His hands up as a target for a pass on its way to him (0 to 1).
	target?: number;
	// A ball not in his hands but near them - in the air, loose, or in
	// another man's - from where he stands (feet; up, from his feet as he
	// is off the floor): his hands meet it (see sculpt.ts) - flat on it,
	// `held` in another man's hands or on his dribble.
	ballBy?: { x: number; y: number; z: number; held?: boolean };
	// The ball in his hands: how far round from the way he faces it faces
	// (radians; see facesTo).
	ballFace?: number;
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
	// On his strides, each foot's own turn on top of that (degrees, right,
	// left - see Pose's footN): down, it points where it came down pointing
	// (see footLock).
	lock?: [number, number];
	// Stepping over out of the last move, how far he has gone since it
	// changed, in his own frame (feet ahead, to his side): a foot still down
	// stays where it was on the floor (see SLIP_MOST).
	slip?: [number, number];
	// His head turned from the way he faces, to keep the ball in sight
	// (degrees, the way his yaw turns - see lookAt).
	look?: number;
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
	low?: number;
	calm?: number;
	flair?: number;
	stride?: Stride;
	target?: number;
	mirror?: boolean;
	w: number;
	arms?: number;
	// Turning where he stood as it changed (see pivot); each leg's turn under
	// him as it showed then (see legs), right, left; and the way he faced.
	pivot?: [number, number, number, number];
	turns?: [number, number];
	// And each foot's own turn then (see lock), and how hard he was going at
	// it (see drive).
	feet?: [number, number];
	drive?: number;
	facing?: number;
	down?: number;
	// Shuffling then, the way he went (see way).
	way?: [number, number];
	// Changed again while still easing out of the move before: that one,
	// and how much of it was still in him then.
	from?: Blend;
	// Stepped out of it, a foot at a time, rather than eased (see
	// startsOff) - or, easing between two gaits by how quick he goes (an
	// official's walk into his run), never stepped: his legs go with his
	// pace, back and forth with it.
	step?: true;
	paced?: true;
	// Where he was as it changed.
	spot?: [number, number];
	// Itself still stepping over out of the move before, how far he had gone
	// since that one changed, in his own frame then (see slipOf).
	slip?: [number, number];
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

// Bounces a second on a dribble, unless a piece of it says otherwise (see
// beatsAt): one steady beat through it, walking or driving, so the ball never
// skips a bounce when he starts or stops.
export const DRIBBLE_RATE = 2.1;
// A crossover's bounces, quicker: low and hand to hand.
export const CROSS_RATE = 3.2;
const otherHand = (h: Hand): Hand => (h === "R" ? "L" : "R");

// Through a crossover: which bounce, how far through it, and the hand it
// left - each bounce the other hand from the one before.
// (Each its own pace, if it says: see keepDribbling in director.ts.)
const crossMs = (seg: { rate?: number }) => 1000 / (seg.rate ?? CROSS_RATE);
const crossAt = (
	seg: { t0: number; hand?: Hand; rate?: number },
	t: number,
) => {
	const b = (t - seg.t0) / crossMs(seg);
	const k = Math.floor(b);
	const from = k % 2 ? otherHand(seg.hand ?? "R") : (seg.hand ?? "R");
	return { ph: b - k, from, to: otherHand(from) };
};
// A bounce of a dribble move done as the move it is - between his legs,
// behind his back - only if he stands through the whole of it. On the move,
// any of them is a crossover in front, his legs running, the ball pushed
// further out ahead of him.
const crossMoveAt = (
	tl: CourtTimeline,
	seg: {
		t0: number;
		pid: number;
		hand?: Hand;
		move?: DribbleMove;
		rate?: number;
	},
	t: number,
) => {
	const c = crossAt(seg, t);
	const tr = tl.tracks.get(seg.pid);
	const CROSS_MS = crossMs(seg);
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
	// How far through this bounce (0 the ball at the top, in his hand), and
	// when it was at the top.
	ph: number;
	top: number;
	// The hand it left, and the hand it comes up to.
	from: Hand;
	to: Hand;
	// The first bounce out of his hands, from holding it.
	first: boolean;
	// Where in the ball's path his dribble starts, and when.
	start: number;
	origin: number;
};
// HIS DRIBBLE'S BEAT. Each piece of a dribble has its own bounces a second -
// stood or walking it up easy, slower; driving or with his man up on him,
// quicker (see dribbleRate in director.ts) - but one man's dribbles back to
// back keep one beat: the next piece takes over at the beat the last had
// got to, not starting the bounce over. The director lays out his moves on
// the beat this keeps, and what is drawn follows it.
export const rateOf = (s: BallSeg | undefined): number =>
	(s?.kind === "hold" && s.rate) || DRIBBLE_RATE;
const dribblingBy = (s: BallSeg | undefined, pid: number): boolean =>
	s?.kind === "hold" && s.pid === pid && s.style === "dribble";
// The first piece of the run of his dribble the piece at `i` is in.
export const dribbleStart = (ball: readonly BallSeg[], i: number): number => {
	const s = ball[i];
	if (s?.kind !== "hold") {
		return i;
	}
	let j = i;
	while (j > 0 && dribblingBy(ball[j - 1], s.pid)) {
		j--;
	}
	return j;
};
// The pieces of the beat of the run of his dribble beginning at `start`:
// from when (t), how many bounces in (n), how many a second (r). A piece of
// his dribble at another rate takes over at the next top of the bounce it
// comes in on - not quicker or slower through the rest of that one - and
// two of them in one bounce, the later.
type BeatPiece = { t: number; n: number; r: number };
const beatPieces = (ball: readonly BallSeg[], start: number): BeatPiece[] => {
	const first = ball[start];
	if (first?.kind !== "hold") {
		return [{ t: 0, n: 0, r: DRIBBLE_RATE }];
	}
	const out: BeatPiece[] = [{ t: first.t0, n: 0, r: rateOf(first) }];
	for (let j = start + 1; j < ball.length; j++) {
		const s = ball[j]!;
		if (!dribblingBy(s, first.pid)) {
			break;
		}
		const r = rateOf(s);
		const last = out.at(-1)!;
		if (s.t0 < last.t) {
			last.r = r;
			continue;
		}
		if (r === last.r) {
			continue;
		}
		const k = Math.ceil(last.n + ((s.t0 - last.t) / 1000) * last.r - 1e-6);
		out.push({ t: last.t + ((k - last.n) / last.r) * 1000, n: k, r });
	}
	return out;
};
// (Worked out once a run, for the timeline drawn.)
const pieceMemo = new WeakMap<object, BeatPiece[]>();
const piecesOf = (ball: readonly BallSeg[], start: number): BeatPiece[] => {
	const key = ball[start];
	if (!key) {
		return beatPieces(ball, start);
	}
	let p = pieceMemo.get(key);
	if (!p) {
		p = beatPieces(ball, start);
		pieceMemo.set(key, p);
	}
	return p;
};
const nAt = (pieces: BeatPiece[], t: number): number => {
	let p = pieces[0]!;
	for (const q of pieces) {
		if (q.t > t) {
			break;
		}
		p = q;
	}
	return p.n + ((t - p.t) / 1000) * p.r;
};
const tAt = (pieces: BeatPiece[], n: number): number => {
	let p = pieces[0]!;
	for (const q of pieces) {
		if (q.n > n) {
			break;
		}
		p = q;
	}
	return p.t + ((n - p.n) / p.r) * 1000;
};
// How many bounces into the run of his dribble beginning at `start` he is
// at t (on at its last rate past the end of it) - and when he is `n` in.
// (The ball's pieces as they are now: laid out still, nothing kept.)
export const beatsAt = (
	ball: readonly BallSeg[],
	start: number,
	t: number,
): number => nAt(beatPieces(ball, start), t);
export const beatTime = (
	ball: readonly BallSeg[],
	start: number,
	n: number,
): number => tAt(beatPieces(ball, start), n);
const bounceOf = (tl: CourtTimeline, i: number, t: number): Bounce => {
	const seg = tl.ball[i]!;
	const pid = seg.kind === "hold" ? seg.pid : -1;
	const j = dribbleStart(tl.ball, i);
	const origin = tl.ball[j]!.t0;
	const beat = piecesOf(tl.ball, j);
	const b = nAt(beat, Math.max(origin, t)) + 1e-9;
	const k = Math.floor(b);
	const top = tAt(beat, k);
	const next = tAt(beat, k + 1);
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
		handAt(k > 0 ? tAt(beat, k - 1) : top - (next - top)) ??
		(seg.kind === "hold" ? seg.hand : "R") ??
		"R";
	const before = j > 0 ? tl.ball[j - 1] : undefined;
	return {
		ph: Math.min(1, b - k),
		top,
		from,
		to: handAt(next) ?? from,
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

// Still up off the floor from what he was doing (a contest, say) as the
// next thing starts (going up for the board): as high as that jump has him
// till it would have had him down, this one's taking him on from there - not
// dropped to the floor in a frame to go up again. (Only a jump he was up in
// as this one started - not one of its own still to come.)
const stillUp = (tr: Track, act: Act, t: number): number => {
	let up = 0;
	const ai = lastIndex(tr.acts, act.t0, (a) => a.t0);
	for (let k = ai; k >= 0 && k >= ai - 4; k--) {
		const a = tr.acts[k]!;
		if (a === act || a.flash || t >= a.t1 || (!a.jump && !a.zKeys)) {
			continue;
		}
		const span = a.t1 - a.t0;
		if (jumpZ(a, (act.t0 - a.t0) / span) > 0.05) {
			up = Math.max(up, jumpZ(a, (t - a.t0) / span));
		}
	}
	return up;
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
type Run = {
	mv: Move;
	s0: number;
	s1: number;
	v0: number;
	v1: number;
	// (Pulled up to stand, not straight on into another run.)
	stands?: true;
};
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
		...(next === undefined || next.t0 - mv.t1 > 1 ? { stands: true } : {}),
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
// A slide's push steps go round no quicker as he goes faster than
// REACH_FROM (feet a second): each one spreads his feet further instead, up
// to REACH_MOST times as far. (His stride's that long then - its "length",
// though more than a full one.)
const REACH_FROM = 6.5;
const REACH_MOST = 1.45;
const reachOf = (v: number) =>
	Math.min(REACH_MOST, Math.max(1, v / REACH_FROM));
// (And goes over to a new spread no quicker than this - so many full
// strides' worth a second: going on into a run at another pace, his feet
// spread wider or closer over a step, not in a frame.)
const REACH_RATE = 2;
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
		} else if (anim === "shuffle" && took === "shuffle") {
			R = { len: was.legR.len };
			L = { ...R };
		}
	}
	if (anim === "shuffle" && R.from === -1) {
		R = { len: reachOf(paceOn(run, run.s0)) };
		L = { ...R };
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
		if (anim === "shuffle") {
			// (Through the push, the foot he pushes off stays where it is as
			// his feet spread further, and the other through the close - so
			// his strides go round that much slower or quicker for it.)
			const most = (REACH_RATE * dt) / 1000;
			const reach = R.len + Math.max(-most, Math.min(most, reachOf(v) - R.len));
			const f = frac(ph);
			const spread = f < 0.5 ? 2 * f : 2 - 2 * f;
			const grew = (reach - R.len) * (f < 0.5 ? 1 : -1);
			ph += Math.max(
				0,
				((v / (S * reach)) * dt) / 1000 - (grew * spread) / (2 * reach),
			);
			R = { len: reach };
			L = R;
		} else if (!was) {
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
		if (t < a.t1 && !a.flash) {
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
const RUN_OPEN_MAX = (60 * Math.PI) / 180;
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
	// A defensive slide too quick to be one - opened up and running, back
	// in front of a drive, say - is turned the way he goes only as far
	// round off the ball as running lets him: his head and shoulders still
	// on it, not his back to the man coming at him.
	if (
		mv &&
		here.moving &&
		(mv.anim === "slide" || mv.anim === "shuffle") &&
		(gait === "run" || gait === "sprint")
	) {
		const heading =
			here.hx * here.hx + here.hy * here.hy > 1e-6
				? Math.atan2(here.hy, here.hx)
				: Math.atan2(mv.to.y - mv.from.y, mv.to.x - mv.from.x);
		const d = wrapAngle(angleTo(here, ballNear(tl, t), heading) - heading);
		return heading + Math.max(-RUN_OPEN_MAX, Math.min(RUN_OPEN_MAX, d));
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
		footfalls.delete(tr);
		footsAt.delete(tr);
		pushLeads.delete(tr);
		doings.delete(tr);
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
// (And still in them through what comes straight after, before anybody
// walks back out: the next period called, the subs made, another timeout.)
const STILL_HUDDLED = new Set([
	"period",
	"overtime",
	"sub",
	"timeout",
	"timeouts",
]);
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
	tl.beats.forEach((b, i) => {
		if (!HUDDLES.has(b.type) || b.end - b.actionStart < DEAD_MIN) {
			return;
		}
		let end = b.end;
		for (
			let k = i + 1;
			k < tl.beats.length && STILL_HUDDLED.has(tl.beats[k]!.type);
			k++
		) {
			end = Math.max(end, tl.beats[k]!.end);
		}
		out.push([b.actionStart, end, HUDDLE_LEAD]);
	});
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
	low?: number;
	calm?: number;
	flair?: number;
	stride?: Stride;
	target?: number;
	// Stepping over off a man (not a shuffle he set off on).
	aside?: true;
	// Pulling up, or stood a moment between runs: his foot up off the floor
	// come down (see footDown).
	down?: number;
	stepLen?: [number, number];
	drive?: number;
	// On the strides of one of his runs - his legs, or under whatever his
	// arms are doing, his stride's: which, and when on it (in a moment's
	// stand between it and the next, its end).
	gait?: { k: number; at: number };
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
	const short = len[0] !== 1 || len[1] !== 1;
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
	| {
			anim: AnimName;
			phase: number;
			down: number;
			len?: [number, number];
			gait: { k: number; at: number };
	  }
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
				gait: { k, at },
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
// EASY ABOUT IT. Nobody of theirs near him (feet: from the farther in, less
// so, and not at all by the nearer), way out from the rim (from the nearer
// out, more so, all of it by the farther - though some, alone, anywhere),
// walking it or stood (feet a second: up to the slower, all of it; none by
// the quicker): stood up tall bouncing it, the ball up round his waist, the
// other arm loose (0 to 1).
// (Each man's own way of dribbling it: see flair.)
const FLAIR_KEY = 4241;
const CALM_FAR = 10;
const CALM_NEAR = 3.5;
const CALM_RIM: [number, number] = [24, 36];
const CALM_ANYWHERE = 0.55;
const CALM_SLOW = 8;
const CALM_QUICK = 14;
const calmAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here: Spot,
): number => {
	let near = Infinity;
	for (const o of tl.tracks.values()) {
		if (o.team === tr.team) {
			continue;
		}
		const si = lastIndex(o.shown, t, (x) => x[0]);
		if (si < 0 || !o.shown[si]![1]) {
			continue;
		}
		const p = spotAt(o, t, false);
		near = Math.min(near, Math.hypot(p.x - here.x, p.y - here.y));
	}
	const alone = clamp01((near - CALM_NEAR) / (CALM_FAR - CALM_NEAR));
	if (alone <= 0) {
		return 0;
	}
	const out = clamp01(
		(Math.hypot(rimX(tr.team) - here.x, COURT_H / 2 - here.y) - CALM_RIM[0]) /
			(CALM_RIM[1] - CALM_RIM[0]),
	);
	const slow = clamp01(
		(CALM_QUICK - speedAt(tr, t)) / (CALM_QUICK - CALM_SLOW),
	);
	return alone * slow * (CALM_ANYWHERE + (1 - CALM_ANYWHERE) * out);
};
// How long his hands take to close on the ball once it is his.
const GRIP_MS = 110;
const GRIP_FLOOR_MS = 50;
// TAKEN INTO HIS HANDS - up off his dribble, off the floor, out of a move -
// the ball comes from where it was into where he holds it as his hands close
// on it, the two together (see closedOn): if the hold at `bi` is that (or
// held on straight from one that is), the first of those holds and when the
// hand-over is done. Meanwhile it is drawn on its own, where it is on its way
// - not in his hands already (see scene.ts), jumped there in a frame.
const takenOver = (
	tl: CourtTimeline,
	bi: number,
): { k: number; until: number } | undefined => {
	const seg = tl.ball[bi];
	if (seg?.kind !== "hold" || seg.style !== "hold") {
		return undefined;
	}
	let k = bi;
	for (; k > 0; k--) {
		const x = tl.ball[k - 1]!;
		if (x.kind !== "hold" || x.pid !== seg.pid || x.style !== "hold") {
			break;
		}
	}
	const prev = tl.ball[k - 1];
	const ms =
		prev?.kind === "rest"
			? GRIP_FLOOR_MS
			: (prev?.kind === "hold" && prev.pid === seg.pid) ||
				  prev?.kind === "bounce" ||
				  prev?.kind === "path"
				? GRIP_MS
				: undefined;
	return ms === undefined ? undefined : { k, until: tl.ball[k]!.t0 + ms };
};
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
// WHAT HE IS DOING AT A MOMENT, WORKED OUT ONCE. Easing from one move into
// the next (see blendInto) looks back at what he was doing a moment ago -
// at the same moments of the game, whatever the frame - so the frames on
// either side of this one ask after the same moments again: kept, the last
// few hundred moments a man, for the timeline they were worked out on.
const DOING_KEEP = 512;
const doings = new WeakMap<
	Track,
	{ tl: CourtTimeline; at: Map<number, Doing> }
>();
const doingAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here?: Spot,
): Doing => {
	if (here !== undefined) {
		return doingNow(tl, tr, t, here);
	}
	let kept = doings.get(tr);
	if (kept?.tl !== tl) {
		kept = { tl, at: new Map() };
		doings.set(tr, kept);
	}
	let d = kept.at.get(t);
	if (d === undefined) {
		d = doingNow(tl, tr, t, spotAt(tr, t));
		// (The longest kept is the first to go: no frame has to work them
		// all out again at once.)
		if (kept.at.size >= DOING_KEEP) {
			kept.at.delete(kept.at.keys().next().value!);
		}
		kept.at.set(t, d);
	}
	return d;
};
const doingNow = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	here: Spot,
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
	let gait: Doing["gait"];
	let down = 0;
	let stepLen: [number, number] | undefined;
	let drive = 0;
	if (act) {
		mirrored = act.mirror === true;
		const u = (t - act.t0) / (act.t1 - act.t0);
		// (A jumper, each man his own way of it.)
		anim =
			act.anim === "shoot"
				? act.push
					? "shootPush"
					: act.lean
						? act.lean === "L"
							? "shootLeanL"
							: "shootLeanR"
						: shotWith(pid, act.arms)
				: act.anim;
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
		z = Math.max(jumpZ(act, u), stillUp(tr, act, t));
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
		gait = { k: here.moveIndex, at: t };
		z = liftOf(anim, phase, stepLen);
		drive = driveOf(tr, anim, t);
		const k = here.moveIndex;
		down = footDownAt(
			here.run,
			t,
			k > 0 ? runOf(tr, k - 1) : undefined,
			k + 1 < tr.moves.length ? runOf(tr, k + 1) : undefined,
		);
	} else if ((bridged = bridgeAt(tl, tr, t))) {
		({ anim, phase, down, len: stepLen, gait } = bridged);
		drive = driveOf(tr, anim, t);
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
			gait = { k: here.moveIndex, at: t };
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
	const low = has?.style === "dribble" ? tl.dribbleLow?.get(tr.pid) : undefined;
	const easy = has?.style === "dribble" ? calmAt(tl, tr, t, here) : 0;
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
		holding:
			has?.style === "hold" && !(t < (takenOver(tl, bi)?.until ?? -Infinity)),
		...(grip === undefined ? {} : { grip }),
		dribble: beat?.ph,
		dribbleHand: beat ? (beat.ph < DOWN ? beat.from : beat.to) : undefined,
		...(beat && beat.from !== beat.to ? { dribbleFrom: beat.from } : {}),
		...(tight > 0 ? { tight } : {}),
		...(easy > 0 ? { calm: easy } : {}),
		...(has?.style === "dribble" ? { flair: hash01(tr.pid, FLAIR_KEY) } : {}),
		...(low ? { low } : {}),
		...(stride ? { stride } : {}),
		...(down > 0 ? { down } : {}),
		...(stepLen ? { stepLen } : {}),
		...(drive !== 0 ? { drive } : {}),
		...(gait ? { gait } : {}),
		target: act || has ? undefined : targetAt(tl, pid, t) || undefined,
	};
};

// HOW HARD HE IS GOING AT IT on his strides, getting going or pulling up
// (see PlayerState's drive): as hard as he has sped up or slowed down over
// the last DRIVE_MS, against all he has (feet a second, a second) - his body
// leaning into it a moment behind, not flicked from the one way to the
// other as one run he keeps on changing hands to the next, or planted
// stood up in a frame - on his feet going forward, not sliding or
// backpedalling, and nothing to speak of left out.
const DRIVE_FULL = 28;
const DRIVE_MIN = 0.08;
const DRIVE_MS = 200;
// (Taken over a few windows, each this much further back: a run set off at
// full pace - no getting going to it - leans him into it over a tenth of a
// second, not in a frame.)
const DRIVE_LAGS = [0, 60, 120];
const DRIVES = new Set<AnimName>(["run", "sprint", "jog", "dribble"]);
const paceAt = (tr: Track, t: number): number => {
	const s = spotAt(tr, t);
	return s.moving && s.run ? paceOn(s.run, t) : 0;
};
const driveOf = (tr: Track, anim: AnimName, t: number): number => {
	if (!DRIVES.has(anim)) {
		return 0;
	}
	const d =
		DRIVE_LAGS.reduce(
			(sum, lag) =>
				sum +
				Math.max(
					-1,
					Math.min(
						1,
						((paceAt(tr, t - lag) - paceAt(tr, t - lag - DRIVE_MS)) * 1000) /
							DRIVE_MS /
							DRIVE_FULL,
					),
				),
			0,
		) / DRIVE_LAGS.length;
	return (
		(Math.sign(d) * Math.max(0, Math.abs(d) - DRIVE_MIN)) / (1 - DRIVE_MIN)
	);
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
// (Setting off from where he stands, a step each foot - see startsOff.)
const START_MS = 220;
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
		if (a.t1 > t || t - a.t1 >= ARMS_DOWN_MS || a.flash) {
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
	// (And the same move done over - a second contest straight off the
	// first: further through it a moment ago than now, it is a new one.)
	const once = ANIMS[now.anim].kind === "act";
	const same = (d: Doing) =>
		d.anim === now.anim &&
		(d.aside === true) === (now.aside === true) &&
		(d.dribble === undefined) === (now.dribble === undefined) &&
		d.stride?.anim === now.stride?.anim &&
		!(once && d.phase > now.phase + 0.02);
	const anim = now.anim;
	// (Back as far as the longest of these eases goes - stepping off, too.)
	let lo = t - Math.max(BLEND_MS, START_MS);
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
			if (
				a.t1 <= hi &&
				hi - a.t1 < SLIVER_MS &&
				ARMS_UP.has(a.anim) &&
				!a.flash
			) {
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
	// (Out of strides that had all but stopped into another way of
	// going - a jog pulled up and then a backpedal, say - he steps off
	// into it as from standing.)
	const stepOff =
		startsOff(before.anim, anim) ||
		(crossesOver(before.anim, anim) && paceAt(tr, lo) < STEP_OFF_PACE);
	const w = ease(
		stepOff
			? START_MS
			: !slowOver(before.anim, anim)
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
		legsAt(tl, tr, there, facing, before.anim, lo),
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
	const ownLock = ownPv ? undefined : footLock(tl, tr, lo, before, own, way);
	const slip = prior && prior.w > 0 ? slipOf(there, facing, prior) : undefined;
	// (How hard he was going at it as it changed: as it showed, itself
	// maybe easing out of the move before.)
	const drive =
		(before.drive ?? 0) +
		((prior?.drive ?? 0) - (before.drive ?? 0)) * (prior?.w ?? 0);
	const feet = ([0, 1] as const).map(
		(i) =>
			(ownLock?.[i] ?? 0) +
			((prior?.feet?.[i] ?? 0) - (ownLock?.[i] ?? 0)) * (prior?.w ?? 0),
	) as [number, number];
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
		...(stepOff ? { step: true as const } : {}),
		dribble: before.dribble,
		dribbleHand: before.dribbleHand,
		...(before.dribbleFrom ? { dribbleFrom: before.dribbleFrom } : {}),
		...(before.tight ? { tight: before.tight } : {}),
		...(before.low ? { low: before.low } : {}),
		...(before.calm ? { calm: before.calm } : {}),
		...(before.flair !== undefined ? { flair: before.flair } : {}),
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
		...(feet[0] || feet[1] ? { feet } : {}),
		...(drive ? { drive } : {}),
		...(way === undefined ? {} : { way }),
		facing,
		spot: [there.x, there.y],
		...(slip ? { slip } : {}),
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
				: actBack(before.anim) && PIVOTS.has(anim)
					? { arms: ease(BLEND_MS) }
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
// The picture running through it fast, no arm says anything - up for a
// frame or two, it would only flash: it goes as the picture speeds up, gone
// at this many times over, and comes back as it slows. (A gesture that
// leaves less than this long of itself (ms) on the screen is left out
// altogether.)
const ARM_FAST = 4;
const ARM_SEEN = 300;
const armSeen = new WeakMap<Gesture, boolean>();
const seenOnScreen = (tl: CourtTimeline, g: Gesture): boolean => {
	let seen = armSeen.get(g);
	if (seen === undefined) {
		let screen = 0;
		for (let t = g.t0; t < g.t1; t += 20) {
			const f = fastAt(tl, t);
			if (f < ARM_FAST) {
				screen += 20 / f;
			}
		}
		seen = screen >= ARM_SEEN;
		armSeen.set(g, seen);
	}
	return seen;
};
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
	// (Run through fast, the picture is no time for one - see ARM_FAST.)
	if (!seenOnScreen(tl, g)) {
		return undefined;
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
	if (g.kind === "bar") {
		// A forearm in a man's back: out in front of him, bent, at his chest.
		return { hand, sh: 64, el: 76, ab: Math.max(0, out), wr: 6, w };
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
	// The arm on his man's side, whichever side of him the lane is - out to
	// that side, or straight out in front of him for a lane there. (His man
	// right in front of him or behind him, which side that is is not clear:
	// no arm, so it never swaps from one to the other.)
	const toMan = wrapAngle(Math.atan2(man.y - here.y, man.x - here.x) - yaw);
	const side = toMan >= 0 ? 1 : -1;
	const manDeg = (Math.abs(toMan) * 180) / Math.PI;
	// (Down while he turns, or while it is not clear which man is his: it
	// changes sides only then.)
	const turning = Math.abs(wrapAngle(yaw - yawAt(tl, tr, t - 100)));
	const w =
		smooth01((7 - near) / 2.5) *
		smooth01((26 - far) / 4) *
		smooth01((far - 9) / 3) *
		smooth01((125 - deg) / 30) *
		smooth01((manDeg - 8) / 12) *
		smooth01((172 - manDeg) / 12) *
		smooth01((0.22 - turning) / 0.12) *
		smooth01((next - near) / 2) *
		smooth01((t - since) / 350) *
		smooth01((until - t) / 300);
	if (w <= 0.05) {
		return undefined;
	}
	return {
		hand: side > 0 ? "R" : "L",
		sh: 90,
		el: 12,
		ab: Math.min(85, Math.max(0, (bearing * side * 180) / Math.PI)),
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

// Whether a man is on the floor at t (see Track.shown) - all a picture of
// it needs to know of the men on the bench, before working out anything
// more of them.
export const shownAt = (tl: CourtTimeline, pid: number, t: number): boolean => {
	const tr = tl.tracks.get(pid);
	if (!tr) {
		return false;
	}
	const si = lastIndex(tr.shown, t, (s) => s[0]);
	return si >= 0 ? tr.shown[si]![1] : false;
};

// THE SAME MAN AT THE SAME MOMENT, WORKED OUT ONCE. A frame asks after the
// man with the ball again for where the ball is in his hands, the officials
// after whoever has it, and a pass in the air after the passer and the
// catcher at either end of it - the same moments every frame it flies. A
// timeline never changes once made (a new one comes whole), so what a man
// was doing at a moment is kept for the last few moments asked about.
const MEMO_MOMENTS = 8;
const memos = new WeakMap<
	CourtTimeline,
	Map<number, Map<number, PlayerState>>
>();
export const evalPlayer = (
	tl: CourtTimeline,
	pid: number,
	t: number,
): PlayerState => {
	let byT = memos.get(tl);
	if (!byT) {
		byT = new Map();
		memos.set(tl, byT);
	}
	let at = byT.get(t);
	if (!at) {
		at = new Map();
		byT.set(t, at);
		if (byT.size > MEMO_MOMENTS) {
			byT.delete(byT.keys().next().value!);
		}
	}
	let st = at.get(pid);
	if (!st) {
		st = playerAt(tl, pid, t);
		at.set(pid, st);
	}
	return st;
};

const playerAt = (
	tl: CourtTimeline,
	pid: number,
	t: number,
	// Without his arms drawn in off anybody (see tuckAt).
	bare = false,
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
	// off the floor in a frame, setting off from where he stood - and goes
	// as they do, out of his stride into a move of his: not all of him
	// down onto the floor in a frame, up into his layup off the run.)
	const lifted =
		from && from.w > 0 && isGait(from.anim)
			? liftOf(from.anim, from.phase, from.stepLen)
			: 0;
	const now =
		from && from.w > 0 && isGait(doing.anim)
			? { ...doing, z: doing.z + (lifted - doing.z) * from.w }
			: lifted > 0
				? { ...doing, z: doing.z + lifted * from!.w }
				: doing;
	const yaw = spinYawAt(tl, tr, t) ?? yawAt(tl, tr, t);
	const room = shown && !EMBRACES.has(now.anim) ? roomAt(tl, tr, t) : undefined;
	const armsIn =
		shown && !bare && TUCKS.has(now.anim) ? tuckAt(tl, tr, t) : undefined;
	const legs = flippedLegs(
		tl,
		tr,
		t,
		now.anim,
		legsAt(tl, tr, here, yaw, now.anim, t, from === undefined),
	);
	const way =
		PUSH_STEPS.has(now.anim) && here.moving ? wayOf(tr, yaw, t) : undefined;
	const pivot = shown ? pivotOf(tl, tr, yaw, now, t) : undefined;
	const lock =
		shown && !pivot ? footLock(tl, tr, t, now, legs, way) : undefined;
	const slip = from && from.w > 0 ? slipOf(here, yaw, from) : undefined;
	const look = shown
		? lookAt(tl, t, here, yaw, now, from) + passLook(tl, tr.pid, t, here, yaw)
		: 0;
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
	const free =
		(now.grip !== undefined ? 1 - now.grip : now.holding ? 0 : 1) *
		clamp01((ARM_FAST - fastAt(tl, t)) / (ARM_FAST - 1));
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
		...(lock ? { lock } : {}),
		...(slip ? { slip } : {}),
		...(look !== 0 ? { look } : {}),
		...(way === undefined ? {} : { way }),
		...(pivot ? { pivot } : {}),
		...(from ? { from } : {}),
		...(arm ? { arm } : {}),
		...(room === undefined ? {} : { room }),
		...(armsIn ? { armsIn } : {}),
	};
};

// KEPT OFF THE MAN IN FRONT OF HIM. Two men face to face - a man up on the
// man with the ball, both down in it - each leans in no further than leaves
// their heads apart: how much room there is in front of him, along the way
// he faces, to the nearest man (feet - more of it the further off to one
// side he is, past ROOM_SIDE, and the nearer beside him, inside
// ROOM_BESIDE), if any is nearer than ROOM_FAR. Worked out every ROOM_STEP,
// each the mean of it and those either side, and eased between: a man
// coming into it, or going by across it, and his lean goes over a quarter
// of a second, never in a frame.
// (As far out as a head bent right over goes - so it comes into it as
// gently as it goes on.)
const ROOM_FAR = 7.6;
const ROOM_SIDE = 0.9;
const ROOM_BESIDE = 0.8;
const ROOM_STEP = 80;
const roomNow = (tl: CourtTimeline, tr: Track, t: number): number => {
	const here = spotAt(tr, t, false);
	const yaw = spinYawAt(tl, tr, t) ?? yawAt(tl, tr, t);
	const fx = Math.cos(yaw);
	const fy = Math.sin(yaw);
	let most = ROOM_FAR;
	for (const o of tl.tracks.values()) {
		if (o === tr) {
			continue;
		}
		const si = lastIndex(o.shown, t, (x) => x[0]);
		if (si < 0 || !o.shown[si]![1]) {
			continue;
		}
		const p = spotAt(o, t, false);
		const dx = p.x - here.x;
		const dy = p.y - here.y;
		const along = dx * fx + dy * fy;
		if (along <= 0 || along >= most) {
			continue;
		}
		const side = Math.abs(dy * fx - dx * fy);
		most = Math.min(
			most,
			along +
				Math.max(0, side - ROOM_SIDE) * 3 +
				Math.max(0, ROOM_BESIDE - along) * 6,
		);
	}
	return most;
};
const roomSteps = new WeakMap<Track, Map<number, number>>();
const roomAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
): number | undefined => {
	let steps = roomSteps.get(tr);
	if (!steps) {
		steps = new Map();
		roomSteps.set(tr, steps);
	}
	const at = (g: number) => {
		let r = steps.get(g);
		if (r === undefined) {
			r = roomNow(tl, tr, g);
			steps.set(g, r);
			if (steps.size > 8) {
				steps.delete(steps.keys().next().value!);
			}
		}
		return r;
	};
	const mean = (g: number) =>
		(at(g - ROOM_STEP) + at(g) + at(g + ROOM_STEP)) / 3;
	const g = Math.floor(t / ROOM_STEP) * ROOM_STEP;
	const a = mean(g);
	const b = mean(g + ROOM_STEP);
	const room = a + (b - a) * ((t - g) / ROOM_STEP);
	return room < ROOM_FAR ? room : undefined;
};

// (Not two men meant to be up against each other: dapping up after the
// game, hugging - see dapHug - or one pulling the other up off the floor by
// the hands.)
const EMBRACES = new Set<AnimName>([
	"dap",
	"dapPat",
	"dapHug",
	"helpUp",
	"pulledUp",
]);
// His head no further out in front of him than half that room, less a head
// apart (in head radii) - his lean given up for it, down to upright at
// most, no more than LEAN_GIVE (degrees).
const HEADS_APART = 1.6;
const LEAN_GIVE = 22;
const leanRoom = (p: Pose, room: number | undefined): Pose => {
	if (room === undefined || p.lean <= 0) {
		return p;
	}
	const sk = skeleton(TYPICAL, p);
	const over = sk.head.f - (room / 2 - TYPICAL.headR * HEADS_APART);
	if (over <= 0) {
		return p;
	}
	const rad = Math.PI / 180;
	const spine = Math.hypot(sk.head.f - sk.pelvis.f, sk.head.u - sk.pelvis.u);
	const want = Math.asin(clamp01(Math.sin(p.lean * rad) - over / spine)) / rad;
	return { ...p, lean: Math.max(want, p.lean - LEAN_GIVE, 0) };
};
// Only his arms carried as he goes or stands - not a hand put on a man on
// purpose, nor one up at the ball.
const TUCKS = new Set<AnimName>([
	"run",
	"jog",
	"sprint",
	"walk",
	"shuffle",
	"slide",
	"back",
	"drift",
	"stance",
	"stanceHands",
	"guard",
	"ready",
	"easy",
	"crouch",
]);
// HIS HANDS KEPT OUT OF ANOTHER MAN. An arm of his that would be in a man
// next to him - out wide in his stance, swung as he runs by - is drawn in
// toward his own chest (TUCK), as far as keeps the hand off him: no nearer
// the line of the man's back, from his hips to his chest, than ARM_CLEAR
// (feet), nor his head than his head's size. Worked out every ROOM_STEP,
// each the mean of it and those either side, and eased between, as his room
// is (see roomAt): never in a frame.
const TUCK = { sh: 22, el: 105, ab: 14 };
const ARM_CLEAR = 0.55;
// Where a man's body is (his hips, chest and head), every ROOM_STEP, without
// his arms drawn in (they make no odds to it).
const bodySteps = new WeakMap<
	Track,
	Map<number, { hip: Pt3; chest: Pt3; head: Pt3 }>
>();
const bodyAtStep = (tl: CourtTimeline, tr: Track, t: number) => {
	let steps = bodySteps.get(tr);
	if (!steps) {
		steps = new Map();
		bodySteps.set(tr, steps);
	}
	let b = steps.get(t);
	if (!b) {
		const st = playerAt(tl, tr.pid, t, true);
		const sk = skeleton(TYPICAL, poseOf(st));
		b = {
			hip: bodyPoint(st, sk.pelvis),
			chest: bodyPoint(st, sk.chest),
			head: bodyPoint(st, sk.head),
		};
		steps.set(t, b);
		if (steps.size > 8) {
			steps.delete(steps.keys().next().value!);
		}
	}
	return b;
};
const toSegment = (p: Pt3, a: Pt3, b: Pt3): number => {
	const ab = { x: b.x - a.x, y: b.y - a.y, z: b.z - a.z };
	const L = ab.x * ab.x + ab.y * ab.y + ab.z * ab.z || 1;
	const u = clamp01(
		((p.x - a.x) * ab.x + (p.y - a.y) * ab.y + (p.z - a.z) * ab.z) / L,
	);
	return Math.hypot(
		p.x - a.x - ab.x * u,
		p.y - a.y - ab.y * u,
		p.z - a.z - ab.z * u,
	);
};
const tuckedArms = (p: Pose, w: readonly [number, number]): Pose => {
	if (w[0] <= 0 && w[1] <= 0) {
		return p;
	}
	const mix = (a: number, b: number, k: number) => a + (b - a) * k;
	return {
		...p,
		shN: mix(p.shN, TUCK.sh, w[0]),
		elN: mix(p.elN, TUCK.el, w[0]),
		abN: mix(p.abN, TUCK.ab, w[0]),
		shF: mix(p.shF, TUCK.sh, w[1]),
		elF: mix(p.elF, TUCK.el, w[1]),
		abF: mix(p.abF, TUCK.ab, w[1]),
	};
};
const NO_TUCK: readonly [number, number] = [0, 0];
const tuckNow = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
): readonly [number, number] => {
	const st = playerAt(tl, tr.pid, t, true);
	if (
		!st.shown ||
		st.holding ||
		st.grip !== undefined ||
		st.dribble !== undefined ||
		st.z > 0 ||
		!TUCKS.has(st.anim)
	) {
		return NO_TUCK;
	}
	const near: { hip: Pt3; chest: Pt3; head: Pt3 }[] = [];
	for (const o of tl.tracks.values()) {
		if (o === tr) {
			continue;
		}
		const si = lastIndex(o.shown, t, (x) => x[0]);
		if (si < 0 || !o.shown[si]![1]) {
			continue;
		}
		const p = spotAt(o, t, false);
		if (Math.hypot(p.x - st.x, p.y - st.y) < 4) {
			near.push(bodyAtStep(tl, o, t));
		}
	}
	if (near.length === 0) {
		return NO_TUCK;
	}
	const p0 = poseOf(st);
	const p1 = tuckedArms(p0, [1, 1]);
	const sk0 = skeleton(TYPICAL, p0);
	const sk1 = skeleton(TYPICAL, p1);
	// (How far the hand is into the nearest man's body: its room off the
	// line of his back past ARM_CLEAR, or off his head past its size - the
	// less of the two.)
	const headClear = TYPICAL.headR * 0.95;
	const clear = (arm: Limb): number => {
		const tip = arm.tip ?? arm.end;
		const h = bodyPoint(st, {
			f: (arm.end.f + tip.f) / 2,
			s: (arm.end.s + tip.s) / 2,
			u: (arm.end.u + tip.u) / 2,
		});
		return Math.min(
			...near.map((q) =>
				Math.min(
					toSegment(h, q.hip, q.chest) - ARM_CLEAR,
					Math.hypot(h.x - q.head.x, h.y - q.head.y, h.z - q.head.z) -
						headClear,
				),
			),
		);
	};
	const way = (a0: Limb, a1: Limb) => {
		const d0 = clear(a0);
		if (d0 >= 0) {
			return 0;
		}
		const d1 = clear(a1);
		return d1 > d0 + 0.02 ? clamp01(-d0 / (d1 - d0)) : 1;
	};
	return [way(sk0.armR, sk1.armR), way(sk0.armL, sk1.armL)];
};
const tuckSteps = new WeakMap<Track, Map<number, readonly [number, number]>>();
const tuckAt = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
): [number, number] | undefined => {
	let steps = tuckSteps.get(tr);
	if (!steps) {
		steps = new Map();
		tuckSteps.set(tr, steps);
	}
	const at = (g: number) => {
		let r = steps.get(g);
		if (r === undefined) {
			r = tuckNow(tl, tr, g);
			steps.set(g, r);
			if (steps.size > 8) {
				steps.delete(steps.keys().next().value!);
			}
		}
		return r;
	};
	const mean = (g: number, i: 0 | 1) =>
		(at(g - ROOM_STEP)[i] + at(g)[i] + at(g + ROOM_STEP)[i]) / 3;
	const g = Math.floor(t / ROOM_STEP) * ROOM_STEP;
	const u = (t - g) / ROOM_STEP;
	const w = ([0, 1] as const).map(
		(i) => mean(g, i) + (mean(g + ROOM_STEP, i) - mean(g, i)) * u,
	) as [number, number];
	return w[0] > 0.01 || w[1] > 0.01 ? w : undefined;
};

// WATCHING THE BALL. Off it - running, down in his stance, stood waiting
// for it - a man turns his head to keep the ball in sight: as far round as
// a neck goes (LOOK_MOST), and a moment behind it (LOOK_LAG), as people
// are; less and less the further round behind him it is, not at all
// straight behind. Not the man with it, not in a move with its own eyes on
// something (a shot, a pass, a catch), not in the huddle - and not while
// the picture runs through fast, his head whipped round with every pass.
const LOOK_MOST = 70;
const LOOK_LAG = 120;
const LOOK_BACK = 115;
// (The huddle's start and end, eased over so long - and where the ball is
// taken over the last LOOK_SPAN, every LOOK_EVERY: a head turned through
// a moment, not snapped round in a frame as the ball is gone on.)
const LOOK_EASE = 500;
const LOOK_SPAN = 200;
const LOOK_NEAR = 1.5;
const LOOK_NEAR_EASE = 4;
const LOOK_EVERY = 40;
const FREE_HEAD = new Set<AnimName>([
	"run",
	"sprint",
	"jog",
	"walk",
	"drift",
	"ready",
	"stance",
	"guard",
	"easy",
	"laneSet",
	"boxOut",
	"back",
	"slide",
	"shuffle",
	"sideStep",
	"closeout",
	"spotUp",
	"callBall",
	"clapCall",
	"stanceHands",
	"stancePoint",
	"hips",
	"crouch",
]);
const lookAt = (
	tl: CourtTimeline,
	t: number,
	here: Pt,
	yaw: number,
	now: Doing,
	from: Blend | undefined,
): number => {
	// (Easing out of one move, itself easing out of another: as free as
	// each of them, as much as each still shows.)
	const free = (anim: AnimName, held: boolean | undefined, hand?: Hand) =>
		FREE_HEAD.has(anim) && !held && hand === undefined ? 1 : 0;
	const freed = (f: Blend | undefined, own: number): number =>
		f
			? own * (1 - f.w) +
				freed(f.from, free(f.anim, f.held, f.dribbleHand)) * f.w
			: own;
	const w =
		freed(from, free(now.anim, now.holding, now.dribbleHand)) *
		(1 - huddledAt(tl, t)) *
		clamp01((ARM_FAST - fastAt(tl, t)) / (ARM_FAST - 1));
	return w > 0 ? lookToward(tl, t, here, yaw) * w : 0;
};
// EYES ON THE PASS. With the ball, a man's head comes round to the one he
// is about to pass it to - over the last PASS_LOOK ms before it goes, all
// the way by PASS_LOOK_SET before, and back once it is gone - as far as a
// neck goes; not with it on its way to him still, and not while the
// picture runs through fast.
const PASS_LOOK = 700;
const PASS_LOOK_SET = 250;
const PASS_LOOK_OFF = 250;
// By his own eyes for it (see passEyes): how much longer the poorest passer
// stares it down, how much later the finest sees it - and, a fine enough
// passer, at the most how many of his passes go without a look.
const STARE_MORE = 0.9;
const SEE_LATE = 0.45;
const NO_LOOK_FROM = 0.3;
const NO_LOOK_MOST = 0.3;
const passLook = (
	tl: CourtTimeline,
	pid: number,
	t: number,
	here: Pt,
	yaw: number,
): number => {
	const i = ballIndexAt(tl, t);
	const now = tl.ball[i];
	const fast = clamp01((ARM_FAST - fastAt(tl, t)) / (ARM_FAST - 1));
	if (!now || fast <= 0) {
		return 0;
	}
	// The pass he is about to make - or has just made, on its way or already
	// there. (Caught and passed straight on: his head comes round from the
	// moment it is in his hands, not before.)
	let pass: Extract<BallSeg, { kind: "fly" }> | undefined;
	let since = Infinity;
	if (now.kind === "hold" && now.pid === pid) {
		// (From when it came into his hands: dribbling it and then picking
		// it up is all the one having it.)
		let got = i;
		while (got > 0) {
			const b = tl.ball[got - 1]!;
			if (b.kind !== "hold" || b.pid !== pid) {
				break;
			}
			got -= 1;
		}
		since = t - tl.ball[got]!.t0;
		for (let k = i + 1; k < tl.ball.length && k <= i + 3; k++) {
			const s = tl.ball[k]!;
			if (s.t0 - t > PASS_LOOK) {
				break;
			}
			if (s.kind === "fly") {
				pass = s;
				break;
			}
		}
	} else {
		for (let k = i; k >= 0 && k >= i - 2; k--) {
			const s = tl.ball[k]!;
			if (t - s.t0 >= PASS_LOOK_OFF) {
				break;
			}
			if (s.kind === "fly" && "pid" in s.from && s.from.pid === pid) {
				pass = s;
				break;
			}
		}
	}
	if (
		!pass ||
		!("pid" in pass.from) ||
		pass.from.pid !== pid ||
		!("pid" in pass.to) ||
		pass.to.pid === pid
	) {
		return 0;
	}
	const to = tl.tracks.get(pass.to.pid);
	if (!to) {
		return 0;
	}
	// His own eyes for it (see passEyes): a poor passer stares it down long
	// before it goes; a fine one sees it late - and now and then throws it
	// without a look at all.
	const eyes = tl.passEyes?.get(pid) ?? 0;
	if (eyes > NO_LOOK_FROM && t < pass.t0) {
		const x = Math.sin(pid * 12.9898 + pass.t0 * 0.0137) * 43758.5453;
		if (
			x - Math.floor(x) <
			NO_LOOK_MOST * ((eyes - NO_LOOK_FROM) / (1 - NO_LOOK_FROM))
		) {
			return 0;
		}
	}
	const lookFor =
		PASS_LOOK * (eyes < 0 ? 1 - STARE_MORE * eyes : 1 - SEE_LATE * eyes);
	const set = Math.min(PASS_LOOK_SET, lookFor * 0.6);
	const w =
		t < pass.t0
			? Math.min(
					smooth01((lookFor - (pass.t0 - t)) / (lookFor - set)),
					smooth01(since / set),
				)
			: 1 - smooth01((t - pass.t0) / PASS_LOOK_OFF);
	const at = spotAt(to, Math.min(t, pass.t0));
	const rel =
		(wrapAngle(Math.atan2(at.y - here.y, at.x - here.x) - yaw) * 180) / Math.PI;
	const a = Math.abs(rel);
	const reach = a <= LOOK_BACK ? 1 : Math.max(0, (180 - a) / (180 - LOOK_BACK));
	return Math.sign(rel) * Math.min(a, LOOK_MOST) * reach * w * fast;
};
// Off the floor - sat on the bench, stood at it - the same: his head turned
// to follow the play, in front of him.
export const benchLook = (
	tl: CourtTimeline,
	t: number,
	at: Pt,
	yaw: number,
): number => {
	const w =
		(1 - huddledAt(tl, t)) *
		clamp01((ARM_FAST - fastAt(tl, t)) / (ARM_FAST - 1));
	return w > 0 ? lookToward(tl, t, at, yaw) * w : 0;
};
// His head's turn to the ball, as far as it goes (degrees).
const lookToward = (
	tl: CourtTimeline,
	t: number,
	here: Pt,
	yaw: number,
): number => {
	const b = ballBearing(tl, t, here);
	if (!b) {
		return 0;
	}
	const rel = (wrapAngle(b.yaw - yaw) * 180) / Math.PI;
	const a = Math.abs(rel);
	const reach = a <= LOOK_BACK ? 1 : Math.max(0, (180 - a) / (180 - LOOK_BACK));
	return Math.sign(rel) * Math.min(a, LOOK_MOST) * reach * b.near;
};
// Which way the ball is from `here` as he sees it - a moment behind it,
// over a moment (see LOOK_LAG): its way (radians), and how far off it is
// to look at (0, coming right by him, to 1).
export const ballBearing = (
	tl: CourtTimeline,
	t: number,
	here: Pt,
): { yaw: number; near: number } | undefined => {
	// (The way to it over that time, each moment's as much as the others':
	// between one step of LOOK_EVERY and the next, the one eased into the
	// other.)
	const end = t - LOOK_LAG;
	const g = Math.floor(end / LOOK_EVERY) * LOOK_EVERY;
	const f = (end - g) / LOOK_EVERY;
	let sx = 0;
	let sy = 0;
	let near = 0;
	const n = LOOK_SPAN / LOOK_EVERY;
	for (let i = -1; i < n; i++) {
		const b = ballSpot(tl, g - i * LOOK_EVERY);
		if (!b) {
			continue;
		}
		const k = i === -1 ? f : i === n - 1 ? 1 - f : 1;
		const d = Math.sqrt(dist2(here, b));
		if (d > 1e-6) {
			sx += ((b.x - here.x) / d) * k;
			sy += ((b.y - here.y) / d) * k;
		}
		// (Coming right by him, or in his hands: ahead - not swung round
		// after it as it goes by under his nose.)
		near += clamp01((d - LOOK_NEAR) / LOOK_NEAR_EASE) * k;
	}
	return sx * sx + sy * sy < 1e-9
		? undefined
		: { yaw: Math.atan2(sy, sx), near: near / n };
};
// Where the ball is on the floor at t, near enough to look at: with
// whoever has it, along its way to wherever it is going.
const ballSpot = (tl: CourtTimeline, t: number): Pt | undefined => {
	const seg = tl.ball[ballIndexAt(tl, t)];
	if (!seg) {
		return undefined;
	}
	const endAt = (e: BallEnd, when: number): Pt | undefined => {
		if (!("pid" in e)) {
			return e;
		}
		const tr = tl.tracks.get(e.pid);
		return tr ? spotAt(tr, when) : undefined;
	};
	if (seg.kind === "hold") {
		const tr = tl.tracks.get(seg.pid);
		return tr ? spotAt(tr, t) : undefined;
	}
	if (seg.kind === "rest") {
		return seg.at;
	}
	if (seg.kind === "path") {
		return playAt(seg.pts, t - seg.t0);
	}
	const u = seg.t1 > seg.t0 ? clamp01((t - seg.t0) / (seg.t1 - seg.t0)) : 1;
	const a = seg.kind === "fly" ? endAt(seg.from, seg.t0) : seg.from;
	const z = seg.kind === "fly" ? endAt(seg.to, seg.t1) : seg.to;
	return a && z ? { x: a.x + (z.x - a.x) * u, y: a.y + (z.y - a.y) * u } : a;
};
// How far into a huddle - a timeout, the break between periods, and on
// through the calls made in it - he is at t (0 to 1, eased in and out).
const huddles = new WeakMap<CourtTimeline, [number, number][]>();
const huddledAt = (tl: CourtTimeline, t: number): number => {
	let spans = huddles.get(tl);
	if (!spans) {
		spans = [];
		tl.beats.forEach((b, i) => {
			if (!HUDDLES.has(b.type)) {
				return;
			}
			let end = b.end;
			for (
				let k = i + 1;
				k < tl.beats.length && STILL_HUDDLED.has(tl.beats[k]!.type);
				k++
			) {
				end = Math.max(end, tl.beats[k]!.end);
			}
			spans!.push([b.actionStart, end]);
		});
		huddles.set(tl, spans);
	}
	let most = 0;
	for (const [a, b] of spans) {
		if (t > a && t < b) {
			most = Math.max(
				most,
				Math.min(1, (t - a) / LOOK_EASE, (b - t) / LOOK_EASE),
			);
		}
	}
	return most;
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
// (Of a step out that far: how much goes ahead or back, as the turn of his
// hip it takes, and how much less goes across.)
const slantOf = (way: [number, number], d: number) => {
	const [f, s] = way;
	const m = Math.hypot(f, s);
	const side = Math.abs(s) / m;
	const on = smooth01((side - 0.3) / 0.3) * Math.min(1, m);
	return {
		on,
		ahead: ((d * (f / m) * on) / SLANT_REACH) * (180 / Math.PI),
		across: d * (1 - side) * on,
	};
};
const slanted = (q: Pose, way: [number, number]): Pose => {
	const [f, s] = way;
	const m = Math.hypot(f, s);
	const d = q.wide - SLIDE_NARROW;
	if (m < 1e-3 || d <= 0) {
		return q;
	}
	const { on, ahead, across } = slantOf(way, d);
	if (on <= 0) {
		return q;
	}
	// (His left leads going to his left.)
	return {
		...q,
		wideN: q.wideN - across,
		wideF: q.wideF - across,
		hipN: q.hipN + (s > 0 ? -ahead : ahead),
		hipF: q.hipF + (s > 0 ? ahead : -ahead),
	};
};

// And the foot that moves comes up off the floor a little as it goes - the
// lead foot out through the push, the trail foot in through the close - not
// swept along it: bent more at the knee at the middle of its step, and at
// the hip with it, so it comes up rather than back. (Degrees more at the
// knee; at the hip, that share of it.) It gets going and slows to land, too
// - not out at twice his pace from the moment it leaves the floor to the
// moment it is down, both feet sliding as the one hands on to the other:
// the foot down goes back under him as ever, the one up eased over its
// step.
const SLIDE_LIFT = 14;
const LIFT_HIP = 0.45;
// How much his left leads, shuffling `way`: all of it going to his left,
// none going to his right - and going so nearly straight ahead or back that
// which foot leads is no way to tell, half and half, so as the way he goes
// comes round past straight ahead the lead goes over from the one foot to
// the other, not in a frame. (Out of how much of the way is across him.)
const LEAD_SURE = 0.35;
// (And barely going anywhere - setting off, pulling up - neither foot leads,
// rather than the one and then the other as the way he is barely going
// swings round: the lead comes in as he gets going, by this much way.)
const LEAD_PACE = 0.25;
const leftLeads = (way: [number, number]): number | undefined => {
	const m = Math.hypot(way[0], way[1]);
	if (m < 1e-6) {
		return undefined;
	}
	const lead = smooth01((way[1] / m + LEAD_SURE) / (2 * LEAD_SURE));
	return 0.5 + (lead - 0.5) * smooth01(m / LEAD_PACE);
};
const liftedStep = (q: Pose, way: [number, number], ph: number): Pose => {
	const [f, s] = way;
	const m = Math.hypot(f, s);
	if (m < 1e-3) {
		return q;
	}
	const at = frac(ph);
	const u = at < 0.5 ? 2 * at : 2 * at - 1;
	const k = SLIDE_LIFT * Math.sin(Math.PI * u) * Math.min(1, m);
	// (How far out it is, eased, beyond where it would be going evenly - its
	// feet's spread so far through the push, or left through the close.)
	const d = q.wide - SLIDE_NARROW;
	const even = at < 0.5 ? u : 1 - u;
	const eased = u * u * u * (u * (u * 6 - 15) + 10);
	const e =
		even > 1e-3 && d > 0
			? (d / even) * (at < 0.5 ? eased - u : u - eased) * Math.min(1, m)
			: 0;
	const { ahead, across } = slantOf(way, e);
	// (His left leads going to his left; pushing, the lead foot moves.)
	const left = leftLeads(way)!;
	const far = at < 0.5 ? left : 1 - left;
	const lead = s > 0 ? ahead : -ahead;
	return {
		...q,
		kneeN: q.kneeN + k * (1 - far),
		hipN: q.hipN + (k * LIFT_HIP - lead) * (1 - far),
		wideN: q.wideN + (e - across) * (1 - far),
		kneeF: q.kneeF + k * far,
		hipF: q.hipF + (k * LIFT_HIP + lead) * far,
		wideF: q.wideF + (e - across) * far,
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
	// the very end for his hips to follow. (Pulled up to stand, not at all:
	// both feet down, his legs coming round under him would swing them
	// round over the floor - they step round one and then the other as he
	// settles, the turn they had as he stopped eased out of a foot at a
	// time; see settleLegs.)
	// (And round a corner, the pace he goes round it at: turned right back
	// on himself, all but stood still at the turn, not swung round with
	// it.)
	const ease =
		(run.v0 > LEGS_FTPS ? 1 : smooth01((t - run.s0) / LEGS_EASE_MS)) *
		(run.v1 > LEGS_FTPS || run.stands
			? 1
			: smooth01((run.s1 - t) / LEGS_EASE_MS)) *
		(here.round === undefined ? 1 : smooth01(here.round / LEGS_FTPS));
	// (His left is the floor's clockwise.)
	return -Math.sign(off) * turn * ease;
};
// (Stood where a run he pulled up from left him, still in its strides, the
// turn it had them at as he stopped - till he settles out of them; or, off
// again on the same strides, eased out of it as he gets going.)
const legsAt = (
	tl: CourtTimeline,
	tr: Track,
	here: Spot,
	yaw: number,
	anim: AnimName,
	t: number,
	carried = false,
): number => {
	const k = here.moveIndex;
	const held = (run: Run) => {
		const end = run.s1 - 1;
		return legsOf(spotAt(tr, end), yawAt(tl, tr, end), anim, end);
	};
	if (!here.moving) {
		const run = k >= 0 ? runOf(tr, k) : undefined;
		return run?.stands && t >= run.s1 ? held(run) : legsOf(here, yaw, anim, t);
	}
	const own = legsOf(here, yaw, anim, t);
	const run = here.run;
	if (
		!carried ||
		!run ||
		k <= 0 ||
		run.v0 > LEGS_FTPS ||
		t - run.s0 >= LEGS_EASE_MS
	) {
		return own;
	}
	const before = runOf(tr, k - 1);
	return before.stands
		? own + held(before) * (1 - smooth01((t - run.s0) / LEGS_EASE_MS))
		: own;
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

// A FOOT DOWN STAYS POINTED WHERE IT CAME DOWN. On his strides - cutting,
// going round a corner, a backpedal getting going - a foot on the floor
// doesn't turn with him: it points where it pointed as it came down till
// it comes up, and goes round to where the rest of him has it only while
// it is up. (As far as hips turn: past that, it goes round with him. And
// turning on the spot with his arms at something on the run - a box-out,
// say - his feet step round as they do standing: see pivotOf.)
//
// When each foot came down and came up on a run, on the steps he shows
// on it - worked out once, along with his strides (see steppingOf); a
// run that goes on from the last carries on from when that one's feet
// came down and up.
// (And whether it is down as the run ends, or how far through its swing;
// one that came down as the run started - sliding back the way he came,
// say - which way it came down pointing; and one that came up then, when.)
type Footfalls = {
	on: number[];
	off: number[];
	down?: boolean;
	swing?: number;
	from?: Map<number, number>;
	cut?: number;
};
type FallsMemo = {
	of: Stepping;
	by: Map<AnimName, [Footfalls, Footfalls]>;
};
const footfalls = new WeakMap<Track, Map<Move, FallsMemo>>();
const fallsMemo = (
	tl: CourtTimeline,
	tr: Track,
	k: number,
	run: Run,
): FallsMemo => {
	let byMove = footfalls.get(tr);
	if (!byMove) {
		byMove = new Map();
		footfalls.set(tr, byMove);
	}
	const st = steppingOf(tl, tr, k, run);
	let memo = byMove.get(run.mv);
	if (!memo || memo.of !== st) {
		memo = { of: st, by: new Map() };
		byMove.set(run.mv, memo);
	}
	return memo;
};
// Push steps - a slide's, say - as a gait's legs: his feet go out and come
// together the same either way he goes (see stride in poses), so which one
// is down is which way he goes - the foot he leads with down through the
// close, the other through the push. (Which he leads with, as he goes
// halfway along the run - or, going straight on from push steps on the run
// before, the foot that led on those: his feet go on stepping as they were,
// not each put half a step on in a frame. Which way he goes having come
// round to his other side, the other foot leads - see leftLeads.)
const PUSH_DUTY = 0.5;
const pushLeads = new WeakMap<Track, Map<Move, boolean>>();
const leadsLeft = (
	tl: CourtTimeline,
	tr: Track,
	k: number,
	run: Run,
): boolean => {
	let memo = pushLeads.get(tr);
	if (!memo) {
		memo = new Map();
		pushLeads.set(tr, memo);
	}
	let left = memo.get(run.mv);
	if (left === undefined) {
		const was =
			k > 0 && (run.v0 > 0 || run.mv.t0 - tr.moves[k - 1]!.t1 <= STRIDE_ON_MS)
				? runOf(tr, k - 1)
				: undefined;
		if (was && PUSH_STEPS.has(endStepsOf(tl, tr, k - 1, was))) {
			left = leadsLeft(tl, tr, k - 1, was);
		} else {
			const mid = (run.s0 + run.s1) / 2;
			left = wayOf(tr, yawAt(tl, tr, mid), mid)[1] > 0;
		}
		memo.set(run.mv, left);
	}
	return left;
};
const stepLegs = (
	tl: CourtTimeline,
	tr: Track,
	k: number,
	run: Run,
	anim: AnimName,
	ph: number,
): ReturnType<typeof legPhases> => {
	if (!PUSH_STEPS.has(anim)) {
		return legPhases(anim, ph);
	}
	const left = leadsLeft(tl, tr, k, run);
	const trail = frac(ph);
	const lead = frac(ph - 0.5);
	return {
		r: left ? trail : lead,
		l: left ? lead : trail,
		duty: PUSH_DUTY,
		back: false,
	};
};
const footfallsOf = (
	tl: CourtTimeline,
	tr: Track,
	k: number,
	run: Run,
	anim: AnimName,
): [Footfalls, Footfalls] | undefined => {
	const g0 = stepLegs(tl, tr, k, run, anim, 0);
	if (!g0) {
		return undefined;
	}
	const memo = fallsMemo(tl, tr, k, run);
	const st = memo.of;
	const had = memo.by.get(anim);
	if (had) {
		return had;
	}
	// (On the steps he shows, his strides as gaitOf counts them.)
	const own = stepsOf(tl, tr, run);
	const extra = anim === own ? 0 : 1 / strideOf(anim) - 1 / strideOf(own);
	const n = st.ph.length - 1;
	const dt = n > 0 ? (st.s1 - st.s0) / n : 0;
	const when = (i: number) => st.s0 + i * dt;
	const at = (i: number) =>
		st.ph[i]! + (extra === 0 ? 0 : alongRun(run, when(i)) * extra);
	const through = (ph: number, leg: 0 | 1) => {
		const g = stepLegs(tl, tr, k, run, anim, ph)!;
		return footThrough(g, leg === 0 ? g.r : g.l);
	};
	// (Gone on from the run before on the same sort of steps: from when its
	// feet last came down and up. Set off from a stand: from now.)
	let before: [Footfalls, Footfalls] | undefined;
	if (
		k > 0 &&
		(run.v0 > 0 || tr.moves[k]!.t0 - tr.moves[k - 1]!.t1 <= STRIDE_ON_MS)
	) {
		const was = runOf(tr, k - 1);
		const ended = endStepsOf(tl, tr, k - 1, was);
		const g = stepLegs(tl, tr, k - 1, was, ended, 0);
		if (g && g.back === g0.back && g.duty === g0.duty) {
			before = footfallsOf(tl, tr, k - 1, was, ended);
		}
	}
	const out = ([0, 1] as const).map((leg): Footfalls => {
		const f: Footfalls = before
			? { on: [before[leg].on.at(-1)!], off: [before[leg].off.at(-1)!] }
			: { on: [st.s0], off: [st.s0] };
		const kept = before?.[leg].from?.get(f.on[0]!);
		if (kept !== undefined) {
			f.from = new Map([[f.on[0]!, kept]]);
		}
		let c0 = through(at(0), leg);
		// (Down as the run before ended and up now, or the other way about:
		// it came up just now, or down - pointing as it was turned in the
		// air, not snapped round to where his legs have it.)
		const b = before?.[leg];
		if (b?.down !== undefined && b.down !== c0 < g0.duty) {
			if (c0 < g0.duty) {
				const up = b.off.at(-1)!;
				const on = lastBy(b.on, up);
				const turn =
					wrapAngle(
						footAt(tl, tr, anim, up, true) -
							(b.from?.get(on) ?? footAt(tl, tr, anim, on, true)),
					) *
					(1 - smooth01(b.swing ?? 0));
				f.on.push(st.s0);
				(f.from ??= new Map()).set(
					st.s0,
					footAt(tl, tr, anim, st.s0, true) - turn,
				);
			} else {
				f.off.push(st.s0);
				f.cut = st.s0;
			}
		}
		for (let i = 1; i <= n; i++) {
			const c1 = through(at(i), leg);
			if (c1 < c0 - 0.5) {
				f.on.push(when(i - 1) + (dt * (1 - c0)) / (1 - c0 + c1));
			} else if (c0 < g0.duty && c1 >= g0.duty) {
				f.off.push(when(i - 1) + (dt * (g0.duty - c0)) / (c1 - c0));
			}
			c0 = c1;
		}
		f.down = c0 < g0.duty;
		if (!f.down) {
			f.swing = (c0 - g0.duty) / (1 - g0.duty);
		}
		return f;
	}) as [Footfalls, Footfalls];
	memo.by.set(anim, out);
	return out;
};
// How far a leg is through its stride from its foot coming down (0) - down
// till the gait's duty, then up - forward or backpedalling.
const footThrough = (
	g: NonNullable<ReturnType<typeof legPhases>>,
	phi: number,
): number => (g.back ? frac(g.duty - phi) : phi);
const frac = (x: number) => x - Math.floor(x);
// The last of them by t (a moment's leeway either side of the moment).
const lastBy = (xs: number[], t: number): number => {
	let lo = 0;
	let hi = xs.length - 1;
	if (xs[0]! > t + 2) {
		return xs[0]!;
	}
	while (lo < hi) {
		const mid = (lo + hi + 1) >> 1;
		if (xs[mid]! <= t + 2) {
			lo = mid;
		} else {
			hi = mid - 1;
		}
	}
	return xs[lo]!;
};
// Which way a foot would point at a moment on these steps - his legs
// turned under him from the way he faces. (As one came down or up: the same
// every frame till the next, so kept.)
const footsAt = new WeakMap<Track, Map<string, number>>();
const footAt = (
	tl: CourtTimeline,
	tr: Track,
	anim: AnimName,
	at: number,
	keep = false,
): number => {
	let memo = keep ? footsAt.get(tr) : undefined;
	if (keep && !memo) {
		memo = new Map();
		footsAt.set(tr, memo);
	}
	const key = `${anim} ${at}`;
	const had = memo?.get(key);
	if (had !== undefined) {
		return had;
	}
	const yaw = yawAt(tl, tr, at);
	const out = yaw - (legsOf(spotAt(tr, at), yaw, anim, at) * Math.PI) / 180;
	memo?.set(key, out);
	return out;
};
// Each foot's own turn (degrees, right, left) on top of his legs' `legs`
// at t, doing `d`: down, as much as keeps it pointing where it came down
// pointing; up, eased back off over its swing. (Shuffling, turned so far
// that the way he goes has come round to his other side - `way`, as he
// shows it - the foot that steps is the other one: each foot's turn is the
// other's, going over to it as the way he goes comes round - see
// leftLeads.)
const footLock = (
	tl: CourtTimeline,
	tr: Track,
	t: number,
	d: Doing,
	legs: number,
	way?: [number, number],
): [number, number] | undefined => {
	const g = d.gait;
	if (!g) {
		return undefined;
	}
	const anim = d.stride ? d.stride.anim : d.anim;
	const run = runOf(tr, g.k);
	const now = stepLegs(
		tl,
		tr,
		g.k,
		run,
		anim,
		d.stride ? d.stride.phase : d.phase,
	);
	if (!now) {
		return undefined;
	}
	const falls = footfallsOf(tl, tr, g.k, run, anim);
	if (!falls) {
		return undefined;
	}
	// (His legs turned under him as this move has them: under his arms at
	// something on the run, not at all.)
	const turned = (from: number, to: number) =>
		hipsTurn(legs + (wrapAngle(to - from) * 180) / Math.PI) - legs;
	const out = ([0, 1] as const).map((leg) => {
		const c = footThrough(now, leg === 0 ? now.r : now.l);
		const f = falls[leg];
		const from = (on: number) =>
			f.from?.get(on) ?? footAt(tl, tr, d.anim, on, true);
		if (c < now.duty) {
			return turned(from(lastBy(f.on, g.at)), footAt(tl, tr, d.anim, t));
		}
		// (Up, from as it was as it came up - over the time till it comes
		// down, if it came up as the run started.)
		const off = lastBy(f.off, g.at);
		const next = off === f.cut ? f.on.find((x) => x > off) : undefined;
		const u =
			next === undefined
				? (c - now.duty) / (1 - now.duty)
				: clamp01((g.at - off) / (next - off));
		return (
			turned(from(lastBy(f.on, off)), footAt(tl, tr, d.anim, off, true)) *
			(1 - smooth01(u))
		);
	}) as [number, number];
	const left = PUSH_STEPS.has(anim) && way ? leftLeads(way) : undefined;
	if (left !== undefined) {
		const w = leadsLeft(tl, tr, g.k, run) ? 1 - left : left;
		const [r, l] = out;
		out[0] = r + (l - r) * w;
		out[1] = l + (r - l) * w;
	}
	return Math.abs(out[0]) < 0.05 && Math.abs(out[1]) < 0.05 ? undefined : out;
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
	"dap",
	"dapPat",
	"dapHug",
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
const blendPose = (f: Blend, style?: RunStyle): Pose => {
	const as = posed(
		f.anim,
		f.phase,
		f.dribble,
		f.dribbleHand,
		f.target,
		f.dribbleFrom,
		f.tight,
		f.low,
		f.calm,
		f.flair,
	);
	const short = shortSteps(legsKept(as, f), f.anim, f.stepLen, f.phase);
	const m0 = strode(f.mirror ? mirror(short) : short, f.stride);
	const m = style ? styledRun(m0, f.anim, style) : m0;
	const own0 = footDown(f.held ? carried(m, f.anim) : m, f.down ?? 0);
	const own1 =
		PUSH_STEPS.has(f.anim) && f.way !== undefined
			? liftedStep(slanted(own0, f.way), f.way, f.phase)
			: own0;
	const own = style ? styledStance(own1, f.anim, style) : own1;
	const g = f.from;
	if (!g || (g.w <= 0 && (g.arms ?? 0) <= 0)) {
		return own;
	}
	const eased = blendFrom(own, g, style);
	// (Itself still settling out of a run, his feet stepping into it, as he
	// went on into something else: from there - as he was - not from the two
	// eased together, both feet slid over to it in a frame. A foot still
	// waiting its turn then, where it was on the floor then - see slipped.)
	if (
		g.w > 0 &&
		g.w < 1 &&
		(g.step === true || (!g.paced && stepsOver(g.anim, f.anim)))
	) {
		const was = blendPose(g, style);
		const ref = standRef(f);
		const p = settleStep(eased, own, was, 1 - g.w, ref);
		if (!f.slip) {
			return p;
		}
		const legs = settleLegs(own, was, 1 - g.w, ref);
		return slipped(p, f.slip, 1 - legs.n.on, 1 - legs.f.on, f.turns ?? [0, 0]);
	}
	return eased;
};
const blendFrom = (q: Pose, f: Blend, style?: RunStyle): Pose => {
	const was = blendPose(f, style);
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
	low?: number;
	calm?: number;
	flair?: number;
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
		x.low,
		x.calm,
		x.flair,
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
// (And setting off from where he stands: his feet step off into his
// strides one and then the other, not both swept round under him - as
// quick as he goes, though, not a settle's slow step.)
const startsOff = (from: AnimName, to: AnimName) =>
	PIVOTS.has(from) && SETTLE_FROM.has(to);
// (From running into shuffling, sliding or backpedalling, or back - stepped
// off into only from all but a standstill, under STEP_OFF_PACE feet a
// second: going, both feet stepped over in a fifth of a second fling his
// legs.)
const SIDEWAYS = new Set<AnimName>(["back", "shuffle", "slide", "sideStep"]);
const crossesOver = (from: AnimName, to: AnimName) =>
	(FORWARD.has(from) && SIDEWAYS.has(to)) ||
	(SIDEWAYS.has(from) && FORWARD.has(to));
const STEP_OFF_PACE = 3;
// (Out of something done where he stands, his feet are where it left them -
// see legsKept.)
// (One gone back into his stance at its end: something done, held, and
// then come out of.)
const actBack = (anim: AnimName) => {
	const a = ANIMS[anim];
	return PIVOTS.has(anim) && a.kind === "act" && a.keys.length >= 3;
};
const stepsOver = (from: AnimName, to: AnimName) =>
	(PIVOTS.has(to) && SETTLE_FROM.has(from)) ||
	(PIVOTS.has(to) && actBack(from)) ||
	(FOOTWORK.has(from) && FOOTWORK.has(to)) ||
	(PIVOTS.has(from) && PIVOTS.has(to) && STOOD.has(from) !== STOOD.has(to)) ||
	startsOff(from, to);
// (Stood easy, his hands go to his hips or his knees, and back, in no
// hurry either: as long as a step over.)
const slowOver = (from: AnimName, to: AnimName) =>
	(stepsOver(from, to) && !startsOff(from, to)) ||
	(STOOD.has(from) && STOOD.has(to));
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
// rolled up onto his toes is on them. Going up, as much less of it as he is
// up through that much either side - not let go in a frame on the way up.)
const SETTLE_AIR = 0.25;
const SETTLE_AIR_EASE = 0.13;
const groundedAt = (z: number) =>
	1 - smooth01((z - SETTLE_AIR + SETTLE_AIR_EASE) / (2 * SETTLE_AIR_EASE));
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

// At the end of something he does where he stands - a pass stepped into,
// say - his feet stay where it had them: back under him after, a foot at a
// time as he settles into what comes next (see actBack), not both slid
// back over the floor at once as it ends.
const LEG_KEYS: (keyof Pose)[] = [
	"hipN",
	"hipF",
	"kneeN",
	"kneeF",
	"toeN",
	"toeF",
	"wide",
	"wideN",
	"wideF",
];
const legsKept = (
	q: Pose,
	x: {
		anim: AnimName;
		phase: number;
		dribble?: number;
		dribbleHand?: Hand;
		target?: number;
		dribbleFrom?: Hand;
		tight?: number;
		low?: number;
		calm?: number;
		flair?: number;
	},
): Pose => {
	const a = ANIMS[x.anim];
	if (!actBack(x.anim) || a.kind !== "act") {
		return q;
	}
	const k0 = a.keys.at(-2)![0];
	if (x.phase <= k0 || k0 >= 1) {
		return q;
	}
	const was = posed(
		x.anim,
		k0,
		x.dribble,
		x.dribbleHand,
		x.target,
		x.dribbleFrom,
		x.tight,
		x.low,
		x.calm,
		x.flair,
	);
	const out = { ...q };
	for (const k of LEG_KEYS) {
		out[k] = was[k];
	}
	return out;
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

// STEPPING OVER, A FOOT DOWN STAYS DOWN. Going on out of one move into
// another a foot at a time (see settleStep), the foot still waiting its
// turn is where it was on the floor as the move changed: his leg reaching
// back (or round) to it as he goes on over it, as far as SLIP_MOST - put
// on the leg as how far back at the hip (for its foot so far behind or
// ahead of him: SLIP_LEG feet a radian) and out to his side.
const SLIP_MOST = 0.5;
const SLIP_LEG = 2.4;
const slipOf = (
	here: Pt,
	yaw: number,
	f: Blend,
): [number, number] | undefined => {
	if (!f.spot) {
		return undefined;
	}
	const wx = f.spot[0] - here.x;
	const wy = f.spot[1] - here.y;
	const c = Math.cos(yaw);
	const sn = Math.sin(yaw);
	let df = wx * c + wy * sn;
	let ds = wx * sn - wy * c;
	const d = Math.hypot(df, ds);
	if (d < 0.01) {
		return undefined;
	}
	if (d > SLIP_MOST) {
		df *= SLIP_MOST / d;
		ds *= SLIP_MOST / d;
	}
	return [df, ds];
};
const slipped = (
	p: Pose,
	sl: [number, number],
	wn: number,
	wf: number,
	// (Each leg's turn under him, if not as the pose has it.)
	turns?: [number, number],
) => {
	const leg = (spin: number, w: number) => {
		const a = (spin * Math.PI) / 180;
		const c = Math.cos(a);
		const sn = Math.sin(a);
		// (Into the leg's own frame, turned under him.)
		const f = sl[0] * c + sl[1] * sn;
		const s = -sl[0] * sn + sl[1] * c;
		return { hip: ((f / SLIP_LEG) * 180 * w) / Math.PI, s: s * w };
	};
	const n = leg(turns ? turns[0] : p.legs + p.spinN, wn);
	const f = leg(turns ? turns[1] : p.legs + p.spinF, wf);
	return {
		...p,
		hipN: p.hipN + n.hip,
		wideN: p.wideN - n.s,
		hipF: p.hipF + f.hip,
		wideF: p.wideF + f.s,
	};
};

// His pose: his move's, eased in from the last one's just after a change -
// and an arm in whatever it is saying.
export const poseOf = (st: PlayerState): Pose => {
	const p = leanRoom(poseFor(st), st.room);
	return st.armsIn ? tuckedArms(p, st.armsIn) : p;
};
const poseFor = (st: PlayerState): Pose => {
	const own = posed(
		st.anim,
		st.phase,
		st.dribble,
		st.dribbleHand,
		st.target,
		st.dribbleFrom,
		st.tight,
		st.low,
		st.calm,
		st.flair,
	);
	const short = shortSteps(
		st.z > 0 ? own : legsKept(own, st),
		st.anim,
		st.stepLen,
		st.phase,
	);
	const style = runStyleOf(st.pid);
	const m = styledRun(
		strode(st.mirror ? mirror(short) : short, st.stride),
		st.anim,
		style,
	);
	// The ball in his hands - not just now coming into them - his arms
	// carry it as the move does (and as the one before did, if he had it
	// then), so going from the one to the other they ease over too.
	const carry = st.holding === true && st.grip === undefined;
	const q0 = footDown(carry ? carried(m, st.anim) : m, st.down ?? 0);
	// (His own way of it on top - see runStyleOf.)
	const q = styledStance(
		PUSH_STEPS.has(st.anim) && st.way !== undefined
			? liftedStep(slanted(q0, st.way), st.way, st.phase)
			: q0,
		st.anim,
		style,
	);
	const f = st.from;
	const eased =
		f && (f.w > 0 || (f.arms ?? 0) > 0) ? blendFrom(q, f, style) : q;
	const fw = f && f.w > 0 ? f.w : 0;
	const ground = groundedAt(st.z);
	const was =
		fw > 0 &&
		fw < 1 &&
		ground > 0 &&
		(f!.step === true || (!f!.paced && stepsOver(f!.anim, st.anim)))
			? blendPose(f!, style)
			: undefined;
	const steps = was ? settleLegs(q, was, 1 - fw, standRef(st)) : undefined;
	let p = steps ? settleStep(eased, q, was!, 1 - fw, standRef(st)) : eased;
	if (steps && ground < 1) {
		p = lerpPose(eased, p, ground);
	}
	// (Easing out of the last move, out of how hard he was going at it then
	// too.)
	const drive =
		(st.drive ?? 0) + ((fw > 0 ? (f!.drive ?? 0) : 0) - (st.drive ?? 0)) * fw;
	if (drive) {
		p = driven(
			p,
			drive,
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
	const onN = steps ? fw + (1 - steps.n.on - fw) * ground : fw;
	const onF = steps ? fw + (1 - steps.f.on - fw) * ground : fw;
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
			spinN: legAt(0, onN),
			spinF: legAt(1, onF),
			hipN: p.hipN + PIVOT_HIP * lift(2),
			kneeN: p.kneeN + PIVOT_KNEE * lift(2),
			hipF: p.hipF + PIVOT_HIP * lift(3),
			kneeF: p.kneeF + PIVOT_KNEE * lift(3),
		};
	}
	// And each foot down on his strides pointing where it came down - easing
	// out of the last move, from where it pointed then.
	const lk = st.lock;
	const ff = fw > 0 ? f!.feet : undefined;
	if (lk || ff) {
		const turnAt = (i: 0 | 1, on: number) =>
			(lk?.[i] ?? 0) + ((ff?.[i] ?? 0) - (lk?.[i] ?? 0)) * on;
		p = { ...p, footN: turnAt(0, onN), footF: turnAt(1, onF) };
	}
	// Stepping over, a foot not yet up stays where it was on the floor as
	// he goes on over it - not carried along under him till it steps.
	const sl = st.slip;
	if (steps && sl) {
		p = slipped(p, sl, (1 - steps.n.on) * ground, (1 - steps.f.on) * ground);
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
// easing out of a move that held it another way (in both hands, into one
// that has it all in one hand from the start - cupped for a windmill out of
// a rebound, pushed out for a pass, pulled round in a spin - or up on his
// shooting hand), where that one had it as it changed, as much as his arms
// are still in it: it goes over from the one hold to the other, not across
// in a frame.
// Whatever draws him and whatever puts the ball in his hands both ask
// this, so the two agree.
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
	// How far it has turned (radians, positive rolling on the way it goes):
	// backspin off a shooter's fingers or a passer's, a roll along the floor.
	roll?: number;
	// The way it goes, turning so (radians across the floor, from toward the
	// right rim - which it is if not given).
	rollWay?: number;
	// The way its printed side faces across the floor, before any roll (a
	// yaw; see facesTo). For the picture only.
	face?: number;
};

// Turns a second of backspin on a ball thrown or shot.
export const BACKSPIN = 2;

// A basketball is 9.4 inches across.
export const BALL_R = 0.39;
// Feet per second, per second.
export const GRAVITY = 32.2;

// The ball's hand-overs from one move to the next - a dribble picked up, a
// ball scooped off the floor - are eased from where the last move left it,
// as his hands close on it (see takenOver), so it never jumps: from no
// farther off than this (feet).
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

// The ball out of a limb of his (from a to b, so thick), across the floor:
// its height as it was.
const outOfLimb = (
	p: Pt3,
	a: Pt3,
	b: Pt3,
	thick: number,
): { x: number; y: number } => {
	const { x, y } = p;
	const dx = b.x - a.x;
	const dy = b.y - a.y;
	const dz = b.z - a.z;
	const L2 = dx * dx + dy * dy + dz * dz;
	const u =
		L2 > 1e-9
			? Math.max(
					0,
					Math.min(
						1,
						((x - a.x) * dx + (y - a.y) * dy + (p.z - a.z) * dz) / L2,
					),
				)
			: 0;
	const cx = a.x + dx * u;
	const cy = a.y + dy * u;
	const cz = a.z + dz * u;
	const need = BALL_R + thick;
	const ox = x - cx;
	const oy = y - cy;
	const oz = p.z - cz;
	if (ox * ox + oy * oy + oz * oz >= need * need || oz * oz >= need * need) {
		return { x, y };
	}
	const across = Math.hypot(ox, oy);
	if (across < 1e-6) {
		return { x, y };
	}
	const want = Math.sqrt(need * need - oz * oz);
	return { x: cx + (ox / across) * want, y: cy + (oy / across) * want };
};

// His limbs a ball goes round (from, to, how thick): his legs, thigh and
// shin - and, for a loose ball, his body and his head.
const limbsOf = (st0: PlayerState, body: Body, whole: boolean) => {
	const st = withBody(st0, body);
	const sk = skeleton(body, poseOf(st));
	const out: [Pt3, Pt3, number][] = [];
	for (const leg of [sk.legR, sk.legL]) {
		const [hip, knee, ankle] = [leg.root, leg.mid, leg.end].map((v) =>
			bodyPoint(st, v),
		);
		out.push(
			[hip!, knee!, body.thighR * 0.9],
			[knee!, ankle!, body.calfR * 0.9],
		);
	}
	if (whole) {
		const head = bodyPoint(st, sk.head);
		out.push(
			[bodyPoint(st, sk.pelvis), bodyPoint(st, sk.chest), body.depth * 0.45],
			[head, head, body.headR * 0.85],
		);
	}
	return out;
};

// The ball on his dribble never goes through his own legs - between them,
// across in front of them, round behind them: out from his thigh or his
// shin, across the floor (its height as it was, so it still meets the
// floor and his hand).
// (Twice round: put out of one leg into the other, between them, it is put
// out of that one again.)
const offLegs = (p: Pt3, st0: PlayerState, body: Body): Pt3 => {
	const limbs = limbsOf(st0, body, false);
	let at = p;
	for (let pass = 0; pass < 2; pass++) {
		for (const [a, b, thick] of limbs) {
			at = { ...outOfLimb(at, a, b, thick), z: p.z };
		}
	}
	return at;
};

// The men on the floor at t within `reach` of p, across it - by where they
// stand, before any of them is worked out (see playerAt: not into the
// moments kept for the picture, which the frame's own men are in).
const menNear = (
	tl: CourtTimeline,
	t: number,
	p: { x: number; y: number },
	reach: number,
): Track[] => {
	const out: Track[] = [];
	for (const tr of tl.tracks.values()) {
		const si = lastIndex(tr.shown, t, (v) => v[0]);
		if (si < 0 || !tr.shown[si]![1]) {
			continue;
		}
		const at = spotAt(tr, t, false);
		if (Math.hypot(at.x - p.x, at.y - p.y) <= reach) {
			out.push(tr);
		}
	}
	return out;
};

// A LOOSE BALL GOES ROUND A MAN, NOT THROUGH HIM. Off the rim, bouncing
// or rolling along the floor, a leg, a body or a head that comes through
// where it is knocks it aside across the floor, and it goes on from there -
// eased back onto its own way as hands take it, or it goes off a man again
// (see knockAt). Not off the man about to take it up, coming to it; not on
// its way out of bounds (it went out off whoever the call says), nor in the
// net (that is the rim's), nor lying there; and only so far. Worked out a step at a time from its start, as
// far as it has been asked for (a few steps a frame, as it plays), and
// kept.
const KNOCK_STEP = 20;
// How near a man has to be to be looked at, and how long its knocks are
// worked out for - after that, as they were.
const KNOCK_NEAR = 3.5;
const KNOCK_MOST = 8000;
const KNOCK_EASE = 300;
const KNOCK_NET = 8.3;
const KNOCK_FAR = 1.5;
const KNOCK_TAKER = 600;
const LOOSE: ReadonlySet<BallSeg["kind"]> = new Set(["path", "bounce", "rest"]);
type Knocks = {
	dx: number[];
	dy: number[];
	n: number;
	end: number;
	// Who takes it up (and when), and whether it goes out of bounds.
	taker?: number;
	taken: number;
	out: boolean;
};
const knocksMemo = new WeakMap<CourtTimeline, Map<BallSeg, Knocks>>();
const knocksTo = (
	tl: CourtTimeline,
	i: number,
	upTo: number,
	bodyFor: (pid: number) => Body,
): Knocks => {
	const seg = tl.ball[i]!;
	let memo = knocksMemo.get(tl);
	if (!memo) {
		memo = new Map();
		knocksMemo.set(tl, memo);
	}
	let k = memo.get(seg);
	if (!k) {
		const end =
			tl.ball[i + 1]?.t0 ?? ("t1" in seg ? seg.t1 : seg.t0 + KNOCK_MOST);
		let j = i + 1;
		while (j < tl.ball.length && LOOSE.has(tl.ball[j]!.kind)) {
			j++;
		}
		const up = tl.ball[j];
		const taken = up?.t0 ?? Infinity;
		const out = tl.fx.find(
			(f) =>
				f.kind === "whistle" &&
				f.call === "out" &&
				f.t >= seg.t0 &&
				f.t <= Math.min(taken, seg.t0 + KNOCK_MOST),
		);
		k = {
			dx: [],
			dy: [],
			n: Math.max(
				1,
				Math.ceil(Math.min(end - seg.t0, KNOCK_MOST) / KNOCK_STEP) + 1,
			),
			end,
			taker:
				up?.kind === "hold"
					? up.pid
					: up?.kind === "fly" && "pid" in up.from
						? up.from.pid
						: undefined,
			taken,
			out: out !== undefined,
		};
		memo.set(seg, k);
	}
	const last = Math.min(k.n - 1, upTo);
	if (k.dx.length > last) {
		return k;
	}
	// Knocked aside already, it goes on from there.
	let ox = k.dx.at(-1);
	let oy = k.dy.at(-1);
	if (ox === undefined || oy === undefined) {
		ox = 0;
		oy = 0;
		const prev = tl.ball[i - 1];
		if (i > 0 && prev && LOOSE.has(prev.kind)) {
			const p = knocksTo(tl, i - 1, Infinity, bodyFor);
			ox = p.dx.at(-1)!;
			oy = p.dy.at(-1)!;
		}
	}
	for (let j = k.dx.length; j <= last; j++) {
		const t = seg.t0 + j * KNOCK_STEP;
		const raw = ballOn(tl, i, t, bodyFor, 0);
		let q: Pt3 = { x: raw.x + ox, y: raw.y + oy, z: raw.z };
		if (seg.kind !== "rest" && !k.out && q.z < KNOCK_NET) {
			for (const tr of menNear(tl, t, q, KNOCK_NEAR)) {
				if (tr.pid === k.taker && t > k.taken - KNOCK_TAKER) {
					continue;
				}
				for (const [a, b, thick] of limbsOf(
					playerAt(tl, tr.pid, t),
					bodyFor(tr.pid),
					true,
				)) {
					q = { ...outOfLimb(q, a, b, thick), z: q.z };
				}
			}
		}
		ox = q.x - raw.x;
		oy = q.y - raw.y;
		const far = Math.hypot(ox, oy);
		if (far > KNOCK_FAR) {
			ox *= KNOCK_FAR / far;
			oy *= KNOCK_FAR / far;
		}
		k.dx.push(ox);
		k.dy.push(oy);
	}
	return k;
};
// Where the knocks have it (see knocksTo), from where its own way has it.
const knockAt = (
	tl: CourtTimeline,
	i: number,
	t: number,
	bodyFor: (pid: number) => Body,
	ball: BallState,
): BallState => {
	const seg = tl.ball[i]!;
	const at = Math.max(0, (t - seg.t0) / KNOCK_STEP);
	const k = knocksTo(tl, i, Math.floor(at) + 1, bodyFor);
	const f = Math.min(k.n - 1, at);
	const j = Math.floor(f);
	const w = f - j;
	const j1 = Math.min(k.dx.length - 1, j + 1);
	let x = k.dx[j]! + (k.dx[j1]! - k.dx[j]!) * w;
	let y = k.dy[j]! + (k.dy[j1]! - k.dy[j]!) * w;
	const next = tl.ball[i + 1];
	if (!next || !LOOSE.has(next.kind)) {
		const ease = Math.max(0, Math.min(1, (k.end - t) / KNOCK_EASE));
		x *= ease;
		y *= ease;
	}
	return x === 0 && y === 0 ? ball : { ...ball, x: ball.x + x, y: ball.y + y };
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
): BallState => {
	if (tl.ball.length === 0) {
		return { x: 47, y: 25, z: 0 };
	}
	const i = ballIndexAt(tl, t);
	const ball = ballOn(tl, i, t, bodyFor, 0);
	return LOOSE.has(tl.ball[i]!.kind) ? knockAt(tl, i, t, bodyFor, ball) : ball;
};

// The way the ball faces (see facesTo), for the picture.
export const ballFaceAt = (tl: CourtTimeline, t: number): number => {
	if (tl.ball.length === 0) {
		return FIRST_FACE;
	}
	const i = ballIndexAt(tl, t);
	const seg = tl.ball[i]!;
	const f = facesTo(tl, i);
	return seg.kind === "hold"
		? f.off[i]! + evalPlayer(tl, seg.pid, t).yaw
		: f.inn[i]!;
};

// WHICH WAY IT FACES. Its printed side, across the floor: as it came to him
// - out of the last man's hands, through the air, off the floor - and on
// round with him as he turns with it in his hands; let go, as it left them.
// So it goes from hand to hand as it went, never all at once some new way
// round. For each piece of its timeline, the way it faces as the piece
// takes it, and - in a man's hands - how far round from the way he faces
// it is; worked out as far as asked, and kept as long as the pieces stand.
type Faces = { segs: BallSeg[]; inn: number[]; off: number[] };
const facesMemo = new WeakMap<CourtTimeline, Faces>();
// Before anybody has had it.
const FIRST_FACE = 0.6;
const facesTo = (tl: CourtTimeline, i: number): Faces => {
	let f = facesMemo.get(tl);
	if (!f) {
		f = { segs: [], inn: [], off: [] };
		facesMemo.set(tl, f);
	}
	let from = f.segs.length;
	for (let j = 0; j < Math.min(from, i + 1); j++) {
		if (f.segs[j] !== tl.ball[j]) {
			from = j;
			break;
		}
	}
	f.segs.length = f.inn.length = f.off.length = from;
	for (let j = from; j <= i; j++) {
		const seg = tl.ball[j]!;
		const prev = tl.ball[j - 1];
		const inn = !prev
			? FIRST_FACE
			: prev.kind === "hold"
				? f.off[j - 1]! + playerAt(tl, prev.pid, seg.t0).yaw
				: f.inn[j - 1]!;
		f.segs[j] = seg;
		f.inn[j] = inn;
		f.off[j] =
			seg.kind === "hold"
				? wrapAngle(inn - playerAt(tl, seg.pid, seg.t0).yaw)
				: 0;
	}
	return f;
};

// A PASS GOES BY A MAN IN THE WAY, NOT THROUGH HIM. Thrown past a man
// between the passer and his man - or out of bounds past one, or dropping
// out of the net past a man under it - it bows out to the side round his
// head, his body, a hand he has out or a leg, just enough to miss him: past his ear, past his
// shoulder - most at the middle of its flight, and not at all leaving the
// passer's hands or getting to the other man's. (Not a shot, on its way
// into a play at the rim.) Worked out once a pass, as it was thrown (feet,
// the most it bows out; nothing, for a ball nobody is in the way of - or so
// far in the way it would have to swing round him).
const PASS_MARGIN = 0.12;
const TORSO_R = 0.55;
const BOW_MOST = 2.2;
const passedBys = new WeakMap<object, Pt3 | null>();
type FlySeg = Extract<CourtTimeline["ball"][number], { kind: "fly" }>;
// Each man near it, each moment of its flight: where it is then, how far
// out it bows there (for a bow of 1), and the parts of him it goes by (from,
// to, how thick): his head, the line up his body, his hands and his legs.
// (Near enough that a bow out his way could bring it to him.)
type Near = { k: number; p: Pt3; parts: [Pt3, Pt3, number][] };
// That worked out a moment at a time (see warmBall): the ends of its
// flight, the men at them, the men near it so far, and the next moment.
type PassWork = {
	a: Pt3;
	b: Pt3;
	ends: Set<number>;
	near: Near[];
	u: number;
};
const passWorks = new WeakMap<object, PassWork>();
const passWorkOf = (
	tl: CourtTimeline,
	seg: FlySeg,
	a: Pt3,
	b: Pt3,
	bodyFor: (pid: number) => Body,
	offRim: boolean,
): PassWork => {
	const had = passWorks.get(seg);
	if (
		had &&
		had.a.x === a.x &&
		had.a.y === a.y &&
		had.a.z === a.z &&
		had.b.x === b.x &&
		had.b.y === b.y &&
		had.b.z === b.z
	) {
		return had;
	}
	// (Not round the men throwing it and catching it - nor one whose hands
	// or head meet it at either end: a steal, a tip, a ball off him.)
	const ends = new Set<number>();
	for (const [e, at] of [
		[seg.from, seg.t0],
		[seg.to, seg.t1],
	] as const) {
		if ("pid" in e) {
			ends.add(e.pid);
			continue;
		}
		if (offRim && at === seg.t0) {
			continue;
		}
		for (const tr of menNear(tl, at, e, 4)) {
			const st = playerAt(tl, tr.pid, at);
			const body = bodyFor(tr.pid);
			const me = withBody(st, body);
			const sk = skeleton(body, poseOf(me));
			if (
				[sk.head, sk.armR.end, sk.armL.end].some((v) => {
					const w = bodyPoint(me, v);
					return Math.hypot(w.x - e.x, w.y - e.y, w.z - e.z) < 2;
				})
			) {
				ends.add(tr.pid);
			}
		}
	}
	const w: PassWork = { a, b, ends, near: [], u: 0.1 };
	passWorks.set(seg, w);
	return w;
};
// Up to `n` more moments of its flight.
const passSteps = (
	tl: CourtTimeline,
	seg: FlySeg,
	w: PassWork,
	bodyFor: (pid: number) => Body,
	n: number,
) => {
	const { a, b } = w;
	const T = Math.max(0, seg.t1 - seg.t0) / 1000;
	const vz = T > 0 ? (b.z - a.z) / T + 0.5 * GRAVITY * T : 0;
	for (; n > 0 && w.u < 0.95; n--, w.u += 0.05) {
		const u = w.u;
		const t = seg.t0 + (seg.t1 - seg.t0) * u;
		const tau = u * T;
		const p = {
			x: a.x + (b.x - a.x) * u,
			y: a.y + (b.y - a.y) * u,
			z: a.z + vz * tau - 0.5 * GRAVITY * tau * tau,
		};
		for (const tr of menNear(tl, t, p, 3 + BOW_MOST)) {
			if (w.ends.has(tr.pid)) {
				continue;
			}
			const st = playerAt(tl, tr.pid, t);
			const body = bodyFor(tr.pid);
			const me = withBody(st, body);
			const sk = skeleton(body, poseOf(me));
			const hand = (arm: Limb): [Pt3, Pt3, number] => {
				const tip = arm.tip ?? arm.end;
				const h = bodyPoint(me, {
					f: (arm.end.f + tip.f) / 2,
					s: (arm.end.s + tip.s) / 2,
					u: (arm.end.u + tip.u) / 2,
				});
				return [h, h, body.handR];
			};
			const head = bodyPoint(me, sk.head);
			const parts: [Pt3, Pt3, number][] = [
				[head, head, body.headR],
				[bodyPoint(me, sk.pelvis), bodyPoint(me, sk.chest), TORSO_R],
				hand(sk.armR),
				hand(sk.armL),
			];
			for (const leg of [sk.legR, sk.legL]) {
				const [hip, knee, ankle] = [leg.root, leg.mid, leg.end].map((v) =>
					bodyPoint(me, v),
				);
				parts.push(
					[hip!, knee!, body.thighR * 0.9],
					[knee!, ankle!, body.calfR * 0.9],
				);
			}
			w.near.push({ k: 4 * u * (1 - u), p, parts });
		}
	}
};
const passedBy = (
	tl: CourtTimeline,
	seg: FlySeg,
	a: Pt3,
	b: Pt3,
	bodyFor: (pid: number) => Body,
	// Coming off the rim: nobody's hand is on it as it starts, whoever's
	// is still up there.
	offRim = false,
): Pt3 | undefined => {
	const had = passedBys.get(seg);
	if (had !== undefined) {
		return had ?? undefined;
	}
	const work = passWorkOf(tl, seg, a, b, bodyFor, offRim);
	passSteps(tl, seg, work, bodyFor, Infinity);
	const near = work.near;
	const along = Math.hypot(b.x - a.x, b.y - a.y);
	// (Out to his side, across the way it goes.)
	const side =
		along > 1e-6
			? { x: -(b.y - a.y) / along, y: (b.x - a.x) / along }
			: { x: 0, y: 1 };
	// Where it is at one of those moments, bowed out `off` (feet): the
	// nearest of him to it - his head, or the line up his body - how far
	// it is, and how far it needs to be.
	const gaps = (n: Near, off: Pt3) => {
		const q = {
			x: n.p.x + off.x * n.k,
			y: n.p.y + off.y * n.k,
			z: n.p.z + off.z * n.k,
		};
		return n.parts
			.map(([a, b, r]) => {
				const k = clamp01(
					((q.x - a.x) * (b.x - a.x) +
						(q.y - a.y) * (b.y - a.y) +
						(q.z - a.z) * (b.z - a.z)) /
						Math.max(
							1e-6,
							(b.x - a.x) ** 2 + (b.y - a.y) ** 2 + (b.z - a.z) ** 2,
						),
				);
				return {
					c: {
						x: a.x + (b.x - a.x) * k,
						y: a.y + (b.y - a.y) * k,
						z: a.z + (b.z - a.z) * k,
					},
					r,
				};
			})
			.map(({ c, r }) => ({
				r: { x: q.x - c.x, y: q.y - c.y, z: q.z - c.z },
				d: Math.hypot(q.x - c.x, q.y - c.y, q.z - c.z),
				need: r + BALL_R + PASS_MARGIN,
			}));
	};
	// How far into anybody it goes, bowed out `off`.
	const into = (off: Pt3) =>
		Math.max(0, ...near.flatMap((n) => gaps(n, off).map((g) => g.need - g.d)));
	// Each moment it would touch him straight.
	const none = { x: 0, y: 0, z: 0 };
	const close = near.flatMap((n) =>
		gaps(n, none)
			.filter((g) => g.d < g.need)
			.map((g) => ({ ...g, k: n.k })),
	);
	// Out to the side it already is of him where it comes closest, across
	// its flight - never up or down: in the air it falls as it would - as far
	// out that way as takes it clear of him every moment it would touch him.
	// (Or, that bringing it into another man, the other way.)
	const worst = close.reduce<(typeof close)[number] | undefined>(
		(w, c) => (!w || c.need - c.d > w.need - w.d ? c : w),
		undefined,
	);
	let out: Pt3 | undefined;
	if (worst) {
		const first = worst.r.x * side.x + worst.r.y * side.y < 0 ? -1 : 1;
		let best = into(none);
		for (const way of [first, -first]) {
			const dir = { x: side.x * way, y: side.y * way, z: 0 };
			const len = Math.max(
				...close.map((c) => {
					const rd = c.r.x * dir.x + c.r.y * dir.y + c.r.z * dir.z;
					return (
						(Math.sqrt(Math.max(0, rd * rd - c.d * c.d + c.need * c.need)) -
							rd) /
						c.k
					);
				}),
			);
			if (len > BOW_MOST) {
				continue;
			}
			const off = { x: dir.x * len, y: dir.y * len, z: 0 };
			const left = into(off);
			if (left < best - 1e-6) {
				best = left;
				out = off;
			}
			if (left <= 1e-6) {
				break;
			}
		}
	}
	passedBys.set(seg, out ?? null);
	passWorks.delete(seg);
	return out;
};

// Where a flight starts and ends: where the last man had it, or the
// thrower's hands - and the catcher's (see ballOn).
const flyEnds = (
	tl: CourtTimeline,
	i: number,
	bodyFor: (pid: number) => Body,
): [Pt3, Pt3] => {
	const seg = tl.ball[i] as FlySeg;
	const prev = tl.ball[i - 1];
	const end = (p: BallEnd, at: number): Pt3 =>
		"pid" in p
			? (p.hand ?? "both") === "both"
				? heldBall(evalPlayer(tl, p.pid, at), bodyFor(p.pid))
				: handWorld(
						evalPlayer(tl, p.pid, at),
						bodyFor(p.pid),
						p.hand as "near" | "far",
					)
			: p;
	return [
		i > 0 &&
		prev?.kind === "hold" &&
		"pid" in seg.from &&
		prev.pid === seg.from.pid
			? ballOn(tl, i - 1, seg.t0 - BEFORE, bodyFor, 1)
			: end(seg.from, seg.t0),
		end(seg.to, seg.t1),
	];
};

// A PASS ABOUT TO BE THROWN, WORKED OUT AHEAD. The way it goes by men in
// its way (see passedBy) a few moments of its flight a frame, in the frames
// before it is thrown - not all of it in the one frame it leaves his
// hands. (For the picture, as it plays: see momentAt.)
const WARM_MS = 600;
const WARM_STEPS = 2;
export const warmBall = (
	tl: CourtTimeline,
	t: number,
	bodyFor: (pid: number) => Body,
) => {
	if (tl.ball.length === 0) {
		return;
	}
	const i = ballIndexAt(tl, t);
	for (let j = i + 1; j <= i + 2; j++) {
		const seg = tl.ball[j];
		if (!seg || seg.t0 - t > WARM_MS) {
			return;
		}
		if (
			seg.kind !== "fly" ||
			tl.ball[j + 1]?.kind === "path" ||
			passedBys.has(seg)
		) {
			continue;
		}
		const w = passWorks.get(seg);
		if (w) {
			passSteps(tl, seg, w, bodyFor, WARM_STEPS);
		} else {
			const [a, b] = flyEnds(tl, j, bodyFor);
			passWorkOf(tl, seg, a, b, bodyFor, tl.ball[j - 1]?.kind === "path");
		}
		return;
	}
};

// Where the ball is at t, by the i-th piece of its path.
// Where a ball he had at `then` is now, held still in his hands: along with
// him as he goes, and round with him as he turns.
const carriedRound = (
	was: Pt,
	then: { x: number; y: number; yaw: number },
	now: { x: number; y: number; yaw: number },
): Pt => {
	const d = now.yaw - then.yaw;
	const c = Math.cos(d);
	const s = Math.sin(d);
	const rx = was.x - then.x;
	const ry = was.y - then.y;
	return { x: now.x + rx * c - ry * s, y: now.y + rx * s + ry * c };
};

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
				t - seg.t0 < crossMs(seg) &&
				ph < down
			) {
				const was = left();
				const then = evalPlayer(tl, seg.pid, seg.t0 - BEFORE);
				const e = ease(ph / down);
				const { x, y } = carriedRound(was, then, st);
				return {
					x: x + (ball.x - x) * e,
					y: y + (ball.y - y) * e,
					z: was.z + (ball.z - was.z) * e,
					holder: seg.pid,
				};
			}
			return { ...offLegs(ball, st, body), holder: seg.pid };
		}
		if (seg.style === "dribble") {
			const bounce = bounceOf(tl, i, t);
			const { ph, from, to, first, start, origin } = bounce;
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
							const held = { ...carriedRound(was, then, st), z: was.z };
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
			// (Back through each move he is easing out of: two changes in a
			// frame or two - off a drive into a backpedal, then stood - and
			// it is still out where the drive had it, coming in.)
			const aheadOf = (anim: AnimName, f?: Blend, depth = 0): number => {
				const a = dribbleAhead(anim);
				return f && f.w > 0 && depth < 3
					? a + (aheadOf(f.anim, f.from, depth + 1) - a) * f.w
					: a;
			};
			const ahead = aheadOf(st.anim, st.from) * (1 - (st.tight ?? 0));
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
			const top = evalPlayer(tl, seg.pid, bounce.top);
			const c = Math.cos(top.yaw);
			const roll =
				dribbleAhead(top.anim) === 1 && Math.abs(c) > 0.5
					? Math.PI * 2 * ph * Math.sign(c)
					: 0;
			const at = offLegs(
				{
					x: h.x + (floor.x - h.x) * tri,
					y: h.y + (floor.y - h.y) * tri,
					z: h.z + (floor.z - h.z) * tri,
				},
				st,
				body,
			);
			return {
				...at,
				holder: seg.pid,
				...(roll !== 0 ? { roll } : {}),
			};
		}
		const held: BallState = { ...heldBall(st, body), holder: seg.pid };
		// Taken into both hands - up off his dribble, off the floor, out of
		// another move - it comes from where it was, as his hands close on it
		// (see takenOver): from the first of his holds straight on. (Not when
		// the picture cut to him with it: that is no hand-over.)
		const over = depth < 2 ? takenOver(tl, i) : undefined;
		if (over && t < over.until) {
			const first = tl.ball[over.k]!;
			const before = tl.ball[over.k - 1]!;
			const was = ballOn(tl, over.k - 1, first.t0 - BEFORE, bodyFor, depth + 1);
			const then = evalPlayer(tl, seg.pid, first.t0 - BEFORE);
			const at = heldBall(then, body);
			if (dist2(was, at) + (was.z - at.z) ** 2 < HANDOVER_REACH ** 2) {
				// Carried along with him if it was already his.
				const moved =
					before.kind === "hold"
						? { x: st.x - then.x, y: st.y - then.y }
						: { x: 0, y: 0 };
				const e = smooth01((t - first.t0) / (over.until - first.t0));
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
		// (By a man in the way, not through him - see passedBy.)
		const by =
			tl.ball[i + 1]?.kind === "path"
				? undefined
				: passedBy(tl, seg, a, b, bodyFor, tl.ball[i - 1]?.kind === "path");
		const bow = by ? 4 * u * (1 - u) : 0;
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
			x:
				a.x +
				(b.x - a.x) * u +
				(w ? (w.x - (b.x - a.x) / T) * bend : 0) +
				(by ? by.x * bow : 0),
			y:
				a.y +
				(b.y - a.y) * u +
				(w ? (w.y - (b.y - a.y) / T) * bend : 0) +
				(by ? by.y * bow : 0),
			z:
				a.z +
				vz * tau -
				0.5 * GRAVITY * tau * tau +
				(w ? (w.z - (vz - GRAVITY * T)) * bend : 0) +
				(by ? by.z * bow : 0),
			// Spinning back the way it goes. (Caught, it has turned whole
			// times over, so it comes into his hands as it left the passer's;
			// into a play at the rim, as the play takes it on.)
			roll:
				-Math.PI *
				2 *
				("pid" in seg.to && T > 0
					? (Math.max(1, Math.round(BACKSPIN * T)) * tau) / T
					: BACKSPIN * tau),
			rollWay:
				next?.kind === "path" && next.t0 === seg.t1 && next.way !== undefined
					? next.way
					: Math.abs(b.x - a.x) + Math.abs(b.y - a.y) > 1e-3
						? Math.atan2(b.y - a.y, b.x - a.x)
						: 0,
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
			...(seg.way !== undefined ? { rollWay: seg.way } : {}),
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
