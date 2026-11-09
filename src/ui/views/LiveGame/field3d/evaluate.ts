import {
	BLEND_DEPTH,
	BLEND_MS,
	blendEase,
	blendEaseRate,
	blendMsOf,
	driveFrom,
	footTurn,
	hipsTurn,
	LEGS_FTPS,
	legsTurned,
	PIVOT_MS,
	pivotFeet,
	SETTLE_MS,
	STOOD_MS,
} from "../court3d/evaluate.ts";
import { legPhases, type AnimName } from "../court3d/poses.ts";
import {
	freshStart,
	LEN_MIN,
	stepThrough,
	STRIDE_START,
	stridesThen,
	type StepLeg,
	type Stretch,
} from "../court3d/stepping.ts";
import { apartAt, apartOf, type Sep } from "./apart.ts";
import { restingZ } from "./ball.ts";
import type {
	BallHold,
	BallSeg,
	FieldTimeline,
	Leg,
	Play,
} from "./director.ts";
import type { Pt3, Side } from "./geometry.ts";
import {
	along,
	CORNER_MS,
	goes,
	joined,
	lagged,
	paceOf,
	placeAt,
	SET,
	smoothAt,
	speedAt,
	TRACK_MS,
	trackLength,
	trackOf,
	type Track,
} from "./pace.ts";
import {
	beatOf,
	gaitOf,
	liftOf,
	loopRate,
	moveKind,
	mouthAt,
	mouthOf,
	stepsOver,
	strideOf,
	tucked,
	type Move,
} from "./poses.ts";
import type { FieldPlayerState } from "./sprite.ts";

// THE FIELD AT ONE MOMENT: where everybody is and what each is doing, read
// off the staged game (see director.ts) - nothing kept from one frame to the
// next, so a moment can be had in any order (a rewind, a jump ahead, a
// replay).

export type FieldMoment = {
	t: number;
	play: Play | undefined;
	players: FieldPlayerState[];
	// The ball when it is on its own (spotted, stood on end, in the air):
	// drawn by itself, its points along `axis` if it is flying. Unset while
	// a man has it - he is drawn with it.
	// (On the turf on its side: the way it lies - see ballAt.)
	ball?: { at: Pt3; stood?: "tee" | "hold"; axis?: Pt3; lies?: Pt3 };
	// Where the ball is, wherever it is, and the side with it if a man has
	// it: for the camera to follow.
	focus?: Pt3;
	carrier?: Side;
	// The flags thrown: in the air (turning over as it comes down), or down
	// on the turf.
	flags?: { at: Pt3; down: boolean; turn: number }[];
	// ...and the bean bags, the same.
	bags?: { at: Pt3; down: boolean; turn: number }[];
	// The tee, the ball kicked off it: knocked on a step and lying there.
	tee?: Pt3;
};

// From one way of facing to the next, his turn eased over this long. (His
// pose, the court's way: see blendOf.)
const TURN_MS = 380;
// (Turning where he stands, no quicker round than STAND_TURN a second: his
// feet stepping round after him - see pivotOf.)
const STAND_TURN = 5;
// (Round into the way he goes, from where he stood - with his first steps,
// as he starts off: see pace.ts paceOf early - or from going another way:
// quicker. Off his stance at the snap: quicker still, round as he comes up
// out of it with his first step (on the clock he is on, slow just off it -
// not swung round in his stance before he is up out of it). But
// never round quicker than TURN_MOST a second (near basketball's TURN_RATE,
// 3 pi: not spun right round in a stride) - walking, TURN_WALK_MOST - nor
// round a corner, if it comes too sharp for that so quick.)
const TURN_GO_MS = 240;
const TURN_SNAP_MS = 150;
const TURN_MOST = 11.2;
const TURN_WALK_MOST = 6;
// (Out of his stance round the other way - a tight end off to the flat
// behind the line - no quicker than this: his hips opened and round over
// his first step or two, as basketball's men turn - not swung round on the
// spot as he comes up.)
const TURN_SNAP_MOST = 9;
// (Coming up into his stance for the snap, square to it as he gets there:
// round into it over the last of his way up.)
const SET_UP_MS = 300;
// (Out of a backpedal into a run the same way - his hips opened, round
// as he backpedals the last of it, this long (ms) at most: not running
// on backwards as he comes round. Or out of anything else he goes along
// faced more than FLIP_AWAY off the way he goes - driven back in a block,
// dragging his toes in - round off it the same, into a run at least
// FLIP_SAME nearer the way he was going than he was faced.)
const FLIP_LEAD_MS = 130;
// (Off from a stand more than this round from the way he stood, radians:
// round on his feet first, this share of it at most - see facingOf.)
const ROUND_OFF = 2;
const ROUND_OFF_LEAD = 0.75;
const FLIP_SAME = Math.PI / 4;
const FLIP_AWAY = Math.PI / 2;
const turnMost = (l: Leg) => (l.move === "walk" ? TURN_WALK_MOST : TURN_MOST);
// (How fast he goes through a move: taken over this long.)
const RATE_MS = 5;

const clamp01 = (u: number) => Math.min(1, Math.max(0, u));
const frac = (x: number) => x - Math.floor(x);

const wrapAngle = (a: number) => {
	const d = (a + Math.PI) % (Math.PI * 2);
	return (d < 0 ? d + Math.PI * 2 : d) - Math.PI;
};

// The last of `items` that has started by t (by `at`), or the first.
const lastStarted = <T>(
	items: T[],
	t: number,
	at: (x: T) => number,
): number => {
	let lo = 0;
	let hi = items.length - 1;
	let found = 0;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (at(items[mid]!) <= t) {
			found = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return found;
};

export const playAt = (tl: FieldTimeline, t: number): Play | undefined =>
	tl.plays[lastStarted(tl.plays, t, (p) => p.t0)];

// Which way he faces through a leg: as told, or the way he goes - or, going
// nowhere and not told, the way he was.
const yawOf = (l: Leg, before: number): number =>
	l.yaw ??
	(goes(l) ? Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x) : before);

// Which way he faces at t (his own clock - see lagged): as each leg has him
// (turning through it as it says), eased in over TURN_MS (or more) from the
// way he was facing as it started - a leg too short for that, still coming
// round as the next one starts. Worked out once for his legs as they are:
// the way he faced coming into each, how far off it he was drawn as it
// started, and over how long that goes. Round a corner from one stretch
// he goes his own way along into the next (see pace.ts rounded), round
// with it as he goes round it - not still facing the way he came, running
// sideways, as he goes off the other way: how far either side of where
// they meet that is. Off from a stand, from when he starts off: how long
// before it starts that is. (Off his stance at the snap, `snap`.)
// (And whether he turns through it at all, and how far ahead of a leg the
// most of any of that starts. Put somewhere new - the picture cut to him
// there, see pace.ts joined - facing the way it has him from the first:
// when each such leg starts.)
type Facing = {
	n: number;
	before: Float64Array;
	aim: Float64Array;
	off: Float64Array;
	ms: Float64Array;
	corner: Float64Array;
	lead: Float64Array;
	turns: Uint8Array;
	ahead: number;
	cuts: number[];
};
// (Facing the way it goes along it, not turning through it.)
const OWN_WAY = 0.1;
const ownWay = (l: Leg) =>
	goes(l) &&
	!l.turn &&
	(l.yaw === undefined ||
		Math.abs(
			wrapAngle(l.yaw - Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x)),
		) < OWN_WAY);
// (Going nearly his own way, faced this near it, radians: turned into it as
// quickly as if he were - not swung round over TURN_MS once he is off.)
const NEAR_OWN = 0.7;
const nearOwn = (l: Leg) =>
	ownWay(l) ||
	(goes(l) &&
		!l.turn &&
		l.yaw !== undefined &&
		Math.abs(
			wrapAngle(l.yaw - Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x)),
		) < NEAR_OWN);
const facings = new WeakMap<Leg[], Facing>();
// (A short way he goes his own way along, faced a little the way he goes on
// after it - where he will be LOOK_MS from its start, or the way he is to
// face next if he squares up to something before then - as near that as
// CRAB_MOST off his own way: not round one way for a step and then back
// the other.)
// (Squaring up to it a little later than that, up to LOOK_FADE_MS on:
// faced that way the less, the later - not round toward where he goes,
// and then back, as it comes nearer.)
const LOOK_MS = 500;
const LOOK_FADE_MS = 500;
const CRAB_MOST = 0.45;
const lookedAhead = (legs: Leg[], j: number): number => {
	const l = legs[j]!;
	const aim = Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x);
	const by = l.t0 + LOOK_MS;
	if (l.t1 >= by) {
		return aim;
	}
	let x = l.to.x;
	let y = l.to.y;
	let next: number | undefined;
	let w = 1;
	let seen = false;
	for (let k = j + 1; k < legs.length; k++) {
		const n = legs[k]!;
		if (Math.abs(n.t0 - legs[k - 1]!.t1) > 1 || n.turn) {
			break;
		}
		if (n.yaw !== undefined && !ownWay(n)) {
			next = n.yaw;
			w = clamp01(1 - (n.t0 - by) / LOOK_FADE_MS);
			break;
		}
		if (!goes(n) || n.t0 >= by + LOOK_FADE_MS) {
			break;
		}
		if (!seen) {
			const u = n.t1 > by ? (by - n.t0) / (n.t1 - n.t0) : 1;
			x = n.from.x + (n.to.x - n.from.x) * u;
			y = n.from.y + (n.to.y - n.from.y) * u;
			seen = n.t1 >= by;
		}
	}
	const way = Math.atan2(y - l.from.y, x - l.from.x);
	const d = wrapAngle(
		(next === undefined ? way : way + wrapAngle(next - way) * w) - aim,
	);
	return aim + Math.max(-CRAB_MOST, Math.min(CRAB_MOST, d));
};
const facingOf = (legs: Leg[]): Facing => {
	const had = facings.get(legs);
	if (had && had.n === legs.length) {
		return had;
	}
	const before = new Float64Array(legs.length);
	const aims = new Float64Array(legs.length);
	const off = new Float64Array(legs.length);
	const ms = new Float64Array(legs.length);
	const corner = new Float64Array(legs.length);
	const lead = new Float64Array(legs.length);
	const snap = new Uint8Array(legs.length);
	const turns = new Uint8Array(legs.length);
	const cuts: number[] = [];
	let ahead = CORNER_MS;
	let b = legs[0]!.yaw ?? 0;
	// (The way the one before had him at its end, `end` into turning into
	// it, `by` off it then over `ms` - and so at its end less e.)
	let end = 0;
	let wasBy = 0;
	let wasMs = 1;
	const was = (e: number) =>
		b + (end - e < wasMs ? wasBy * (1 - Math.max(0, end - e) / wasMs) : 0);
	let ownBefore = false;
	for (let j = 0; j < legs.length; j++) {
		before[j] = b;
		const lj = legs[j]!;
		const own = ownWay(lj);
		const aim = own ? lookedAhead(legs, j) : yawOf(lj, b);
		aims[j] = aim;
		const cut = j > 0 && !joined(legs[j - 1]!, lj);
		if (cut) {
			cuts.push(lj.t0);
		}
		const lp = cut ? undefined : legs[j - 1];
		const near = nearOwn(lj);
		const startsOff = near && lp && !goes(lp) && !lp.turn;
		const setsUp = !goes(lj) && lp && goes(lp) && SET.has(lj.move);
		// (Or down on a knee: round as he gets there - or, stood, round on
		// his feet first, all the way, and then down.)
		const lpWay =
			lp && goes(lp) ? Math.atan2(lp.to.y - lp.from.y, lp.to.x - lp.from.x) : 0;
		const away = Math.abs(wrapAngle(b - lpWay));
		const onWay = Math.abs(
			wrapAngle(Math.atan2(lj.to.y - lj.from.y, lj.to.x - lj.from.x) - lpWay),
		);
		const flips =
			own &&
			lp !== undefined &&
			goes(lp) &&
			Math.abs(lp.t1 - lj.t0) <= 1 &&
			(lp.move === "back"
				? onWay < FLIP_SAME
				: away > FLIP_AWAY && away - onWay > FLIP_SAME);
		const kneels =
			lj.move === "takeKnee" &&
			lp &&
			!lp.turn &&
			(goes(lp) || PIVOTS.has(lp.move));
		const stood = kneels && !goes(lp);
		// (Off from a stand well round from the way he stood - back the way
		// he came, say: round on his feet as he stands first (see pivotOf),
		// near STAND_TURN - the rest as he gets going - not spun round as
		// he steps off.)
		const roundOff =
			startsOff &&
			!SET.has(lp.move) &&
			PIVOTS.has(lp.move) &&
			Math.abs(wrapAngle(was(0) - aim)) > ROUND_OFF;
		const roundMs = (Math.abs(wrapAngle(was(0) - aim)) / STAND_TURN) * 1000;
		lead[j] = roundOff
			? Math.max(
					paceOf(legs, j).early,
					Math.min(roundMs * ROUND_OFF_LEAD, (lp.t1 - lp.t0) * 0.9),
				)
			: startsOff && !SET.has(lp.move)
				? paceOf(legs, j).early
				: flips
					? Math.min(FLIP_LEAD_MS, (lp.t1 - lp.t0) * 0.9)
					: stood
						? Math.min(
								Math.max(
									TURN_MS,
									(Math.abs(wrapAngle(was(0) - aim)) / STAND_TURN) * 1000,
								),
								(lp.t1 - lp.t0) * 0.9,
							)
						: setsUp || kneels
							? Math.min(SET_UP_MS, (lp.t1 - lp.t0) / 2)
							: 0;
		snap[j] = startsOff && SET.has(lp.move) ? 1 : 0;
		ahead = Math.max(ahead, lead[j]!);
		let by = lp ? wrapAngle(was(lead[j]!) - aim) : 0;
		if (
			lp &&
			ownBefore &&
			own &&
			Math.abs(lp.t1 - lj.t0) <= 1 &&
			Math.abs(by) > 1e-6
		) {
			const w = Math.min(CORNER_MS, (lp.t1 - lp.t0) / 2, (lj.t1 - lj.t0) / 2);
			// (A smoothstep round it at its quickest: 1.5 times the even
			// rate.)
			const most = Math.min(turnMost(lp), turnMost(lj));
			if (w > 0 && (0.75 * Math.abs(by)) / w <= most / 1000) {
				corner[j] = w;
				by = 0;
			}
		}
		off[j] = by;
		turns[j] = lj.turn || Math.abs(by) > 1e-6 || corner[j]! > 0 ? 1 : 0;
		const goMs = Math.max(TURN_GO_MS, (Math.abs(by) / turnMost(lj)) * 1000);
		ms[j] = roundOff
			? Math.max(goMs, Math.min(roundMs, lead[j]! + goMs))
			: (PIVOTS.has(lj.move) && !goes(lj)) || stood
				? Math.max(TURN_MS, (Math.abs(by) / STAND_TURN) * 1000)
				: snap[j] || near
					? Math.max(
							snap[j] ? TURN_SNAP_MS : TURN_GO_MS,
							(Math.abs(by) / (snap[j] ? TURN_SNAP_MOST : turnMost(lj))) * 1000,
						)
					: TURN_MS;
		b = aim + (lj.turn ?? 0);
		end = lj.t1 - lj.t0 + lead[j]!;
		wasBy = by;
		wasMs = ms[j]!;
		ownBefore = own;
	}
	const f = {
		n: legs.length,
		before,
		aim: aims,
		off,
		ms,
		corner,
		lead,
		turns,
		ahead,
		cuts,
	};
	facings.set(legs, f);
	return f;
};
// (Round the corner into leg j: from the way the one before has him -
// still coming round into it, maybe - to the next's, halfway where the two
// meet.)
const roundAt = (legs: Leg[], f: Facing, j: number, t: number): number => {
	const w = f.corner[j]!;
	const v = clamp01((t - legs[j]!.t0 + w) / (2 * w));
	const a =
		f.aim[j - 1]! +
		f.off[j - 1]! *
			(1 - clamp01((t - legs[j - 1]!.t0 + f.lead[j - 1]!) / f.ms[j - 1]!));
	return a + wrapAngle(f.aim[j]! - a) * v * v * (3 - 2 * v);
};
const turnedAt = (legs: Leg[], t: number, f = facingOf(legs)): number => {
	let k = lastStarted(legs, t, (l) => l.t0);
	if (k + 1 < f.n && legs[k + 1]!.t0 - t <= f.lead[k + 1]!) {
		k++;
	}
	const l = legs[k]!;
	const yaw = f.aim[k]! + (l.turn ?? 0) * along(l, t);
	const since = t - l.t0;
	if (f.corner[k]! > 0 && since < f.corner[k]!) {
		return roundAt(legs, f, k, t);
	}
	const next = legs[k + 1];
	if (next && next.t0 - t < f.corner[k + 1]!) {
		return roundAt(legs, f, k + 1, t);
	}
	const turning = since + f.lead[k]!;
	return turning < f.ms[k]!
		? yaw + f.off[k]! * (1 - clamp01(turning / f.ms[k]!))
		: yaw;
};
// (Whether he could be turning at all from a to b: none of his legs
// turning him then, nor turning him round into it.)
// (j the leg he is on at a.)
const steadyFrom = (
	legs: Leg[],
	f: Facing,
	j: number,
	a: number,
	b: number,
): boolean => {
	const l = legs[j]!;
	if (
		l.turn ||
		(f.off[j] !== 0 && a - l.t0 + f.lead[j]! < f.ms[j]!) ||
		a - l.t0 < f.corner[j]!
	) {
		return false;
	}
	for (let i = j + 1; i < legs.length && legs[i]!.t0 <= b + f.ahead; i++) {
		if (f.turns[i] && legs[i]!.t0 - Math.max(f.corner[i]!, f.lead[i]!) <= b) {
			return false;
		}
	}
	return true;
};
// (Put somewhere new this near t, ms either side.)
const cutNear = (f: Facing, t: number, ms: number): boolean =>
	f.cuts.some((u) => u > t - ms && u <= t + ms);
const steady = (legs: Leg[], a: number, b: number): boolean =>
	steadyFrom(
		legs,
		facingOf(legs),
		lastStarted(legs, a, (l) => l.t0),
		a,
		b,
	);
// And not from stood still to turning flat out in a frame, nor stopped dead
// the next, like a turret: that run through a short bell, a beat apart
// either side (each beat taken from this one, so it never goes the long way
// round) - into a turn a touch before its reason, out of it a touch after,
// halfway round at the same moment.
const TURN_EASE = [1, 4, 6, 4, 1];
const TURN_EASE_SUM = 16;
const TURN_EASE_MS = 40;
const TURN_EASE_HALF = (TURN_EASE.length - 1) / 2;
const yawAt = (legs: Leg[], t: number): number => {
	const half = TURN_EASE_HALF;
	const reach = half * TURN_EASE_MS;
	const f = facingOf(legs);
	if (cutNear(f, t, reach)) {
		return turnedAt(legs, t, f);
	}
	const j = lastStarted(legs, t - reach, (l) => l.t0);
	if (steadyFrom(legs, f, j, t - reach, t + reach)) {
		return f.aim[j]!;
	}
	const c = turnedAt(legs, t, f);
	let off = 0;
	for (let i = 0; i < TURN_EASE.length; i++) {
		if (i !== half) {
			off +=
				TURN_EASE[i]! *
				wrapAngle(turnedAt(legs, t + (i - half) * TURN_EASE_MS, f) - c);
		}
	}
	return c + off / TURN_EASE_SUM;
};

// TURNING WHERE HE STANDS (the court's way - see court3d's pivotFeet): his
// feet don't spin under him with the rest of him. One stays where it was,
// turned on, while the other steps round after him. Which way each foot
// points on the turf, and how far up off it (a full step 1): drawn turned
// under him as far as hips turn (see sprite.ts).
const PIVOTS = new Set<Move>([
	"stand",
	"easy",
	"armsFolded",
	"onHips",
	"easyHips",
	"hands",
	"blowing",
	"winded",
	"pointSky",
	"qbSet",
	"pocket",
	"shotgun",
	"gunReady",
	"settle",
	"overShoulders",
	"carryWater",
	"shoulderCam",
	"holdPole",
	"ballUp",
	"shoot",
	"kickReady",
	"shove",
	"stiffArm",
	"receiverStance",
	"receiverReady",
	"cornerStance",
	"safetyStance",
	"safetyReady",
	"backerStance",
	"backerReady",
	"lineReady",
]);
type Feet = [number, number, number, number];
// Thrown off his front foot: from the stride down at the target (this far
// through the move - the ball back by his ear) on through the follow-
// through, it stays pointing where it landed on the turf, turned on, as he
// comes round over it - the other with him. (His left, a right-hander.)
const PLANTS = new Map<Move, number>([
	["throwWind", 0.55],
	["throwFollow", 0],
]);
const pivotOf = (legs: Leg[], t: number): Feet =>
	pivotFeet((at) => yawAt(legs, at), t);
// (Nothing to show: both feet where he faces, neither up.)
const square = (p: Feet, yaw: number) =>
	Math.abs(wrapAngle(p[0] - yaw)) < 0.01 &&
	Math.abs(wrapAngle(p[1] - yaw)) < 0.01 &&
	p[2] < 0.01 &&
	p[3] < 0.01;

// NUDGED ASIDE WHERE HE STANDS (see apart.ts): he steps it, not slid over
// the turf stood still - a step each ASIDE_MS, the one foot and then the
// other, still till it is up (the last ASIDE_STEP_MS of it) and over by the
// time it is down; up the higher the quicker he is going over (a full step
// at ASIDE_FPS ft/s - none under ASIDE_LEAST_FPS, eased over as he was).
// Only the part of it he stands through (`stood`) - all of it, or from when
// he comes to a stand in it, or up to when he is off out of one: it starts
// and ends where he would be eased over to anyway.
const ASIDE_MS = 300;
const ASIDE_STEP_MS = 220;
const ASIDE_LEAST_FPS = 0.6;
const ASIDE_FPS = 3;
// (When in it he comes to a stand, or is off: to within this, ms.)
const ASIDE_EDGE_MS = 5;
const asideAt = (
	spans: Sep[] | undefined,
	t: number,
	stood: (t: number) => boolean,
): { x: number; y: number; up?: [number, number] } | undefined => {
	const now = apartAt(spans, t);
	if (!spans || !stood(t)) {
		return now;
	}
	const k = Math.floor(t / ASIDE_MS);
	// (From `out`, not stood, to `t`, stood: the first of it he is.)
	const edge = (out: number) => {
		let [o, i] = [out, t];
		while (Math.abs(i - o) > ASIDE_EDGE_MS) {
			const c = (o + i) / 2;
			if (stood(c)) {
				i = c;
			} else {
				o = c;
			}
		}
		return i;
	};
	const g0 = stood(k * ASIDE_MS) ? k * ASIDE_MS : edge(k * ASIDE_MS);
	const g1 = stood((k + 1) * ASIDE_MS)
		? (k + 1) * ASIDE_MS
		: edge((k + 1) * ASIDE_MS);
	if (g1 - g0 < ASIDE_EDGE_MS) {
		return now;
	}
	const a = apartAt(spans, g0) ?? { x: 0, y: 0 };
	const b = apartAt(spans, g1) ?? { x: 0, y: 0 };
	const v = Math.hypot(b.x - a.x, b.y - a.y) / ((g1 - g0) / 1000);
	if (v < ASIDE_LEAST_FPS) {
		return now;
	}
	const step = Math.min(ASIDE_STEP_MS, g1 - g0);
	const u = clamp01((t - (g1 - step)) / step);
	const e = u * u * (3 - 2 * u);
	// (A short one, a foot only just off the turf.)
	const up =
		Math.sin(Math.PI * u) *
		Math.min(1, v / ASIDE_FPS) *
		Math.min(1, step / ASIDE_STEP_MS);
	return {
		x: a.x + (b.x - a.x) * e,
		y: a.y + (b.y - a.y) * e,
		up: k % 2 === 0 ? [up, 0] : [0, up],
	};
};

// How far through his move he is: a stride cycle as far round his strides
// as he has come (see stridesOf - each man a step out of time with the
// next), an act by the time (or its share of the act, cut in two), a loop
// by the clock.
const phaseOf = (l: Leg, t: number, offset: number, strides = 0): number => {
	const kind = moveKind(l.move);
	if (kind === "still") {
		return 0;
	}
	if (kind === "act") {
		const [p0, p1] = l.span ?? [0, 1];
		return p0 + (p1 - p0) * along(l, t);
	}
	if (kind === "loop") {
		return (t / 1000) * loopRate(l.move) + offset;
	}
	// (A gait of the court's, from the quarter of a stride in it sets off
	// from - see stridesOf.)
	return strides + (gaitOf(l.move) ? 0 : offset);
};

// HIS STRIDES, worked out through the play (every TRACK_MS, once for his
// legs as they are - see trackOf): how far round them he is, and how long
// each leg's stride is, as a share of a full one. On a gait of the court's
// - walking, jogging, running, sprinting, backpedaling, sliding, shuffling
// - the court's strides (see court3d/stepping.ts): a foot down keeps the
// stride it came down on, so it stays put on the turf as he picks up pace
// or slows, and pulling up his last steps come down short; carried on from
// one gait into the next, each foot's stride as long in feet as it was.
// Setting off from where he stood, a quarter of a stride in or three
// quarters - whichever comes next from where his strides last were - over
// the foot he pushes off with, nearest how he stood. In a block, his feet
// going under him by the ground he covers, at least so fast (CHOPS); held
// while he does anything else - and his strides back to full ones.
// (As good as a full one.)
const LEN_FULL = 0.98;
// (His pace, over this long either side.)
const PACE_MS = 100;
const LEN_RATE = 6;
// (Steps that keep no beat - his feet going under him in a block - round no
// quicker than this a second: on into one still at a run, his feet not a
// blur, but sliding the last of it.)
const SHUFFLE_MOST = 3.5;
// (In a block his feet keep going under him even when the two of them get
// nowhere - and sat in his set in pass protection, light on them - no
// slower round than this a second.)
const CHOPS = new Map<Move, number>([
	["drive", 1.4],
	["engage", 1.4],
	["passSet", 0.9],
]);
type Strides = {
	t0: number;
	ph: Float32Array;
	r: Float32Array;
	l: Float32Array;
};
const strided = new WeakMap<
	Leg[],
	{ n: number; t0: number; s: Strides | undefined }
>();
const cycles = (l: Leg | undefined) =>
	l !== undefined && moveKind(l.move) === "cycle";
const stridesOf = (legs: Leg[], t0: number, n: number): Strides | undefined => {
	const had = strided.get(legs);
	if (
		had &&
		had.n === legs.length &&
		had.t0 === t0 &&
		(!had.s || had.s.ph.length === n)
	) {
		return had.s;
	}
	if (!legs.some(cycles)) {
		strided.set(legs, { n: legs.length, t0, s: undefined });
		return undefined;
	}
	// (Striding on his own leg, pulling up from the one before it, or
	// getting going on the next.)
	const tr = trackOf(legs, t0, n);
	const strideOn = legs.map((l) => (cycles(l) ? strideOf(l.move) : 0));
	const chopOn = legs.map((l) => CHOPS.get(l.move as Move) ?? 0);
	const gaitAt = (i: number) => {
		const k = tr.late[i]!;
		const on = tr.on[i]!;
		return strideOn[k]! > 0 ? k : strideOn[on]! > 0 ? on : -1;
	};
	// (Not the ground from where he was to where he was put, if he was.)
	const gone = new Float64Array(n);
	for (let i = 1; i < n; i++) {
		const dx = tr.xs[i]! - tr.xs[i - 1]!;
		const dy = tr.ys[i]! - tr.ys[i - 1]!;
		gone[i] = gone[i - 1]! + (tr.cut[i] ? 0 : Math.sqrt(dx * dx + dy * dy));
	}
	const along = (t: number) => {
		const f = Math.min(n - 1, Math.max(0, (t - t0) / TRACK_MS));
		const i = Math.min(n - 2, Math.floor(f));
		return gone[i]! + (gone[i + 1]! - gone[i]!) * (f - i);
	};
	const pace = (t: number) =>
		((along(t + PACE_MS) - along(t - PACE_MS)) * 1000) / (2 * PACE_MS);
	// The leg he strides on at each step (striding at either end of it).
	const on = new Int32Array(n);
	for (let i = 0; i < n; i++) {
		const g = gaitAt(i);
		on[i] = g >= 0 || i === 0 ? g : gaitAt(i - 1);
	}
	const gait = (i: number) =>
		on[i]! >= 0 ? gaitOf(legs[on[i]!]!.move) : undefined;
	const ph = new Float32Array(n);
	const r = new Float32Array(n);
	const l = new Float32Array(n);
	const dt = TRACK_MS / 1000;
	let p = 0;
	let lr = 1;
	let ll = 1;
	// (The gait he was just on, and each foot as it ended.)
	let was: { anim: AnimName; R: StepLeg; L: StepLeg } | undefined;
	let i = 0;
	while (i < n) {
		const j = on[i]!;
		const anim = gait(i);
		if (anim === undefined) {
			if (i > 0 && j >= 0) {
				const d = gone[i]! - gone[i - 1]!;
				p += Math.min(
					Math.max(d / strideOn[j]!, chopOn[j]! * dt),
					SHUFFLE_MOST * dt,
				);
			}
			const most = LEN_RATE * dt;
			lr += Math.min(most, 1 - lr);
			ll += Math.min(most, 1 - ll);
			ph[i] = p;
			r[i] = lr;
			l[i] = ll;
			was = undefined;
			i++;
			continue;
		}
		// On to the last step of it - the same gait, with nowhere he is put.
		let i1 = i;
		while (i1 + 1 < n && !tr.cut[i1 + 1] && gait(i1 + 1) === anim) {
			i1++;
		}
		// (From the step before, where his strides were.)
		const i0 = i > 0 && !tr.cut[i] ? i - 1 : i;
		const stretch: Stretch = {
			s0: t0 + i0 * TRACK_MS,
			s1: t0 + i1 * TRACK_MS,
			along,
			pace,
		};
		let start: { ph: number; R: StepLeg; L: StepLeg };
		if (was && i0 === i - 1) {
			const S0 = strideOf(was.anim as Move);
			const S = strideOf(anim as Move);
			const as = (len: number) =>
				Math.min(1, Math.max(LEN_MIN, (len * S0) / S));
			const carry = (leg: StepLeg): StepLeg => ({
				len: as(leg.len),
				...(leg.up === undefined ? {} : { up: as(leg.len), from: -1 }),
			});
			start = { ph: p, R: carry(was.R), L: carry(was.L) };
		} else {
			// (Out of a block, on in step with it; from where he stood, from
			// over the foot he pushes off with.)
			const chopped = i0 === i - 1 && on[i0]! >= 0;
			const phStart = chopped
				? p
				: p + ((((STRIDE_START - p) % 0.5) + 0.5) % 0.5);
			start = freshStart(anim, stretch, phStart);
		}
		const steps = stepThrough(anim, stretch, start.ph, start.R, start.L);
		ph[i0] = start.ph;
		for (let k = i; k <= i1; k++) {
			const at = stridesThen(steps, t0 + k * TRACK_MS);
			ph[k] = at.ph;
			r[k] = at.len[0];
			l[k] = at.len[1];
		}
		p = ph[i1]!;
		lr = r[i1]!;
		ll = l[i1]!;
		was = { anim, R: steps.legR, L: steps.legL };
		i = i1 + 1;
	}
	const s = { t0, ph, r, l };
	strided.set(legs, { n: legs.length, t0, s });
	return s;
};
// When his strides were `ph` round (or nothing, before the play).
const strideTime = (s: Strides, ph: number): number | undefined => {
	const a = s.ph;
	if (!(ph >= a[0]!)) {
		return undefined;
	}
	let lo = 0;
	let hi = a.length - 1;
	if (ph >= a[hi]!) {
		return s.t0 + hi * TRACK_MS;
	}
	while (hi - lo > 1) {
		const m = (lo + hi) >> 1;
		if (a[m]! <= ph) {
			lo = m;
		} else {
			hi = m;
		}
	}
	const d = a[hi]! - a[lo]!;
	return s.t0 + (lo + (d > 0 ? (ph - a[lo]!) / d : 0)) * TRACK_MS;
};
// A FOOT DOWN ON HIS STRIDES stays pointed where it came down (the court's
// footLock), cutting, say: turned at the ankle from the way he faces now
// as far as hips turn, and no further than a foot turns (see court3d's
// footTurn) - and a foot up eases round from there to where he faces by
// the time it comes down. Each foot's turn (degrees, his right then his
// left - see Pose's footN), or nothing - both as he faces.
const strideLock = (
	s: Strides,
	gait: AnimName,
	ph: number,
	// Which way his feet point at t, as his legs have them (radians) - and
	// his legs turned under him now (see legsUnder).
	footAt: (t: number) => number,
	now: number,
	under: number,
): [number, number] | undefined => {
	const g = legPhases(gait, ph);
	if (!g) {
		return undefined;
	}
	// (A foot pointing `foot`, its leg pointing `leg`. His left is the
	// turf's clockwise.)
	const turned = (foot: number, leg: number) =>
		footTurn(hipsTurn(under + (wrapAngle(leg - foot) * 180) / Math.PI) - under);
	const to = footAt(now);
	const foot = (phi: number): number => {
		const c = g.back ? frac(g.duty - phi) : phi;
		const fell = strideTime(s, ph - c);
		if (fell === undefined) {
			return 0;
		}
		const down = footAt(fell);
		if (c < g.duty) {
			return turned(down, to);
		}
		const rose = strideTime(s, ph - c + g.duty) ?? fell;
		const u = (c - g.duty) / (1 - g.duty);
		const v = Math.min(1, Math.max(0, u));
		return turned(down, footAt(rose)) * (1 - v * v * (3 - 2 * v));
	};
	const r = foot(g.r);
	const l = foot(g.l);
	return Math.abs(r) < 0.5 && Math.abs(l) < 0.5 ? undefined : [r, l];
};

// GOING ONE WAY WHILE HE LOOKS ANOTHER - his eyes back on the ball, say -
// his legs go the way he goes, his hips turned under him (the court's
// legsTurned: as far as hips turn, and right round, backing off, not at
// all). Backpedaling, his legs go back the way he goes. Eased in as he gets
// going and out as he pulls up, so he is square again when he stops. (Not
// shuffling across: his feet step out the way he faces.)
const legsUnder = (
	track: ReturnType<typeof trackOf>,
	now: number,
	gait: AnimName,
	yaw: number,
): number => {
	if (gait === "shuffle") {
		return 0;
	}
	const n = track.xs.length;
	const i = Math.round((now - track.t0) / TRACK_MS);
	if (i < 1 || i + 1 >= n || track.cut[i] || track.cut[i + 1]) {
		return 0;
	}
	const hx = track.xs[i + 1]! - track.xs[i - 1]!;
	const hy = track.ys[i + 1]! - track.ys[i - 1]!;
	const v = (Math.hypot(hx, hy) * 1000) / (2 * TRACK_MS);
	if (v < LEGS_FTPS) {
		return 0;
	}
	const off =
		(wrapAngle(Math.atan2(hy, hx) - yaw + (gait === "back" ? Math.PI : 0)) *
			180) /
		Math.PI;
	const e = Math.min(1, (v - LEGS_FTPS) / LEGS_FTPS);
	return legsTurned(off) * e * e * (3 - 2 * e);
};
// How far round his strides he is at t, and how long each leg's are.
const strideAt = (
	s: Strides,
	t: number,
): { ph: number; len: [number, number] } => {
	const n = s.ph.length;
	const f = Math.min(n - 1, Math.max(0, (t - s.t0) / TRACK_MS));
	const i = Math.min(n - 2, Math.floor(f));
	const u = f - i;
	const at = (a: Float32Array) => a[i]! + (a[i + 1]! - a[i]!) * u;
	return { ph: at(s.ph), len: [at(s.r), at(s.l)] };
};

// How far his arms are through an act of their own (see Leg.arms).
const armsPhaseOf = (l: Leg, t: number): number => {
	const [p0, p1] = l.armsSpan ?? [0, 1];
	return p0 + (p1 - p0) * along(l, t);
};

// The ball on the turf on its side, put down where it was: up and down the
// field; on end, straight up.
const ALONG: Pt3 = { x: 1, y: 0, z: 0 };
const UPRIGHT: Pt3 = { x: 0, y: 0, z: 1 };

// The ball in the air: along the straight line, up over it in a parabola.
// (End over end, turned so far over through it - see tumble.)
const flying = (
	s: Extract<BallSeg, { kind: "fly" }>,
	t: number,
	spin?: Spin,
	// (Dropped straight down, the way it was going before.)
	way: Pt3 = ALONG,
): { at: Pt3; axis: Pt3 } => {
	const u = s.t1 > s.t0 ? clamp01((t - s.t0) / (s.t1 - s.t0)) : 1;
	// (Bent off the line, if it is - see bend: out to its left.)
	const dx = s.to.x - s.from.x;
	const dy = s.to.y - s.from.y;
	const len = Math.hypot(dx, dy) || 1;
	const bend = s.bend ?? 0;
	const off = 4 * bend * u * (1 - u);
	const at = {
		x: s.from.x + dx * u - (dy / len) * off,
		y: s.from.y + dy * u + (dx / len) * off,
		z: s.from.z + (s.to.z - s.from.z) * u + 4 * s.peak * u * (1 - u),
	};
	// A spiral points the way it goes; end over end, it tumbles.
	const secs = (s.t1 - s.t0) / 1000 || 1;
	const turnOff = (4 * bend * (1 - 2 * u)) / secs;
	let axis = {
		x: dx / secs - (dy / len) * turnOff,
		y: dy / secs + (dx / len) * turnOff,
		z: (s.to.z - s.from.z) / secs + (4 * s.peak * (1 - 2 * u)) / secs,
	};
	if (!s.spiral) {
		const turn = spin
			? spin.from + (spin.to - spin.from) * u
			: ((t - s.t0) / 1000) * TUMBLE;
		const flat = Math.hypot(axis.x, axis.y);
		const fx = flat > 0.01 ? axis.x / flat : way.x;
		const fy = flat > 0.01 ? axis.y / flat : way.y;
		axis = {
			x: fx * Math.cos(turn),
			y: fy * Math.cos(turn),
			z: Math.sin(turn),
		};
	}
	return { at, axis };
};
// The way the ball was last going across the field before segs[k], back
// over its flights (flat, a unit long) - if it was.
const wayBefore = (segs: BallSeg[], k: number): Pt3 | undefined => {
	for (let j = k - 1; j >= 0; j--) {
		const f = segs[j]!;
		if (f.kind !== "fly") {
			return undefined;
		}
		const x = f.to.x - f.from.x;
		const y = f.to.y - f.from.y;
		const flat = Math.hypot(x, y);
		if (flat > REST_WAY_LEAST) {
			return { x: x / flat, y: y / flat, z: 0 };
		}
	}
	return undefined;
};
// The ball in flight k, as it goes (see flying).
const flightOf = (play: Play, k: number, t: number) => {
	const s = play.ball[k] as Fly;
	return flying(
		s,
		t,
		s.spiral ? undefined : tumble(play, k),
		wayBefore(play.ball, k),
	);
};

// A ball let go turns from the way it was in his hands into the way it goes
// over this long (ms) - and, caught, into the way it will be in the other
// man's over the last so long of its flight (no farther out than a stride
// or so from his hands, at a pass's pace).
const TURN_OUT_MS = 120;
const TURN_IN_MS = 80;
// The way `axis` points, turned so far (0 to 1, eased) to the way `to` does
// - whichever end of it is nearer the way it points at the end of the turn,
// `by` (the same end all the way, not first one and then the other).
const turnedTo = (axis: Pt3, to: Pt3, w: number, by: Pt3): Pt3 => {
	if (w <= 0) {
		return axis;
	}
	const v = Math.min(1, w);
	const e = v * v * (3 - 2 * v);
	const la = Math.hypot(axis.x, axis.y, axis.z) || 1;
	const lt = Math.hypot(to.x, to.y, to.z) || 1;
	const n = by.x * to.x + by.y * to.y + by.z * to.z < 0 ? -1 : 1;
	return {
		x: axis.x / la + ((n * to.x) / lt - axis.x / la) * e,
		y: axis.y / la + ((n * to.y) / lt - axis.y / la) * e,
		z: axis.z / la + ((n * to.z) / lt - axis.z / la) * e,
	};
};

// END OVER END, off the turf: how far over it has turned (radians) as a
// flight starts and as it ends - going on turning from the way the last
// one left it, one bounce into the next, not set back flat at each; and,
// coming to rest, down on its side (a half turn) as it does - lying the
// way it went (see ballAt). So many radians a second.
const TUMBLE = 9;
type Spin = { from: number; to: number };
const spins = new WeakMap<Fly, Spin>();
const tumble = (play: Play, k: number): Spin => {
	const s = play.ball[k] as Fly;
	let had = spins.get(s);
	if (had) {
		return had;
	}
	const prev = play.ball[k - 1];
	// (Off its tee, or the holder's finger: up on end as it goes.)
	const from =
		prev?.kind === "fly" && !prev.spiral && Math.abs(prev.t1 - s.t0) < 2
			? tumble(play, k - 1).to
			: prev?.kind === "spot" && prev.stood
				? Math.PI / 2
				: 0;
	let to = from + ((s.t1 - s.t0) / 1000) * TUMBLE;
	const next = play.ball[k + 1];
	if (next?.kind === "spot" && !next.stood && Math.abs(next.t0 - s.t1) < 2) {
		to =
			Math.max(Math.round(to / Math.PI), Math.ceil(from / Math.PI)) * Math.PI;
	}
	had = { from, to };
	spins.set(s, had);
	return had;
};

// Where the ball he has is drawn, his hands on it (the field's own
// coordinates, as the ball's flight is - see drawnUp): for a ball thrown,
// pitched or snapped to leave his hands, and get to the hands catching it;
// and the way its points run there, as it is drawn (up drawn up).
export type HandsOf = (
	st: FieldPlayerState,
) => (Pt3 & { axis?: Pt3 }) | undefined;

type Fly = Extract<BallSeg, { kind: "fly" }>;
type Met = { off: Pt3; on: Pt3; offWay?: Pt3; onWay?: Pt3 };
const metBy = new WeakMap<Fly, Met>();
const NONE: Pt3 = { x: 0, y: 0, z: 0 };

// How far the flight's ends are from the hands it leaves and gets to - and
// the way it is turned in them.
const handsMet = (
	play: Play,
	k: number,
	apart: Map<number, Sep[]>,
	handsOf: HandsOf,
): Met => {
	const s = play.ball[k] as Fly;
	let met = metBy.get(s);
	if (met) {
		return met;
	}
	const gap = (
		seg: BallSeg | undefined,
		t: number,
		end: Pt3,
		thrown: boolean,
	): { by: Pt3; way?: Pt3 } => {
		const part = seg?.kind === "held" ? holderOf(play, seg.pid) : undefined;
		const st = part ? stateOf(part, t, apart, play) : undefined;
		const hold =
			seg?.kind === "held" ? holdNear(play, seg.pid, t, thrown) : undefined;
		const at = st && hold ? handsOf({ ...st, ball: hold }) : undefined;
		return at
			? {
					by: { x: at.x - end.x, y: at.y - end.y, z: at.z - end.z },
					...(at.axis ? { way: at.axis } : {}),
				}
			: { by: NONE };
	};
	const next = play.ball[k + 1];
	const off = gap(play.ball[k - 1], s.t0 - 1, s.from, true);
	const on =
		next && Math.abs(next.t0 - s.t1) < 2
			? gap(next, s.t1 + 1, s.to, false)
			: { by: NONE };
	met = {
		off: off.by,
		on: on.by,
		...(off.way ? { offWay: off.way } : {}),
		...(on.way ? { onWay: on.way } : {}),
	};
	metBy.set(s, met);
	return met;
};

// The way a ball come to rest at segs[k] went, the last of its way there
// (back over its last bounces as far as it takes to have gone this far,
// feet) - or, put down where it was, none: lying up and down the field.
const REST_WAY_LEAST = 0.3;
const restWay = (segs: BallSeg[], k: number): Pt3 | undefined => {
	const at = (segs[k] as Extract<BallSeg, { kind: "spot" }>).at;
	let t = segs[k]!.t0;
	for (let j = k - 1; j >= 0; j--) {
		const f = segs[j]!;
		if (f.kind !== "fly" || Math.abs(f.t1 - t) >= 2) {
			return undefined;
		}
		const x = at.x - f.from.x;
		const y = at.y - f.from.y;
		if (Math.hypot(x, y) > REST_WAY_LEAST) {
			return { x, y, z: 0 };
		}
		t = f.t0;
	}
	return undefined;
};
// The way a ball on its side on the turf at segs[k] lies (flat, a unit
// long): as it came to rest, or put down up and down the field.
const lyingWay = (segs: BallSeg[], k: number): Pt3 => {
	const w = restWay(segs, k);
	const flat = w ? Math.hypot(w.x, w.y) : 0;
	return w && flat > 0 ? { x: w.x / flat, y: w.y / flat, z: 0 } : ALONG;
};
const ballAt = (
	play: Play,
	t: number,
	players: FieldPlayerState[],
	apart: Map<number, Sep[]>,
	handsOf: HandsOf | undefined,
): { ball?: FieldMoment["ball"]; focus?: Pt3; carrier?: Side } => {
	const segs = play.ball;
	const k = lastStarted(segs, t, (b) => b.t0);
	const s = segs[k];
	if (!s) {
		return {};
	}
	if (s.kind === "spot") {
		// (Come to rest off the turf, or tossed down onto it: on its side,
		// the way it was going - put down, up and down the field.)
		const way = s.stood ? undefined : restWay(segs, k);
		return {
			ball: {
				at: s.at,
				...(s.stood ? { stood: s.stood } : {}),
				...(way ? { lies: way } : {}),
			},
			focus: s.at,
		};
	}
	if (s.kind === "fly") {
		const f = flightOf(play, k, t);
		let met: Met | undefined;
		if (handsOf) {
			// (Out of his hands and into the other man's, the whole way
			// eased from the one to the other.)
			met = handsMet(play, k, apart, handsOf);
			const { off, on } = met;
			const u = s.t1 > s.t0 ? clamp01((t - s.t0) / (s.t1 - s.t0)) : 1;
			f.at = {
				x: f.at.x + off.x * (1 - u) + on.x * u,
				y: f.at.y + off.y * (1 - u) + on.y * u,
				z: f.at.z + off.z * (1 - u) + on.z * u,
			};
		}
		// (And turned out of the way it was - in his hands, or as it went
		// before it bounced or was knocked away - into the way it goes, and
		// into the way it will be in the other man's hands, over the first
		// and the last of its flight: not all at once.)
		const ease = Math.min(TURN_OUT_MS, (s.t1 - s.t0) / 2);
		const easeIn = Math.min(TURN_IN_MS, (s.t1 - s.t0) / 2);
		if (ease > 0 && t - s.t0 < ease) {
			const prev = segs[k - 1];
			const before =
				met?.offWay ??
				(prev?.kind === "fly" && Math.abs(prev.t1 - s.t0) < 2
					? flightOf(play, k - 1, prev.t1).axis
					: undefined);
			if (before) {
				const w = 1 - (t - s.t0) / ease;
				f.axis = turnedTo(f.axis, before, w, flightOf(play, k, s.t0).axis);
			}
		}
		if (easeIn > 0 && s.t1 - t < easeIn && met?.onWay) {
			const w = 1 - (s.t1 - t) / easeIn;
			f.axis = turnedTo(f.axis, met.onWay, w, flightOf(play, k, s.t1).axis);
		}
		// (Off a man's hands: the eye from him onto it - not to where it
		// was, out in front of him, from where he stood, in a frame.)
		const was = segs[k - 1];
		const from =
			was?.kind === "held" && t - s.t0 < FOCUS_EASE_MS
				? players.find((x) => x.id === was.pid)
				: undefined;
		return {
			ball: f,
			focus: from
				? eased(
						{ x: from.x, y: from.y, z: HELD_Z },
						f.at,
						(t - s.t0) / FOCUS_EASE_MS,
					)
				: f.at,
		};
	}
	const p = players.find((x) => x.id === s.pid);
	if (p?.ball === "snap") {
		// (The center over it: where it sits on the line, not where he is -
		// a step behind it - for the frame or two it is still there after
		// the snap.)
		return {
			focus: { x: play.losX, y: play.ballY, z: SNAP_FOCUS_Z },
			carrier: p.team,
		};
	}
	if (!p) {
		return {};
	}
	// (An official's, given it after the play: nobody's side.)
	const side = s.pid >= 0 ? { carrier: p.team } : {};
	const him = { x: p.x, y: p.y, z: HELD_Z };
	// (Into his hands - caught, or handed to him: the eye from the ball
	// there, out in front of him, onto him - not from the one to the other
	// in a frame.)
	const into = s.t0 + (segs[k - 1]?.kind === "fly" ? 0 : PASS_MS / 2);
	if (handsOf && p.ball && k > 0 && t - into < FOCUS_EASE_MS) {
		const drawn = handsOf(p);
		if (drawn) {
			return {
				focus: eased(drawn, him, (t - into) / FOCUS_EASE_MS),
				...side,
			};
		}
	}
	return { focus: him, ...side };
};
const SNAP_FOCUS_Z = 0.6;
const HELD_Z = 3.5;
// The eye from a man onto the ball as it leaves his hands, and from the
// ball onto him as it comes into them, over this long (ms).
const FOCUS_EASE_MS = 250;
const eased = (a: Pt3, b: Pt3, u: number): Pt3 => {
	const v = clamp01(u);
	const e = v * v * (3 - 2 * v);
	return {
		x: a.x + (b.x - a.x) * e,
		y: a.y + (b.y - a.y) * e,
		z: a.z + (b.z - a.z) * e,
	};
};

// HANDED OVER, NOT THROWN: the ball from one man's hands into another's (a
// handoff, a snap under center), up off the turf into a man's hands, or put
// down on it - over this long, the moment the play has it change hands in
// the middle of it (or less, with less time for it either side), from
// where it was drawn to where it will be, up over a little rise if it goes
// any way: so much a foot it goes, and no more than so much.
const PASS_MS = 240;
const PASS_RISE = 0.12;
const PASS_RISE_MOST = 1;

// Whoever has it: a man in the play, or an official given it after (see
// crew.ts ballBack).
const holderOf = (play: Play, pid: number) =>
	play.parts.find((p) => p.pid === pid) ??
	(pid < 0 ? play.crew.find((p) => p.pid === pid) : undefined);

// How he has the ball handing it over at t - the last way he had it by
// then - or taking it, the first way he has it from then on. (Not long
// before or after: it is his only so long.)
const HOLD_NEAR_MS = 600;
const holdNear = (
	play: Play,
	pid: number,
	t: number,
	giving: boolean,
): BallHold | undefined => {
	const legs = holderOf(play, pid)?.legs ?? [];
	if (giving) {
		for (let i = legs.length - 1; i >= 0; i--) {
			const l = legs[i]!;
			if (l.t0 < t && l.ball) {
				return l.t1 > t - HOLD_NEAR_MS ? l.ball : undefined;
			}
		}
		return undefined;
	}
	const l = legs.find((x) => x.t1 > t && x.ball);
	return l && l.t0 < t + HOLD_NEAR_MS ? l.ball : undefined;
};

// Which end of the ball goes first into the hands taking it (see passing),
// for each handover - by the piece of its timeline it is handed into.
const endFirst = new WeakMap<BallSeg, 1 | -1>();
const passing = (
	play: Play,
	t: number,
	players: FieldPlayerState[],
	handsOf: HandsOf,
	cuts: readonly number[],
	apart: Map<number, Sep[]>,
):
	| {
			at: Pt3;
			axis: Pt3;
			unseen: number[];
			// How long it has been on its way (ms), and from whose hands.
			since: number;
			giver?: number;
	  }
	| undefined => {
	const segs = play.ball;
	const k = lastStarted(segs, t, (b) => b.t0);
	for (const j of [k, k + 1]) {
		const a = segs[j - 1];
		const b = segs[j];
		if (
			!a ||
			!b ||
			a.kind === "fly" ||
			b.kind === "fly" ||
			(a.kind === "spot" && b.kind === "spot") ||
			(a.kind === "held" && b.kind === "held" && a.pid === b.pid)
		) {
			continue;
		}
		const t0 = b.t0;
		const after = segs[j + 1];
		let half = Math.min(
			PASS_MS / 2,
			t0 - a.t0,
			after ? after.t0 - t0 : PASS_MS / 2,
		);
		// (Not across the picture cutting away: from or to where the man
		// with it no longer is.)
		for (const c of cuts) {
			half = Math.min(half, Math.abs(c - t0));
		}
		if (half <= 0 || Math.abs(t - t0) >= half) {
			continue;
		}
		// (Each end where the ball is drawn at t: in a man's hands, the way he
		// has it just before or just after it changes hands - or on the turf.)
		const end = (
			seg: BallSeg,
			i: number,
			giving: boolean,
			when?: number,
		): { at: Pt3; axis?: Pt3 } | undefined => {
			if (seg.kind === "spot") {
				return {
					at: { ...seg.at, z: seg.at.z + restingZ(seg.stood) },
					axis: seg.stood ? UPRIGHT : lyingWay(segs, i),
				};
			}
			if (seg.kind !== "held") {
				return undefined;
			}
			const part = when === undefined ? undefined : holderOf(play, seg.pid);
			const st =
				when === undefined
					? players.find((p) => p.id === seg.pid)
					: part && stateOf(part, when, apart, play);
			const hold = holdNear(play, seg.pid, t0, giving);
			const at = st && hold ? handsOf({ ...st, ball: hold }) : undefined;
			return at ? { at, axis: at.axis } : undefined;
		};
		const giving = end(a, j - 1, true);
		const taking = end(b, j, false);
		if (!giving || !taking) {
			return undefined;
		}
		// (The end of it nearer the way it will be in the other man's hands
		// as the two of them have it either side of the handover - the same
		// end all the way through, not first one and then the other.)
		let first = endFirst.get(b);
		if (first === undefined) {
			const w0 = end(a, j - 1, true, t0 - half)?.axis;
			const w1 = end(b, j, false, t0 + half)?.axis;
			first = w0 && w1 && w0.x * w1.x + w0.y * w1.y + w0.z * w1.z < 0 ? -1 : 1;
			endFirst.set(b, first);
		}
		const from = giving.at;
		const to = taking.at;
		const u = (t - (t0 - half)) / (half * 2);
		const e = u * u * (3 - 2 * u);
		const flat = Math.hypot(to.x - from.x, to.y - from.y);
		const rise = Math.min(PASS_RISE_MOST, PASS_RISE * flat);
		// (Turned from the way it was in the one man's hands, or on the
		// turf - stood up on end, or on its side - to the way it will be in
		// the other's, whichever of its ends is nearer: not swung round to
		// point the way it goes between them.)
		const ax: Pt3 =
			flat > 0.01
				? { x: (to.x - from.x) / flat, y: (to.y - from.y) / flat, z: 0.3 }
				: { x: 1, y: 0, z: 0.3 };
		const a0 = giving.axis ?? ax;
		const a1 = taking.axis ?? ax;
		const n = giving.axis && taking.axis ? first : 1;
		const axis = {
			x: a0.x + (n * a1.x - a0.x) * e,
			y: a0.y + (n * a1.y - a0.y) * e,
			z: a0.z + (n * a1.z - a0.z) * e,
		};
		return {
			at: {
				x: from.x + (to.x - from.x) * e,
				y: from.y + (to.y - from.y) * e,
				z: from.z + (to.z - from.z) * e + 4 * rise * e * (1 - e),
			},
			axis,
			unseen: [a, b].flatMap((x) => (x.kind === "held" ? [x.pid] : [])),
			since: t - (t0 - half),
			// (Not off the center: it was where it sits on the line already.)
			...(a.kind === "held" && holdNear(play, a.pid, t0, true) !== "snap"
				? { giver: a.pid }
				: {}),
		};
	}
	return undefined;
};

// How long he is eased into a leg over: into an act, over no more than this
// share of it (or the first of it is gone by before he is into it).
const ACT_BLEND = 0.5;
// (Down into his stance on the line from on his feet, and up out of it
// slowly, the court's STOOD_MS: not dropped into it.)
const DOWN = new Set<Move>(["threePoint", "dlThree", "center"]);
// (Down on a knee the same: into it and up out of it no quicker.)
const LOW = new Set<Move>([
	...DOWN,
	"holdReady",
	"holdSpot",
	"holdKneel",
	"holdPlace",
	"holdDown",
	"kneelDown",
	"takeKnee",
	"tend",
	"kneelPoint",
	"hurtKnee",
]);
const BLEND_MOST_MS = Math.max(SETTLE_MS, STOOD_MS);
// (Down to a loose ball on the run: quickly into the scoop, the bend down
// its own - not drawn out under his strides and all at the last.)
const SCOOP_IN_MS = 100;
// (Up out of his stance at the snap and off: quickly - not slow to come
// up, swung round still down in it - most of the way up at once, and
// settled into his stride at the last; see intoOf.)
const UP_MS = 240;
const offTheSnap = (l: Leg, prev: Leg | undefined) =>
	prev !== undefined &&
	DOWN.has(prev.move as Move) &&
	moveKind(l.move) === "cycle" &&
	l.move !== "walk";
// (Up out of it otherwise - stood up, or walking off: no quicker than he
// got down into it.)
const upSlowly = (l: Leg, prev: Leg | undefined) =>
	prev !== undefined &&
	LOW.has(prev.move as Move) &&
	!LOW.has(l.move as Move) &&
	moveKind(l.move) !== "act" &&
	!offTheSnap(l, prev);
// How far he is into a leg, so far through his blend into it (and how
// fast he is going into it, per ms): eased in and settled at the last, the
// court's way (see its blendEase) - not at an even rate. Up out of his
// stance at the snap: off at once, and settled into his stride at the last
// - not still coming up as fast as ever as he gets there. (The play is only
// getting going for him then anyway - see lagged.)
const intoOf = (l: Leg, prev: Leg | undefined, u: number): number => {
	const v = clamp01(u);
	return offTheSnap(l, prev) ? 1 - (1 - v) * (1 - v) : 1 - blendEase(v, 1);
};
const intoRate = (
	l: Leg,
	prev: Leg | undefined,
	u: number,
	ms: number,
): number => {
	const v = clamp01(u);
	return offTheSnap(l, prev) ? (2 * (1 - v)) / ms : blendEaseRate(v * ms, ms);
};
// (Setting off from where he stood, a step each foot; pulled up out of his
// strides into where he stands, a step with each foot, the court's way -
// see its blendMsOf.)
const setsOff = (l: Leg, prev: Leg) =>
	PIVOTS.has(prev.move as Move) && gaitOf(l.move as Move) !== undefined;
const pullsUp = (l: Leg, prev: Leg) =>
	gaitOf(prev.move as Move) !== undefined && PIVOTS.has(l.move as Move);
const blendOf = (l: Leg, prev?: Leg): number => {
	if (moveKind(l.move) !== "act") {
		return !prev
			? BLEND_MS
			: offTheSnap(l, prev)
				? UP_MS
				: upSlowly(l, prev) ||
					  (LOW.has(l.move as Move) &&
							!LOW.has(prev.move as Move) &&
							moveKind(prev.move) !== "act")
					? STOOD_MS
					: blendMsOf(
							setsOff(l, prev),
							stepsOver(prev.move as Move, l.move as Move) || pullsUp(l, prev),
							false,
						);
	}
	if (l.move === "scoop") {
		return SCOOP_IN_MS;
	}
	const [p0, p1] = l.span ?? [0, 1];
	const ms = (l.t1 - l.t0) / Math.max(0.05, p1 - p0);
	return Math.min(BLEND_MS, ms * ACT_BLEND);
};

// Whether he does something else from one leg to the next: his move, his
// arms, how he has the ball.
// (The ball coming into his hands, or going, partway through what he is
// doing: no change to him unless it is tucked away under his arm - see
// poses.ts holding.)
// (Or the same act over again, from the first of it: eased back into it,
// not jumped there.)
const changed = (a: Leg, b: Leg) =>
	a.move !== b.move ||
	(moveKind(a.move) === "act" &&
		(b.span?.[0] ?? 0) < (a.span?.[1] ?? 1) - 1e-6) ||
	a.arms !== b.arms ||
	(a.ball !== b.ball && (tucked(a.ball) || tucked(b.ball))) ||
	!a.mirrored !== !b.mirrored;

// HOW HARD HE IS GOING AT IT on his strides, getting going or pulling up
// (see sprite.ts driven): the court's (see its driveFrom) - over a longer
// window, his pace off the play's track a rougher thing than the court's.
const DRIVE_MS = 300;
const DRIVES = new Set<Move>(["jog", "run", "sprint"]);
const driveOf = (tr: Track, t: number): number =>
	speedAt(tr, t) === undefined || speedAt(tr, t - DRIVE_MS) === undefined
		? 0
		: driveFrom((u) => speedAt(tr, u)!, t, DRIVE_MS, [0]);

// (How wide the referee's mouth opens giving a call over his microphone:
// see poses.ts TALKS.)
const MIC_TALK = 0.55;

// Where one man is at t and what he is doing.
const stateOf = (
	part: { pid: number; team: Side; legs: Leg[] },
	now: number,
	apart: Map<number, Sep[]>,
	play: Play,
): FieldPlayerState | undefined => {
	const legs = part.legs;
	if (legs.length === 0) {
		return undefined;
	}
	// (Just off his stance, where - and what - the play has him a moment
	// before.)
	const t = lagged(legs, now);
	const k = lastStarted(legs, t, (l) => l.t0);
	const l = legs[k]!;
	const offset = (part.pid * 0.618) % 1;
	const at = placeAt(legs, k, t);
	// (A sudden start or stop taken over a step - see smoothAt.)
	const track = trackOf(legs, play.t0, trackLength(play.t0, play.t1));
	const smooth = smoothAt(track, now);
	if (smooth) {
		at.x = smooth.x;
		at.y = smooth.y;
	}
	const aside = asideAt(apart.get(part.pid), now, (u) =>
		PIVOTS.has(legs[lastStarted(legs, u, (x) => x.t0)]!.move as Move),
	);
	if (aside) {
		at.x += aside.x;
		at.y += aside.y;
	}
	// How far round his strides he is, and a moment on.
	const strides = stridesOf(legs, play.t0, trackLength(play.t0, play.t1));
	const stride = strides
		? strideAt(strides, now)
		: { ph: 0, len: [1, 1] as [number, number] };
	const ahead = strides ? strideAt(strides, now + RATE_MS).ph : 0;
	// The leg he last changed what he was doing at (on through any more of
	// it since), for him to be eased out of what he was before it - and, that
	// one too short for him to have been eased all the way into it, the one
	// before that, still part of what he was.
	const changedAt = (j: number, at: number): number => {
		while (
			j > 0 &&
			at - legs[j]!.t0 < BLEND_MOST_MS &&
			!changed(legs[j - 1]!, legs[j]!) &&
			joined(legs[j - 1]!, legs[j]!)
		) {
			j--;
		}
		return j;
	};
	const c = changedAt(k, t);
	const leftOf = (j: number, j2: number) =>
		j2 > 0 && changed(legs[j2 - 1]!, legs[j2]!)
			? 1 -
				intoOf(
					legs[j2]!,
					legs[j2 - 1],
					(legs[j]!.t0 - legs[j2]!.t0) / blendOf(legs[j2]!, legs[j2 - 1]),
				)
			: 0;
	// (And that one, then, itself still partly what he was before it - and
	// so on back, as far as the court's go: see its BLEND_DEPTH. Each the leg
	// it changed into, and how much of what he was before it was left.)
	const chain: { j: number; left: number }[] = [];
	for (let j = c; chain.length < BLEND_DEPTH - 1; ) {
		const next = j > 1 ? changedAt(j - 1, legs[j]!.t0) : 0;
		const left = leftOf(j, next);
		if (left <= 0) {
			break;
		}
		chain.push({ j: next, left });
		j = next;
	}
	// (What he was under all that, outermost first.)
	type Under = NonNullable<NonNullable<FieldPlayerState["from"]>["under"]>;
	const underFrom = (i: number): Under | undefined => {
		const at = chain[i];
		if (!at) {
			return undefined;
		}
		const deeper = underFrom(i + 1);
		return {
			...underOf(legs[at.j - 1]!, at.left),
			...(deeper ? { under: deeper } : {}),
		};
	};
	const yaw = yawAt(legs, t);
	// Stood turning, a foot at a time (or not yet - or no longer - turning,
	// each foot where he faced as he stood, or as it stood on the turf).
	const pivots = (u: Leg) => PIVOTS.has(u.move as Move);
	const reach = PIVOT_MS * 2 + TURN_EASE_HALF * TURN_EASE_MS;
	const feetAt = (at: number): Feet | undefined => {
		if (
			cutNear(facingOf(legs), at, reach) ||
			steady(legs, at - reach, at + reach)
		) {
			return undefined;
		}
		const p = pivotOf(legs, at);
		return square(p, yawAt(legs, at)) ? undefined : p;
	};
	// (Or thrown off his front foot - see PLANTS: where it landed.)
	const plantOf = (j: number): number | undefined => {
		const on = (u: Leg) => {
			const p = PLANTS.get(u.move as Move);
			return p !== undefined && (u.span?.[0] ?? 0) >= p - 1e-6;
		};
		if (!on(legs[j]!)) {
			return undefined;
		}
		let i = j;
		while (i > 0 && on(legs[i - 1]!)) {
			i--;
		}
		return yawAt(legs, legs[i]!.t0);
	};
	const feetOf = (j: number, at: number, faced: number): Feet | undefined => {
		const u = legs[j]!;
		if (pivots(u)) {
			return feetAt(at);
		}
		const p = plantOf(j);
		return p === undefined
			? undefined
			: u.mirrored
				? [p, faced, 0, 0]
				: [faced, p, 0, 0];
	};
	const feetKind = (j: number) =>
		pivots(legs[j]!) ? 1 : plantOf(j) !== undefined ? 2 : 0;
	const phase = phaseOf(l, t, offset, stride.ph);
	const z = liftOf(l.move, phase);
	// (Talking or yelling, his mouth open.)
	const mouth = l.talk
		? mouthAt(part.pid, t, MIC_TALK)
		: mouthOf(part.pid, t, l.move as Move, phase);
	const turned = z <= 0 ? feetOf(k, t, yaw) : undefined;
	const pivot: Feet | undefined =
		turned ??
		(aside?.up && z <= 0 && PIVOTS.has(l.move as Move)
			? [yaw, yaw, aside.up[0], aside.up[1]]
			: undefined);

	// (Just changed what he is doing, his arms, or how he has the ball:
	// eased out of what he was - his strides, if it was a stride, going on
	// round in step with these; anything else, as it was when it ended.)
	let from: FieldPlayerState["from"];
	const prior = c > 0 ? legs[c - 1]! : undefined;
	const wasOf = (u: Leg) =>
		cycles(u) ? phaseOf(u, t, offset, stride.ph) : phaseOf(u, u.t1, offset);
	// (What he was before that, as he was when it ended.)
	const underOf = (u: Leg, w: number) => ({
		move: u.move as Move,
		phase: wasOf(u),
		...(u.mirrored ? { mirrored: true } : {}),
		w,
		...(u.arms ? { arms: { move: u.arms, phase: armsPhaseOf(u, u.t1) } } : {}),
	});
	const blended = t - legs[c]!.t0;
	const blending = blendOf(legs[c]!, legs[c - 1]);
	if (
		prior &&
		blended < blending &&
		changed(legs[c - 1]!, legs[c]!) &&
		joined(legs[c - 1]!, legs[c]!)
	) {
		// (And how fast each goes through its frames, a moment on.)
		const was = wasOf(prior);
		const rateOf = (u: Leg, now: number) =>
			cycles(u)
				? (ahead - stride.ph) / RATE_MS
				: u === l
					? (phaseOf(u, t + RATE_MS, offset) - now) / RATE_MS
					: 0;
		from = {
			move: prior.move as Move,
			phase: was,
			rate: {
				move: rateOf(l, phase),
				from: rateOf(prior, was),
				w: intoRate(legs[c]!, prior, blended / blending, blending),
			},
			...(prior.mirrored ? { mirrored: true } : {}),
			w: 1 - intoOf(legs[c]!, prior, blended / blending),
			...(prior.arms
				? { arms: { move: prior.arms, phase: armsPhaseOf(prior, prior.t1) } }
				: {}),
			...(prior.ball ? { ball: prior.ball } : {}),
			...(chain.length > 0 ? { under: underFrom(0) } : {}),
			...(stepsOver(prior.move as Move, l.move as Move) &&
			steady(legs, legs[c]!.t0 - reach, legs[c]!.t0 + blending + reach)
				? { steps: true as const }
				: {}),
		};
		// (Into one he stands his feet in, each foot pointing on the turf
		// where it did as it changed - he turns on the foot he is down on,
		// not with it slid round under him; out of one into one he moves
		// his feet in, each turned under him as it was, on the move with
		// him.)
		const kindWas = feetKind(c - 1);
		const kindNow = feetKind(c);
		if (kindWas !== kindNow) {
			const at = legs[c]!.t0;
			const faced = yawAt(legs, at);
			const had = kindWas ? feetOf(c - 1, at, faced) : undefined;
			const by = had && !kindNow ? yaw - faced : 0;
			if (had || kindNow) {
				from.feet = had
					? [had[0] + by, had[1] + by, had[2], had[3]]
					: [faced, faced, 0, 0];
			}
		}
	}
	// (Getting going or pulling up on his strides - or just into them, as
	// much as his pose already is them; just off them into anything else, as
	// hard as he was going at it as he came off them, as much as his pose
	// still is them - what he was before that too, if it still shows: not
	// thrown back as he is stopped in a hit, say, nor straightened up in a
	// frame as the run under a catch gives out.)
	let drive = 0;
	let share = 1;
	let off = now;
	const shows = [{ move: l.move as Move, w: from?.w ?? 0, at: t }];
	if (from) {
		shows.push({ move: from.move, w: from.under?.w ?? 0, at: legs[c]!.t0 });
		let u = from.under;
		for (const at of chain) {
			if (!u) {
				break;
			}
			shows.push({ move: u.move, w: u.under?.w ?? 0, at: legs[at.j]!.t0 });
			u = u.under;
		}
	}
	for (const [i, u] of shows.entries()) {
		const striding = DRIVES.has(u.move as Move);
		if (!striding || i === 0 || !DRIVES.has(shows[i - 1]!.move as Move)) {
			off = now - (t - u.at);
		}
		if (striding) {
			drive += driveOf(track, off) * share * (1 - u.w);
		}
		share *= u.w;
	}
	// Each foot down on his strides pointing where it came down - or on the
	// ones he is just easing out of.
	const own = gaitOf(l.move as Move);
	const under = own && !pivot && z <= 0 ? legsUnder(track, now, own, yaw) : 0;
	const gait = own ?? (from && gaitOf(from.move));
	// (Which way his feet point at a moment of his strides: the way he faces
	// then, his legs turned under him.)
	const footAt = (u: number) => {
		const faced = yawAt(legs, lagged(legs, u));
		return gait
			? faced - (legsUnder(track, u, gait, faced) * Math.PI) / 180
			: faced;
	};
	const lock =
		!pivot && z <= 0 && gait && strides
			? strideLock(strides, gait, stride.ph, footAt, now, under)
			: undefined;
	// (Short steps, if he is striding - or just was.)
	const short =
		Math.min(stride.len[0], stride.len[1]) < LEN_FULL &&
		(beatOf(l.move) > 0 || (from !== undefined && beatOf(from.move) > 0));
	return {
		id: part.pid,
		team: part.team,
		x: at.x,
		y: at.y,
		z,
		yaw,
		move: l.move,
		phase,
		...(l.mirrored ? { mirrored: true } : {}),
		...(short ? { len: stride.len } : {}),
		...(drive ? { drive } : {}),
		...(pivot ? { pivot } : {}),
		...(lock ? { lock } : {}),
		...(under ? { legs: under } : {}),
		...(from ? { from } : {}),
		...(l.ball ? { ball: l.ball } : {}),
		...(l.arms ? { arms: { move: l.arms, phase: armsPhaseOf(l, t) } } : {}),
		...(l.bare ? { bare: true as const } : {}),
		...(mouth > 0 ? { mouth } : {}),
	};
};
export const momentAt = (
	tl: FieldTimeline,
	t: number,
	handsOf?: HandsOf,
): FieldMoment => {
	const play = playAt(tl, t);
	if (!play) {
		return { t, play, players: [] };
	}
	const players: FieldPlayerState[] = [];
	const apart = apartOf(play);
	const reporter = play.postgame?.interview?.reporter;
	for (const part of [
		...play.parts,
		...play.crew,
		...play.chains,
		...play.tv,
		// (The trainers.)
		...(play.medics ?? []),
		...(play.coaches ?? []),
		// (The sideline reporter, once she is out for the interview.)
		...(reporter && t >= reporter.legs[0]!.t0 ? [reporter] : []),
	]) {
		const st = stateOf(part, t, apart, play);
		if (st) {
			players.push(st);
		}
	}
	const held = ballAt(play, t, players, apart, handsOf);
	const carrier = held.carrier;
	let { ball, focus } = held;
	// (Over it, getting his hand on it - but it on the turf still, not yet
	// his: the one ball, there.)
	if (ball) {
		for (const st of players) {
			if (st.ball === "snap") {
				st.unseen = true;
			}
		}
	}
	const over = handsOf && passing(play, t, players, handsOf, tl.cuts, apart);
	if (over) {
		for (const st of players) {
			if (over.unseen.includes(st.id) && st.ball) {
				st.unseen = true;
			}
		}
		ball = { at: over.at, axis: over.axis };
		// (The eye off the man giving it onto it as it goes.)
		const giver =
			over.since < FOCUS_EASE_MS
				? players.find((st) => st.id === over.giver)
				: undefined;
		focus = giver
			? eased(
					{ x: giver.x, y: giver.y, z: HELD_Z },
					over.at,
					over.since / FOCUS_EASE_MS,
				)
			: over.at;
	}
	const flags = play.flags
		.filter((f) => t >= f.t0)
		.map((f) => {
			// (Tossed up, and fluttering down slower than it went up.)
			const u = clamp01((t - f.t0) / (f.t1 - f.t0));
			const rise = u < 0.35 ? u / 0.35 : 1 - (u - 0.35) / 0.65;
			return {
				at: {
					x: f.from.x + (f.to.x - f.from.x) * u,
					y: f.from.y + (f.to.y - f.from.y) * u,
					z:
						f.from.z * (1 - u) +
						(f.rise ?? FLAG_RISE) * Math.sin((rise * Math.PI) / 2),
				},
				down: u >= 1,
				turn: u >= 1 ? f.t0 % 6.28 : ((t - f.t0) / 1000) * 7,
			};
		});
	// (A bean bag: tossed low, end over end, down quick.)
	const bags = (play.bags ?? [])
		.filter((f) => t >= f.t0)
		.map((f) => {
			const u = clamp01((t - f.t0) / (f.t1 - f.t0));
			return {
				at: {
					x: f.from.x + (f.to.x - f.from.x) * u,
					y: f.from.y + (f.to.y - f.from.y) * u,
					z: f.from.z * (1 - u) + BAG_RISE * 4 * u * (1 - u),
				},
				down: u >= 1,
				turn: u >= 1 ? f.t0 % 6.28 : ((t - f.t0) / 1000) * 9,
			};
		});
	const tee = teeLeft(play, t);
	return {
		t,
		play,
		players,
		...(ball ? { ball } : {}),
		...(focus ? { focus } : {}),
		...(carrier !== undefined ? { carrier } : {}),
		...(flags.length > 0 ? { flags } : {}),
		...(bags.length > 0 ? { bags } : {}),
		...(tee ? { tee } : {}),
	};
};
// The tee, once the ball is kicked off it: knocked on this far the way the
// kick goes, up off the turf this high over it, in this long.
const TEE_KNOCK = 1.6;
const TEE_HOP = 0.5;
const TEE_HOP_MS = 260;
const teeLeft = (play: Play, t: number): Pt3 | undefined => {
	const k = play.ball.findIndex((b) => b.kind === "spot" && b.stood === "tee");
	const on = play.ball[k];
	const off = play.ball[k + 1];
	if (k < 0 || on?.kind !== "spot" || !off || t < off.t0) {
		return undefined;
	}
	const u = clamp01((t - off.t0) / TEE_HOP_MS);
	return {
		x: on.at.x + play.dir * TEE_KNOCK * u,
		y: on.at.y,
		z: TEE_HOP * 4 * u * (1 - u),
	};
};
// How high over the straight way down a bean bag goes, halfway.
const BAG_RISE = 1.2;

// How high over his hand a flag goes.
const FLAG_RISE = 5;
