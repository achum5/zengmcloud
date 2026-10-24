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
	lagged,
	paceOf,
	placeAt,
	SET,
	smoothAt,
	TRACK_MS,
	trackLength,
	trackOf,
} from "./pace.ts";
import {
	beatOf,
	liftOf,
	loopRate,
	moveKind,
	stepsOver,
	strideOf,
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
	ball?: { at: Pt3; stood?: "tee" | "hold"; axis?: Pt3 };
	// Where the ball is, wherever it is, and the side with it if a man has
	// it: for the camera to follow.
	focus?: Pt3;
	carrier?: Side;
	// The flags thrown: in the air (turning over as it comes down), or down
	// on the turf.
	flags?: { at: Pt3; down: boolean; turn: number }[];
};

// From one move into the next, his pose eased over this long; and his
// turn, from the way he was facing to the way he faces now.
const BLEND_MS = 260;
const TURN_MS = 380;
// (Turning where he stands, no quicker round than STAND_TURN a second: his
// feet stepping round after him - see pivotOf.)
const STAND_TURN = 5;
// (Round into the way he goes, from where he stood - with his first steps,
// as he starts off: see pace.ts paceOf early - or from going another way:
// quicker. Off his stance at the snap: quicker still, round as he comes up
// out of it with his first step (on the clock he is on, slow just off it -
// not swung round in his stance before he is up out of it). But
// never round quicker than TURN_MOST a second - walking, half that - nor
// round a corner, if it comes too sharp for that so quick.)
const TURN_GO_MS = 240;
const TURN_SNAP_MS = 150;
const TURN_MOST = 12;
// (Coming up into his stance for the snap, square to it as he gets there:
// round into it over the last of his way up.)
const SET_UP_MS = 300;
// (Out of a backpedal into a run the same way - his hips opened, round
// as he backpedals the last of it, this long (ms) at most: not running
// on backwards as he comes round.)
const FLIP_LEAD_MS = 130;
const FLIP_SAME = Math.PI / 4;
const turnMost = (l: Leg) => (l.move === "walk" ? TURN_MOST / 2 : TURN_MOST);
// (How fast he goes through a move: taken over this long.)
const RATE_MS = 5;

const clamp01 = (u: number) => Math.min(1, Math.max(0, u));

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
// most of any of that starts.)
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
const facings = new WeakMap<Leg[], Facing>();
// (A short way he goes his own way along, faced a little the way he goes on
// after it - where he will be LOOK_MS from its start, or the way he is to
// face next if he squares up to something before then - as near that as
// CRAB_MOST off his own way: not round one way for a step and then back
// the other.)
const LOOK_MS = 500;
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
	for (let k = j + 1; k < legs.length; k++) {
		const n = legs[k]!;
		if (Math.abs(n.t0 - legs[k - 1]!.t1) > 1 || n.turn) {
			break;
		}
		if (n.yaw !== undefined && !ownWay(n)) {
			next = n.yaw;
			break;
		}
		if (!goes(n)) {
			break;
		}
		const u = n.t1 > by ? (by - n.t0) / (n.t1 - n.t0) : 1;
		x = n.from.x + (n.to.x - n.from.x) * u;
		y = n.from.y + (n.to.y - n.from.y) * u;
		if (n.t1 >= by) {
			break;
		}
	}
	const d = wrapAngle((next ?? Math.atan2(y - l.from.y, x - l.from.x)) - aim);
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
		const lp = legs[j - 1];
		const startsOff = own && lp && !goes(lp) && !lp.turn;
		const setsUp = !goes(lj) && lp && goes(lp) && SET.has(lj.move);
		// (Or down on a knee: round as he gets there - or, stood, round on
		// his feet first, all the way, and then down.)
		const flips =
			own &&
			lp !== undefined &&
			lp.move === "back" &&
			goes(lp) &&
			Math.abs(lp.t1 - lj.t0) <= 1 &&
			Math.abs(
				wrapAngle(
					Math.atan2(lj.to.y - lj.from.y, lj.to.x - lj.from.x) -
						Math.atan2(lp.to.y - lp.from.y, lp.to.x - lp.from.x),
				),
			) < FLIP_SAME;
		const kneels =
			lj.move === "takeKnee" &&
			lp &&
			!lp.turn &&
			(goes(lp) || PIVOTS.has(lp.move));
		const stood = kneels && !goes(lp);
		lead[j] =
			startsOff && !SET.has(lp.move)
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
		let by = j > 0 ? wrapAngle(was(lead[j]!) - aim) : 0;
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
		ms[j] =
			(PIVOTS.has(lj.move) && !goes(lj)) || stood
				? Math.max(TURN_MS, (Math.abs(by) / STAND_TURN) * 1000)
				: snap[j] || own
					? Math.max(
							snap[j] ? TURN_SNAP_MS : TURN_GO_MS,
							(Math.abs(by) / turnMost(lj)) * 1000,
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

// TURNING WHERE HE STANDS (the way the court has it - see court3d's
// pivotOf): his feet don't spin under him with the rest of him. One stays
// where it was, turned on, while the other steps round after him - every
// PIVOT_MS the other way about, a PIVOT_STEP_MS step up off the turf to
// where he will be facing by the end of it. Which way each foot points on
// the turf, and how far up off it (a full step 1): drawn turned under him
// as far as hips turn (see sprite.ts).
const PIVOT_MS = 160;
const PIVOT_STEP_MS = 110;
// (How far round a step is a full one, radians.)
const PIVOT_FULL = 0.35;
const PIVOTS = new Set<Move>([
	"stand",
	"armsFolded",
	"onHips",
	"hands",
	"pointSky",
	"qbSet",
	"pocket",
	"shotgun",
	"settle",
	"overShoulders",
	"carryWater",
	"coolerCarry",
	"shoulderCam",
	"holdPole",
	"ballUp",
	"shoot",
	"kickReady",
	"shove",
	"stiffArm",
	"receiverStance",
	"cornerStance",
	"safetyStance",
	"backerStance",
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
const pivotOf = (legs: Leg[], t: number): Feet => {
	const k = Math.floor(t / PIVOT_MS);
	const g = k * PIVOT_MS;
	// (Right steps round on even beats, left on odd.)
	const foot = (even: boolean): [number, number] => {
		if ((k % 2 === 0) !== even) {
			return [yawAt(legs, g), 0];
		}
		const a = yawAt(legs, g - PIVOT_MS);
		const d = wrapAngle(yawAt(legs, g + PIVOT_MS) - a);
		const u = clamp01((t - (g + PIVOT_MS - PIVOT_STEP_MS)) / PIVOT_STEP_MS);
		return [
			a + d * u * u * (3 - 2 * u),
			Math.sin(Math.PI * u) * Math.min(1, Math.abs(d) / PIVOT_FULL),
		];
	};
	const [r, rl] = foot(true);
	const [l, ll] = foot(false);
	return [r, l, rl, ll];
};
// (Nothing to show: both feet where he faces, neither up.)
const square = (p: Feet, yaw: number) =>
	Math.abs(wrapAngle(p[0] - yaw)) < 0.01 &&
	Math.abs(wrapAngle(p[1] - yaw)) < 0.01 &&
	p[2] < 0.01 &&
	p[3] < 0.01;

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
	return strides + offset;
};

// HIS STRIDES, worked out step by step through the play (every TRACK_MS,
// once for his legs as they are - see trackOf): how far round them he is -
// by the ground he covers, carried on from one stretch to the next so his
// feet keep their rhythm, held while he does anything else - and how long
// they are, as a share of a full one. Getting going, or pulling up, short
// quick steps (see court3d/poses.ts shortSteps): below the pace a full
// stride makes at its gait's beat, each that much shorter (no shorter than
// LEN_MIN, and no quicker round than BEAT_MOST a second), by his pace over
// PACE_MS either side, changing no quicker than LEN_RATE a second.
const LEN_MIN = 0.1;
// (As good as a full one.)
const LEN_FULL = 0.98;
const BEAT_MOST = 2.6;
const PACE_MS = 100;
const LEN_RATE = 6;
// (Steps that keep no beat - his feet going under him in a block, or
// kick-sliding back - round no quicker than this a second: on into one
// still at a run, his feet not a blur, but sliding the last of it.)
const SHUFFLE_MOST = 3.5;
type Strides = { t0: number; ph: Float32Array; len: Float32Array };
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
	const beatOn = legs.map((l) => beatOf(l.move));
	const gaitAt = (i: number) => {
		const k = tr.late[i]!;
		const on = tr.on[i]!;
		return strideOn[k]! > 0 ? k : strideOn[on]! > 0 ? on : -1;
	};
	const gone = new Float64Array(n);
	for (let i = 1; i < n; i++) {
		const dx = tr.xs[i]! - tr.xs[i - 1]!;
		const dy = tr.ys[i]! - tr.ys[i - 1]!;
		gone[i] = gone[i - 1]! + Math.sqrt(dx * dx + dy * dy);
	}
	const ph = new Float32Array(n);
	const len = new Float32Array(n);
	const first = strideOn.findIndex((x) => x > 0);
	let stride = first >= 0 ? strideOn[first]! : 1;
	let beat = first >= 0 ? beatOn[first]! : 0;
	let p = 0;
	let l = LEN_MIN;
	const dt = TRACK_MS / 1000;
	const w = Math.max(1, Math.round(PACE_MS / TRACK_MS));
	for (let i = 0; i < n; i++) {
		// (Striding at either end of the step, on it.)
		const g = gaitAt(i);
		const j = g >= 0 || i === 0 ? g : gaitAt(i - 1);
		if (j >= 0) {
			stride = strideOn[j]!;
			beat = beatOn[j]!;
		}
		const lo = Math.max(0, i - w);
		const hi = Math.min(n - 1, i + w);
		const v = hi > lo ? (gone[hi]! - gone[lo]!) / ((hi - lo) * dt) : 0;
		const want =
			beat > 0 ? Math.min(1, Math.max(LEN_MIN, v / (beat * stride))) : 1;
		l =
			i === 0
				? want
				: l + Math.min(LEN_RATE * dt, Math.max(-LEN_RATE * dt, want - l));
		if (i > 0 && j >= 0) {
			const d = gone[i]! - gone[i - 1]!;
			if (beat > 0) {
				const most = Math.max(BEAT_MOST * dt, d / stride);
				if (d / (stride * l) > most) {
					l = d / (stride * most);
				}
				p += d / (stride * l);
			} else {
				p += Math.min(d / stride, SHUFFLE_MOST * dt);
			}
		}
		ph[i] = p;
		len[i] = l;
	}
	const s = { t0, ph, len };
	strided.set(legs, { n: legs.length, t0, s });
	return s;
};
// How far round his strides he is at t, and how long they are.
const strideAt = (s: Strides, t: number): { ph: number; len: number } => {
	const n = s.ph.length;
	const f = Math.min(n - 1, Math.max(0, (t - s.t0) / TRACK_MS));
	const i = Math.min(n - 2, Math.floor(f));
	const u = f - i;
	return {
		ph: s.ph[i]! + (s.ph[i + 1]! - s.ph[i]!) * u,
		len: s.len[i]! + (s.len[i + 1]! - s.len[i]!) * u,
	};
};

// How far his arms are through an act of their own (see Leg.arms).
const armsPhaseOf = (l: Leg, t: number): number => {
	const [p0, p1] = l.armsSpan ?? [0, 1];
	return p0 + (p1 - p0) * along(l, t);
};

// The ball in the air: along the straight line, up over it in a parabola.
const flying = (
	s: Extract<BallSeg, { kind: "fly" }>,
	t: number,
): { at: Pt3; axis: Pt3 } => {
	const u = s.t1 > s.t0 ? clamp01((t - s.t0) / (s.t1 - s.t0)) : 1;
	const at = {
		x: s.from.x + (s.to.x - s.from.x) * u,
		y: s.from.y + (s.to.y - s.from.y) * u,
		z: s.from.z + (s.to.z - s.from.z) * u + 4 * s.peak * u * (1 - u),
	};
	// A spiral points the way it goes; end over end, it tumbles.
	const secs = (s.t1 - s.t0) / 1000 || 1;
	let axis = {
		x: (s.to.x - s.from.x) / secs,
		y: (s.to.y - s.from.y) / secs,
		z: (s.to.z - s.from.z) / secs + (4 * s.peak * (1 - 2 * u)) / secs,
	};
	if (!s.spiral) {
		const turn = ((t - s.t0) / 1000) * 9;
		const flat = Math.hypot(axis.x, axis.y) || 1;
		axis = {
			x: (axis.x / flat) * Math.cos(turn),
			y: (axis.y / flat) * Math.cos(turn),
			z: Math.sin(turn),
		};
	}
	return { at, axis };
};

// Where the ball he has is drawn, his hands on it (the field's own
// coordinates, as the ball's flight is - see drawnUp): for a ball thrown,
// pitched or snapped to leave his hands, and get to the hands catching it.
export type HandsOf = (st: FieldPlayerState) => Pt3 | undefined;

type Fly = Extract<BallSeg, { kind: "fly" }>;
const metBy = new WeakMap<Fly, { off: Pt3; on: Pt3 }>();
const NONE: Pt3 = { x: 0, y: 0, z: 0 };

// How far the flight's ends are from the hands it leaves and gets to.
const handsMet = (
	play: Play,
	k: number,
	apart: Map<number, Sep[]>,
	handsOf: HandsOf,
): { off: Pt3; on: Pt3 } => {
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
	): Pt3 => {
		const part =
			seg?.kind === "held"
				? play.parts.find((p) => p.pid === seg.pid)
				: undefined;
		const st = part ? stateOf(part, t, apart, play) : undefined;
		const hold =
			seg?.kind === "held" ? holdNear(play, seg.pid, t, thrown) : undefined;
		const at = st && hold ? handsOf({ ...st, ball: hold }) : undefined;
		return at ? { x: at.x - end.x, y: at.y - end.y, z: at.z - end.z } : NONE;
	};
	const next = play.ball[k + 1];
	met = {
		off: gap(play.ball[k - 1], s.t0 - 1, s.from, true),
		on:
			next && Math.abs(next.t0 - s.t1) < 2
				? gap(next, s.t1 + 1, s.to, false)
				: NONE,
	};
	metBy.set(s, met);
	return met;
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
		return {
			ball: { at: s.at, ...(s.stood ? { stood: s.stood } : {}) },
			focus: s.at,
		};
	}
	if (s.kind === "fly") {
		const f = flying(s, t);
		if (handsOf) {
			// (Out of his hands and into the other man's, the whole way
			// eased from the one to the other.)
			const { off, on } = handsMet(play, k, apart, handsOf);
			const u = s.t1 > s.t0 ? clamp01((t - s.t0) / (s.t1 - s.t0)) : 1;
			f.at = {
				x: f.at.x + off.x * (1 - u) + on.x * u,
				y: f.at.y + off.y * (1 - u) + on.y * u,
				z: f.at.z + off.z * (1 - u) + on.z * u,
			};
		}
		return { ball: f, focus: f.at };
	}
	const p = players.find((x) => x.id === s.pid);
	return p ? { focus: { x: p.x, y: p.y, z: 3.5 }, carrier: p.team } : {};
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
	const legs = play.parts.find((p) => p.pid === pid)?.legs ?? [];
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

const passing = (
	play: Play,
	t: number,
	players: FieldPlayerState[],
	handsOf: HandsOf,
): { at: Pt3; axis: Pt3; unseen: number[] } | undefined => {
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
		const half = Math.min(
			PASS_MS / 2,
			t0 - a.t0,
			after ? after.t0 - t0 : PASS_MS / 2,
		);
		if (half <= 0 || Math.abs(t - t0) >= half) {
			continue;
		}
		// (Each end where the ball is drawn at t: in a man's hands, the way he
		// has it just before or just after it changes hands - or on the turf.)
		const end = (seg: BallSeg, giving: boolean): Pt3 | undefined => {
			if (seg.kind === "spot") {
				return { ...seg.at, z: seg.at.z + restingZ(seg.stood) };
			}
			if (seg.kind !== "held") {
				return undefined;
			}
			const st = players.find((p) => p.id === seg.pid);
			const hold = holdNear(play, seg.pid, t0, giving);
			return st && hold ? handsOf({ ...st, ball: hold }) : undefined;
		};
		const from = end(a, true);
		const to = end(b, false);
		if (!from || !to) {
			return undefined;
		}
		const u = (t - (t0 - half)) / (half * 2);
		const e = u * u * (3 - 2 * u);
		const flat = Math.hypot(to.x - from.x, to.y - from.y);
		const rise = Math.min(PASS_RISE_MOST, PASS_RISE * flat);
		const ax =
			flat > 0.01
				? { x: (to.x - from.x) / flat, y: (to.y - from.y) / flat, z: 0.3 }
				: { x: 1, y: 0, z: 0.3 };
		// (Set down on end, stood up as it goes.)
		const up = b.kind === "spot" && b.stood ? e : 0;
		return {
			at: {
				x: from.x + (to.x - from.x) * e,
				y: from.y + (to.y - from.y) * e,
				z: from.z + (to.z - from.z) * e + 4 * rise * e * (1 - e),
			},
			axis: {
				x: ax.x * (1 - up),
				y: ax.y * (1 - up),
				z: ax.z + (1 - ax.z) * up,
			},
			unseen: [a, b].flatMap((x) => (x.kind === "held" ? [x.pid] : [])),
		};
	}
	return undefined;
};

// How long he is eased into a leg over: into an act, over no more than this
// share of it (or the first of it is gone by before he is into it).
const ACT_BLEND = 0.5;
// (From one way of standing into another, a step with each foot - see
// stepsOver: longer. Down into his stance on the line from on his feet:
// longer still, not dropped into it.)
const SETTLE_MS = 380;
const GET_DOWN_MS = 480;
const DOWN = new Set<Move>(["threePoint", "dlThree", "center"]);
const BLEND_MOST_MS = Math.max(SETTLE_MS, GET_DOWN_MS);
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
	DOWN.has(prev.move as Move) &&
	!DOWN.has(l.move as Move) &&
	moveKind(l.move) !== "act" &&
	!offTheSnap(l, prev);
// How far he is into a leg, so far through his blend into it (and how
// fast he is going into it, per ms): down into his stance, or up out of
// it slowly, eased in and settled at the last - not at an even rate,
// dropped into it. Up out of it at the snap: off at once, and settled into
// his stride at the last - not still coming up as fast as ever as he gets
// there. (The play is only getting going for him then anyway - see
// lagged.)
const eases = (l: Leg, prev: Leg | undefined) =>
	DOWN.has(l.move as Move) || upSlowly(l, prev);
const intoOf = (l: Leg, prev: Leg | undefined, u: number): number => {
	const v = clamp01(u);
	return eases(l, prev)
		? v * v * (3 - 2 * v)
		: offTheSnap(l, prev)
			? 1 - (1 - v) * (1 - v)
			: v;
};
const intoRate = (
	l: Leg,
	prev: Leg | undefined,
	u: number,
	ms: number,
): number => {
	const v = clamp01(u);
	return (
		(eases(l, prev) ? 6 * v * (1 - v) : offTheSnap(l, prev) ? 2 * (1 - v) : 1) /
		ms
	);
};
const blendOf = (l: Leg, prev?: Leg): number => {
	if (moveKind(l.move) !== "act") {
		return !prev
			? BLEND_MS
			: offTheSnap(l, prev)
				? UP_MS
				: upSlowly(l, prev)
					? GET_DOWN_MS
					: DOWN.has(l.move as Move) &&
						  !DOWN.has(prev.move as Move) &&
						  moveKind(prev.move) !== "act"
						? GET_DOWN_MS
						: stepsOver(prev.move as Move, l.move as Move)
							? SETTLE_MS
							: BLEND_MS;
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
	(a.ball !== b.ball && (a.ball === "carry" || b.ball === "carry")) ||
	!a.mirrored !== !b.mirrored;

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
	const smooth = smoothAt(
		trackOf(legs, play.t0, trackLength(play.t0, play.t1)),
		now,
	);
	if (smooth) {
		at.x = smooth.x;
		at.y = smooth.y;
	}
	const aside = apartAt(apart.get(part.pid), now);
	if (aside) {
		at.x += aside.x;
		at.y += aside.y;
	}
	// How far round his strides he is, and a moment on.
	const strides = stridesOf(legs, play.t0, trackLength(play.t0, play.t1));
	const stride = strides ? strideAt(strides, now) : { ph: 0, len: 1 };
	const ahead = strides ? strideAt(strides, now + RATE_MS).ph : 0;
	// The leg he last changed what he was doing at (on through any more of
	// it since), for him to be eased out of what he was before it - and, that
	// one too short for him to have been eased all the way into it, the one
	// before that, still part of what he was.
	const changedAt = (j: number, at: number): number => {
		while (
			j > 0 &&
			at - legs[j]!.t0 < BLEND_MOST_MS &&
			!changed(legs[j - 1]!, legs[j]!)
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
	const c2 = c > 1 ? changedAt(c - 1, legs[c]!.t0) : 0;
	const left = leftOf(c, c2);
	// (And that one, then, itself still partly what he was before it.)
	const c3 = left > 0 && c2 > 1 ? changedAt(c2 - 1, legs[c2]!.t0) : 0;
	const left2 = left > 0 ? leftOf(c2, c3) : 0;
	const yaw = yawAt(legs, t);
	// Stood turning, a foot at a time (or not yet - or no longer - turning,
	// each foot where he faced as he stood, or as it stood on the turf).
	const pivots = (u: Leg) => PIVOTS.has(u.move as Move);
	const reach = PIVOT_MS * 2 + TURN_EASE_HALF * TURN_EASE_MS;
	const feetAt = (at: number): Feet | undefined => {
		if (steady(legs, at - reach, at + reach)) {
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
	const pivot = z <= 0 ? feetOf(k, t, yaw) : undefined;
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
	if (prior && blended < blending && changed(legs[c - 1]!, legs[c]!)) {
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
			...(left > 0
				? {
						under: {
							...underOf(legs[c2 - 1]!, left),
							...(left2 > 0 ? { under: underOf(legs[c3 - 1]!, left2) } : {}),
						},
					}
				: {}),
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
	// (Short steps, if he is striding - or just was.)
	const short =
		stride.len < LEN_FULL &&
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
		...(pivot ? { pivot } : {}),
		...(from ? { from } : {}),
		...(l.ball ? { ball: l.ball } : {}),
		...(l.arms ? { arms: { move: l.arms, phase: armsPhaseOf(l, t) } } : {}),
		...(l.bare ? { bare: true as const } : {}),
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
	for (const part of [
		...play.parts,
		...play.crew,
		...play.chains,
		...play.tv,
		...(play.medics ?? []),
		...(play.coaches ?? []),
		...(play.toss && t < play.toss.cut ? play.toss.men : []),
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
	const over = handsOf && passing(play, t, players, handsOf);
	if (over) {
		for (const st of players) {
			if (over.unseen.includes(st.id) && st.ball) {
				st.unseen = true;
			}
		}
		ball = { at: over.at, axis: over.axis };
		focus = over.at;
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
					z: f.from.z * (1 - u) + FLAG_RISE * Math.sin((rise * Math.PI) / 2),
				},
				down: u >= 1,
				turn: u >= 1 ? f.t0 % 6.28 : ((t - f.t0) / 1000) * 7,
			};
		});
	return {
		t,
		play,
		players,
		...(ball ? { ball } : {}),
		...(focus ? { focus } : {}),
		...(carrier !== undefined ? { carrier } : {}),
		...(flags.length > 0 ? { flags } : {}),
	};
};

// How high over his hand a flag goes.
const FLAG_RISE = 5;
