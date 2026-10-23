import { apartAt, apartOf, type Sep } from "./apart.ts";
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
	goes,
	lagged,
	placeAt,
	TRACK_MS,
	trackLength,
	trackOf,
} from "./pace.ts";
import {
	beatOf,
	liftOf,
	loopRate,
	moveKind,
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
// (How fast he goes through a move: taken over this long.)
const RATE_MS = 5;

const clamp01 = (u: number) => Math.min(1, Math.max(0, u));

const lerpAngle = (a: number, b: number, u: number) => {
	let d = (b - a) % (Math.PI * 2);
	if (d > Math.PI) {
		d -= Math.PI * 2;
	} else if (d < -Math.PI) {
		d += Math.PI * 2;
	}
	return a + d * u;
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
				p += d / stride;
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

// (Not off a kicker's foot: the ball is not in his hands.)
const KICKED = new Set<Move>(["punt", "kick"]);
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
		const at =
			st && hold && !(thrown && KICKED.has(st.move))
				? handsOf({ ...st, ball: hold })
				: undefined;
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
				return seg.at;
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
		return {
			at: {
				x: from.x + (to.x - from.x) * e,
				y: from.y + (to.y - from.y) * e,
				z: from.z + (to.z - from.z) * e + 4 * rise * e * (1 - e),
			},
			axis:
				flat > 0.01
					? { x: (to.x - from.x) / flat, y: (to.y - from.y) / flat, z: 0.3 }
					: { x: 1, y: 0, z: 0.3 },
			unseen: [a, b].flatMap((x) => (x.kind === "held" ? [x.pid] : [])),
		};
	}
	return undefined;
};

// How long he is eased into a leg over: into an act, over no more than this
// share of it (or the first of it is gone by before he is into it).
const ACT_BLEND = 0.5;
const blendOf = (l: Leg): number => {
	if (moveKind(l.move) !== "act") {
		return BLEND_MS;
	}
	const [p0, p1] = l.span ?? [0, 1];
	const ms = (l.t1 - l.t0) / Math.max(0.05, p1 - p0);
	return Math.min(BLEND_MS, ms * ACT_BLEND);
};

// Whether he does something else from one leg to the next: his move, his
// arms, how he has the ball.
const changed = (a: Leg, b: Leg) =>
	a.move !== b.move ||
	a.arms !== b.arms ||
	a.ball !== b.ball ||
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
			at - legs[j]!.t0 < BLEND_MS &&
			!changed(legs[j - 1]!, legs[j]!)
		) {
			j--;
		}
		return j;
	};
	const c = changedAt(k, t);
	const c2 = c > 1 ? changedAt(c - 1, legs[c]!.t0) : 0;
	const left =
		c2 > 0 && changed(legs[c2 - 1]!, legs[c2]!)
			? 1 - (legs[c]!.t0 - legs[c2]!.t0) / blendOf(legs[c2]!)
			: 0;
	// The way he faced coming into this leg.
	// (And the way he was drawn facing as it started: a leg too short for
	// his turn into it to be done, still turning.)
	let before = legs[0]!.yaw ?? 0;
	let turned = before;
	for (let j = 0; j < k; j++) {
		const lj = legs[j]!;
		const aim = yawOf(lj, before);
		before = aim + (lj.turn ?? 0);
		turned =
			j > 0 && lj.t1 - lj.t0 < TURN_MS
				? lerpAngle(turned, before, (lj.t1 - lj.t0) / TURN_MS)
				: before;
	}
	let yaw = yawOf(l, before) + (l.turn ?? 0) * along(l, t);
	const since = t - l.t0;
	const prev = k > 0 ? legs[k - 1] : undefined;
	if (prev && since < TURN_MS) {
		yaw = lerpAngle(turned, yaw, clamp01(since / TURN_MS));
	}
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
	const phase = phaseOf(l, t, offset, stride.ph);
	const blended = t - legs[c]!.t0;
	const blending = blendOf(legs[c]!);
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
				w: 1 / blending,
			},
			...(prior.mirrored ? { mirrored: true } : {}),
			w: 1 - clamp01(blended / blending),
			...(prior.arms
				? { arms: { move: prior.arms, phase: armsPhaseOf(prior, prior.t1) } }
				: {}),
			...(prior.ball ? { ball: prior.ball } : {}),
			...(left > 0 ? { under: underOf(legs[c2 - 1]!, left) } : {}),
		};
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
		z: liftOf(l.move, phase),
		yaw,
		move: l.move,
		phase,
		...(l.mirrored ? { mirrored: true } : {}),
		...(short ? { len: stride.len } : {}),
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
