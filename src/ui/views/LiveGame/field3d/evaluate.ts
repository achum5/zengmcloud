import { apartAt, apartOf, type Sep } from "./apart.ts";
import type {
	BallHold,
	BallSeg,
	FieldTimeline,
	Leg,
	Play,
} from "./director.ts";
import type { Pt, Pt3, Side } from "./geometry.ts";
import { along, goes, paceAt } from "./pace.ts";
import { liftOf, moveKind, strideOf, type Move } from "./poses.ts";
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

const pointOf = (l: Leg, t: number, u = along(l, t)): Pt => {
	return {
		x: l.from.x + (l.to.x - l.from.x) * u,
		y: l.from.y + (l.to.y - l.from.y) * u,
	};
};

// Which way he faces through a leg: as told, or the way he goes - or, going
// nowhere and not told, the way he was.
const yawOf = (l: Leg, before: number): number =>
	l.yaw ??
	(goes(l) ? Math.atan2(l.to.y - l.from.y, l.to.x - l.from.x) : before);

const lengthOf = (l: Leg) => Math.hypot(l.to.x - l.from.x, l.to.y - l.from.y);

// How far through his move he is: a stride cycle by the ground covered -
// carried on from the legs before it, so his feet keep their rhythm from
// one stretch to the next (and each man a step out of time with the
// next) - an act by the time (or its share of the act, cut in two).
const phaseOf = (
	l: Leg,
	t: number,
	offset: number,
	before = 0,
	u = along(l, t),
): number => {
	const kind = moveKind(l.move);
	if (kind === "still") {
		return 0;
	}
	if (kind === "act") {
		const [p0, p1] = l.span ?? [0, 1];
		return p0 + (p1 - p0) * along(l, t);
	}
	return (before + lengthOf(l) * u) / strideOf(l.move) + offset;
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
		const st = part ? stateOf(part, t, apart) : undefined;
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

// Where one man is at t and what he is doing.
const stateOf = (
	part: { pid: number; team: Side; legs: Leg[] },
	t: number,
	apart: Map<number, Sep[]>,
): FieldPlayerState | undefined => {
	const legs = part.legs;
	if (legs.length === 0) {
		return undefined;
	}
	const k = lastStarted(legs, t, (l) => l.t0);
	const l = legs[k]!;
	const offset = (part.pid * 0.618) % 1;
	// (Into a stand, still pulling up from the stretch before.)
	const { on, u: eased } = paceAt(legs, k, t);
	const pulling = on !== k;
	const at = pointOf(legs[on]!, t, eased);
	const aside = apartAt(apart.get(part.pid), t);
	if (aside) {
		at.x += aside.x;
		at.y += aside.y;
	}
	// The way he faced coming into this leg, and how far he had come in
	// strides.
	// (And the way he was drawn facing as it started: a leg too short for
	// his turn into it to be done, still turning.)
	let before = legs[0]!.yaw ?? 0;
	let turned = before;
	let ran = 0;
	let ranBefore = 0;
	for (let j = 0; j < k; j++) {
		const lj = legs[j]!;
		const aim = yawOf(lj, before);
		before = aim + (lj.turn ?? 0);
		turned =
			j > 0 && lj.t1 - lj.t0 < TURN_MS
				? lerpAngle(turned, before, (lj.t1 - lj.t0) / TURN_MS)
				: before;
		ranBefore = ran;
		ran = moveKind(lj.move) === "cycle" ? ran + lengthOf(lj) : 0;
	}
	let yaw = yawOf(l, before) + (l.turn ?? 0) * along(l, t);
	const since = t - l.t0;
	const prev = k > 0 ? legs[k - 1] : undefined;
	if (prev && since < TURN_MS) {
		yaw = lerpAngle(turned, yaw, clamp01(since / TURN_MS));
	}
	// (Just changed what he is doing, his arms, or how he has the ball:
	// eased out of what he was, his feet - the same stride - kept in
	// step.)
	let from: FieldPlayerState["from"];
	if (
		prev &&
		since < BLEND_MS &&
		(prev.move !== l.move ||
			prev.arms !== l.arms ||
			prev.ball !== l.ball ||
			!prev.mirrored !== !l.mirrored)
	) {
		const striding = prev.move === l.move && moveKind(l.move) === "cycle";
		from = {
			move: prev.move as Move,
			phase: striding
				? phaseOf(l, t, offset, ran, eased)
				: pulling
					? phaseOf(prev, t, offset, ranBefore, eased)
					: phaseOf(prev, prev.t1, offset, ranBefore),
			...(prev.mirrored ? { mirrored: true } : {}),
			w: 1 - clamp01(since / BLEND_MS),
			...(prev.arms
				? { arms: { move: prev.arms, phase: armsPhaseOf(prev, prev.t1) } }
				: {}),
			...(prev.ball ? { ball: prev.ball } : {}),
		};
	}
	return {
		id: part.pid,
		team: part.team,
		x: at.x,
		y: at.y,
		z: liftOf(l.move, phaseOf(l, t, offset, ran, eased)),
		yaw,
		move: l.move,
		phase: phaseOf(l, t, offset, ran, eased),
		...(l.mirrored ? { mirrored: true } : {}),
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
		const st = stateOf(part, t, apart);
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
