import { apartAt, apartOf } from "./apart.ts";
import type { BallSeg, FieldTimeline, Leg, Play } from "./director.ts";
import type { Pt, Pt3, Side } from "./geometry.ts";
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

const along = (l: Leg, t: number) =>
	l.t1 > l.t0 ? clamp01((t - l.t0) / (l.t1 - l.t0)) : 1;

const pointOf = (l: Leg, t: number): Pt => {
	const u = along(l, t);
	return {
		x: l.from.x + (l.to.x - l.from.x) * u,
		y: l.from.y + (l.to.y - l.from.y) * u,
	};
};

const goes = (l: Leg) => l.from.x !== l.to.x || l.from.y !== l.to.y;

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
const phaseOf = (l: Leg, t: number, offset: number, before = 0): number => {
	const kind = moveKind(l.move);
	if (kind === "still") {
		return 0;
	}
	if (kind === "act") {
		const [p0, p1] = l.span ?? [0, 1];
		return p0 + (p1 - p0) * along(l, t);
	}
	return (before + lengthOf(l) * along(l, t)) / strideOf(l.move) + offset;
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

const ballAt = (
	segs: BallSeg[],
	t: number,
	players: FieldPlayerState[],
): { ball?: FieldMoment["ball"]; focus?: Pt3; carrier?: Side } => {
	const s = segs[lastStarted(segs, t, (b) => b.t0)];
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
		return { ball: f, focus: f.at };
	}
	const p = players.find((x) => x.id === s.pid);
	return p ? { focus: { x: p.x, y: p.y, z: 3.5 }, carrier: p.team } : {};
};

export const momentAt = (tl: FieldTimeline, t: number): FieldMoment => {
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
	]) {
		const legs = part.legs;
		if (legs.length === 0) {
			continue;
		}
		const k = lastStarted(legs, t, (l) => l.t0);
		const l = legs[k]!;
		const offset = (part.pid * 0.618) % 1;
		const at = pointOf(l, t);
		const aside = apartAt(apart.get(part.pid), t);
		if (aside) {
			at.x += aside.x;
			at.y += aside.y;
		}
		// The way he faced coming into this leg, and how far he had come in
		// strides.
		let before = legs[0]!.yaw ?? 0;
		let ran = 0;
		let ranBefore = 0;
		for (let j = 0; j < k; j++) {
			const lj = legs[j]!;
			before = yawOf(lj, before);
			ranBefore = ran;
			ran = moveKind(lj.move) === "cycle" ? ran + lengthOf(lj) : 0;
		}
		let yaw = yawOf(l, before);
		const since = t - l.t0;
		const prev = k > 0 ? legs[k - 1] : undefined;
		if (prev && since < TURN_MS) {
			yaw = lerpAngle(before, yaw, clamp01(since / TURN_MS));
		}
		let from: FieldPlayerState["from"];
		if (prev && prev.move !== l.move && since < BLEND_MS) {
			from = {
				move: prev.move as Move,
				phase: phaseOf(prev, prev.t1, offset, ranBefore),
				...(prev.mirrored ? { mirrored: true } : {}),
				w: 1 - clamp01(since / BLEND_MS),
			};
		}
		players.push({
			id: part.pid,
			team: part.team,
			x: at.x,
			y: at.y,
			z: liftOf(l.move, phaseOf(l, t, offset, ran)),
			yaw,
			move: l.move,
			phase: phaseOf(l, t, offset, ran),
			...(l.mirrored ? { mirrored: true } : {}),
			...(from ? { from } : {}),
			...(l.ball ? { ball: l.ball } : {}),
			...(l.arms ? { arms: { move: l.arms, phase: armsPhaseOf(l, t) } } : {}),
		});
	}
	const { ball, focus, carrier } = ballAt(play.ball, t, players);
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
