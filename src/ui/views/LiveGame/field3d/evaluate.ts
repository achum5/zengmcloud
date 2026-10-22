import type { BallSeg, FieldTimeline, Leg, Play } from "./director.ts";
import type { Pt, Pt3 } from "./geometry.ts";
import { moveKind, strideOf, type Move } from "./poses.ts";
import type { FieldPlayerState } from "./sprite.ts";

// THE FIELD AT ONE MOMENT: where everybody is and what each is doing, read
// off the staged game (see director.ts) - nothing kept from one frame to the
// next, so a moment can be had in any order (a rewind, a jump ahead, a
// replay).

export type FieldMoment = {
	t: number;
	play: Play | undefined;
	players: FieldPlayerState[];
	// The ball when it is on its own (spotted, on its tee): drawn by itself.
	// Unset while a man has it - he is drawn with it.
	ball?: { at: Pt3; tee?: boolean };
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

// How far through his move he is: a stride cycle by the ground covered (each
// man a step out of time with the next), an act by the time.
const phaseOf = (l: Leg, t: number, offset: number): number => {
	const kind = moveKind(l.move);
	if (kind === "still") {
		return 0;
	}
	if (kind === "act") {
		return along(l, t);
	}
	const d = Math.hypot(l.to.x - l.from.x, l.to.y - l.from.y) * along(l, t);
	return d / strideOf(l.move) + offset;
};

const ballAt = (segs: BallSeg[], t: number): FieldMoment["ball"] => {
	const s = segs[lastStarted(segs, t, (b) => b.t0)];
	if (!s || s.kind !== "spot") {
		return undefined;
	}
	return { at: s.at, ...(s.tee ? { tee: true } : {}) };
};

export const momentAt = (tl: FieldTimeline, t: number): FieldMoment => {
	const play = playAt(tl, t);
	if (!play) {
		return { t, play, players: [] };
	}
	const players: FieldPlayerState[] = [];
	for (const part of play.parts) {
		const legs = part.legs;
		if (legs.length === 0) {
			continue;
		}
		const k = lastStarted(legs, t, (l) => l.t0);
		const l = legs[k]!;
		const offset = (part.pid * 0.618) % 1;
		const at = pointOf(l, t);
		// The way he faced coming into this leg.
		let before = legs[0]!.yaw ?? 0;
		for (let j = 0; j < k; j++) {
			before = yawOf(legs[j]!, before);
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
				phase: phaseOf(prev, prev.t1, offset),
				...(prev.mirrored ? { mirrored: true } : {}),
				w: 1 - clamp01(since / BLEND_MS),
			};
		}
		players.push({
			id: part.pid,
			team: part.team,
			x: at.x,
			y: at.y,
			z: 0,
			yaw,
			move: l.move,
			phase: phaseOf(l, t, offset),
			...(l.mirrored ? { mirrored: true } : {}),
			...(from ? { from } : {}),
			...(l.ball ? { ball: l.ball } : {}),
		});
	}
	const ball = ballAt(play.ball, t);
	return { t, play, players, ...(ball ? { ball } : {}) };
};
