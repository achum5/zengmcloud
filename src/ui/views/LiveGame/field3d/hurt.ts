import { posOf, type CrewPart } from "./crew.ts";
import type { BallSeg, Leg, Part } from "./director.ts";
import {
	COACH_LINE,
	dist,
	FIELD_W,
	TEAM_AREA,
	type Pt,
	type Side,
} from "./geometry.ts";
import { moveKind, type Move } from "./poses.ts";
import { sidelineOf } from "./sideline.ts";

// A MAN HURT ON THE PLAY, as the sim says one was: down on the turf after
// the whistle; the men round him - both sides - on a knee; two trainers and
// the team doctor out from his sideline to him, down beside him a while;
// and then up, and off to his sideline - helped, an arm over each
// trainer's shoulders, or on his own with one at his side.

// The trainers' ids: three for each man hurt (two at most a play), by his
// team - each team's in its own colors.
export const MEDICS = 3;
export const HURT_MOST = 2;
export const medicId = (team: Side, n: number, k: number) =>
	-40 - team * 10 - n * MEDICS - k;

// Down this long after the whistle; the trainers off the sideline this long
// after he is, at this pace (feet a second), with him this long; up off the
// turf; off the field, at this pace helped, or on his own - and the picture
// on him going this long at most, before it cuts away to the next play.
const DOWN_AFTER = 250;
const FALL_MS = 450;
const OUT_AFTER = 700;
const OUT_FPS = 17;
const TEND_MS = 3600;
const SIT_UP = 0.55;
const UP_MS = 1000;
// Sat up off his back - or, down on his face, up onto a knee - over this
// long: that far through getting up (see poses.ts).
const SIT_MS = 800;
const SAT_AT = 0.35;
const KNEE_AT = 0.5;
// Brought down on his face (or else on his back, or sat).
const FACE_DOWN = new Set<Move>(["tackled", "dive", "prone"]);
// Going down still on the move, at this pace or more (ft/s): carried on
// the way he goes as far as half his pace would take him over the fall (no
// more than FALL_ON_MOST feet) - not dropped where he was.
const FALL_ON_FPS = 8;
const FALL_ON_MOST = 4.5;
const HELPED_FPS = 5;
const ALONE_FPS = 7;
const SHOWN_OFF = 6000;
const HELPED = 0.6;
// The men round him: this near, most of them, down on a knee a moment after
// the trainers go, up again as he is.
const KNEEL_NEAR = 30;
const KNEELS = 0.75;
// (Getting up off the turf to, on his feet a moment first - turned to him -
// before he goes down on the knee.)
const STOOD_FIRST_MS = 700;
// (No nearer each other than this (feet), nor him and the trainers by
// him than KNEEL_OFF_HIM: a step or two round him to a place of his own,
// at this pace, ft/s.)
const KNEEL_APART = 2.6;
const KNEEL_OFF_HIM = 5;
const KNEEL_WALK_FPS = 4;
// (Hardly a step off where he is, feet: down there.)
const KNEEL_STEP_FT = 0.75;
// Where the trainers kneel by him, and walk with him (feet off him).
const BESIDE = 2.6;
const ALONGSIDE = 2.3;
const BEHIND = 4;

const toward = (a: Pt, b: Pt) => Math.atan2(b.y - a.y, b.x - a.x);

// Where a man at `from` goes down on a knee round a man hurt at `at`, the
// others down at `knelt`: where he is, or round him at that much off him
// (or out a little further), the nearest clear of them all.
const kneelSpot = (from: Pt, at: Pt, knelt: Pt[]): Pt => {
	const clear = (p: Pt) =>
		dist(p, at) >= KNEEL_OFF_HIM - 0.01 &&
		knelt.every((q) => dist(p, q) >= KNEEL_APART);
	if (clear(from)) {
		return from;
	}
	const a0 = toward(at, from);
	const r0 = Math.max(dist(from, at), KNEEL_OFF_HIM);
	for (let ring = 0; ring < 4; ring++) {
		const rad = r0 + ring * KNEEL_APART;
		for (let k = 0; k < 24; k++) {
			const a = a0 + Math.ceil(k / 2) * 0.28 * (k % 2 === 0 ? 1 : -1);
			const p = { x: at.x + Math.cos(a) * rad, y: at.y + Math.sin(a) * rad };
			if (clear(p)) {
				return p;
			}
		}
	}
	return from;
};

// (Into what he does next less than this long, ms, at t: not begun.)
const BEGUN_MS = 200;

// His legs up to t, and nothing after - the leg he is in at t ended there
// (an act let finish, or - getting up, and not up yet - not begun): when
// they end, and whether he is down on the turf then.
const cut = (legs: Leg[], t: number): { at: number; down: boolean } => {
	let k = legs.findLastIndex((l) => l.t0 <= t);
	if (k < 0) {
		k = 0;
	}
	let l = legs[k]!;
	if (
		(l.move === "getUp" || l.move === "getUpBack" || l.move === "sitUp") &&
		l.t1 > t
	) {
		legs.length = k;
		l = legs[k - 1] ?? l;
		return { at: Math.min(t, l.t1), down: true };
	}
	if (moveKind(l.move) === "act") {
		legs.length = k + 1;
		return {
			at: l.t1,
			down:
				l.move === "tackled" ||
				l.move === "tackledBack" ||
				l.move === "dive" ||
				l.move === "qbSlide",
		};
	}
	const p = posOf(legs, t);
	legs.length = k + 1;
	const before = legs[k - 1];
	if (
		l.t1 > t &&
		t - l.t0 < BEGUN_MS &&
		before &&
		before.t1 === l.t0 &&
		moveKind(before.move) === "still" &&
		before.from.x === before.to.x &&
		before.from.y === before.to.y &&
		!before.arms
	) {
		// (Only just off from a stand: as if he never was.)
		legs.length = k;
		legs[k - 1] = { ...before, t1: t };
	} else if (l.t1 > t) {
		legs[k] = { ...l, t1: t, to: p };
	} else if (l.t1 < t) {
		// (Stood as the play left him, up to then.)
		const still = moveKind(l.move) === "still";
		if (still && l.from.x === l.to.x && l.from.y === l.to.y && !l.arms) {
			legs[k] = { ...l, t1: t };
		} else {
			legs.push({
				t0: l.t1,
				t1: t,
				from: l.to,
				to: l.to,
				move: still ? l.move : "stand",
				...(l.yaw !== undefined ? { yaw: l.yaw } : {}),
				...(l.ball ? { ball: l.ball } : {}),
			});
		}
	}
	return {
		at: t,
		down: l.move === "prone" || l.move === "supine" || l.move === "slid",
	};
};

// Still on the move at t, quicker than a walk (PULL_LEAST ft/s): pulled
// up first on the way he was going, at PULL_EASE ft/s/s, in steps
// OUT_STEP_MS long - not down on a knee in his stride. (When he is stood.)
const PULL_LEAST = 6.5;
const PULL_EASE = 14;
const pullUp = (legs: Leg[], t: number): number => {
	const l = legs.at(-1)!;
	const v = l.t1 > l.t0 ? dist(l.from, l.to) / ((l.t1 - l.t0) / 1000) : 0;
	if (moveKind(l.move) !== "cycle" || l.yaw !== undefined || v < PULL_LEAST) {
		return t;
	}
	const cos = (l.to.x - l.from.x) / (v * ((l.t1 - l.t0) / 1000));
	const sin = (l.to.y - l.from.y) / (v * ((l.t1 - l.t0) / 1000));
	const T = v / PULL_EASE;
	const n = Math.max(1, Math.round((T * 1000) / OUT_STEP_MS));
	const dt = T / n;
	let at = l.to;
	let t0 = t;
	for (let i = 0; i < n; i++) {
		const pace = v * (1 - (i + 0.5) / n);
		const to = { x: at.x + cos * pace * dt, y: at.y + sin * pace * dt };
		const t1 = t0 + dt * 1000;
		legs.push({
			t0,
			t1,
			from: at,
			to,
			move: pace < PULL_LEAST ? "walk" : l.move,
		});
		at = to;
		t0 = t1;
	}
	return t0;
};

// Out to him at OUT_FPS - pulled up over the last of it at OUT_EASE
// (ft/s/s, down to a walk's pace), in steps this long, not stopped dead.
const OUT_EASE = 12;
const OUT_WALK = 4;
const OUT_STEP_MS = 100;
const runOut = (legs: Leg[], from: Pt, to: Pt, t0: number) => {
	const far = dist(from, to);
	const slow = Math.min(
		far * 0.6,
		(OUT_FPS * OUT_FPS - OUT_WALK * OUT_WALK) / (2 * OUT_EASE),
	);
	const at = (s: number): Pt => ({
		x: from.x + ((to.x - from.x) * s) / far,
		y: from.y + ((to.y - from.y) * s) / far,
	});
	let t = t0 + ((far - slow) / OUT_FPS) * 1000;
	legs.push({ t0, t1: t, from, to: at(far - slow), move: "run" });
	let s = far - slow;
	let v = Math.sqrt(OUT_WALK * OUT_WALK + 2 * OUT_EASE * slow);
	while (s < far - 0.01) {
		const dt = OUT_STEP_MS / 1000;
		const next = Math.max(OUT_WALK, v - OUT_EASE * dt);
		const ds = Math.min(far - s, ((v + next) / 2) * dt);
		const pace = ds / dt;
		legs.push({
			t0: t,
			t1: t + OUT_STEP_MS,
			from: at(s),
			to: at(s + ds),
			move: pace < 6.5 ? "walk" : pace < 12.5 ? "jog" : "run",
		});
		s += ds;
		t += OUT_STEP_MS;
		v = next;
	}
};

const still = (
	legs: Leg[],
	t0: number,
	t1: number,
	at: Pt,
	move: Move,
	yaw?: number,
) => {
	if (t1 > t0) {
		legs.push({
			t0,
			t1,
			from: at,
			to: at,
			move,
			...(yaw !== undefined ? { yaw } : {}),
		});
	}
};

export type Hurt = {
	medics: CrewPart[];
	// When he is down, when the trainers are down beside him (for a replay
	// of the play to show meanwhile), and when it is all over.
	down: number;
	tending: number;
	t1: number;
};

export const stageHurt = (input: {
	pid: number;
	n: number;
	parts: Part[];
	whistle: number;
	t0: number;
	t1: number;
	ball: BallSeg[];
	// Men already kneeling for another man hurt on the play.
	busy: Set<number>;
	r: () => number;
}): Hurt | undefined => {
	const { parts, r } = input;
	const him = parts.find((p) => p.pid === input.pid);
	if (!him) {
		return undefined;
	}
	const team = him.team;
	// Down.
	const fell = cut(him.legs, input.whistle + DOWN_AFTER);
	const last = him.legs.at(-1)!;
	let at = last.to;
	const goes = last.from.x !== last.to.x || last.from.y !== last.to.y;
	const yaw =
		last.yaw ??
		(goes ? Math.atan2(last.to.y - last.from.y, last.to.x - last.from.x) : 0);
	let t = fell.at;
	if (!fell.down) {
		const v =
			last.t1 > last.t0
				? dist(last.from, last.to) / ((last.t1 - last.t0) / 1000)
				: 0;
		const on = goes && v >= FALL_ON_FPS;
		const way = Math.atan2(last.to.y - last.from.y, last.to.x - last.from.x);
		const far = Math.min(FALL_ON_MOST, (v * FALL_MS) / 2000);
		const to = on
			? { x: at.x + Math.cos(way) * far, y: at.y + Math.sin(way) * far }
			: at;
		him.legs.push({
			t0: t,
			t1: t + FALL_MS,
			from: at,
			to,
			// (Going the way he faces: down on his face.)
			move: on && Math.cos(way - yaw) > 0 ? "tackled" : "tackledBack",
			yaw,
		});
		at = to;
		t += FALL_MS;
	}
	const downAt = t;
	const faceDown = FACE_DOWN.has(him.legs.at(-1)!.move);
	input.busy.add(him.pid);
	// (The ball, if he had it, on the turf by him - not tossed down once he
	// was up.)
	const k = input.ball.findLastIndex((b) => b.t0 <= downAt);
	const held = input.ball[k];
	if (held?.kind === "held" && held.pid === him.pid) {
		input.ball.length = k + 1;
		input.ball.push({
			kind: "spot",
			t0: downAt,
			at: { x: at.x + Math.cos(yaw) * 2, y: at.y + Math.sin(yaw) * 2, z: 0 },
		});
	}

	// The trainers out to him from his sideline.
	const line = sidelineOf(team);
	const sideY = line === 0 ? -(COACH_LINE - 1) : FIELD_W + COACH_LINE - 1;
	const fromX = Math.min(TEAM_AREA[1], Math.max(TEAM_AREA[0], at.x));
	const go = downAt + OUT_AFTER;
	const besideOf = [
		{ x: at.x, y: at.y + (line === 0 ? -BESIDE : BESIDE) },
		{ x: at.x + BESIDE, y: at.y },
		{ x: at.x - BESIDE, y: at.y },
	];
	const medics: CrewPart[] = [];
	let arrived = go;
	for (let k = 0; k < MEDICS; k++) {
		const start = { x: fromX + (k - 1) * 3, y: sideY };
		const to = besideOf[k]!;
		const leave = go + k * 220;
		const legs: Leg[] = [];
		still(legs, input.t0, leave, start, "stand", toward(start, at));
		runOut(legs, start, to, leave);
		arrived = Math.max(arrived, legs.at(-1)!.t1);
		medics.push({ pid: medicId(team, input.n, k), team, legs });
	}
	const tending = arrived + 300;
	const up = arrived + TEND_MS;
	for (const m of medics) {
		const to = m.legs.at(-1)!.to;
		still(m.legs, m.legs.at(-1)!.t1, up, to, "tend", toward(to, at));
	}
	// Him: on his back, then sat up with them, then up - or face down, up
	// on a knee, then up.
	const sitUp = downAt + (up - downAt) * SIT_UP;
	const sat = Math.min(sitUp + SIT_MS, up);
	const rise = faceDown ? "getUp" : "getUpBack";
	const part = faceDown ? KNEE_AT : SAT_AT;
	still(him.legs, downAt, sitUp, at, faceDown ? "hurtProne" : "hurt", yaw);
	him.legs.push({
		t0: sitUp,
		t1: sat,
		from: at,
		to: at,
		move: rise,
		span: [0, part],
		yaw,
	});
	still(him.legs, sat, up, at, faceDown ? "hurtKnee" : "hurtUp", yaw);
	him.legs.push({
		t0: up,
		t1: up + UP_MS,
		from: at,
		to: at,
		move: rise,
		span: [part, 1],
		yaw,
	});

	// And off: to his sideline, helped or on his own.
	const helped = r() < HELPED;
	const off = {
		x: Math.min(TEAM_AREA[1], Math.max(TEAM_AREA[0], at.x + (r() - 0.5) * 20)),
		y: line === 0 ? -(COACH_LINE + 3) : FIELD_W + COACH_LINE + 3,
	};
	const way = toward(at, off);
	const fps = helped ? HELPED_FPS : ALONE_FPS;
	const walk0 = up + UP_MS + 150;
	const walk1 = walk0 + (dist(at, off) / fps) * 1000;
	still(him.legs, up + UP_MS, walk0, at, "stand", way);
	him.legs.push({
		t0: walk0,
		t1: walk1,
		from: at,
		to: off,
		move: helped ? "walk" : "jog",
		...(helped ? { arms: "overShoulders" as Move } : {}),
	});
	// (Each trainer's place by him on the way: either side, or behind.)
	const side = (d: number, back: number): Pt => ({
		x: -Math.sin(way) * d - Math.cos(way) * back,
		y: Math.cos(way) * d - Math.sin(way) * back,
	});
	const places = helped
		? [side(ALONGSIDE, 0), side(-ALONGSIDE, 0), side(0, BEHIND)]
		: [side(ALONGSIDE, 0.6), side(-1.5, BEHIND), side(1.5, BEHIND + 1)];
	medics.forEach((m, k) => {
		const o = places[k]!;
		const from = m.legs.at(-1)!.to;
		const a = { x: at.x + o.x, y: at.y + o.y };
		const b = { x: off.x + o.x, y: off.y + o.y };
		still(m.legs, up, up + UP_MS * 0.6, from, "stand", toward(from, at));
		m.legs.push(
			{ t0: up + UP_MS * 0.6, t1: walk0, from, to: a, move: "walk" },
			{ t0: walk0, t1: walk1, from: a, to: b, move: helped ? "walk" : "jog" },
		);
	});

	// The men round him, down on a knee while he is - each in a place of his
	// own (see KNEEL_APART).
	const knelt: Pt[] = [];
	for (const p of parts) {
		// (Not the man with the ball.)
		if (
			input.busy.has(p.pid) ||
			p.pid < 0 ||
			p.legs.some((l) => l.ball && l.t1 > downAt)
		) {
			continue;
		}
		const pos = posOf(p.legs, downAt);
		if (dist(pos, at) > KNEEL_NEAR || r() > KNEELS) {
			continue;
		}
		input.busy.add(p.pid);
		// (Up off the turf first, one getting up then.)
		let kneel = go + 500 + r() * 1400;
		const rising = p.legs.find(
			(l) =>
				(l.move === "getUp" || l.move === "getUpBack" || l.move === "sitUp") &&
				l.t0 <= kneel &&
				l.t1 > kneel,
		);
		if (rising) {
			kneel = rising.t1 + STOOD_FIRST_MS;
		}
		const rise = up + 300 + r() * 700;
		const c = cut(p.legs, kneel);
		const off = c.down ? c.at : pullUp(p.legs, c.at);
		const from = p.legs.at(-1)!.to;
		// (Off the men stood round as well.)
		const by = parts
			.filter((o) => o !== p && o.pid >= 0 && o.legs.length > 0)
			.map((o) => posOf(o.legs, off))
			.filter((q) => dist(q, from) < KNEEL_APART * 2);
		const spot = kneelSpot(from, at, [...knelt, ...by]);
		const where = dist(from, spot) < KNEEL_STEP_FT ? from : spot;
		const there = off + (dist(from, where) / KNEEL_WALK_FPS) * 1000;
		if (there < rise - 500) {
			if (there > off) {
				p.legs.push({ t0: off, t1: there, from, to: where, move: "walk" });
			}
			knelt.push(where);
			still(p.legs, there, rise, where, "takeKnee", toward(where, at));
			still(p.legs, rise, rise + 1, where, "stand", toward(where, at));
		}
	}

	const t1 = Math.max(input.t1, Math.min(walk1 + 900, walk0 + SHOWN_OFF));
	for (const m of medics) {
		const l = m.legs.at(-1)!;
		still(m.legs, l.t1, t1, l.to, "stand", toward(l.to, at));
	}
	return { medics, down: downAt, tending, t1 };
};
