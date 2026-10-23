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
const HELPED_FPS = 5;
const ALONE_FPS = 7;
const SHOWN_OFF = 6000;
const HELPED = 0.6;
// The men round him: this near, most of them, down on a knee a moment after
// the trainers go, up again as he is.
const KNEEL_NEAR = 30;
const KNEELS = 0.75;
// Where the trainers kneel by him, and walk with him (feet off him).
const BESIDE = 2.6;
const ALONGSIDE = 2.3;
const BEHIND = 4;

const toward = (a: Pt, b: Pt) => Math.atan2(b.y - a.y, b.x - a.x);

// His legs up to t, and nothing after - the leg he is in at t ended there
// (an act let finish, or - getting up, and not up yet - not begun): when
// they end, and whether he is down on the turf then.
const cut = (legs: Leg[], t: number): { at: number; down: boolean } => {
	let k = legs.findLastIndex((l) => l.t0 <= t);
	if (k < 0) {
		k = 0;
	}
	let l = legs[k]!;
	if (l.move === "getUp" && l.t1 > t) {
		legs.length = k;
		l = legs[k - 1] ?? l;
		return { at: Math.min(t, l.t1), down: true };
	}
	if (moveKind(l.move) === "act") {
		legs.length = k + 1;
		return { at: l.t1, down: l.move === "tackled" || l.move === "dive" };
	}
	const p = posOf(legs, t);
	legs.length = k + 1;
	if (l.t1 > t) {
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
	return { at: t, down: l.move === "prone" };
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
	const at = him.legs.at(-1)!.to;
	const yaw = him.legs.at(-1)!.yaw ?? 0;
	let t = fell.at;
	if (!fell.down) {
		him.legs.push({
			t0: t,
			t1: t + FALL_MS,
			from: at,
			to: at,
			move: "tackled",
			yaw,
		});
		t += FALL_MS;
	}
	const downAt = t;
	input.busy.add(him.pid);
	// (The ball, if he had it, on the turf by him.)
	const held = input.ball.at(-1);
	if (held?.kind === "held" && held.pid === him.pid && held.t0 <= downAt) {
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
		const there = leave + (dist(start, to) / OUT_FPS) * 1000;
		arrived = Math.max(arrived, there);
		const legs: Leg[] = [];
		still(legs, input.t0, leave, start, "stand", toward(start, at));
		legs.push({ t0: leave, t1: there, from: start, to, move: "run" });
		medics.push({ pid: medicId(team, input.n, k), team, legs });
	}
	const tending = arrived + 300;
	const up = arrived + TEND_MS;
	for (const m of medics) {
		const to = m.legs.at(-1)!.to;
		still(m.legs, m.legs.at(-1)!.t1, up, to, "tend", toward(to, at));
	}
	// Him: on his back, then sat up with them, then up.
	const sitUp = downAt + (up - downAt) * SIT_UP;
	still(him.legs, downAt, sitUp, at, "hurt", yaw);
	still(him.legs, sitUp, up, at, "hurtUp", yaw);
	him.legs.push({
		t0: up,
		t1: up + UP_MS,
		from: at,
		to: at,
		move: "getUp",
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

	// The men round him, down on a knee while he is.
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
			(l) => l.move === "getUp" && l.t0 <= kneel && l.t1 > kneel,
		);
		if (rising) {
			kneel = rising.t1;
		}
		const rise = up + 300 + r() * 700;
		const c = cut(p.legs, kneel);
		const where = p.legs.at(-1)!.to;
		if (c.at < rise - 500) {
			still(p.legs, c.at, rise, where, "takeKnee", toward(where, at));
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
