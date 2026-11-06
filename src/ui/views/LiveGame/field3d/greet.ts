import { drawnBody, footballBody } from "./body.ts";
import { posOf, type CrewPart } from "./crew.ts";
import type { Leg } from "./director.ts";
import {
	COACH_LINE,
	dist,
	FIELD_W,
	MID_X,
	type Pt,
	type Side,
} from "./geometry.ts";
import { HIGH_FIVE_AT, handsMeet } from "./poses.ts";
import { Man } from "./postgame.ts";
import { coordinatorAt, headCoachAt } from "./sideline.ts";

// THE SCORER BACK TO HIS SIDELINE (the 3D court's bench greeting a man
// coming off: see court3d/scene.ts greetsFor): the home side's man who
// scored, in off the field as the try is set up - two of his teammates out
// of the front of the crowd to him as he jogs in, a high five from the one
// and a hug from the other - the picture in close on it from down on the field in
// front of them (see aim.ts), then out wide for the kick. Only where none
// of the coaches up front is in it, nor anybody on the field in the way of
// the shot.

// In off the field from this far along it either way and this far off it
// (feet, on the border), at a jog (feet a second), to this far off it - the
// two of them out of the crowd from this far off the field, this far
// either side of where he comes to, on out this far to meet him, from this
// long in (ms) at a walk.
const IN_ALONG = 9;
const IN_FROM = 6;
const IN_FPS = 7;
const IN_TO = COACH_LINE - 5.2;
const MATES_OFF = COACH_LINE - 0.6;
const MATES_SIDE = 2.6;
const MATES_OUT = 1.8;
const MATES_FROM_MS = 300;
const MATES_FPS = 3.2;
// Where along the sideline (feet off midfield, either way - toward this far
// out, where the crowd is a row deep, not three), no nearer than this to the
// head coach's spot (or as far as he paces off it) - nor his way in this
// near it - or a coordinator's.
const ALONG = [20, 38] as const;
const ALONG_MOST = 28;
const ALONG_STEP = 2;
const HEAD_CLEAR = 12;
const HEAD_PATH = 8;
const COORD_CLEAR = 4.5;
// The high five: squared up this long first, this long; then over to the
// other a step (taken over this long at least), and the hug: this far apart,
// this long - stood after, turned to the field, until the picture goes.
const FIVE = handsMeet(drawnBody(footballBody()), "highFive", HIGH_FIVE_AT);
const SET_MS = 200;
const FIVE_MS = 800;
// (Come to as far along from the man for it as this share of how far
// apart they are for it.)
const FIVE_ALONG = 0.9;
const STEP_MS = 450;
const HUG_APART = 1.7;
const HUG_MS = 1300;
const AFTER_MS = 400;
// (The one for the hug clapping him in as he comes, if he waits this long,
// ms.)
const CLAP_LEAST_MS = 700;
// (Side on to the camera in front of them, near enough - not one of them
// in front of the other: no more than this far round from it, radians.)
const LEVEL = 0.25;
const level = (way: number): number => {
	const along = Math.cos(way) >= 0 ? 0 : Math.PI;
	const off = Math.atan2(Math.sin(way - along), Math.cos(way - along));
	return along + Math.min(LEVEL, Math.max(-LEVEL, off));
};
// The camera this far out in front of them, on the field (see aim.ts) -
// nobody on the field this near the middle of them along it, from just in
// front of them out to it, while the picture is on them.
export const GREET_CAM_FT = 30;
const CLEAR = 5;
// (Nor this near his way in, this far out from them.)
const PATH_CLEAR = 2.5;
const PATH_BAND = 7;

export type Greet = {
	t0: number;
	t1: number;
	pid: number;
	// The middle of the three of them.
	at: Pt;
};

export const stageGreet = (input: {
	team: Side;
	line: 0 | 1;
	t0: number;
	// The latest the picture can stay on it.
	most: number;
	playI: number;
	scorer: number;
	// His teammates off the field who could be the two.
	mates: number[];
	// Everybody on the field, kept out of the shot.
	field: { legs: Leg[] }[];
	r: () => number;
}): { greet: Greet; men: CrewPart[] } | undefined => {
	const { team, line, t0, r } = input;
	if (input.mates.length < 2) {
		return undefined;
	}
	// (Off the field, out from it.)
	const yOf = (off: number) => (line === 0 ? -off : FIELD_W + off);
	const out = line === 0 ? -1 : 1;
	const head = headCoachAt(team, input.playI);
	const coords = [
		coordinatorAt(team, 1, input.playI),
		coordinatorAt(team, 2, input.playI),
	];
	const xs: number[] = [];
	for (let d = ALONG[0]; d <= ALONG[1]; d += ALONG_STEP) {
		xs.push(MID_X - d, MID_X + d);
	}
	const lean = r() < 0.5 ? -1 : 1;
	xs.sort(
		(a, b) =>
			Math.abs(a - MID_X - lean * ALONG_MOST) -
			Math.abs(b - MID_X - lean * ALONG_MOST),
	);
	const mid = (x: number): Pt => ({ x, y: yOf((IN_TO + MATES_OFF) / 2) });
	// (Clear for the picture from `t0` to `most`: the coaches up front out
	// of it, and anybody on the field out from in front of them to the
	// camera - and out of his way in, from `from`.)
	const clear = (x: number, from: number) => {
		const lo = Math.min(x, x + from * IN_ALONG);
		const hi = Math.max(x, x + from * IN_ALONG);
		const off = (px: number) => (px < lo ? lo - px : px > hi ? px - hi : 0);
		if (
			Math.abs(x - head.x) < HEAD_CLEAR ||
			off(head.x) < HEAD_PATH ||
			coords.some(
				(c) => Math.abs(x - c.x) < COORD_CLEAR || off(c.x) < COORD_CLEAR,
			)
		) {
			return false;
		}
		const at = mid(x);
		for (let t = t0; t <= input.most; t += 100) {
			for (const q of input.field) {
				if (q.legs.length === 0) {
					continue;
				}
				const p = posOf(q.legs, t);
				const d = (p.y - at.y) * -out;
				if (
					d > -1.5 &&
					d < GREET_CAM_FT &&
					(Math.abs(p.x - at.x) < CLEAR ||
						(d < PATH_BAND && off(p.x) < PATH_CLEAR))
				) {
					return false;
				}
			}
		}
		return true;
	};
	const toField = line === 0 ? Math.PI / 2 : -Math.PI / 2;
	const start = (x: number, y: number): Leg[] => [
		{ t0, t1: t0, from: { x, y }, to: { x, y }, move: "stand", yaw: toField },
	];
	// Staged at x, him in from `from`: the three of them, and when it is
	// over. (In from one side or the other: the high five from the one of
	// them that side, the hug from the other.)
	const stage = (x: number, from: number) => {
		const pick = [...input.mates];
		const mates = [0, 1].map(
			() => pick.splice(Math.floor(r() * pick.length), 1)[0]!,
		);
		const s = new Man(team, start(x + from * IN_ALONG, yOf(IN_FROM)), IN_FPS);
		const a = new Man(team, start(x - MATES_SIDE, yOf(MATES_OFF)), MATES_FPS);
		const b = new Man(team, start(x + MATES_SIDE, yOf(MATES_OFF)), MATES_FPS);
		const [five, hug] = from < 0 ? [a, b] : [b, a];
		a.bare = true;
		b.bare = true;
		for (const m of [a, b]) {
			m.hold(t0 + MATES_FROM_MS + r() * 200);
			m.walk({ x: m.at.x, y: yOf(MATES_OFF - MATES_OUT) });
		}
		// (On past the first of them, to as far off him as the high five
		// takes.)
		s.walk({ x: five.at.x - from * FIVE.apart * FIVE_ALONG, y: yOf(IN_TO) });
		// The high five: each over to the other a step, squared up, slapped.
		const pair = (m: Man, n: Man, apart: number) => {
			const c = { x: (m.at.x + n.at.x) / 2, y: (m.at.y + n.at.y) / 2 };
			const way = level(Math.atan2(n.at.y - m.at.y, n.at.x - m.at.x));
			const ux = (Math.cos(way) * apart) / 2;
			const uy = (Math.sin(way) * apart) / 2;
			const over = (q: Man, to: Pt) =>
				q.walk(to, Math.min(MATES_FPS, (dist(q.at, to) / STEP_MS) * 1000));
			over(m, { x: c.x - ux, y: c.y - uy });
			over(n, { x: c.x + ux, y: c.y + uy });
			return { way, at: Math.max(m.t, n.t) + SET_MS };
		};
		five.hold(s.t);
		const f = pair(s, five, FIVE.apart);
		s.hold(f.at, "stand", f.way - FIVE.turn);
		five.hold(f.at, "stand", f.way + Math.PI - FIVE.turn);
		s.act("highFive", FIVE_MS, f.way - FIVE.turn);
		five.act("highFive", FIVE_MS, f.way + Math.PI - FIVE.turn);
		// The hug. (Waiting on him a while first: clapping him in as he
		// comes, turned to him - not stood stock still.)
		if (s.t - hug.t >= CLAP_LEAST_MS) {
			hug.act(
				"clap",
				s.t - hug.t,
				level(Math.atan2(s.at.y - hug.at.y, s.at.x - hug.at.x)),
			);
		}
		hug.hold(s.t);
		const h = pair(s, hug, HUG_APART);
		s.hold(h.at, "stand", h.way);
		hug.hold(h.at, "stand", h.way + Math.PI);
		s.act("hug", HUG_MS, h.way);
		hug.act("hug", HUG_MS, h.way + Math.PI);
		const t1 = s.t + AFTER_MS;
		// (Stood after, turned to the field - easy, his weight from one foot
		// to the other, as long as it is till the snap: not a statue.)
		for (const m of [s, five, hug]) {
			m.hold(t1, "easy", toField);
		}
		const men = [
			{ pid: input.scorer, team, legs: s.legs },
			{ pid: mates[0]!, team, legs: a.legs },
			{ pid: mates[1]!, team, legs: b.legs },
		];
		return { men, t1 };
	};
	const first = r() < 0.5 ? -1 : 1;
	for (const x of xs) {
		const from = [first, -first].find((f) => clear(x, f));
		if (from === undefined) {
			continue;
		}
		const { men, t1 } = stage(x, from);
		if (t1 > input.most) {
			return undefined;
		}
		return { greet: { t0, t1, pid: input.scorer, at: mid(x) }, men };
	}
	return undefined;
};
