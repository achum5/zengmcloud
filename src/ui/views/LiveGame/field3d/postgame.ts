import { skeleton } from "../court3d/poses.ts";
import { FIGURE_SCALE, footballBody } from "./body.ts";
import { posOf, type CrewPart } from "./crew.ts";
import type { BallSeg, Leg, Part } from "./director.ts";
import {
	dist,
	FIELD_L,
	FIELD_W,
	MID_X,
	MID_Y,
	TEAM_AREA,
	type Pt,
	type Side,
} from "./geometry.ts";
import { moveKind, poseOfMove, type Move } from "./poses.ts";
import { coachId, headCoachAt, sidelineOf } from "./sideline.ts";

// THE GAME OVER. The winners' moment where the end of it finds them - up
// off their feet, chests bumped, a fist pumped (a rout: a clap, and that is
// all; won on the last play, all of them all over the man who won it) - and
// the beaten side's: hands on hips, on helmets, a man down on a knee. Then both sides out toward the middle of the field, each man to one
// of the other side's: a handshake, a hug now and then, and on to the next.
// And the two head coaches out from their sidelines through it all, to
// shake hands in the middle - the picture in on them as they do.

// From this long after the whistle (ms).
const GO_AFTER = 350;
// A win by this many points or more is a rout.
const ROUT = 17;
// The winners: this share of them chest to chest with a teammate (one no
// further off than this), the rest each in a moment of his own.
const BUMP = 0.3;
const BUMP_NEAR = 15;
const BUMP_APART = 2;
const BUMP_MS = 900;
// Won on the last play: the winners on the field to the man who won it, at a
// sprint (these paces), round him in a ring this far off him (this many
// in it) and another outside it - up off their feet with him, and all over
// him this long after the last of them gets there (of those no further off
// than this: the rest get there when they get there).
const MOB_RINGS: [number, number] = [2.6, 4.8];
const MOB_INNER = 6;
const MOB_FPS: [number, number] = [15, 19];
const MOB_MS = 2000;
const MOB_NEAR = 60;
// The middle they make for: from where they all are (or the man who won
// it), pulled this share of the way toward midfield (no further than this,
// feet) - the head coaches meeting this share of the rest of the way to it.
// Each man going this much of the way to a place of his own round it -
// spread this far either way along the field and across it - at a walk, at
// these paces (feet a second; the big men slower), quicker if it is a long
// way (there in this long, ms), and none of them right where the coaches
// meet.
const PULL = 0.45;
const PULL_MOST = 30;
const COACH_PULL = 0.25;
const SHARE = 0.9;
const WALK_MOST = 6000;
const SPREAD_X = 22;
const SPREAD_Y = 30;
const PACE: [number, number] = [4.6, 6];
const BIG_SLOWER = 0.85;
const BIG = new Set(["C", "LG", "RG", "LT", "RT", "DE", "DT", "NT"]);
const CLEAR = 7;
// A handshake this long (and up to this much longer); a hug after it this
// often, this long; on to a second man this often, one no further off
// than this.
const SHAKE_MS = 1000;
const SHAKE_MORE = 300;
const HUG = 0.4;
const HUG_MS = 1500;
const HUG_APART = 1.7;
const HUG_IN = 0.22;
const SECOND = 0.75;
const SECOND_FROM = 30;
// The head coaches: off their sidelines this long after the whistle (and
// up to this much later), at a jog; a hug after the handshake this often;
// the picture on them this long after it.
const COACH_GO = 400;
const COACH_LATE = 500;
const COACH_FPS = 10.5;
const COACH_HUG = 0.6;
const COACH_AFTER = 2600;
// The ball, if a man still has it: tossed down by his feet.
const TOSS_MS = 380;
// The officials: off this long after it is over (and up to this much
// later), at a jog, to just off the near sideline past the end of the team
// area nearer them - this far past it.
const CREW_GO = 900;
const CREW_LATE = 900;
const CREW_FPS = 8;
const CREW_PAST = 18;

// Two men shaking hands stand this far apart (feet, middle to middle),
// each turned this far (radians) to his left of straight at the other -
// right hand to right hand between them (as the men are drawn).
const SHAKE_HAND = (() => {
	const hand = skeleton(footballBody(), poseOfMove("shake", 0.24)).armR.end;
	return { f: hand.f * FIGURE_SCALE, s: hand.s * FIGURE_SCALE };
})();
const SHAKE_APART = 2 * Math.hypot(SHAKE_HAND.f, SHAKE_HAND.s);
const SHAKE_TURN = Math.atan2(-SHAKE_HAND.s, SHAKE_HAND.f);

// Down, or getting up, or in the middle of something: left to finish it.
const DOWN = new Set<Move>([
	"tackled",
	"tackledBack",
	"dive",
	"diveCatch",
	"prone",
	"supine",
	"getUp",
	"getUpBack",
	"qbSlide",
	"slid",
	"sitUp",
	"kneelDown",
]);

const toward = (a: Pt, b: Pt) => Math.atan2(b.y - a.y, b.x - a.x);
const clamp = (x: number, lo: number, hi: number) =>
	Math.min(hi, Math.max(lo, x));

export type Postgame = {
	coaches: CrewPart[];
	// When it starts, when the head coaches shake hands, and where.
	from: number;
	shake: number;
	at: Pt;
	winner: Side | undefined;
	// Won on the last play: where they are all over the man who won it, and
	// until when.
	mob?: { at: Pt; until: number };
	t1: number;
};

// When he is free of what the play gave him, from t on.
const freeAt = (legs: Leg[], t: number): number => {
	let k = Math.max(
		0,
		legs.findLastIndex((l) => l.t0 <= t),
	);
	let at = t;
	for (; k < legs.length; k++) {
		const l = legs[k]!;
		if (l.t1 <= at) {
			continue;
		}
		if (moveKind(l.move) !== "act" && !DOWN.has(l.move)) {
			break;
		}
		at = l.t1;
	}
	return at;
};

// Everything he does after t thrown away: what he was in the middle of cut
// short there.
const cutAt = (legs: Leg[], t: number) => {
	while (legs.length > 1 && legs.at(-1)!.t0 >= t) {
		legs.pop();
	}
	const l = legs.at(-1)!;
	if (l.t1 > t && t > l.t0) {
		const u = (t - l.t0) / (l.t1 - l.t0);
		l.to = {
			x: l.from.x + (l.to.x - l.from.x) * u,
			y: l.from.y + (l.to.y - l.from.y) * u,
		};
		if (moveKind(l.move) === "act") {
			const [p0, p1] = l.span ?? [0, 1];
			l.span = [p0, p0 + (p1 - p0) * u];
		}
		if (l.arms) {
			const [q0, q1] = l.armsSpan ?? [0, 1];
			l.armsSpan = [q0, q0 + (q1 - q0) * u];
		}
		l.t1 = t;
	}
};

// One man's legs, a stretch at a time from where they leave him.
export class Man {
	readonly team: Side;
	readonly legs: Leg[];
	pace: number;
	constructor(team: Side, legs: Leg[], pace: number) {
		this.team = team;
		this.legs = legs;
		this.pace = pace;
	}
	get at(): Pt {
		return this.legs.at(-1)!.to;
	}
	get t(): number {
		return this.legs.at(-1)!.t1;
	}
	// Stood (or in a still pose) where he is until t.
	hold(t: number, move: Move = "stand", yaw?: number) {
		if (t <= this.t) {
			return;
		}
		const l = this.legs.at(-1)!;
		this.legs.push({
			t0: l.t1,
			t1: t,
			from: l.to,
			to: l.to,
			move,
			...(yaw !== undefined ? { yaw } : {}),
		});
	}
	// An act where he is (or going a little way), part of it at a time.
	act(move: Move, ms: number, yaw?: number, to?: Pt, span?: [number, number]) {
		const l = this.legs.at(-1)!;
		this.legs.push({
			t0: l.t1,
			t1: l.t1 + Math.max(1, ms),
			from: l.to,
			to: to ?? l.to,
			move,
			...(yaw !== undefined ? { yaw } : {}),
			...(span ? { span } : {}),
		});
	}
	// To `to` at his pace (or this one); when he gets there.
	walk(to: Pt, fps = this.pace): number {
		const far = dist(this.at, to);
		if (far > 0.3) {
			const l = this.legs.at(-1)!;
			this.legs.push({
				t0: l.t1,
				t1: l.t1 + (far / fps) * 1000,
				from: l.to,
				to,
				move: fps < 6.5 ? "walk" : fps < 12.5 ? "jog" : "run",
			});
		}
		return this.t;
	}
}

// Two men out to each other from where they are (by when each is free),
// to shake hands - and hug, now and then - meeting at `at` (or halfway).
// When they shake, and when they are done.
export const meet = (
	a: Man,
	b: Man,
	r: () => number,
	at?: Pt,
	hug = HUG,
): { shake: number; end: number } => {
	const mid = at ?? { x: (a.at.x + b.at.x) / 2, y: (a.at.y + b.at.y) / 2 };
	const way = dist(a.at, b.at) > 0.5 ? toward(a.at, b.at) : r() * Math.PI * 2;
	const ux = Math.cos(way);
	const uy = Math.sin(way);
	const half = SHAKE_APART / 2;
	const spotA = { x: mid.x - ux * half, y: mid.y - uy * half };
	const spotB = { x: mid.x + ux * half, y: mid.y + uy * half };
	const there = Math.max(a.walk(spotA), b.walk(spotB)) + 150;
	const ms = SHAKE_MS + r() * SHAKE_MORE;
	a.hold(there, "stand", way - SHAKE_TURN);
	b.hold(there, "stand", way + Math.PI - SHAKE_TURN);
	a.act("shake", ms, way - SHAKE_TURN);
	b.act("shake", ms, way + Math.PI - SHAKE_TURN);
	if (r() < hug) {
		// (A step in to him, and back.)
		const inA = {
			x: mid.x - (ux * HUG_APART) / 2,
			y: mid.y - (uy * HUG_APART) / 2,
		};
		const inB = {
			x: mid.x + (ux * HUG_APART) / 2,
			y: mid.y + (uy * HUG_APART) / 2,
		};
		for (const [m, close, back, yaw] of [
			[a, inA, spotA, way],
			[b, inB, spotB, way + Math.PI],
		] as const) {
			m.act("hug", HUG_MS * HUG_IN, yaw, close, [0, HUG_IN]);
			m.act("hug", HUG_MS * (1 - 2 * HUG_IN), yaw, close, [HUG_IN, 1 - HUG_IN]);
			m.act("hug", HUG_MS * HUG_IN, yaw, back, [1 - HUG_IN, 1]);
		}
	}
	return { shake: there, end: Math.max(a.t, b.t) };
};

// Each of `as` with the nearest of `bs` not already taken, nearest first
// (by `far`).
const pairUp = <T>(
	as: T[],
	bs: T[],
	far: (a: T, b: T) => number,
	most = Infinity,
): [T, T][] => {
	const all: { a: T; b: T; d: number }[] = [];
	for (const a of as) {
		for (const b of bs) {
			const d = far(a, b);
			if (d <= most) {
				all.push({ a, b, d });
			}
		}
	}
	all.sort((x, y) => x.d - y.d);
	const taken = new Set<T>();
	const out: [T, T][] = [];
	for (const { a, b } of all) {
		if (!taken.has(a) && !taken.has(b)) {
			taken.add(a);
			taken.add(b);
			out.push([a, b]);
		}
	}
	return out;
};

export const stagePostgame = (input: {
	parts: Part[];
	// The final score, each side's.
	pts: [number, number];
	// The ball dead (or the moment after it seen out).
	whistle: number;
	// The man who won it on the last play, if anybody did.
	hero?: number;
	t0: number;
	t1: number;
	playI: number;
	ball: BallSeg[];
	r: () => number;
}): Postgame => {
	const { parts, pts, ball, r } = input;
	const winner: Side | undefined =
		pts[0] > pts[1] ? 0 : pts[1] > pts[0] ? 1 : undefined;
	const margin = Math.abs(pts[0] - pts[1]);
	const rout = margin >= ROUT;
	const go = input.whistle + GO_AFTER;

	// Each man free of the play: up off the turf, done what he was doing -
	// and the ball let go of.
	const men: Man[] = [];
	let hero: Man | undefined;
	for (const p of parts) {
		if (p.pid < 0 || p.legs.length === 0) {
			continue;
		}
		let free = freeAt(p.legs, go);
		const k = ball.findLastIndex((b) => b.t0 <= free);
		const seg = ball[k];
		if (seg?.kind === "held" && seg.pid === p.pid) {
			const next = ball[k + 1];
			if (next) {
				free = Math.max(free, next.t0 + 100);
			} else {
				const at = posOf(p.legs, free);
				const yaw = p.legs.at(-1)!.yaw ?? 0;
				const to = {
					x: at.x + Math.cos(yaw) * 4,
					y: at.y + Math.sin(yaw) * 4,
					z: 0.3,
				};
				ball.push(
					{
						kind: "fly",
						t0: free,
						t1: free + TOSS_MS,
						from: { x: at.x + Math.cos(yaw), y: at.y + Math.sin(yaw), z: 3.2 },
						to,
						peak: 1.2,
					},
					{ kind: "spot", t0: free + TOSS_MS, at: { ...to, z: 0 } },
				);
				free += TOSS_MS;
			}
		}
		cutAt(p.legs, free);
		const l = p.legs.at(-1)!;
		if (l.t1 < free) {
			p.legs.push({ t0: l.t1, t1: free, from: l.to, to: l.to, move: "stand" });
		}
		const big = BIG.has(p.role);
		const pace = (PACE[0] + r() * (PACE[1] - PACE[0])) * (big ? BIG_SLOWER : 1);
		const man = new Man(p.team, p.legs, pace);
		men.push(man);
		if (p.pid === input.hero) {
			hero = man;
		}
	}

	// The moment: the winners'...
	const won = men.filter((m) => m.team === winner);
	const lost = men.filter((m) => winner !== undefined && m.team !== winner);
	const bumped = new Set<Man>();
	let mob: Postgame["mob"];
	if (hero && hero.team === winner) {
		mob = mobbed(hero, won, r);
		for (const m of won) {
			bumped.add(m);
		}
	} else if (!rout && winner !== undefined) {
		const wanting = won.filter(() => r() < BUMP);
		for (const [a, b] of pairUp(
			wanting,
			won,
			(a, b) => (a === b ? Infinity : dist(a.at, b.at)),
			BUMP_NEAR,
		)) {
			if (bumped.has(a) || bumped.has(b)) {
				continue;
			}
			bumped.add(a);
			bumped.add(b);
			const mid = { x: (a.at.x + b.at.x) / 2, y: (a.at.y + b.at.y) / 2 };
			const way = dist(a.at, b.at) > 0.5 ? toward(a.at, b.at) : r() * 6.28;
			const ux = Math.cos(way) * (BUMP_APART / 2);
			const uy = Math.sin(way) * (BUMP_APART / 2);
			a.walk({ x: mid.x - ux, y: mid.y - uy }, 9);
			b.walk({ x: mid.x + ux, y: mid.y + uy }, 9);
			const at = Math.max(a.t, b.t) + 60;
			a.hold(at, "stand", way);
			b.hold(at, "stand", way + Math.PI);
			a.act("chestBump", BUMP_MS, way);
			b.act("chestBump", BUMP_MS, way + Math.PI);
		}
	}
	for (const m of won) {
		if (bumped.has(m)) {
			continue;
		}
		m.hold(m.t + r() * 400);
		const u = r();
		if (rout) {
			if (u < 0.5) {
				m.act("clap", 1200 + r() * 400);
			}
		} else if (u < 0.35) {
			m.act("leap", 800);
			if (r() < 0.4) {
				m.act("leap", 800);
			}
		} else if (u < 0.6) {
			m.act("fistPump", 1300 + r() * 300);
		} else if (u < 0.8) {
			m.act("flex", 1300);
		} else {
			m.act("clap", 1200 + r() * 400);
		}
	}
	// ...and the beaten side's.
	for (const m of lost) {
		m.hold(m.t + 200 + r() * 600);
		// (Beaten on the last play: none of them stood about.)
		const u = mob ? r() * 0.64 : r();
		if (rout) {
			m.hold(m.t + 1200, u < 0.5 ? "onHips" : "stand");
		} else if (u < 0.3) {
			m.act("handsOnHelmet", 1500 + r() * 300);
		} else if (u < 0.48) {
			m.hold(m.t + 1700 + r() * 600, "takeKnee");
			m.hold(m.t + 250);
		} else if (u < 0.64) {
			m.hold(m.t + 1500, "backStance");
		} else {
			m.hold(m.t + 1500 + r() * 500, "onHips");
		}
	}
	// (A tie: nobody's.)
	if (winner === undefined) {
		for (const m of men) {
			m.hold(m.t + 600 + r() * 600, r() < 0.4 ? "onHips" : "stand");
		}
	}

	// The middle they make for, and the head coaches' place in it.
	const n = Math.max(1, men.length);
	const cx = mob ? mob.at.x : men.reduce((s, m) => s + m.at.x, 0) / n;
	const middle = {
		x: cx + clamp((MID_X - cx) * PULL, -PULL_MOST, PULL_MOST),
		y: MID_Y + (r() - 0.5) * 12,
	};
	const at = {
		x: middle.x + (MID_X - middle.x) * COACH_PULL,
		y: middle.y,
	};
	const placeOf = new Map<Man, Pt>();
	for (const m of men) {
		let p = {
			x: m.at.x + (middle.x + (r() - 0.5) * 2 * SPREAD_X - m.at.x) * SHARE,
			y: m.at.y + (middle.y + (r() - 0.5) * 2 * SPREAD_Y - m.at.y) * SHARE,
		};
		const d = dist(p, at);
		if (d < CLEAR) {
			const a = d > 0.1 ? toward(at, p) : r() * Math.PI * 2;
			p = { x: at.x + Math.cos(a) * CLEAR, y: at.y + Math.sin(a) * CLEAR };
		}
		p = { x: clamp(p.x, 3, FIELD_L - 3), y: clamp(p.y, 3, FIELD_W - 3) };
		m.pace = Math.max(m.pace, dist(m.at, p) / (WALK_MOST / 1000));
		placeOf.set(m, p);
	}
	// Each to one of the other side's, by where they are making for - and
	// on to the next.
	let done = go;
	const metWith = new Map<Man, Man>();
	const zero = men.filter((m) => m.team === 0);
	const one = men.filter((m) => m.team === 1);
	for (const [a, b] of pairUp(zero, one, (a, b) =>
		dist(placeOf.get(a)!, placeOf.get(b)!),
	)) {
		const pa = placeOf.get(a)!;
		const pb = placeOf.get(b)!;
		done = Math.max(
			done,
			meet(a, b, r, { x: (pa.x + pb.x) / 2, y: (pa.y + pb.y) / 2 }).end,
		);
		metWith.set(a, b);
		metWith.set(b, a);
	}
	for (const m of men) {
		if (!metWith.has(m)) {
			m.walk(placeOf.get(m)!);
		}
	}
	const again = (m: Man) => r() < SECOND;
	for (const [a, b] of pairUp(
		zero.filter(again),
		one.filter(again),
		(a, b) => (metWith.get(a) === b ? Infinity : dist(a.at, b.at)),
		SECOND_FROM,
	)) {
		done = Math.max(done, meet(a, b, r).end);
	}

	// The head coaches: out from their sidelines to each other.
	const coaches: CrewPart[] = [];
	const bosses: Man[] = [];
	for (const team of [0, 1] as Side[]) {
		const from = headCoachAt(team, input.playI);
		const toField = sidelineOf(team) === 0 ? Math.PI / 2 : -Math.PI / 2;
		const legs: Leg[] = [
			{
				t0: input.t0,
				t1: go + COACH_GO + r() * COACH_LATE,
				from,
				to: from,
				move: "armsFolded",
				yaw: toField,
			},
		];
		coaches.push({ pid: coachId(team, 0), team, legs });
		bosses.push(new Man(team, legs, COACH_FPS));
	}
	// (Side on to the camera: along the field, the far side's coach on the
	// left.)
	const [far, near] =
		sidelineOf(0) === 0 ? [bosses[0]!, bosses[1]!] : [bosses[1]!, bosses[0]!];
	const half = SHAKE_APART / 2;
	far.walk({ x: at.x - half - 1.5, y: at.y });
	near.walk({ x: at.x + half + 1.5, y: at.y });
	const { shake, end } = meet(far, near, r, at, COACH_HUG);

	const t1 = Math.max(input.t1, end + COACH_AFTER, done + 800);
	for (const m of [...men, ...bosses]) {
		const l = m.legs.at(-1)!;
		m.hold(t1, moveKind(l.move) === "still" ? l.move : "stand");
	}
	return { coaches, from: go, shake, at, winner, ...(mob ? { mob } : {}), t1 };
};

// The winners all over the man who won it: each at a sprint to a place in
// a ring round him (the nearest in the inner one), turned in to him, up off
// his feet as he gets there, and there a while - him up with them.
const mobbed = (
	hero: Man,
	won: Man[],
	r: () => number,
): { at: Pt; until: number } => {
	const at = hero.at;
	hero.act("leap", 800);
	hero.act(r() < 0.5 ? "fistPump" : "flex", 1300);
	const mates = won
		.filter((m) => m !== hero)
		.sort((a, b) => dist(a.at, at) - dist(b.at, at));
	const rings = [mates.slice(0, MOB_INNER), mates.slice(MOB_INNER)];
	let until = hero.t;
	rings.forEach((ring, k) => {
		// (Each to the place in it on his side of him, in turn round it.)
		const from = (m: Man) => toward(at, m.at);
		const byWay = [...ring].sort((a, b) => from(a) - from(b));
		const first = byWay[0] ? from(byWay[0]) : 0;
		byWay.forEach((m, j) => {
			const a = first + (j / byWay.length) * Math.PI * 2;
			const spot = {
				x: at.x + Math.cos(a) * MOB_RINGS[k]!,
				y: at.y + Math.sin(a) * MOB_RINGS[k]!,
			};
			const near = dist(m.at, at) <= MOB_NEAR;
			m.hold(m.t + r() * 200);
			m.walk(spot, MOB_FPS[0] + r() * (MOB_FPS[1] - MOB_FPS[0]));
			const face = toward(spot, at);
			m.act("leap", 800, face);
			if (r() < 0.5) {
				m.act("leap", 800, face);
			}
			if (near) {
				until = Math.max(until, m.t);
			}
		});
	});
	until += MOB_MS;
	for (const m of [hero, ...mates]) {
		if (m !== hero && m.t < until - 1000 && r() < 0.35) {
			m.act("fistPump", Math.min(1500, until - m.t));
		}
		m.hold(until, "stand");
	}
	return { at, until };
};

// The officials, once it is over: off the field together toward the near
// sideline, out of the way - there until t1.
export const crewOff = (
	crew: CrewPart[],
	from: number,
	t1: number,
	r: () => number,
) => {
	for (const c of crew) {
		const free = freeAt(c.legs, from + CREW_GO + r() * CREW_LATE);
		cutAt(c.legs, free);
		const l = c.legs.at(-1)!;
		if (l.t1 < free) {
			c.legs.push({ t0: l.t1, t1: free, from: l.to, to: l.to, move: "stand" });
		}
		const man = new Man(c.team, c.legs, CREW_FPS * (0.9 + r() * 0.2));
		const ends = [TEAM_AREA[0] - CREW_PAST, TEAM_AREA[1] + CREW_PAST];
		const x = ends.reduce((a, b) =>
			Math.abs(b - man.at.x) < Math.abs(a - man.at.x) ? b : a,
		);
		man.walk({ x: x + (r() - 0.5) * 10, y: FIELD_W + 1 + r() * 2 });
		man.hold(t1);
	}
};
