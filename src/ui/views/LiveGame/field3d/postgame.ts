import { drawnBody, footballBody } from "./body.ts";
import { posOf, type CrewPart } from "./crew.ts";
import type { BallSeg, Leg, Part } from "./director.ts";
import {
	dist,
	FIELD_L,
	FIELD_W,
	MID_X,
	MID_Y,
	sideOn,
	TEAM_AREA,
	type Pt,
	type Side,
} from "./geometry.ts";
import { HIGH_FIVE_AT, handsMeet, moveKind, type Move } from "./poses.ts";
import { stageShower, type Shower } from "./shower.ts";
import { coachId, headCoachAt, sidelineOf } from "./sideline.ts";

// THE GAME OVER. The winners' moment where the end of it finds them - up
// off their feet, chests bumped, hands slapped, a fist pumped (a rout: a
// clap, and that is all; won on the last play, all of them all over the
// man who won it) - and the beaten side's: hands on hips, on helmets, a man
// down on a knee. Then both sides out toward the middle of the field, each
// man to one of the other side's: a handshake, a hug now and then, and on
// to the next.
// And the two head coaches out from their sidelines through it all, to
// shake hands in the middle - the picture in on them as they do.

// From this long after the whistle (ms).
const GO_AFTER = 350;
// A win by this many points or more is a rout.
const ROUT = 17;
// The winners: this share of them chest to chest with a teammate (one no
// further off than this) - or, half the time, a high five - the rest each
// in a moment of his own.
const BUMP = 0.3;
const BUMP_NEAR = 15;
const BUMP_APART = 2;
const BUMP_MS = 900;
// (No quicker to each other than this, and set this long before it, ms.)
const BUMP_STEP_MS = 450;
const BUMP_SET_MS = 200;
const BUMP_FIVE = 0.5;
const FIVE_MS = 800;
// Won on the last play: the winners on the field to the man who won it, at a
// sprint (these paces), round him in a ring this far off him (this many
// in it) and another outside it - up off their feet with him, and all over
// him this long after the last of them gets there (of those no further off
// than this: the rest get there when they get there).
const MOB_RINGS: [number, number] = [2.6, 4.8];
const MOB_INNER = 6;
const MOB_FPS: [number, number] = [15, 19];
// Pulled up at the end of the way, from this pace on (ft/s): over this far
// at most, down to this share of his pace (no slower than this).
const PULL_UP_FPS = 7;
const PULL_UP_FT = 3;
const PULL_UP_TO = 0.45;
const PULL_UP_LEAST = 4.5;
// (Not for a step or two: not over less than this, feet.)
const PULL_UP_SHORT = 1;
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
// (Hurried along to get there in time - but a step or two over to the next
// man, no further than this, at his own pace.)
const SHORT_WALK_FT = 8;
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
// Most of them with their helmets off for the handshakes - this share,
// each a moment after his own moment - each pulling it off over this long,
// off his head this far into it.
const HELMET_OFF = 0.8;
const HELMET_LATE = 500;
const HELMET_MS = 1100;
const HELMET_UP = 0.5;
// The ball, if a man still has it: tossed down by his feet.
const TOSS_MS = 380;
// Still pulling up off a run when he would be free: let come to his stop
// first, if it is no further off than this.
const STOPPING_MS = 1500;
// The officials: off this long after it is over (and up to this much
// later), at a jog, to just off the near sideline past the end of the team
// area nearer them - this far past it.
const CREW_GO = 900;
const CREW_LATE = 900;
const CREW_FPS = 8;
const CREW_PAST = 18;

// Two men shaking hands, or slapping hands: how far apart they stand, and
// turned (see handsMeet) - right hand to right hand between them (as the
// men are drawn).
const { apart: SHAKE_APART, turn: SHAKE_TURN } = handsMeet(
	drawnBody(footballBody()),
	"shake",
	0.24,
);
const FIVE = handsMeet(drawnBody(footballBody()), "highFive", HIGH_FIVE_AT);

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
	"pileIn",
	"overPile",
	"offPile",
	"pileDive",
	"pileTop",
	"offTop",
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
	// The winners' head coach under the cooler (see shower.ts).
	shower?: Shower;
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
	// (Slowing to a stop still: there first - not stood still in a frame,
	// at speed.)
	let end = at;
	for (; k < legs.length; k++) {
		const l = legs[k]!;
		if (l.t1 <= end) {
			continue;
		}
		if (l.from.x === l.to.x && l.from.y === l.to.y) {
			return end;
		}
		end = l.t1;
		if (moveKind(l.move) !== "cycle" || end - at > STOPPING_MS) {
			break;
		}
	}
	return at;
};

// Everything he does after t thrown away: what he was in the middle of cut
// short there.
export const cutAt = (legs: Leg[], t: number) => {
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
	// (The pace he walks at, left to himself.)
	private readonly own: number;
	// His helmet off, from now on.
	bare = false;
	constructor(team: Side, legs: Leg[], pace: number) {
		this.team = team;
		this.legs = legs;
		this.pace = pace;
		this.own = pace;
	}
	private get off(): { bare?: true } {
		return this.bare ? { bare: true } : {};
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
			...this.off,
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
			...this.off,
		});
	}
	// His helmet pulled off in both hands, and down in his left: off as it
	// comes up over his head.
	helmetOff() {
		this.act("helmetOff", HELMET_MS * HELMET_UP, undefined, undefined, [
			0,
			HELMET_UP,
		]);
		this.bare = true;
		this.act("helmetOff", HELMET_MS * (1 - HELMET_UP), undefined, undefined, [
			HELMET_UP,
			1,
		]);
	}
	// To `to` at his pace (or this one); when he gets there. (From a jog or
	// more, pulled up over the last of the way - PULL_UP_FT, or a third of
	// it - down to PULL_UP_TO of his pace: not stopped dead there.)
	walk(to: Pt, pace?: number): number {
		const far = dist(this.at, to);
		const fps =
			pace ?? (far < SHORT_WALK_FT ? Math.min(this.pace, this.own) : this.pace);
		if (far > 0.3) {
			const most = Math.min(PULL_UP_FT, far / 3);
			const tail = fps >= PULL_UP_FPS && most >= PULL_UP_SHORT ? most : 0;
			const slow = Math.max(PULL_UP_LEAST, fps * PULL_UP_TO);
			const u = (far - tail) / far;
			const from = this.at;
			const mid = {
				x: from.x + (to.x - from.x) * u,
				y: from.y + (to.y - from.y) * u,
			};
			this.step(mid, ((far - tail) / fps) * 1000, fps);
			if (tail > 0) {
				this.step(to, (tail / slow) * 1000, slow);
			}
		}
		return this.t;
	}
	// On through `to` at his pace (or this one), not pulling up there: a way
	// taken a stretch at a time.
	on(to: Pt, pace = this.pace): number {
		const far = dist(this.at, to);
		if (far > 0.05) {
			this.step(to, (far / pace) * 1000, pace);
		}
		return this.t;
	}
	private step(to: Pt, ms: number, fps: number) {
		const l = this.legs.at(-1)!;
		this.legs.push({
			t0: l.t1,
			t1: l.t1 + ms,
			from: l.to,
			to,
			move: fps < 6.5 ? "walk" : fps < 12.5 ? "jog" : "run",
			...this.off,
		});
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

// Each man (of the players) free of the play from `go`: up off the turf,
// done what he was doing - and the ball, if he has it, tossed down by his
// feet. Each at his own walking pace.
export const freeMen = (
	parts: Part[],
	go: number,
	ball: BallSeg[],
	r: () => number,
): { p: Part; man: Man }[] => {
	const out: { p: Part; man: Man }[] = [];
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
		out.push({ p, man: new Man(p.team, p.legs, pace) });
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
	// The two of the winners' men off the field with the cooler, if they
	// have it ready.
	shower?: { men: [number, number]; r: () => number };
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
	const freed = freeMen(parts, go, ball, r);
	const men = freed.map((f) => f.man);
	const hero = freed.find((f) => f.p.pid === input.hero)?.man;

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
			const way = sideOn(
				dist(a.at, b.at) > 0.5 ? toward(a.at, b.at) : r() * 6.28,
			);
			const five = r() < BUMP_FIVE;
			const half = (five ? FIVE.apart : BUMP_APART) / 2;
			const turn = five ? FIVE.turn : 0;
			const ux = Math.cos(way) * half;
			const uy = Math.sin(way) * half;
			// (A step or so to each other taken, not hopped.)
			const over = (m: Man, to: Pt) =>
				m.walk(to, Math.min(9, (dist(m.at, to) / BUMP_STEP_MS) * 1000));
			over(a, { x: mid.x - ux, y: mid.y - uy });
			over(b, { x: mid.x + ux, y: mid.y + uy });
			const at = Math.max(a.t, b.t) + BUMP_SET_MS;
			a.hold(at, "stand", way - turn);
			b.hold(at, "stand", way + Math.PI - turn);
			const move = five ? "highFive" : "chestBump";
			a.act(move, five ? FIVE_MS : BUMP_MS, way - turn);
			b.act(move, five ? FIVE_MS : BUMP_MS, way + Math.PI - turn);
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

	// Helmets off.
	for (const m of men) {
		if (r() < HELMET_OFF) {
			m.hold(m.t + r() * HELMET_LATE);
			m.helmetOff();
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

	// The head coaches: out from their sidelines to each other - the
	// winners' under the cooler first, if his men have it ready.
	const coaches: CrewPart[] = [];
	const bosses: Man[] = [];
	const dumpers: CrewPart[] = [];
	let shower: Shower | undefined;
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
		if (input.shower && team === winner && !mob) {
			const s = stageShower({
				men: input.shower.men,
				team,
				coach: coachId(team, 0),
				legs,
				at: from,
				yaw: toField,
				t0: input.t0,
				whistle: input.whistle,
				from: go,
				r: input.shower.r,
			});
			dumpers.push(...s.parts);
			shower = s.shower;
		}
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
	const others = dumpers.map((p) => new Man(p.team, p.legs, PACE[0]));
	for (const m of [...men, ...bosses, ...others]) {
		const l = m.legs.at(-1)!;
		m.hold(t1, moveKind(l.move) === "still" ? l.move : "stand");
	}
	return {
		coaches: [...coaches, ...dumpers],
		from: go,
		shake,
		at,
		winner,
		...(mob ? { mob } : {}),
		...(shower ? { shower } : {}),
		t1,
	};
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

// THE HALF - and the end of regulation, before overtime: the last play of
// it over, everybody off the field for the break, each side toward its own
// bench - at a walk, now and then a jog - from a moment after the whistle
// (and up to a little later), to just off the field; the picture on them
// this long, until it cuts to the kickoff after it.
const HALF_GO = 900;
const HALF_LATE = 1300;
// (Not stood there a moment between pulling up and walking off: straight
// on, with less than this to wait.)
const HALF_STILL_MS = 400;
const HALF_JOG = 0.3;
const HALF_JOG_FPS = 8.5;
const HALF_MS = 6500;
// (Along the field, this share of the way toward midfield, within the
// team area - and to this far off the field.)
const HALF_IN = 0.4;
const HALF_OFF = 4;

// When the play is over, now.
export const stageHalftime = (input: {
	parts: Part[];
	whistle: number;
	t1: number;
	ball: BallSeg[];
	r: () => number;
}): number => {
	const { parts, ball, r } = input;
	const go = input.whistle + HALF_GO;
	for (const { p, man } of freeMen(parts, go, ball, r)) {
		const leave = go + r() * HALF_LATE;
		man.hold(leave - man.t < HALF_STILL_MS ? man.t : leave);
		const line = sidelineOf(p.team);
		const x = clamp(
			man.at.x + (MID_X - man.at.x) * HALF_IN + (r() - 0.5) * 16,
			TEAM_AREA[0],
			TEAM_AREA[1],
		);
		const off = HALF_OFF + r() * 3;
		man.walk(
			{ x, y: line === 0 ? -off : FIELD_W + off },
			r() < HALF_JOG ? HALF_JOG_FPS : man.pace,
		);
	}
	return Math.max(input.t1, go + HALF_MS);
};
