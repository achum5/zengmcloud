import { makeCourtRng } from "../courtRng.ts";
import type { BallSeg, Leg, Part, PlayKind } from "./director.ts";
import type { Foul } from "./fouls.ts";
import {
	END_ZONE,
	FIELD_L,
	FIELD_W,
	HASH_FAR_Y,
	HASH_NEAR_Y,
	MID_Y,
	POST_W,
	dist,
	type Dir,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import type { Move } from "./poses.ts";

// THE OFFICIALS: an NFL crew of seven, where each lines up for a snap and
// where he goes with the play. The referee in the backfield behind the
// offense, on the right, fifteen yards deep; the umpire across from him -
// or over the ball behind the linebackers, at the goal line and late in a
// half; the down judge and the line judge on the line of scrimmage at
// either sideline; the side judge and the field judge twenty yards down
// the sidelines; the back judge deep in the middle. Each keeps his place
// to the ball as the play goes - the men in the backfield behind it, the
// deep men ahead of it, the wings level with it - and, the ball dead, in
// toward where it is down: the wing on that side to mark it, the umpire to
// spot it. A foul, and the nearest of the men who would see it throws his
// flag; after the play the referee faces the press box and gives the
// signal for it. A score, an incompletion, a kick good or not, a first
// down: the man nearest it signals that.

export const CREW = ["R", "U", "DJ", "LJ", "SJ", "FJ", "BJ"] as const;
export type Official = (typeof CREW)[number];
export const officialId = (o: Official): number => -1 - CREW.indexOf(o);
export const officialOf = (id: number): Official | undefined =>
	id < 0 ? CREW[-1 - id] : undefined;

export type CrewPart = { pid: number; team: Side; legs: Leg[] };

// The crew working a game: each official's number and build, the same
// every time the game is watched.
export const crewFor = (
	gid: number,
): {
	o: Official;
	id: number;
	number: string;
	hgt: number;
	weight: number;
}[] => {
	const r = makeCourtRng(`crew|${gid}`);
	const taken = new Set<number>();
	return CREW.map((o) => {
		let n = 1 + Math.floor(r() * 135);
		while (taken.has(n)) {
			n = 1 + (n % 135);
		}
		taken.add(n);
		return {
			o,
			id: officialId(o),
			number: String(n),
			hgt: 69 + Math.floor(r() * 7),
			weight: 185 + Math.floor(r() * 50),
		};
	});
};

export type CrewInput = {
	kind: PlayKind;
	offense: Side;
	dir: Dir;
	losX: number;
	ballY: number;
	t0: number;
	set: number;
	snap: number;
	// The kick away (the snap, for a play from scrimmage).
	follow: number;
	whistle: number;
	t1: number;
	parts: Part[];
	ball: BallSeg[];
	// The ball dead back where it was snapped from: an incompletion.
	backToLine: boolean;
	// The umpire over the ball, on the defense's side of it.
	umpireDeep: boolean;
	// The fouls called (see fouls.ts); the referee's word on each after the
	// play, when its line is said; what else gets signaled, and when.
	fouls: Foul[];
	calls: { t: number; foul: Foul }[];
	signals: { what: Signaled; t: number; at: Pt; way?: Dir }[];
	// The spots marked with a bean bag (see BAG_NEAR): when, and where.
	bags?: { t: number; at: Pt }[];
	r: () => number;
};

export type Signaled =
	| "td"
	| "incomplete"
	| "good"
	| "noGood"
	| "safety"
	| "first"
	| "out"
	| "timeout"
	// Whose ball it is now - a fumble's, a takeaway's - pointed the way they
	// go (see `way`).
	| "ball"
	| "touchback";

// A flag thrown: from his hand at t0, down on the turf at `to` at t1, lying
// there from then on.
export type FlagFlight = { t0: number; t1: number; from: Pt3; to: Pt };

// Just off the field: the wings on the sidelines, the deep men a step in.
const OFF_FIELD = 3;
const ON_SIDELINE = 2;
// A snap from scrimmage: the umpire over the ball (this far off it) as the
// picture comes up on it, putting it down (this long) and stood a moment -
// then away from it this far and across as far as his place is - wide of
// the huddle - and on to it, there this long before the offense is set, at
// no more than this pace (or he is there already).
const SPOTTED = new Set<PlayKind>(["pass", "run", "kneel", "none"]);
const SPOT_OFF = 1.6;
const SPOT_MS = 900;
const SPOT_STAND = 250;
const SPOT_WIDE = 10;
const SPOT_BY = 200;
const SPOT_FPS = 18;
// How fast an official goes (feet a second): flat out, turned and running
// - and an easy jog, a walk.
const TOP = 21;
const TURN_AND_RUN = 14;
const JOG = 10;
const WALK = 4.6;
// (Told to stop where he is and give a signal, still going quicker than
// this: pulling up turned the way he goes - not slid to a stop on his
// feet turned square to it, walking.)
const HELD_TURN_FPS = 6.5;
const ACCEL = 20;
const STEP_MS = 150;
const REACT_MS = 250;
// A man in the way of an official on his way somewhere (a player, where he
// is this long on, ms): gone round, by him this far off (center to center,
// feet), the side of him he is on already - or the other, where there is
// not this much (of that) room by him - looked for no further on than
// this, none nearer where he is going than this, and not when he goes no
// quicker than this (ft/s); turned no further off his way than this (rad).
// A man down on the turf, all of him that matters: these far (feet) along
// him from his hips to his head.
const ROUND_CLEAR = 2.6;
const ROUND_ROOM = 0.8;
const ROUND_LOOK = 10;
const ROUND_AHEAD_MS = 250;
const ROUND_NOT_NEAR = 3.5;
const ROUND_LEAST_FPS = 1.5;
const ROUND_TURN = 1.2;
const LIE_AT = [-0.4, 1.2, 2.8];
const ON_FRONT = new Set<Move>([
	"prone",
	"hurtProne",
	"tackled",
	"dive",
	"pileDive",
	"diveCatch",
]);
const ON_BACK = new Set<Move>(["supine", "hurt", "tackledBack"]);
// The flag: in his hand over his head (its height, feet) as it goes; at
// most this far from him it comes down.
const FLAG_Z = 7.2;
const FLAG_REACH = 40;
// A bean bag (to mark a spot: a punt fielded, a fumble): tossed by an
// official no further from it than this, his arm through it over this long
// either side of the moment; from this high, down on it in this long and
// this much more a foot it goes, at most this long.
const BAG_NEAR = 30;
const BAG_ARMS = 300;
const BAG_Z = 5.5;
const BAG_MS = 350;
const BAG_MS_FT = 12;
const BAG_MOST_MS = 900;
// The whistle at the end of the play: blown by the man nearest the ball
// then (no further off than this, feet) not busy with his arms, this long
// after it and for this long (ms).
const WHISTLE_NEAR = 45;
const WHISTLE_AFTER_MS = 80;
const WHISTLE_MS = 900;
// The referee on the microphone: the signal, then the side that fouled (or
// the wave of a call declined); stopped and faced to the press box a moment
// before it.
const CALL_SIGNAL_MS = 1500;
const CALL_SIDE_MS = 1000;
const SETTLE_MS = 600;
export const CALL_FOR_MS = CALL_SIGNAL_MS + CALL_SIDE_MS;
// (Talking from this far into it, ms.)
const CALL_TALK_AFTER = 150;
// The picture in close on him giving it (see aim.ts): from down on the
// field this far in front of him.
export const REF_CAM_FT = 20;
// The foul told to the referee (see stageCrew): from this long after the
// whistle, over to him at this pace (ft/s), the referee no further than
// this to meet him (feet), and no nearer the sidelines than this (with the
// picture in close on him from in front - REF_CAM_FT - on the field), this
// far apart side by side; over the last of it (feet off), turned to him;
// over this long to get there on top of the going, at it this long at
// least, his signal shown from this long in for this long (from this long
// in, from far off) - done this long before the referee turns to give it.
const TOLD_GO = 300;
const TOLD_FPS = 13;
const TOLD_R_MOST = 15;
const TOLD_IN = 6;
const TOLD_APART = 3.6;
const TOLD_TURN_FT = 2.5;
const TOLD_EASE_MS = 450;
const TOLD_LEAST = 900;
const TOLD_SIGN_AFTER = 150;
const TOLD_SIGN_MS = 1100;
const TOLD_FAR_SIGN = 500;
const TOLD_GAP = 150;
// (Off this far further along from him after.)
const TOLD_BACK = 8;
const TOLD_BEHIND = 0.8;
// (On the side of him nobody is standing at: no player this near the spot
// there, looked at every so often, ms.)
const TOLD_CLEAR = 2.5;
const TOLD_CLEAR_STEP = 100;
// How long each other signal is held.
const SIGNAL_MS: Record<Signaled, number> = {
	td: 2000,
	incomplete: 1300,
	good: 2000,
	noGood: 1300,
	safety: 1800,
	first: 1500,
	out: 1300,
	timeout: 1600,
	ball: 1500,
	touchback: 1500,
};
// Whose ball it is: pointed out by the first two officials in this near it
// (feet) - by this long after the time for it at the latest, looked for
// this often (ms) - the one no sooner than this long after the other.
// (Faced to the press box, unless he is on the near sideline - then in to
// the field: this far in from it, feet.)
const BALL_NEAR = 50;
const BALL_LATE = 1800;
const BALL_STEP = 100;
const BALL_AFTER = 250;
const BALL_IN = 3;
// (Nobody on the field this near where he stops for it, from just before
// to the end of it - looked at these long in, ms.)
const BALL_ROOM = 5;
const BALL_ROOM_AT = [-200, 300, 800, 1300];
// (At it this long, at least, before the next play.)
const BALL_LEAST = 1000;
// Facing the press box.
const FACING_US = Math.PI / 2;

// Where he keeps himself, along the field, to the ball (in the offense's
// frame): no farther behind it than `lo` past where it is, no farther
// ahead of it than `hi` - and across the field, held to his sideline, or
// drawn some way over toward the ball.
type Keep = {
	lo?: number;
	hi?: number;
	y?: number;
	toward?: number;
};
type Post = {
	o: Official;
	at: Pt;
	// The way he stands waiting for the snap.
	stance: Move;
	keep: Keep;
};

type OPt = { d: number; a: number };

const clamp = (x: number, lo: number, hi: number) =>
	Math.min(hi, Math.max(lo, x));

const gait = (fps: number): Move =>
	fps < 6.5 ? "walk" : fps < 12.5 ? "jog" : fps < 21 ? "run" : "sprint";

export const posOf = (legs: Leg[], t: number): Pt => {
	for (let k = legs.length - 1; k >= 0; k--) {
		const l = legs[k]!;
		if (t >= l.t0) {
			const u = l.t1 > l.t0 ? clamp((t - l.t0) / (l.t1 - l.t0), 0, 1) : 1;
			return {
				x: l.from.x + (l.to.x - l.from.x) * u,
				y: l.from.y + (l.to.y - l.from.y) * u,
			};
		}
	}
	return legs[0]!.from;
};

// Where the ball is at t, along the ground - with a man, or loose on it.
// (In the air, or stood up for a kick, unsaid: nobody goes by that.)
const groundBall = (
	segs: BallSeg[],
	parts: Part[],
	t: number,
): Pt | undefined => {
	let s: BallSeg | undefined;
	for (const x of segs) {
		if (x.t0 <= t) {
			s = x;
		}
	}
	if (!s || s.kind === "fly" || (s.kind === "spot" && s.stood)) {
		return undefined;
	}
	if (s.kind === "spot") {
		return s.at;
	}
	const pid = s.pid;
	const p = parts.find((x) => x.pid === pid);
	return p ? posOf(p.legs, t) : undefined;
};
// And, wherever it is, where it is over the ground.
export const anyBall = (
	segs: BallSeg[],
	parts: Part[],
	t: number,
): Pt | undefined => {
	let s: BallSeg | undefined;
	for (const x of segs) {
		if (x.t0 <= t) {
			s = x;
		}
	}
	if (s?.kind === "fly") {
		const u = s.t1 > s.t0 ? clamp((t - s.t0) / (s.t1 - s.t0), 0, 1) : 1;
		return {
			x: s.from.x + (s.to.x - s.from.x) * u,
			y: s.from.y + (s.to.y - s.from.y) * u,
		};
	}
	return s?.kind === "spot" ? s.at : groundBall(segs, parts, t);
};

// Stopped where he is, facing `yaw`, from t0 to t1 (to give a signal) -
// or, `to` somewhere, over there at that pace first.
type Hold = {
	o: Official;
	t0: number;
	t1: number;
	yaw: number;
	to?: Pt;
	fps?: number;
};

// The leg he is in at t made two, there.
const splitAt = (legs: Leg[], t: number) => {
	const k = legs.findIndex((l) => l.t0 < t && t < l.t1);
	if (k < 0) {
		return;
	}
	const l = legs[k]!;
	const u = (t - l.t0) / (l.t1 - l.t0);
	const mid = {
		x: l.from.x + (l.to.x - l.from.x) * u,
		y: l.from.y + (l.to.y - l.from.y) * u,
	};
	const [q0, q1] = l.armsSpan ?? [0, 1];
	legs.splice(k + 1, 0, {
		...l,
		t0: t,
		from: mid,
		...(l.arms
			? { armsSpan: [q0 + (q1 - q0) * u, q1] as [number, number] }
			: {}),
	});
	l.t1 = t;
	l.to = mid;
	if (l.arms) {
		l.armsSpan = [q0, q0 + (q1 - q0) * u];
	}
};

// His arms through `move` over t0..t1, whatever the rest of him is doing -
// with his left arm, `mirrored`.
const armsOver = (
	legs: Leg[],
	move: Move,
	t0: number,
	t1: number,
	mirrored = false,
) => {
	splitAt(legs, t0);
	splitAt(legs, t1);
	for (const l of legs) {
		if (l.t0 >= t0 && l.t1 <= t1 && l.t1 > l.t0) {
			l.arms = move;
			l.armsSpan = [(l.t0 - t0) / (t1 - t0), (l.t1 - t0) / (t1 - t0)];
			if (mirrored) {
				l.mirrored = true;
			}
		}
	}
};

// Talking over t0..t1, whatever he is doing.
const talkOver = (legs: Leg[], t0: number, t1: number) => {
	splitAt(legs, t0);
	splitAt(legs, t1);
	for (const l of legs) {
		if (l.t0 >= t0 && l.t1 <= t1 && l.t1 > l.t0) {
			l.talk = true;
		}
	}
};

// Whether, facing `yaw`, he points his left arm (rather than his right)
// out to the side to point along the field `way` (+1 up it, -1 down it).
const leftArm = (yaw: number, way: number) => -Math.sin(yaw) * way < 0;

export const stageCrew = (
	input: CrewInput,
): {
	parts: CrewPart[];
	flags: FlagFlight[];
	bags: FlagFlight[];
	// The foul told to the referee (see below): from when to when, and
	// whether at his side.
	report?: { t0: number; t1: number; near: boolean };
} => {
	const { kind, dir, losX, ballY, r } = input;
	const w = (p: OPt): Pt => ({ x: losX + dir * p.d, y: ballY + dir * p.a });
	const of = (p: Pt): OPt => ({
		d: (p.x - losX) * dir,
		a: (p.y - ballY) * dir,
	});
	// The goal line the offense goes for, the end line behind it, and its
	// own, in its frame; the sidelines, off and on the field.
	const goalD = ((dir === 1 ? FIELD_L - END_ZONE : END_ZONE) - losX) * dir;
	const endD = goalD + END_ZONE - 2;
	const ownEndD = ((dir === 1 ? 0 : FIELD_L) - losX) * dir + 2;
	const farOff = -OFF_FIELD;
	const nearOff = FIELD_W + OFF_FIELD;
	const farIn = ON_SIDELINE;
	const nearIn = FIELD_W - ON_SIDELINE;
	const yd = (d: number, y: number): Pt => ({ x: losX + dir * d, y });
	const deep = (d: number) => Math.min(d, endD);

	const posts: Post[] = [];
	const post = (o: Official, at: Pt, keep: Keep, stance: Move = "stand") =>
		posts.push({ o, at, keep, stance });
	if (kind === "kickoff" || kind === "onside") {
		// The referee back at the goal line the ball is kicked to, the wings
		// up at the receiving team's setup zone, the side and field judges on
		// the goal line, the umpire and back judge on the kicking team's line
		// at either sideline - each with the return as it comes back.
		const short = kind === "onside";
		post("R", w({ d: short ? 120 : goalD + 4, a: -14 }), {
			hi: 30,
			toward: 0.5,
		});
		post("SJ", yd(short ? 90 : goalD, farIn), { hi: 15, y: farIn });
		post("FJ", yd(short ? 90 : goalD, nearIn), { hi: 15, y: nearIn });
		post("DJ", yd(short ? 30 : 105, farOff), { hi: 0, y: farOff });
		post("LJ", yd(short ? 30 : 105, nearOff), { hi: 0, y: nearOff });
		post("U", yd(-6, farIn), { hi: -30, y: farIn });
		post("BJ", yd(-6, nearIn), { hi: -30, y: nearIn });
	} else if (kind === "punt") {
		// The referee behind the punter on his kicking side, the umpire over
		// the ball on the other side of it, the wings on the line, the side
		// and field judges down the sidelines short of the returner, the
		// back judge deep behind him.
		const back = input.parts
			.filter((p) => p.team !== input.offense)
			.reduce((m, p) => Math.max(m, of(p.legs[0]!.from).d), 60);
		const pAt = input.parts.find((p) => p.role === "P");
		const pd = pAt ? of(pAt.legs.at(-1)!.from).d : -45;
		post("R", w({ d: Math.max(ownEndD, Math.min(pd, -40) - 10), a: 18 }), {
			hi: -30,
		});
		post("U", w({ d: 18, a: -4 }), { hi: -25 });
		post("DJ", yd(0, farOff), { lo: 0, hi: 0, y: farOff });
		post("LJ", yd(0, nearOff), { lo: 0, hi: 0, y: nearOff });
		post("SJ", yd(deep(back - 36), farIn), { lo: 0, hi: 0, y: farIn });
		post("FJ", yd(deep(back - 36), nearIn), { lo: 0, hi: 0, y: nearIn });
		post("BJ", w({ d: deep(back + 15), a: 10 }), { hi: 25, toward: 0.5 });
	} else if (kind === "fieldGoal" || kind === "extraPoint") {
		// The referee behind the kicker, the umpire and the side judge over
		// the ball, the wings on the line - and the field judge and back
		// judge under the posts, one just outside each upright.
		const postX = dir === 1 ? FIELD_L : 0;
		post("R", w({ d: -36, a: -18 }), {});
		post("U", w({ d: 18, a: 4 }), {});
		post("SJ", w({ d: 18, a: 26 }), {});
		post("DJ", yd(0, farOff), {}, "hands");
		post("LJ", yd(0, nearOff), {}, "hands");
		post("FJ", { x: postX + dir * 2, y: MID_Y - POST_W / 2 - 2.5 }, {});
		post("BJ", { x: postX + dir * 2, y: MID_Y + POST_W / 2 + 2.5 }, {});
	} else {
		// From scrimmage.
		post("R", w({ d: Math.max(ownEndD, -42 - r() * 6), a: 16 + r() * 4 }), {
			lo: -60,
			hi: -24,
			toward: 0.25,
		});
		if (input.umpireDeep) {
			post("U", w({ d: deep(18 + r() * 4), a: -3 - r() * 4 }), {
				lo: -45,
				toward: 0.3,
			});
		} else {
			post("U", w({ d: Math.max(ownEndD, -36 - r() * 6), a: -12 - r() * 4 }), {
				lo: -60,
				hi: -18,
				toward: 0.25,
			});
		}
		post("DJ", yd(0, farOff), { lo: 0, hi: 0, y: farOff }, "hands");
		post("LJ", yd(0, nearOff), { lo: 0, hi: 0, y: nearOff }, "hands");
		post("SJ", yd(deep(60), farIn), { lo: 12, y: farIn });
		post("FJ", yd(deep(60), nearIn), { lo: 12, y: nearIn });
		post("BJ", w({ d: deep(75), a: -6 + r() * 12 }), { lo: 20, toward: 0.4 });
	}

	const { snap, follow, whistle, t0, t1 } = input;
	// Where the ball is down: where the whistle finds it - or, an
	// incompletion, back on the line.
	const dead = input.backToLine
		? w({ d: 0, a: 0 })
		: (anyBall(input.ball, input.parts, whistle) ?? w({ d: 0, a: 0 }));
	const deadO = of(dead);
	// (Spotted for the next snap no wider than the hashes.)
	const spotO = of({ x: dead.x, y: clamp(dead.y, HASH_FAR_Y, HASH_NEAR_Y) });
	const kick = kind === "fieldGoal" || kind === "extraPoint";
	// After the whistle, from where it finds him: in toward the ball - the
	// wing on its side of the field to mark it, the umpire to spot it, the
	// rest some of the way. (A kick at the posts, everybody where he is.)
	const afterWhistle = (o: Official, from: Pt): { to: Pt; fps: number } => {
		if (kick) {
			return { to: from, fps: WALK };
		}
		if (o === "U" && !input.backToLine) {
			return { to: w({ d: spotO.d - 3, a: spotO.a - 2 }), fps: JOG };
		}
		if (o === "R") {
			// (Not back out past the end line, dead near it.)
			return {
				to: onField(w({ d: deadO.d - 30, a: clamp(deadO.a + 15, -60, 60) })),
				fps: JOG,
			};
		}
		if (o === "DJ" || o === "LJ") {
			const far = o === "DJ";
			const side = far ? farOff : nearOff;
			const mine = far === dead.y < MID_Y;
			return {
				to: { x: dead.x, y: mine ? side + (far ? 12 : -12) : side },
				fps: mine ? JOG : WALK + 2,
			};
		}
		const far = dist(from, dead) || 1;
		const keep = 24 + keeps.get(o)! * 14;
		return {
			to:
				far > keep
					? {
							x: dead.x + ((from.x - dead.x) / far) * keep,
							y: dead.y + ((from.y - dead.y) / far) * keep,
						}
					: from,
			fps: JOG - 2,
		};
	};
	const keeps = new Map(posts.map((p) => [p.o, r()]));
	const walk = (p: Post, holds: Hold[]): Leg[] => {
		// (The players, each where he is as the play goes on - looked at
		// in order, from where he was last looked at.)
		const others = input.parts
			.filter((q) => q.pid >= 0 && q.legs.length > 0)
			.map((q) => ({ legs: q.legs, k: 0 }));
		// The nearest man in his way on to `target` (`far` off, the way
		// ux, uy), where they are at t: the spot to go by him at, the side
		// of him he is on already - or the other, somebody there.
		const roundBy = (
			at: Pt,
			ux: number,
			uy: number,
			target: Pt,
			far: number,
			t: number,
		): Pt | undefined => {
			const look = Math.min(far, ROUND_LOOK);
			const pts: Pt[] = [];
			for (const o of others) {
				while (o.k < o.legs.length - 1 && o.legs[o.k]!.t1 <= t) {
					o.k++;
				}
				const l = o.legs[o.k]!;
				const u = l.t1 > l.t0 ? clamp((t - l.t0) / (l.t1 - l.t0), 0, 1) : 1;
				const q = {
					x: l.from.x + (l.to.x - l.from.x) * u,
					y: l.from.y + (l.to.y - l.from.y) * u,
				};
				const reach = look + ROUND_CLEAR + LIE_AT.at(-1)!;
				if (Math.abs(q.x - at.x) > reach || Math.abs(q.y - at.y) > reach) {
					continue;
				}
				const lies = ON_FRONT.has(l.move)
					? 0
					: ON_BACK.has(l.move)
						? Math.PI
						: undefined;
				const body =
					lies === undefined || l.yaw === undefined
						? [q]
						: LIE_AT.map((f) => ({
								x: q.x + Math.cos(l.yaw! + lies) * f,
								y: q.y + Math.sin(l.yaw! + lies) * f,
							}));
				for (const b of body) {
					if (dist(b, target) >= ROUND_NOT_NEAR) {
						pts.push(b);
					}
				}
			}
			let first: Pt | undefined;
			let firstAlong = Infinity;
			let firstLat = 0;
			for (const b of pts) {
				const rx = b.x - at.x;
				const ry = b.y - at.y;
				const along = rx * ux + ry * uy;
				const lat = ry * ux - rx * uy;
				if (
					along > 0 &&
					along < look &&
					Math.abs(lat) < ROUND_CLEAR &&
					along < firstAlong
				) {
					first = b;
					firstAlong = along;
					firstLat = lat;
				}
			}
			if (!first) {
				return undefined;
			}
			const f = first;
			const side = firstLat >= 0 ? -1 : 1;
			const past = (s: number) =>
				onField({
					x: f.x - uy * s * ROUND_CLEAR,
					y: f.y + ux * s * ROUND_CLEAR,
				});
			const room = (w: Pt) => Math.min(...pts.map((b) => dist(b, w)));
			const mine = past(side);
			const other = past(-side);
			return room(mine) >= ROUND_CLEAR * ROUND_ROOM || room(mine) >= room(other)
				? mine
				: other;
		};
		const legs: Leg[] = [];
		const start = p.at;
		const startO = of(start);
		const facing = Math.atan2(ballY - start.y, losX - start.x);
		// (The umpire, a snap from scrimmage, over the ball as the picture
		// comes up on it - just put it down - and back to his place as the
		// offense comes out of its huddle.)
		let from = t0;
		if (p.o === "U" && SPOTTED.has(kind)) {
			const side = startO.d < 0 ? -1 : 1;
			const over = w({ d: side * SPOT_OFF, a: -SPOT_OFF / 2 });
			const wide = w({ d: side * SPOT_WIDE, a: startO.a });
			const leave = t0 + SPOT_MS + SPOT_STAND;
			const far = dist(over, wide) + dist(wide, start);
			const by = input.set - SPOT_BY;
			const fps = (far / Math.max(1, by - leave)) * 1000;
			if (fps <= SPOT_FPS) {
				const face = Math.atan2(ballY - over.y, losX - over.x);
				const at = t0 + SPOT_MS;
				const turn = leave + (dist(over, wide) / fps) * 1000;
				legs.push(
					{ t0, t1: at, from: over, to: over, move: "spotBall", yaw: face },
					{ t0: at, t1: leave, from: over, to: over, move: "stand", yaw: face },
					{ t0: leave, t1: turn, from: over, to: wide, move: gait(fps) },
					{ t0: turn, t1: by, from: wide, to: start, move: gait(fps) },
				);
				from = by;
			}
		}
		// Standing waiting for the offense to come up, and set for the snap.
		legs.push(
			{
				t0: from,
				t1: input.set,
				from: start,
				to: start,
				move: "stand",
				yaw: facing,
			},
			{
				t0: input.set,
				t1: snap + REACT_MS,
				from: start,
				to: start,
				move: p.stance,
				yaw: facing,
			},
		);
		// Through the play, a step at a time, after where he should be.
		let at = start;
		let vx = 0;
		let vy = 0;
		let goal: OPt = startO;
		let after: { to: Pt; fps: number } | undefined;
		for (let t = snap + REACT_MS; t < t1; t += STEP_MS) {
			const t2 = Math.min(t1, t + STEP_MS);
			const playing = t < whistle + 200;
			const hold = holds.find((h) => t >= h.t0 && t < h.t1);
			let target: Pt;
			let fps = TOP;
			if (hold) {
				target = hold.to ?? at;
				if (hold.to) {
					fps = hold.fps ?? JOG;
				}
			} else if (playing) {
				const ball =
					t >= follow ? groundBall(input.ball, input.parts, t) : undefined;
				if (ball) {
					const b = of(ball);
					const k = p.keep;
					let d = goal.d;
					if (k.hi !== undefined) {
						d = Math.min(d, b.d + k.hi);
					}
					if (k.lo !== undefined) {
						d = Math.max(d, b.d + k.lo);
					}
					// (The wings keep to the line while it is behind it.)
					if (k.lo === 0 && k.hi === 0 && b.d < 0) {
						d = Math.max(d, Math.min(startO.d, 0) - 8);
					}
					const a =
						k.toward !== undefined
							? startO.a + (b.a - startO.a) * k.toward
							: goal.a;
					goal = { d: Math.min(d, endD + 6), a };
				}
				const g = w(goal);
				target = p.keep.y !== undefined ? { x: g.x, y: p.keep.y } : onField(g);
			} else {
				after ??= afterWhistle(p.o, at);
				target = after.to;
				fps = after.fps;
			}
			// Toward it, speeding up and slowing down as a man does - round
			// anybody in his way to it, not through him.
			const dx = target.x - at.x;
			const dy = target.y - at.y;
			const far = Math.hypot(dx, dy);
			let want = Math.min(fps, far / 0.4);
			let ux = far > 0.01 ? dx / far : 0;
			let uy = far > 0.01 ? dy / far : 0;
			const by =
				want >= ROUND_LEAST_FPS
					? roundBy(at, ux, uy, target, far, t2 + ROUND_AHEAD_MS)
					: undefined;
			if (by && dist(at, by) > 0.01) {
				// (Never back the way he came: no more than so far round,
				// slowed to a walk, if it takes more.)
				const off = Math.atan2(
					(by.y - at.y) * ux - (by.x - at.x) * uy,
					(by.x - at.x) * ux + (by.y - at.y) * uy,
				);
				const turn = clamp(off, -ROUND_TURN, ROUND_TURN);
				if (turn !== off) {
					want = Math.min(want, WALK);
				}
				const c = Math.cos(turn);
				const s = Math.sin(turn);
				[ux, uy] = [ux * c - uy * s, ux * s + uy * c];
			}
			const secs = (t2 - t) / 1000;
			let nvx = vx + clamp(ux * want - vx, -ACCEL * secs, ACCEL * secs);
			let nvy = vy + clamp(uy * want - vy, -ACCEL * secs, ACCEL * secs);
			const sp = Math.hypot(nvx, nvy);
			if (sp > fps) {
				nvx *= fps / sp;
				nvy *= fps / sp;
			}
			vx = nvx;
			vy = nvy;
			const to = { x: at.x + vx * secs, y: at.y + vy * secs };
			const speed = Math.hypot(vx, vy);
			// Facing the ball: walking or running to it, stepping across,
			// backing off - or, going fast, turned and running.
			const ball = anyBall(input.ball, input.parts, t) ?? dead;
			const look = Math.atan2(ball.y - at.y, ball.x - at.x);
			const going = Math.atan2(vy, vx);
			let move: Move;
			let yaw: number;
			if (
				hold &&
				speed >= 0.8 &&
				((hold.to && dist(at, hold.to) > TOLD_TURN_FT) ||
					(speed > HELD_TURN_FPS && Math.cos(going - hold.yaw) > -0.4))
			) {
				// (On his way over, or still pulling up: turned the way he
				// goes.)
				move = gait(speed);
				yaw = going;
			} else if (hold) {
				// (Backing off, still turned the way he holds.)
				move =
					speed < 0.8
						? "stand"
						: Math.cos(going - hold.yaw) < -0.4
							? "back"
							: "walk";
				yaw = hold.yaw;
			} else if (speed < 0.8) {
				move = "stand";
				yaw = look;
			} else if (speed > TURN_AND_RUN || !playing) {
				move = gait(speed);
				yaw = going;
			} else {
				const rel = Math.cos(going - look);
				if (rel > 0.4) {
					move = gait(speed);
					yaw = going;
				} else if (rel < -0.4) {
					move = "back";
					yaw = look;
				} else if (speed < 6.5) {
					// (Stepping across, still facing it.)
					move = "walk";
					yaw = look;
				} else {
					move = gait(speed);
					yaw = going;
				}
			}
			const l = legs.at(-1)!;
			const still = l.from.x === l.to.x && l.from.y === l.to.y;
			if (
				speed < 0.8 &&
				still &&
				l.move === move &&
				Math.abs((l.yaw ?? 0) - yaw) < 0.2
			) {
				l.t1 = t2;
			} else {
				legs.push({
					t0: l.t1,
					t1: t2,
					from: l.to,
					to: speed < 0.8 ? l.to : to,
					move,
					yaw,
				});
			}
			if (speed >= 0.8) {
				at = to;
			}
		}
		return legs;
	};
	const legsOf = new Map(posts.map((p) => [p.o, walk(p, [])]));
	const where = (o: Official, t: number) => posOf(legsOf.get(o)!, t);
	const nearest = (among: readonly Official[], pt: Pt, t: number) =>
		[...among].sort(
			(a, b) => dist(where(a, t), pt) - dist(where(b, t), pt),
		)[0]!;

	// Who signals what: the referee each call, stopped and faced to the
	// press box; the man nearest anything else, stopped and faced in to
	// the field - and what each does with his arms.
	const holds: Hold[] = [];
	const arms: {
		o: Official;
		move: Move;
		t0: number;
		t1: number;
		left?: boolean;
	}[] = [];
	for (const c of input.calls) {
		const { foul } = c;
		holds.push({
			o: "R",
			t0: c.t - SETTLE_MS,
			t1: c.t + CALL_SIGNAL_MS + CALL_SIDE_MS,
			yaw: FACING_US,
		});
		arms.push({
			o: "R",
			move: foul.signal,
			t0: c.t,
			t1: c.t + CALL_SIGNAL_MS,
		});
		// (Toward the end the side that fouled defends.)
		const way = (foul.side === input.offense ? -1 : 1) * dir;
		arms.push({
			o: "R",
			move: foul.accepted ? "sigSide" : "sigWave",
			t0: c.t + CALL_SIGNAL_MS,
			t1: c.t + CALL_SIGNAL_MS + CALL_SIDE_MS,
			left: foul.accepted && leftArm(FACING_US, way),
		});
	}
	for (const sg of input.signals) {
		const ms = SIGNAL_MS[sg.what];
		const who: Official[] =
			sg.what === "good" || sg.what === "noGood"
				? ["FJ", "BJ"]
				: sg.what === "timeout"
					? ["R"]
					: sg.what === "td"
						? [...CREW]
								.filter((o) => o !== "R")
								.sort(
									(a, b) =>
										dist(where(a, sg.t), sg.at) - dist(where(b, sg.t), sg.at),
								)
								.slice(0, 2)
						: sg.what === "ball"
							? []
							: [
									nearest(
										sg.what === "first"
											? ["DJ", "LJ"]
											: sg.what === "out"
												? ["DJ", "LJ", "SJ", "FJ"]
												: sg.what === "touchback"
													? CREW
													: CREW.filter((o) => o !== "R"),
										sg.at,
										sg.t,
									),
								];
		// Whose ball it is: pointed out by the first two to get in near it
		// once it is (each when he does).
		const when = new Map<Official, number>();
		if (sg.what === "ball") {
			for (const o of CREW) {
				if (o === "R") {
					continue;
				}
				const late = Math.min(sg.t + BALL_LATE, t1 - BALL_LEAST);
				for (let t = sg.t; t <= late; t += BALL_STEP) {
					// (Stopped there, out of the way of the men getting up off
					// it and going.)
					const at = where(o, t);
					if (
						dist(at, sg.at) <= BALL_NEAR &&
						!input.parts.some((p) =>
							BALL_ROOM_AT.some(
								(ms) => dist(posOf(p.legs, t + ms), at) < BALL_ROOM,
							),
						)
					) {
						when.set(o, t);
						break;
					}
				}
			}
			const firsts = [...when.keys()]
				.filter(
					(o) =>
						!holds.some(
							(h) =>
								h.o === o && h.t1 > when.get(o)! && h.t0 < when.get(o)! + ms,
						),
				)
				.sort((a, b) => when.get(a)! - when.get(b)!)
				.slice(0, 2);
			if (firsts.length === 2) {
				when.set(
					firsts[1]!,
					Math.max(when.get(firsts[1]!)!, when.get(firsts[0]!)! + BALL_AFTER),
				);
			}
			who.push(...firsts);
		}
		for (const o of who) {
			const t = when.get(o) ?? sg.t;
			// (Busy with a call of his own: not this.)
			if (holds.some((h) => h.o === o && h.t1 > t && h.t0 < t + ms)) {
				continue;
			}
			// (Faced in to the field - under the posts, back out to the kick;
			// whose ball it is, or a touchback, to the press box, if he is
			// out on it.)
			const p = where(o, t);
			const yaw =
				sg.what === "good" || sg.what === "noGood"
					? dir === 1
						? Math.PI
						: 0
					: sg.what === "timeout" ||
						  ((sg.what === "ball" || sg.what === "touchback") &&
								p.y < FIELD_W - BALL_IN)
						? FACING_US
						: p.y > MID_Y
							? -Math.PI / 2
							: Math.PI / 2;
			holds.push({ o, t0: t - 200, t1: t + ms, yaw });
			arms.push({
				o,
				move:
					sg.what === "td" || sg.what === "good"
						? "sigTD"
						: sg.what === "safety"
							? "sigSafety"
							: sg.what === "first" || sg.what === "ball"
								? "sigSide"
								: sg.what === "out" || sg.what === "timeout"
									? "sigTime"
									: sg.what === "touchback"
										? "waveOver"
										: "sigWave",
				t0: t,
				t1: t + ms,
				left:
					(sg.what === "first" || sg.what === "ball") &&
					leftArm(yaw, sg.way ?? dir),
			});
		}
	}
	// THE FOUL TOLD TO THE REFEREE: the first seen in the play, by the man
	// who threw for it (not the referee's own), once it is dead - over to
	// him, the referee a few steps to meet him, side by side to the press
	// box, the foul's signal shown him as he tells it - then off as the
	// referee turns to give it. From too far off for that, stopped where he
	// is, turned to him and the signal shown him from there.
	let report: { t0: number; t1: number; near: boolean } | undefined;
	let beside:
		| {
				o: Official;
				side: number;
				there: Hold;
				at: Hold;
				after: Hold;
				ref: Hold;
		  }
		| undefined;
	const thrower = new Map<Foul, Official>();
	const told = input.calls.find((c) => c.foul.told);
	if (told) {
		const f = told.foul;
		const o = nearest(f.by, f.at, f.t);
		thrower.set(f, o);
		const by = told.t - SETTLE_MS - TOLD_GAP;
		const go = Math.max(
			whistle + TOLD_GO,
			...holds
				.filter((h) => (h.o === o || h.o === "R") && h.t0 < by)
				.map((h) => h.t1),
		);
		if (o !== "R" && by - go >= TOLD_LEAST) {
			const pO = where(o, go);
			const pR = where("R", go);
			const apart = dist(pO, pR) || 1;
			const step = Math.min(apart / 2, TOLD_R_MOST);
			const rTo = {
				x: clamp(pR.x + ((pO.x - pR.x) / apart) * step, 2, FIELD_L - 2),
				y: clamp(
					pR.y + ((pO.y - pR.y) / apart) * step,
					TOLD_IN,
					FIELD_W - REF_CAM_FT - TOLD_IN,
				),
			};
			const side = pO.x >= rTo.x ? 1 : -1;
			// (A touch back of him from the press box, never in front.)
			const oTo = { x: rTo.x + side * TOLD_APART, y: rTo.y - TOLD_BEHIND };
			const met =
				go +
				Math.max(
					(dist(pO, oTo) / TOLD_FPS) * 1000,
					(dist(pR, rTo) / JOG) * 1000,
				) +
				TOLD_EASE_MS;
			const faceR = Math.atan2(rTo.y - oTo.y, rTo.x - oTo.x);
			if (by - met >= TOLD_LEAST) {
				// (Then a few steps off along the field, out of the picture
				// in close on the referee - not across in front of him - and
				// turned to him again as he gives it.)
				const off = { x: oTo.x + side * TOLD_BACK, y: oTo.y };
				const gone = Math.max(...input.calls.map((c) => c.t)) + CALL_FOR_MS;
				beside = {
					o,
					side,
					there: { o, t0: go, t1: met, yaw: faceR, to: oTo, fps: TOLD_FPS },
					at: { o, t0: met, t1: by, yaw: faceR },
					after: { o, t0: by, t1: gone, yaw: faceR, to: off, fps: WALK },
					ref: {
						o: "R",
						t0: met,
						t1: told.t - SETTLE_MS,
						yaw: faceR + Math.PI,
					},
				};
				holds.push(
					beside.there,
					beside.at,
					beside.after,
					{ o: "R", t0: go, t1: met, yaw: faceR + Math.PI, to: rTo, fps: JOG },
					beside.ref,
				);
				arms.push({
					o,
					move: f.signal,
					t0: met + TOLD_SIGN_AFTER,
					t1: Math.min(by, met + TOLD_SIGN_AFTER + TOLD_SIGN_MS),
				});
				report = { t0: met, t1: by, near: true };
			} else {
				const yaw = Math.atan2(pR.y - pO.y, pR.x - pO.x);
				holds.push(
					{ o, t0: go, t1: by, yaw },
					{ o: "R", t0: go, t1: told.t - SETTLE_MS, yaw: yaw + Math.PI },
				);
				arms.push({
					o,
					move: f.signal,
					t0: go + TOLD_FAR_SIGN,
					t1: Math.min(by, go + TOLD_FAR_SIGN + TOLD_SIGN_MS),
				});
				report = { t0: go, t1: by, near: false };
			}
		}
	}
	for (const p of posts) {
		const mine = holds.filter((h) => h.o === p.o);
		if (mine.length > 0) {
			legsOf.set(p.o, walk(p, mine));
		}
	}
	// (Beside the referee where he has got to, not where he was going - on
	// the other side of him, if a player is stood where he would be.)
	if (beside && report) {
		const r = where("R", report.t0);
		const spot = (side: number) => ({
			x: r.x + side * TOLD_APART,
			y: r.y - TOLD_BEHIND,
		});
		const clear = (pt: Pt) => {
			for (let t = report!.t0; t <= report!.t1; t += TOLD_CLEAR_STEP) {
				if (
					input.parts.some(
						(q) =>
							q.pid >= 0 &&
							q.legs.length > 0 &&
							dist(posOf(q.legs, t), pt) < TOLD_CLEAR,
					)
				) {
					return false;
				}
			}
			return true;
		};
		if (!clear(spot(beside.side)) && clear(spot(-beside.side))) {
			beside.side = -beside.side;
		}
		const to = spot(beside.side);
		const yaw = Math.atan2(r.y - to.y, r.x - to.x);
		beside.there.to = to;
		// (And onto it, if he got there off it - round a man in his way,
		// say, or pulling up past it: not stood where he got to.)
		beside.at.to = to;
		beside.at.fps = WALK;
		beside.after.to = { x: to.x + beside.side * TOLD_BACK, y: to.y };
		for (const h of [beside.there, beside.at, beside.after]) {
			h.yaw = yaw;
		}
		// (The referee turned to him there - the other side, too.)
		const turned = beside.ref.yaw !== yaw + Math.PI;
		beside.ref.yaw = yaw + Math.PI;
		for (const p of posts.filter(
			(q) => q.o === beside!.o || (turned && q.o === "R"),
		)) {
			legsOf.set(
				p.o,
				walk(
					p,
					holds.filter((h) => h.o === p.o),
				),
			);
		}
	}

	// The flags: each thrown by the nearest of the men who would see it,
	// toward where it was, high and down there.
	const flags: FlagFlight[] = [];
	for (const f of input.fouls) {
		const o = thrower.get(f) ?? nearest(f.by, f.at, f.t);
		arms.push({ o, move: "flagToss", t0: f.t - 350, t1: f.t + 350 });
		const t0 = f.t + 175;
		const from = where(o, t0);
		const far = dist(from, f.at);
		const reach = Math.min(far, FLAG_REACH) / (far || 1);
		const to = onField({
			x: from.x + (f.at.x - from.x) * reach + (r() - 0.5) * 2,
			y: from.y + (f.at.y - from.y) * reach + (r() - 0.5) * 2,
		});
		flags.push({
			t0,
			t1: t0 + Math.min(1300, 650 + dist(from, to) * 14),
			from: { ...from, z: FLAG_Z },
			to,
		});
	}
	// The bean bags: each tossed down on its spot by the nearest man to it
	// then - no further off than BAG_NEAR, his arms not busy - low and
	// short.
	const bags: FlagFlight[] = [];
	for (const b of input.bags ?? []) {
		const o = [...CREW]
			.filter(
				(o) =>
					dist(where(o, b.t), b.at) <= BAG_NEAR &&
					!arms.some(
						(a) => a.o === o && a.t1 > b.t - BAG_ARMS && a.t0 < b.t + BAG_ARMS,
					),
			)
			.sort((x, y) => dist(where(x, b.t), b.at) - dist(where(y, b.t), b.at))[0];
		if (!o) {
			continue;
		}
		arms.push({ o, move: "flagToss", t0: b.t - BAG_ARMS, t1: b.t + BAG_ARMS });
		const t0 = b.t + BAG_ARMS / 2;
		const from = where(o, t0);
		bags.push({
			t0,
			t1: t0 + Math.min(BAG_MOST_MS, BAG_MS + dist(from, b.at) * BAG_MS_FT),
			from: { ...from, z: BAG_Z },
			to: onField(b.at),
		});
	}
	// The whistle, the play over: his hand up to it at his mouth.
	const blowT = whistle + WHISTLE_AFTER_MS;
	const blower = [...CREW]
		.filter((o) => dist(where(o, whistle), dead) <= WHISTLE_NEAR)
		.sort(
			(a, b) => dist(where(a, whistle), dead) - dist(where(b, whistle), dead),
		)
		.find(
			(o) =>
				!arms.some(
					(a) => a.o === o && a.t1 > blowT && a.t0 < blowT + WHISTLE_MS,
				),
		);
	if (blower) {
		arms.push({
			o: blower,
			move: "blowWhistle",
			t0: blowT,
			t1: blowT + WHISTLE_MS,
		});
	}
	for (const a of arms) {
		armsOver(legsOf.get(a.o)!, a.move, a.t0, a.t1, a.left);
	}
	// (The referee giving each call over his microphone as he signals it.)
	for (const c of input.calls) {
		talkOver(legsOf.get("R")!, c.t + CALL_TALK_AFTER, c.t + CALL_FOR_MS);
	}
	// (And the man telling him, telling him.)
	if (report?.near && told) {
		const o = thrower.get(told.foul)!;
		talkOver(legsOf.get(o)!, report.t0 + CALL_TALK_AFTER, report.t1);
	}

	return {
		parts: posts.map((p) => ({
			pid: officialId(p.o),
			team: input.offense,
			legs: legsOf.get(p.o)!,
		})),
		flags,
		bags,
		...(report ? { report } : {}),
	};
};

// Back inside the stadium's walls, if he would go out past an end line.
const onField = (p: Pt): Pt => ({
	x: clamp(p.x, 2, FIELD_L - 2),
	y: clamp(p.y, -OFF_FIELD, FIELD_W + OFF_FIELD),
});
