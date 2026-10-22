import { makeCourtRng } from "../courtRng.ts";
import type { BallSeg, Leg, Part, PlayKind } from "./director.ts";
import {
	END_ZONE,
	FIELD_L,
	FIELD_W,
	MID_Y,
	POST_W,
	dist,
	type Dir,
	type Pt,
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
// spot it.

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
	r: () => number;
};

// Just off the field: the wings on the sidelines, the deep men a step in.
const OFF_FIELD = 3;
const ON_SIDELINE = 2;
// How fast an official goes (feet a second): flat out, turned and running
// - and an easy jog, a walk.
const TOP = 21;
const TURN_AND_RUN = 14;
const JOG = 10;
const WALK = 4.6;
const ACCEL = 20;
const STEP_MS = 150;
const REACT_MS = 250;

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

const posOf = (legs: Leg[], t: number): Pt => {
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
const anyBall = (segs: BallSeg[], parts: Part[], t: number): Pt | undefined => {
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

export const stageCrew = (input: CrewInput): CrewPart[] => {
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
	const kick = kind === "fieldGoal" || kind === "extraPoint";
	// After the whistle, from where it finds him: in toward the ball - the
	// wing on its side of the field to mark it, the umpire to spot it, the
	// rest some of the way. (A kick at the posts, everybody where he is.)
	const afterWhistle = (o: Official, from: Pt): { to: Pt; fps: number } => {
		if (kick) {
			return { to: from, fps: WALK };
		}
		if (o === "U" && !input.backToLine) {
			return { to: w({ d: deadO.d - 3, a: deadO.a - 2 }), fps: JOG };
		}
		if (o === "R") {
			return {
				to: w({ d: deadO.d - 30, a: clamp(deadO.a + 15, -60, 60) }),
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
		const keep = 24 + r() * 14;
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
	const out: CrewPart[] = [];
	for (const p of posts) {
		const legs: Leg[] = [];
		const start = p.at;
		const startO = of(start);
		const facing = Math.atan2(ballY - start.y, losX - start.x);
		// Standing waiting for the offense to come up, and set for the snap.
		legs.push(
			{
				t0,
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
			let target: Pt;
			let fps = TOP;
			if (playing) {
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
			// Toward it, speeding up and slowing down as a man does.
			const dx = target.x - at.x;
			const dy = target.y - at.y;
			const far = Math.hypot(dx, dy);
			const want = Math.min(fps, far / 0.4);
			const ux = far > 0.01 ? dx / far : 0;
			const uy = far > 0.01 ? dy / far : 0;
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
			if (speed < 0.8) {
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
		out.push({ pid: officialId(p.o), team: input.offense, legs });
	}
	return out;
};

// Back inside the stadium's walls, if he would go out past an end line.
const onField = (p: Pt): Pt => ({
	x: clamp(p.x, 2, FIELD_L - 2),
	y: clamp(p.y, -OFF_FIELD, FIELD_W + OFF_FIELD),
});
