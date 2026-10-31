import { makeCourtRng } from "../courtRng.ts";
import { cheerAt } from "./crowd.ts";
import type { FieldTimeline, Play } from "./director.ts";
import type { FieldMoment } from "./evaluate.ts";
import { playAt } from "./evaluate.ts";
import { FIELD_L, MID_X, SIDE_ROOM, TUNNEL_X, type Pt } from "./geometry.ts";
import { liftOf, loopRate, type Move } from "./poses.ts";
import type { FieldPlayerState } from "./sprite.ts";

// THE CHEERLEADERS: the home team's, on its sideline across the field, back
// by the wall at either end of it - half of them either side of the way out
// of the tunnel, opening out toward the field from it, the rest in two rows
// down at the other end - each in her place all game, a pom-pom in each
// hand. Turned in to the way out as the team runs out of the tunnel, the
// poms going. Facing the
// field, hands on hips, watching while the ball is live; between plays the
// poms going, each half of them as one; at a timeout, and at the end of a
// quarter, the routine, all of them as one (the court's dance team's: see
// court3d/poses.ts); up and jumping, poms in the air, for a big play of the
// home team's.

export const cheerleaderId = (k: number) => -300 - k;

export type Cheerleader = {
	id: number;
	at: Pt;
	// The tunnel's half of them (0), or the other (1); which side of the
	// way out of it she stands (-1 nearer the end line, 1 the other way).
	group: 0 | 1;
	flank: number;
	hgt: number;
	weight: number;
	// How long after the rest she is up for a big play (ms).
	delay: number;
};

// Either side of the way out of the tunnel, from this far from the middle of
// it and this far off the wall, each further out along it, and on toward the
// field, by this much.
const FLANK = 10;
const FLANK_FROM = 9;
const FLANK_OUT = 6;
const FLANK_ON = 3;
const FLANK_N = 3;
// (Turned in to it, half way round toward the field.)
const LANE_TURN = Math.PI / 4;
// At the other end: the middle of the rows (as far along as the tunnel is
// at its end), this far off the wall; their places in them.
export const ROWS_X = FIELD_L - TUNNEL_X;
const ROWS = [
	{ off: 13, xs: [-6, 0, 6] },
	{ off: 7, xs: [-9, -3, 3, 9] },
];

export const cheerleadersFor = (gid: number): Cheerleader[] => {
	const r = makeCourtRng(`cheerleaders|${gid}`);
	const out: Cheerleader[] = [];
	const add = (at: Pt, group: 0 | 1, flank: number) => {
		out.push({
			id: cheerleaderId(out.length),
			at,
			group,
			flank,
			hgt: 63 + r() * 6,
			weight: 112 + r() * 22,
			delay: r() * 220,
		});
	};
	for (const flank of [-1, 1]) {
		for (let k = 0; k < FLANK_N; k++) {
			add(
				{
					x: TUNNEL_X + flank * (FLANK + k * FLANK_OUT),
					y: -SIDE_ROOM + FLANK_FROM + k * FLANK_ON,
				},
				0,
				flank,
			);
		}
	}
	for (const row of ROWS) {
		for (const dx of row.xs) {
			add({ x: ROWS_X + dx, y: -SIDE_ROOM + row.off }, 1, 0);
		}
	}
	return out;
};

// What she is doing, from when, and which way she faces.
type Doing = {
	move: Move;
	phase: number;
	z: number;
	yaw: number;
	from: number;
};

// Turned in to the way out from this long before the first of them is out.
const LANE_EARLY = 2500;
// Watching from this long before the snap to this long after the whistle -
// turned after the ball, as far round as this.
const LIVE_BEFORE = 1500;
const LIVE_AFTER = 1500;
const WATCH_TURN = 0.7;
// The routine: this long into a timeout (from the picture's cut to it), or
// after the whistle that ends a quarter; done this long before the next
// play.
const DANCE_AFTER = 1000;
const QUARTER_AFTER = 2500;
const DANCE_DONE = 300;
const DANCE_LEAST = 3000;
// Up for a big play as long as the crowd is this far up for it; a jump
// every this long.
const UP_LEVEL = 0.5;
const LEAP_MS = 900;
// From one thing into the next, this long.
const BLEND = 320;

const FACING_FIELD = Math.PI / 2;
const DANCE_RATE = loopRate("dance");
const POMS_RATE = loopRate("poms");

const smooth01 = (u: number) => {
	const v = Math.min(1, Math.max(0, u));
	return v * v * (3 - 2 * v);
};

// When the routine starts on this play, if it does.
const danceFrom = (tl: FieldTimeline, play: Play): number | undefined => {
	if (play.timeout) {
		return play.timeout.cut + DANCE_AFTER;
	}
	const next = tl.plays[tl.plays.indexOf(play) + 1];
	const from = play.whistle + QUARTER_AFTER;
	return next && next.quarter !== play.quarter && play.t1 - from >= DANCE_LEAST
		? from
		: undefined;
};

const doingAt = (
	c: Cheerleader,
	tl: FieldTimeline,
	t: number,
	ball: Pt | undefined,
): Doing => {
	const play = playAt(tl, t);
	const still = (move: Move, from: number, yaw = FACING_FIELD): Doing => ({
		move,
		phase: 0,
		z: 0,
		yaw,
		from,
	});
	if (!play) {
		return still("onHips", -Infinity);
	}
	const ro = play.runOut;
	const poms = (from: number, yaw = FACING_FIELD): Doing => ({
		...still("poms", from, yaw),
		phase: (t / 1000) * POMS_RATE + c.group * 0.5,
	});
	if (c.group === 0 && ro && t < ro.cut) {
		const lane = ro.out - LANE_EARLY;
		return t >= lane
			? poms(lane, c.flank < 0 ? LANE_TURN : Math.PI - LANE_TURN)
			: poms(play.t0);
	}
	const cheer = cheerAt(tl, t);
	if (cheer && cheer.side === 1 && cheer.level > UP_LEVEL) {
		const e = cheer.since - c.delay;
		if (e > 0) {
			const phase = (e / LEAP_MS) % 1;
			return {
				move: "leap",
				phase,
				z: liftOf("leap", phase),
				yaw: FACING_FIELD,
				from: t - e,
			};
		}
	}
	const dance = danceFrom(tl, play);
	if (dance !== undefined && t >= dance && t < play.t1 - DANCE_DONE) {
		return {
			...still("dance", dance),
			phase: ((t - dance) / 1000) * DANCE_RATE,
		};
	}
	if (dance !== undefined && t >= play.t1 - DANCE_DONE) {
		return poms(play.t1 - DANCE_DONE);
	}
	const live = play.snap - LIVE_BEFORE;
	if (t >= live && t < play.whistle + LIVE_AFTER) {
		let yaw = FACING_FIELD;
		if (ball) {
			let d = Math.atan2(ball.y - c.at.y, ball.x - c.at.x) - yaw;
			d = Math.atan2(Math.sin(d), Math.cos(d));
			yaw += Math.max(-WATCH_TURN, Math.min(WATCH_TURN, d));
		}
		return still("onHips", live, yaw);
	}
	return poms(t < live ? play.t0 : play.whistle + LIVE_AFTER);
};

export const cheerleadersAt = (
	squad: Cheerleader[],
	tl: FieldTimeline,
	m: FieldMoment,
): FieldPlayerState[] => {
	const ball = m.focus;
	return squad.map((c) => {
		const now = doingAt(c, tl, m.t, ball);
		const st: FieldPlayerState = {
			id: c.id,
			team: 1,
			x: c.at.x,
			y: c.at.y,
			z: now.z,
			yaw: now.yaw,
			move: now.move,
			phase: now.phase,
		};
		// (Into it from what she was doing.)
		const into = m.t - now.from;
		if (into >= BLEND) {
			return st;
		}
		const was = doingAt(c, tl, now.from - 1, ball);
		if (was.move === now.move && was.yaw === now.yaw) {
			return st;
		}
		const w = 1 - smooth01(into / BLEND);
		let d = was.yaw - now.yaw;
		d = Math.atan2(Math.sin(d), Math.cos(d));
		return {
			...st,
			z: now.z * (1 - w) + was.z * w,
			yaw: now.yaw + d * w,
			from: { move: was.move, phase: was.phase, w },
		};
	});
};

// THE PICTURE ON THEM: now and then, for the end of a timeout, cut away from
// the huddle to them at their routine - the half of them at the end it is
// nearer - and from them to the next play (see aim.ts). Where it looks, and
// how wide (the tunnel's half of them spread wider).
export type CheerShot = { cut: number; x: number; y: number; width: number };
const SHOT_SHARE = 0.5;
const SHOT_MS = 2600;
const SHOT_INTO = 1500;
const SHOT_Y = -SIDE_ROOM + 13;
const SHOT_WIDTH = [60, 46];

export const cheerShotFor = (
	cut: number,
	t1: number,
	at: Pt,
	r: () => number,
): CheerShot | undefined => {
	const from = t1 - SHOT_MS;
	if (r() >= SHOT_SHARE || from < cut + DANCE_AFTER + SHOT_INTO) {
		return undefined;
	}
	const group = at.x < MID_X ? 0 : 1;
	return {
		cut: from,
		x: group === 0 ? TUNNEL_X : ROWS_X,
		y: SHOT_Y,
		width: SHOT_WIDTH[group]!,
	};
};
