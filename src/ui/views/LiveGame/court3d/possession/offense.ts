// THE OFFENSE OFF THE BALL (see README.md, step 6).
//
// Each of the four without the ball, a step at a time, between the things
// the play pins him to (his catch, his screen, his part in its actions):
// he sees the floor a beat late (his own reaction time), has the spot the
// set gives him, and works it the way a man off the ball does - never stood
// on it like a cone, but walking it, a few steps along the line and back,
// toward where the man with the ball can see him and away from teammates
// crowding him, out of the lane before the count of three. Real men out on
// the floor while nothing much happens are stood still under a third of
// the time and walking about half (tools/court3d-tape/nba).
//
// Nothing here knows about the plan or the timeline: the stepper says when
// he is his own (loop.ts), hands over to his acts, and keeps the time.

import { type Pt, type Side } from "../geometry.ts";
import type { AnimName } from "../poses.ts";
import { type CourtRatings, levelOf } from "../ratings.ts";
import type { Body } from "./body.ts";
import type { Seen } from "./defense.ts";
import { type Goal, type Mover } from "./movement.ts";

// What an attacker is like off the ball, from his ratings.
export type Attacker = {
	body: Body;
	// How long (ms) before he answers what he sees.
	react: number;
	// How well he reads the floor (0 to 1).
	iq: number;
	// From three (0 to 1): a shooter stays out behind the line.
	shoots: number;
	// Plays inside: spaces round the rim, not out on the arc.
	big: boolean;
	// With the ball (see handler.ts): his handle, and how quick he is with
	// it (0 to 1).
	handle: number;
	quick: number;
};

// (Nothing to go by: the man in the middle.)
const MID = 0.5;

// His reaction (ms): this for the sharpest, that for the slowest - with a
// hair of his own either way.
const REACT_SHARP = 150;
const REACT_SLOW = 320;
const REACT_OWN = 25;

// A pid's own draw for `salt`, 0 to 1 (the same everywhere).
export const own01 = (pid: number, salt: number): number => {
	let h = (pid * 2654435761 + salt * 40503) >>> 0;
	h = Math.imul(h ^ (h >>> 16), 2246822507) >>> 0;
	h = Math.imul(h ^ (h >>> 13), 3266489909) >>> 0;
	return ((h ^ (h >>> 16)) >>> 0) / 4294967296;
};

export const attackerOf = (
	pid: number,
	r: CourtRatings | undefined,
	body: Body,
	big: boolean,
): Attacker => {
	const iq = levelOf(r, ["oiq"]) ?? MID;
	return {
		body,
		react:
			REACT_SLOW -
			(REACT_SLOW - REACT_SHARP) * iq +
			REACT_OWN * (2 * own01(pid, 11) - 1),
		iq,
		shoots: levelOf(r, ["tp"]) ?? MID,
		big,
		handle: levelOf(r, ["drb"]) ?? MID,
		quick: levelOf(r, ["spd"]) ?? MID,
	};
};

// What he keeps between picks of where next on his spot.
export type Mind = {
	// Where he is walking to, and from when he picks again - and where he
	// was when he picked it.
	at?: Pt;
	from?: { at: Pt; here: Pt };
	until: number;
	// The set's spot that was picked round, and who had the ball.
	set?: Pt;
	holder?: number;
	// Picks so far (his own draws), and since when he has been in the lane
	// (ms; NaN out of it).
	n: number;
	lane: number;
	// What he is about: working his spot, answering a drive, cutting, or
	// on his way somewhere of his own (clearing out after a cut, relocating
	// after his pass).
	intent: "spot" | "drive" | "cut" | "move";
	// Whether he has read the drive going on; the pass of his he has
	// answered (when it left his hands); when he last read his man (ms).
	drove: boolean;
	passKey?: number;
	read: number;
	// Jogging to where he is going (not walking it) - how fast, and whether
	// he stands a moment there once he gets there.
	jog: boolean;
	pace?: number;
	hold?: boolean;
	// When he picked where he is going (ms).
	picked?: number;
};
export const mindOf = (): Mind => ({
	until: -Infinity,
	n: 0,
	lane: Number.NaN,
	intent: "spot",
	drove: false,
	read: -Infinity,
	jog: false,
});

export type Floor = {
	t: number;
	// Where the set has him now.
	set: Pt;
	rim: Pt;
	team: Side;
	// Where the ball is, and the man with it, as he sees them.
	ball?: Pt;
	holder?: Seen & { pid: number };
	// His four teammates (the man with the ball too), as he sees them.
	mates: readonly Pt[];
	// The man guarding him, as he sees him.
	man?: Pt;
	// The man with the ball on his way to the rim, as he sees him.
	drive?: Seen;
	// When the ball last left his own hands (ms), if only a moment ago.
	passKey?: number;
	// Handing over to the play by `until` (ms from now): where it has him
	// then, all of the way by then - setting off for it `lead` (ms) before
	// (see defense.ts Guarding).
	handTo?: { at: Pt; v: Pt; until: number; lead: number; commit?: boolean };
	// His next job (something of his that pins him, or the play taking him
	// on again): where, and how soon (ms).
	next?: { at: Pt; in: number };
};

// THE FLOOR, from his rim: how deep off the baseline and how far across
// from the middle (feet).
const RIM_INSET = 5.25;
const depthOf = (p: Pt, rim: Pt): number => Math.abs(p.x - rim.x) + RIM_INSET;
const acrossOf = (p: Pt, rim: Pt): number => Math.abs(p.y - rim.y);
// How far behind the three-point line (feet): out of the corners, round
// the arc; in them, the straight line along the sideline.
const CORNER_DEEP = 14;
// (No nearer the baseline in the corner than this, feet.)
const CORNER_LEAST = 2.5;
export const behindLine = (p: Pt, rim: Pt): number =>
	depthOf(p, rim) < CORNER_DEEP
		? acrossOf(p, rim) - 22
		: Math.hypot(p.x - rim.x, p.y - rim.y) - 23.75;
// The lane: 16 ft wide, 19 ft deep.
const inLane = (p: Pt, rim: Pt): boolean =>
	depthOf(p, rim) < 19 && acrossOf(p, rim) < 8;
// On the floor, a step in from the lines.
const onFloor = (p: Pt): Pt => ({
	x: Math.min(93, Math.max(1, p.x)),
	y: Math.min(48.4, Math.max(1.6, p.y)),
});

// WORKING HIS SPOT. Out on the arc, a step or a few along it either way -
// or on round it to the next spot - a share of a step out, or in off it;
// in from it, a few feet any way; in close, a step or two. Feet.
const ARC_STEPS = [3, 5.5, 8.5, 12];
const ARC_OUT = [0, 1.5, -3];
const MID_STEPS = [3, 6];
const IN_STEPS = [2, 4];
// A shooter stays behind the line with the ball this near him (feet);
// farther from it, he may sink in off it, and come back out as it comes
// round to him.
const BALL_NEAR = 30;
// Out on the arc from this far off the rim (feet); in close within this.
const OUT_FROM = 20;
const IN_WITHIN = 10;
// A shooter out there stays this far behind the line at least (feet).
const BEHIND = 0.75;
// What he weighs: room from his nearest teammate, up to this much (feet),
// and none at all nearer than this (on the arc; inside, that);
const ROOM_FULL = 15;
const ROOM_LEAST = 9;
const ROOM_LEAST_IN = 5;
// staying near the set's spot (a foot off it costs this much room);
const HOME = 0.35;
// out of the way the ball is going to the rim (feet off its line, within
// this far of the rim);
const LANE_OFF = 5;
const DRIVE_FROM = 26;
// out of his man's way to the ball (feet off the line to it);
const SEEN_OFF = 2.5;
// not by way of a teammate (feet off him, going by);
const PASS_BY = 6;
// and a draw of his own, so he is not on the same few spots (feet).
const OWN = 3;
// Not where he is going already: somewhere at least this far (feet).
const FRESH = 1.5;
// Not straight back the way he just came, working his spot: going back
// along it costs this much room (feet), going round less, across it none
// (a man stepped along the arc steps on, or back up it a while later - not
// to and fro: real men off the ball go back through a stop a third as
// often as ours did, tools/court3d-tape/nba).
const BACK = 5;
// Set there a moment once he gets there (ms), and at walking pace up to
// this far (feet), jogging past it (feet a second) - and this often
// jogging it anyway, past a few steps (real: on the arc with nothing
// going on, a man walks 45% of the time and jogs 25%).
const HOLD: [number, number] = [200, 1100];
// (Only this share of the spots he gets to: at the rest he goes on into
// the next, still going as he gets there - this share of his pace. Real
// men off the ball in the half court stand 17% of the time; ours, stood on
// every spot they got to, 20%.)
const HOLD_SHARE = 0.15;
const SPOT_FLOW = 0.6;
// (Going on into the next once this near where he was going (feet), and no
// sooner than this after picking it (ms); a step or two to a spot no
// farther than this (feet) he stands on - not dithering round his spot.
// Not in the lane: out of it first.)
const SPOT_THERE = 2;
const ONWARD_AFTER = 800;
const SPOT_SETTLE = 2;
// (Walking it, as fast as this at the least - a drift, not a stride.)
const DRIFT = 4;
// Square to the ball and stepping off away from it (its cosine under this),
// no quicker than this (feet a second): a backpedal, not running backwards.
const AWAY = -0.3;
const BACKPEDAL = 3.5;
const WALK = 5;
const WALK_FAR = 7;
const JOG = 10;
const JOG_ANYWAY = 0.35;
// Farther than this (feet), he walks it facing the way he goes - nearer,
// he steps it square to the ball.
const STEP_FAR = 4;
// Out of the lane before the count of three: gone by this long in it (ms).
const LANE_MOST = 2000;
// Back to the set's spot if it moves this far (feet).
const SET_MOVED = 3;
// (Facing the way he goes once going this fast, feet a second; otherwise
// the ball.)
const FACE_GOING = 7.5;

const smooth01 = (u: number) => {
	const x = u < 0 ? 0 : u > 1 ? 1 : u;
	return x * x * (3 - 2 * x);
};

// How far a point is off the segment from a to b, and how far along it
// (0 to 1).
const offLine = (p: Pt, a: Pt, b: Pt): { off: number; u: number } => {
	const vx = b.x - a.x;
	const vy = b.y - a.y;
	const L = vx * vx + vy * vy;
	const u =
		L > 1e-9
			? Math.max(0, Math.min(1, ((p.x - a.x) * vx + (p.y - a.y) * vy) / L))
			: 0;
	return { off: Math.hypot(p.x - a.x - vx * u, p.y - a.y - vy * u), u };
};

// The places on his spot he might go to next.
const around = (f: Floor, a: Attacker): Pt[] => {
	const { set, rim } = f;
	const r = Math.hypot(set.x - rim.x, set.y - rim.y);
	const out: Pt[] = [set];
	if (r >= OUT_FROM) {
		// Along the arc, and a share of a step out.
		const th = Math.atan2(set.y - rim.y, set.x - rim.x);
		const corner = depthOf(set, rim) < CORNER_DEEP;
		for (const s of ARC_STEPS) {
			for (const w of [1, -1]) {
				for (const dr of ARC_OUT) {
					let Q: Pt;
					if (corner) {
						// (In the corner, up and down the sideline - not out along
						// the baseline.)
						const sx = Math.sign(set.x - rim.x) || 1;
						Q = {
							x: set.x + sx * w * s,
							y: set.y + Math.sign(set.y - rim.y) * dr,
						};
						if (depthOf(Q, rim) < CORNER_LEAST) {
							continue;
						}
					} else {
						const R = r + dr;
						const to = th + (w * s) / R;
						Q = { x: rim.x + Math.cos(to) * R, y: rim.y + Math.sin(to) * R };
					}
					out.push(onFloor(Q));
				}
			}
		}
	} else {
		const steps = r < IN_WITHIN ? IN_STEPS : MID_STEPS;
		for (const s of steps) {
			for (let k = 0; k < 8; k++) {
				const th = (k * Math.PI) / 4;
				out.push(
					onFloor({ x: set.x + Math.cos(th) * s, y: set.y + Math.sin(th) * s }),
				);
			}
		}
	}
	// Set in the lane, the nearest ways out of it too.
	if (inLane(set, rim)) {
		const out1 = rim.x < 47 ? 1 : -1;
		out.push(
			onFloor({ x: set.x, y: rim.y + 9 }),
			onFloor({ x: set.x, y: rim.y - 9 }),
			onFloor({ x: rim.x + out1 * (19.5 - RIM_INSET), y: set.y }),
		);
	}
	// A shooter set out behind the line stays behind it - with the ball
	// near him.
	if (behindLine(set, rim) > -0.5 && a.shoots >= 0.35) {
		const B = f.ball;
		return out.map((Q) => {
			const b = behindLine(Q, rim);
			if (b >= BEHIND || (B && Math.hypot(Q.x - B.x, Q.y - B.y) > BALL_NEAR)) {
				return Q;
			}
			if (depthOf(Q, rim) < CORNER_DEEP) {
				return onFloor({
					x: Q.x,
					y: rim.y + Math.sign(Q.y - rim.y || 1) * (22 + BEHIND),
				});
			}
			const R = Math.hypot(Q.x - rim.x, Q.y - rim.y) || 1;
			const k = (23.75 + BEHIND) / R;
			return onFloor({
				x: rim.x + (Q.x - rim.x) * k,
				y: rim.y + (Q.y - rim.y) * k,
			});
		});
	}
	return out;
};

// THE REST OF THE FLOOR: spots round his rim, as across (+ to one side)
// and deep (off the rim, along the floor), feet (see playbook.ts SPOTS).
const FLOOR_SPOTS = {
	corner: [23.2, -2],
	wing: [21.8, 13.2],
	slot: [13, 22],
	top: [0, 24.5],
	shortCorner: [15, 0],
	dunker: [8, -2.5],
	elbow: [8, 13.75],
	// In behind a drive: off the block, a step up the lane line.
	behind: [9.5, 5],
} as const;
const floorSpot = (
	rim: Pt,
	name: keyof typeof FLOOR_SPOTS,
	side: 1 | -1,
): Pt => {
	const [across, deep] = FLOOR_SPOTS[name];
	// (Out from the rim along the floor: toward half court.)
	const out = rim.x < 47 ? 1 : -1;
	return onFloor({ x: rim.x + out * deep, y: rim.y + side * across });
};
// Which side of the floor a point is on (from the rim, across).
const sideOf = (p: Pt, rim: Pt): 1 | -1 => (p.y >= rim.y ? 1 : -1);

// Where next, of `places`: the most room from his teammates, not in the
// way the ball is going to the rim, where the ball can find him - and,
// working his spot, near the set's spot (`home` per foot off it) with a
// draw of his own.
const best = (
	m: Mover,
	f: Floor,
	mind: Mind,
	pid: number,
	places: readonly Pt[],
	home: number,
	own: number,
	back = 0,
): Pt => {
	const here = { x: m.x, y: m.y };
	// (The way he came to where he was going, if he picked it here - not
	// on his way out of the lane: that is where he is going.)
	const came =
		back > 0 && Number.isNaN(mind.lane) && mind.at && mind.from?.at === mind.at
			? (() => {
					const dx = mind.at.x - mind.from.here.x;
					const dy = mind.at.y - mind.from.here.y;
					const d = Math.hypot(dx, dy);
					return d > FRESH ? { x: dx / d, y: dy / d } : undefined;
				})()
			: undefined;
	const laneLong =
		!Number.isNaN(mind.lane) && f.t - mind.lane > LANE_MOST * 0.6;
	const H = f.drive ?? f.holder;
	const driveLine =
		H && Math.hypot(H.x - f.rim.x, H.y - f.rim.y) < DRIVE_FROM ? H : undefined;
	let out = f.set;
	let score = -Infinity;
	places.forEach((Q, k) => {
		if (mind.at && Math.hypot(Q.x - mind.at.x, Q.y - mind.at.y) < FRESH) {
			return;
		}
		let room = ROOM_FULL;
		for (const P of f.mates) {
			room = Math.min(room, Math.hypot(Q.x - P.x, Q.y - P.y));
		}
		const least =
			Math.hypot(Q.x - f.rim.x, Q.y - f.rim.y) < IN_WITHIN
				? ROOM_LEAST_IN
				: ROOM_LEAST;
		let s = room - (room < least ? 2 * (least - room) : 0);
		s -= home * Math.hypot(Q.x - f.set.x, Q.y - f.set.y);
		if (laneLong && inLane(Q, f.rim)) {
			s -= 30;
		}
		if (driveLine) {
			const { off } = offLine(Q, driveLine, f.rim);
			if (off < LANE_OFF) {
				s -= 2 * (LANE_OFF - off);
			}
		}
		if (f.man && f.ball) {
			const { off, u } = offLine(f.man, f.ball, Q);
			if (u > 0.15 && u < 0.95 && off < SEEN_OFF) {
				s -= 2 * (SEEN_OFF - off);
			}
		}
		// (Not by way of a teammate's spot.)
		for (const P of f.mates) {
			const by = offLine(P, here, Q);
			if (by.u > 0.05 && by.u < 0.95 && by.off < PASS_BY) {
				s -= 2 * (PASS_BY - by.off);
			}
		}
		s += own * own01(pid, mind.n * 31 + k);
		// (Back toward a job of his coming is on his way, not to and fro.)
		const toJob =
			f.next !== undefined &&
			Math.hypot(Q.x - f.next.at.x, Q.y - f.next.at.y) <
				Math.hypot(here.x - f.next.at.x, here.y - f.next.at.y) - FRESH;
		if (came && !toJob) {
			const dx = Q.x - here.x;
			const dy = Q.y - here.y;
			const d = Math.hypot(dx, dy);
			if (d > 1e-6) {
				s -= back * Math.max(0, -(dx * came.x + dy * came.y) / d);
			}
		}
		// (Somewhere to go: not where he stands.)
		if (Math.hypot(Q.x - here.x, Q.y - here.y) < FRESH) {
			s -= 1;
		}
		if (s > score) {
			score = s;
			out = Q;
		}
	});
	return out;
};

// The man with the ball going at the rim: from no farther out than this
// (feet), at least this fast at it (feet a second).
const DRIVE_SEEN_FAR = 28;
const DRIVE_SEEN_PACE = 7;
export const goingAtRim = (h: Seen, rim: Pt): boolean => {
	const dx = rim.x - h.x;
	const dy = rim.y - h.y;
	const d = Math.hypot(dx, dy);
	return (
		d <= DRIVE_SEEN_FAR &&
		d >= 3 &&
		(h.vx * dx + h.vy * dy) / d >= DRIVE_SEEN_PACE
	);
};

// HIS NEXT JOB (a catch, a screen - or the play taking him on again):
// nothing of his own that takes him so far off that he could not get there
// from it at a run (feet a second) with this long (ms) to spare.
const JOB_PACE = 12;
const JOB_SPARE = 1200;
const fits = (f: Floor, here: Pt, via: Pt): boolean =>
	!f.next ||
	(1000 *
		(Math.hypot(via.x - here.x, via.y - here.y) +
			Math.hypot(f.next.at.x - via.x, f.next.at.y - via.y))) /
		JOB_PACE +
		JOB_SPARE <=
		f.next.in;

// READING THE FLOOR. Each a beat after he sees it.
//
// A drive (real: of the four, 2.3 move 3 ft or more in its first second;
// out in the corners men mostly stay, on the wings and at the top they
// sink in or cut in behind it, a man in close slides along the baseline or
// steps out of its way - tools/court3d-tape/nba):
// - in its way, he gets out of it: a big to the dunker spot on the far
//   side, anybody else to the short corner or the wing over there;
// - in close, along the baseline to the dunker spot away from it;
// - in the corner, he stays - lifting up to the wing if it comes down his
//   baseline at him;
// - on the wing or at the top, with it gone past him on his side, he cuts
//   in behind it (the better he reads it, the more often) or drifts on
//   round the arc away from it; on the far side, he sinks in a few steps,
//   or lifts a step.
const DRIVE_LANE = 6;
const BASELINE_DRIVE = (40 * Math.PI) / 180;
const CUT_BEHIND = [0.3, 0.6] as const;
const SINK = 0.35;
const SINK_FT = 5;
const DRIFT_FT = [5, 8] as const;
// How long he holds where the drive took him after it is over (ms).
const DRIVE_HOLD = 700;
const onDrive = (m: Mover, f: Floor, a: Attacker, mind: Mind, pid: number) => {
	const H = f.drive!;
	const rim = f.rim;
	const here = { x: m.x, y: m.y };
	const r = Math.hypot(here.x - rim.x, here.y - rim.y);
	const hr = Math.hypot(H.x - rim.x, H.y - rim.y);
	const dSide = sideOf(H, rim);
	const mine = sideOf(here, rim);
	const draw = own01(pid, mind.n * 13 + 3);
	const { off, u } = offLine(here, H, rim);
	let places: Pt[];
	if (off < DRIVE_LANE && u > 0.05 && r < hr + 3) {
		// (Out of it on the side of it he is on.)
		const cross =
			(rim.x - H.x) * (here.y - H.y) - (rim.y - H.y) * (here.x - H.x);
		const out = (
			Math.abs(cross) > 1e-6
				? Math.sign(cross) * Math.sign(rim.x - H.x || 1)
				: mine
		) as 1 | -1;
		places = a.big
			? [floorSpot(rim, "dunker", out), floorSpot(rim, "shortCorner", out)]
			: [
					floorSpot(rim, "shortCorner", out),
					floorSpot(rim, "wing", out),
					floorSpot(rim, "corner", out),
				];
	} else if (r < IN_WITHIN) {
		places = [floorSpot(rim, "dunker", -dSide as 1 | -1), here];
	} else if (depthOf(here, rim) < CORNER_DEEP && r >= OUT_FROM) {
		// (Coming in from his side of the floor - off the wing, down the
		// baseline: at him.)
		const baseline =
			mine === dSide &&
			Math.atan2(acrossOf(H, rim), Math.abs(H.x - rim.x)) >= BASELINE_DRIVE;
		places = baseline ? [floorSpot(rim, "wing", mine)] : [here];
	} else if (r >= OUT_FROM - 2) {
		const past = hr < r - 4;
		const same = mine === dSide || acrossOf(H, rim) < 4;
		const read = CUT_BEHIND[0] + (CUT_BEHIND[1] - CUT_BEHIND[0]) * a.iq;
		if (
			same &&
			past &&
			draw < read &&
			fits(f, here, floorSpot(rim, "behind", mine))
		) {
			places = [floorSpot(rim, "behind", mine)];
		} else if (same && past) {
			// (On round the arc away from it: toward the corner if it went in
			// off the top, up toward the top if it went in off the wing.)
			const th = Math.atan2(here.y - rim.y, here.x - rim.x);
			const hth = Math.atan2(H.y - rim.y, H.x - rim.x);
			const way = Math.sign(th - hth) || mine;
			const ft =
				DRIFT_FT[0] + (DRIFT_FT[1] - DRIFT_FT[0]) * own01(pid, mind.n + 7);
			const to = th + (way * ft) / r;
			places = [
				onFloor({
					x: rim.x + Math.cos(to) * r,
					y: rim.y + Math.sin(to) * r,
				}),
			];
		} else if (draw < SINK) {
			const k = SINK_FT / Math.max(1, r);
			places = [
				onFloor({
					x: here.x + (rim.x - here.x) * k,
					y: here.y + (rim.y - here.y) * k,
				}),
			];
		} else {
			places = [here];
		}
	} else {
		// In from the arc: out to it if he shoots it, in to the dunker spot
		// away from it if not.
		places =
			a.shoots >= 0.35 && !a.big
				? [
						floorSpot(rim, "wing", mine),
						floorSpot(rim, "corner", mine),
						floorSpot(rim, "slot", mine),
					]
				: [floorSpot(rim, "dunker", -dSide as 1 | -1)];
	}
	return best(m, f, mind, pid, places, 0, 0.5);
};

// HIS PASS, AND ON (real: two seconds after giving it up, a man has
// relocated 60% of the time, cut to the rim 30%, stayed put 10%): he cuts
// to the rim - the more often the better he reads it, and when his man
// turns to watch the ball - with the lane open for it; or stays; or
// relocates to an open spot a good few feet away.
const CUT_PASS = 0.3;
const CUT_PASS_IQ = 0.2;
const CUT_TURNED = 0.2;
const STAY_PASS = [0.06, 0.16] as const;
const RELOCATE_FT = [6, 18] as const;
// One of his in the paint already: no cut.
const LANE_BUSY = 1;
// His man turned away from him: the ball and he this far apart round his
// man (radians) - and this far off him (feet).
const BLIND = 2.3;
const BLIND_OFF = 8;
const laneOpen = (f: Floor): boolean =>
	f.mates.filter((P) => inLane(P, f.rim) && depthOf(P, f.rim) < 14).length <
	LANE_BUSY;
const turned = (f: Floor, here: Pt): boolean => {
	const M = f.man;
	const B = f.ball;
	if (!M || !B) {
		return false;
	}
	let d =
		Math.atan2(B.y - M.y, B.x - M.x) - Math.atan2(here.y - M.y, here.x - M.x);
	d = Math.abs(Math.atan2(Math.sin(d), Math.cos(d)));
	return d > BLIND && Math.hypot(here.x - M.x, here.y - M.y) > BLIND_OFF;
};
const relocate = (m: Mover, f: Floor, a: Attacker, mind: Mind, pid: number) => {
	const here = { x: m.x, y: m.y };
	const rim = f.rim;
	const names = a.big
		? (["dunker", "shortCorner", "elbow"] as const)
		: (["corner", "wing", "slot", "top"] as const);
	const places: Pt[] = [];
	for (const name of names) {
		for (const side of [1, -1] as const) {
			const Q = floorSpot(rim, name, side);
			const d = Math.hypot(Q.x - here.x, Q.y - here.y);
			if (d >= RELOCATE_FT[0] && d <= RELOCATE_FT[1] && fits(f, here, Q)) {
				places.push(Q);
			}
		}
	}
	return places.length > 0
		? best(m, f, mind, pid, places, HOME / 3, OWN)
		: undefined;
};

// A CUT: hard to the rim from his side, and on through - out to the dunker
// spot (a big, or a man who can't shoot it) or the corner away from the
// ball, unless the ball finds him on the way.
const CUT_AT = 4.5;
const CUT_MOST = 1800;
const cutTo = (here: Pt, rim: Pt): Pt =>
	onFloor({
		x: rim.x + (rim.x < 47 ? 1 : -1) * 2,
		y: rim.y + sideOf(here, rim) * CUT_AT * 0.6,
	});
const cutFor = (m: Mover, f: Floor, mind: Mind) => {
	mind.intent = "cut";
	mind.at = cutTo({ x: m.x, y: m.y }, f.rim);
	mind.until = f.t + CUT_MOST;
	mind.n += 1;
};
// Whether he can cut from here and still make his next job.
const cutFits = (m: Mover, f: Floor): boolean => {
	const here = { x: m.x, y: m.y };
	return fits(f, here, cutTo(here, f.rim));
};
const clearOut = (m: Mover, f: Floor, a: Attacker, mind: Mind, pid: number) => {
	const rim = f.rim;
	const away = f.ball ? (-sideOf(f.ball, rim) as 1 | -1) : sideOf(m, rim);
	const places =
		a.big || a.shoots < 0.35
			? [
					floorSpot(rim, "dunker", away),
					floorSpot(rim, "dunker", -away as 1 | -1),
				]
			: [
					floorSpot(rim, "corner", away),
					floorSpot(rim, "wing", away),
					floorSpot(rim, "corner", -away as 1 | -1),
				];
	const here = { x: m.x, y: m.y };
	const ok = places.filter((Q) => fits(f, here, Q));
	if (ok.length === 0) {
		// (Somewhere of his to be soon: back to working his spot.)
		mind.intent = "spot";
		mind.at = undefined;
		return;
	}
	const Q = best(m, f, mind, pid, ok, 0, 1);
	mind.intent = "move";
	mind.at = Q;
	mind.from = { at: Q, here: { x: m.x, y: m.y } };
	mind.until =
		f.t + (1000 * Math.hypot(Q.x - m.x, Q.y - m.y)) / JOG + 600 + HOLD[1];
	mind.n += 1;
};

// READING HIS MAN, every so often (ms): turned away from him to watch the
// ball, or up in the way of the pass to him close in - he goes behind him
// to the rim, a share of the time each second it lasts, the more the
// better he reads it.
const READ_EVERY = 400;
const CUT_BLIND = 0.06;
const CUT_DENY = 0.25;
const DENY_OFF = 3;
const DENY_NEAR = 7;
const DENY_BALL = 26;
const denied = (f: Floor, here: Pt): boolean => {
	const M = f.man;
	const B = f.ball;
	if (!M || !B) {
		return false;
	}
	const { off, u } = offLine(M, B, here);
	return (
		off < DENY_OFF &&
		u > 0.55 &&
		Math.hypot(M.x - here.x, M.y - here.y) < DENY_NEAR &&
		Math.hypot(B.x - here.x, B.y - here.y) < DENY_BALL &&
		Math.hypot(here.x - f.rim.x, here.y - f.rim.y) >= OUT_FROM - 4
	);
};

// His goal this step, and how he goes about it. (Keeps `mind` up to date.)
export const decide = (
	m: Mover,
	f: Floor,
	a: Attacker,
	mind: Mind,
	pid: number,
): { goal: Goal; anim: AnimName } => {
	const here = { x: m.x, y: m.y };
	if (inLane(here, f.rim)) {
		if (Number.isNaN(mind.lane)) {
			mind.lane = f.t;
		}
	} else {
		mind.lane = Number.NaN;
	}
	const soon = f.handTo !== undefined;
	// The ball going to the rim.
	if (f.drive && !mind.drove && !soon) {
		mind.drove = true;
		if (mind.intent !== "cut") {
			const Q = onDrive(m, f, a, mind, pid);
			mind.intent = "drive";
			mind.at = Q;
			mind.until = Infinity;
			mind.n += 1;
		}
	} else if (!f.drive && mind.drove) {
		mind.drove = false;
		if (mind.intent === "drive") {
			mind.until = f.t + DRIVE_HOLD;
		}
	}
	// Just rid of it himself - and it caught.
	if (
		f.passKey !== undefined &&
		f.passKey !== mind.passKey &&
		f.holder !== undefined &&
		!soon &&
		!f.drive
	) {
		mind.passKey = f.passKey;
		const draw = own01(pid, Math.round(f.passKey));
		const cut =
			laneOpen(f) && cutFits(m, f)
				? CUT_PASS + CUT_PASS_IQ * a.iq + (turned(f, here) ? CUT_TURNED : 0)
				: 0;
		const stay = STAY_PASS[1] - (STAY_PASS[1] - STAY_PASS[0]) * a.iq;
		if (draw < cut) {
			cutFor(m, f, mind);
		} else if (draw >= cut + stay) {
			const Q = relocate(m, f, a, mind, pid);
			if (Q) {
				mind.intent = "move";
				mind.at = Q;
				mind.from = { at: Q, here: { x: m.x, y: m.y } };
				mind.until =
					f.t + (1000 * Math.hypot(Q.x - m.x, Q.y - m.y)) / JOG + 400 + HOLD[0];
				mind.set = { ...f.set };
				mind.holder = f.holder?.pid;
				mind.n += 1;
			}
		}
	}
	// His man.
	if (
		mind.intent === "spot" &&
		!soon &&
		!f.drive &&
		f.t - mind.read >= READ_EVERY
	) {
		mind.read = f.t;
		const blind = turned(f, here);
		const deny = !blind && denied(f, here);
		if ((blind || deny) && laneOpen(f) && cutFits(m, f)) {
			const per = (blind ? CUT_BLIND : CUT_DENY) * (0.5 + a.iq);
			if (
				own01(pid, Math.round(f.t / READ_EVERY)) <
				(per * READ_EVERY) / 1000
			) {
				cutFor(m, f, mind);
			}
		}
	}
	// Off somewhere of his own he could no longer get back from in time for
	// his next job: back to working his spot.
	if (
		(mind.intent === "cut" || mind.intent === "move") &&
		!fits(f, here, mind.at!)
	) {
		mind.intent = "spot";
		mind.at = undefined;
	}
	// On through a cut, and out of there.
	if (
		mind.intent === "cut" &&
		(f.t >= mind.until || Math.hypot(mind.at!.x - m.x, mind.at!.y - m.y) < 2.5)
	) {
		clearOut(m, f, a, mind, pid);
	}
	if (
		(mind.intent === "move" || mind.intent === "drive") &&
		f.t >= mind.until
	) {
		mind.intent = "spot";
		mind.set = { ...f.set };
		mind.holder = f.holder?.pid;
	}
	// Working his spot.
	if (mind.intent === "spot") {
		const moved =
			!mind.set ||
			Math.hypot(f.set.x - mind.set.x, f.set.y - mind.set.y) > SET_MOVED;
		const laneDue =
			!Number.isNaN(mind.lane) &&
			f.t - mind.lane > LANE_MOST * 0.6 &&
			mind.at !== undefined &&
			inLane(mind.at, f.rim);
		const onward =
			mind.at !== undefined &&
			!mind.hold &&
			Number.isNaN(mind.lane) &&
			f.t - (mind.picked ?? -Infinity) >= ONWARD_AFTER &&
			Math.hypot(mind.at.x - m.x, mind.at.y - m.y) < SPOT_THERE;
		if (
			!mind.at ||
			f.t >= mind.until ||
			moved ||
			laneDue ||
			(f.holder !== undefined && mind.holder !== f.holder.pid) ||
			onward
		) {
			const Q = best(m, f, mind, pid, around(f, a), HOME, OWN, BACK);
			const far = Math.hypot(Q.x - m.x, Q.y - m.y);
			mind.jog =
				far > WALK_FAR ||
				(far > STEP_FAR && own01(pid, mind.n * 29 + 11) < JOG_ANYWAY);
			// (Out of the lane at a walk.)
			const pace = mind.jog
				? JOG
				: !Number.isNaN(mind.lane)
					? WALK
					: DRIFT + (WALK - DRIFT) * own01(pid, mind.n * 23 + 3);
			mind.hold = far < SPOT_SETTLE || own01(pid, mind.n * 31 + 7) < HOLD_SHARE;
			const hold = mind.hold
				? HOLD[0] + (HOLD[1] - HOLD[0]) * own01(pid, mind.n * 17 + 5)
				: 0;
			mind.at = Q;
			mind.pace = pace;
			mind.picked = f.t;
			mind.from = { at: Q, here: { x: m.x, y: m.y } };
			mind.until = f.t + (1000 * far) / pace + 400 + hold;
			mind.set = { ...f.set };
			mind.holder = f.holder?.pid;
			mind.n += 1;
		}
	}
	let to = mind.at!;
	let toV: Pt = { x: 0, y: 0 };
	const far = Math.hypot(to.x - m.x, to.y - m.y);
	// (Cutting, or answering a drive: quick, a man with somewhere to be.)
	const hard = mind.intent === "cut" || mind.intent === "drive";
	const jog = mind.intent === "move" || mind.jog || far > WALK_FAR;
	const walk =
		mind.intent === "spot" && mind.pace !== undefined ? mind.pace : WALK;
	let most = hard ? Infinity : jog ? JOG : walk;
	let effort = mind.intent === "cut" ? 0.85 : hard ? 0.75 : jog ? 0.6 : 0.35;
	let arrive = mind.intent === "spot" && !mind.hold ? SPOT_FLOW * most : 0;
	if (f.handTo?.commit) {
		// Something of his coming that pins him: straight there.
		to = f.handTo.at;
		toV = f.handTo.v;
		most = Infinity;
		effort = 1;
		arrive = 0;
	} else if (f.handTo) {
		const u = smooth01(1 - f.handTo.until / f.handTo.lead);
		to = {
			x: to.x + (f.handTo.at.x - to.x) * u,
			y: to.y + (f.handTo.at.y - to.y) * u,
		};
		toV = { x: f.handTo.v.x * u, y: f.handTo.v.y * u };
		if (u > 0.5) {
			most = Infinity;
			effort = Math.max(effort, 0.7);
			arrive = 0;
		}
	}
	const v = Math.hypot(m.vx, m.vy);
	const ball = f.ball;
	const going = v > FACE_GOING || most > JOG + 1;
	const walking = !going && far > STEP_FAR;
	// (Square to the ball, stepping off away from it: a backpedal, no
	// quicker than this.)
	if (ball && !going && !walking) {
		const tx = to.x - m.x;
		const ty = to.y - m.y;
		const bx = ball.x - m.x;
		const by = ball.y - m.y;
		const tl = Math.hypot(tx, ty);
		const bl = Math.hypot(bx, by);
		if (tl > 0.5 && bl > 1e-6 && (tx * bx + ty * by) / (tl * bl) < AWAY) {
			most = Math.min(most, BACKPEDAL);
			arrive = Math.min(arrive, most);
		}
	}
	const goal: Goal = {
		to,
		toV,
		effort,
		arrive,
		...(Number.isFinite(most) ? { most } : {}),
		...(ball && !going && !walking
			? { face: Math.atan2(ball.y - m.y, ball.x - m.x) }
			: {}),
	};
	const anim: AnimName = going ? "run" : walking ? "walk" : "drift";
	return { goal, anim };
};
