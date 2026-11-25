// THE DEFENSE DECIDING (see README.md, step 5).
//
// Each of the five on defense, a step at a time, guarding his man: he sees
// the floor a beat late (his own reaction time), picks the spot a defender
// would want, and goes there with his own body (movement.ts). On the ball,
// between his man and the rim at a cushion; off it, sunk toward the rim and
// over toward the ball, the more the farther his man is from it. Where he
// stands is fit to real NBA tracking (tools/court3d-tape/nba, targets.json:
// def_on_ball, def_off_ball_*), and leans on ratings the way real defenses
// do: up on a shooter, off a man who can't shoot, a step looser on a man who
// can get by him.
//
// Nothing here knows about the plan or the timeline: the stepper (stepper.ts)
// says who he guards and when, hands over to his acts, and keeps the time.

import { COURT_H, COURT_W, type Pt } from "../geometry.ts";
import type { AnimName } from "../poses.ts";
import { type CourtRatings, levelOf } from "../ratings.ts";
import { type Body, quicknessOf } from "./body.ts";
import { type Goal, type Mover, STEP_S, step } from "./movement.ts";

// Who a defender is guarding, from when to when (ms), as the planner has it.
export type Guard = { t0: number; t1: number; man: number };

// What a defender is like at it, from his ratings.
export type Defender = {
	body: Body;
	// How long (ms) before he answers what he sees.
	react: number;
	// How much of where his man is going he reads ahead (0 to 1).
	lead: number;
	// How quick he is on his feet (0 to 1).
	quick: number;
	// His quickest slide (feet a second): past it, he turns and runs.
	slide: number;
};

// What a man with the ball, or without it, makes his defender do.
export type Threat = {
	// From three, and from inside the arc.
	three: number;
	shot: number;
	// Getting by his man off the dribble.
	drive: number;
};

// (Nothing to go by: the man in the middle.)
const MID = 0.5;

// His reaction (ms): this for the sharpest, that for the slowest - with a
// hair of his own either way.
const REACT_SHARP = 150;
const REACT_SLOW = 300;
const REACT_OWN = 25;
// How far ahead he reads (share of his reaction time): this much for the
// least of them, this much more for the best.
const LEAD_LEAST = 0.3;
const LEAD_MORE = 0.5;
// His quickest slide (feet a second): the slowest man's, and how much
// quicker the quickest's.
const SLIDE_LEAST = 9.5;
const SLIDE_MORE = 2.5;

// A pid's own draw for `salt`, 0 to 1 (the same everywhere).
const own01 = (pid: number, salt: number): number => {
	let h = (pid * 2654435761 + salt * 40503) >>> 0;
	h = Math.imul(h ^ (h >>> 16), 2246822507) >>> 0;
	h = Math.imul(h ^ (h >>> 13), 3266489909) >>> 0;
	return ((h ^ (h >>> 16)) >>> 0) / 4294967296;
};

export const defenderOf = (
	pid: number,
	r: CourtRatings | undefined,
	body: Body,
): Defender => {
	const iq = levelOf(r, ["diq"]) ?? MID;
	const quick = quicknessOf(r);
	return {
		body,
		react:
			REACT_SLOW -
			(REACT_SLOW - REACT_SHARP) * iq +
			REACT_OWN * (2 * own01(pid, 7) - 1),
		lead: LEAD_LEAST + LEAD_MORE * iq,
		quick,
		slide: SLIDE_LEAST + SLIDE_MORE * quick,
	};
};

export const threatOf = (r: CourtRatings | undefined): Threat => ({
	three: levelOf(r, ["tp"]) ?? MID,
	shot: levelOf(r, ["fg"]) ?? MID,
	drive: levelOf(r, ["spd", "drb"]) ?? MID,
});

// A value along a few points (x, y), straight between them and level past
// either end.
const along = (pts: readonly (readonly [number, number])[], x: number) => {
	if (x <= pts[0]![0]) {
		return pts[0]![1];
	}
	for (let k = 1; k < pts.length; k++) {
		const [x1, y1] = pts[k]!;
		if (x <= x1) {
			const [x0, y0] = pts[k - 1]!;
			return y0 + ((y1 - y0) * (x - x0)) / (x1 - x0);
		}
	}
	return pts.at(-1)![1];
};

// ON THE BALL: how far off his man he sits (feet, middle to middle), by how
// far his man is from the rim - up close in the paint, a step and a half off
// at the arc, picking him up farther out the farther out he is. (Real
// median 5.6 ft in the half court.)
const CUSHION: readonly (readonly [number, number])[] = [
	[6, 2.7],
	[15, 3.8],
	[23, 5.2],
	[30, 6.4],
	[40, 8.5],
	[55, 11],
];
// Tighter on a shooter at the range he is at, looser on a man who can get by
// him, tighter the quicker the defender: each this much either way of the
// middle man's, at the most.
const CUSHION_SHOOTER = 0.3;
const CUSHION_DRIVER = 0.15;
const CUSHION_QUICK = 0.1;
// (His shot that counts: from three out past this far from the rim, feet -
// his jumper inside it, down to this far.)
const RANGE_3 = 21;
const RANGE_2 = 10;

const rangeThreat = (threat: Threat, out: number): number =>
	out >= RANGE_3
		? threat.three
		: out <= RANGE_2
			? MID
			: threat.shot +
				((threat.three - threat.shot) * (out - RANGE_2)) / (RANGE_3 - RANGE_2);

export const cushionOf = (out: number, threat: Threat, d: Defender): number =>
	along(CUSHION, out) *
	(1 - 2 * CUSHION_SHOOTER * (rangeThreat(threat, out) - MID)) *
	(1 + 2 * CUSHION_DRIVER * (threat.drive - MID)) *
	(1 - 2 * CUSHION_QUICK * (d.quick - MID));

// OFF THE BALL: how far from his man toward the rim (a share of the way)
// and over toward the ball's side (feet), by how far his man is from the
// ball (feet) - the medians of real NBA tracking.
const SAG: readonly (readonly [number, number])[] = [
	[11, 0.23],
	[20, 0.32],
	[30, 0.4],
	[40, 0.45],
];
const SHADE: readonly (readonly [number, number])[] = [
	[11, 0.42],
	[20, 1.12],
	[30, 1.59],
	[40, 1.94],
];
// Off a man who can't shoot, this much more of the way (a great shooter,
// this much less): a share of the middle man's.
const SAG_SHOOTER = 0.4;
// Never nearer the rim than this (feet), sunk in off his man.
const SAG_RIM = 4;

// Where the man guarding `man` wants to be: `rim` the rim they attack, `ball`
// where the ball is, `on` whether his man has it.
export const guardSpot = (
	man: Pt,
	rim: Pt,
	ball: Pt | undefined,
	on: boolean,
	threat: Threat,
	d: Defender,
): Pt => {
	const rx = rim.x - man.x;
	const ry = rim.y - man.y;
	const out = Math.hypot(rx, ry);
	if (out < 1e-6) {
		return { ...man };
	}
	const ux = rx / out;
	const uy = ry / out;
	if (on || !ball) {
		const c = Math.min(out - 1, cushionOf(out, threat, d));
		return inside({ x: man.x + ux * c, y: man.y + uy * c });
	}
	const far = Math.hypot(ball.x - man.x, ball.y - man.y);
	const sag = Math.min(
		Math.max(0, out - SAG_RIM),
		along(SAG, far) *
			(1 - 2 * SAG_SHOOTER * (rangeThreat(threat, out) - MID)) *
			out,
	);
	// (Over to the ball's side of the line from his man to the rim.)
	const px = -uy;
	const py = ux;
	const side = (ball.x - man.x) * px + (ball.y - man.y) * py >= 0 ? 1 : -1;
	const shade = along(SHADE, far) * side;
	return inside({
		x: man.x + ux * sag + px * shade,
		y: man.y + uy * sag + py * shade,
	});
};

// (Defending, a step inside the lines.)
const IN = 1;
const inside = (p: Pt): Pt => ({
	x: Math.min(COURT_W - IN, Math.max(IN, p.x)),
	y: Math.min(COURT_H - IN, Math.max(IN, p.y)),
});

// What he sees of a man: where he was his reaction time ago, carried on by
// how he was going - as far as he reads ahead.
export type Seen = { x: number; y: number; vx: number; vy: number };
export const seenOf = (s: Seen, d: Defender): Seen => {
	const k = (d.react / 1000) * d.lead;
	return { x: s.x + s.vx * k, y: s.y + s.vy * k, vx: s.vx, vy: s.vy };
};

// BEATEN: his man with the ball level with him or by him on the way to the
// rim, and going: he turns and runs to get back in front, not slides along
// behind. (Level within this much, feet; going at least this fast, feet a
// second.)
const LEVEL = 0.5;
const GOING = 6;
// Off the ball, how hard he goes about it (0 to 1); on it, all he has.
const OFF_EFFORT = 0.8;
// Within this (feet) of the man he goes out to, he breaks down: going at it
// faster than this (feet a second) from farther, a closeout.
const CLOSE_FAR = 4.5;
const CLOSE_PACE = 4;
// Moving away from the ball, this far round from it (cosine), he drops -
// a slide; across it, a shuffle.
const DROP = -0.55;
// Down in his stance only when it is on him: up on the man with the ball,
// stepping up into a drive (or one coming within this many feet of him),
// into something of his the play pins him to. Off the ball otherwise, as
// real defenders are, he is up on his feet: walking his spot, eyes on the
// ball, running when it takes running.
const ENGAGE_DRIVE = 12;
// Never nearer anybody than this (feet, middle to middle): a step aside.
const APART = 2;
// (Stepping aside, no quicker than this, feet a second.)
const ASIDE = 8;
// A man in his way, nearer than this (feet): braked for in time - to no
// slower than this (feet a second) coming up on him, a pace he is
// stepped round him at, not jolted.
const HEED = 12;
const HEED_LEAST = 4;
// (How far on, s, the way his spot is going is read from.)
const SPOT_DT = 0.1;
// Off his spot, he means to make it up over about this long (s).
const CLOSE_S = 0.9;

// HELP. The ball on its way to the rim and nobody in front of it: the man
// best placed to steps up into its way, a stride in front of the man with
// it - and the others near it show, a step toward its way in. (Real help
// comes from wherever it can get there: the big at the rim, the man sunk
// in off a poor shooter - least from the man one pass away, whose man is
// the easy pass.)
// (On his way in from within this far of the rim, feet, at this pace or
// more; or this near it, however fast.)
const DRIVE_FAR = 24;
const DRIVE_PACE = 5;
const DRIVE_NEAR = 12;
// In front of him: ahead of him on his way in by this much or more (feet)
// and within this of his line (feet), a little wider the farther ahead.
const AHEAD_LEAST = 0.5;
const LANE = 3;
const LANE_WIDEN = 0.08;
// Where the help steps up to: this far in front of him (feet), and never
// nearer the rim than this.
const HELP_AHEAD = 4.5;
const HELP_RIM = 3;
// Leaving a man within this of the ball (feet) costs as much as this much
// farther to go (feet): the pass to him is there.
const ONE_PASS = 15;
const ONE_PASS_COST = 8;
// Farther than this (feet, cost) from the spot, he can't help.
export const HELP_REACH = 20;
// Showing: a man of theirs within this of the ball (feet) goes this much
// of the way toward its way in (share), no more than this far (feet).
const SHOW_NEAR = 14;
const SHOW_SHARE = 0.35;
const SHOW_MOST = 3;

const unit = (from: Pt, to: Pt): Pt => {
	const dx = to.x - from.x;
	const dy = to.y - from.y;
	const d = Math.hypot(dx, dy) || 1;
	return { x: dx / d, y: dy / d };
};

// The man with the ball on his way to the rim.
export const driving = (h: Seen, rim: Pt): boolean => {
	const d = Math.hypot(rim.x - h.x, rim.y - h.y);
	if (d <= DRIVE_NEAR) {
		return true;
	}
	if (d > DRIVE_FAR) {
		return false;
	}
	const u = unit(h, rim);
	return h.vx * u.x + h.vy * u.y >= DRIVE_PACE;
};

// Whether a man at `p` is in his way to the rim.
export const inFront = (h: Pt, p: Pt, rim: Pt): boolean => {
	const u = unit(h, rim);
	const along = (p.x - h.x) * u.x + (p.y - h.y) * u.y;
	const off = Math.abs((p.x - h.x) * u.y - (p.y - h.y) * u.x);
	const to = Math.hypot(rim.x - h.x, rim.y - h.y);
	return (
		along > AHEAD_LEAST && along < to + 2 && off < LANE + LANE_WIDEN * along
	);
};

export const helpSpotOf = (h: Pt, rim: Pt): Pt => {
	const u = unit(h, rim);
	const to = Math.hypot(rim.x - h.x, rim.y - h.y);
	const a = Math.max(0, Math.min(HELP_AHEAD, to - HELP_RIM));
	return { x: h.x + u.x * a, y: h.y + u.y * a };
};

// What it costs him to help at `spot` (feet, more or less): from where he
// is, his own man `his` where the ball `h` is.
export const helpCost = (
	me: Pt,
	spot: Pt,
	his: Pt | undefined,
	h: Pt,
): number =>
	Math.hypot(spot.x - me.x, spot.y - me.y) +
	(his && Math.hypot(his.x - h.x, his.y - h.y) < ONE_PASS ? ONE_PASS_COST : 0);

export type Guarding = {
	// Where his man is, as he sees him.
	man: Seen;
	// Whether his man has the ball (as he sees it).
	on: boolean;
	// Where the ball is, as he sees it.
	ball: Pt | undefined;
	rim: Pt;
	threat: Threat;
	// Handing over to the plan by `until` (ms from now): where it has him
	// then (see stepper.ts), all of the way by then - setting off for it
	// `lead` (ms) before.
	handTo?: { at: Pt; v: Pt; until: number; lead: number; commit?: boolean };
	// In the middle of an act of his that leaves him where he is (a hand up
	// at a shot, say): his feet set for it.
	planted?: boolean;
	// The man with the ball on his way to the rim, as he sees him - and,
	// if he is the one stepping up into his way, where to.
	drive?: Seen;
	help?: Pt;
};

// Before he hands over to the plan (ms), he makes for where it has him:
// this long at the least, and as long again as getting there at half his
// top speed takes - no longer than this.
export const HAND_MS = 700;
const HAND_MOST = 2500;
export const handLead = (gap: number, d: Pick<Defender, "body">): number =>
	Math.min(HAND_MOST, HAND_MS + (1000 * gap) / (0.5 * d.body.top));
// How long getting to `to` takes him from the way he is going now (ms):
// what he has going the other way (or across) taken out first, then up to
// a good running pace (this much of his top) at this much of his push - not
// flat out, not his very best start.
const REACH_PACE = 0.8;
const REACH_GO = 0.6;
export const reachMs = (
	m: Mover,
	to: Pt,
	d: Pick<Defender, "body">,
): number => {
	const dx = to.x - m.x;
	const dy = to.y - m.y;
	const gap = Math.hypot(dx, dy);
	if (gap < 1e-6) {
		return 0;
	}
	const b = d.body;
	const along = (m.vx * dx + m.vy * dy) / gap;
	const across = Math.abs(m.vx * dy - m.vy * dx) / gap;
	const A = REACH_GO * b.go;
	const V = REACH_PACE * b.top;
	let t = across / b.side;
	let D = gap;
	let v0 = along;
	if (along < 0) {
		t += -along / b.brake;
		D += (along * along) / (2 * b.brake);
		v0 = 0;
	}
	if (v0 >= V) {
		t += D / v0;
	} else {
		const d1 = (V * V - v0 * v0) / (2 * A);
		t +=
			d1 >= D
				? (Math.sqrt(v0 * v0 + 2 * A * D) - v0) / A
				: (V - v0) / A + (D - d1) / V;
	}
	return 1000 * t;
};
// (Further off it than this, feet, he runs to it; nearer, he slides in.)
const COMMIT_SLIDE = 4;
// Into something of his that pins him, he sets off as long before it as
// getting there takes him (see reachMs), and this long more (ms) - no
// sooner than this.
const COMMIT_SPARE = 700;
export const COMMIT_MOST = 3000;
export const commitLead = (
	m: Mover,
	to: Pt,
	d: Pick<Defender, "body">,
	most = COMMIT_MOST,
): number => Math.min(most, COMMIT_SPARE + reachMs(m, to, d));
// His feet set for an act, no faster than this (feet a second).
const PLANTED = 3;

const smooth01 = (u: number) => {
	const x = u < 0 ? 0 : u > 1 ? 1 : u;
	return x * x * (3 - 2 * x);
};

// His goal this step, and how he goes about it.
export const decide = (
	m: Mover,
	g: Guarding,
	d: Defender,
): { goal: Goal; anim: AnimName } => {
	let spot = guardSpot(g.man, g.rim, g.ball, g.on, g.threat, d);
	// (How fast that spot is going: not his man's pace - sunk toward the
	// rim off a man cutting in to it, it slows as he gets there.)
	const ahead = guardSpot(
		{ x: g.man.x + g.man.vx * SPOT_DT, y: g.man.y + g.man.vy * SPOT_DT },
		g.rim,
		g.ball,
		g.on,
		g.threat,
		d,
	);
	let toV: Pt = {
		x: (ahead.x - spot.x) / SPOT_DT,
		y: (ahead.y - spot.y) / SPOT_DT,
	};
	if (g.help) {
		// Stepping up into the way of the ball, going with it.
		spot = g.help;
		toV = g.drive
			? { x: g.drive.vx * 0.5, y: g.drive.vy * 0.5 }
			: { x: 0, y: 0 };
	} else if (g.drive && !g.on) {
		// Near it, showing: a step toward its way in.
		const h = g.drive;
		const near = Math.hypot(h.x - spot.x, h.y - spot.y);
		if (near < SHOW_NEAR) {
			const u = unit(h, g.rim);
			const along = Math.max(0, (spot.x - h.x) * u.x + (spot.y - h.y) * u.y);
			const L = { x: h.x + u.x * along, y: h.y + u.y * along };
			const k = Math.min(
				SHOW_MOST / Math.max(1e-6, Math.hypot(L.x - spot.x, L.y - spot.y)),
				SHOW_SHARE * (1 - near / SHOW_NEAR),
			);
			spot = {
				x: spot.x + (L.x - spot.x) * Math.min(1, k),
				y: spot.y + (L.y - spot.y) * Math.min(1, k),
			};
		}
	}
	let to = spot;
	// Beaten on the ball: get back in front of him, between him and the rim.
	let beaten = false;
	if (g.on) {
		const rx = g.rim.x - m.x;
		const ry = g.rim.y - m.y;
		const rl = Math.hypot(rx, ry);
		const past =
			rl > 1e-6
				? ((g.man.x - m.x) * rx + (g.man.y - m.y) * ry) / rl
				: -Infinity;
		const pace = Math.hypot(g.man.vx, g.man.vy);
		beaten = past > -LEVEL && pace > GOING;
	}
	if (g.handTo?.commit) {
		// Something of his coming that pins him: straight there.
		to = g.handTo.at;
		toV = g.handTo.v;
	} else if (g.handTo) {
		const u = smooth01(1 - g.handTo.until / g.handTo.lead);
		to = {
			x: to.x + (g.handTo.at.x - to.x) * u,
			y: to.y + (g.handTo.at.y - to.y) * u,
		};
		toV = {
			x: toV.x + (g.handTo.v.x - toV.x) * u,
			y: toV.y + (g.handTo.v.y - toV.y) * u,
		};
	}
	const ball = g.ball ?? g.man;
	const face = Math.atan2(ball.y - m.y, ball.x - m.x);
	// How fast getting there asks him to go (closing the gap over about
	// CLOSE_S): past his slide, he runs.
	const gap = Math.hypot(to.x - m.x, to.y - m.y);
	const need = Math.hypot(toV.x, toV.y) + gap / CLOSE_S;
	const run =
		beaten ||
		need > d.slide + 1 ||
		(g.handTo?.commit === true && gap > COMMIT_SLIDE);
	const effort = run || g.on || g.help || g.handTo?.commit ? 1 : OFF_EFFORT;
	const goal: Goal = g.planted
		? { to, toV, effort: 1, face, most: PLANTED }
		: run
			? { to, toV, effort }
			: { to, toV, effort, face, most: d.slide };
	// What it looks like.
	const v = Math.hypot(m.vx, m.vy);
	const engaged =
		g.on ||
		g.help !== undefined ||
		g.planted === true ||
		g.handTo?.commit === true ||
		(g.drive !== undefined &&
			Math.hypot(g.drive.x - m.x, g.drive.y - m.y) < ENGAGE_DRIVE);
	let anim: AnimName;
	if (run && v > d.slide - 0.5) {
		anim = "run";
	} else if (!engaged) {
		anim = "walk";
	} else if (g.on && gap > CLOSE_FAR && v > CLOSE_PACE && !beaten) {
		const tx = g.man.x - m.x;
		const ty = g.man.y - m.y;
		const tl = Math.hypot(tx, ty);
		anim =
			tl > 1e-6 && (m.vx * tx + m.vy * ty) / (tl * v) > 0.5
				? "closeout"
				: "shuffle";
	} else {
		const bx = ball.x - m.x;
		const by = ball.y - m.y;
		const bl = Math.hypot(bx, by);
		anim =
			v > 1 && bl > 1e-6 && (m.vx * bx + m.vy * by) / (v * bl) < DROP
				? "slide"
				: v > 1
					? "shuffle"
					: "slide";
	}
	return { goal, anim };
};

// (Going round a man in his way: wide of him by this much, feet, middle to
// middle - looking this far ahead, feet; a man on the move, where he will
// be by the time he gets there, looking no further on than this, s.)
const ROUND = 2.6;
const ROUND_LOOK = 8;
const ROUND_AHEAD = 0.6;
// (Anybody else on the floor, as he sees him: where, and how fast going.)
export type Other = Pt & { vx?: number; vy?: number };

// Where he heads for on his way to `to`: round the nearest man in the way
// - where that man will be, if he is on the move - on the side he is on
// already, not into him.
export const roundOf = (m: Mover, to: Pt, others: readonly Other[]): Pt => {
	const dx = to.x - m.x;
	const dy = to.y - m.y;
	const d = Math.hypot(dx, dy);
	if (d < 1e-6) {
		return to;
	}
	const ux = dx / d;
	const uy = dy / d;
	const pace = Math.max(STROLL_ON, m.vx * ux + m.vy * uy);
	let best: { along: number; side: number; o: Pt } | undefined;
	for (const p of others) {
		const now = (p.x - m.x) * ux + (p.y - m.y) * uy;
		const ahead = Math.min(ROUND_AHEAD, Math.max(0, now) / pace);
		const o = {
			x: p.x + (p.vx ?? 0) * ahead,
			y: p.y + (p.vy ?? 0) * ahead,
		};
		const along = (o.x - m.x) * ux + (o.y - m.y) * uy;
		if (along <= 0 || along >= Math.min(d, ROUND_LOOK)) {
			continue;
		}
		const side = (o.x - m.x) * -uy + (o.y - m.y) * ux;
		if (Math.abs(side) < ROUND - 0.2 && (!best || along < best.along)) {
			best = { along, side, o };
		}
	}
	if (!best) {
		return to;
	}
	// (Past him on the side of him he is already: the man to his left goes
	// by on his right - right behind him, the way he is drifting.)
	const drift = m.vx * -uy + m.vy * ux;
	const s =
		Math.abs(best.side) < 0.6 && Math.abs(drift) > 0.3
			? drift >= 0
				? 1
				: -1
			: best.side >= 0
				? -1
				: 1;
	return {
		x: best.o.x + -uy * s * ROUND,
		y: best.o.y + ux * s * ROUND,
	};
};
// (The least pace, feet a second, he is reckoned to come up on a man at.)
const STROLL_ON = 4;
// (A man coming at him faster than this, feet a second, he gets out of the
// way of - aside, not backed up the way that man is going.)
const GIVE_PACE = 6;

// A man on the move coming through where he is within this long (s):
// he steps out of his way - across it, as far as going round him takes.
const GIVE_AHEAD = 0.5;
const giveWay = (m: Mover, goal: Goal, others: readonly Other[]): Goal => {
	let worst:
		| { tc: number; cx: number; cy: number; dc: number; wx: number; wy: number }
		| undefined;
	for (const o of others) {
		const ovx = o.vx ?? 0;
		const ovy = o.vy ?? 0;
		if (Math.hypot(ovx, ovy) <= GIVE_PACE) {
			continue;
		}
		const rx = m.x - o.x;
		const ry = m.y - o.y;
		const wx = m.vx - ovx;
		const wy = m.vy - ovy;
		const w2 = wx * wx + wy * wy;
		if (w2 < 1e-6) {
			continue;
		}
		const tc = -(rx * wx + ry * wy) / w2;
		if (tc <= 0 || tc > GIVE_AHEAD) {
			continue;
		}
		const cx = rx + wx * tc;
		const cy = ry + wy * tc;
		const dc = Math.hypot(cx, cy);
		if (dc < ROUND && (!worst || tc < worst.tc)) {
			worst = { tc, cx, cy, dc, wx, wy };
		}
	}
	if (!worst) {
		return goal;
	}
	const wl = Math.hypot(worst.wx, worst.wy);
	const px = -worst.wy / wl;
	const py = worst.wx / wl;
	const on = worst.cx * px + worst.cy * py;
	const s = Math.abs(on) > 0.1 ? Math.sign(on) : 1;
	const by = ROUND - worst.dc;
	return {
		...goal,
		to: { x: goal.to.x + px * s * by, y: goal.to.y + py * s * by },
		effort: 1,
	};
};

// One step for him, kept off everybody else on the floor.
export const moveOn = (
	m: Mover,
	want: Goal,
	d: Pick<Defender, "body">,
	others: readonly Other[],
	// (Coming in on a spot the play pins him to: on it, as the play has
	// everybody else round it.)
	keep = false,
): Mover => {
	if (keep) {
		return step(m, want, d.body, STEP_S);
	}
	const goal = giveWay(m, want, others);
	const way = roundOf(m, goal.to, others);
	const g: Goal =
		way === goal.to
			? goal
			: {
					...goal,
					to: way,
					toV: undefined,
					// (Going by at the pace he means to go, not stopping there.)
					arrive: Math.min(goal.most ?? Infinity, d.body.top),
				};
	let n = step(m, g, d.body, STEP_S);
	// (Coming up on a man faster than he could still pull up from before he
	// is on him - his own man ahead of him, say: slowed for him as he
	// comes, no harder than he brakes, not stopped dead on him.)
	for (const o of others) {
		const dx = o.x - n.x;
		const dy = o.y - n.y;
		const r = Math.hypot(dx, dy);
		if (r >= HEED || r < 1e-6) {
			continue;
		}
		const ux = dx / r;
		const uy = dy / r;
		// (His own going at him, less the man's going on away from him: a man
		// coming at him is not his to brake for - that one gets out of his
		// way, see giveWay - and he is never backed off.)
		const away = Math.max(0, (o.vx ?? 0) * ux + (o.vy ?? 0) * uy);
		const at = n.vx * ux + n.vy * uy - away;
		const may = Math.max(
			HEED_LEAST,
			Math.sqrt(2 * d.body.brake * Math.max(0, r - APART)),
		);
		if (at <= may) {
			continue;
		}
		const was = m.vx * ux + m.vy * uy - away;
		const cut = at - Math.max(may, was - d.body.brake * STEP_S);
		if (cut > 0) {
			// (Where the step has him, and how hard he pushes, as well.)
			const cx = cut * ux;
			const cy = cut * uy;
			n = {
				...n,
				x: n.x - (cx / 2) * STEP_S,
				y: n.y - (cy / 2) * STEP_S,
				vx: n.vx - cx,
				vy: n.vy - cy,
				ax: n.ax - cx / STEP_S,
				ay: n.ay - cy / STEP_S,
			};
		}
	}
	for (const o of others) {
		const dx = n.x - o.x;
		const dy = n.y - o.y;
		const r = Math.hypot(dx, dy);
		if (r >= APART) {
			continue;
		}
		let ux = r > 1e-6 ? dx / r : 1;
		let uy = r > 1e-6 ? dy / r : 0;
		// (A man coming through: aside, out of his way.)
		const ov = Math.hypot(o.vx ?? 0, o.vy ?? 0);
		if (ov > GIVE_PACE) {
			const ox = o.vx! / ov;
			const oy = o.vy! / ov;
			if (ux * ox + uy * oy > 0) {
				const across = -ux * oy + uy * ox;
				const s =
					Math.abs(across) > 0.15
						? Math.sign(across)
						: Math.sign(-n.vx * oy + n.vy * ox) || 1;
				const px = -oy * s + ux * 0.3;
				const py = ox * s + uy * 0.3;
				const pl = Math.hypot(px, py);
				ux = px / pl;
				uy = py / pl;
			}
		}
		// (Out toward a step off him - a step aside, not put there - and no
		// longer going into him. Off him no quicker than he gets going, from
		// however quick he was going off him already, and both reckoned from
		// how quick the man goes on away from him: a man on his way ahead
		// of him kept up with, not stopped dead behind.)
		const away = Math.min(0, (o.vx ?? 0) * ux + (o.vy ?? 0) * uy);
		const off = Math.max(0, m.vx * ux + m.vy * uy - away);
		const by = Math.min(
			APART - r,
			Math.min(ASIDE, off + d.body.go * STEP_S) * STEP_S,
		);
		const short = away + by / STEP_S - (n.vx * ux + n.vy * uy);
		n = {
			...n,
			x: n.x + ux * by,
			y: n.y + uy * by,
			vx: short > 0 ? n.vx + short * ux : n.vx,
			vy: short > 0 ? n.vy + short * uy : n.vy,
		};
	}
	return n;
};
