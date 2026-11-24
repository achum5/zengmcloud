// THE MAN WITH THE BALL (see README.md, step 7).
//
// Between the things the play pins him to (his catch, a shot, a handoff,
// its actions: a drive, coming off a screen, backing in), the man with the
// ball works his man himself, a step at a time, within a few steps of
// where the play has him (Alex's call: the play's passes and shots leave
// from about where it lays them). On the dribble he is never stood on it
// for long: he goes side to side along the arc, attacks his man a few
// dribbles and backs out of it, gives ground when his man crowds him, and
// now and then sizes him up a beat where he is. With it in his hands his
// feet are where they are: a stride or two to gather, then his pivot.
//
// Real men with the ball, a long touch on the dribble in the half court
// (tools/court3d-tape/nba, 17 games): stood under a fifth of the time;
// moving, across the floor 55% of it, at the rim 33%, back out 12%; most
// of it within a few feet of where they work it (2.4 ft off it, median).
//
// Nothing here knows about the plan or the timeline: the stepper says when
// he is his own (loop.ts), hands over to his acts, and keeps the time.

import type { Pt } from "../geometry.ts";
import type { AnimName } from "../poses.ts";
import type { Seen } from "./defense.ts";
import type { Goal, Mover } from "./movement.ts";
import { type Attacker, own01 } from "./offense.ts";

export type Handling = {
	t: number;
	// Where the play has him now: what he works round.
	set: Pt;
	rim: Pt;
	// The ball on the floor, his dribble: his feet free. In his hands
	// (picked up, a move put through where he stands, passing it), they are
	// where they are.
	dribbling: boolean;
	// The man guarding him, as he sees him.
	man?: Seen;
	// His four teammates, as he sees them.
	mates: readonly Pt[];
	// Handing over to the play by `until` (ms from now): where it has him
	// then (see offense.ts Floor).
	handTo?: { at: Pt; v: Pt; until: number; lead: number; commit?: boolean };
	// Picking it up this soon (ms) with something of the play's to come
	// before it leaves his hands (a shot): where the play has him then -
	// there by then, since he cannot go anywhere with it after.
	home?: { at: Pt; in: number };
};

// What he keeps between picks of what next.
export type HandMind = {
	// What he is about: sizing him up where he is, attacking him, backing
	// out of that, across the floor, giving ground.
	kind: "size" | "probe" | "back" | "side" | "retreat";
	// Where to, how fast (feet a second), and from when he picks again.
	at?: Pt;
	pace: number;
	until: number;
	// The play's spot he picked round.
	set?: Pt;
	// Picks so far (his own draws).
	n: number;
	// Where his feet are with it in his hands (his gather's end).
	pivot?: Pt;
};
export const handMindOf = (): HandMind => ({
	kind: "size",
	pace: 0,
	until: -Infinity,
	n: 0,
});

// A few steps (feet): no farther than this off where the play has him.
export const LEASH = 6;
// On his own he stays out of here (feet off the rim): in there is a drive,
// the play's.
const INSIDE = 13;
// Back to the play's spot if it moves this far (feet).
const SET_MOVED = 3;
// Attacking his man: this far in (feet), this fast (feet a second; the
// quicker and better a handler, the harder) - and in at all, more the
// better his handle and the more room his man gives him.
const PROBE_FT: [number, number] = [3, 6];
const PROBE_PACE: [number, number] = [8, 13];
const PROBE_SHARE = 0.3;
// Across the floor along the arc: this far (feet), this fast.
const SIDE_FT: [number, number] = [3, 6.5];
const SIDE_PACE: [number, number] = [4.5, 8.5];
const SIDE_SHARE = 0.45;
// (Going one way along it, he goes on that way rather than straight back
// about this much more often - feet of room it is worth.)
const ON_WAY = 2.5;
// Sizing him up where he is: this long (ms).
const SIZE_MS: [number, number] = [350, 900];
const SIZE_SHARE = 0.25;
// Backing out to his spot after attacking: this fast; set there this
// long (ms) after.
const BACK_PACE: [number, number] = [5, 7.5];
const BACK_SET: [number, number] = [100, 450];
// His man this near (feet), in front of him: crowded, he gives ground -
// this far back (feet). His man farther off than this (feet), or not in
// front of him: room to attack.
const CROWD = 3.5;
const GIVE_FT: [number, number] = [2.5, 4];
const SAG = 7;
// Never on top of a teammate: this far off one at least (feet).
const ROOM = 8;
// Done with a move this near where it was going (feet).
const THERE = 0.8;
// The play's spot for him as he picks it up: making for it from this long
// before (ms), all the way there this long before.
const HOME_LEAD = 1400;
const HOME_SPARE = 900;
const HOME_SURE = 450;
// Gathering, he stops in a stride or two: about this hard (a share of his
// brakes).
const GATHER = 0.75;
// Facing the way he goes from this fast (feet a second); slower, square to
// the rim.
const FACE_GOING = 8;

const smooth01 = (u: number) => {
	const x = u < 0 ? 0 : u > 1 ? 1 : u;
	return x * x * (3 - 2 * x);
};
const lerp = (a: number, b: number, u: number) => a + (b - a) * u;
const dist = (a: Pt, b: Pt) => Math.hypot(a.x - b.x, a.y - b.y);

// (No farther than the leash off the play's spot, no nearer the rim than
// INSIDE - unless the play has him in there itself.)
const kept = (Q: Pt, f: Handling): Pt => {
	let P = Q;
	const off = dist(P, f.set);
	if (off > LEASH) {
		const k = LEASH / off;
		P = { x: f.set.x + (P.x - f.set.x) * k, y: f.set.y + (P.y - f.set.y) * k };
	}
	const least = Math.min(INSIDE, dist(f.set, f.rim));
	const r = dist(P, f.rim);
	if (r < least && r > 1e-6) {
		const k = least / r;
		P = { x: f.rim.x + (P.x - f.rim.x) * k, y: f.rim.y + (P.y - f.rim.y) * k };
	}
	return {
		x: Math.min(93, Math.max(1, P.x)),
		y: Math.min(48.4, Math.max(1.6, P.y)),
	};
};

// (Room there from his nearest teammate, feet.)
const roomAt = (Q: Pt, f: Handling): number => {
	let room = Infinity;
	for (const P of f.mates) {
		room = Math.min(room, dist(Q, P));
	}
	return room;
};

// His man: how far off him, and whether between him and the rim.
const manOf = (
	here: Pt,
	f: Handling,
): { gap: number; front: boolean } | undefined => {
	const M = f.man;
	if (!M) {
		return undefined;
	}
	const gap = dist(here, M);
	const ux = f.rim.x - here.x;
	const uy = f.rim.y - here.y;
	const L = Math.hypot(ux, uy) || 1;
	const along = ((M.x - here.x) * ux + (M.y - here.y) * uy) / L;
	return { gap, front: along > 0.5 * gap };
};

// What next, on the dribble.
const pick = (
	m: Mover,
	f: Handling,
	a: Attacker,
	mind: HandMind,
	pid: number,
) => {
	const here = { x: m.x, y: m.y };
	const draw = (salt: number) => own01(pid, mind.n * 37 + salt);
	const r = dist(here, f.rim);
	const ux = (f.rim.x - here.x) / (r || 1);
	const uy = (f.rim.y - here.y) / (r || 1);
	const man = manOf(here, f);
	const crowded = man !== undefined && man.front && man.gap < CROWD;
	const room = man === undefined || !man.front || man.gap > SAG;
	// (Better with it, quicker at it.)
	const skill = 0.6 * a.handle + 0.4 * a.quick;
	const was = mind.kind;
	mind.n += 1;
	mind.set = { ...f.set };
	// Attacking him, then back out of it.
	if (was === "probe") {
		const Q = kept(
			{
				x: f.set.x + (draw(3) - 0.5) * 3,
				y: f.set.y + (draw(4) - 0.5) * 3,
			},
			f,
		);
		mind.kind = "back";
		mind.at = Q;
		mind.pace = lerp(BACK_PACE[0], BACK_PACE[1], draw(5));
		mind.until =
			f.t +
			(1000 * dist(here, Q)) / mind.pace +
			lerp(BACK_SET[0], BACK_SET[1], draw(6));
		return;
	}
	// Crowded: he gives ground first.
	if (crowded && was !== "retreat") {
		const k = lerp(GIVE_FT[0], GIVE_FT[1], draw(7));
		const Q = kept({ x: here.x - ux * k, y: here.y - uy * k }, f);
		mind.kind = "retreat";
		mind.at = Q;
		mind.pace = lerp(BACK_PACE[0], BACK_PACE[1], draw(8));
		mind.until = f.t + (1000 * dist(here, Q)) / mind.pace + 150;
		return;
	}
	const probe =
		r > INSIDE + 2
			? PROBE_SHARE *
				(0.4 + 1.2 * a.handle) *
				(a.big ? 0.4 : 1) *
				(room ? 1.6 : 1)
			: 0;
	const size = SIZE_SHARE * (a.big ? 1.6 : 1) * (1.3 - 0.6 * skill);
	const side = SIDE_SHARE;
	const x = draw(1) * (probe + size + side);
	if (x < probe) {
		// In at him - by his weak side: off the way he is set, the side of
		// him with more room.
		const k = lerp(PROBE_FT[0], PROBE_FT[1], draw(9));
		let across = (draw(10) - 0.5) * 3;
		if (man) {
			const s = (f.man!.x - here.x) * -uy + (f.man!.y - here.y) * ux;
			across = -Math.sign(s || 1) * (1 + 1.5 * draw(10));
		}
		const Q = kept(
			{
				x: here.x + ux * k - uy * across,
				y: here.y + uy * k + ux * across,
			},
			f,
		);
		mind.kind = "probe";
		mind.at = Q;
		mind.pace = lerp(
			PROBE_PACE[0],
			PROBE_PACE[1],
			0.5 * skill + 0.5 * draw(11),
		);
		mind.until = f.t + (1000 * dist(here, Q)) / mind.pace + 150;
		return;
	}
	if (x < probe + size) {
		mind.kind = "size";
		mind.at = here;
		mind.pace = 0;
		mind.until = f.t + lerp(SIZE_MS[0], SIZE_MS[1], draw(12));
		return;
	}
	// Along the arc round the rim from the play's spot, to whichever side
	// has more room - with a draw of his own.
	const R = Math.max(dist(f.set, f.rim), INSIDE);
	const th = Math.atan2(f.set.y - f.rim.y, f.set.x - f.rim.x);
	const s = lerp(SIDE_FT[0], SIDE_FT[1], draw(13));
	const at = (w: number): Pt => {
		const to = th + (w * s) / R;
		return kept(
			{ x: f.rim.x + Math.cos(to) * R, y: f.rim.y + Math.sin(to) * R },
			f,
		);
	};
	const A = at(1);
	const B = at(-1);
	// (Not where he is already: the other way - and on the way he is
	// going, more often than not, not straight back.)
	const v = Math.hypot(m.vx, m.vy);
	const score = (Q: Pt, salt: number) =>
		Math.min(ROOM * 1.5, roomAt(Q, f)) +
		Math.min(3, dist(Q, here)) +
		(v > 1.5 && (Q.x - here.x) * m.vx + (Q.y - here.y) * m.vy > 0
			? ON_WAY
			: 0) +
		3 * draw(salt);
	const Q = score(A, 14) >= score(B, 15) ? A : B;
	mind.kind = "side";
	mind.at = roomAt(Q, f) < ROOM ? kept(f.set, f) : Q;
	mind.pace = lerp(SIDE_PACE[0], SIDE_PACE[1], 0.4 * skill + 0.6 * draw(16));
	mind.until =
		f.t +
		(1000 * dist(here, mind.at)) / Math.max(1, mind.pace) +
		200 * draw(17);
};

export const decide = (
	m: Mover,
	f: Handling,
	a: Attacker,
	mind: HandMind,
	pid: number,
): { goal: Goal; anim: AnimName } => {
	const here = { x: m.x, y: m.y };
	const face = Math.atan2(f.rim.y - m.y, f.rim.x - m.x);
	const v = Math.hypot(m.vx, m.vy);
	if (!f.dribbling) {
		// In his hands: a stride or two to stop, and there.
		if (!mind.pivot) {
			const k = v > 0.5 ? (v * v) / (2 * GATHER * a.body.brake) / v : 0;
			mind.pivot = { x: m.x + m.vx * k, y: m.y + m.vy * k };
		}
		// (Into something of the play's from where he is: a step into it.)
		const to = f.handTo?.commit ? f.handTo.at : mind.pivot;
		return {
			goal: { to, effort: 1, arrive: 0, face },
			anim: "walk",
		};
	}
	mind.pivot = undefined;
	const moved = !mind.set || dist(f.set, mind.set) > SET_MOVED;
	const homing = f.home !== undefined && f.home.in <= HOME_LEAD;
	const homeFrom = () => {
		mind.kind = "back";
		mind.at = f.home!.at;
		mind.pace = BACK_PACE[1];
		mind.until = f.t + f.home!.in;
		mind.set = { ...f.set };
	};
	if (
		!mind.at ||
		moved ||
		f.t >= mind.until ||
		(mind.kind !== "size" && dist(here, mind.at) < THERE && v < 2)
	) {
		if (homing) {
			// (Nothing new with his pick-up coming: back to the spot for it.)
			homeFrom();
			mind.n += 1;
		} else {
			pick(m, f, a, mind, pid);
			// (Nor anything he could not be done with and back from in time.)
			if (
				f.home &&
				mind.until -
					f.t +
					(1000 * dist(mind.at!, f.home.at)) / BACK_PACE[0] +
					HOME_SPARE >
					f.home.in
			) {
				homeFrom();
			}
		}
	}
	let to = mind.at!;
	let toV: Pt = { x: 0, y: 0 };
	let most = mind.kind === "size" ? undefined : mind.pace;
	let effort = mind.kind === "probe" ? 0.85 : mind.kind === "size" ? 0.3 : 0.55;
	// Something of the play's coming: back to its spot for it.
	if (f.handTo?.commit) {
		to = f.handTo.at;
		toV = f.handTo.v;
		most = undefined;
		effort = 1;
	} else if (f.home && f.home.in <= HOME_SURE) {
		to = f.home.at;
		most = undefined;
		effort = 1;
	} else {
		const homes: { at: Pt; v: Pt; u: number }[] = [];
		if (f.handTo) {
			homes.push({
				at: f.handTo.at,
				v: f.handTo.v,
				u: smooth01(1 - f.handTo.until / f.handTo.lead),
			});
		}
		if (f.home && f.home.in <= HOME_LEAD) {
			homes.push({
				at: f.home.at,
				v: { x: 0, y: 0 },
				u: smooth01((HOME_LEAD - f.home.in) / (HOME_LEAD - HOME_SURE)),
			});
		}
		for (const h of homes) {
			to = { x: to.x + (h.at.x - to.x) * h.u, y: to.y + (h.at.y - to.y) * h.u };
			toV = {
				x: toV.x + (h.v.x - toV.x) * h.u,
				y: toV.y + (h.v.y - toV.y) * h.u,
			};
			if (h.u > 0.5) {
				most = undefined;
				effort = Math.max(effort, 0.7);
			}
		}
	}
	const goal: Goal = {
		to,
		toV,
		effort,
		...(most !== undefined ? { most } : {}),
		...(v < FACE_GOING || mind.kind !== "probe" ? { face } : {}),
	};
	return { goal, anim: "dribble" };
};
