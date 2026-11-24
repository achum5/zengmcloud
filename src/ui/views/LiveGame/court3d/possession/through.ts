// ON THROUGH THE CATCH AND THE THROW (see README.md, step 8a).
//
// Real men catch a pass on the move - 5.6 ft/s at the catch, median, over
// 17 NBA games of tracking - and gather after it into whatever they do with
// it: about 4 ft/s as a catch-and-shoot comes into their hands, still 2 or
// 3 a few tenths later as they go up; passed straight on or dribbled off,
// hardly slowing at all. They throw it on the move too (3.9 ft/s as it
// leaves their hands). The play lays a man's run to the spot it has him
// catch at - or throw from - as a run that ends there, stood, before the
// ball gets to him: braked from a run to a stand, sometimes in a few tenths
// of a second, and stood when it comes (0.5 ft/s at the catch, median; 0.2
// at the throw). That is the jolt, and the staged look.
//
// So the catch is a window, not a point, and the throw the same. Each such
// run is laid on through it instead:
// - on into the run he goes off on straight after it (a dribble up the
//   floor off an outlet, a drive off the catch), handed on to it at the
//   pace both runs - and the turn between them - allow;
// - or still going as the ball gets to him (about as fast as he can stop in
//   the time before he has to be stood: up into a shot, a move with his feet
//   set, his next run), and stopped on the play's spot by then. A pass on
//   from him, or anything else his legs go on under (see evaluate.ts), he
//   does on the way.
// He catches it a step short of the play's spot, along his own way in; the
// ball flies to his hands wherever they are, and whatever he does after it
// is from the play's spot, as it was.
//
// (The new sim only: called on the play's runs before they are stepped.)

import type { Act, BallSeg, Move, Track } from "../director.ts";
import { runShape } from "../motion.ts";
import type { AnimName } from "../poses.ts";

// A run into the catch: at least this far (feet), ended no more than this
// long (ms) before the ball gets to him - or this long after.
const RUN_LEAST = 2;
const ARRIVED_WITHIN = 900;
const ARRIVING_WITHIN = 450;
// Under this far (feet), a move is him stood where he is.
const STOOD = 0.05;
// After the catch, a stride or two to stop in (ms) at the most - no more
// than he can stop in braking this hard (feet a second, a second) - and at
// the catch going no faster than this (feet a second). Too short a stride
// or two (ms) to be worth it: the run as it was.
const GATHER_MOST = 450;
const GATHER_BRAKE = 11;
const CATCH_MOST = 7;
const GATHER_LEAST = 120;
// Going slower than this (feet a second) at the catch, it is not worth it.
const CATCH_LEAST = 1.5;
// At most this much of the run (its length) is left for after the catch.
const AFTER_MOST = 0.4;
// Off on another run this soon (ms) after the catch, he goes on into it -
// as fast through the catch as the slower of the two, no faster than this
// much over the run in's own pace (its length over its time), and no more
// of a change in the way he is going, turning into the other, than this
// (feet a second).
const ON_WITHIN = 450;
const ON_OVER = 1.5;
const TURN_MOST = 4;
// Going up off the floor, he is stood this long (ms) before it - unless
// the play has him on a run as he goes up (starting no more than this long,
// ms, before then, and no more than this long after the catch or the pick-
// up): on into it.
const UP_SET = 60;
const UP_ON = 150;
// (Going on this much the way he was (its cosine), a step of a hop is one
// run with the one into it.)
const HOP_ON = 0.7;
const UP_ON_WITHIN = 1000;
// A pass that meets the floor on its way (feet up, at most): bounced.
const BOUNCED = 0.5;
// What he can be gathering into as it starts: facing up, holding it.
const GATHER_INTO = new Set<AnimName>([
	"catch",
	"triple",
	"tripleHigh",
	"tripleSweep",
	"hold",
	"callPlay",
]);
// What he does on the go, his legs on under it (as evaluate.ts has them).
const ON_THE_GO = new Set<AnimName>([
	"catchHigh",
	"catchLow",
	"pass",
	"passSnap",
	"passBounce",
	"passBounceSnap",
	"passOverhead",
	"passOutlet",
	"passPush",
	"passBouncePush",
	"passBehind",
	"passNoLook",
	"callBall",
]);

const len = (m: Move) => Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);

// By when, after `t`, something of his wants him stood on his spot: as it
// starts (a move with his feet set), or - up into the air off it - a moment
// before he goes up.
const standBy = (acts: readonly Act[], t: number): number => {
	let by = Infinity;
	for (const a of acts) {
		if (
			a.t0 <= t ||
			a.flash ||
			a.quiet ||
			GATHER_INTO.has(a.anim) ||
			ON_THE_GO.has(a.anim)
		) {
			continue;
		}
		const at = a.jump
			? Math.max(a.t0, a.t0 + a.jump[0] * (a.t1 - a.t0) - UP_SET)
			: a.t0;
		by = Math.min(by, at);
	}
	return by;
};

// Lays the run that brings him to his catch at `tc` on through it, if the
// play has him stood there for it. Whether it did.
export const carryThrough = (
	tr: Track,
	tc: number,
	// The set's spacing: runs a man makes on his own, as the set has him go
	// (see loop.ts) - the stride or two after one of them, too.
	spacing?: Set<Move>,
): boolean => {
	const moves = tr.moves;
	// The run in: the last of his that ends about the catch, nothing but
	// standing after it till then.
	let k = -1;
	for (let i = moves.length - 1; i >= 0; i--) {
		const m = moves[i]!;
		if (m.t0 >= tc) {
			continue;
		}
		const L = len(m);
		if (L < STOOD) {
			continue;
		}
		if (
			L >= RUN_LEAST &&
			!(m.v1 ?? 0) &&
			m.t1 >= tc - ARRIVED_WITHIN &&
			m.t1 <= tc + ARRIVING_WITHIN
		) {
			k = i;
		}
		break;
	}
	if (k < 0) {
		return false;
	}
	const m = moves[k]!;
	const L = len(m);
	const ux = (m.to.x - m.from.x) / L;
	const uy = (m.to.y - m.from.y) / L;
	// Stood by the next thing that wants him stood, or the next run of his.
	const stand = standBy(tr.acts, tc);
	let j = -1;
	for (let i = k + 1; i < moves.length; i++) {
		if (len(moves[i]!) >= STOOD) {
			j = i;
			break;
		}
	}
	const n = j >= 0 ? moves[j]! : undefined;

	// On into his next run, straight off the catch - or up into the air on
	// it: the play carrying him on through his jump (a drift, the last of a
	// hop into it), he goes up going, as a man must to go anywhere in the
	// air. (A run the play has him make going already - its pace up off the
	// floor - no faster than that.)
	// (The first of a hop into the air the play has him make, going on the
	// way he was: one run with the one in, carried on into the rest of it.)
	if (
		n &&
		len(n) < RUN_LEAST &&
		(n.v1 ?? 0) > 0 &&
		n.t0 - m.t1 <= 1 &&
		n.t0 <= stand + UP_SET &&
		(ux * (n.to.x - n.from.x) + uy * (n.to.y - n.from.y)) / len(n) >= HOP_ON
	) {
		m.to = { ...n.to };
		m.t1 = n.t1;
		m.v1 = n.v1;
		moves.splice(k + 1, j - k);
		return true;
	}
	const upOn =
		n !== undefined &&
		stand < Infinity &&
		n.t0 >= stand - UP_ON &&
		n.t0 - tc <= UP_ON_WITHIN;
	if (
		n &&
		(n.t0 - tc <= ON_WITHIN || upOn) &&
		n.t0 <= stand + UP_SET &&
		(len(n) >= RUN_LEAST || (n.v1 ?? 0) > 0)
	) {
		const Ln = len(n);
		const cos = (ux * (n.to.x - n.from.x) + uy * (n.to.y - n.from.y)) / Ln;
		const D = (n.t0 - m.t0) / 1000;
		const p = Math.min(
			runShape(L, D, m.v0 ?? 0, 0).vc,
			(n.v0 ?? 0) ||
				runShape(Ln, (n.t1 - n.t0) / 1000, 0, n.v1 ?? 0).vc ||
				Infinity,
			(ON_OVER * L) / D,
			cos > 0.999 ? Infinity : TURN_MOST / Math.sqrt(2 - 2 * cos),
		);
		if (p >= CATCH_LEAST) {
			m.t1 = n.t0;
			m.v1 = p;
			n.v0 = p;
			moves.splice(k + 1, j - k - 1);
			return true;
		}
	}

	// Or stopped on his spot a stride or two after it.
	const cap = Math.min(tc + GATHER_MOST, stand, n?.t0 ?? Infinity);
	const T2 = (cap - tc) / 1000;
	if (T2 * 1000 < GATHER_LEAST) {
		return false;
	}
	const D = (m.t1 - m.t0) / 1000;
	// (As fast as he was running, at the most - and no faster than he can
	// stop from in the time, nor so fast it leaves too much of the run.)
	let p = Math.min(
		runShape(L, D, m.v0 ?? 0, 0).vc,
		CATCH_MOST,
		GATHER_BRAKE * T2,
	);
	let g = (p * T2) / 2;
	if (g > AFTER_MOST * L) {
		g = AFTER_MOST * L;
		p = (2 * g) / T2;
	}
	if (p < CATCH_LEAST) {
		return false;
	}
	const S = { ...m.to };
	const C = { x: S.x - ux * g, y: S.y - uy * g };
	const end = tc + T2 * 1000;
	// (The run, kept - other things know it by itself - to the catch.)
	m.to = C;
	m.t1 = tc;
	m.v1 = p;
	const gather: Move = {
		t0: tc,
		t1: end,
		from: { ...C },
		to: S,
		anim: m.anim,
		v0: p,
		v1: 0,
		...(m.face !== undefined ? { face: m.face } : {}),
	};
	// (What had him stood on his spot meanwhile starts once he is.)
	const after: Move[] = [];
	for (let i = k + 1; i < moves.length; i++) {
		const o = moves[i]!;
		if (o.t0 >= end) {
			after.push(o);
		} else if (o.t1 > end) {
			const later = { ...o, t0: end };
			if (spacing?.has(o)) {
				spacing.add(later);
			}
			after.push(later);
		}
	}
	if (spacing?.has(m)) {
		spacing.add(gather);
	}
	moves.splice(k + 1, moves.length - k - 1, gather, ...after);
	return true;
};

// Every pass in play - thrown, and caught by a man of his own side or
// picked off - laid on through its throw and through its catch where the
// play has the man stood for it. (Its catches first: a pass thrown straight
// off one is thrown on the way out of it already.) How many it laid.
export const throughPasses = (
	tracks: ReadonlyMap<number, Track>,
	ball: readonly BallSeg[],
	spacing?: Set<Move>,
): number => {
	let n = 0;
	const thrown: [number, number][] = [];
	for (let k = 1; k < ball.length; k++) {
		const f = ball[k]!;
		if (f.kind !== "fly" || f.dead || !("pid" in f.from)) {
			continue;
		}
		// (To a man, or off the floor to one: not his shot.)
		const g = ball[k + 1];
		const to =
			"pid" in f.to
				? f.to.pid
				: f.to.z < BOUNCED &&
					  g?.kind === "fly" &&
					  !g.dead &&
					  "pid" in g.to &&
					  !("pid" in g.from)
					? g.to.pid
					: undefined;
		if (to === undefined) {
			continue;
		}
		thrown.push([f.from.pid, f.t0]);
		const s = ball["pid" in f.to ? k + 1 : k + 2];
		const tr = tracks.get(to);
		if (
			s?.kind === "hold" &&
			s.pid === to &&
			tr &&
			carryThrough(tr, s.t0, spacing)
		) {
			n++;
		}
	}
	for (const [pid, t] of thrown) {
		const tr = tracks.get(pid);
		if (tr && carryThrough(tr, t, spacing)) {
			n++;
		}
	}
	return n;
};

// Every jumper off the dribble - a pull-up - laid on through his pick-up:
// still going as he takes it into his hands, the stride or two of his
// gather under him, stopped on the play's spot a moment before he goes up.
// (Real pull-ups: 5.7 ft/s 0.6 s before it leaves his hands, 4.8 at 0.4 s,
// median, over the 17 games.) How many it laid.
export const throughPullUps = (
	tracks: ReadonlyMap<number, Track>,
	ball: readonly BallSeg[],
	spacing?: Set<Move>,
): number => {
	let n = 0;
	for (let k = 2; k < ball.length; k++) {
		const f = ball[k]!;
		if (f.kind !== "fly" || f.dead || !("pid" in f.from) || "pid" in f.to) {
			continue;
		}
		const pid = f.from.pid;
		const up = ball[k - 1]!;
		const was = ball[k - 2]!;
		if (
			up.kind !== "hold" ||
			up.pid !== pid ||
			up.style !== "hold" ||
			was.kind !== "hold" ||
			was.pid !== pid ||
			was.style === "hold"
		) {
			continue;
		}
		// (His shot: up off the floor with it.)
		const tr = tracks.get(pid);
		const shot = tr?.acts.find(
			(a) => a.jump !== undefined && a.t0 <= f.t0 && a.t1 >= f.t0,
		);
		if (tr && shot && carryThrough(tr, up.t0, spacing)) {
			n++;
		}
	}
	return n;
};
