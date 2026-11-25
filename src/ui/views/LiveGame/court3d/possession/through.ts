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
import { COURT_H, COURT_W } from "../geometry.ts";
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

// A number from 0 to 1 that depends only on a and b (rounded to whole
// numbers) - the same on every device.
const hash01 = (a: number, b: number): number => {
	let h =
		Math.imul(Math.round(a) | 0, 0x9e3779b1) ^
		Math.imul(Math.round(b) | 0, 0x85ebca77);
	h = Math.imul(h ^ (h >>> 16), 0x7feb352d);
	h = Math.imul(h ^ (h >>> 15), 0x846ca68b);
	return ((h ^ (h >>> 16)) >>> 0) / 4294967296;
};

// The value at `u` (0 to 1) of a spread with these quantiles (at SPREAD_Q:
// its tenth, quarter, half, three quarters and nine tenths) - straight
// between them, no further out than the outermost.
const SPREAD_Q = [0.1, 0.25, 0.5, 0.75, 0.9];
const quantileAt = (q: readonly number[], u: number): number => {
	if (u <= SPREAD_Q[0]!) {
		return q[0]!;
	}
	for (let k = 1; k < SPREAD_Q.length; k++) {
		if (u <= SPREAD_Q[k]!) {
			const f = (u - SPREAD_Q[k - 1]!) / (SPREAD_Q[k]! - SPREAD_Q[k - 1]!);
			return q[k - 1]! + (q[k]! - q[k - 1]!) * f;
		}
	}
	return q.at(-1)!;
};

// Whether something of his other than `c` has him stood (or up in the air)
// somewhere from `t0` till `t1`.
const pinned = (
	acts: readonly Act[],
	c: Act,
	t0: number,
	t1: number,
): boolean =>
	acts.some(
		(a) =>
			a !== c &&
			a.t0 < t1 &&
			a.t1 > t0 &&
			!a.flash &&
			!a.quiet &&
			!GATHER_INTO.has(a.anim) &&
			!ON_THE_GO.has(a.anim),
	);

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

// CONTESTS. Real defenders are still coming as a jumper leaves the
// shooter's hands - 6.5 ft/s at 4.7 ft off him, median, over the 17 games;
// 7.9 at 5.1 ft off a quick catch - moving on 92 shots in 100, about 30
// degrees off straight at him, and carry on in after it, up into the air
// with it and down beyond, by him. The play laid his closeout as a run that
// ended on its spot (the room it gives the shooter), stood, and his hand
// up from there - or had him stood on that spot all along, or still on his
// way to it as he went up, stopping in the air. Laid on through it
// instead: on his spot as it leaves the shooter's hands, as before (how
// open the shot is, is the play's), but still coming - off the floor going
// (on at that pace in the air, as a man must), braking once he is down -
// and no nearer the shooter than a man gets. Stood there all along, he was
// stood a step or two further off it, and comes in through it; drifting in
// too near it to get going that fast, he pulls up first. Whatever the play
// has him do next waits till he is down (and, if it must, no longer: off
// on it once he is stood).

const CONTESTS = new Set<AnimName>(["contest", "contestTwo", "contestFace"]);
// How fast the nearest man is going as it leaves the shooter's hands, and
// how far off straight at him (degrees) - off a catch he shot within a
// second (QUICK, ms), and off one he held: 1,504 real jumpers from 10 to 30
// ft (see SPREAD_Q). Each man's own draw from it (not leaning made or
// missed: how open it is does that), no faster than his way in allows.
const QUICK = 1000;
const CLOSE_PACE_QUICK = [3, 5, 7.9, 10.8, 13];
const CLOSE_PACE_HELD = [2, 3.5, 5.6, 8.1, 10.4];
const CLOSE_ANGLE_QUICK = [0, 12.7, 24.6, 39.2, 62.8];
const CLOSE_ANGLE_HELD = [0, 16.6, 36.6, 64.4, 80];
// Stopped no nearer the shooter than this (feet, middle to middle) braking
// this hard once he is down (feet a second, a second).
const CLOSE_NEAR = 1.5;
const LAND_BRAKE = 15;
// Stood on his spot for it: from this far further off it at the least, and
// at the most (feet) - far enough to get going as fast as his draw (at this
// much a second, a second) - and no more than this long before he goes up
// (ms) on it.
const LUNGE_LEAST = 1.5;
const LUNGE_MOST = 4.5;
const LUNGE_ACC = 10;
const LUNGE_STOOD = 5000;
// (Coming onto it any faster than this, feet a second, he is on his way.)
const LUNGE_STILL = 1.5;
// (On it: both ends this near his spot, feet.)
const ON_SPOT = 1.2;
// A run in of at least this far (feet) - his choppy steps in, say - ended
// no more than ARRIVED_WITHIN before he goes up, or this long (ms) after;
// his way in from no more than CLOSE_BACK (ms) before he goes up; and a
// contest going up no later than UP_AFTER (ms) after the shot does (a late
// hand up, still coming as it goes).
const CLOSE_RUN_LEAST = 1;
const CLOSE_LATE = 50;
const CLOSE_BACK = 1600;
const UP_AFTER = 400;
// (On the one way: each step of it this near the way to the spot, its
// cosine.)
const CLOSE_ONE_WAY = 0.7;
// (Bent by him by no more than this (radians), tried this much at a time.)
const BEND_MOST = 0.6;
const BEND_STEP = 0.1;
// Where he stops (or goes on from): a step inside the lines (feet) - and
// the play's next run of his from there no more than this much (feet a
// second) harder than from where it had him.
const IN = 1;
const NEXT_HARDER = 1;
// (A closeout's, more: a real man flies on by the shooter and has further
// to come back.)
const CLOSE_NEXT_HARDER = 3;
// (Stood still at least this long, ms, after a run of his put off.)
const STOOD_LEAST = 50;
// (No run in faster than this on average, feet a second.)
const TOP_RUN = 18;

// Where a man is at t, as his moves have him (evenly along each).
export const spotAt = (tr: Track, t: number): { x: number; y: number } => {
	let at = tr.start;
	for (const m of tr.moves) {
		if (m.t0 > t) {
			break;
		}
		if (t < m.t1 && m.t1 > m.t0) {
			const u = (t - m.t0) / (m.t1 - m.t0);
			return {
				x: m.from.x + (m.to.x - m.from.x) * u,
				y: m.from.y + (m.to.y - m.from.y) * u,
			};
		}
		at = m.to;
	}
	return { ...at };
};

// How near (feet) the way from A to B comes to Q.
const nearest = (
	A: { x: number; y: number },
	B: { x: number; y: number },
	Q: { x: number; y: number },
): number => {
	const dx = B.x - A.x;
	const dy = B.y - A.y;
	const L2 = dx * dx + dy * dy;
	const u =
		L2 < 1e-9
			? 0
			: Math.max(0, Math.min(1, ((Q.x - A.x) * dx + (Q.y - A.y) * dy) / L2));
	return Math.hypot(A.x + dx * u - Q.x, A.y + dy * u - Q.y);
};

// How a run is laid on up into a jump (see upThrough).
type UpThrough = {
	// Kept this far (feet) off each of these, at the least, where he goes
	// - and, `bend`, bent by the first of them as he goes up if he must.
	avoid: readonly { x: number; y: number }[];
	near: number;
	bend: boolean;
	// The fastest he goes through it (feet a second).
	most: number;
	// The run in may carry him on up off the floor, to `r` at the most.
	carried: boolean;
	// His way in from no longer than this (ms) before he goes up - and, from
	// stood, `wait`, set off on it as late as gets him there that fast (not
	// a walk all the way).
	back: number;
	wait: boolean;
	// The play's runs either side of it give way: his way in may be a run
	// still going as he goes up (cut where it has him at `r`), and his next
	// may wait till he is down and slowed for it - or, stood once he is,
	// set off from there.
	give: boolean;
	// How much harder (feet a second, at its cruise) the play's next run of
	// his may be made, from where he comes to.
	harder: number;
};

// Lays his run into the act `c` - up off the floor - on through it, if the
// play has him stood for it: on the play's spot at `r` (as the shot leaves
// the shooter's hands, or the ball comes to his own), still going. Whether
// it did.
const upThrough = (tr: Track, c: Act, r: number, o: UpThrough): boolean => {
	if (!c.jump) {
		return false;
	}
	const D = c.t1 - c.t0;
	const off = c.t0 + c.jump[0] * D;
	const down = c.t0 + c.jump[1] * D;
	const moves = tr.moves;
	let k = -1;
	// (And the last move of the run he is on then - his way in, to the
	// play's spot at `r` on it.)
	let ke = -1;
	let C = { x: 0, y: 0 };
	for (let i = moves.length - 1; i >= 0; i--) {
		const m = moves[i]!;
		if (m.t0 >= off) {
			continue;
		}
		const L = len(m);
		if (L < STOOD) {
			continue;
		}
		if (o.give && m.t1 >= off - o.back) {
			let e = i;
			while (
				(moves[e]!.v1 ?? 0) &&
				e + 1 < moves.length &&
				(moves[e + 1]!.v0 ?? 0) &&
				Math.abs(moves[e + 1]!.t0 - moves[e]!.t1) <= 1
			) {
				e++;
			}
			const me = moves[e]!;
			if (
				!(me.v1 ?? 0) &&
				(e > i || m.t1 > off + CLOSE_LATE) &&
				me.t1 <= down + ON_WITHIN
			) {
				k = i;
				ke = e;
				C = spotAt(tr, Math.min(r, me.t1));
				break;
			}
		}
		if (
			!(m.v1 ?? 0) &&
			m.t1 >= off - (o.wait ? o.back : ARRIVED_WITHIN) &&
			m.t1 <= (o.carried ? Math.max(off + CLOSE_LATE, r) : off + CLOSE_LATE)
		) {
			k = i;
			ke = i;
			C = m.to;
		}
		break;
	}
	if (k < 0) {
		return false;
	}
	// (His way in: the run - choppy steps and all, and the shuffle onto the
	// spot it ends with - that ends there, back as far as it goes on the one
	// way, at the most `back` before he goes up, and with nothing of his
	// that has him stood on the way.)
	if (pinned(tr.acts, c, moves[k]!.t0, off)) {
		return false;
	}
	let k0 = k;
	while (k0 > 0) {
		const a = moves[k0 - 1]!;
		const b = moves[k0]!;
		const La = len(a);
		const Lc = Math.hypot(C.x - a.from.x, C.y - a.from.y);
		if (
			(o.wait ? a.t1 > b.t0 + 1 : Math.abs(a.t1 - b.t0) > 1) ||
			!(
				b.v0 ||
				(k0 === k && len(b) < CLOSE_RUN_LEAST) ||
				(o.wait && !(a.v1 ?? 0))
			) ||
			La < STOOD ||
			off - a.t0 > o.back ||
			pinned(tr.acts, c, a.t0, b.t0) ||
			Lc < 1e-6 ||
			((a.to.x - a.from.x) * (C.x - a.from.x) +
				(a.to.y - a.from.y) * (C.y - a.from.y)) /
				(La * Lc) <
				CLOSE_ONE_WAY
		) {
			break;
		}
		k0--;
	}
	const m = moves[k0]!;
	if (Math.hypot(C.x - m.from.x, C.y - m.from.y) < CLOSE_RUN_LEAST) {
		return false;
	}
	let j = -1;
	for (let i = ke + 1; i < moves.length; i++) {
		if (len(moves[i]!) >= STOOD) {
			j = i;
			break;
		}
	}
	const n = j >= 0 ? moves[j]! : undefined;
	if (n && n.t0 < down && !o.give) {
		return false;
	}
	const L = Math.hypot(C.x - m.from.x, C.y - m.from.y);
	const ux = (C.x - m.from.x) / L;
	const uy = (C.y - m.from.y) / L;
	const air = (down - off) / 1000;
	// He goes on the way he came - or, coming at the shooter, bends by
	// him as he goes up (no more of a change in the way he is going than
	// TURN_MOST, nor of a turn than this): by him on the side he is off to.
	const S0 = o.avoid[0];
	const side = S0 && ux * (S0.y - C.y) - uy * (S0.x - C.x) > 0 ? -1 : 1;
	const wayAt = (th: number) => {
		const c = Math.cos(th * side);
		const sn = Math.sin(th * side);
		return { x: ux * c - uy * sn, y: uy * c + ux * sn };
	};
	// Where he goes up from, coming at `v` and going on `w`: on the play's
	// spot at `r`.
	const upAt = (v: number, w: { x: number; y: number }) => {
		const s = (v * (off - r)) / 1000;
		return s >= 0
			? { x: C.x + ux * s, y: C.y + uy * s }
			: { x: C.x + w.x * s, y: C.y + w.y * s };
	};
	// Down, he brakes to a stop - or, off on the play's next run of his soon
	// after (to the boards, say), on into it as fast as the turn allows.
	const Pt = (x: number, y: number) => ({ x, y });
	type Plan = {
		w: { x: number; y: number };
		X: { x: number; y: number };
		Y: { x: number; y: number };
		E: { x: number; y: number };
		end: number;
		p: number;
		go: number;
		at: number;
		till: number;
		halt?: { t: number; Q: { x: number; y: number } };
	};
	// (When he sets off on it, to be going `v` as he goes up from `X`.)
	// (Or from `from`, going `v0` then.)
	const goFor = (
		v: number,
		Lin: number,
		from = m.t0,
		v0 = m.v0 ?? 0,
	): number | undefined => {
		const T = (off - from) / 1000;
		if (v <= runShape(Lin, T, v0, 0).vc) {
			return from;
		}
		if (!o.wait || v0 > 0) {
			return undefined;
		}
		let lo = 0;
		let hi = T;
		for (let i = 0; i < 16; i++) {
			const mid = (lo + hi) / 2;
			if (runShape(Lin, mid, 0, 0).vc >= v) {
				lo = mid;
			} else {
				hi = mid;
			}
		}
		return lo > 0 && Lin / lo <= TOP_RUN ? off - lo * 1000 : undefined;
	};
	const plan = (v: number): Plan | undefined => {
		const T = (off - m.t0) / 1000;
		const most = !o.bend
			? 0
			: Math.min(BEND_MOST, 2 * Math.asin(Math.min(1, TURN_MOST / (2 * v))));
		for (let th = 0; th <= most + 1e-9; th += BEND_STEP) {
			const w = wayAt(th);
			const X = upAt(v, w);
			const Lin = Math.hypot(X.x - m.from.x, X.y - m.from.y);
			let go = T > 0 && Lin >= STOOD ? goFor(v, Lin) : undefined;
			// (Coming already - and too near to be going that fast at it
			// on from that: he pulls up first, and goes from there.)
			let halt: Plan["halt"];
			const v0 = m.v0 ?? 0;
			if (go === undefined && o.give && v0 > 0 && T > 0 && Lin >= STOOD) {
				const tb = v0 / LAND_BRAKE;
				const sb = (v0 * tb) / 2;
				if (Lin - sb >= STOOD) {
					const t = m.t0 + tb * 1000;
					go = goFor(v, Lin - sb, t, 0);
					halt = {
						t,
						Q: Pt(
							m.from.x + ((X.x - m.from.x) * sb) / Lin,
							m.from.y + ((X.y - m.from.y) * sb) / Lin,
						),
					};
				}
			}
			if (go === undefined) {
				return undefined;
			}
			const Y = Pt(X.x + w.x * v * air, X.y + w.y * v * air);
			let p = 0;
			let end = down + (1000 * v) / LAND_BRAKE;
			let s = (v * v) / (2 * LAND_BRAKE);
			// (When the next run sets off.)
			let at = n ? n.t0 : Infinity;
			let till = n ? n.t1 : Infinity;
			if (n && (n.t0 - down <= ON_WITHIN || (o.give && end > n.t0))) {
				// (Its way from about where he comes down.)
				const nx = n.to.x - Y.x;
				const ny = n.to.y - Y.y;
				const Ln = Math.hypot(nx, ny);
				const cos = Ln < 1e-6 ? 1 : (w.x * nx + w.y * ny) / Ln;
				let Tb = (n.t0 - down) / 1000;
				p = Math.min(
					v,
					cos > 0.999 ? Infinity : TURN_MOST / Math.sqrt(2 - 2 * cos),
					runShape(Ln, (n.t1 - Math.max(n.t0, down)) / 1000, 0, n.v1 ?? 0).vc ||
						Infinity,
				);
				if (v - p > LAND_BRAKE * Tb) {
					if (!o.give) {
						return undefined;
					}
					// (Off on it once he is down and slowed for it - all of it
					// that much later, if he is only stood after it.)
					Tb = (v - p) / LAND_BRAKE;
					at = down + Tb * 1000;
					const late = at - n.t0;
					const nn = moves[j + 1];
					if (
						(!nn ||
							(len(nn) < STOOD &&
								Math.abs(nn.t0 - n.t1) <= 1 &&
								nn.t1 - nn.t0 > late + STOOD_LEAST)) &&
						!pinned(tr.acts, c, n.t1, n.t1 + late)
					) {
						till = n.t1 + late;
					}
					if (at >= till) {
						return undefined;
					}
				}
				end = at;
				s = ((v + p) / 2) * Tb;
			} else if (n && end > n.t0) {
				return undefined;
			}
			// (And stopped, or off on it, by when something of his wants him
			// stood - a box-out, say.)
			if (end > standBy(tr.acts, down)) {
				return undefined;
			}
			const E = Pt(Y.x + w.x * s, Y.y + w.y * s);
			// (On the floor still - and the play's next run of his, from
			// where he comes to, no harder than it had him make it: or,
			// `give`, set off on from there as soon as he is stood, if that
			// is not, and nothing of his has him stood there till then.)
			const harder = (t0: number, p: number) =>
				n !== undefined &&
				runShape(
					Math.hypot(n.to.x - E.x, n.to.y - E.y),
					(till - t0) / 1000,
					p,
					n.v1 ?? 0,
				).vc >
					runShape(len(n), (n.t1 - n.t0) / 1000, n.v0 ?? 0, n.v1 ?? 0).vc +
						o.harder;
			if (
				n &&
				o.give &&
				p === 0 &&
				end < n.t0 &&
				harder(at, p) &&
				!harder(end, 0) &&
				!pinned(tr.acts, c, end, n.t0)
			) {
				at = end;
			}
			if (
				E.x < IN ||
				E.x > COURT_W - IN ||
				E.y < IN ||
				E.y > COURT_H - IN ||
				harder(at, p)
			) {
				continue;
			}
			if (o.avoid.every((S) => nearest(X, E, S) >= o.near)) {
				return {
					w,
					X,
					Y,
					E,
					end,
					p,
					go,
					at,
					till,
					...(halt ? { halt } : {}),
				};
			}
		}
		return undefined;
	};
	if (!plan(CATCH_LEAST)) {
		return false;
	}
	let lo = CATCH_LEAST;
	let hi = o.most;
	if (plan(hi)) {
		lo = hi;
	} else {
		for (let i = 0; i < 12; i++) {
			const v = (lo + hi) / 2;
			if (plan(v)) {
				lo = v;
			} else {
				hi = v;
			}
		}
	}
	const v = lo;
	const { X, Y, E, end, p, go, at, till, halt } = plan(v)!;
	const face = m.face !== undefined ? { face: m.face } : {};
	// (Pulling up first: on from where he has.)
	const brake: Move[] = halt
		? [
				{
					t0: m.t0,
					t1: halt.t,
					from: { ...m.from },
					to: { ...halt.Q },
					anim: m.anim,
					v0: m.v0 ?? 0,
					...face,
				},
			]
		: [];
	if (halt) {
		m.from = { ...halt.Q };
		m.v0 = undefined;
	}
	m.t0 = go;
	m.to = X;
	m.t1 = off;
	m.v1 = v;
	const on: Move[] = [
		{
			t0: off,
			t1: down,
			from: { ...X },
			to: Y,
			anim: m.anim,
			v0: v,
			v1: v,
			...face,
		},
		{
			t0: down,
			t1: end,
			from: { ...Y },
			to: E,
			anim: m.anim,
			v0: v,
			v1: p,
			...face,
		},
	].filter((q) => !o.give || q.t1 > q.t0);
	// (Stood from then where he stopped - and off from there on whatever
	// the play has him do next.)
	const after: Move[] = [];
	const pulled = n !== undefined && at < n.t0;
	for (let i = ke + 1; i < moves.length; i++) {
		const o = moves[i]!;
		if (o === n) {
			after.push(...moves.slice(i));
			break;
		}
		if (o.t1 > end && !pulled) {
			after.push({
				...o,
				t0: Math.max(o.t0, end),
				from: { ...E },
				to: { ...E },
			});
		}
	}
	if (n) {
		n.from = { ...E };
		n.t0 = at;
		if (till !== n.t1) {
			const nn = moves[j + 1];
			if (nn) {
				nn.t0 += till - n.t1;
			}
			n.t1 = till;
		}
		if (p > 0) {
			n.v0 = p;
		}
	}
	moves.splice(k0 + 1, moves.length - k0 - 1, ...on, ...after);
	moves.splice(k0, 0, ...brake);
	return true;
};

// Every jumper's contest laid on through it, where the play has the man
// stood for it. How many it laid.
export const throughContests = (
	tracks: ReadonlyMap<number, Track>,
	ball: readonly BallSeg[],
): number => {
	let n = 0;
	for (const f of ball) {
		if (f.kind !== "fly" || f.dead || !("pid" in f.from) || "pid" in f.to) {
			continue;
		}
		const shooter = tracks.get(f.from.pid);
		if (!shooter) {
			continue;
		}
		const r = f.t0;
		// (How long he had it: off a quick catch, or held.)
		let j = ball.indexOf(f);
		while (j > 0) {
			const h = ball[j - 1]!;
			if (h.kind !== "hold" || h.pid !== f.from.pid) {
				break;
			}
			j--;
		}
		const quick = r - (ball[j]?.t0 ?? r) < QUICK;
		for (const tr of tracks.values()) {
			if (tr.team === shooter.team) {
				continue;
			}
			const c = tr.acts.find(
				(a) =>
					CONTESTS.has(a.anim) &&
					a.jump !== undefined &&
					a.t0 >= r - 400 &&
					a.t0 <= r + 700,
			);
			if (!c) {
				continue;
			}
			// (Not one going up after it is long gone - a hand up as it goes
			// over him: on his way then, he is coming already.)
			const D = c.t1 - c.t0;
			if (c.t0 + c.jump![0] * D > r + UP_AFTER) {
				continue;
			}
			const S = [spotAt(shooter, r), spotAt(shooter, c.t0 + c.jump![1] * D)];
			const o: UpThrough = {
				avoid: S,
				near: CLOSE_NEAR,
				bend: true,
				most: Math.max(
					CATCH_LEAST,
					quantileAt(
						quick ? CLOSE_PACE_QUICK : CLOSE_PACE_HELD,
						hash01(tr.pid * 61 + 3, r),
					),
				),
				carried: false,
				back: CLOSE_BACK,
				wait: true,
				give: true,
				harder: CLOSE_NEXT_HARDER,
			};
			const angle =
				(quantileAt(
					quick ? CLOSE_ANGLE_QUICK : CLOSE_ANGLE_HELD,
					hash01(tr.pid * 61 + 4, r),
				) *
					Math.PI) /
				180;
			if (upThrough(tr, c, r, o) || lungeThrough(tr, c, r, angle, o)) {
				n++;
			}
		}
	}
	return n;
};

// Stood on the play's spot for his contest `c` (a man holding it a while,
// his man in front of him): stood a step or two further off it, and in
// through it - `angle` off straight at the shooter, from the side he is on
// (or back along the way he came onto it, on the move) - laid on as any
// closeout is (see upThrough). Whether it did.
const lungeThrough = (
	tr: Track,
	c: Act,
	r: number,
	angle: number,
	o: UpThrough,
): boolean => {
	const D = c.t1 - c.t0;
	const off = c.t0 + c.jump![0] * D;
	const C = spotAt(tr, r);
	const S0 = o.avoid[0]!;
	const d0 = Math.hypot(S0.x - C.x, S0.y - C.y);
	if (d0 < 1e-3 || pinned(tr.acts, c, off - LUNGE_STOOD, off)) {
		return false;
	}
	// (Stood on it from his last run there - every move of his from then
	// till he goes up, near it.)
	const moves = tr.moves;
	const near = (P: { x: number; y: number }) =>
		Math.hypot(P.x - C.x, P.y - C.y) <= ON_SPOT;
	let a = -1;
	for (let i = moves.length - 1; i >= 0; i--) {
		const m = moves[i]!;
		if (m.t0 >= off) {
			continue;
		}
		if (m.t1 > off || !near(m.to)) {
			return false;
		}
		if (!near(m.from)) {
			a = i;
			break;
		}
	}
	if (a < 0 || moves[a]!.t1 < off - LUNGE_STOOD) {
		return false;
	}
	// The way he comes in: `angle` off straight at the shooter, from the side
	// of it he came from - or, on his way there still going, the way he
	// was (stood back along it no nearer than he pulls up in).
	const ux = (S0.x - C.x) / d0;
	const uy = (S0.y - C.y) / d0;
	const A = moves[a]!.from;
	const side = ux * (A.y - C.y) - uy * (A.x - C.x) > 0 ? -1 : 1;
	const cs = Math.cos(angle * side);
	const sn = Math.sin(angle * side);
	let wx = ux * cs - uy * sn;
	let wy = uy * cs + ux * sn;
	let L = Math.min(
		LUNGE_MOST,
		Math.max(LUNGE_LEAST, (o.most * o.most) / (2 * LUNGE_ACC) + 0.5),
	);
	const va = moves[a]!.v0 ?? 0;
	if (va > LUNGE_STILL) {
		const dA = Math.hypot(C.x - A.x, C.y - A.y);
		L = Math.min(L, dA - (va * va) / (2 * LAND_BRAKE));
		if (L < LUNGE_LEAST) {
			return false;
		}
		wx = (C.x - A.x) / dA;
		wy = (C.y - A.y) / dA;
	}
	const P = { x: C.x - wx * L, y: C.y - wy * L };
	if (P.x < IN || P.x > COURT_W - IN || P.y < IN || P.y > COURT_H - IN) {
		return false;
	}
	const was = moves.map((m) => ({ ...m }));
	// Stood there instead, from his run there on - and off in, set to go
	// as late as upThrough has him go.
	const arrive = moves[a]!;
	arrive.to = { ...P };
	arrive.v1 = undefined;
	const stood: Move[] = [];
	for (let i = a + 1; i < moves.length && moves[i]!.t0 < off; i++) {
		stood.push({ ...moves[i]!, from: { ...P }, to: { ...P } });
	}
	const rest = moves.slice(a + 1 + stood.length);
	const t0 = Math.max(arrive.t1, off - 2 * (L / Math.max(o.most, 1)) * 1000);
	const lunge: Move = {
		t0,
		t1: Math.max(t0 + 1, off - 100),
		from: { ...P },
		to: { ...C },
		anim: "closeout",
		...(arrive.face !== undefined ? { face: arrive.face } : {}),
	};
	moves.splice(
		a + 1,
		moves.length - a - 1,
		...stood
			.filter((m) => m.t0 < lunge.t0)
			.map((m) => ({ ...m, t1: Math.min(m.t1, lunge.t0) })),
		lunge,
		...rest,
	);
	if (upThrough(tr, c, r, o)) {
		return true;
	}
	moves.splice(0, moves.length, ...was);
	return false;
};

// REBOUNDS. A real man who comes down with a rebound is on the move through
// it - 4.5 to 5 ft/s in the second before he has it and as he does, median,
// over the 17 games; stood for half a second or more before it only one
// time in twelve. The play laid his run to the spot he takes it at as one
// that ended there - pulled up in the air - and his jump straight up from
// it. Laid on through it instead: on the play's spot as the ball comes to
// his hands (the top of his jump), still going, off the floor going and on
// at that pace in the air, braking once he is down, or on into what he
// does next. His way in is the play's run there with the shuffle onto the
// spot at the end of it, and any stop on the way; how fast he goes up is
// drawn from the real spread, and from stood he sets off as late as gets
// him there that fast - not a walk all the way.

const BOARDS = new Set<AnimName>(["board", "rebound"]);
// How fast real men are going as they come down with it (feet a second):
// its tenth, quarter, half, three quarters and nine tenths, at their own
// end and at the other - each man's drawn from it, going up for it no
// faster than his. His way in from no longer than this (ms) before he goes
// up.
const BOARD_DEF = [1.5, 2.5, 4.5, 6.7, 8.9];
const BOARD_OFF = [1.9, 3.1, 5.8, 9.5, 12.8];
const BOARD_BACK = 2500;

// Every rebound laid on through it, where the play has the man stood for
// it. How many it laid.
export const throughBoards = (
	tracks: ReadonlyMap<number, Track>,
	ball: readonly BallSeg[],
): number => {
	let n = 0;
	for (const tr of tracks.values()) {
		for (const a of tr.acts) {
			if (!BOARDS.has(a.anim) || !a.jump) {
				continue;
			}
			const top = a.t0 + ((a.jump[0] + a.jump[1]) / 2) * (a.t1 - a.t0);
			// (At their own end: the shot he goes up for was theirs.)
			const shot = ball.findLast(
				(b) => b.kind === "fly" && "pid" in b.from && b.t0 < top - 300,
			);
			const shooter =
				shot?.kind === "fly" && "pid" in shot.from
					? tracks.get(shot.from.pid)
					: undefined;
			const most = quantileAt(
				shooter?.team === tr.team ? BOARD_OFF : BOARD_DEF,
				hash01(tr.pid * 53 + 1, top),
			);
			if (
				upThrough(tr, a, top, {
					avoid: [],
					near: 0,
					bend: false,
					most,
					carried: true,
					back: BOARD_BACK,
					wait: true,
					give: false,
					harder: NEXT_HARDER,
				})
			) {
				n++;
			}
		}
	}
	return n;
};
