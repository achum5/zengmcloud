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
// 7.9 at 5.1 ft off a quick catch - and carry on in after it, up into the
// air with it and down beyond. The play laid his closeout as a run that
// ended on its spot (the room it gives the shooter), stood, and his hand
// up from there. Laid on through it instead: on his spot as it leaves the
// shooter's hands, as before, but still coming - off the floor going (on
// at that pace in the air, as a man must), braking once he is down - and
// no faster than leaves him stopped short of the shooter.

const CONTESTS = new Set<AnimName>(["contest", "contestTwo", "contestFace"]);
// Coming in at most this fast (feet a second) - not worth it slower than
// CATCH_LEAST - stopped no nearer the shooter than this (feet, middle to
// middle) braking this hard once he is down (feet a second, a second).
const CLOSE_MOST = 8;
const CLOSE_NEAR = 2;
const LAND_BRAKE = 15;
// A run in of at least this far (feet) - his choppy steps in, say - ended
// no more than ARRIVED_WITHIN before he goes up, or this long (ms) after.
const CLOSE_RUN_LEAST = 1;
const CLOSE_LATE = 50;
const UP_AFTER = 150;
const CLOSE_BACK = 1600;
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

// Lays his run into the contest `c` of a jumper that leaves the shooter's
// hands at `r` on through it, if the play has him stood for it. Whether it
// did. (`shooter`: where the shooter is as it goes, and as he comes down.)
export const closeThrough = (
	tr: Track,
	c: Act,
	r: number,
	shooter: readonly { x: number; y: number }[],
): boolean => {
	if (!c.jump) {
		return false;
	}
	const D = c.t1 - c.t0;
	const off = c.t0 + c.jump[0] * D;
	const down = c.t0 + c.jump[1] * D;
	const moves = tr.moves;
	let k = -1;
	for (let i = moves.length - 1; i >= 0; i--) {
		const m = moves[i]!;
		if (m.t0 >= off) {
			continue;
		}
		const L = len(m);
		if (L < STOOD) {
			continue;
		}
		// (Not one going up after it is long gone - a hand up as it goes
		// over him: on his way then, he is coming already.)
		if (
			!(m.v1 ?? 0) &&
			off <= r + UP_AFTER &&
			m.t1 >= off - ARRIVED_WITHIN &&
			m.t1 <= off + CLOSE_LATE
		) {
			k = i;
		}
		break;
	}
	if (k < 0) {
		return false;
	}
	// (His way in: the run - choppy steps and all - that ends there, back
	// as far as it goes on the one way, at the most CLOSE_BACK before he
	// goes up.)
	const C = moves[k]!.to;
	let k0 = k;
	while (k0 > 0) {
		const a = moves[k0 - 1]!;
		const b = moves[k0]!;
		const La = len(a);
		const Lc = Math.hypot(C.x - a.from.x, C.y - a.from.y);
		if (
			Math.abs(a.t1 - b.t0) > 1 ||
			!(b.v0 ?? 0) ||
			La < STOOD ||
			off - a.t0 > CLOSE_BACK ||
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
	for (let i = k + 1; i < moves.length; i++) {
		if (len(moves[i]!) >= STOOD) {
			j = i;
			break;
		}
	}
	const n = j >= 0 ? moves[j]! : undefined;
	if (n && n.t0 < down) {
		return false;
	}
	const L = Math.hypot(C.x - m.from.x, C.y - m.from.y);
	const ux = (C.x - m.from.x) / L;
	const uy = (C.y - m.from.y) / L;
	const air = (down - off) / 1000;
	// He goes on the way he came - or, coming at the shooter, bends by
	// him as he goes up (no more of a change in the way he is going than
	// TURN_MOST, nor of a turn than this): by him on the side he is off to.
	const S0 = shooter[0]!;
	const side = ux * (S0.y - C.y) - uy * (S0.x - C.x) > 0 ? -1 : 1;
	const wayAt = (th: number) => {
		const c = Math.cos(th * side);
		const sn = Math.sin(th * side);
		return { x: ux * c - uy * sn, y: uy * c + ux * sn };
	};
	// Where he goes up from, coming at `v` and going on `w`: on the play's
	// spot as it leaves the shooter's hands.
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
	};
	const plan = (v: number): Plan | undefined => {
		const T = (off - m.t0) / 1000;
		const most = Math.min(
			BEND_MOST,
			2 * Math.asin(Math.min(1, TURN_MOST / (2 * v))),
		);
		for (let th = 0; th <= most + 1e-9; th += BEND_STEP) {
			const w = wayAt(th);
			const X = upAt(v, w);
			const Lin = Math.hypot(X.x - m.from.x, X.y - m.from.y);
			if (T <= 0 || Lin < STOOD || v > runShape(Lin, T, m.v0 ?? 0, 0).vc) {
				return undefined;
			}
			const Y = Pt(X.x + w.x * v * air, X.y + w.y * v * air);
			let p = 0;
			let end = down + (1000 * v) / LAND_BRAKE;
			let s = (v * v) / (2 * LAND_BRAKE);
			if (n && n.t0 - down <= ON_WITHIN) {
				// (Its way from about where he comes down.)
				const nx = n.to.x - Y.x;
				const ny = n.to.y - Y.y;
				const Ln = Math.hypot(nx, ny);
				const cos = Ln < 1e-6 ? 1 : (w.x * nx + w.y * ny) / Ln;
				const Tb = (n.t0 - down) / 1000;
				p = Math.min(
					v,
					cos > 0.999 ? Infinity : TURN_MOST / Math.sqrt(2 - 2 * cos),
					runShape(Ln, (n.t1 - n.t0) / 1000, 0, n.v1 ?? 0).vc || Infinity,
				);
				if (v - p > LAND_BRAKE * Tb) {
					return undefined;
				}
				end = n.t0;
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
			// where he comes to, no harder than it had him make it.)
			if (
				E.x < IN ||
				E.x > COURT_W - IN ||
				E.y < IN ||
				E.y > COURT_H - IN ||
				(n &&
					runShape(
						Math.hypot(n.to.x - E.x, n.to.y - E.y),
						(n.t1 - n.t0) / 1000,
						p,
						n.v1 ?? 0,
					).vc >
						runShape(len(n), (n.t1 - n.t0) / 1000, n.v0 ?? 0, n.v1 ?? 0).vc +
							NEXT_HARDER)
			) {
				continue;
			}
			if (shooter.every((S) => nearest(X, E, S) >= CLOSE_NEAR)) {
				return { w, X, Y, E, end, p };
			}
		}
		return undefined;
	};
	if (!plan(CATCH_LEAST)) {
		return false;
	}
	let lo = CATCH_LEAST;
	let hi = CLOSE_MOST;
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
	const { X, Y, E, end, p } = plan(v)!;
	const face = m.face !== undefined ? { face: m.face } : {};
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
	];
	// (Stood from then where he stopped - and off from there on whatever
	// the play has him do next.)
	const after: Move[] = [];
	for (let i = k + 1; i < moves.length; i++) {
		const o = moves[i]!;
		if (o === n) {
			after.push(...moves.slice(i));
			break;
		}
		if (o.t1 > end) {
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
		if (p > 0) {
			n.v0 = p;
		}
	}
	moves.splice(k0 + 1, moves.length - k0 - 1, ...on, ...after);
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
			const D = c.t1 - c.t0;
			const S = [spotAt(shooter, r), spotAt(shooter, c.t0 + c.jump![1] * D)];
			if (closeThrough(tr, c, r, S)) {
				n++;
			}
		}
	}
	return n;
};
