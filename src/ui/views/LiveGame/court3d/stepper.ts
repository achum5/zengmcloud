// THE TEN ON THE FLOOR, STEPPED (see tools/court3d-tape/SIM.md).
//
// The director lays out the game - every line acted out, every man's runs
// and what he does on them. This takes that, and moves every man the way a
// man moves: a step at a time (STEP_MS), no quicker to get going, turn or
// pull up than a man can (see motion.ts), following where the play wants
// him - and exactly where it has him whenever he is doing something there
// (an act: a shot, a pass, a catch, a screen...), so every moment of the
// play is where and when it was. Out come the same tracks the court reads.
//
// This first step only follows the director's own runs: the picture is the
// one it laid out, less what no man could do (a lurch, a corner turned on
// the spot at a run). What each man wants comes next, a part at a time.

import { compileCourt, type CourtTimeline, type Move } from "./director.ts";
import { floorSpotOf } from "./evaluate.ts";
import { rimX, type Pt } from "./geometry.ts";
import { pushFrom } from "./motion.ts";

export const STEP_MS = 50;

const unitVec = (a: Pt, b: Pt): Pt => {
	const d = Math.hypot(b.x - a.x, b.y - a.y) || 1;
	return { x: (b.x - a.x) / d, y: (b.y - a.y) / d };
};

// A number from 0 to 1 from two others, the same every time (every device
// stepping the same game the same way).
const hash01 = (a: number, b: number): number => {
	let h = Math.imul(a | 0, 0x9e3779b1) ^ Math.imul(b | 0, 0x85ebca77);
	h = Math.imul(h ^ (h >>> 15), 0x2c1b3c6d);
	h = Math.imul(h ^ (h >>> 12), 0x297a2d39);
	return ((h ^ (h >>> 15)) >>> 0) / 4294967296;
};
// How hard he can pull up or turn (feet a second, a second) - the same
// braking motion.ts has him pulling up with, a touch over.
const BRAKE = 24;
const TURN = 26;
// How far ahead (s) he steers at where the play will have him - the time it
// takes him to pull up, within these - and how much faster (feet a second)
// than the play is going he goes to catch up with it.
const LOOK_LEAST = 0.2;
const LOOK_MOST = 1.2;
const CATCH_UP = 5;
// Doing something where the play has him (an act), he is eased onto it
// over this long before (ms).
const ONTO_MS = 250;
// ... and easing up into it over as long as this before (ms), pulling up
// no harder than this (feet a second, a second).
const ARRIVE_MS = 1500;
const EASE_UP = 14;
// Waiting a beat to react, only with this long (ms) still to spare before
// something of his.
const REACT_ROOM = 1500;
// How long a man takes to react, setting off (ms): from one end to the
// other, his own each time.
const REACT_MS: [number, number] = [60, 320];
const REACT_PACE = 8;
// Fastest anybody goes (feet a second).
const TOP = 30;
// Further off where the play wants him than this (feet), something has
// gone wrong (a cut, a man brought on): he is just there.
const JUMP = 6;
// A path laid down as straight runs where it bends no more than this
// (feet).
const KNOT_TOL = 0.12;
const KNOT_PINNED = 0.02;
// ... each no longer than this (ms), its pace not changing back and forth
// by more than this (feet a second) along it.
const RUN_MOST = 1500;
const PACE_NOISE = 0.25;
// Under this pace (feet a second) he is stood.
const STILL = 0.2;

type Sample = { t: number; x: number; y: number; vx: number; vy: number };

// When something of his is going on - his acts, the picture's cuts - as
// stretches of time.
type Spans = {
	// Whether any of them is going on between t0 and t1.
	hit: (t0: number, t1: number) => boolean;
	// When the next one begins, from t on.
	next: (t: number) => number;
};
const spansAt = (list: (readonly [number, number])[]): Spans => {
	// (One list of stretches, run together where they meet - each found by
	// halving.)
	const spans: [number, number][] = [];
	for (const [a, b] of [...list].sort((x, y) => x[0] - y[0])) {
		const last = spans.at(-1);
		if (last && a <= last[1]) {
			last[1] = Math.max(last[1], b);
		} else {
			spans.push([a, b]);
		}
	}
	// (The first beginning at t or after.)
	const from = (t: number) => {
		let lo = 0;
		let hi = spans.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (spans[mid]![0] < t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		return lo;
	};
	return {
		hit: (t0, t1) => {
			const k = from(t1) - 1;
			return k >= 0 && spans[k]![1] > t0;
		},
		next: (t) => spans[from(t)]?.[0] ?? Infinity,
	};
};

// One man, stepped: where he is every STEP_MS from t0 to t1, following
// `want`: as near on it as he can be whenever `held`, and on it outright
// whenever the picture `cut`s.
export const follow = (
	want: (t: number) => Pt | undefined,
	t0: number,
	t1: number,
	// Doing something there: on it, eased onto it over the moment before.
	held: Spans,
	// The picture cutting: wherever the play has him, then and there.
	cut: Spans = spansAt([]),
	// How long he takes to react (ms), setting off from standing at t: each
	// man in his own time, not all of them at once.
	lagAt: (t: number) => number = () => 0,
): Sample[] => {
	const out: Sample[] = [];
	let p: Pt | undefined;
	let vx = 0;
	let vy = 0;
	// (When the play set him off from standing, and how long he takes.)
	let off: { t: number; lag: number } | undefined;
	// (Where the play had him the step before.)
	let was: Pt | undefined;
	const dt = STEP_MS / 1000;
	// (Where he is wanted next step, kept for the step after.)
	let ahead: Pt | undefined = want(t0);
	for (let t = t0; t <= t1; t += STEP_MS) {
		const W = ahead;
		ahead = want(t + STEP_MS);
		if (!W) {
			p = undefined;
			was = undefined;
			continue;
		}
		const W1 = ahead ?? W;
		const wx = (W1.x - W.x) / dt;
		const wy = (W1.y - W.y) / dt;
		if (
			!p ||
			!was ||
			held.hit(t - STEP_MS, t) ||
			cut.hit(t - STEP_MS, t) ||
			Math.hypot(W.x - was.x, W.y - was.y) > JUMP
		) {
			was = W;
			// (Where the play has him, as fast as it has him going.)
			p = { ...W };
			vx = wx;
			vy = wy;
			out.push({ t, x: p.x, y: p.y, vx, vy });
			continue;
		}
		// Where he wants to be going: at where the play will have him a
		// moment from now - as far ahead as it takes him to pull up at the
		// pace he is going, so a turn or a stop coming is one he sees coming
		// - and no faster than a little quicker than the play has him going.
		const v0 = Math.hypot(vx, vy);
		// (Stood, and the play not yet sending him anywhere, he waits for it:
		// no setting off ahead of it.)
		const look =
			v0 < 1 && Math.hypot(wx, wy) < 0.5
				? 0
				: Math.max(LOOK_LEAST, Math.min(LOOK_MOST, (v0 / BRAKE) * 0.9));
		const L = look > 0 ? (want(t + look * 1000) ?? W) : W;
		const L1 = look > 0 ? (want(t + look * 1000 + STEP_MS) ?? L) : (ahead ?? W);
		const reach = Math.max(look, LOOK_LEAST);
		let cx = (L.x - p.x) / reach + (look > 0 ? 0 : wx);
		let cy = (L.y - p.y) / reach + (look > 0 ? 0 : wy);
		{
			const lv = Math.hypot(L1.x - L.x, L1.y - L.y) / dt;
			const most = Math.max(lv, Math.hypot(wx, wy)) + CATCH_UP;
			const cv = Math.hypot(cx, cy);
			if (cv > most) {
				cx *= most / cv;
				cy *= most / cv;
			}
		}
		// Stood, and the play sets him off: a beat to react first - his own
		// - unless something of his is coming up soon that he has to be in
		// time for.
		const due0 = held.next(t);
		if (Math.hypot(vx, vy) < 1 && Math.hypot(wx, wy) > 2) {
			if (!off) {
				// (Sent off flat out, he goes the quicker: his beat to react
				// shorter the faster the play has him going.)
				const lag = lagAt(t) * Math.min(1, REACT_PACE / Math.hypot(wx, wy));
				off = due0 - t > lag + REACT_ROOM ? { t, lag } : { t, lag: 0 };
			}
		} else if (Math.hypot(wx, wy) < 0.5) {
			off = undefined;
		}
		if (off && t - off.t < off.lag) {
			cx = 0;
			cy = 0;
		}
		// Something of his coming up where the play has him: he sees it
		// coming and is easing up into it in time - no faster, with the room
		// left, than he can comfortably pull up to the pace he does it at.
		let cap = TOP;
		const due = held.next(t);
		if (due - t < ARRIVE_MS) {
			const Q = want(due);
			const Q1 = want(due + STEP_MS);
			if (Q && Q1) {
				const qv = Math.hypot(Q1.x - Q.x, Q1.y - Q.y) / dt;
				cap = Math.min(
					cap,
					qv + Math.sqrt(2 * EASE_UP * Math.hypot(Q.x - p.x, Q.y - p.y)),
				);
			}
		}
		const c = Math.hypot(cx, cy);
		if (c > cap) {
			cx *= cap / c;
			cy *= cap / c;
		}
		// No quicker than his legs: speeding up as hard as he still can at
		// the pace he is going, pulling up and turning as hard as a man can.
		let ax = (cx - vx) / dt;
		let ay = (cy - vy) / dt;
		const v = Math.hypot(vx, vy);
		const ux = v > 1e-6 ? vx / v : 0;
		const uy = v > 1e-6 ? vy / v : 0;
		let along = v > 1e-6 ? ax * ux + ay * uy : Math.hypot(ax, ay);
		let side = v > 1e-6 ? -ax * uy + ay * ux : 0;
		const fx =
			v > 1e-6 ? ux : Math.hypot(ax, ay) > 1e-9 ? ax / Math.hypot(ax, ay) : 0;
		const fy =
			v > 1e-6 ? uy : Math.hypot(ax, ay) > 1e-9 ? ay / Math.hypot(ax, ay) : 0;
		along = Math.max(-BRAKE, Math.min(pushFrom(v), along));
		side = Math.max(-TURN, Math.min(TURN, side));
		ax = fx * along - fy * side;
		ay = fy * along + fx * side;
		vx += ax * dt;
		vy += ay * dt;
		p = { x: p.x + vx * dt, y: p.y + vy * dt };
		// Something of his coming up: eased onto where the play has him for
		// it, there as it starts.
		if (due - t < ONTO_MS) {
			const u = 1 - Math.max(0, due - t) / ONTO_MS;
			const k = u * u * (3 - 2 * u);
			p = { x: p.x + (W.x - p.x) * k, y: p.y + (W.y - p.y) * k };
			vx += (wx - vx) * k;
			vy += (wy - vy) * k;
		}
		was = W;
		out.push({ t, x: p.x, y: p.y, vx, vy });
	}
	return out;
};

// The samples as runs: a knot wherever the way bends, or his pace changes,
// more than a hair - each run with his pace at either end of it.
export const toMoves = (
	s: Sample[],
	animAt: (t: number) => Pick<Move, "anim" | "face"> | undefined,
	// (Where the play has him doing something: laid down to the step.)
	pinned: Spans = spansAt([]),
): Move[] => {
	const moves: Move[] = [];
	const pace = (q: Sample) => Math.hypot(q.vx, q.vy);
	// (Stood somewhere else than his last run left him - back on the floor,
	// the picture cut: put there, a run that goes nowhere.)
	const place = (q: Sample) => {
		const last = moves.at(-1);
		if (!last || Math.hypot(last.to.x - q.x, last.to.y - q.y) > 0.05) {
			moves.push({
				t0: q.t,
				t1: q.t + 1,
				from: { x: q.x, y: q.y },
				to: { x: q.x, y: q.y },
				anim: animAt(q.t)?.anim ?? "walk",
				v0: 0,
				v1: 0,
			});
		}
	};
	let i = 0;
	while (i < s.length - 1) {
		// (Stood: no run.)
		if (
			pace(s[i]!) < STILL &&
			Math.hypot(s[i + 1]!.x - s[i]!.x, s[i + 1]!.y - s[i]!.y) < 0.01
		) {
			place(s[i]!);
			i += 1;
			continue;
		}
		// As far on as one straight run covers it: the way bending no more
		// than a hair off it, his pace only getting quicker or only slower
		// along it (how the court lays a run out - see runShape), and none
		// too long.
		// (Not across a jump - the picture cutting: he is just there.)
		if (
			Math.hypot(s[i + 1]!.x - s[i]!.x, s[i + 1]!.y - s[i]!.y) /
				((s[i + 1]!.t - s[i]!.t) / 1000) >
			TOP * 1.2
		) {
			i += 1;
			place(s[i]!);
			continue;
		}
		let j = i + 1;
		let trend = 0;
		while (
			j + 1 < s.length &&
			s[j + 1]!.t - s[j]!.t <= STEP_MS + 1 &&
			s[j + 1]!.t - s[i]!.t <= RUN_MOST
		) {
			const a = s[i]!;
			const b = s[j + 1]!;
			const L = Math.hypot(b.x - a.x, b.y - a.y);
			let ok =
				pace(b) >= STILL &&
				Math.hypot(b.x - s[j]!.x, b.y - s[j]!.y) / ((b.t - s[j]!.t) / 1000) <=
					TOP * 1.2;
			const tol = pinned.hit(s[i]!.t, b.t) ? KNOT_PINNED : KNOT_TOL;
			// (Off the line, or along it ahead of or behind where a pace
			// changing evenly from one end's to the other's would have him.)
			const T = (b.t - a.t) / 1000;
			const v0 = pace(a);
			const v1 = pace(b);
			const mean = ((v0 + v1) / 2) * T;
			for (let k = i + 1; k <= j && ok && L > 1e-6; k++) {
				const q = s[k]!;
				const tau = (q.t - a.t) / 1000;
				const along =
					((q.x - a.x) * (b.x - a.x) + (q.y - a.y) * (b.y - a.y)) / L;
				const model =
					mean > 1e-6
						? (L * (v0 * tau + ((v1 - v0) / T) * tau * tau * 0.5)) / mean
						: L * (tau / T);
				ok =
					Math.abs(
						((q.x - a.x) * (b.y - a.y) - (q.y - a.y) * (b.x - a.x)) / L,
					) < tol && Math.abs(along - model) < tol;
			}
			const dv = pace(b) - pace(s[j]!);
			if (Math.abs(dv) > PACE_NOISE) {
				const sign = Math.sign(dv);
				ok &&= trend === 0 || trend === sign;
				trend = trend || sign;
			}
			if (!ok) {
				break;
			}
			j += 1;
		}
		const a = s[i]!;
		const b = s[j]!;
		if (b.t - a.t > STEP_MS + 1 || Math.hypot(b.x - a.x, b.y - a.y) >= 0.01) {
			const mid = animAt((a.t + b.t) / 2);
			const v = Math.hypot(b.x - a.x, b.y - a.y) / ((b.t - a.t) / 1000);
			moves.push({
				t0: a.t,
				t1: b.t,
				from: { x: a.x, y: a.y },
				to: { x: b.x, y: b.y },
				anim: mid?.anim ?? (v < 5 ? "walk" : v < 10 ? "jog" : "run"),
				...(mid?.face === undefined ? {} : { face: mid.face }),
				// (From standing, or pulled up to stand: said so - the court
				// would otherwise run the pause between two runs into one.)
				v0: pace(a) < STILL ? 0 : pace(a),
				v1: pace(b) < STILL ? 0 : pace(b),
			});
		}
		i = j;
	}
	return moves;
};

// Where a man is, step by step over a stretch of time: as a lookup between
// the steps.
type Path = {
	from: number;
	spots: (Pt | undefined)[];
	at: (t: number) => Pt | undefined;
};
const pathOf = (from: number, spots: (Pt | undefined)[]): Path => ({
	from,
	spots,
	at: (t) => {
		const f = (t - from) / STEP_MS;
		const k = Math.floor(f);
		const a = spots[k];
		const b = spots[k + 1];
		if (!a || !b) {
			return a;
		}
		const u = f - k;
		return { x: a.x + (b.x - a.x) * u, y: a.y + (b.y - a.y) * u };
	},
});

// THE MAN WITH THE BALL, WORKING HIS MAN. Stood on his dribble out on the
// floor a while with nothing of his to do - waiting on a screen, on the set
// - he is never planted: he sways across his man, jabs a step in at him and
// back, gives ground a step and comes again - each move his own, a beat
// each - eased in and out of, so he is back where the play wants him for
// whatever comes next. (His man goes with him: see guardOf.)
const PROBE_LEAST = 1200;
const PROBE_EASE = 400;
const PROBE_BEAT: [number, number] = [700, 1100];
// (Across his man, in at him and back - feet, his own way of it.)
const PROBE_MOVES: [number, number][] = [
	[1.2, 0],
	[-1.2, 0],
	[0.3, 1.4],
	[-0.3, 1.4],
	[0, -1.2],
	[0.9, 0.7],
	[-0.9, 0.7],
];
const probesOf = (
	tl: CourtTimeline,
	pid: number,
	laid: Path,
	held: Spans,
	team: 0 | 1,
): ((t: number) => Pt) => {
	const rim = { x: rimX(team), y: 25 };
	// When he has it on his dribble.
	const dribbling: [number, number][] = [];
	tl.ball.forEach((s, k) => {
		if (s.kind === "hold" && s.pid === pid && s.style !== "hold") {
			dribbling.push([s.t0, tl.ball[k + 1]?.t0 ?? s.t0]);
		}
	});
	const on = spansAt(dribbling);
	// Each stretch of it he stands, nothing of his on: his moves through it.
	const stretches: { a: number; b: number; keys: [number, Pt][]; u: Pt }[] = [];
	let a = -1;
	const close = (b: number) => {
		if (a >= 0 && b - a >= PROBE_LEAST) {
			const P = laid.at(a)!;
			const u = unitVec(P, rim);
			const keys: [number, Pt][] = [[a, { x: 0, y: 0 }]];
			let t = a + PROBE_EASE;
			let n = 0;
			while (t < b - PROBE_EASE - 500) {
				const r = hash01(pid * 31 + n, Math.round(t / 50));
				const beat = PROBE_BEAT[0] + (PROBE_BEAT[1] - PROBE_BEAT[0]) * r;
				const [across, inAt] =
					PROBE_MOVES[
						Math.floor(hash01(pid * 7 + n, Math.round(a)) * PROBE_MOVES.length)
					]!;
				keys.push([
					t,
					{
						x: -u.y * across + u.x * inAt,
						y: u.x * across + u.y * inAt,
					},
				]);
				t += beat;
				n += 1;
			}
			keys.push([b, { x: 0, y: 0 }]);
			if (keys.length > 2) {
				stretches.push({ a, b, keys, u });
			}
		}
		a = -1;
	};
	const begin = laid.from;
	for (let k = 0; k + 1 < laid.spots.length; k++) {
		const t = begin + k * STEP_MS;
		const P = laid.spots[k];
		const Q = laid.spots[k + 1];
		const still =
			P !== undefined &&
			Q !== undefined &&
			Math.hypot(Q.x - P.x, Q.y - P.y) / (STEP_MS / 1000) < 0.7 &&
			on.hit(t, t + STEP_MS) &&
			!held.hit(t - 300, t + STEP_MS + 300) &&
			Math.hypot(P.x - rim.x, P.y - rim.y) < 30;
		if (still && a < 0) {
			a = t;
		} else if (!still) {
			close(t);
		}
	}
	close(begin + laid.spots.length * STEP_MS);
	const spans = spansAt(stretches.map((x) => [x.a, x.b] as const));
	return (t) => {
		if (!spans.hit(t, t + 1)) {
			return { x: 0, y: 0 };
		}
		let lo = 0;
		let hi = stretches.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (stretches[mid]!.a <= t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		const st = stretches[lo - 1];
		if (st && !(t < st.b)) {
			return { x: 0, y: 0 };
		}
		if (!st) {
			return { x: 0, y: 0 };
		}
		const k = st.keys.findIndex(([at]) => at > t);
		const [t0, A] = st.keys[k - 1]!;
		const [t1, B] = st.keys[k]!;
		const u = (t - t0) / (t1 - t0);
		const e = u * u * (3 - 2 * u);
		return { x: A.x + (B.x - A.x) * e, y: A.y + (B.y - A.y) * e };
	};
};

// HIS MAN WITH HIM. A defender up on a man goes where that man really goes
// - not only where the play laid him out - a beat behind him: the man
// he guards the nearest of the other side to where the play has him, all
// of it the nearer they are (a step back from it in help, none of it off
// him).
const GUARD_FULL = 7;
const GUARD_NONE = 12;
const GUARD_LAG: [number, number] = [160, 280];

// The whole game stepped: every man's tracks re-laid as he would really
// move them. (In place.)
export const stepTimeline = (tl: CourtTimeline): CourtTimeline => {
	const cut = spansAt(tl.cuts.map((c) => [c - STEP_MS, c] as const));
	const men = [...tl.tracks.values()].filter(
		(tr) => tr.pid >= 0 && tr.moves.length > 0,
	);
	// Where the play has each man, step by step.
	const laid = new Map<number, Path>();
	const helds = new Map<number, Spans>();
	for (const tr of men) {
		const from = Math.max(0, tr.moves[0]!.t0 - 1000);
		const until = tr.moves.at(-1)!.t1 + 1000 + LOOK_MOST * 1000;
		const spots: (Pt | undefined)[] = [];
		for (let t = from; t <= until; t += STEP_MS) {
			spots.push(floorSpotOf(tr, t));
		}
		laid.set(tr.pid, pathOf(from, spots));
		helds.set(tr.pid, spansAt(tr.acts.map((x) => [x.t0, x.t1] as const)));
	}
	// Who has the ball, from when.
	const offenseAt = (t: number): 0 | 1 => {
		let lo = 0;
		let hi = tl.poss.length;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if (tl.poss[mid]![0] <= t) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		return lo > 0 ? tl.poss[lo - 1]![1] : 1;
	};
	// Each man stepped after what he wants, over his stretches on the floor.
	const step = (
		tr: (typeof men)[number],
		want: (t: number) => Pt | undefined,
	) => {
		const L = laid.get(tr.pid)!;
		const held = helds.get(tr.pid)!;
		const samples: Sample[] = [];
		const begin = L.from;
		const end = tr.moves.at(-1)!.t1 + 1000;
		tr.shown.forEach(([at, on], k) => {
			const from = Math.max(begin, at);
			const until = Math.min(end, tr.shown[k + 1]?.[0] ?? end);
			if (!on || !(until > from)) {
				return;
			}
			for (const q of follow(
				want,
				from,
				until - 1,
				held,
				cut,
				(t) =>
					REACT_MS[0] +
					(REACT_MS[1] - REACT_MS[0]) *
						hash01(tr.pid * 17 + 3, Math.round(t / 100)),
			)) {
				samples.push(q);
			}
		});
		return samples;
	};
	// Each with the ball working his own way (see probesOf).
	const probes = new Map<number, (t: number) => Pt>();
	for (const tr of men) {
		probes.set(
			tr.pid,
			probesOf(tl, tr.pid, laid.get(tr.pid)!, helds.get(tr.pid)!, tr.team),
		);
	}
	// And on defense, each with his man as his man really goes.
	for (const tr of men) {
		const L = laid.get(tr.pid)!;
		const probe = probes.get(tr.pid)!;
		const lag =
			GUARD_LAG[0] + (GUARD_LAG[1] - GUARD_LAG[0]) * hash01(tr.pid * 13 + 5, 1);
		const wantNow = (t: number) => {
			const P = L.at(t);
			if (!P) {
				return P;
			}
			if (offenseAt(t) === tr.team) {
				const o = probe(t);
				return { x: P.x + o.x, y: P.y + o.y };
			}
			let man: number | undefined;
			let d = GUARD_NONE;
			for (const o of men) {
				if (o.team === tr.team) {
					continue;
				}
				const Q = laid.get(o.pid)!.at(t);
				const g = Q ? Math.hypot(Q.x - P.x, Q.y - P.y) : Infinity;
				if (g < d) {
					d = g;
					man = o.pid;
				}
			}
			if (man === undefined) {
				return P;
			}
			const went = probes.get(man)!(t - lag);
			const w = Math.min(
				1,
				Math.max(0, (GUARD_NONE - d) / (GUARD_NONE - GUARD_FULL)),
			);
			return { x: P.x + went.x * w, y: P.y + went.y * w };
		};
		const want = pathOf(
			L.from,
			L.spots.map((_, k) => wantNow(L.from + k * STEP_MS)),
		).at;
		const samples = step(tr, want);
		const old = tr.moves;
		const animAt = (t: number) => {
			let lo = 0;
			let hi = old.length;
			while (lo < hi) {
				const mid = (lo + hi) >> 1;
				if (old[mid]!.t0 <= t) {
					lo = mid + 1;
				} else {
					hi = mid;
				}
			}
			const m = old[lo - 1];
			return m && t <= m.t1 + STEP_MS ? m : undefined;
		};
		// (Off the floor, where it had him: the runs it laid there stay.)
		const off = old.filter((m) => !floorSpotOf(tr, (m.t0 + m.t1) / 2));
		tr.moves = [...toMoves(samples, animAt, helds.get(tr.pid)!), ...off].sort(
			(a, b) => a.t0 - b.t0,
		);
	}
	for (const tr of men) {
		delete tr.nudges;
		delete tr.sep;
	}
	return tl;
};

// A game staged, then stepped.
export const compileStepped = (
	...args: Parameters<typeof compileCourt>
): CourtTimeline => stepTimeline(compileCourt(...args));
