// THE GAME A POSSESSION AT A TIME (see README.md, "Step 5" and "Step 6").
//
// Time runs forward one possession at a time, and through each the ten
// together, a step at a time, each seeing the other side as it was a beat
// before. On defense each guards his man himself (defense.ts) wherever the
// planner (for now, the director's following of his man - see mark in
// director.ts) has him on a man; on offense each man off the ball works
// the spot the set gives him himself (offense.ts) whenever the play has
// nothing of his on. Everywhere else, after the play: into his acts,
// through the picture's cuts, a shot in the air, the runs the play has him
// make, the ball in his hands. Every man carries on from wherever the last
// stretch left him. (Without the offense's own way - `attacking` - the side
// with the ball is stepped after the play first, as stepper.ts always has,
// and then the five on defense.)

import type { BallSeg, Move, Track } from "../director.ts";
import { COURT_H, COURT_W, type Pt, rimX } from "../geometry.ts";
import type { AnimName } from "../poses.ts";
import type { Sample, Spans } from "../stepper.ts";
import {
	type Defender,
	decide,
	driving,
	type Guard,
	COMMIT_MOST,
	commitLead,
	type Guarding,
	HELP_REACH,
	handLead,
	helpCost,
	helpSpotOf,
	inFront,
	moveOn,
	type Other,
	type Seen,
	seenOf,
	type Threat,
} from "./defense.ts";
import type { Body } from "./body.ts";
import { type Mover, STEP_S, stood } from "./movement.ts";
import { decide as decideHand, type HandMind, handMindOf } from "./handler.ts";
import {
	type Attacker,
	decide as decideOff,
	goingAtRim,
	type Mind,
	mindOf,
	own01,
} from "./offense.ts";

const STEP_MS = STEP_S * 1000;

// What the planner (the director, for now) gives the defense.
export type Defending = {
	// Who each defender guards, from when to when.
	guards: ReadonlyMap<number, readonly Guard[]>;
	// The runs the play has a defender make in the middle of that, from when
	// to when (stepping up to stop the ball): the play's.
	planned: ReadonlyMap<number, readonly (readonly [number, number])[]>;
	defender: (pid: number) => Defender;
	threat: (pid: number) => Threat;
};

// What the planner (the director, for now) gives the offense: each man off
// the ball is his own whenever the play has nothing of his on.
export type Attacking = {
	attacker: (pid: number) => Attacker;
	// The runs of the play's that only say where the set wants him: his
	// own to make (see offense.ts).
	spacing: ReadonlySet<Move>;
};

// Where the play wants a man (stepper.ts), and how far ahead he reads it.
export type Wanted = {
	want: (t: number) => Pt | undefined;
	seeAt: (t: number) => number;
};

type Man = Pick<Track, "pid" | "team" | "shown" | "moves" | "acts">;

export type Together<M extends Man> = {
	men: readonly M[];
	wanted: ReadonlyMap<number, Wanted>;
	defending: Defending;
	attacking?: Attacking;
	// Stepping a man after the play (stepper.ts), from `lo` till `hi`.
	stepInto: (
		tr: M,
		want: Wanted["want"],
		seeAt: Wanted["seeAt"],
		samples: Sample[],
		lo?: number,
		hi?: number,
		heldBy?: Spans,
	) => Sample[];
	laid: ReadonlyMap<
		number,
		{ from: number; at: (t: number) => Pt | undefined }
	>;
	helds: ReadonlyMap<number, Spans>;
	offenseAt: (t: number) => 0 | 1;
	holderAt: (t: number) => number | undefined;
	ball: readonly BallSeg[];
	poss: readonly (readonly [number, 0 | 1])[];
	cut: Spans;
	// A shot in the air till it is rebounded; free throws; the ball in a
	// man's hands on the floor.
	sharp: Spans;
	ft: Spans;
	live: Spans;
	skipAt: (t: number) => readonly [number, number] | undefined;
	// Spans of time (stepper.ts), and when a man goes off the floor (held,
	// as his acts are).
	spans: (list: (readonly [number, number])[]) => Spans;
	offFloor: (tr: M) => [number, number][];
};

// An act of his coming this soon (ms), he is the play's again: on to it
// the way the play has him go (see follow.ts COMMIT_MS).
const ACT_LEAD = 600;
// Acts a defender guarding his man does wherever he is, feet set: a hand up
// at a shot, a reach at the ball, the jolt of a screen - not ones that have
// to be at the ball or into a man (a block, a steal, a foul, a box-out).
// (Only if he is still that near what he does it at, feet - a hand up at
// the shooter, a reach at the ball; and somebody of theirs that near, for
// the jolt of a screen.)
const AT_NEAR = 8;
const BUMP_NEAR = 3.5;
const LOOSE = new Set<AnimName>([
	"contest",
	"contestTwo",
	"contestFace",
	"reach",
	"bump",
]);
// What the man with the ball does wherever he is, working his man himself
// (see handler.ts): facing up, a jab or a fake, a move put through where
// he stands, calling the play, his pass - not his shot (the play's spot is
// its zone), a move that takes him somewhere (a spin, a step-back), nor
// his catch.
const WITH_BALL = new Set<AnimName>([
	"triple",
	"jab",
	"shotFake",
	"callPlay",
	"dribbleHesi",
	"dribbleJab",
	"dribbleInOutR",
	"dribbleInOutL",
	"dribbleShake",
	"dribbleRocker",
	"pass",
	"passBounce",
	"passOverhead",
	"passPush",
	"passBouncePush",
	"passBehind",
	"passNoLook",
	"passSnap",
	"passBounceSnap",
]);
// Handing back to the play with something of his that pins him coming
// this soon after (ms), he makes for that (as soon as he must: see
// commitLead).
const COMMIT_AFTER = COMMIT_MOST;
// (On the floor, this far in at the least (feet): working his spot
// himself, a man doesn't go off it to get round anybody.)
const IN_BOUNDS = 0.5;
const keepIn = (n: Mover): Mover => {
	const x = Math.min(COURT_W - IN_BOUNDS, Math.max(IN_BOUNDS, n.x));
	const y = Math.min(COURT_H - IN_BOUNDS, Math.max(IN_BOUNDS, n.y));
	return x === n.x && y === n.y
		? n
		: {
				...n,
				x,
				y,
				vx: x === n.x ? n.vx : 0,
				vy: y === n.y ? n.vy : 0,
			};
};
// A man of theirs doing something the play pins this near the man with
// the ball (feet): at him.
const AT_HIM = 7;
// Coming to meet a pass: this fast (feet a second) through where the play
// has him catch it, over the last this long (ms) - not with the man who
// threw it this near (feet). (Real: caught at 5.8 ft/s, median.)
const MEET_PACE: [number, number] = [4, 7];
const MEET_MS = 800;
const MEET_LEAST = 8;
// On offense, a long way from something of his that pins him, he sets off
// as soon as he must for it this long before at the most (ms) - out of a
// corner across the floor for a catch, say.
const OFF_COMMIT_MOST = 6000;
// How long the five take to see the ball going to the rim (ms).
const TEAM_READ = 200;
// Dribbling again for less than this (ms) between two moves with the ball
// is not going anywhere again (see pickUpFor).
const ON_AGAIN_LEAST = 500;
// Something that pins him this soon (ms) after one he does wherever he is:
// that one pins him too (see strict).
const CHAIN_MS = 2000;
// Taking over from the play this soon (ms) before something of his he does
// wherever he is: not till it is done.
const LEAD_IN = 1200;
// Within this (feet) of a spot the play pins him to, coming in on it, he
// keeps to it - not stepping round anybody.
const KEEP_NEAR = 5;
// Guarding his man himself for less than this (ms) is not worth it.
const AGENT_LEAST = 500;
// (Who he guards holds this long past the last the planner said, ms: it
// says so every tenth of a second.)
const GUARD_SLACK = 100;
// A run of the play's: one that takes him at least this far (feet).
const RUN_LEAST = 1;
// A run of its dribble shorter than this (feet) is the play working the
// ball - the man with it does that himself.
const WORKED_MOST = 6;
// Rid of the ball this long ago (ms) at the most, a man on offense still
// answers it (see offense.ts).
const PASS_ANSWER = 1500;

type Run = { a: number; b: number; agent: boolean };
// A pass of theirs he comes to meet: when it gets to him, and who threw it.
type Catch = { at: number; from: number };

// Back to the play from guarding his man himself, off where it has him: on
// the play's way, shifted over by how far off it he is, the shift let go
// over a moment (this long at the least, ms, and as long again as making it
// up at under half his top speed takes - no longer than this, and done by
// the next thing of his that pins him: this long before it).
const MERGE_LEAST = 600;
const MERGE_MOST = 2000;
const MERGE_BEFORE = 150;
const mergedFrom = (
	want: Wanted["want"],
	q: Sample,
	top: number,
	next: number,
): Wanted["want"] => {
	const A = want(q.t);
	if (!A) {
		return want;
	}
	const ox = q.x - A.x;
	const oy = q.y - A.y;
	const gap = Math.hypot(ox, oy);
	if (gap < 0.05) {
		return want;
	}
	const T = Math.max(
		STEP_MS * 2,
		Math.min(
			MERGE_MOST,
			MERGE_LEAST + (1000 * gap) / (0.4 * top),
			next - q.t - MERGE_BEFORE,
		),
	);
	return (t) => {
		const W = want(t);
		if (!W || t >= q.t + T) {
			return W;
		}
		const u = t <= q.t ? 0 : (t - q.t) / T;
		const k = 1 - u * u * (3 - 2 * u);
		return { x: W.x + ox * k, y: W.y + oy * k };
	};
};

const lastAtOrBefore = <X>(
	xs: readonly X[],
	t: number,
	key: (x: X) => number,
) => {
	let lo = 0;
	let hi = xs.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (key(xs[mid]!) <= t) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return lo - 1;
};

export const defendTogether = <M extends Man>(x: Together<M>) => {
	const samples = new Map<number, Sample[]>();
	const own = new Map<number, [number, AnimName][]>();
	// (When each man with the ball dribbled it his own way, from when till
	// when.)
	const dribbled = new Map<number, [number, number][]>();
	const teamOf = new Map<number, 0 | 1>();
	for (const tr of x.men) {
		samples.set(tr.pid, []);
		own.set(tr.pid, []);
		teamOf.set(tr.pid, tr.team);
	}
	// (His body: the same on either side of the ball.)
	const bodyOf = (pid: number): Body => x.defending.defender(pid).body;
	// Where a man is at t, as stepped so far.
	const at = (pid: number, t: number): Sample | undefined => {
		const s = samples.get(pid);
		if (!s || s.length === 0) {
			return undefined;
		}
		const i = lastAtOrBefore(s, t, (q) => q.t);
		if (i < 0) {
			return undefined;
		}
		const a = s[i]!;
		const b = s[i + 1];
		if (!b) {
			return t - a.t <= 2 * STEP_MS ? a : undefined;
		}
		if (b.t - a.t > STEP_MS + 1) {
			return t - a.t <= STEP_MS ? a : undefined;
		}
		const u = (t - a.t) / (b.t - a.t);
		return {
			t,
			x: a.x + (b.x - a.x) * u,
			y: a.y + (b.y - a.y) * u,
			vx: a.vx + (b.vx - a.vx) * u,
			vy: a.vy + (b.vy - a.vy) * u,
		};
	};
	// (The play's way for him from `from` on, merged into from where he
	// guarded his man himself, if he just did.)
	const mergeIfHis = (
		pid: number,
		want: Wanted["want"],
		from: number,
		held: Spans,
	): Wanted["want"] => {
		const q = samples.get(pid)!.at(-1);
		const mine = own.get(pid)!.at(-1);
		if (!q || !mine || mine[0] !== q.t || from - q.t > STEP_MS + 1) {
			return want;
		}
		return mergedFrom(want, q, bodyOf(pid).top, held.next(q.t + 1));
	};
	// A pass in the air, from one of them to another (bounced to him: from
	// the throw till it is in his hands).
	const passes: [number, number][] = [];
	x.ball.forEach((s, k) => {
		if (s.kind !== "fly" || s.dead || !("pid" in s.from)) {
			return;
		}
		const n = x.ball[k + 1];
		if ("pid" in s.to) {
			passes.push([s.t0, s.t1]);
		} else if (
			s.to.z < 1 &&
			n?.kind === "fly" &&
			n.t0 === s.t1 &&
			"pid" in n.to
		) {
			passes.push([s.t0, n.t1]);
		}
	});
	const passing = (t: number) => {
		const i = lastAtOrBefore(passes, t, (p) => p[0]);
		return i >= 0 && t < passes[i]![1];
	};
	// Each pass to a man, as it gets to him: when, and who threw it.
	const passesTo = new Map<number, Catch[]>();
	x.ball.forEach((s, k) => {
		if (s.kind !== "fly" || s.dead || !("pid" in s.to)) {
			return;
		}
		// (Bounced: off the floor on its way from him.)
		const p = x.ball[k - 1];
		const from =
			"pid" in s.from
				? s.from.pid
				: p?.kind === "fly" && "pid" in p.from && !("pid" in p.to)
					? p.from.pid
					: undefined;
		if (from !== undefined && from !== s.to.pid) {
			const list = passesTo.get(s.to.pid) ?? [];
			list.push({ at: s.t1, from });
			passesTo.set(s.to.pid, list);
		}
	});
	// Where the ball is at t (on the floor's plan), if he can tell.
	const ballAt = (t: number): Pt | undefined => {
		const i = lastAtOrBefore(x.ball, t, (s) => s.t0);
		const s = x.ball[i];
		if (!s) {
			return undefined;
		}
		if (s.kind === "hold") {
			return at(s.pid, t);
		}
		if (s.kind === "fly") {
			const end = (e: typeof s.from, when: number) =>
				"pid" in e ? at(e.pid, when) : { x: e.x, y: e.y };
			const A = end(s.from, s.t0);
			const B = end(s.to, s.t1);
			if (!A || !B) {
				return A ?? B;
			}
			const u = Math.min(1, Math.max(0, (t - s.t0) / (s.t1 - s.t0 || 1)));
			return { x: A.x + (B.x - A.x) * u, y: A.y + (B.y - A.y) * u };
		}
		if (s.kind === "bounce") {
			const u = Math.min(1, Math.max(0, (t - s.t0) / (s.t1 - s.t0 || 1)));
			return {
				x: s.from.x + (s.to.x - s.from.x) * u,
				y: s.from.y + (s.to.y - s.from.y) * u,
			};
		}
		if (s.kind === "rest") {
			return { x: s.at.x, y: s.at.y };
		}
		return undefined;
	};
	const guardAt = (pid: number, t: number): number | undefined => {
		const gs = x.defending.guards.get(pid);
		if (!gs) {
			return undefined;
		}
		const g = gs[lastAtOrBefore(gs, t, (q) => q.t0)];
		return g && t <= g.t1 + GUARD_SLACK ? g.man : undefined;
	};
	// His acts that pin him to a spot (at the ball, into a man), and those
	// he does wherever he is (see LOOSE) - but not one with something that
	// pins him coming close behind it (a contest, and the box-out after it):
	// the play has him from the one into the other.
	// (And the man with the ball, the same with his own - see WITH_BALL -
	// but not one with something that pins him coming close behind it
	// before it leaves his hands: facing up, and his shot.)
	// (His catch too, in play in the front court, coming to meet it -
	// unless it is his shot's: caught where the play has him for it.)
	const strict = new Map<number, Spans>();
	const loose = new Map<number, Spans>();
	const looseOf = new Map<number, [number, number][]>();
	const withBall = new Map<number, Spans>();
	const withBallOf = new Map<number, [number, number][]>();
	const catches = new Map<number, Catch[]>();
	// (And the man with the ball, anything of theirs at him that the play
	// pins - a swipe at the ball, a foul: on the play's spot for it, where
	// it has them do it.)
	const atHim = new Map<number, [number, number][]>();
	if (x.attacking) {
		for (const tr of x.men) {
			for (const a of tr.acts) {
				if (LOOSE.has(a.anim)) {
					continue;
				}
				const h = x.holderAt(a.t0);
				const H = h === undefined ? undefined : x.laid.get(h)?.at(a.t0);
				const P = x.laid.get(tr.pid)?.at(a.t0);
				if (
					h !== undefined &&
					teamOf.get(h) !== tr.team &&
					H &&
					P &&
					Math.hypot(H.x - P.x, H.y - P.y) <= AT_HIM
				) {
					const list = atHim.get(h) ?? [];
					list.push([a.t0, a.t1]);
					atHim.set(h, list);
				}
			}
		}
	}
	for (const tr of x.men) {
		const his = (a: M["acts"][number]) =>
			x.attacking !== undefined &&
			WITH_BALL.has(a.anim) &&
			x.holderAt(a.t0) === tr.pid;
		const caught = (a: M["acts"][number]): Catch | undefined => {
			if (!x.attacking || a.anim !== "catch") {
				return undefined;
			}
			const p = (passesTo.get(tr.pid) ?? []).find(
				(q) => q.at >= a.t0 && q.at <= a.t1,
			);
			const P = p && x.laid.get(tr.pid)?.at(p.at);
			const O = teamOf.get(tr.pid)!;
			return p &&
				P &&
				x.live.hit(p.at, p.at + 1) &&
				(P.x - COURT_W / 2) * (rimX(O) - COURT_W / 2) > 0
				? p
				: undefined;
		};
		const pins = [
			...tr.acts
				.filter((a) => !LOOSE.has(a.anim) && !his(a) && !caught(a))
				.map((a) => [a.t0, a.t1] as [number, number]),
			...x.offFloor(tr),
			...(atHim.get(tr.pid) ?? []),
		];
		const starts = pins.map(([a]) => a).sort((a, b) => a - b);
		const chained = (t1: number, holding = false) => {
			const i = starts.findIndex((a) => a >= t1);
			return (
				i >= 0 &&
				starts[i]! - t1 <= CHAIN_MS &&
				(!holding || x.holderAt(starts[i]!) === tr.pid)
			);
		};
		const free: [number, number][] = [];
		const mine: [number, number][] = [];
		const met: Catch[] = [];
		for (const a of tr.acts) {
			const c = caught(a);
			if (LOOSE.has(a.anim)) {
				(chained(a.t1) ? pins : free).push([a.t0, a.t1]);
			} else if (his(a) || c) {
				if (chained(a.t1, true)) {
					pins.push([a.t0, a.t1]);
				} else {
					mine.push([a.t0, a.t1]);
					if (c) {
						met.push(c);
					}
				}
			}
		}
		catches.set(
			tr.pid,
			met.sort((a, b) => a.at - b.at),
		);
		free.sort((a, b) => a[0] - b[0]);
		looseOf.set(tr.pid, free);
		loose.set(tr.pid, x.spans(free));
		mine.sort((a, b) => a[0] - b[0]);
		withBallOf.set(tr.pid, mine);
		withBall.set(tr.pid, x.spans(mine));
		strict.set(
			tr.pid,
			free.length === 0 && mine.length === 0 && !atHim.has(tr.pid)
				? x.helds.get(tr.pid)!
				: x.spans(pins),
		);
	}
	const committing = (pid: number, t: number): boolean => {
		const due = strict.get(pid)!.next(t) - t;
		return (
			due > ACT_LEAD &&
			due <= COMMIT_AFTER &&
			!x.cut.hit(t - STEP_MS, t + STEP_MS) &&
			!x.ft.hit(t, t + 1) &&
			!x.skipAt(t)
		);
	};
	const plannedAt = (pid: number, t: number): boolean =>
		(x.defending.planned.get(pid) ?? []).some(
			([a, b]) => a - ACT_LEAD <= t && t <= b,
		);
	// Whether he guards his man himself at t: on a man of theirs the planner
	// has him on, the ball in play, nothing of his own coming.
	const agentAt = (
		pid: number,
		t: number,
		O: 0 | 1,
		// (Whether his man is on the floor then.)
		there: (man: number, t: number) => boolean,
	): boolean => {
		const man = guardAt(pid, t);
		return (
			man !== undefined &&
			!plannedAt(pid, t) &&
			teamOf.get(man) === O &&
			!strict.get(pid)!.hit(t - STEP_MS, t + ACT_LEAD) &&
			!x.cut.hit(t - STEP_MS, t + STEP_MS) &&
			!x.sharp.hit(t, t + 1) &&
			!x.ft.hit(t, t + 1) &&
			!x.skipAt(t) &&
			(x.live.hit(t, t + 1) || passing(t)) &&
			there(man, t)
		);
	};
	// The runs the play has each man make (to a spot, a screen, a cut).
	const runsOf = new Map<number, Spans>();
	for (const tr of x.men) {
		runsOf.set(
			tr.pid,
			x.spans(
				tr.moves
					.filter(
						(m) =>
							Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y) >= RUN_LEAST &&
							!x.attacking?.spacing.has(m),
					)
					.map((m) => [m.t0, m.t1] as [number, number]),
			),
		);
	}
	// ...and those of them that take the man with the ball off his own
	// working of it: not a step or two of its dribble this way or that, the
	// play working it - his own to do (see handler.ts) - but a drive, coming
	// off a screen, backing in.
	const handRunsOf = new Map<number, Spans>();
	for (const tr of x.men) {
		handRunsOf.set(
			tr.pid,
			x.spans(
				tr.moves
					.filter((m) => {
						const L = Math.hypot(m.to.x - m.from.x, m.to.y - m.from.y);
						return (
							L >= RUN_LEAST &&
							!x.attacking?.spacing.has(m) &&
							!(m.anim === "dribble" && L < WORKED_MOST)
						);
					})
					.map((m) => [m.t0, m.t1] as [number, number]),
			),
		);
	}
	// When each man had the ball, from when till when.
	const holdsOf = new Map<number, [number, number][]>();
	x.ball.forEach((s, k) => {
		if (s.kind === "hold") {
			const list = holdsOf.get(s.pid) ?? [];
			list.push([s.t0, x.ball[k + 1]?.t0 ?? Infinity]);
			holdsOf.set(s.pid, list);
		}
	});
	// When the ball last left his hands, at or before t, if it has.
	const letGo = (pid: number, t: number): number | undefined => {
		const list = holdsOf.get(pid) ?? [];
		const i = lastAtOrBefore(list, t, (h) => h[0]);
		const h = list[i];
		return h && h[1] <= t ? h[1] : undefined;
	};
	// Where the set wants a man at t: where the play has him - or, on a run
	// of the set's spacing, where that takes him.
	const setAt = (tr: M, t: number): Pt | undefined => {
		const m = tr.moves[lastAtOrBefore(tr.moves, t, (q) => q.t0)];
		return m && t <= m.t1 && x.attacking?.spacing.has(m)
			? m.to
			: x.laid.get(tr.pid)!.at(t);
	};
	// Who the ball in the air (the k-th of it) is on its way to, if a man:
	// bounced to him, from the throw on - down to the floor, and up off it
	// into his hands.
	const goingTo = (k: number): number | undefined => {
		const s = x.ball[k];
		if (s?.kind !== "fly") {
			return undefined;
		}
		if ("pid" in s.to) {
			return s.to.pid;
		}
		const n = x.ball[k + 1];
		return s.to.z < 1 && n?.kind === "fly" && n.t0 === s.t1 && "pid" in n.to
			? n.to.pid
			: undefined;
	};
	// The man the ball is in the air to at t, if it is.
	const flyingTo = (t: number): number | undefined =>
		goingTo(lastAtOrBefore(x.ball, t, (q) => q.t0));
	// Where the play has the ball at t: with the man who has it, or the man
	// it is on its way to.
	const ballLaid = (t: number): Pt | undefined => {
		const k = lastAtOrBefore(x.ball, t, (q) => q.t0);
		const s = x.ball[k];
		if (!s) {
			return undefined;
		}
		const pid = s.kind === "hold" ? s.pid : goingTo(k);
		return pid === undefined ? undefined : x.laid.get(pid)?.at(t);
	};
	// Whether a man on offense works his spot himself at t: off the ball, in
	// play in the front court with it, nothing of his own coming, no run of
	// the play's on.
	const ownOnOffense = (pid: number, t: number, O: 0 | 1): boolean => {
		if (
			teamOf.get(pid) !== O ||
			x.holderAt(t) === pid ||
			strict.get(pid)!.hit(t - STEP_MS, t + ACT_LEAD) ||
			runsOf.get(pid)!.hit(t - STEP_MS, t + STEP_MS) ||
			x.cut.hit(t - STEP_MS, t + STEP_MS) ||
			x.sharp.hit(t, t + 1) ||
			x.ft.hit(t, t + 1) ||
			x.skipAt(t) ||
			!(x.live.hit(t, t + 1) || passing(t))
		) {
			return false;
		}
		const half = rimX(O) - COURT_W / 2;
		const P = x.laid.get(pid)?.at(t);
		const B = ballLaid(t);
		return (
			P !== undefined &&
			B !== undefined &&
			(P.x - COURT_W / 2) * half > 0 &&
			(B.x - COURT_W / 2) * half > 0
		);
	};
	// Whether the man with the ball works his man himself at t: in play in
	// the front court, nothing of the play's that pins him coming, none of
	// its runs on (its actions: a drive, coming off a screen, backing in).
	const ownWithBall = (pid: number, t: number, O: 0 | 1): boolean => {
		if (
			!x.attacking ||
			teamOf.get(pid) !== O ||
			x.holderAt(t) !== pid ||
			strict.get(pid)!.hit(t - STEP_MS, t + ACT_LEAD) ||
			handRunsOf.get(pid)!.hit(t - STEP_MS, t + STEP_MS) ||
			x.cut.hit(t - STEP_MS, t + STEP_MS) ||
			x.sharp.hit(t, t + 1) ||
			x.ft.hit(t, t + 1) ||
			x.skipAt(t) ||
			!x.live.hit(t, t + 1)
		) {
			return false;
		}
		const P = x.laid.get(pid)?.at(t);
		return P !== undefined && (P.x - COURT_W / 2) * (rimX(O) - COURT_W / 2) > 0;
	};
	// How long ago (ms) he caught the pass that has the ball his at t - if a
	// pass of theirs is how he came to have it.
	const caughtAgo = (pid: number, t: number): number | undefined => {
		let k = lastAtOrBefore(x.ball, t, (q) => q.t0);
		while (k > 0) {
			const s = x.ball[k]!;
			const b = x.ball[k - 1]!;
			if (s.kind !== "hold" || b.kind !== "hold" || b.pid !== pid) {
				break;
			}
			k--;
		}
		const s = x.ball[k];
		const f = x.ball[k - 1];
		return s?.kind === "hold" &&
			s.pid === pid &&
			f?.kind === "fly" &&
			!f.dead &&
			"pid" in f.from &&
			"pid" in f.to &&
			f.to.pid === pid
			? t - s.t0
			: undefined;
	};
	// Whether he has the ball on his dribble at t - not in his hands,
	// not a move put through where he stands, not passing it.
	const dribblingAt = (pid: number, t: number): boolean => {
		const s = x.ball[lastAtOrBefore(x.ball, t, (q) => q.t0)];
		return (
			s?.kind === "hold" &&
			s.pid === pid &&
			s.style !== "hold" &&
			!s.still &&
			!withBall.get(pid)!.hit(t, t + 1)
		);
	};
	// When he next stops dribbling from t on (picks it up, or puts a move
	// through where he stands), and when he is next on it again after that
	// - or it leaves his hands.
	const pickUpFor = (
		pid: number,
		t: number,
	): { up: number | undefined; again: number } => {
		// (His dribbling changes only as the ball's stretches with him begin
		// and his moves with it begin and end.)
		const turns: number[] = [];
		let k = lastAtOrBefore(x.ball, t, (q) => q.t0);
		for (k += 1; k < x.ball.length; k++) {
			const s = x.ball[k]!;
			if (s.kind !== "hold" || s.pid !== pid) {
				break;
			}
			turns.push(s.t0);
		}
		const gone = x.ball[k]?.t0 ?? Infinity;
		const acts = withBallOf.get(pid)!;
		for (
			let i = Math.max(
				0,
				lastAtOrBefore(acts, t, (q) => q[0]),
			);
			i < acts.length && acts[i]![0] < gone;
			i++
		) {
			for (const u of acts[i]!) {
				if (u > t && u < gone) {
					turns.push(u);
				}
			}
		}
		turns.sort((a, b) => a - b);
		let up: number | undefined;
		for (let i = 0; i < turns.length; i++) {
			const u = turns[i]!;
			const on = dribblingAt(pid, u);
			if (up === undefined && !on) {
				up = u;
			} else if (up !== undefined && on) {
				// (On it again only for a blink between two moves of his with
				// it - a hesitation, then the ball in his hands to throw it:
				// all one, he goes nowhere in between.)
				const off = turns.find((w, j) => j > i && !dribblingAt(pid, w));
				if (off !== undefined && off - u < ON_AGAIN_LEAST) {
					continue;
				}
				return { up, again: u };
			}
		}
		return { up, again: gone };
	};
	// Each man's stretches on the floor from `lo` till `hi`, on his own beat
	// (as stepper.ts steps them).
	const ticksOf = (tr: M, lo: number, hi: number): [number, number][] => {
		const L = x.laid.get(tr.pid)!;
		const end = tr.moves.at(-1)!.t1 + 1000;
		const out: [number, number][] = [];
		tr.shown.forEach(([when, on], k) => {
			const whole = Math.max(L.from, when);
			const from =
				lo > whole
					? whole + Math.ceil((lo - whole) / STEP_MS - 1e-6) * STEP_MS
					: whole;
			const until = Math.min(end, tr.shown[k + 1]?.[0] ?? end, hi);
			if (on && until > from) {
				out.push([from, until === hi ? until - 1e-6 : until - 1]);
			}
		});
		return out;
	};
	// The five on defense from `lo` till `hi`, and with them, a step at a
	// time, the men on offense `A` (none: stepped already).
	const defend = (
		D: readonly M[],
		A: readonly M[],
		lo: number,
		hi: number,
		O: 0 | 1,
	) => {
		const rim = { x: rimX(O), y: COURT_H / 2 };
		type Who = {
			tr: M;
			off: boolean;
			// (On offense: what he keeps between his picks of where next - off
			// the ball, and with it.)
			mind?: Mind;
			hand?: HandMind;
			runs: Run[];
			k: number;
			t: number;
			m?: Mover;
			mt?: number;
			man?: number;
			// (What of his he has set off for, by when it is due.)
			sure?: number;
			// (When he last stepped with the ball himself.)
			ballT?: number;
		};
		const who: Who[] = [];
		let first = Infinity;
		let lastT = -Infinity;
		// (Whether a man of theirs is on the floor at t: stepped there
		// already, or - stepped in this same loop - to be.)
		const onFloor = new Map(A.map((tr) => [tr.pid, ticksOf(tr, lo, hi)]));
		const there = (man: number, t: number) =>
			at(man, t) !== undefined ||
			(onFloor.get(man)?.some(([a, b]) => a <= t && t <= b) ?? false);
		for (const tr of [...A, ...D]) {
			const off = tr.team === O;
			const runs: Run[] = [];
			for (const [a, b] of ticksOf(tr, lo, hi)) {
				const here: Run[] = [];
				for (let t = a; t <= b; t += STEP_MS) {
					// (Guarding his man himself and something of his that pins
					// him coming: on his own way to it, man or no man, till the
					// play takes him in to it.)
					const agent = off
						? ownOnOffense(tr.pid, t, O) || ownWithBall(tr.pid, t, O)
						: agentAt(tr.pid, t, O, there) ||
							(here.at(-1)?.agent === true && committing(tr.pid, t));
					const r = here.at(-1);
					if (r && r.agent === agent) {
						r.b = t;
					} else {
						here.push({ a: t, b: t, agent });
					}
				}
				// (Taking over just before something he does wherever he is: the
				// play's till that is done.)
				const mine = looseOf.get(tr.pid)!;
				for (let i = 0; i < here.length; i++) {
					const r = here[i]!;
					const act = r.agent
						? mine.find(([a0]) => a0 >= r.a && a0 <= r.b)
						: undefined;
					if (!act || act[0] - r.a >= LEAD_IN) {
						continue;
					}
					const on = r.a + Math.ceil((act[1] - r.a) / STEP_MS) * STEP_MS;
					if (on > r.b) {
						r.agent = false;
					} else {
						here.splice(
							i,
							1,
							{ a: r.a, b: on - STEP_MS, agent: false },
							{ a: on, b: r.b, agent: true },
						);
					}
				}
				// (Too short a stretch on his own: the play's.)
				for (const r of here) {
					if (r.agent && r.b - r.a + STEP_MS < AGENT_LEAST) {
						r.agent = false;
					}
				}
				for (const r of here) {
					const p = runs.at(-1);
					if (p && !p.agent && !r.agent && r.a - p.b <= STEP_MS) {
						p.b = r.b;
					} else {
						runs.push(r);
					}
				}
			}
			if (runs.length > 0) {
				who.push({
					tr,
					off,
					...(off ? { mind: mindOf(), hand: handMindOf() } : {}),
					runs,
					k: 0,
					t: runs[0]!.a,
				});
				first = Math.min(first, runs[0]!.a);
				lastT = Math.max(lastT, runs.at(-1)!.b);
			}
		}
		const tick = (w: Who, r: Run, t: number, mine: Wanted, S: Sample[]) => {
			const pid = w.tr.pid;
			const me = x.defending.defender(pid);
			// (His man - or, on his way to something of his with nobody to
			// guard, the last he had.)
			const man = guardAt(pid, t) ?? w.man;
			const see = t - me.react;
			const manSeen =
				man === undefined ? undefined : (at(man, see) ?? at(man, t));
			if (man === undefined || !manSeen) {
				return;
			}
			w.man = man;
			const ball = ballAt(see);
			// (On from where he is: the last step he took himself, or where
			// the play's left him.)
			let m: Mover | undefined;
			const last = S.at(-1);
			if (w.m && w.mt === t - STEP_MS) {
				m = w.m;
			} else if (last && t - last.t <= STEP_MS + 1) {
				const pace = Math.hypot(last.vx, last.vy);
				m = {
					x: last.x,
					y: last.y,
					vx: last.vx,
					vy: last.vy,
					ax: 0,
					ay: 0,
					yaw:
						pace > 0.5
							? Math.atan2(last.vy, last.vx)
							: Math.atan2(
									(ball ?? manSeen).y - last.y,
									(ball ?? manSeen).x - last.x,
								),
				};
			} else {
				const P = mine.want(t) ?? x.laid.get(pid)!.at(t);
				if (!P) {
					return;
				}
				m = stood(P, Math.atan2(manSeen.y - P.y, manSeen.x - P.x));
			}
			// (Handing back to the play as his stretch ends - into an act of
			// his, the play's run, off the floor: on his way to where it has
			// him by then.)
			// (Or, something of his that pins him coming soon after - a box-out
			// as the shot goes up, say - straight to where that has him.)
			// (Whatever the play pins him to once it has him again - a contest
			// too.)
			// (And something of his on his own way - a contest, a reach: there
			// for it, on the spot the play has for it.)
			const end = r.b + STEP_MS;
			const soon = loose.get(pid)!.next(t);
			const due = soon < end ? soon : x.helds.get(pid)!.next(end);
			const commit = soon < end || due - end <= COMMIT_AFTER;
			const f = commit ? due : end;
			let handTo: Guarding["handTo"];
			const A = mine.want(f);
			if (A) {
				const gap = Math.hypot(A.x - m.x, A.y - m.y);
				// (On his way to it once he has set off.)
				const lead = !commit
					? handLead(gap, me)
					: w.sure === f
						? Infinity
						: f - t <= COMMIT_MOST
							? commitLead(m, A, me)
							: 0;
				if (commit && f - t <= lead) {
					w.sure = f;
				}
				const B = mine.want(f + STEP_MS);
				if (f - t <= lead) {
					handTo = {
						at: A,
						// (Into something of his: there, and set.)
						v:
							B && !commit
								? { x: (B.x - A.x) / STEP_S, y: (B.y - A.y) / STEP_S }
								: { x: 0, y: 0 },
						until: f - t,
						lead: Math.min(lead, COMMIT_MOST),
						commit,
					};
				}
			}
			const { goal, anim } = decide(
				m,
				{
					man: seenOf(manSeen, me),
					on: x.holderAt(see) === man,
					ball,
					rim,
					threat: x.defending.threat(man),
					handTo,
					planted: loose.get(pid)!.hit(t, t + 1),
					drive,
					help: helper === pid ? helpAt : undefined,
				},
				me,
			);
			const others: Other[] = [];
			for (const o of x.men) {
				if (o.pid !== pid) {
					const q = at(o.pid, t);
					if (q) {
						others.push(q);
					}
				}
			}
			const n = moveOn(
				m,
				goal,
				me,
				others,
				handTo?.commit === true &&
					Math.hypot(handTo.at.x - m.x, handTo.at.y - m.y) < KEEP_NEAR,
			);
			w.m = n;
			w.mt = t;
			S.push({ t, x: n.x, y: n.y, vx: n.vx, vy: n.vy });
			own.get(pid)!.push([t, anim]);
		};
		// The man with the ball working his man himself, a step.
		const tickHand = (w: Who, r: Run, t: number, mine: Wanted, S: Sample[]) => {
			const pid = w.tr.pid;
			const me = x.attacking!.attacker(pid);
			const see = t - me.react;
			const rim = { x: rimX(O), y: COURT_H / 2 };
			const face = (P: Pt) => Math.atan2(rim.y - P.y, rim.x - P.x);
			if (w.ballT !== t - STEP_MS) {
				// (A new touch of his: what he was about the last is over.)
				w.hand = { ...handMindOf(), n: w.hand!.n };
			}
			let m: Mover | undefined;
			const last = S.at(-1);
			if (w.m && w.mt === t - STEP_MS) {
				m = w.m;
			} else if (last && t - last.t <= STEP_MS + 1) {
				const pace = Math.hypot(last.vx, last.vy);
				m = {
					x: last.x,
					y: last.y,
					vx: last.vx,
					vy: last.vy,
					ax: 0,
					ay: 0,
					yaw: pace > 0.5 ? Math.atan2(last.vy, last.vx) : face(last),
				};
			} else {
				const P = mine.want(t) ?? x.laid.get(pid)!.at(t);
				if (!P) {
					return;
				}
				m = stood(P, face(P));
			}
			// (Handing back to the play as his stretch ends - its run, his
			// shot: on his way to where it has him by then; with something
			// that pins him coming soon after, straight to it, as soon as his
			// legs say he must.)
			const end = r.b + STEP_MS;
			const due = strict.get(pid)!.next(end);
			const commit = due - end <= COMMIT_AFTER;
			const f = commit ? due : end;
			let handTo: Guarding["handTo"];
			const A = mine.want(f);
			const B = mine.want(f + STEP_MS);
			const goingThen: Pt =
				A && B
					? { x: (B.x - A.x) / STEP_S, y: (B.y - A.y) / STEP_S }
					: { x: 0, y: 0 };
			if (A) {
				const gap = Math.hypot(A.x - m.x, A.y - m.y);
				const lead = !commit
					? handLead(gap, me)
					: w.sure === f
						? Infinity
						: f - t <= COMMIT_MOST
							? commitLead(m, A, me)
							: 0;
				if (commit && f - t <= lead) {
					w.sure = f;
				}
				if (f - t <= lead) {
					handTo = {
						at: A,
						v: goingThen,
						until: f - t,
						lead: Math.min(lead, COMMIT_MOST),
						commit,
					};
				}
			}
			// (Back where the play has him by the time he stops dribbling -
			// picks it up, or puts a move through where he stands - with
			// something of the play's to come before he is on it again: his
			// shot, or the play taking him on: he can go nowhere till then.)
			const { up, again } = pickUpFor(pid, t);
			const H =
				up !== undefined &&
				up < f &&
				(strict.get(pid)!.next(up) < again || end < again)
					? mine.want(up)
					: undefined;
			// (And how the play has him going then: into a pass on the move,
			// still going - see through.ts.)
			const H1 = H && mine.want(up! + STEP_MS);
			const mates: Pt[] = [];
			let man: Seen | undefined;
			for (const o of x.men) {
				if (o.pid === pid) {
					continue;
				}
				const q = at(o.pid, see);
				if (!q) {
					continue;
				}
				if (o.team === O) {
					mates.push(q);
				} else if (guardAt(o.pid, see) === pid) {
					man = q;
				}
			}
			const dribbling = dribblingAt(pid, t);
			const { goal, anim } = decideHand(
				m,
				{
					t,
					set: setAt(w.tr, t) ?? { x: m.x, y: m.y },
					rim,
					dribbling,
					man,
					mates,
					handTo,
					caught: caughtAgo(pid, t),
					// (Or, nothing of his coming before the play takes him back,
					// where it has him by then: there by then just the same, not
					// snatched onto it.)
					home:
						H && up !== undefined
							? {
									at: H,
									in: up - t,
									v: H1
										? { x: (H1.x - H.x) / STEP_S, y: (H1.y - H.y) / STEP_S }
										: { x: 0, y: 0 },
								}
							: A
								? { at: A, in: f - t, v: goingThen, end: true }
								: undefined,
				},
				me,
				w.hand!,
				pid,
			);
			const others: Other[] = [];
			for (const o of x.men) {
				if (o.pid !== pid) {
					const q = at(o.pid, t);
					if (q) {
						others.push(q);
					}
				}
			}
			// (With it in his hands, he holds his ground: they go round him.)
			const n = keepIn(
				moveOn(
					m,
					goal,
					me,
					others,
					!dribbling ||
						(handTo?.commit === true &&
							Math.hypot(handTo.at.x - m.x, handTo.at.y - m.y) < KEEP_NEAR),
				),
			);
			w.m = n;
			w.mt = t;
			w.ballT = t;
			if (dribbling) {
				const list = dribbled.get(pid) ?? [];
				const d = list.at(-1);
				if (d && t - d[1] <= STEP_MS + 1) {
					d[1] = t;
				} else {
					list.push([t, t]);
				}
				dribbled.set(pid, list);
			}
			S.push({ t, x: n.x, y: n.y, vx: n.vx, vy: n.vy });
			own.get(pid)!.push([t, anim]);
		};
		// A man on offense working his spot himself, a step.
		const tickOff = (w: Who, r: Run, t: number, mine: Wanted, S: Sample[]) => {
			const pid = w.tr.pid;
			if (x.holderAt(t) === pid) {
				tickHand(w, r, t, mine, S);
				return;
			}
			const me = x.attacking!.attacker(pid);
			const see = t - me.react;
			const ball = ballAt(see);
			// (On from where he is: the last step he took himself, or where
			// the play's left him.)
			let m: Mover | undefined;
			const last = S.at(-1);
			if (!(w.m && w.mt === t - STEP_MS) || w.ballT === t - STEP_MS) {
				// (A new stretch of his own: what he was about the last time is
				// over - but not his draws, nor the pass he answered.)
				w.mind = { ...mindOf(), n: w.mind!.n, passKey: w.mind!.passKey };
			}
			if (w.m && w.mt === t - STEP_MS) {
				m = w.m;
			} else if (last && t - last.t <= STEP_MS + 1) {
				const pace = Math.hypot(last.vx, last.vy);
				m = {
					x: last.x,
					y: last.y,
					vx: last.vx,
					vy: last.vy,
					ax: 0,
					ay: 0,
					yaw:
						pace > 0.5 || !ball
							? Math.atan2(last.vy, last.vx)
							: Math.atan2(ball.y - last.y, ball.x - last.x),
				};
			} else {
				const P = mine.want(t) ?? x.laid.get(pid)!.at(t);
				if (!P) {
					return;
				}
				m = stood(P, ball ? Math.atan2(ball.y - P.y, ball.x - P.x) : 0);
			}
			// (Handing back to the play as his stretch ends - its run, his
			// catch, his screen: on his way to where it has him by then; with
			// something of his that pins him coming soon after, straight to
			// it, as soon as his legs say he must. And a pass to him he comes
			// to meet, in his stretch: there as it gets there.)
			const end = r.b + STEP_MS;
			const meet = catches.get(pid)!.find((c) => c.at > t && c.at <= end);
			const due = meet ? meet.at : x.helds.get(pid)!.next(end);
			const commit = meet !== undefined || due - end <= COMMIT_AFTER;
			const f = commit ? due : end;
			let handTo: Guarding["handTo"];
			const A = mine.want(f);
			if (A) {
				const gap = Math.hypot(A.x - m.x, A.y - m.y);
				const lead = !commit
					? handLead(gap, me)
					: w.sure === f
						? Infinity
						: f - t <= OFF_COMMIT_MOST
							? commitLead(m, A, me, OFF_COMMIT_MOST)
							: 0;
				if (commit && f - t <= lead) {
					w.sure = f;
				}
				const B = mine.want(f + STEP_MS);
				if (f - t <= lead) {
					handTo = {
						at: A,
						// (Into it as the play has him going into it.)
						v: B
							? { x: (B.x - A.x) / STEP_S, y: (B.y - A.y) / STEP_S }
							: { x: 0, y: 0 },
						until: f - t,
						lead: Math.min(lead, OFF_COMMIT_MOST),
						commit,
					};
					// (Coming to meet it: on through its spot as it gets there,
					// at the man who threw it - a spot coming at it that fast,
					// the last of the way.)
					const F =
						meet && (at(meet.from, see) ?? x.laid.get(meet.from)?.at(t));
					const L = F ? Math.hypot(F.x - A.x, F.y - A.y) : 0;
					if (F && L > MEET_LEAST) {
						const pace =
							MEET_PACE[0] +
							(MEET_PACE[1] - MEET_PACE[0]) * own01(pid, Math.round(f));
						const v = {
							x: ((F.x - A.x) / L) * pace,
							y: ((F.y - A.y) / L) * pace,
						};
						const k = Math.min(f - t, MEET_MS) / 1000;
						handTo.at = { x: A.x - v.x * k, y: A.y - v.y * k };
						handTo.v = f - t <= MEET_MS ? v : { x: 0, y: 0 };
					}
				}
			}
			// (The man with the ball - or, with it in the air to a man of
			// his, the man it is going to.)
			const to = flyingTo(see);
			const h =
				x.holderAt(see) ??
				(to !== undefined && teamOf.get(to) === O ? to : undefined);
			const H = h !== undefined && h !== pid ? at(h, see) : undefined;
			const gone = letGo(pid, see);
			const mates: Pt[] = [];
			let man: Pt | undefined;
			for (const o of x.men) {
				if (o.pid === pid) {
					continue;
				}
				const q = at(o.pid, see);
				if (!q) {
					continue;
				}
				if (o.team === O) {
					mates.push(q);
				} else if (guardAt(o.pid, see) === pid) {
					man = q;
				}
			}
			const { goal, anim } = decideOff(
				m,
				{
					t,
					set: setAt(w.tr, t) ?? { x: m.x, y: m.y },
					rim,
					team: O,
					ball,
					holder: H && h !== undefined ? { ...H, pid: h } : undefined,
					mates,
					man,
					drive:
						H && h === x.holderAt(see) && goingAtRim(H, rim) ? H : undefined,
					passKey:
						gone !== undefined && see - gone <= PASS_ANSWER ? gone : undefined,
					handTo,
					next: A && commit ? { at: A, in: f - t } : undefined,
				},
				me,
				w.mind!,
				pid,
			);
			const others: Other[] = [];
			for (const o of x.men) {
				if (o.pid !== pid) {
					const q = at(o.pid, t);
					if (q) {
						others.push(q);
					}
				}
			}
			const n = keepIn(
				moveOn(
					m,
					goal,
					me,
					others,
					handTo?.commit === true &&
						Math.hypot(handTo.at.x - m.x, handTo.at.y - m.y) < KEEP_NEAR,
				),
			);
			w.m = n;
			w.mt = t;
			S.push({ t, x: n.x, y: n.y, vx: n.vx, vy: n.vy });
			own.get(pid)!.push([t, anim]);
		};
		// The five as one: whether the ball is on its way to the rim, and
		// who steps up into its way (the same man till it stops or goes).
		let helper: number | undefined;
		let drive: Seen | undefined;
		let helpAt: Pt | undefined;
		const together = (c: number) => {
			const see = c - TEAM_READ;
			const h = x.holderAt(see);
			const H = h !== undefined && teamOf.get(h) === O ? at(h, see) : undefined;
			if (!H || !driving(H, rim)) {
				helper = undefined;
				drive = undefined;
				helpAt = undefined;
				return;
			}
			drive = H;
			helpAt = helpSpotOf(H, rim);
			// (On his own still, guarding his man himself: free to help.)
			const free = (w: Who) => {
				const r = w.runs[w.k];
				return (
					!w.off &&
					r !== undefined &&
					r.agent &&
					r.a <= c &&
					c <= r.b &&
					guardAt(w.tr.pid, c) !== h
				);
			};
			const cost = (w: Who) => {
				const P = at(w.tr.pid, c);
				const man = guardAt(w.tr.pid, c);
				return P
					? helpCost(P, helpAt!, man === undefined ? undefined : at(man, c), H)
					: Infinity;
			};
			const was = who.find((w) => w.tr.pid === helper);
			if (was && free(was)) {
				return;
			}
			helper = undefined;
			// (Nobody needed, if somebody is in his way already.)
			for (const w of who) {
				if (w.off) {
					continue;
				}
				const P = at(w.tr.pid, c);
				if (P && inFront(H, P, rim)) {
					return;
				}
			}
			let best = HELP_REACH;
			for (const w of who) {
				if (free(w)) {
					const k = cost(w);
					if (k < best) {
						best = k;
						helper = w.tr.pid;
					}
				}
			}
		};
		for (let c = first; c <= lastT + STEP_MS; c += STEP_MS) {
			together(c);
			for (const w of who) {
				while (w.k < w.runs.length && w.t <= c) {
					const r = w.runs[w.k]!;
					const mine = x.wanted.get(w.tr.pid)!;
					const S = samples.get(w.tr.pid)!;
					if (!r.agent) {
						x.stepInto(
							w.tr,
							mergeIfHis(w.tr.pid, mine.want, r.a, x.helds.get(w.tr.pid)!),
							mine.seeAt,
							S,
							r.a,
							r.b + 1,
						);
						w.k += 1;
						w.t = w.runs[w.k]?.a ?? Infinity;
						continue;
					}
					(w.off ? tickOff : tick)(w, r, w.t, mine, S);
					w.t += STEP_MS;
					if (w.t > r.b) {
						w.k += 1;
						w.t = w.runs[w.k]?.a ?? Infinity;
					}
				}
			}
		}
	};
	const bounds = [
		-Infinity,
		...[...new Set(x.poss.map(([t]) => t))]
			.filter((t) => t > -Infinity)
			.sort((a, b) => a - b),
		Infinity,
	];
	for (let k = 0; k + 1 < bounds.length; k++) {
		const lo = bounds[k]!;
		const hi = bounds[k + 1]!;
		if (!(hi > lo)) {
			continue;
		}
		const O = x.offenseAt(lo);
		const A = x.men.filter((tr) => tr.team === O);
		if (!x.attacking) {
			for (const tr of A) {
				const mine = x.wanted.get(tr.pid)!;
				x.stepInto(
					tr,
					mergeIfHis(tr.pid, mine.want, lo, x.helds.get(tr.pid)!),
					mine.seeAt,
					samples.get(tr.pid)!,
					lo,
					hi,
				);
			}
		}
		defend(
			x.men.filter((tr) => tr.team !== O),
			x.attacking ? A : [],
			lo,
			hi,
			O,
		);
	}
	// An act of his left where he is, guarding his man himself, that no
	// longer has anybody to do it at - a hand up at a shooter way off him,
	// the jolt of a screen he went round: not done.
	for (const tr of x.men) {
		const list = own.get(tr.pid)!;
		if (list.length === 0) {
			continue;
		}
		tr.acts = tr.acts.filter((a) => {
			if (!LOOSE.has(a.anim)) {
				return true;
			}
			const i = lastAtOrBefore(list, a.t1, (q) => q[0]);
			if (i < 0 || list[i]![0] < a.t0 - STEP_MS) {
				return true;
			}
			const P = at(tr.pid, a.t0);
			if (!P) {
				return true;
			}
			if (a.anim === "bump") {
				let near = Infinity;
				for (const o of x.men) {
					if (o.team !== tr.team) {
						const Q = at(o.pid, a.t0);
						if (Q) {
							near = Math.min(near, Math.hypot(Q.x - P.x, Q.y - P.y));
						}
					}
				}
				return near <= BUMP_NEAR;
			}
			return !a.look || Math.hypot(a.look.x - P.x, a.look.y - P.y) <= AT_NEAR;
		});
	}
	// What each did as he decided it, by when.
	const anims = new Map<
		number,
		(t: number) => { anim: AnimName } | undefined
	>();
	for (const [pid, list] of own) {
		anims.set(pid, (t) => {
			const i = lastAtOrBefore(list, t, (q) => q[0]);
			const q = list[i];
			return q && t - q[0] <= STEP_MS ? { anim: q[1] } : undefined;
		});
	}
	return { samples, anims, dribbled };
};
