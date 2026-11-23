// THE GAME A POSSESSION AT A TIME (see README.md, "Step 5").
//
// Time runs forward one possession at a time. Each, the side with the ball
// is stepped first - after where the play wants each of them, as stepper.ts
// always has (they don't answer the defense yet: step 6) - and then the five
// on defense together, a step at a time, from what the offense really did:
// each guarding his man himself (defense.ts) wherever the planner (for now,
// the director's following of his man - see mark in director.ts) has him on
// a man, and after the play everywhere else: into his acts, through the
// picture's cuts, a shot in the air, the runs the play has him make. Every
// man carries on from wherever the last stretch left him.

import type { BallSeg, Track } from "../director.ts";
import { COURT_H, type Pt, rimX } from "../geometry.ts";
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
import { type Mover, STEP_S, stood } from "./movement.ts";

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
// Handing back to the play with something of his that pins him coming
// this soon after (ms), he makes for that (as soon as he must: see
// commitLead).
const COMMIT_AFTER = COMMIT_MOST;
// How long the five take to see the ball going to the rim (ms).
const TEAM_READ = 200;
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

type Run = { a: number; b: number; agent: boolean };

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
	const teamOf = new Map<number, 0 | 1>();
	for (const tr of x.men) {
		samples.set(tr.pid, []);
		own.set(tr.pid, []);
		teamOf.set(tr.pid, tr.team);
	}
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
		return mergedFrom(
			want,
			q,
			x.defending.defender(pid).body.top,
			held.next(q.t + 1),
		);
	};
	// A pass in the air, from one of them to another.
	const passes: [number, number][] = [];
	for (const s of x.ball) {
		if (s.kind === "fly" && !s.dead && "pid" in s.from && "pid" in s.to) {
			passes.push([s.t0, s.t1]);
		}
	}
	const passing = (t: number) => {
		const i = lastAtOrBefore(passes, t, (p) => p[0]);
		return i >= 0 && t < passes[i]![1];
	};
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
	const strict = new Map<number, Spans>();
	const loose = new Map<number, Spans>();
	const looseOf = new Map<number, [number, number][]>();
	for (const tr of x.men) {
		const pins = [
			...tr.acts
				.filter((a) => !LOOSE.has(a.anim))
				.map((a) => [a.t0, a.t1] as [number, number]),
			...x.offFloor(tr),
		];
		const starts = pins.map(([a]) => a).sort((a, b) => a - b);
		const chained = (t1: number) => {
			const i = starts.findIndex((a) => a >= t1);
			return i >= 0 && starts[i]! - t1 <= CHAIN_MS;
		};
		const free: [number, number][] = [];
		for (const a of tr.acts) {
			if (LOOSE.has(a.anim)) {
				(chained(a.t1) ? pins : free).push([a.t0, a.t1]);
			}
		}
		free.sort((a, b) => a[0] - b[0]);
		looseOf.set(tr.pid, free);
		loose.set(tr.pid, x.spans(free));
		strict.set(
			tr.pid,
			free.length === 0 ? x.helds.get(tr.pid)! : x.spans(pins),
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
	const agentAt = (pid: number, t: number, O: 0 | 1): boolean => {
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
			at(man, t) !== undefined
		);
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
					? whole + Math.ceil((lo - whole) / STEP_MS) * STEP_MS
					: whole;
			const until = Math.min(end, tr.shown[k + 1]?.[0] ?? end, hi);
			if (on && until > from) {
				out.push([from, until === hi ? until - 1e-6 : until - 1]);
			}
		});
		return out;
	};
	const defend = (D: readonly M[], lo: number, hi: number, O: 0 | 1) => {
		const rim = { x: rimX(O), y: COURT_H / 2 };
		type Who = {
			tr: M;
			runs: Run[];
			k: number;
			t: number;
			m?: Mover;
			mt?: number;
			man?: number;
			// (What of his he has set off for, by when it is due.)
			sure?: number;
		};
		const who: Who[] = [];
		let first = Infinity;
		let lastT = -Infinity;
		for (const tr of D) {
			const runs: Run[] = [];
			for (const [a, b] of ticksOf(tr, lo, hi)) {
				const here: Run[] = [];
				for (let t = a; t <= b; t += STEP_MS) {
					// (Guarding his man himself and something of his that pins
					// him coming: on his own way to it, man or no man, till the
					// play takes him in to it.)
					const agent =
						agentAt(tr.pid, t, O) ||
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
				who.push({ tr, runs, k: 0, t: runs[0]!.a });
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
					tick(w, r, w.t, mine, S);
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
		for (const tr of x.men) {
			if (tr.team === O) {
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
	return { samples, anims };
};
