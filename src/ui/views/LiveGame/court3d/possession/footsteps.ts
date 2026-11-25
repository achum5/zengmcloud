import type { Pt } from "../geometry.ts";

// WHERE HIS FEET COME DOWN (see README.md, step 4): each step a spot on the
// floor and a time, the foot staying put on it while it is down - planned
// from the way he really goes, so the picture's legs only have to reach for
// them (and never skate).
//
// One rule places every step, at any pace, any way he goes: a foot comes
// down where it will hold him up through its time down - under him, put off
// to the side his gait spreads his feet, and off that the other way from how
// hard he is changing his going, as far as it takes to lean him into it (a
// body is a pendulum on its foot: to brake he plants it out in front and
// leans back; to cut, out wide on the far side of the turn, leaning in; to
// set off, back under him, leaning on). So a stop is braking steps out in
// front, a cut a plant out wide - a crossover if the inside foot is the one
// coming down - and a slide steps out and closes, its feet never crossing.
//
// How often a step comes is his own (gait.ts): what the picture shows of
// his going is how far apart they are. Stood, he steps only to stay under
// himself - his weight drifting off his feet, or turned from the way they
// point.

export type FootStep = {
	// 0 his right foot, 1 his left.
	foot: 0 | 1;
	// Down from t0 till t1 (ms) - t1 Infinity, still down as it ends.
	t0: number;
	t1: number;
	x: number;
	y: number;
	// Which way it points (radians).
	yaw: number;
};

// How he is going about it: stood, or stepping forward (walking, running),
// back (giving ground facing up), across (sliding in his stance).
export type StepMode = "stand" | "walk" | "run" | "back" | "slide";

export type StepIn = {
	// Over this stretch (ms), his legs are his own to step.
	from: number;
	to: number;
	// Where his weight is over the floor (his hips), and the way he faces.
	at: (t: number) => Pt;
	yaw: (t: number) => number;
	// Whether he is down in his stance (his feet spread wide).
	stance: (t: number) => boolean;
	// His steps a second, going v (feet a second) that way.
	cadence: (mode: Exclude<StepMode, "stand">, v: number) => number;
	// How tall he is (feet).
	height: number;
	// Where his feet are as it starts, if he is coming out of something
	// (otherwise under him, set) - and whether he comes down on both as it
	// starts, under way or not (landing from a jump), rather than in stride.
	feet?: [Pt, Pt];
	set?: boolean;
	// Where what he is doing has his feet, stood (his right, his left), if it
	// has a stance of its own: a screen wide, his triple threat with one foot
	// back, a jab step out at his man and back. Stood, he steps to them - up
	// as it takes a foot, down where it sets it.
	own?: (t: number) => [Pt, Pt] | undefined;
};

const G = 32.17;
// (The path read on this grid, ms; its going and how hard that changes, each
// over about this long either side.)
const GRID = 25;
const V_OVER = 50;
const A_OVER = 100;
// Under this pace (feet a second), over the next moment (ms), he is stood:
// he steps only to stay over his feet.
const STOOD = 1.2;
const STOOD_LOOK = 300;
// Down in his stance, he slides - up to this pace (feet a second): past it
// he is running, as the picture draws him (SLIDE_FTPS in evaluate.ts).
const STANCE_RUN = 11.5;
// Each foot this far out from the line he goes along (feet, for a man 6 ft
// 6 in - in proportion for anybody else): close in running, wider walking,
// and wide in his stance.
const SPREAD: Record<StepMode, number> = {
	stand: 0.55,
	walk: 0.3,
	run: 0.14,
	back: 0.42,
	slide: 0.85,
};
// (In his stance, wider - a defender's stance up on the man with the ball,
// held through a possession: about as wide as his shoulders and a little
// more, not a drill's.)
const STANCE_SPREAD = 0.95;
// How much of a stride each foot is down (see poses.ts GAITS: a walk over
// half - a foot always down - a run under it), going slow to quick.
const DUTY: Record<Exclude<StepMode, "stand">, [number, number]> = {
	walk: [0.62, 0.58],
	run: [0.36, 0.25],
	back: [0.6, 0.5],
	slide: [0.62, 0.5],
};
// (Slow and quick, for those: feet a second.)
const DUTY_V: Record<Exclude<StepMode, "stand">, [number, number]> = {
	walk: [2, 6.5],
	run: [7, 20],
	back: [2, 10],
	slide: [2, 10],
};
// A foot is up at least this long between one time down and the next (ms):
// walking, sliding; running.
const SWING_LEAST = 180;
const SWING_LEAST_RUN = 230;
// Stood: his weight further than this (feet) off the middle of his feet, or
// turned more than this from the way they point, and he steps (a step this
// long, ms) - the foot whose place under him is the further off.
const STAY_OVER = 0.4;
const STAY_TURN = (40 * Math.PI) / 180;
const SET_STEP = 260;
// Stepping any way but running, a foot stays to its own side of the other,
// at least this far (feet, for a man 6 ft 6 in) - sliding, in his stance,
// further: his feet closing, never together.
const SIDE_LEAST = 0.3;
const SLIDE_LEAST = 0.5;
// Stood in a stance of his move's own (see StepIn.own): a foot further than
// this from where it has it (feet, for a man 6 ft 6 in) steps there - looked
// at so far ahead (ms), as a step takes that long to get there. It comes down
// once his move has it still (moving under OWN_STILL feet a second), a swing
// at least on (ms), and no later than so long. And a foot down stays down at
// least so long (ms) before it steps again.
const OWN_OFF = 0.35;
const OWN_AHEAD = 100;
const OWN_STILL = 1;
const OWN_SWING = [150, 320] as const;
const OWN_STAY = 120;
// Pulling up, his last step going comes down this far (feet) out in front of
// where he stops - the other then set as far behind it.
const STAGGER = 0.55;
// (No further out from under him than this, feet, as it comes down.)
const STAGGER_REACH = 1.9;
// Sliding, his steps a second: so many, and more the quicker he goes (a
// second for each foot a second), up to so many.
const SLIDE_RATE = [2.4, 0.36, 5.5] as const;
// Leaned no further than this (feet off under him): what is left of a pull
// harder than that he takes on more steps.
const LEAN_MOST = 1.6;
// And no further out from under him, as a foot comes down, than this (feet):
// about half as far as his leg is long (a hard stop's plant - further, and
// his hips would have to sink a long way down to get to it).
const REACH_MOST = 1.7;
// A foot down holds him only till his hips are this far past it (feet):
// then it is up - and, running, the other down no more than so long after
// (ms). (Looked for so far ahead, ms.)
const TRAIL = 2;
const FLIGHT_MOST = 160;
const OUTRUN_LOOK = 2000;
// Walking or running, under him a little before halfway through its time
// down (a share of it): he comes down on it a touch less out in front than
// he leaves it behind (walking, half a step out in front as it comes down,
// and up off it as the other has been down a moment).
const UNDER = 0.42;
// (His weight this high, as a share of his height: what he leans on.)
const HIPS = 0.55;
// Walking under this pace (feet a second), running from it - or going it
// as the step to come comes down: getting going hard, the step after a
// walking one is a running one (a foot down a run's short time, not a
// walk's, out in front of him only as far as a runner's).
const RUN_FROM = 6;
// Going, he steps the way he goes; under this pace (feet a second), the way
// he faces.
const POINT_WITH = 4;
// Toes turned out (radians): a little going, more in his stance.
const TOE_OUT = (8 * Math.PI) / 180;
const TOE_OUT_STANCE = (14 * Math.PI) / 180;

const wrap = (a: number) => a - 2 * Math.PI * Math.round(a / (2 * Math.PI));
const clamp01 = (x: number) => (x < 0 ? 0 : x > 1 ? 1 : x);

// A stretch's steps, planned as far on as they are wanted - a long stretch
// a little at a time, not all of it the first time it is looked at.
export type FootPlanner = {
	// His steps so far (in the order they were planned), and each foot's:
	// right, left.
	steps: FootStep[];
	feet: [FootStep[], FootStep[]];
	// Plans on till his steps through t are settled.
	through: (t: number) => void;
};
// (Planned on this far past where it is wanted, ms: no later step changes
// one this far back. Kept that far ahead as it is wanted - a step or so at a
// time, as the game plays - rather than a whole stretch of it at once each
// time it runs out: a frame here and there that took that much longer to
// draw.)
const SETTLED = 2500;

// Every step of a whole stretch.
export const footsteps = (x: StepIn): FootStep[] => {
	const plan = footPlanner(x);
	plan.through(x.to);
	return plan.steps;
};

export const footPlanner = (x: StepIn): FootPlanner => {
	const scale = x.height / 6.5;
	const h = HIPS * x.height;
	const { from, to } = x;
	// The path on the grid: where, how fast, how hard that is changing (each
	// point read the first time it is wanted).
	const n = Math.max(2, Math.ceil((to - from) / GRID) + 1);
	const T = (i: number) => Math.min(to, from + i * GRID);
	const Ps: Pt[] = [];
	const P = (i: number): Pt => (Ps[i] ??= x.at(T(i)));
	const idx = (t: number) =>
		Math.max(0, Math.min(n - 1, Math.round((t - from) / GRID)));
	// (The way he faces and whether he is down in his stance, read on the
	// same grid - each the first time it is wanted.)
	const Y: number[] = [];
	const S: boolean[] = [];
	const yawAt = (t: number) => {
		const i = idx(t);
		return (Y[i] ??= x.yaw(T(i)));
	};
	const stanceAt = (t: number) => {
		const i = idx(t);
		return (S[i] ??= x.stance(T(i)));
	};
	// (And where his move has his feet - null for none.)
	const O: ([Pt, Pt] | null)[] = [];
	const ownAt = (t: number): [Pt, Pt] | undefined => {
		if (!x.own) {
			return undefined;
		}
		const i = idx(t);
		let o = O[i];
		if (o === undefined) {
			o = x.own(T(i)) ?? null;
			O[i] = o;
		}
		return o ?? undefined;
	};
	const span = (k: number) => Math.max(1, Math.round(k / GRID));
	const velAt = (t: number): Pt => {
		const i = idx(t);
		const k = span(V_OVER);
		const a = Math.max(0, i - k);
		const b = Math.min(n - 1, i + k);
		const dt = (T(b) - T(a)) / 1000;
		if (dt <= 0) {
			return { x: 0, y: 0 };
		}
		const A = P(a);
		const B = P(b);
		return { x: (B.x - A.x) / dt, y: (B.y - A.y) / dt };
	};
	const accAt = (t: number): Pt => {
		const k = A_OVER;
		const a = velAt(Math.max(from, t - k));
		const b = velAt(Math.min(to, t + k));
		const dt = (Math.min(to, t + k) - Math.max(from, t - k)) / 1000;
		return dt > 0
			? { x: (b.x - a.x) / dt, y: (b.y - a.y) / dt }
			: { x: 0, y: 0 };
	};
	const posAt = (t: number): Pt => {
		const u = (Math.max(from, Math.min(to, t)) - from) / GRID;
		const i = Math.min(n - 2, Math.floor(u));
		const w = u - i;
		const A = P(i);
		const B = P(i + 1);
		return { x: A.x + (B.x - A.x) * w, y: A.y + (B.y - A.y) * w };
	};
	const paceAt = (t: number) => {
		const v = velAt(t);
		return Math.hypot(v.x, v.y);
	};
	// Stood over the next moment.
	const stoodAt = (t: number) => {
		for (let s = t; s <= Math.min(to, t + STOOD_LOOK); s += GRID * 2) {
			if (paceAt(s) >= STOOD) {
				return false;
			}
		}
		return true;
	};
	// How he steps at t: the way he goes against the way he faces.
	const modeAt = (t: number): StepMode => {
		if (stoodAt(t)) {
			return "stand";
		}
		const v = velAt(t);
		const pace = Math.hypot(v.x, v.y);
		const off = Math.abs(wrap(Math.atan2(v.y, v.x) - yawAt(t)));
		if (
			(stanceAt(t) && pace < STANCE_RUN) ||
			(pace < 11 && off > Math.PI * 0.3 && off < Math.PI * 0.72)
		) {
			return "slide";
		}
		if (off >= Math.PI * 0.72 && pace < 11) {
			return "back";
		}
		return pace < RUN_FROM ? "walk" : "run";
	};
	// Out to his right (a unit), going that way at t: for stepping forward
	// or back, square to the way he goes; stood or sliding, to the way he
	// faces.
	const rightAt = (t: number, mode: StepMode): Pt => {
		const v = velAt(t);
		const pace = Math.hypot(v.x, v.y);
		const dir =
			(mode === "walk" || mode === "run") && pace > 0.5
				? Math.atan2(v.y, v.x)
				: yawAt(t);
		// (The floor's y runs toward the camera: his left is (sin, -cos) - see
		// bodyPoint in evaluate.ts.)
		return { x: -Math.sin(dir), y: Math.cos(dir) };
	};
	const spreadOf = (t: number, mode: StepMode) =>
		(stanceAt(t) ? Math.max(SPREAD[mode], STANCE_SPREAD) : SPREAD[mode]) *
		scale;
	const toeAt = (t: number, mode: StepMode, foot: 0 | 1) => {
		const v = velAt(t);
		const pace = Math.hypot(v.x, v.y);
		const face = yawAt(t);
		const dir =
			(mode === "walk" || mode === "run") && pace > POINT_WITH
				? Math.atan2(v.y, v.x)
				: face;
		const out = stanceAt(t) || mode === "slide" ? TOE_OUT_STANCE : TOE_OUT;
		// (Turned out: the right foot round to his right.)
		return wrap(dir + (foot === 0 ? -out : out));
	};
	// Where a foot comes down for a time down over [t0, t1]: under him
	// halfway through it, out to its side, and leaned off how hard his going
	// changes over it.
	const spotFor = (foot: 0 | 1, t0: number, t1: number, mode: StepMode): Pt => {
		const mid =
			t0 + (t1 - t0) * (mode === "run" || mode === "walk" ? UNDER : 0.5);
		const c = posAt(mid);
		let ax = 0;
		let ay = 0;
		let k = 0;
		for (let s = t0; s <= t1; s += GRID) {
			const a = accAt(s);
			ax += a.x;
			ay += a.y;
			k += 1;
		}
		ax /= Math.max(1, k);
		ay /= Math.max(1, k);
		let lx = (-h / G) * ax;
		let ly = (-h / G) * ay;
		const l = Math.hypot(lx, ly);
		const most = LEAN_MOST * scale;
		if (l > most) {
			lx *= most / l;
			ly *= most / l;
		}
		const r = rightAt(mid, mode);
		const w = spreadOf(mid, mode) * (foot === 0 ? 1 : -1);
		return within({ x: c.x + lx + r.x * w, y: c.y + ly + r.y * w }, t0);
	};
	// (No further out from under him as it comes down, at t0, than he
	// reaches.)
	const within = (at: Pt, t0: number): Pt => {
		const c0 = posAt(t0);
		const d = Math.hypot(at.x - c0.x, at.y - c0.y);
		const reach = REACH_MOST * scale;
		return d <= reach
			? at
			: {
					x: c0.x + ((at.x - c0.x) * reach) / d,
					y: c0.y + ((at.y - c0.y) * reach) / d,
				};
	};
	const steps: FootStep[] = [];
	const feet: [FootStep[], FootStep[]] = [[], []];
	// Each foot's last time down (its step in `steps`).
	const last: [number, number] = [-1, -1];
	const put = (s: FootStep) => {
		steps.push(s);
		feet[s.foot].push(s);
		last[s.foot] = steps.length - 1;
	};
	// His beat going that way at that pace (ms a step), and how long each
	// foot is down for it.
	const beatOf = (mode: Exclude<StepMode, "stand">, pace: number) => {
		const rate =
			mode === "slide"
				? Math.min(SLIDE_RATE[2], SLIDE_RATE[0] + SLIDE_RATE[1] * pace)
				: x.cadence(mode, pace);
		const beat = 1000 / Math.max(0.8, rate);
		const [d0, d1] = DUTY[mode];
		const [v0, v1] = DUTY_V[mode];
		const duty = d0 + (d1 - d0) * clamp01((pace - v0) / (v1 - v0));
		return { beat, down: duty * 2 * beat };
	};
	// Which foot steps next, and when the last came down.
	let next: 0 | 1 = 0;
	let t = from;
	let landed = from;
	const going = x.feet || x.set ? "stand" : modeAt(from);
	if (going === "stand") {
		// Set as it starts.
		for (const foot of [0, 1] as const) {
			const at =
				x.feet?.[foot] ??
				ownAt(from)?.[foot] ??
				spotFor(foot, from, from, "stand");
			put({
				foot,
				t0: from,
				t1: Infinity,
				x: at.x,
				y: at.y,
				yaw: toeAt(from, "stand", foot),
			});
		}
	} else {
		// Under way as it starts: mid-stride - his right foot come down a
		// moment ago, his left a step before that (where he was then, going
		// as he goes now).
		const { beat, down } = beatOf(going, paceAt(from));
		const a0 = from - 0.25 * down;
		const a1 = a0 - beat;
		const v = velAt(from);
		const c = posAt(from);
		const back = (a1 + down / 2 - from) / 1000;
		const r = rightAt(from, going);
		const w = spreadOf(from, going);
		const at0 = spotFor(0, a0, a0 + down, going);
		const at1 = {
			x: c.x + v.x * back - r.x * w,
			y: c.y + v.y * back - r.y * w,
		};
		put({
			foot: 1,
			t0: a1,
			t1: a1 + down,
			x: at1.x,
			y: at1.y,
			yaw: toeAt(from, going, 1),
		});
		put({
			foot: 0,
			t0: a0,
			t1: a0 + down,
			x: at0.x,
			y: at0.y,
			yaw: toeAt(from, going, 0),
		});
		next = 1;
		landed = a0;
	}
	let guard = 0;
	// (How far a foot is out to its own side of the other, along his right.)
	const sideOf = (foot: 0 | 1, at: Pt, other: Pt, r: Pt) =>
		((at.x - other.x) * r.x + (at.y - other.y) * r.y) * (foot === 0 ? 1 : -1);
	// Kept to its own side of the other foot, at least so far (feet).
	const keepSide = (
		foot: 0 | 1,
		at: Pt,
		other: Pt,
		r: Pt,
		least: number,
	): Pt => {
		const by = least - sideOf(foot, at, other, r);
		if (by <= 0) {
			return at;
		}
		const k = foot === 0 ? by : -by;
		return { x: at.x + r.x * k, y: at.y + r.y * k };
	};
	// When he is stood from, between t and u (if he is).
	const restBy = (t: number, u: number): number | undefined => {
		for (let s = t; s <= Math.min(to, u); s += GRID) {
			if (stoodAt(s)) {
				return s;
			}
		}
		return undefined;
	};
	// When his hips are first so far past a foot down at p, from s0 - or
	// never, as far as this looks.
	const outrun = (p: Pt, s0: number, limit: number): number => {
		let was = Infinity;
		for (let s = s0; s <= Math.min(to, s0 + OUTRUN_LOOK); s += GRID) {
			const c = posAt(s);
			const d = Math.hypot(c.x - p.x, c.y - p.y);
			if (d > limit && d > was) {
				return s;
			}
			was = d;
		}
		return Infinity;
	};
	const meanYaw = (a: number, b: number) =>
		Math.atan2(Math.sin(a) + Math.sin(b), Math.cos(a) + Math.cos(b));
	let done = false;
	const finish = () => {
		// (Whatever is down as it ends stays down.)
		for (const f of [0, 1] as const) {
			const s = steps[last[f]]!;
			if (s.t1 >= to) {
				s.t1 = Infinity;
			}
		}
		done = true;
	};
	let upTo = from;
	const through = (want: number) => {
		if (done || want + SETTLED <= upTo) {
			return;
		}
		upTo = Math.min(to, want + SETTLED);
		planTo(upTo);
		if (t >= to) {
			finish();
		}
	};
	const planTo = (limit: number) => {
		while (t < limit && guard++ < 1e6) {
			const how = modeAt(t);
			if (how === "stand") {
				// Stood: a foot still up comes down under him; one down stays.
				for (const f of [0, 1] as const) {
					const s = steps[last[f]]!;
					if (s.t1 === Infinity) {
						continue;
					}
					if (s.t1 > t) {
						s.t1 = Infinity;
						continue;
					}
					const land = Math.min(to, Math.max(t, s.t1 + SWING_LEAST));
					const o = steps[last[f === 0 ? 1 : 0]]!;
					const at = keepSide(
						f,
						ownAt(land)?.[f] ?? spotFor(f, land, land + 400, "stand"),
						o,
						rightAt(land, "stand"),
						SIDE_LEAST * scale,
					);
					put({
						foot: f,
						t0: land,
						t1: Infinity,
						x: at.x,
						y: at.y,
						yaw: toeAt(land, "stand", f),
					});
					landed = Math.max(landed, land);
				}
				const R = steps[last[0]]!;
				const L = steps[last[1]]!;
				// ... in a stance of his move's own, a step to where it has a foot
				// he is too far off (the further off of the two) - when it takes it.
				const own = ownAt(Math.min(to, t + OWN_AHEAD));
				if (own) {
					const off = ([R, L] as const).map((s, f) =>
						s.t0 <= t && t - s.t0 >= OWN_STAY
							? Math.hypot(own[f]!.x - s.x, own[f]!.y - s.y)
							: 0,
					);
					const foot: 0 | 1 = off[0]! >= off[1]! ? 0 : 1;
					if (
						off[foot]! > OWN_OFF * scale &&
						t + OWN_SWING[0] < to &&
						t - landed >= OWN_STAY
					) {
						// (Down once his move has it still - or as late as a step
						// takes.)
						let land = t + OWN_SWING[0];
						const most = Math.min(to, t + OWN_SWING[1]);
						for (; land + GRID <= most; land += GRID) {
							const a = ownAt(land)?.[foot];
							const b = ownAt(land + GRID)?.[foot];
							if (
								!a ||
								!b ||
								Math.hypot(b.x - a.x, b.y - a.y) < (OWN_STILL * GRID) / 1000
							) {
								break;
							}
						}
						const o = foot === 0 ? L : R;
						const at = keepSide(
							foot,
							ownAt(land)?.[foot] ?? own[foot]!,
							o,
							rightAt(land, "stand"),
							(SIDE_LEAST / 2) * scale,
						);
						steps[last[foot]]!.t1 = t;
						put({
							foot,
							t0: land,
							t1: Infinity,
							x: at.x,
							y: at.y,
							yaw: toeAt(land, "stand", foot),
						});
						landed = land;
						next = foot === 0 ? 1 : 0;
						t = land;
						continue;
					}
					t += GRID * 2;
					continue;
				}
				// ... and otherwise a step only to stay over his feet: his weight
				// off them, or turned from the way they point.
				const c = posAt(Math.min(to, t + 150));
				const mid = { x: (R.x + L.x) / 2, y: (R.y + L.y) / 2 };
				const off = Math.hypot(c.x - mid.x, c.y - mid.y) / scale;
				const turned = Math.abs(wrap(yawAt(t) - meanYaw(R.yaw, L.yaw)));
				if (
					R.t0 <= t &&
					L.t0 <= t &&
					(off > STAY_OVER || turned > STAY_TURN) &&
					t - landed >= SET_STEP
				) {
					const land = Math.min(to, t + SET_STEP);
					const want = ([0, 1] as const).map((f) =>
						spotFor(f, land, land + 400, "stand"),
					);
					const d0 = Math.hypot(want[0]!.x - R.x, want[0]!.y - R.y);
					const d1 = Math.hypot(want[1]!.x - L.x, want[1]!.y - L.y);
					const foot: 0 | 1 = d0 >= d1 ? 0 : 1;
					const o = foot === 0 ? L : R;
					const at = keepSide(
						foot,
						want[foot]!,
						o,
						rightAt(land, "stand"),
						SIDE_LEAST * scale,
					);
					steps[last[foot]]!.t1 = t;
					put({
						foot,
						t0: land,
						t1: Infinity,
						x: at.x,
						y: at.y,
						yaw: toeAt(land, "stand", foot),
					});
					landed = land;
					next = foot === 0 ? 1 : 0;
					t = land;
					continue;
				}
				t += GRID * 2;
				continue;
			}
			// Going: the next step on his own beat (sliding, quick short steps).
			// (On the beat of how fast he is going over this step, not as it
			// starts: getting going hard, his steps quicken with him.)
			const mode: Exclude<StepMode, "stand"> =
				how === "walk" &&
				paceAt(Math.min(to, t + beatOf(how, paceAt(t)).beat)) >= RUN_FROM
					? "run"
					: how;
			const { beat: beat0 } = beatOf(mode, paceAt(t));
			const { beat, down } = beatOf(mode, paceAt(Math.min(to, t + beat0 / 2)));
			const swingLeast = mode === "run" ? SWING_LEAST_RUN : SWING_LEAST;
			const R = steps[last[0]]!;
			const L = steps[last[1]]!;
			const setOff = R.t1 === Infinity && L.t1 === Infinity;
			if (setOff && paceAt(t) < STOOD / 2 && t + GRID < to) {
				// (Set off as he gets going - not a foot up a moment before he
				// moves, to come down as he only starts to.)
				t += GRID;
				continue;
			}
			const r = rightAt(t, mode);
			if (setOff) {
				const v = velAt(t + 100);
				if (mode === "run" || mode === "walk") {
					// Setting off forward: the foot further back steps first.
					const c = posAt(t);
					const back = (s: FootStep) => (s.x - c.x) * v.x + (s.y - c.y) * v.y;
					next = back(R) <= back(L) ? 0 : 1;
				} else {
					// Across or back: the foot on the side he goes steps out first.
					next = v.x * r.x + v.y * r.y >= 0 ? 0 : 1;
				}
			}
			let land = setOff
				? t + Math.max(swingLeast, beat * 0.6)
				: Math.max(t, landed + beat);
			// (And down before he is past the foot he is on - or, running, only
			// a moment's flight after it comes up.)
			{
				const o = steps[last[next === 0 ? 1 : 0]]!;
				if (o.t1 > t) {
					const out = outrun(o, Math.max(t, o.t0), TRAIL * scale);
					land = Math.min(land, out + (mode === "run" ? FLIGHT_MOST : 0));
				}
			}
			// Pulling up before then: this foot comes down beside the other, set,
			// and the other - braking - stays down.
			const rest = setOff ? undefined : restBy(t, land);
			if (rest !== undefined) {
				const prev = steps[last[next]]!;
				const o = steps[last[next === 0 ? 1 : 0]]!;
				const lift = prev.t1 === Infinity || prev.t1 > t ? t : prev.t1;
				prev.t1 = Math.max(prev.t0, lift);
				const set = Math.min(to, Math.max(rest, prev.t1 + swingLeast));
				// (Where his move has it, if it has a stance of its own - or under
				// him, at its side: behind him as far as the other is out in front
				// braking, up to a stagger, so his weight is between them. A foot
				// left behind steps up after, to stay over his feet.)
				let at: Pt | undefined = ownAt(set)?.[next];
				if (!at) {
					at = spotFor(next, set, set + 400, "stand");
					const c = posAt(set);
					const v = velAt(Math.max(from, set - 150));
					const p = Math.hypot(v.x, v.y);
					if (p > 0.1) {
						const ahead = ((o.x - c.x) * v.x + (o.y - c.y) * v.y) / p;
						const k = Math.min(Math.max(0, ahead), STAGGER * scale) / p;
						at = { x: at.x - v.x * k, y: at.y - v.y * k };
					}
				}
				at = keepSide(
					next,
					at,
					o,
					rightAt(set, "stand"),
					SIDE_LEAST * 2 * scale,
				);
				o.t1 = Infinity;
				put({
					foot: next,
					t0: set,
					t1: Infinity,
					x: at.x,
					y: at.y,
					yaw: toeAt(set, "stand", next),
				});
				landed = set;
				next = next === 0 ? 1 : 0;
				t = set;
				continue;
			}
			let foot = next;
			let at = spotFor(foot, land, land + down, mode);
			// (Stood before the next would come: this one's his last going - out
			// in front of where he pulls up, a short stagger, out to its side.)
			// (Not if that is further out in front of him than such a step goes:
			// a long way yet to where he stops, he steps on toward it.)
			const stops = restBy(land, land + beat);
			if (stops !== undefined) {
				const c = posAt(stops);
				const v = velAt(land);
				const p = Math.hypot(v.x, v.y);
				const rr = rightAt(stops, "stand");
				const w = SPREAD.stand * scale * (foot === 0 ? 1 : -1);
				const ahead = p > 0.1 ? (STAGGER * scale) / p : 0;
				const last = {
					x: c.x + v.x * ahead + rr.x * w,
					y: c.y + v.y * ahead + rr.y * w,
				};
				const c0 = posAt(land);
				if (Math.hypot(last.x - c0.x, last.y - c0.y) <= STAGGER_REACH * scale) {
					at = last;
				}
			}
			// (However it came to be put there, no further out from under him
			// than he reaches - only then kept to its own side of the other.)
			at = within(at, land);
			if (mode !== "run") {
				// Never across the other foot, stepping any way but running: if it
				// would be, the other steps instead (out first, then this one
				// closes) - kept to its own side whichever. (This one down only
				// till he is past it, as any: not left down behind him while the
				// other steps on and on.)
				const o = steps[last[foot === 0 ? 1 : 0]]!;
				if (sideOf(foot, at, o, r) < SIDE_LEAST * scale && !setOff) {
					const other: 0 | 1 = foot === 0 ? 1 : 0;
					const mine = steps[last[foot]]!;
					if (mine.t1 === Infinity || mine.t1 > t) {
						mine.t1 = outrun(mine, Math.max(t, mine.t0), TRAIL * scale);
						foot = other;
						at = spotFor(foot, land, land + down, mode);
					}
				}
				at = keepSide(
					foot,
					at,
					steps[last[foot === 0 ? 1 : 0]]!,
					r,
					(mode === "slide" ? SLIDE_LEAST : SIDE_LEAST) * scale,
				);
			}
			const prev = steps[last[foot]]!;
			if (setOff || prev.t1 === Infinity) {
				prev.t1 = Math.max(prev.t0, Math.min(t, land - swingLeast));
			} else {
				// (Up a swing's worth before it comes down again.)
				prev.t1 = Math.max(prev.t0, Math.min(prev.t1, land - swingLeast));
			}
			land = Math.max(land, prev.t1 + swingLeast);
			if (land >= to) {
				t = to;
				break;
			}
			put({
				foot,
				t0: land,
				t1: Math.min(land + down, outrun(at, land, TRAIL * scale)),
				x: at.x,
				y: at.y,
				yaw: toeAt(land + down / 2, mode, foot),
			});
			landed = land;
			next = foot === 0 ? 1 : 0;
			t = land;
		}
	};
	return { steps, feet, through };
};
