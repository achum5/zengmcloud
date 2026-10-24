// HOW A MAN GETS ABOUT THE FLOOR: how quickly he gets up to a pace and pulls
// up from it, how long a run of his takes, and how far along it he is at any
// moment - one account of it for the director, scheduling runs, and for the
// court, showing them.
//
// Getting going, he pushes hardest from a standstill and less and less as
// he picks up pace, closing on a top speed he never quite reaches - the way
// sprinters are timed doing it: a step or two in a blink, flat out in a
// second and more. Pulling up, he brakes about as hard all the way down. A
// run timed tighter than that - a man somewhere already, or in a hurry the
// schedule insists on - is taken as hard as it has to be.

// The pace he closes on getting going (feet a second) and how quickly
// (seconds): from a standstill, eight feet a second takes him a third of a
// second, twenty-one a second and a quarter. And how hard he brakes (feet a
// second squared).
const TOP = 27;
const TAU = 0.85;
const BRAKE = 22;

// How hard (feet a second, a second) a man going v can still speed up: the
// quicker he already is, the less he has in hand.
export const pushFrom = (v: number): number =>
	(TOP - Math.min(Math.max(0, v), TOP - 0.5)) / TAU;

// How fast a run that follows straight on from another carries its pace
// through the join: all of it straight on, a third of it round a right
// angle, none turning back.
export const keepThrough = (cos: number): number =>
	Math.max(0, cos * 0.5 + 0.5) ** 1.5;

// A ramp from one pace to another: how long it takes (s), at `k` times the
// usual push.
const ramp = (from: number, to: number, k: number): number => {
	if (to < from) {
		return (from - to) / (BRAKE * k);
	}
	const a = Math.min(from, TOP - 0.5);
	const b = Math.min(to, TOP - 0.5);
	return (TAU / k) * Math.log((TOP - a) / (TOP - b)) + Math.max(0, to - b) / 4;
};

// The area under an S-shaped ramp from 0 to 1, x of the way along it.
const rampArea = (x: number) => x * x * x - (x * x * x * x) / 2;

// Getting going over a ramp `t` seconds long, from `from` up to `to`: the
// push he has in hand all of it, from the first step - hardest then, less and
// less as he picks up pace (see pushFrom) - not built up to. The time
// constant of that push (s); nothing for a ramp that eases in and out
// instead: pulling up, or getting up to a pace he never quite reaches.
const upTau = (from: number, to: number, t: number): number | undefined =>
	to <= from + 1e-9 || to > TOP - 0.5 || t <= 0
		? undefined
		: t / Math.log((TOP - Math.max(0, from)) / (TOP - to));

// How far (feet) a ramp from one pace to another `t` seconds long takes him,
// and how far along it he is `s` seconds in.
const rampDist = (from: number, to: number, t: number): number => {
	const tau = upTau(from, to, t);
	return tau === undefined
		? ((from + to) / 2) * t
		: TOP * t - tau * (to - from);
};
const rampAlong = (from: number, to: number, t: number, s: number): number => {
	const tau = upTau(from, to, t);
	return tau === undefined
		? from * s + (to - from) * t * rampArea(s / t)
		: TOP * s - tau * (TOP - Math.max(0, from)) * (1 - Math.exp(-s / tau));
};
// How fast he is going `s` seconds into it, and how hard he is speeding up
// (feet a second, a second - slowing, less than nothing).
const rampPace = (from: number, to: number, t: number, s: number): number => {
	const tau = upTau(from, to, t);
	const x = Math.min(1, Math.max(0, s / t));
	return tau === undefined
		? from + (to - from) * x * x * (3 - 2 * x)
		: TOP - (TOP - Math.max(0, from)) * Math.exp(-Math.min(s, t) / tau);
};
const rampPush = (from: number, to: number, t: number, s: number): number => {
	const tau = upTau(from, to, t);
	const x = Math.min(1, Math.max(0, s / t));
	return tau === undefined
		? ((to - from) / t) * 6 * x * (1 - x)
		: ((TOP - Math.max(0, from)) / tau) * Math.exp(-Math.min(s, t) / tau);
};

// The way a run goes: up (or down) from its pace at the start to `vc`, on at
// that, then to its pace at the end.
export type RunShape = {
	T: number;
	v0: number;
	v1: number;
	vc: number;
	ta: number;
	td: number;
	L: number;
};

// The shape of a run L feet long taking T seconds, from pace v0 to pace v1:
// the pace he cruises at between his ramps, found by trying them out - at
// the usual push if that gets him there in time, harder if it has to be.
// (The farther he has to go in the time, the faster he cruises and the
// longer his ramps - as far as both ramps fitting in, flat out.)
export const runShape = (L: number, T: number, v0 = 0, v1 = 0): RunShape => {
	if (T <= 0.001 || L <= 0) {
		return { T: Math.max(T, 0.001), v0, v1, vc: 0, ta: 0, td: 0, L };
	}
	const STEPS = 80;
	const fits = (vc: number, k: number) =>
		ramp(v0, vc, k) + ramp(vc, v1, k) <= T + 1e-9;
	const far = (vc: number, k: number) => {
		const ta = ramp(v0, vc, k);
		const td = ramp(vc, v1, k);
		return (
			rampDist(v0, vc, ta) +
			vc * Math.max(0, T - ta - td) +
			rampDist(vc, v1, td)
		);
	};
	// Between a cruise that fits (a) and one that doesn't (b), the edge.
	const edge = (a: number, b: number, k: number) => {
		for (let n = 0; n < 28; n++) {
			const m = (a + b) / 2;
			if (fits(m, k)) {
				a = m;
			} else {
				b = m;
			}
		}
		return a;
	};
	for (let k = 1; k < 100; k *= 1.1) {
		const hi = Math.max(v0, v1, (2 * L) / T) * 1.5 + 1;
		// The cruises that fit both ramps in: from the slowest to the fastest.
		let first = -1;
		let last = -1;
		for (let i = 0; i <= STEPS; i++) {
			if (fits((hi * i) / STEPS, k)) {
				if (first < 0) {
					first = i;
				}
				last = i;
			} else if (last >= 0) {
				break;
			}
		}
		if (first < 0) {
			continue;
		}
		const slow =
			first > 0 ? edge((hi * first) / STEPS, (hi * (first - 1)) / STEPS, k) : 0;
		const fast =
			last < STEPS
				? edge((hi * last) / STEPS, (hi * (last + 1)) / STEPS, k)
				: hi;
		// Too little way even easing right off: he has to push harder.
		if (far(slow, k) > L + 1e-6) {
			continue;
		}
		// The cruise that gets him farthest in the time - past it, the ramps
		// up to it and down from it eat more than it gives.
		let p = slow;
		let q = fast;
		for (let n = 0; n < 40; n++) {
			const m1 = p + (q - p) / 3;
			const m2 = q - (q - p) / 3;
			if (far(m1, k) < far(m2, k)) {
				p = m1;
			} else {
				q = m2;
			}
		}
		const best = (p + q) / 2;
		// Too far to get even so: he has to push harder.
		if (far(best, k) < L - 1e-6) {
			continue;
		}
		let a = slow;
		let b = best;
		for (let n = 0; n < 30; n++) {
			const m = (a + b) / 2;
			if (far(m, k) < L) {
				a = m;
			} else {
				b = m;
			}
		}
		return { T, v0, v1, vc: b, ta: ramp(v0, b, k), td: ramp(b, v1, k), L };
	}
	// Nothing fits: straight there at an even pace.
	return { T, v0: L / T, v1: L / T, vc: L / T, ta: 0, td: 0, L };
};

// How far along it (feet) a run of this shape is, s seconds in.
export const alongShape = (r: RunShape, s: number): number => {
	const { T, v0, v1, vc, ta, td, L } = r;
	if (s <= 0) {
		return 0;
	}
	if (s >= T) {
		return L;
	}
	let d: number;
	if (s < ta) {
		d = rampAlong(v0, vc, ta, s);
	} else if (s <= T - td) {
		d = rampDist(v0, vc, ta) + vc * (s - ta);
	} else {
		d =
			rampDist(v0, vc, ta) +
			vc * (T - ta - td) +
			rampAlong(vc, v1, td, s - (T - td));
	}
	return Math.min(L, Math.max(0, d));
};

// How fast (feet a second) a run of this shape has him going s seconds in -
// and how hard he is speeding up then (feet a second, a second; pulling up,
// less than nothing).
export const paceAlong = (r: RunShape, s: number): number => {
	const { T, v0, v1, vc, ta, td } = r;
	if (s < ta) {
		return rampPace(v0, vc, ta, Math.max(0, s));
	}
	return s <= T - td ? vc : rampPace(vc, v1, td, Math.min(td, s - (T - td)));
};
export const pushAlong = (r: RunShape, s: number): number => {
	const { T, v0, v1, vc, ta, td } = r;
	if (s < 0 || s > T) {
		return 0;
	}
	if (s < ta) {
		return rampPush(v0, vc, ta, s);
	}
	return s <= T - td ? 0 : rampPush(vc, v1, td, s - (T - td));
};

// All out - after a loose ball, up for a rebound - he gets going and pulls
// up this much harder than he does to get somewhere in the run of play:
// the way a player bursts through a shuttle run.
export const BURST = 1.3;

// As hard as a man pulls up (feet a second, a second): all out.
export const HARDEST_PULL_UP = BRAKE * BURST;

// How fast (feet a second) a man going v0 is going L feet on, getting going
// all he can.
export const paceAfter = (L: number, v0 = 0): number => {
	const a = Math.min(Math.max(0, v0), TOP - 0.5);
	const along = (t: number) =>
		TOP * t - TAU * (TOP - a) * (1 - Math.exp(-t / TAU));
	let lo = 0;
	let hi = 10;
	for (let n = 0; n < 40; n++) {
		const m = (lo + hi) / 2;
		if (along(m) < L) {
			lo = m;
		} else {
			hi = m;
		}
	}
	return TOP - (TOP - a) * Math.exp(-(lo + hi) / 2 / TAU);
};

// The pace (feet a second) he can pull up from in L feet, `effort` times as
// hard as he usually brakes.
export const pullUpFrom = (L: number, effort = 1): number =>
	Math.sqrt(2 * BRAKE * effort * Math.max(0, L));

// How long (ms) a run of L feet takes a man going for `speed`, already
// going v0 as he sets off, and pulled up at the end of it - to v1, if he
// goes straight on into something else - `effort` times as hard as he
// usually goes about it.
export const runMs = (
	L: number,
	speed: number,
	v0 = 0,
	effort = 1,
	v1 = 0,
): number => {
	const v = Math.max(0.5, speed);
	const s0 = Math.min(v0, v);
	const s1 = Math.min(v1, v);
	const k = effort;
	const dUp = rampDist(s0, v, ramp(s0, v, k));
	const dDown = rampDist(v, s1, ramp(v, s1, k));
	if (dUp + dDown <= L) {
		return (ramp(s0, v, k) + (L - dUp - dDown) / v + ramp(v, s1, k)) * 1000;
	}
	// Never up to it: as fast as he gets in the room there is.
	let a = Math.max(s0, s1);
	let b = v;
	for (let n = 0; n < 24; n++) {
		const m = (a + b) / 2;
		const dd =
			rampDist(s0, m, ramp(s0, m, k)) + rampDist(m, s1, ramp(m, s1, k));
		if (dd < L) {
			a = m;
		} else {
			b = m;
		}
	}
	const vp = (a + b) / 2;
	return (ramp(s0, vp, k) + ramp(vp, s1, k)) * 1000;
};

// The pace (feet a second) to go for to cover L feet in `ms` from a pace of
// v0, pulling up at the end - no faster than `most`.
export const paceFor = (
	L: number,
	ms: number,
	v0 = 0,
	most = Infinity,
	effort = 1,
): number => {
	const cap = Math.min(most, 60);
	if (runMs(L, cap, v0, effort) >= ms) {
		return cap;
	}
	let a = 0.5;
	let b = cap;
	for (let n = 0; n < 22; n++) {
		const m = (a + b) / 2;
		if (runMs(L, m, v0, effort) > ms) {
			a = m;
		} else {
			b = m;
		}
	}
	return b;
};

// The farthest (feet) a man going for `speed` gets in `ms` - from
// standing, and pulled up at the end of it (see runMs).
export const reachIn = (ms: number, speed: number, effort = 1): number => {
	if (ms <= 0) {
		return 0;
	}
	let a = 0;
	let b = (Math.max(0.5, speed) * ms) / 1000;
	if (runMs(b, speed, 0, effort) <= ms) {
		return b;
	}
	for (let n = 0; n < 24; n++) {
		const m = (a + b) / 2;
		if (runMs(m, speed, 0, effort) <= ms) {
			a = m;
		} else {
			b = m;
		}
	}
	return a;
};
