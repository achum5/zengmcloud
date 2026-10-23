import type { Leg, Play } from "./director.ts";
import type { Move } from "./poses.ts";

// KEPT OFF EACH OTHER: never one man drawn standing in another. However the
// play left them - two men's ways crossing, a man in coverage on the
// receiver's heels, the whole field walking back to the huddles after the
// whistle - wherever two would be in each other, each is eased a little way
// aside, over a few steps before and back after, so they go round rather
// than through. Not men meant to be on each other: blocking, tackled, in a
// pile, handing the ball off, shaking hands - nor a man the ball is coming
// to or leaving.
// Worked out once a play, the first time it is looked at.

// Every this long (ms) along the play, how far over each man is (feet).
export const SEP_MS = 50;
export type Sep = { t0: number; dx: Float32Array; dy: Float32Array };

// Center to center, no nearer (feet); how many goes at it; smoothed over
// this many samples either side (twice over).
const APART = 2.1;
const ROUNDS = 3;
const SMOOTH = 4;
// (Too little to bother with, feet; and nowhere near anybody.)
const SLIGHT = 0.02;
const GONE = -1e5;
// The man the ball comes to, left where he is from this long (ms) before
// it is his; the man it leaves, until this long after.
const GATHER_MS = 400;
const LET_GO_MS = 200;

// On each other, meant to be.
const TOUCH = new Set<Move>([
	"center",
	"underCenter",
	"qbSurvey",
	"qbPoint",
	"getUnder",
	"holdKneel",
	"holdDown",
	"kickReady",
	"longSnap",
	"kick",
	"engage",
	"drive",
	"handOff",
	"fakeCarry",
	"catch",
	"leapCatch",
	"diveCatch",
	"toeTap",
	"swat",
	"scoop",
	"juke",
	"stiffArm",
	"shove",
	"tackled",
	"tackledBack",
	"dive",
	"prone",
	"supine",
	"getUp",
	"getUpBack",
	"qbSlide",
	"slid",
	"sitUp",
	"pileIn",
	"overPile",
	"offPile",
	"pileDive",
	"pileTop",
	"offTop",
	"kneelDown",
	"leap",
	"hurt",
	"hurtUp",
	"tend",
	"helpUp",
	"takeKnee",
	"shake",
	"hug",
	"chestBump",
]);

const touching = (l: Leg) =>
	TOUCH.has(l.move) || (l.arms !== undefined && TOUCH.has(l.arms));

export const keepApart = (play: Play): Map<number, Sep[]> => {
	const men = [
		...play.parts.filter((p) => p.pid >= 0),
		...(play.coaches ?? []),
		...(play.toss?.men ?? []),
	].filter((p) => p.legs.length > 0);
	const t0 = play.t0;
	const n = Math.max(1, Math.ceil((play.t1 - t0) / SEP_MS) + 1);
	const xs = men.map(() => new Float32Array(n));
	const ys = men.map(() => new Float32Array(n));
	const free = men.map(() => new Uint8Array(n));
	men.forEach((m, i) => sample(m, t0, n, xs[i]!, ys[i]!, free[i]!));
	// (The captains out for the coin toss gone once it is done.)
	const toss = play.toss;
	if (toss) {
		const from = Math.max(0, Math.ceil((toss.cut - t0) / SEP_MS));
		men.forEach((m, i) => {
			if (toss.men.includes(m as (typeof toss.men)[number])) {
				xs[i]!.fill(GONE, from);
			}
		});
	}
	// (Nor the man with the ball coming to him, or just gone from him.)
	const index = new Map(men.map((m, i) => [m.pid, i]));
	const hold = (pid: number, from: number, to: number) => {
		const i = index.get(pid);
		if (i === undefined) {
			return;
		}
		const lo = Math.max(0, Math.floor((from - t0) / SEP_MS));
		const hi = Math.min(n - 1, Math.ceil((to - t0) / SEP_MS));
		free[i]!.fill(0, lo, hi + 1);
	};
	let holder: number | undefined;
	for (const s of play.ball) {
		if (s.kind === "held") {
			hold(s.pid, s.t0 - GATHER_MS, s.t0 + 100);
			holder = s.pid;
		} else {
			if (holder !== undefined) {
				hold(holder, s.t0 - 100, s.t0 + LET_GO_MS);
			}
			holder = undefined;
		}
	}

	const dx = men.map(() => new Float32Array(n));
	const dy = men.map(() => new Float32Array(n));
	for (let s = 0; s < n; s++) {
		for (let round = 0; round < ROUNDS; round++) {
			let moved = false;
			for (let i = 0; i < men.length; i++) {
				const fi = free[i]![s]!;
				for (let j = i + 1; j < men.length; j++) {
					const fj = free[j]![s]!;
					if (!fi && !fj) {
						continue;
					}
					const ax = xs[i]![s]! + dx[i]![s]!;
					const bx = xs[j]![s]! + dx[j]![s]!;
					if (Math.abs(ax - bx) >= APART) {
						continue;
					}
					const ay = ys[i]![s]! + dy[i]![s]!;
					const by = ys[j]![s]! + dy[j]![s]!;
					const d = Math.hypot(ax - bx, ay - by);
					if (d >= APART) {
						continue;
					}
					// (Right on top of each other: apart across the field.)
					const ux = d > 1e-3 ? (ax - bx) / d : 0;
					const uy = d > 1e-3 ? (ay - by) / d : 1;
					// (Each half the way - or, the other where he has to be, the
					// whole way himself.)
					const push = APART - d;
					const wi = fi ? (fj ? 0.5 : 1) : 0;
					const ix = dx[i]!;
					const iy = dy[i]!;
					const jx = dx[j]!;
					const jy = dy[j]!;
					ix[s] = ix[s]! + ux * push * wi;
					iy[s] = iy[s]! + uy * push * wi;
					jx[s] = jx[s]! - ux * push * (1 - wi);
					jy[s] = jy[s]! - uy * push * (1 - wi);
					moved = true;
				}
			}
			if (!moved) {
				break;
			}
		}
	}

	const out = new Map<number, Sep[]>();
	men.forEach((m, i) => {
		const sx = smooth(smooth(dx[i]!));
		const sy = smooth(smooth(dy[i]!));
		const spans: Sep[] = [];
		let s = 0;
		while (s < n) {
			if (Math.abs(sx[s]!) < SLIGHT && Math.abs(sy[s]!) < SLIGHT) {
				s++;
				continue;
			}
			const from = Math.max(0, s - 1);
			while (
				s < n &&
				(Math.abs(sx[s]!) >= SLIGHT || Math.abs(sy[s]!) >= SLIGHT)
			) {
				s++;
			}
			const to = Math.min(n, s + 1);
			spans.push({
				t0: t0 + from * SEP_MS,
				dx: sx.slice(from, to),
				dy: sy.slice(from, to),
			});
		}
		if (spans.length > 0) {
			out.set(m.pid, spans);
		}
	});
	return out;
};

// Where he is every SEP_MS from t0 (his legs as they go), and whether he
// is free to be moved over.
const sample = (
	m: { legs: Leg[] },
	t0: number,
	n: number,
	xs: Float32Array,
	ys: Float32Array,
	free: Uint8Array,
) => {
	const legs = m.legs;
	let k = 0;
	for (let s = 0; s < n; s++) {
		const t = t0 + s * SEP_MS;
		while (k + 1 < legs.length && legs[k + 1]!.t0 <= t) {
			k++;
		}
		const l = legs[k]!;
		const u =
			l.t1 > l.t0 ? Math.min(1, Math.max(0, (t - l.t0) / (l.t1 - l.t0))) : 1;
		xs[s] = l.from.x + (l.to.x - l.from.x) * u;
		ys[s] = l.from.y + (l.to.y - l.from.y) * u;
		free[s] = touching(l) ? 0 : 1;
	}
};

// Each sample the mean of those either side of it.
const smooth = (a: Float32Array): Float32Array => {
	const out = new Float32Array(a.length);
	for (let s = 0; s < a.length; s++) {
		let sum = 0;
		let k = 0;
		for (
			let j = Math.max(0, s - SMOOTH);
			j <= Math.min(a.length - 1, s + SMOOTH);
			j++
		) {
			sum += a[j]!;
			k++;
		}
		out[s] = sum / k;
	}
	return out;
};

// How far over he is at t.
export const apartAt = (
	spans: Sep[] | undefined,
	t: number,
): { x: number; y: number } | undefined => {
	if (!spans) {
		return undefined;
	}
	for (const sp of spans) {
		const f = (t - sp.t0) / SEP_MS;
		if (f < 0 || f > sp.dx.length - 1) {
			continue;
		}
		const i = Math.min(sp.dx.length - 2, Math.floor(f));
		const u = f - i;
		if (i < 0) {
			return { x: sp.dx[0]!, y: sp.dy[0]! };
		}
		return {
			x: sp.dx[i]! + (sp.dx[i + 1]! - sp.dx[i]!) * u,
			y: sp.dy[i]! + (sp.dy[i + 1]! - sp.dy[i]!) * u,
		};
	}
	return undefined;
};

const kept = new WeakMap<Play, Map<number, Sep[]>>();
export const apartOf = (play: Play): Map<number, Sep[]> => {
	let got = kept.get(play);
	if (!got) {
		got = keepApart(play);
		kept.set(play, got);
	}
	return got;
};
