import type { Leg, Play } from "./director.ts";
import { TRACK_MS, trackLength, trackOf } from "./pace.ts";
import type { Move } from "./poses.ts";

// KEPT OFF EACH OTHER: never one man drawn standing in another. However the
// play left them - two men's ways crossing, a man in coverage on the
// receiver's heels, the whole field walking back to the huddles after the
// whistle - wherever two would be in each other, each is eased a little way
// aside, over a few steps before and back after, so they go round rather
// than through. Not men meant to be on each other: blocking, tackled, in a
// pile, handing the ball off, shaking hands - nor a man the ball is coming
// to or leaving. An official in a man's way gives him it: it is the
// official who goes round.
// Worked out once a play, the first time it is looked at.

// Every this long (ms) along the play, how far over each man is (feet).
export const SEP_MS = TRACK_MS;
export type Sep = { t0: number; dx: Float32Array; dy: Float32Array };

// Center to center, no nearer (feet); how many goes at it; smoothed over
// this many samples either side (twice over).
const APART = 2.1;
const ROUNDS = 3;
const SMOOTH = 4;
const PASSES = 3;
// (Pushed over and let back no quicker than this, ft/s: not slid aside.)
const SLIDE_FPS = 3.5;
// (Coming by each other quicker than this (ft/s), over across the way they
// come - all of it by this - not shoved back along it.)
const BY_FPS = 2;
const BY_ALL_FPS = 5;
// (Samples either side of a push that the push, smoothed, reaches.)
const NEAR = SMOOTH * 2 + 1;
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
	"holdPlace",
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
	"hurtProne",
	"hurtKnee",
	"hurtUp",
	"tend",
	"helpUp",
	"takeKnee",
	"shake",
	"hug",
	"chestBump",
	"highFive",
	"coolerCarry",
	"coolerDump",
	"doused",
]);

const touching = (l: Leg) =>
	TOUCH.has(l.move) || (l.arms !== undefined && TOUCH.has(l.arms));

export const keepApart = (play: Play): Map<number, Sep[]> => {
	const men = [
		...play.parts.filter((p) => p.pid >= 0),
		...(play.coaches ?? []),
		...(play.toss?.men ?? []),
		...play.crew,
	].filter((p) => p.legs.length > 0);
	const crew = new Set(play.crew);
	const official = men.map((m) => crew.has(m as (typeof play.crew)[number]));
	const t0 = play.t0;
	const n = trackLength(t0, play.t1);
	const tracks = men.map((m) => trackOf(m.legs, t0, n));
	const xs = tracks.map((tr) => tr.xs.slice());
	const ys = tracks.map((tr) => tr.ys);
	const free = men.map((m, i) => {
		const k = tracks[i]!.k;
		const out = new Uint8Array(n);
		for (let s = 0; s < n; s++) {
			out[s] = touching(m.legs[k[s]!]!) ? 0 : 1;
		}
		return out;
	});
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

	// (Each man pushed aside where he is in somebody, and that smoothed out
	// - and then, where that left anybody still in somebody, again.)
	const m = men.length;
	const sx: Float32Array[] = men.map(() => new Float32Array(n));
	const sy: Float32Array[] = men.map(() => new Float32Array(n));
	let near: Uint8Array | undefined;
	for (let pass = 0; pass < PASSES; pass++) {
		const bx =
			pass === 0 ? xs : xs.map((x, i) => x.map((v, s) => v + sx[i]![s]!));
		const by =
			pass === 0 ? ys : ys.map((y, i) => y.map((v, s) => v + sy[i]![s]!));
		const { dx, dy, hit } = pushed(bx, by, free, official, n, near);
		if (!hit) {
			break;
		}
		for (let i = 0; i < m; i++) {
			const ax = smooth(smooth(eased(dx[i]!)));
			const ay = smooth(smooth(eased(dy[i]!)));
			for (let s = 0; s < n; s++) {
				sx[i]![s] = sx[i]![s]! + ax[s]!;
				sy[i]![s] = sy[i]![s]! + ay[s]!;
			}
		}
		// (Nobody in anybody after it but near where somebody was before.)
		near = new Uint8Array(n);
		for (let s = 0; s < n; s++) {
			if (hit[s]) {
				near.fill(1, Math.max(0, s - NEAR), Math.min(n, s + NEAR + 1));
			}
		}
	}
	// (And all of it, put together, no quicker than that either.)
	for (let i = 0; i < m; i++) {
		sx[i] = smooth(eased(sx[i]!));
		sy[i] = smooth(eased(sy[i]!));
	}

	const out = new Map<number, Sep[]>();
	men.forEach((m, i) => {
		const sxi = sx[i]!;
		const syi = sy[i]!;
		const spans: Sep[] = [];
		let s = 0;
		while (s < n) {
			if (Math.abs(sxi[s]!) < SLIGHT && Math.abs(syi[s]!) < SLIGHT) {
				s++;
				continue;
			}
			const from = Math.max(0, s - 1);
			while (
				s < n &&
				(Math.abs(sxi[s]!) >= SLIGHT || Math.abs(syi[s]!) >= SLIGHT)
			) {
				s++;
			}
			const to = Math.min(n, s + 1);
			spans.push({
				t0: t0 + from * SEP_MS,
				dx: sxi.slice(from, to),
				dy: syi.slice(from, to),
			});
		}
		if (spans.length > 0) {
			out.set(m.pid, spans);
		}
	});
	return out;
};

// How far over each is pushed, at each time on their tracks, to be apart
// there (see keepApart).
const pushed = (
	xs: Float32Array[],
	ys: Float32Array[],
	free: Uint8Array[],
	official: boolean[],
	n: number,
	near: Uint8Array | undefined,
) => {
	const m = xs.length;
	const dx = xs.map(() => new Float32Array(n));
	const dy = xs.map(() => new Float32Array(n));
	let hit: Uint8Array | undefined;
	// (After the first go, only those moved since they were last looked at:
	// the rest are where they were, and were apart then.)
	let was = new Uint8Array(m);
	let now = new Uint8Array(m);
	// (Where each is, moved over so far, and whether he can be.)
	const px = new Float64Array(m);
	const py = new Float64Array(m);
	const fr = new Uint8Array(m);
	for (let s = 0; s < n; s++) {
		if (near && !near[s]) {
			continue;
		}
		for (let i = 0; i < m; i++) {
			px[i] = xs[i]![s]!;
			py[i] = ys[i]![s]!;
			fr[i] = free[i]![s]!;
		}
		for (let round = 0; round < ROUNDS; round++) {
			let moved = false;
			[was, now] = [now, was];
			now.fill(0);
			for (let i = 0; i < m; i++) {
				const fi = fr[i]!;
				for (let j = i + 1; j < m; j++) {
					const fj = fr[j]!;
					if (!fi && !fj) {
						continue;
					}
					if (round > 0 && !was[i] && !was[j] && !now[i] && !now[j]) {
						continue;
					}
					const ex = px[i]! - px[j]!;
					if (ex >= APART || ex <= -APART) {
						continue;
					}
					const ey = py[i]! - py[j]!;
					if (ey >= APART || ey <= -APART) {
						continue;
					}
					const d = Math.sqrt(ex * ex + ey * ey);
					if (d >= APART) {
						continue;
					}
					// (Right on top of each other: apart across the field.)
					let ux = d > 1e-3 ? ex / d : 0;
					let uy = d > 1e-3 ? ey / d : 1;
					// (Coming by each other: round each other, the side they
					// would go by on - the same all the way by, as the line
					// between them swings round.)
					const vx = pace(xs[i]!, s, n) - pace(xs[j]!, s, n);
					const vy = pace(ys[i]!, s, n) - pace(ys[j]!, s, n);
					const v = Math.sqrt(vx * vx + vy * vy);
					if (v > BY_FPS) {
						const nx = -vy / v;
						const ny = vx / v;
						const k = ex * nx + ey * ny < -0.05 ? -1 : 1;
						const w = Math.min(1, (v - BY_FPS) / (BY_ALL_FPS - BY_FPS));
						const qx = ux * (1 - w) + nx * k * w;
						const qy = uy * (1 - w) + ny * k * w;
						const q = Math.sqrt(qx * qx + qy * qy);
						if (q > 1e-3) {
							ux = qx / q;
							uy = qy / q;
						}
					}
					// (Each half the way - or, the other where he has to be, or
					// a player, the whole way himself.)
					const push = APART - d;
					const oi = official[i]!;
					const oj = official[j]!;
					const wi = fi ? (fj ? (oi === oj ? 0.5 : oi ? 1 : 0) : 1) : 0;
					const ix = dx[i]!;
					const iy = dy[i]!;
					const jx = dx[j]!;
					const jy = dy[j]!;
					ix[s] = ix[s]! + ux * push * wi;
					iy[s] = iy[s]! + uy * push * wi;
					jx[s] = jx[s]! - ux * push * (1 - wi);
					jy[s] = jy[s]! - uy * push * (1 - wi);
					px[i] = xs[i]![s]! + ix[s]!;
					py[i] = ys[i]![s]! + iy[s]!;
					px[j] = xs[j]![s]! + jx[s]!;
					py[j] = ys[j]![s]! + jy[s]!;
					now[i] = 1;
					now[j] = 1;
					moved = true;
					hit ??= new Uint8Array(n);
					hit[s] = 1;
				}
			}
			if (!moved) {
				break;
			}
		}
	}

	return { dx, dy, hit };
};

// How quick a track goes at sample s, ft/s.
const pace = (a: Float32Array, s: number, n: number): number => {
	const lo = Math.max(0, s - 1);
	const hi = Math.min(n - 1, s + 1);
	return hi > lo ? ((a[hi]! - a[lo]!) / ((hi - lo) * SEP_MS)) * 1000 : 0;
};

// Each push taken up and let go no quicker than SLIDE_FPS: each sample the
// most of any either side of it, less that much for the time between - the
// one way and the other each.
const eased = (a: Float32Array): Float32Array => {
	const r = (SLIDE_FPS * SEP_MS) / 1000;
	const n = a.length;
	const out = new Float32Array(n);
	for (const sign of [1, -1]) {
		const e = new Float32Array(n);
		for (let s = 0; s < n; s++) {
			e[s] = Math.max(0, sign * a[s]!, s > 0 ? e[s - 1]! - r : 0);
		}
		for (let s = n - 2; s >= 0; s--) {
			e[s] = Math.max(e[s]!, e[s + 1]! - r);
		}
		for (let s = 0; s < n; s++) {
			out[s] = out[s]! + sign * e[s]!;
		}
	}
	return out;
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
