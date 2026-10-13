import type {
	BallSeg,
	CourtPlayer,
	CourtTimeline,
	Fx,
	Track,
} from "./director.ts";
import {
	BACKSPIN,
	evalBall,
	evalPlayer,
	withBody,
	type BallState,
	type PlayerState,
} from "./evaluate.ts";
import {
	attackDir,
	COURT_W,
	dist,
	rimPt,
	spot,
	type Pt,
	type Pt3,
	type Side,
} from "./geometry.ts";
import { findShot, SAMPLE_MS, type ShotKind } from "./physics.ts";
import { JUMPER, releaseAt, type Body } from "./poses.ts";
import type { CourtFit, Shot } from "./camera.ts";
import type { Moment } from "./scene.ts";

// THE WARM-UPS, while a game is still being staged: each team out at the
// basket it shoots at to start, in its warm-up tops. At each end a big
// under the rim takes every shot out of the net and feeds it back out to
// the next shooter on his spot, two of them at a time, each moving round
// to a new spot once he has let it go; out toward half court a guard or
// two work on their handle. Every ball is its own little timeline (the
// evaluator plays one ball to a timeline), and the picture puts them
// together.

export type Warmup = {
	// The picture's: the building, the shots at both rims, and every man
	// warming up.
	tl: CourtTimeline;
	// One to a ball: the men it goes between, and its path.
	parts: CourtTimeline[];
};

// How long it is worked out for (ms): longer than staging a game takes,
// even on a slow phone.
export const WARMUP_MS = 60_000;
const PASS_FTPS = 42;
const RELEASE_MS = 120;
const passMs = (d: number) =>
	Math.min(1500, Math.max(260, 180 + (d * 1000) / PASS_FTPS));
const JOG = 9;
// From catching it to letting it go: set, and up.
const SET_MS = 520;
const SHOT_MS = 1000;
const SHOT_PEAK = 1.35;

// Bigs feed, guards handle it, everybody else shoots.
const BIG: Record<string, number> = { C: 0, FC: 1, PF: 2, F: 3 };
const GUARD: Record<string, number> = { PG: 0, G: 1, SG: 2 };
const rank = (table: Record<string, number>, pos: string | undefined) =>
	table[(pos ?? "").toUpperCase()] ?? 9;

// Their spots, as [depth from the baseline, across]: one group the half
// of the floor nearer the camera, the other the far half - round the arc
// and in at the elbows and the short corners.
const SPOTS: [number, number][][] = [
	[
		[5, 47],
		[16, 44],
		[19, 31],
		[27, 34],
		[9, 39],
	],
	[
		[5, 3],
		[16, 6],
		[19, 19],
		[27, 16],
		[9, 11],
	],
];
// Where the man feeding stands, under the rim to his group's side.
const FEED: [number, number][] = [
	[6.5, 29.5],
	[9.5, 19],
];
// Out toward half court, a ball each.
const HANDLES: [number, number][] = [
	[37, 41],
	[37, 9],
];

const seeded = (seed: number) => {
	let s = seed | 0;
	return () => {
		s = (s + 0x6d2b79f5) | 0;
		let x = Math.imul(s ^ (s >>> 15), 1 | s);
		x = (x + Math.imul(x ^ (x >>> 7), 61 | x)) ^ x;
		return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
	};
};

const unit = (a: Pt, b: Pt): Pt => {
	const d = dist(a, b) || 1;
	return { x: (b.x - a.x) / d, y: (b.y - a.y) / d };
};

// One ball, and the men it goes between.
class Part {
	tracks = new Map<number, Track>();
	ball: BallSeg[] = [];
	fx: Fx[] = [];
	at = new Map<number, Pt>();
	free = new Map<number, number>();

	readonly team: Side;
	readonly rng: () => number;

	constructor(team: Side, rng: () => number) {
		this.team = team;
		this.rng = rng;
	}

	add(pid: number, start: Pt, look: Pt) {
		this.tracks.set(pid, {
			pid,
			team: this.team,
			start: { ...start },
			moves: [],
			acts: [],
			arms: [],
			faces: [],
			looks: [[0, { ...look }]],
			shown: [[0, true]],
		});
		this.at.set(pid, { ...start });
		this.free.set(pid, 0);
	}

	tr(pid: number): Track {
		return this.tracks.get(pid)!;
	}

	pos(pid: number): Pt {
		return this.at.get(pid)!;
	}

	go(pid: number, to: Pt, t: number, speed = JOG): number {
		const from = this.pos(pid);
		const t0 = Math.max(t, this.free.get(pid) ?? 0);
		const d = dist(from, to);
		if (d < 0.2) {
			return t0;
		}
		const t1 = t0 + Math.max(350, (d / speed) * 1000);
		this.tr(pid).moves.push({
			t0,
			t1,
			from: { ...from },
			to: { ...to },
			anim: speed > 7 ? "jog" : "walk",
		});
		this.at.set(pid, { ...to });
		this.free.set(pid, t1);
		return t1;
	}

	look(pid: number, t: number, at: Pt) {
		this.tr(pid).looks.push([t, { x: at.x, y: at.y }]);
	}

	face(pid: number, t: number, at: Pt) {
		this.tr(pid).faces.push([t, at.x >= this.pos(pid).x ? 1 : -1]);
	}

	// From his hands to the other man's: thrown at `start`, caught - and his.
	pass(from: number, to: number, start: number): number {
		const A = this.pos(from);
		const B = this.pos(to);
		this.face(from, start, B);
		this.tr(from).acts.push({
			t0: start,
			t1: start + RELEASE_MS + 180,
			anim: "pass",
			look: { ...B },
		});
		const release = start + RELEASE_MS;
		const arrive = release + passMs(dist(A, B));
		this.ball.push({
			kind: "fly",
			t0: release,
			t1: arrive,
			from: { pid: from },
			to: { pid: to },
		});
		this.face(to, arrive - 90, A);
		this.tr(to).acts.push({
			t0: arrive - 90,
			t1: arrive + 110,
			anim: "catch",
			look: { ...A },
		});
		this.ball.push({ kind: "hold", t0: arrive, pid: to, style: "hold" });
		this.free.set(from, Math.max(this.free.get(from) ?? 0, start + 300));
		this.free.set(to, Math.max(this.free.get(to) ?? 0, arrive + 110));
		return arrive;
	}

	// Up from where he stands and in. Returns when, and where, it comes out
	// of the bottom of the net - or nothing if no shot from here would go.
	shoot(pid: number, t: number): { out: number; at: Pt3 } | undefined {
		const rim = rimPt(this.team);
		const P = this.pos(pid);
		const gather = Math.max(t, this.free.get(pid) ?? 0);
		const v = (JUMPER.release - JUMPER.off) / (JUMPER.land - JUMPER.off);
		const hand = releaseAt("shoot", JUMPER.release);
		const u = unit(P, rim);
		const from = {
			x: P.x + u.x * hand.f + u.y * hand.s,
			y: P.y + u.y * hand.f - u.x * hand.s,
			z: hand.u + 4 * SHOT_PEAK * v * (1 - v),
		};
		const flight = 620 + dist(P, rim) * 22;
		const kinds: ShotKind[] = [this.rng() < 0.6 ? "swish" : "rim"];
		const found =
			findShot(
				this.team,
				from,
				[flight * 0.92, flight * 1.08],
				{ made: true, kinds },
				this.rng,
			) ??
			findShot(
				this.team,
				from,
				[flight * 0.92, flight * 1.08],
				{ made: true },
				this.rng,
			);
		if (!found) {
			return undefined;
		}
		this.face(pid, gather, rim);
		this.look(pid, gather, rim);
		this.tr(pid).acts.push({
			t0: gather,
			t1: gather + SHOT_MS,
			anim: "shoot",
			jump: [JUMPER.off, JUMPER.land, SHOT_PEAK],
			look: { x: rim.x, y: rim.y },
		});
		const release = gather + SHOT_MS * JUMPER.release;
		const t0 = release + found.handoff;
		this.ball.push({
			kind: "fly",
			t0: release,
			t1: t0,
			from: { pid },
			to: found.p,
		});
		const spin = -Math.PI * 2 * BACKSPIN * (Math.sign(rim.x - from.x) || 1);
		const pts = found.play.pts;
		const t1 = t0 + (pts.length / 3 - 1) * SAMPLE_MS;
		this.ball.push({
			kind: "path",
			t0,
			t1,
			pts,
			v0: found.v,
			roll0: (spin * found.handoff) / 1000,
			spin,
		});
		const play = found.play;
		for (const h of play.touches) {
			if (h.t <= play.decided + 1) {
				this.fx.push({ kind: "clank", t: t0 + h.t, rim: this.team });
			}
		}
		this.fx.push({ kind: "swish", t: t0 + play.decided, rim: this.team });
		// Held up after he lands, till it goes in.
		const landed = gather + SHOT_MS * JUMPER.land;
		if (t0 + play.decided > landed + 300) {
			this.tr(pid).arms.push({
				t0: landed,
				t1: t0 + play.decided,
				kind: "follow",
			});
		}
		this.free.set(pid, gather + SHOT_MS);
		const n = pts.length;
		return { out: t1, at: { x: pts[n - 3]!, y: pts[n - 2]!, z: pts[n - 1]! } };
	}

	// Out of the net, into his hands: a step toward where it comes down -
	// staying his own side of the rim (the other group's big has the other).
	takeOut(pid: number, out: number, at: Pt3, home: Pt): number {
		const rim = rimPt(this.team);
		const dir = attackDir(this.team);
		const side = home.y >= rim.y ? 1 : -1;
		// Halfway in from where he stands, never out under the rim itself.
		const mid = { x: (home.x + at.x) / 2, y: (home.y + at.y) / 2 };
		const P = {
			x: dir > 0 ? Math.min(mid.x, at.x - 1.2) : Math.max(mid.x, at.x + 1.2),
			y: side > 0 ? Math.max(mid.y, rim.y + 1.6) : Math.min(mid.y, rim.y - 1.6),
		};
		this.go(pid, P, out - 900, 7);
		const arrive = out + 260;
		this.face(pid, arrive - 300, rim);
		this.tr(pid).acts.push({
			t0: arrive - 90,
			t1: arrive + 110,
			anim: "catch",
			look: { x: at.x, y: at.y },
		});
		this.ball.push(
			{ kind: "fly", t0: out, t1: arrive, from: at, to: { pid } },
			{ kind: "hold", t0: arrive, pid, style: "hold" },
		);
		this.free.set(pid, Math.max(this.free.get(pid) ?? 0, arrive + 110));
		return arrive;
	}

	timeline(end: number): CourtTimeline {
		for (const tr of this.tracks.values()) {
			tr.moves.sort((a, b) => a.t0 - b.t0);
			tr.acts.sort((a, b) => a.t0 - b.t0);
			tr.looks.sort((a, b) => a[0] - b[0]);
			tr.faces.sort((a, b) => a[0] - b[0]);
		}
		return {
			tracks: this.tracks,
			ball: this.ball,
			fx: this.fx.sort((a, b) => a.t - b.t),
			beats: [],
			poss: [[0, this.team]],
			cuts: [],
			shots: [],
			tension: [],
			fast: [],
			end,
		};
	}
}

// A big under the rim feeding his shooters, one after the other, each
// round to his next spot once he has let it go.
const shootaround = (
	team: Side,
	feeder: number,
	shooters: number[],
	side: 0 | 1,
	rng: () => number,
	lead: number,
): CourtTimeline => {
	const part = new Part(team, rng);
	const rim = rimPt(team);
	const spots = SPOTS[side]!.map(([d, a]) => spot(team, d, a));
	const next = shooters.map((_, i) => (i * 2) % spots.length);
	const F = FEED[side]!;
	const home = spot(team, F[0], F[1]);
	part.add(feeder, home, { x: rim.x, y: rim.y });
	shooters.forEach((pid, i) => {
		part.add(pid, spots[next[i]!]!, rim);
		next[i] = (next[i]! + 1) % spots.length;
	});
	// The first shooter starts with it.
	part.ball.push({ kind: "hold", t0: 0, pid: shooters[0]!, style: "hold" });
	let t = lead;
	let k = 0;
	while (t < WARMUP_MS) {
		const i = k % shooters.length;
		const pid = shooters[i]!;
		const shot = part.shoot(pid, t);
		if (!shot) {
			// (No shot from here would go: on to the next spot, and the big
			// gets it back to him there.)
			break;
		}
		// Round to his next spot once he is down.
		const s = next[i]!;
		next[i] = (s + 1) % spots.length;
		part.go(pid, spots[s]!, part.free.get(pid)! + 250 + rng() * 300);
		part.look(pid, part.free.get(pid)!, rim);
		const caught = part.takeOut(feeder, shot.out, shot.at, home);
		// To the next man up, once he is on his spot.
		const to = shooters[(k + 1) % shooters.length]!;
		const ready = part.free.get(to) ?? 0;
		const throwAt = Math.max(
			caught + 380 + rng() * 260,
			ready - RELEASE_MS - passMs(dist(part.pos(feeder), part.pos(to))) + 150,
		);
		part.look(feeder, caught + 150, part.pos(to));
		const arrive = part.pass(feeder, to, throwAt);
		// Back under, out of the way.
		part.go(feeder, home, throwAt + 450, 5);
		part.look(feeder, part.free.get(feeder)!, rim);
		t = arrive + SET_MS + rng() * 220;
		k++;
	}
	return part.timeline(WARMUP_MS);
};

// A ball of his own out toward half court: low and quick, through his legs,
// behind his back, back and forth across in front of him.
const handle = (
	team: Side,
	pid: number,
	j: number,
	rng: () => number,
): CourtTimeline => {
	const part = new Part(team, rng);
	const [d, a] = HANDLES[j]!;
	const P = spot(team, d, a);
	// Facing the basket he will be shooting at.
	const rim = rimPt(team);
	part.add(pid, P, rim);
	part.face(pid, 0, rim);
	let hand: "R" | "L" = rng() < 0.75 ? "R" : "L";
	let t = 0;
	part.ball.push({ kind: "hold", t0: 0, pid, style: "dribble", hand });
	const moves = ["front", "legs", "back", "front", "legs"] as const;
	while (t < WARMUP_MS) {
		t += 900 + rng() * 1400;
		const move = moves[Math.floor(rng() * moves.length)]!;
		const bounces = 1 + Math.floor(rng() * 3);
		part.ball.push({ kind: "hold", t0: t, pid, style: "cross", hand, move });
		t += (bounces * 1000) / 3.2;
		if (bounces % 2 === 1) {
			hand = hand === "R" ? "L" : "R";
		}
		part.ball.push({ kind: "hold", t0: t, pid, style: "dribble", hand });
	}
	return part.timeline(WARMUP_MS);
};

// Both teams warming up: `players` the whole of both rosters, `seed` the
// game's own (the same warm-up every time it is shown).
export const stageWarmup = (players: CourtPlayer[], seed: number): Warmup => {
	const rng = seeded(seed * 7919 + 17);
	const parts: CourtTimeline[] = [];
	for (const team of [0, 1] as const) {
		const mine = players
			.filter((p) => p.team === team && !(p.injury && p.injury.games > 0))
			.slice(0, 9);
		const bigs = [...mine]
			.sort((a, b) => rank(BIG, a.pos) - rank(BIG, b.pos))
			.slice(0, mine.length >= 4 ? 2 : 1);
		const rest = mine.filter((p) => !bigs.includes(p));
		const guards =
			rest.length >= 6
				? [...rest]
						.sort((a, b) => rank(GUARD, a.pos) - rank(GUARD, b.pos))
						.slice(0, Math.min(HANDLES.length, rest.length - 4))
				: [];
		const shooters = rest.filter((p) => !guards.includes(p));
		bigs.forEach((big, side) => {
			const mineToo = shooters.filter((_, i) => i % bigs.length === side);
			if (mineToo.length > 0) {
				parts.push(
					shootaround(
						team,
						big.pid,
						mineToo.map((p) => p.pid),
						side as 0 | 1,
						rng,
						300 + side * 1700 + team * 900,
					),
				);
			}
		});
		guards.forEach((g, j) => {
			parts.push(handle(team, g.pid, j, rng));
		});
	}
	const tracks = new Map<number, Track>();
	const fx: Fx[] = [];
	for (const p of parts) {
		for (const [pid, tr] of p.tracks) {
			tracks.set(pid, tr);
		}
		fx.push(...p.fx);
	}
	return {
		tl: {
			tracks,
			ball: [],
			fx: fx.sort((a, b) => a.t - b.t),
			beats: [],
			poss: [[0, 1]],
			cuts: [],
			shots: [],
			tension: [],
			fast: [],
			warmup: true,
			end: WARMUP_MS,
		},
		parts,
	};
};

// The warm-ups at t: everyone in them, and every ball.
export const warmupAt = (
	w: Warmup,
	t: number,
	bodyFor: (pid: number) => Body,
): { players: PlayerState[]; balls: BallState[] } => {
	const players: PlayerState[] = [];
	const balls: BallState[] = [];
	for (const part of w.parts) {
		for (const pid of part.tracks.keys()) {
			const st = evalPlayer(part, pid, t);
			if (st.shown) {
				players.push(withBody(st, bodyFor(pid)));
			}
		}
		balls.push(evalBall(part, t, bodyFor));
	}
	return { players, balls };
};

// The picture's moment: everyone warming up, every ball - in warm-up tops.
export const warmupMoment = (
	w: Warmup,
	t: number,
	bodyFor: (pid: number) => Body,
): Moment => {
	const { players, balls } = warmupAt(w, t, bodyFor);
	return {
		t,
		players,
		ball: balls[0] ?? { x: 47, y: 25, z: 0.39 },
		balls: balls.slice(1),
		warm: true,
	};
};

// The picture over the warm-ups: the game's own framing, in close enough to
// see them, drifting from the road team's end to the home team's and back.
const PAN_MS = 18_000;
export const warmupAim = (t: number, narrow: boolean, fit?: CourtFit): Shot => {
	const width = Math.max(narrow ? 58 : 74, fit?.min ?? 0);
	const u = (1 - Math.cos((Math.PI * 2 * t) / (PAN_MS * 2))) / 2;
	const lo = width / 2 - 8;
	const hi = COURT_W + 8 - width / 2;
	return { x: lo + (hi - lo) * u, width, y: fit ? fit.y(width) : 24 };
};
