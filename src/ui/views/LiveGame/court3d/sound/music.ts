import type { CourtTimeline } from "../director.ts";
import { deadSpansOf, tensionAt } from "../evaluate.ts";
import { HOME, crowdOf, possAt } from "./crowd.ts";

// WHAT THE BUILDING PLAYS: the arena's own music, the way an NBA game
// operations crew runs it. Loud in a timeout and between periods (and
// through the warm-ups, and after a home win); a snatch of something in a
// long dead ball; and in live play only while the road team has the ball -
// the beat under "de-fense" once they are set up in the half court, cut the
// moment the shot goes up. Never while the home team has it in play. Read
// off the timeline like everything else (see crowd.ts); the same for the
// device and a film.

export type MusicKind = "musicBreak" | "musicDead" | "musicDefense";
export const MUSIC_KINDS: MusicKind[] = [
	"musicBreak",
	"musicDead",
	"musicDefense",
];
export type MusicLevels = Record<MusicKind, number>;

const smooth = (u: number) => {
	const v = Math.min(1, Math.max(0, u));
	return v * v * (3 - 2 * v);
};
// Up from `a` over `up` ms, down to nothing by `b` over `down` ms.
const window01 = (t: number, a: number, b: number, up: number, down: number) =>
	Math.min(smooth((t - a) / up), smooth((b - t) / down));

// A timeout, the break between periods: in once the whistle has gone, out
// as they walk back on.
const BREAK_IN = 1200;
const BREAK_UP = 1500;
const BREAK_OUT = 2500;
const BREAK_LEVEL = 0.9;
// A dead ball long enough for a snatch of music (ms) - not at the line, where
// the building goes quiet for the shooter.
const DEAD_LEAST = 4000;
const DEAD_IN = 800;
const DEAD_UP = 1000;
const DEAD_OUT = 1500;
const DEAD_LEVEL = 0.6;
// The road team set up in the half court this long after it got the ball;
// the beat comes up over so long, and is cut this quickly when the shot
// goes up.
const SET_UP = 3500;
const DEFENSE_UP = 1500;
const DEFENSE_CUT = 300;
// The warm-ups, and after a home win.
const WARMUP_LEVEL = 0.75;
const WIN_AFTER = 2500;

const AT_LINE = new Set(["ft", "missFt"]);

type MusicPlan = {
	breaks: [number, number][];
	dead: [number, number][];
	shots: number[];
	homeWin?: number;
};
const plans = new WeakMap<CourtTimeline, MusicPlan>();
const planOf = (tl: CourtTimeline): MusicPlan => {
	let plan = plans.get(tl);
	if (plan) {
		return plan;
	}
	const crowd = crowdOf(tl);
	const breaks = crowd.breaks;
	const lines = tl.beats
		.filter((b) => AT_LINE.has(b.type))
		.map((b) => [b.preStart, b.end] as const);
	const overlaps = (
		a: number,
		b: number,
		list: readonly (readonly [number, number])[],
	) => list.some(([c, d]) => c < b && d > a);
	const dead = deadSpansOf(tl)
		.filter(
			([a, b]) =>
				b - a >= DEAD_LEAST &&
				!overlaps(a, b, breaks) &&
				!overlaps(a, b, lines),
		)
		.map(([a, b]): [number, number] => [a, b]);
	const shots = crowd.flights
		.filter((f) => f.team !== HOME)
		.map((f) => f.t0)
		.sort((a, b) => a - b);
	const over = tl.beats.findLast((b) => b.type === "gameOver")?.actionStart;
	const homeWin =
		over !== undefined &&
		tl.fx.some((f) => f.kind === "cheer" && f.t >= over && f.team === HOME)
			? over + WIN_AFTER
			: undefined;
	plan = { breaks, dead, shots, homeWin };
	plans.set(tl, plan);
	return plan;
};

// The last of a sorted list at or before t.
const lastAt = (list: number[], t: number): number => {
	let lo = 0;
	let hi = list.length - 1;
	let k = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (list[mid]! <= t) {
			k = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return k;
};

// When the side with the ball at t got it.
const possSince = (tl: CourtTimeline, t: number): number | undefined => {
	const p = tl.poss;
	let k = -1;
	let lo = 0;
	let hi = p.length - 1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (p[mid]![0] <= t) {
			k = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	if (k < 0) {
		return undefined;
	}
	// (Back past any entry that only says the same side has it still.)
	while (k > 0 && p[k - 1]![1] === p[k]![1]) {
		k--;
	}
	return p[k]![0];
};

export const musicAt = (tl: CourtTimeline, t: number): MusicLevels => {
	const out: MusicLevels = { musicBreak: 0, musicDead: 0, musicDefense: 0 };
	if (tl.warmup) {
		out.musicBreak = WARMUP_LEVEL;
		return out;
	}
	const plan = planOf(tl);
	if (plan.homeWin !== undefined && t >= plan.homeWin) {
		out.musicBreak = BREAK_LEVEL * smooth((t - plan.homeWin) / BREAK_UP);
		return out;
	}
	for (const [a, b] of plan.breaks) {
		if (t > a && t < b) {
			out.musicBreak = Math.max(
				out.musicBreak,
				BREAK_LEVEL * window01(t, a + BREAK_IN, b, BREAK_UP, BREAK_OUT),
			);
		}
	}
	if (out.musicBreak > 0) {
		return out;
	}
	for (const [a, b] of plan.dead) {
		if (t > a && t < b) {
			out.musicDead =
				DEAD_LEVEL * window01(t, a + DEAD_IN, b, DEAD_UP, DEAD_OUT);
			return out;
		}
	}
	// In play: only while the road team has it.
	const side = possAt(tl, t);
	const since = possSince(tl, t);
	if (side === undefined || side === HOME || since === undefined) {
		return out;
	}
	if (deadSpansOf(tl).some(([a, b]) => t >= a && t <= b)) {
		return out;
	}
	let level = smooth((t - since - SET_UP) / DEFENSE_UP);
	const s = lastAt(plan.shots, t);
	if (s >= 0 && plan.shots[s]! >= since) {
		level *= 1 - smooth((t - plan.shots[s]!) / DEFENSE_CUT);
	}
	out.musicDefense = level * (0.55 + 0.45 * tensionAt(tl, t));
	return out;
};
