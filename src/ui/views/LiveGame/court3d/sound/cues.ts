import type { BallSeg, Beat, CourtTimeline, Fx } from "../director.ts";
import {
	BALL_R,
	CROSS_RATE,
	DOWN,
	beatTime,
	floorSpotOf,
} from "../evaluate.ts";
import { floorTouches } from "../floorRoll.ts";
import { rimX } from "../geometry.ts";
import { spotAt } from "../possession/through.ts";

// WHAT IS HEARD ON THE FLOOR, AND WHEN: every sound the picture makes, read
// off the timeline the game was staged into (see director.ts) - nothing new
// worked out about the game, only when the ball hits the hardwood, the iron
// or the net, when a man's sneakers squeak, and when an official blows his
// whistle or the horn goes. The same for the device playing it (see
// engine.ts) and for a film made of it. Sorted by when.

export type CueKind =
	// The ball on the hardwood: a dribble, or loose.
	| "dribble"
	| "floor"
	// Sneakers on it: a man planting hard.
	| "squeak"
	// The ball into a man's hands.
	| "catch"
	// Down through the net; off the iron; slammed through.
	| "swish"
	| "rim"
	| "dunk"
	// Swatted.
	| "block"
	| "whistle"
	// The end of a period; the shot clock run out.
	| "horn"
	| "buzzer";

export type Cue = {
	t: number;
	kind: CueKind;
	// How hard (0 to 1): how loud, and which take of it.
	hard: number;
	// Where along the floor (feet from one baseline), for the side of the
	// picture it is heard on.
	x?: number;
	// A whistle's long blast (a foul) or short one.
	long?: boolean;
	// Heard only at real speed - a dribble, a hop of the ball - not at 2x and
	// up, nor through a stretch run through fast.
	fine?: boolean;
};

const dribblingBy = (s: BallSeg | undefined, pid: number): boolean =>
	s?.kind === "hold" && s.pid === pid && s.style === "dribble";

// Never more bounces than this in one run of a dribble (a guard, nothing
// real comes near it).
const MOST_BOUNCES = 4000;

// A man's dribble: the ball on the floor each bounce (see beatTime: a top of
// the bounce at each whole beat, the floor DOWN of the way to the next).
const dribbleCues = (tl: CourtTimeline, out: Cue[]) => {
	const ball = tl.ball;
	for (let i = 0; i < ball.length; i++) {
		const s = ball[i]!;
		if (s.kind !== "hold") {
			continue;
		}
		const tr = tl.tracks.get(s.pid);
		const xAt = (t: number) => (tr ? spotAt(tr, t).x : undefined);
		if (s.style === "dribble") {
			if (dribblingBy(ball[i - 1], s.pid)) {
				continue;
			}
			let j = i;
			while (dribblingBy(ball[j + 1], s.pid)) {
				j++;
			}
			const end = ball[j + 1]?.t0 ?? tl.end;
			for (let k = 0; k < MOST_BOUNCES; k++) {
				const t = beatTime(ball, i, k + DOWN);
				if (!(t < end)) {
					break;
				}
				if (t >= s.t0) {
					out.push({ t, kind: "dribble", hard: 0.55, x: xAt(t), fine: true });
				}
			}
		} else if (s.style === "cross") {
			const ms = 1000 / (s.rate ?? CROSS_RATE);
			const end = ball[i + 1]?.t0 ?? tl.end;
			for (let k = 0; k < MOST_BOUNCES; k++) {
				const t = s.t0 + (k + 0.5) * ms;
				if (!(t < end)) {
					break;
				}
				out.push({ t, kind: "dribble", hard: 0.45, x: xAt(t), fine: true });
			}
		}
	}
};

// The loose ball's hops (see evaluate's bounce, and floorRoll for one with
// nobody after it): each time it comes down, as hard as it came down.
const G = 32.2;
const fall = (h: number) => Math.sqrt((2 * Math.max(0.01, h)) / G);
const hardOf = (up: number) => Math.min(1, Math.max(0.1, up / 22));
const bounceCues = (tl: CourtTimeline, out: Cue[]) => {
	for (const s of tl.ball) {
		if (s.kind !== "bounce" || !(s.t1 > s.t0)) {
			continue;
		}
		const xAt = (t: number) =>
			s.from.x + (s.to.x - s.from.x) * ((t - s.t0) / (s.t1 - s.t0));
		if (s.free) {
			for (const { s: sec, up } of floorTouches(s.free)) {
				const t = s.t0 + sec * 1000;
				if (t <= s.t1) {
					out.push({
						t,
						kind: "floor",
						hard: hardOf(up),
						x: xAt(t),
						fine: true,
					});
				}
			}
			continue;
		}
		const drop = Math.max(0, s.from.z - BALL_R);
		const span = Math.max(0.001, ((s.t1 - s.t0) / 1000) * 0.85);
		const dropT = drop > 0.005 ? fall(drop) : 0;
		let hopsT = 0;
		for (let i = 0; i < s.hops; i++) {
			hopsT += 2 * fall(s.h0 * Math.pow(0.42, i));
		}
		const room = span - dropT;
		const low =
			dropT + hopsT <= span || hopsT <= 0
				? 1
				: room > 0.05
					? (room / hopsT) ** 2
					: 1;
		const h0 = s.h0 * low;
		const hops: { d: number; h: number }[] =
			drop > 0.005 ? [{ d: dropT, h: drop }] : [];
		for (let i = 0; i < s.hops; i++) {
			const h = h0 * Math.pow(0.42, i);
			hops.push({ d: 2 * fall(h), h });
		}
		const real = hops.reduce((a, h) => a + h.d, 0);
		const speed = Math.max(1, real / span);
		let at = 0;
		for (const hop of hops) {
			at += hop.d;
			const t = s.t0 + (at * 1000) / speed;
			if (t <= s.t1) {
				out.push({
					t,
					kind: "floor",
					hard: hardOf(Math.sqrt(2 * G * hop.h)),
					x: xAt(t),
					fine: true,
				});
			}
		}
	}
};

// THE BALL INTO A MAN'S HANDS: a pass caught, as hard as it came (a lob
// soft, a two-hand chest pass in a smack) - and a bounce pass off the floor
// on its way.
const PASS_HARD = 45;
const catchCues = (tl: CourtTimeline, out: Cue[]) => {
	const ball = tl.ball;
	const spot = (end: BallSeg, t: number) => {
		if (end.kind !== "fly") {
			return undefined;
		}
		const e = t === end.t0 ? end.from : end.to;
		if (!("pid" in e)) {
			return e;
		}
		const tr = tl.tracks.get(e.pid);
		return tr ? floorSpotOf(tr, t) : undefined;
	};
	for (let i = 0; i < ball.length; i++) {
		const s = ball[i]!;
		if (s.kind !== "fly" || !(s.t1 > s.t0)) {
			continue;
		}
		const next = ball[i + 1];
		if ("pid" in s.to) {
			const pid = s.to.pid;
			const kept =
				(next?.kind === "hold" && next.pid === pid) ||
				(next?.kind === "fly" && "pid" in next.from && next.from.pid === pid);
			if (!kept) {
				continue;
			}
			const a = spot(s, s.t0);
			const b = spot(s, s.t1);
			const v =
				a && b
					? Math.hypot(b.x - a.x, b.y - a.y) / ((s.t1 - s.t0) / 1000)
					: PASS_HARD / 2;
			out.push({
				t: s.t1,
				kind: "catch",
				hard: Math.min(1, Math.max(0.2, v / PASS_HARD)),
				x: b?.x,
				fine: true,
			});
		} else if (
			s.to.z <= BALL_R + 0.5 &&
			next?.kind === "fly" &&
			next.t0 === s.t1 &&
			!("pid" in next.from)
		) {
			out.push({ t: s.t1, kind: "floor", hard: 0.5, x: s.to.x, fine: true });
		}
	}
};

// Breaks in play the horn ends (the clock run out on a period), not a
// whistle: the line of the play-by-play each comes with.
const HORN_BEATS = new Set(["endOfPeriod", "gameOver"]);

// Whose beat a moment is in.
const beatOf = (beats: Beat[], t: number): Beat | undefined => {
	let lo = 0;
	let hi = beats.length - 1;
	let k = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (beats[mid]!.preStart <= t) {
			k = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return k >= 0 ? beats[k] : undefined;
};

// A foul's whistle is a long hard blast; the rest, a short one.
const LONG_CALLS = new Set<Fx["call"]>([
	"foul",
	"shootingFoul",
	"andOne",
	"offensive",
]);

// The last period's horn is the final one: none again this soon after it.
const FINAL_HORN_MS = 5000;

// A slam's net is in its own sound: no swish of its own so soon after.
const DUNK_NET_MS = 250;

const fxCues = (tl: CourtTimeline, out: Cue[]) => {
	const beats = tl.beats;
	let lastDunk = -Infinity;
	let hornedBeat: Beat | undefined;
	for (const f of tl.fx) {
		const rx = f.rim !== undefined ? rimX(f.rim) : undefined;
		switch (f.kind) {
			case "swish":
				if (f.t - lastDunk > DUNK_NET_MS) {
					out.push({ t: f.t, kind: "swish", hard: f.big ? 0.8 : 0.6, x: rx });
				}
				break;
			case "clank":
				out.push({ t: f.t, kind: "rim", hard: 0.6, x: rx });
				break;
			case "dunk":
				lastDunk = f.t;
				out.push({ t: f.t, kind: "dunk", hard: f.big ? 1 : 0.8, x: rx });
				break;
			case "block":
				out.push({ t: f.t, kind: "block", hard: 0.8, x: f.at?.x });
				break;
			case "whistle": {
				if (!f.call || f.call === "noBasket") {
					break;
				}
				// The clock run out on a period: the horn, once - not the
				// official's whistle.
				const beat = beatOf(beats, f.t);
				const prev = beat ? beats[beats.indexOf(beat) - 1] : undefined;
				const periodOver =
					f.call === "stop" &&
					beat &&
					(HORN_BEATS.has(beat.type) ||
						((beat.type === "period" || beat.type === "overtime") &&
							prev?.type !== "endOfPeriod"));
				if (periodOver) {
					if (hornedBeat !== beat) {
						hornedBeat = beat;
						out.push({ t: f.t, kind: "horn", hard: 1 });
					}
					break;
				}
				if (f.call === "clock") {
					out.push({ t: f.t - 250, kind: "buzzer", hard: 1 });
				}
				out.push({
					t: f.t,
					kind: "whistle",
					hard: LONG_CALLS.has(f.call) ? 1 : 0.7,
					x: f.at?.x,
					long: LONG_CALLS.has(f.call),
				});
				break;
			}
			default:
				break;
		}
	}
	// The final horn, if the end of the last period's didn't just go.
	const over = beats.findLast((b) => b.type === "gameOver");
	if (
		over &&
		!out.some(
			(c) =>
				c.kind === "horn" &&
				c.t <= over.actionStart &&
				over.actionStart - c.t < FINAL_HORN_MS,
		)
	) {
		out.push({ t: over.actionStart, kind: "horn", hard: 1 });
	}
};

// SNEAKERS ON THE HARDWOOD: a squeak where a man plants hard - cutting,
// stopping short, changing direction down in his stance - at the moment he
// turns hardest, read off where the picture has him. As often as the
// footage has them (some 30 to 55 a minute of play), one a plant, and a
// scramble only its hardest few. Worked out a stretch at a time, as the
// game gets there (every man, every moment, is too much at once).
const SQUEAK_STRETCH = 2000;
// (Where he is, read this often (ms); how fast he goes, over twice it; how
// hard that changes, over four times it.)
const STEP_MS = 25;
// Planted this hard (feet a second, a second) or harder, going at least so
// fast (feet a second) one side of it or the other. The new sim's runs ease
// in and out the way legs take them; today's turns its corners sharper, so
// it takes a harder one to sound.
const PLANT = 35;
const PLANT_SHARP = 45;
const PLANT_PACE = 4;
// One a plant: the same man no sooner again than this (ms).
const PLANT_AGAIN = 400;
// No more than so many in a stretch, the hardest; none on top of another.
const SQUEAKS_MOST = 8;
const SQUEAK_APART = 60;
const squeakStretch = (tl: CourtTimeline, k: number): Cue[] => {
	const t0 = k * SQUEAK_STRETCH;
	const t1 = t0 + SQUEAK_STRETCH;
	const hard = tl.gaits ? PLANT : PLANT_SHARP;
	// (Read from a little before to a little after, for how fast and how
	// hard either side of its edges.)
	const pad = 6 * STEP_MS;
	const n = Math.round((t1 - t0 + 2 * pad) / STEP_MS) + 1;
	const at = (i: number) => t0 - pad + i * STEP_MS;
	const found: { t: number; a: number; x: number }[] = [];
	for (const tr of tl.tracks.values()) {
		const P: ({ x: number; y: number } | undefined)[] = [];
		for (let i = 0; i < n; i++) {
			P.push(floorSpotOf(tr, at(i)));
		}
		const V: (readonly [number, number] | undefined)[] = P.map((_, i) => {
			const a = P[i - 1];
			const b = P[i + 1];
			return a && b
				? [
						((b.x - a.x) * 1000) / (2 * STEP_MS),
						((b.y - a.y) * 1000) / (2 * STEP_MS),
					]
				: undefined;
		});
		const A = V.map((_, i) => {
			const a = V[i - 2];
			const b = V[i + 2];
			return a && b
				? Math.hypot(b[0] - a[0], b[1] - a[1]) / ((4 * STEP_MS) / 1000)
				: 0;
		});
		const pace = (v: readonly [number, number] | undefined) =>
			v ? Math.hypot(v[0], v[1]) : 0;
		let last = -Infinity;
		for (let i = 3; i < n - 3; i++) {
			const a = A[i]!;
			const t = at(i);
			if (
				a < hard ||
				!(a > A[i - 1]! && a >= A[i + 1]!) ||
				Math.max(pace(V[i - 3]), pace(V[i + 3])) < PLANT_PACE ||
				t - last < PLANT_AGAIN
			) {
				continue;
			}
			last = t;
			if (t >= t0 && t < t1 && t < tl.end) {
				found.push({ t, a, x: P[i]!.x });
			}
		}
	}
	found.sort((p, q) => q.a - p.a);
	const out: Cue[] = [];
	for (const f of found) {
		if (out.length >= SQUEAKS_MOST) {
			break;
		}
		if (out.some((c) => Math.abs(c.t - f.t) < SQUEAK_APART)) {
			continue;
		}
		out.push({
			t: f.t,
			kind: "squeak",
			hard: Math.min(1, 0.35 + (0.65 * (f.a - hard)) / hard),
			x: f.x,
			fine: true,
		});
	}
	return out.sort((p, q) => p.t - q.t);
};
const squeaksMade = new WeakMap<CourtTimeline, Map<number, Cue[]>>();
// The squeaks from `from` up to `to` (timeline ms), in order.
export const squeaksIn = (
	tl: CourtTimeline,
	from: number,
	to: number,
): Cue[] => {
	let kept = squeaksMade.get(tl);
	if (!kept) {
		kept = new Map();
		squeaksMade.set(tl, kept);
	}
	const out: Cue[] = [];
	const k1 = Math.floor((Math.min(to, tl.end) - 1) / SQUEAK_STRETCH);
	for (let k = Math.max(0, Math.floor(from / SQUEAK_STRETCH)); k <= k1; k++) {
		let list = kept.get(k);
		if (!list) {
			list = squeakStretch(tl, k);
			kept.set(k, list);
		}
		for (const c of list) {
			if (c.t >= from && c.t < to) {
				out.push(c);
			}
		}
	}
	return out;
};

// (Worked out once a timeline.)
const made = new WeakMap<CourtTimeline, Cue[]>();
export const cuesOf = (tl: CourtTimeline): Cue[] => {
	let cues = made.get(tl);
	if (!cues) {
		cues = [];
		dribbleCues(tl, cues);
		bounceCues(tl, cues);
		catchCues(tl, cues);
		fxCues(tl, cues);
		cues.sort((a, b) => a.t - b.t);
		made.set(tl, cues);
	}
	return cues;
};

// The first cue at or after t.
export const firstCueAt = (cues: Cue[], t: number): number => {
	let lo = 0;
	let hi = cues.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (cues[mid]!.t < t) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return lo;
};
