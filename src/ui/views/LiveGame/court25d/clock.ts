import type { CourtTimeline, RawEvent } from "./director.ts";

// THE CLOCKS ON SCREEN.
//
// Every play-by-play line carries the game clock when it happened; between
// two lines the clock runs down while the court shows the lead-in, reaching
// the line's time just as its text appears. It runs at real speed; where the
// picture runs fast through the walk up the floor (or cuts past it), the
// clock makes up the time the sim spent there. The sim keeps no shot clock
// in the play-by-play, so the one on screen is read off possession: 24 when
// a team gets the ball, 14 after an offensive rebound, running with the game
// clock - and off once the game clock is shorter.

const SHOT_CLOCK = 24;
const RESET_ORB = 14;

type Mark = { t: number; clock: number; period: number };

export type Clocks = {
	// Game clock marks in timeline order: at time t the clock read `clock`.
	marks: Mark[];
	// When the shot clock restarted, and from what.
	resets: { t: number; from: number }[];
};

export const buildClocks = (tl: CourtTimeline, events: RawEvent[]): Clocks => {
	const marks: Mark[] = [];
	const resets: { t: number; from: number }[] = [];
	let period = 1;
	let last: number | undefined;
	for (const b of tl.beats) {
		const e = events[b.i];
		if (!e) {
			continue;
		}
		if (e.type === "period" || e.type === "overtime") {
			period = typeof e.period === "number" ? e.period : period + 1;
			last = undefined;
		}
		if (typeof e.clock !== "number") {
			continue;
		}
		// The lead-in runs the clock from the last line's reading to this one's -
		// unless the clock went up, which is a new period starting.
		if (last !== undefined && e.clock <= last) {
			marks.push({ t: b.preStart, clock: last, period });
			// The time the sim spent in the lead-in that the picture runs through
			// fast (or cuts past): the clock runs true on either side of it and
			// makes up the rest there.
			const fast = fastIn(tl.fast, b.preStart, b.actionStart);
			const cut = lastCutIn(tl.cuts, b.preStart, b.actionStart);
			const shown = (b.actionStart - b.preStart) / 1000;
			if (fast) {
				const [f0, f1] = fast;
				const at0 = last - (f0 - b.preStart) / 1000;
				const at1 = e.clock + (b.actionStart - f1) / 1000;
				if (at0 >= at1 && at0 <= last && at1 >= e.clock) {
					marks.push(
						{ t: f0, clock: at0, period },
						{ t: f1, clock: at1, period },
					);
				}
			} else if (cut !== undefined && last - e.clock > shown) {
				marks.push(
					{ t: cut - 1, clock: last - (cut - 1 - b.preStart) / 1000, period },
					{ t: cut, clock: e.clock + (b.actionStart - cut) / 1000, period },
				);
			}
		}
		marks.push({ t: b.actionStart, clock: e.clock, period });
		last = e.clock;
		if (e.type === "orb") {
			resets.push({ t: b.actionStart, from: RESET_ORB });
		} else if (
			e.type === "jumpBall" ||
			e.type === "period" ||
			e.type === "overtime"
		) {
			// A fresh clock to start a period, whoever has the ball.
			resets.push({ t: b.actionStart, from: SHOT_CLOCK });
		}
	}
	for (const [t] of tl.poss) {
		if (Number.isFinite(t)) {
			resets.push({ t, from: SHOT_CLOCK });
		}
	}
	resets.sort((a, b) => a.t - b.t);
	return { marks, resets };
};

// The part of the last fast stretch in [a, b) that is in it, if any.
const fastIn = (
	fast: [number, number][],
	a: number,
	b: number,
): [number, number] | undefined => {
	let out: [number, number] | undefined;
	for (const [f0, f1] of fast) {
		if (f0 >= b) {
			break;
		}
		if (f1 > a) {
			out = [Math.max(a, f0), Math.min(b, f1)];
		}
	}
	return out && out[1] - out[0] > 1 ? out : undefined;
};

// The last cut in [a, b), if any.
const lastCutIn = (
	cuts: number[],
	a: number,
	b: number,
): number | undefined => {
	let out: number | undefined;
	for (const c of cuts) {
		if (c >= b) {
			break;
		}
		if (c >= a) {
			out = c;
		}
	}
	return out;
};

const lastAt = <T extends { t: number }>(list: T[], t: number): number => {
	let lo = 0;
	let hi = list.length - 1;
	let ans = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (list[mid]!.t <= t) {
			ans = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return ans;
};

// The game clock (seconds left in the period) at time t, or undefined before
// the first line with a clock.
export const gameClockAt = (c: Clocks, t: number): number | undefined => {
	const i = lastAt(c.marks, t);
	if (i < 0) {
		return c.marks[0]?.clock;
	}
	const a = c.marks[i]!;
	const b = c.marks[i + 1];
	if (!b || b.period !== a.period || b.clock > a.clock || b.t <= a.t) {
		return a.clock;
	}
	const u = Math.min(1, (t - a.t) / (b.t - a.t));
	return a.clock + (b.clock - a.clock) * u;
};

export const shotClockAt = (c: Clocks, t: number): number | undefined => {
	const game = gameClockAt(c, t);
	const i = lastAt(c.resets, t);
	if (game === undefined || i < 0) {
		return undefined;
	}
	const r = c.resets[i]!;
	const start = gameClockAt(c, r.t);
	if (start === undefined || start < game) {
		return undefined;
	}
	const left = Math.max(0, r.from - (start - game));
	// Turned off when it can't run out before the period does.
	return left > game ? undefined : left;
};

export const formatGameClock = (seconds: number): string => {
	if (seconds < 60) {
		return seconds.toFixed(1);
	}
	const s = Math.floor(seconds);
	return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
};
