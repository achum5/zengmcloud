import type { CourtTimeline, RawEvent } from "./director.ts";

// THE CLOCKS ON SCREEN.
//
// Every play-by-play line carries the game clock when it happened; between
// two lines the clock runs down while the court shows the lead-in, reaching
// the line's time just as its text appears. It runs at real speed; where the
// picture runs fast through the walk up the floor (or cuts past it), the
// clock makes up the time the sim spent there. Through live play - a shot in
// the air, a rebound, a steal - it never stops: it runs on from one line
// straight into the next one's lead-in. After a whistle, a free throw or a
// ball out of bounds it waits for the ball to be back in play. The sim keeps
// no shot clock in the play-by-play, so the one on screen is read off
// possession: 24 when a team gets the ball, 14 after an offensive rebound,
// running with the game clock - and off once the game clock is shorter.

const SHOT_CLOCK = 24;
const RESET_ORB = 14;
// The last seconds into a line run true even when the picture holds more
// than the sim's time (see buildClocks).
const LEAD_TRUE = 3.5;

type Mark = { t: number; clock: number; period: number };

// A made basket (field goal or free throw; not an attempt - "fga..." - or a
// miss). The clock is reset by it; between free throws it doesn't matter.
const madeBasket = (e: RawEvent): boolean =>
	/^fg(?!a)/.test(e.type) || /^tp/.test(e.type) || e.type === "ft";

// A line the ball stays live through.
const runsOn = (e: RawEvent): boolean =>
	(/^(fga|fg|tp|miss|blk|drb|orb)/.test(e.type) &&
		!e.type.endsWith("AndOne")) ||
	((e.type === "stl" || e.type === "tov") && e.outOfBounds !== true) ||
	e.type === "jumpBall";

export type Clocks = {
	// Game clock marks in timeline order: at time t the clock read `clock`.
	marks: Mark[];
	// When the shot clock restarted, and from what.
	// `hold`: the clock is reset but not running - a basket was made, and it
	// starts again when the ball is inbounded (the next reset, at tl.poss).
	resets: { t: number; from: number; hold?: true }[];
};

export const buildClocks = (tl: CourtTimeline, events: RawEvent[]): Clocks => {
	const marks: Mark[] = [];
	const resets: Clocks["resets"] = [];
	let period = 1;
	let last: number | undefined;
	// From when the clock may run on toward the next line: the last line's
	// moment, if the ball stayed live.
	let live: number | undefined;
	// The last line that carried the sim's shot clock.
	let anchor:
		| { t: number; clock: number; shot: number; period: number }
		| undefined;
	// Whether this game's lines carry the shot clock at all (older saved
	// replays don't, and fall back to reading it off possession).
	let exact = false;
	const possStarts = tl.poss.map(([t]) => t).filter(Number.isFinite);
	const possStartSet = new Set(possStarts);
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
			let from = Math.min(live ?? b.preStart, b.preStart);
			if (live === undefined) {
				// A dead ball: the clock waits for it to be put back in play -
				// the inbound caught.
				const inbound = inboundCaught(tl, from, b.actionStart);
				if (inbound !== undefined) {
					marks.push({ t: from, clock: last, period });
					from = inbound;
				}
			}
			marks.push({ t: from, clock: last, period });
			// The time the sim spent in the lead-in that the picture runs through
			// fast (or cuts past): the clock runs true on either side of it and
			// makes up the rest there.
			// Where the picture holds more than the sim's time, the play itself -
			// what leads straight into the line - still runs true, and the
			// clock takes less of the time before it (waiting, if it must).
			const fast = fastIn(tl.fast, from, b.actionStart);
			const cut = lastCutIn(tl.cuts, from, b.actionStart);
			const shown = (b.actionStart - from) / 1000;
			const gap = last - e.clock;
			if (fast) {
				const [f0, f1] = fast;
				const tail = Math.min((b.actionStart - f1) / 1000, gap);
				const head = Math.min((f0 - from) / 1000, gap - tail);
				marks.push(
					{ t: f0, clock: last - head, period },
					{ t: f1, clock: e.clock + tail, period },
				);
			} else if (cut !== undefined && gap > shown) {
				marks.push(
					{ t: cut - 1, clock: last - (cut - 1 - from) / 1000, period },
					{ t: cut, clock: e.clock + (b.actionStart - cut) / 1000, period },
				);
			} else if (shown > gap + 0.05 && shown > LEAD_TRUE) {
				const tail = Math.min(gap, LEAD_TRUE);
				marks.push({
					t: b.actionStart - tail * 1000,
					clock: e.clock + tail,
					period,
				});
			}
		}
		marks.push({ t: b.actionStart, clock: e.clock, period });
		const shot = typeof e.shotClock === "number" ? e.shotClock : undefined;
		if (shot !== undefined) {
			// The sim's own shot clock (games simmed since it started logging
			// it). If it reads higher than the last line's, run down by the game
			// clock between them, the clock was reset in between - an offensive
			// rebound, a foul, a timeout, a new possession - and that happened
			// on the earlier line, so it restarts there, not here.
			// (A new possession already gets its fresh clock when the ball is
			// inbounded - see tl.poss below - so it isn't restarted twice.)
			if (
				anchor !== undefined &&
				anchor.period === period &&
				shot > anchor.shot - (anchor.clock - e.clock) + 0.5 &&
				!possStarts.some(
					(t) => t > (anchor?.t ?? Infinity) && t <= b.actionStart,
				)
			) {
				resets.push({
					t: anchor.t,
					from: Math.min(SHOT_CLOCK, shot + (anchor.clock - e.clock)),
				});
			}
			// A line that hands the ball over (a defensive rebound, a steal)
			// carries the clock of the possession that just ended; the new
			// one starts fresh there (tl.poss), so it isn't an anchor for it.
			if (!possStartSet.has(b.actionStart)) {
				resets.push({ t: b.actionStart, from: shot });
				anchor = { t: b.actionStart, clock: e.clock, shot, period };
			}
			exact = true;
		}
		last = e.clock;
		live = runsOn(e) ? b.actionStart : undefined;
		if (madeBasket(e)) {
			// Reset by the make, and stopped until the inbound - it doesn't run
			// on toward zero while the ball is dead.
			resets.push({ t: b.actionStart, from: SHOT_CLOCK, hold: true });
		}
		if (exact) {
			// Read off the sim, line by line - no guessing.
		} else if (e.type === "orb") {
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
	// Stable, so at one moment the later entry - the more specific one - wins.
	resets.sort((a, b) => a.t - b.t);
	return { marks, resets };
};

// When the first pass from one man to another in [a, b) - the inbound, after
// a dead ball - is caught, if there is one.
const inboundCaught = (
	tl: CourtTimeline,
	a: number,
	b: number,
): number | undefined => {
	let lo = 0;
	let hi = tl.ball.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (tl.ball[mid]!.t0 < a) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	for (let i = lo; i < tl.ball.length; i++) {
		const s = tl.ball[i]!;
		if (s.t0 >= b) {
			break;
		}
		if (s.kind === "fly" && "pid" in s.from && "pid" in s.to && s.t1 < b) {
			return s.t1;
		}
	}
	return undefined;
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
	if (r.hold) {
		return r.from > game ? undefined : r.from;
	}
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
