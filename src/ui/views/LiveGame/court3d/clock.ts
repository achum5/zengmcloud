import type { CourtTimeline, RawEvent } from "./director.ts";

// THE CLOCKS ON SCREEN.
//
// Every play-by-play line carries the game clock when it happened; between
// two lines the clock runs down while the court shows the lead-in, reaching
// the line's time just as its text appears. It runs at real speed; where the
// picture runs fast through the walk up the floor (or cuts past it), the
// clock makes up the time the sim spent there. Through live play - a shot in
// the air, a rebound, a steal - it never stops: it runs on from one line
// straight into the next one's lead-in. Where the picture takes longer over
// it than the sim (the ball in the air longer than the sim gives the shot),
// it runs on true all the same, a little ahead of the sim's clock, and
// gives that back over the next trip up the floor - and with the period
// running out, it is out as time runs out, not crawling to 0.0 as the
// heave comes down (a shot as time runs out is up before it). After a
// whistle, a free throw or a ball out of bounds it waits for the ball to be
// back in play. The sim keeps no shot clock in the play-by-play, so the one
// on screen is read off possession: 24 when a team gets the ball, 14 after
// an offensive rebound, running with the game clock - and off once the game
// clock is shorter.

const SHOT_CLOCK = 24;
const RESET_ORB = 14;
// The last seconds into a line run true even when the picture holds more
// than the sim's time (see buildClocks).
const LEAD_TRUE = 3.5;
// How far (seconds) the clock on screen may run ahead of the sim's - and, a
// shot still to go up, never out before it is (see buildClocks).
export const LEAD_MOST = 2;
const SHOT_UP = 0.1;
// The shot clock runs at most this much faster (or slower) than the game
// clock on screen, catching up with the sim's (see shotClockAt).
const RATE_MOST = 1.5;
// (In the last few seconds, no more than this share of what is left: what
// comes next gets the time the sim gives it.)
const LEAD_SHARE = 0.25;
// A whistle on a shot: the and-one, or the free throws.
const SHOT_FOUL = /AndOne$|^pf(FG|TP)$/;
// A shot is let go this soon after its line, if at all (ms).
const RELEASE_MOST = 2000;

type Mark = { t: number; clock: number; period: number };

// A made basket (field goal or free throw; not an attempt - "fga..." - or a
// miss). The clock is reset by it; between free throws it doesn't matter.
const madeBasket = (e: RawEvent): boolean =>
	/^fg(?!a)/.test(e.type) || /^tp/.test(e.type) || e.type === "ft";

// A line the ball stays live through.
export const runsOn = (e: RawEvent): boolean =>
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
	// `line`: not a reset at all - the sim's own reading on a line, which the
	// clock on screen reads exactly there (see shotClockAt).
	resets: { t: number; from: number; hold?: true; line?: true }[];
	// An Elam Ending: from when the game clock is off, played to a target
	// score instead.
	elam?: { t: number; target: number };
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
	// The game clock a period started at, while it still reads that: the
	// lines that open a period carry the shot clock as the last one left it
	// (the sim restarts it after logging them), so the new quarter came up
	// with the last one's 14 or 17 on it until the ball was inbounded. Before
	// any time has run in a period its shot clock is a full one.
	let fresh: number | undefined;
	const possStarts = tl.poss.map(([t]) => t).filter(Number.isFinite);
	const possStartSet = new Set(possStarts);
	// How far the clock on screen is ahead of the sim's (see LEAD_MOST).
	let lead = 0;
	for (const [j, b] of tl.beats.entries()) {
		const e = events[b.i];
		if (!e) {
			continue;
		}
		if (e.type === "period" || e.type === "overtime") {
			period = typeof e.period === "number" ? e.period : period + 1;
			last = undefined;
			lead = 0;
			fresh = typeof e.clock === "number" ? e.clock : undefined;
		}
		// (Off for an Elam Ending: no reading to run it by.)
		if (typeof e.clock !== "number" || !Number.isFinite(e.clock)) {
			continue;
		}
		if (fresh !== undefined && e.clock < fresh) {
			fresh = undefined;
		}
		const logged = typeof e.shotClock === "number" ? e.shotClock : undefined;
		const shot =
			logged !== undefined && fresh !== undefined ? SHOT_CLOCK : logged;
		// The reading on the line - a shot the period ends on, as it is let go
		// (it is up before the horn).
		let at = b.actionStart;
		if (/^fga/.test(e.type)) {
			const next = nextTimed(tl, events, j);
			const out = next !== undefined && events[next.i]!.clock === 0;
			const release = out ? releaseOf(tl, b.actionStart, e.pid) : undefined;
			if (release !== undefined) {
				at = Math.min(release, (b.actionStart + next!.actionStart) / 2);
			}
		}
		// The lead-in runs the clock from the last line's reading to this one's -
		// unless the clock went up, which is a new period starting.
		if (last !== undefined && e.clock <= last) {
			// (What the clock read at the last line.)
			const was = last - lead;
			let from = Math.max(
				Math.min(live ?? b.preStart, b.preStart),
				marks.at(-1)?.t ?? -Infinity,
			);
			if (live === undefined) {
				// A dead ball: the clock waits for it to be put back in play -
				// the inbound caught.
				const inbound = inboundCaught(tl.ball, from, b.actionStart);
				if (inbound !== undefined) {
					marks.push({ t: from, clock: was, period });
					from = inbound;
				}
			}
			marks.push({ t: from, clock: was, period });
			// The time the sim spent in the lead-in that the picture runs through
			// fast (or cuts past): the clock runs true on either side of it and
			// makes up the rest there.
			// Where the picture holds more than the sim's time, the play itself -
			// what leads straight into the line - still runs true, and the
			// clock takes less of the time before it (waiting, if it must).
			const fast = fastIn(tl.fast, from, at);
			const cut = lastCutIn(tl.cuts, from, at);
			const shown = (at - from) / 1000;
			// Ahead of the sim's: no less than it was (it never goes back up),
			// and - the ball live all the while, the picture taking longer
			// than the sim - as far as it gets running true. (Not after a
			// dead ball - whatever is thrown about before it is put back in
			// play - nor on past a whistle on the shot: it stops as he is
			// hit.)
			const kept = Math.max(0, lead - (last - e.clock));
			lead =
				live === undefined ||
				fast ||
				cut !== undefined ||
				SHOT_FOUL.test(e.type)
					? kept
					: Math.max(
							kept,
							Math.min(
								shown - (was - e.clock),
								LEAD_MOST,
								Math.max(0, e.clock - SHOT_UP) * LEAD_SHARE,
								Math.max(0, (shot ?? Infinity) - SHOT_UP) * LEAD_SHARE,
							),
						);
			const now = e.clock - lead;
			const gap = was - now;
			if (
				e.clock === 0 &&
				live !== undefined &&
				!fast &&
				cut === undefined &&
				shown > gap
			) {
				// Time running out: out on time, and stopped there.
				marks.push({ t: from + gap * 1000, clock: 0, period });
			} else if (fast) {
				const [f0, f1] = fast;
				const tail = Math.min((at - f1) / 1000, gap);
				const head = Math.min((f0 - from) / 1000, gap - tail);
				marks.push(
					{ t: f0, clock: was - head, period },
					{ t: f1, clock: now + tail, period },
				);
			} else if (cut !== undefined && gap > shown) {
				marks.push(
					{ t: cut - 1, clock: was - (cut - 1 - from) / 1000, period },
					{ t: cut, clock: now + (at - cut) / 1000, period },
				);
			} else if (shown > gap + 0.05 && shown > LEAD_TRUE) {
				// (The ball live all the while, it never stops: a little of it
				// runs down before that too, slower.)
				const tail = Math.min(live === undefined ? gap : gap * 0.75, LEAD_TRUE);
				marks.push({
					t: at - tail * 1000,
					clock: now + tail,
					period,
				});
			}
		} else {
			lead = 0;
		}
		marks.push({ t: at, clock: e.clock - lead, period });
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
				resets.push({ t: b.actionStart, from: shot, line: true });
				anchor = { t: b.actionStart, clock: e.clock, shot, period };
			}
			exact = true;
		}
		last = e.clock;
		live = runsOn(e) ? at : undefined;
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
	// The Elam target, from the first moment of the line it is set on (or the
	// next one shown, if that line has nothing of its own to show).
	const ei = events.findIndex(
		(e) =>
			e.type === "elamActive" &&
			typeof (e as { target?: unknown }).target === "number",
	);
	const at = ei < 0 ? undefined : tl.beats.find((b) => b.i >= ei);
	return {
		marks,
		resets,
		...(ei >= 0
			? {
					elam: {
						t: at ? at.preStart : tl.end,
						target: (events[ei] as unknown as { target: number }).target,
					},
				}
			: {}),
	};
};

// When the first pass from one man to another in [a, b) - the inbound, after
// a dead ball - is caught, if there is one: in the air to him, or bounced
// to him (down off the floor and up into his hands).
export const inboundCaught = (
	ball: CourtTimeline["ball"],
	a: number,
	b: number,
): number | undefined => {
	let lo = 0;
	let hi = ball.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (ball[mid]!.t0 < a) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	for (let i = lo; i < ball.length; i++) {
		const s = ball[i]!;
		if (s.t0 >= b) {
			break;
		}
		if (s.kind !== "fly" || !("pid" in s.from)) {
			continue;
		}
		const up = ball[i + 1];
		const to =
			"pid" in s.to
				? s
				: up?.kind === "fly" &&
					  !("pid" in up.from) &&
					  "pid" in up.to &&
					  up.t0 - s.t1 < 1
					? up
					: undefined;
		if (to && to.t1 < b) {
			return to.t1;
		}
	}
	return undefined;
};

// The next line after the j-th that carries the clock.
const nextTimed = (
	tl: CourtTimeline,
	events: RawEvent[],
	j: number,
): CourtTimeline["beats"][number] | undefined => {
	for (let k = j + 1; k < tl.beats.length; k++) {
		const e = events[tl.beats[k]!.i];
		if (typeof e?.clock === "number" && Number.isFinite(e.clock)) {
			return tl.beats[k];
		}
	}
	return undefined;
};

// When the shot on a line at t is let go by him - the ball off his hands -
// if it is soon enough.
const releaseOf = (
	tl: CourtTimeline,
	t: number,
	pid: unknown,
): number | undefined => {
	let lo = 0;
	let hi = tl.ball.length;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (tl.ball[mid]!.t0 < t) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	for (let i = lo; i < tl.ball.length; i++) {
		const s = tl.ball[i]!;
		if (s.t0 > t + RELEASE_MOST) {
			break;
		}
		if (s.kind === "fly" && "pid" in s.from && s.from.pid === pid) {
			return s.t0;
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
	// It runs with the game clock on screen - stopped when that is - down to
	// exactly what the sim has on its next line. (The game clock on screen
	// can be a little ahead of the sim's, and give it back - see LEAD_MOST -
	// so the shot clock runs that little faster or slower to get there: it
	// never jumps at the line.)
	const next = c.resets[i + 1];
	const end = next?.line ? gameClockAt(c, next.t) : undefined;
	const rate =
		next && end !== undefined && start - end > 0.5 && next.from < r.from
			? Math.min(
					RATE_MOST,
					Math.max(1 / RATE_MOST, (r.from - next.from) / (start - end)),
				)
			: 1;
	const left = Math.max(0, r.from - (start - game) * rate);
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
