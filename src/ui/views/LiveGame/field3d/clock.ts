import { formatClock } from "../../../../common/formatClock.ts";
import { getPeriodName } from "../../../../common/getPeriodName.ts";
import { breakText, finalText } from "../court3d/scoreBug.ts";
import type { FieldTimeline, Play, RawEvent } from "./director.ts";

// THE GAME CLOCK ON THE BUG (the 3D court's, for football: see
// court3d/clock.ts), running with the picture instead of jumping from one
// line to the next. The sim gives the clock at each snap, and nothing in
// between: from the snap it runs at real speed, as long as the play takes
// on the field or the sim's time to the next snap runs out (a pass
// incomplete, out of bounds: stopped there) - and what is left of that
// time runs off after the whistle, at real speed too where the picture
// has the room for it, quicker where it has not: by the next snap, or by
// the picture cutting to a timeout, a man down, the chain out. The last
// play of a quarter runs it out to 0:00 - and from then the bug says so
// where the clock was, END Q1, HALF, END Q3, and FINAL at the end.

type Mark = { t: number; clock: number };
type Stopped = { from: number; until: number; text: string };

export type FieldClocks = {
	// Clock readings (minutes left in the quarter) in timeline order, run
	// straight from one to the next.
	marks: Mark[];
	// Between quarters, and the game over: what the bug says instead.
	stopped: Stopped[];
};

// The last of a quarter's time run off in no more than this (ms) after the
// whistle - or half of what the picture has left of the play.
const OUT_MOST = 2500;

const readingOf = (e: RawEvent | undefined): number | undefined =>
	e?.type === "clock" && typeof e.clock === "number" ? e.clock : undefined;

// (The quarter as the bug carries it: Q1, OT, 2OT.)
const periodOf = (quarter: number, numPeriods: number): string => {
	const over = quarter - numPeriods;
	return over > 0
		? over === 1
			? "OT"
			: `${over}OT`
		: `${getPeriodName(numPeriods, true)}${quarter}`;
};

// Where the picture stops the clock for something after the play, if it
// does.
const stopOf = (play: Play): number | undefined =>
	play.timeout?.cut ?? play.hurt?.from ?? play.measure?.cut;

export const buildFieldClocks = (
	tl: FieldTimeline,
	events: RawEvent[],
	numPeriods: number,
): FieldClocks => {
	const marks: Mark[] = [];
	const stopped: Stopped[] = [];
	const plays = tl.plays;
	for (const [k, play] of plays.entries()) {
		const at = readingOf(events[play.i]);
		if (at === undefined) {
			continue;
		}
		const next = plays[k + 1];
		const nextAt = next ? readingOf(events[next.i]) : undefined;
		const sameQuarter = next !== undefined && next.quarter === play.quarter;
		// What it reads at the next snap - or, the last play of the
		// quarter, nothing left.
		const over = events
			.slice(play.i, next?.i ?? events.length)
			.find((e) => e?.type === "gameOver");
		const end = sameQuarter
			? (nextAt ?? at)
			: over && typeof over.clock === "number"
				? over.clock
				: 0;
		// (A new quarter: its time from the start of its first play.)
		if (marks.length === 0 || marks.at(-1)!.clock !== at) {
			marks.push({ t: play.t0, clock: at });
		}
		marks.push({ t: play.snap, clock: at });
		const left = Math.max(0, (at - end) * 60_000);
		const inPlay = Math.min(left, Math.max(0, play.whistle - play.snap));
		marks.push({ t: play.snap + inPlay, clock: at - inPlay / 60_000 });
		if (left > inPlay) {
			const stop = stopOf(play);
			const by = sameQuarter
				? Math.min(next.snap, stop ?? Infinity)
				: Math.min(
						play.whistle + OUT_MOST,
						play.whistle + (play.t1 - play.whistle) / 2,
					);
			const room = Math.max(1, by - play.whistle);
			marks.push({
				t: play.whistle + Math.min(left - inPlay, room),
				clock: end,
			});
		}
		if (!sameQuarter) {
			const from = marks.at(-1)!.t;
			const period = periodOf(play.quarter, numPeriods);
			stopped.push(
				over
					? { from, until: Infinity, text: finalText(period) }
					: {
							from,
							until: next?.t0 ?? Infinity,
							text: breakText(period, numPeriods),
						},
			);
		}
	}
	return { marks, stopped };
};

// What the bug's clock says at t.
export const fieldClockAt = (
	c: FieldClocks,
	t: number,
): { text: string; ended: boolean } | undefined => {
	const s = c.stopped.find((x) => t >= x.from && t < x.until);
	if (s) {
		return { text: s.text, ended: true };
	}
	const { marks } = c;
	let lo = 0;
	let hi = marks.length - 1;
	let k = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (marks[mid]!.t <= t) {
			k = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	const a = marks[k] ?? marks[0];
	if (!a) {
		return undefined;
	}
	const b = marks[k + 1];
	let clock = a.clock;
	if (k >= 0 && b && b.t > a.t && b.clock <= a.clock) {
		clock += (b.clock - a.clock) * Math.min(1, (t - a.t) / (b.t - a.t));
	}
	return { text: formatClock(Math.max(0, clock)), ended: false };
};
