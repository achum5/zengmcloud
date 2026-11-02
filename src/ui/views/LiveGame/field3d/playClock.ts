import { makeCourtRng } from "../courtRng.ts";
import type { FieldTimeline, Play, RawEvent } from "./director.ts";

// THE PLAY CLOCK ON THE BUG (the 3D court's shot clock, for football: the
// same place on the bug). From the picture coming up on a down from
// scrimmage to its snap, counting down at real speed to what the sim's
// time between snaps leaves on it: the clock running between them, a
// long while off it (37 to 62 seconds, the sim's) and little left; the
// two-minute drill, most of it; the clock stopped, anything from a few
// seconds to most of the 25 or 40. A kneel to run it out bleeds it, and a
// delay of game runs it out.

export type PlayClockSpan = {
	// Shown from the picture coming up on the play to the snap.
	from: number;
	until: number;
	// Seconds left at `until` (under 0: at 0 a while before it).
	at: number;
};

// Snaps with this little left on it at most, and with this much at most.
const LEAST = 1;
const MOST = 30;
// The clock running between snaps, the sim's 37 to 62 seconds off it (or
// at least this many - fewer is the sim's play running longer than the
// picture's, not the clock): snapped with this many left at 37, and fewer
// for each second over.
const RUNNING_OVER = 30;
const RUNNING_FROM = 37;
const RUNNING_LEFT = 18;
const RUNNING_PER = 0.64;
// The two-minute drill, no huddle: the clock running, the play clock
// started this long after the whistle; stopped, snapped with between this
// many left and this many more.
const HURRY_DEAD = 4;
const HURRY: [number, number] = [18, 10];
// Otherwise, the clock stopped (the sim says nothing of how long):
// between this many left and this many more.
const STOPPED: [number, number] = [4, 13];
// A try after a touchdown.
const TRY: [number, number] = [6, 9];
// A kneel with the clock running: down to this, and up to this many more.
const BLED: [number, number] = [1, 2];
// A delay of game: 0 on it this long (ms) past when the snap would have
// been.
const DELAY_HOLD = 1500;

const KINDS = new Set(["pass", "run", "kneel", "punt", "fieldGoal"]);

const readingOf = (e: RawEvent | undefined): number | undefined =>
	e?.type === "clock" && typeof e.clock === "number" ? e.clock : undefined;

// (Flagged for a delay of game, the flag standing.)
const delayed = (events: RawEvent[], i0: number, i1: number): boolean =>
	events
		.slice(i0, i1)
		.some(
			(e) =>
				e?.type === "penalty" &&
				e.penaltyName === "Delay of game" &&
				e.decision !== "decline",
		);

export const buildPlayClocks = (
	tl: FieldTimeline,
	events: RawEvent[],
): PlayClockSpan[] => {
	const out: PlayClockSpan[] = [];
	const plays = tl.plays;
	for (const [k, play] of plays.entries()) {
		const next = plays[k + 1];
		const i1 = next?.i ?? events.length;
		const delay = play.kind === "none" && delayed(events, play.i, i1);
		if (
			!(
				KINDS.has(play.kind) ||
				play.kind === "extraPoint" ||
				delay ||
				(play.kind === "none" && play.flags.length > 0)
			) ||
			play.snap <= play.t0
		) {
			continue;
		}
		const r = makeCourtRng(`field|playClock|${play.i}`);
		out.push(
			delay
				? {
						from: play.t0,
						until: play.snap + DELAY_HOLD,
						at: -DELAY_HOLD / 1000,
					}
				: {
						from: play.t0,
						until: play.snap,
						at:
							play.kind === "extraPoint"
								? TRY[0] + TRY[1] * r()
								: leftAt(play, plays[k - 1], events, r),
					},
		);
	}
	return out;
};

// What is left on it at the snap, by how long the sim ran the game clock
// between the last play and this one.
const leftAt = (
	play: Play,
	last: Play | undefined,
	events: RawEvent[],
	r: () => number,
): number => {
	const now = readingOf(events[play.i]);
	const was = last ? readingOf(events[last.i]) : undefined;
	// (The sim's minutes off the clock from the last snap to this one, less
	// the last play's own, in seconds.)
	const off =
		last &&
		last.quarter === play.quarter &&
		now !== undefined &&
		was !== undefined
			? Math.max(
					0,
					(was - now) * 60 - Math.max(0, last.whistle - last.snap) / 1000,
				)
			: 0;
	let at: number;
	if (off >= RUNNING_OVER) {
		at =
			play.kind === "kneel"
				? BLED[0] + BLED[1] * r()
				: RUNNING_LEFT - (off - RUNNING_FROM) * RUNNING_PER;
	} else if (play.hurry) {
		at = off > 2 ? 40 - HURRY_DEAD - off : HURRY[0] + HURRY[1] * r();
	} else {
		at = STOPPED[0] + STOPPED[1] * r();
	}
	return Math.min(MOST, Math.max(LEAST, at));
};

// What the play clock says at t, if it shows.
export const playClockAt = (
	spans: PlayClockSpan[],
	t: number,
): number | undefined => {
	let lo = 0;
	let hi = spans.length - 1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		const s = spans[mid]!;
		if (t < s.from) {
			hi = mid - 1;
		} else if (t >= s.until) {
			lo = mid + 1;
		} else {
			return Math.max(0, s.at + (s.until - t) / 1000);
		}
	}
	return undefined;
};
