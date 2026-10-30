// HOW OFTEN THE PICTURE IS DRAWN.
//
// Every frame drawn costs the same whatever the screen: everybody's moment
// worked out, the new poses among them made, the whole picture put together.
// So it is drawn at most 60 times a second - a screen that refreshes 90 or
// 120 times a second gets every second or third of them, not twice the work
// for motion nobody can tell from 60.
//
// And a device that can't draw 60 - its typical frame, a few seconds at a
// time, coming slower than 50 a second with the drawing what takes the time -
// draws a steady 30 instead: half the work, and smoother than an uneven 40
// with the odd long frame in it. A phone too - but one that draws 60 keeps
// 60, however little room that leaves it. Back to 60 once a frame at 30
// takes so little that 60 would leave room to spare, and then not again for
// a while.
//
// Paused, the game holds still - only the crowd and the benches move - and 30
// is plenty. Off the screen (scrolled away to the box score), nothing is
// drawn at all: the game goes on, as it does in a tab out of sight.

export const FAST = 60;
export const SLOW = 30;
// Settling in (faces loading, the first poses made) before judging, and
// after a change; how long a stretch is judged over; how many in a row must
// agree.
const SETTLE_MS = 2500;
const STRETCH_MS = 3000;
const AGREE = 2;
// Slower than this between frames (ms, typically), with this much of each
// spent drawing: down to SLOW.
const SLOW_GAP_MS = 20;
const SLOW_WORK_MS = 10;
// At SLOW, a frame drawn in under this (typically): up to FAST again - but
// not within this long of going down to SLOW, and only so many times.
const FAST_WORK_MS = 6;
const RETRY_MS = 20_000;
const MAX_RISES = 3;
// A frame due this soon is drawn now: the screen's own frames don't land
// exactly on ours.
const SLACK_MS = 3;

export type Pace = {
	fps: typeof FAST | typeof SLOW;
	// The last frame drawn, and when the rate last changed (or the stretch
	// began), and the stretch so far: the time between frames and drawing
	// each (ms).
	drawnAt: number;
	since: number;
	from: number;
	gaps: number[];
	work: number[];
	said: number[];
	droppedAt: number;
	rises: number;
};

export const makePace = (now: number): Pace => ({
	fps: FAST,
	drawnAt: -Infinity,
	since: now,
	from: now,
	gaps: [],
	work: [],
	said: [],
	droppedAt: -Infinity,
	rises: 0,
});

// The rate frames are drawn at now - paused, no more than SLOW.
export const rateOf = (pace: Pace, paused = false): number =>
	paused ? Math.min(pace.fps, SLOW) : pace.fps;

// Whether to draw a frame now.
export const due = (pace: Pace, now: number, paused = false): boolean =>
	now - pace.drawnAt >= 1000 / rateOf(pace, paused) - SLACK_MS;

const median = (xs: number[]) => {
	const s = [...xs].sort((a, b) => a - b);
	return s[s.length >> 1] ?? 0;
};

// A frame was drawn at `now`, taking `workMs`: the books kept, and true if
// the rate just changed. (A frame drawn after a break in drawing - off the
// screen, out of sight - or held to SLOW while paused starts a fresh
// stretch rather than counting a gap that says nothing about the device.)
export const drawn = (
	pace: Pace,
	now: number,
	workMs: number,
	paused = false,
): boolean => {
	const gap = now - pace.drawnAt;
	pace.drawnAt = now;
	if (now - pace.since < SETTLE_MS || gap > 250 || paused) {
		pace.from = now;
		pace.gaps = [];
		pace.work = [];
		return false;
	}
	pace.gaps.push(gap);
	pace.work.push(workMs);
	if (now - pace.from < STRETCH_MS) {
		return false;
	}
	const gapMs = median(pace.gaps);
	const work = median(pace.work);
	pace.from = now;
	pace.gaps = [];
	pace.work = [];
	let verdict = 0;
	if (pace.fps === FAST && gapMs > SLOW_GAP_MS && work > SLOW_WORK_MS) {
		verdict = -1;
	} else if (
		pace.fps === SLOW &&
		work < FAST_WORK_MS &&
		now - pace.droppedAt > RETRY_MS &&
		pace.rises < MAX_RISES
	) {
		verdict = 1;
	}
	pace.said.push(verdict);
	const last = pace.said.slice(-AGREE);
	if (verdict === 0 || last.length < AGREE || last.some((v) => v !== verdict)) {
		return false;
	}
	pace.said = [];
	pace.since = now;
	if (verdict < 0) {
		pace.fps = SLOW;
		pace.droppedAt = now;
	} else {
		pace.fps = FAST;
		pace.rises += 1;
	}
	return true;
};
