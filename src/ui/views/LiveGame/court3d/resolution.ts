// HOW FINE THE PICTURE IS.
//
// The court is drawn at the screen's own resolution - one of its pixels to
// each of the screen's, sharp - up to about ROWS of them top to bottom (past
// that, on a big high-density screen, two or more of the screen's to each,
// blown up without smoothing). A device that can't draw that many fast
// enough steps a size coarser - pixel art, the fallback - never coarser than
// about MIN_ROWS, and back finer once it has time to spare. (Sculpting new
// poses, the costliest part of a frame and the more so the finer it is, is
// done off the page's thread where it can be - see sculptPool.ts.)

export const ROWS = 1440;
const MIN_ROWS = 300;
// Settling in (faces loading, the first sprites drawn) before judging, and
// between steps.
const SETTLE_MS = 2500;
const MAX_CHANGES = 6;
// A size found too slow this many times is not gone back to.
const MAX_SLOW = 2;
// A frame that hitched - a new pose sculpted on the spot, a face come in -
// counts for no more than this (ms).
const MAX_DRAW = 40;

export type Resolution = {
	// Steps coarser than the finest.
	coarser: number;
	// The time between frames and the time drawing one, smoothed (ms).
	frameMs?: number;
	drawMs?: number;
	// When it last changed (or the first frame was drawn), and how many
	// times it has.
	since: number;
	changes: number;
	// How many times each size (steps coarser) has been found too slow.
	slow: Record<number, number>;
};

export const makeResolution = (now: number): Resolution => ({
	coarser: 0,
	since: now,
	changes: 0,
	slow: {},
});

const bounds = (deviceRows: number) => {
	const finest = Math.max(1, Math.ceil(deviceRows / ROWS));
	return {
		finest,
		coarsest: Math.max(finest, Math.ceil(deviceRows / MIN_ROWS)),
	};
};

// Screen pixels to one of the picture's, for a picture `deviceRows` screen
// pixels tall.
export const artFor = (deviceRows: number, res: Resolution): number => {
	const { finest, coarsest } = bounds(deviceRows);
	return Math.min(coarsest, finest + res.coarser);
};

// A frame took `drawMs` to draw and came `frameMs` after the last: true if
// the picture should now change size.
export const adjust = (
	res: Resolution,
	deviceRows: number,
	frameMs: number,
	drawMs: number,
	now: number,
): boolean => {
	const smooth = (old: number | undefined, v: number) =>
		old === undefined ? v : old * 0.94 + v * 0.06;
	if (res.frameMs === undefined) {
		// The first frame at this size - or at all, which can be a while
		// coming (the game staged first): settling in from here.
		res.since = Math.max(res.since, now);
	}
	res.frameMs = smooth(res.frameMs, Math.min(100, frameMs));
	res.drawMs = smooth(res.drawMs, Math.min(MAX_DRAW, drawMs));
	if (now - res.since < SETTLE_MS || res.changes >= MAX_CHANGES) {
		return false;
	}
	const art = artFor(deviceRows, res);
	const { finest, coarsest } = bounds(deviceRows);
	let next = res.coarser;
	if (res.frameMs > 26 && res.drawMs > 12 && art < coarsest) {
		// Under about 40 frames a second - and the picture the reason: if
		// drawing it is not what takes the time (something else on the page,
		// the browser cleaning up after itself), a coarser one would only look
		// worse, no faster.
		res.slow[res.coarser] = (res.slow[res.coarser] ?? 0) + 1;
		next += 1;
	} else if (
		art > finest &&
		res.frameMs < 18.5 &&
		// A size finer is more to draw - though not so much more as it has
		// pixels, most of the work (the players sculpted) done elsewhere.
		res.drawMs * (art / (art - 1)) < 11 &&
		(res.slow[res.coarser - 1] ?? 0) < MAX_SLOW
	) {
		next -= 1;
	}
	if (next === res.coarser) {
		return false;
	}
	res.coarser = next;
	res.since = now;
	res.changes += 1;
	res.frameMs = undefined;
	res.drawMs = undefined;
	return true;
};
