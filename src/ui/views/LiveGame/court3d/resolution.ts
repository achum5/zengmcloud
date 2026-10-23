// HOW FINE THE PICTURE IS.
//
// The court is drawn small and blown up without smoothing, each of its
// pixels a whole number of the screen's (see Court3D) - as fine as about
// ROWS of them top to bottom. A device that can't draw that many fast
// enough steps a size coarser, never coarser than about MIN_ROWS, and back
// finer once it has time to spare.
//
// It is judged a few seconds at a time, on the typical frame of each
// stretch - not the odd long one - and changes only when two stretches in a
// row agree. Long frames come in bursts that say nothing of how fast the
// device draws: every man's new pose at a cut or a jump to the next key
// play, the teams coming out, the page busy elsewhere for a moment. Judged
// a frame at a time, a burst like that every few seconds stepped a picture
// drawn easily at 60 frames a second coarser and coarser, to the coarsest,
// for the rest of the game.

export const ROWS = 760;
const MIN_ROWS = 300;
// Settling in (faces loading, the first sprites drawn) before judging, and
// between steps.
const SETTLE_MS = 2500;
const MAX_CHANGES = 6;
// How long a stretch is judged over, and how many in a row must agree.
const STRETCH_MS = 3000;
const AGREE = 2;

export type Resolution = {
	// Steps coarser than the finest.
	coarser: number;
	// The time drawing a frame, smoothed (ms) - what Court3D goes by for
	// finer poses.
	drawMs?: number;
	// This stretch so far, frame by frame: the time since the last frame,
	// drawing it, and making new pictures of the players for it (ms).
	stretch: { from: number; frame: number[]; draw: number[]; sprite: number[] };
	// The stretches judged since the last change: too slow (1), time to
	// spare for a size finer (-1), or neither (0).
	said: number[];
	// When it last changed, and how many times it has.
	since: number;
	changes: number;
};

const freshStretch = (from: number): Resolution["stretch"] => ({
	from,
	frame: [],
	draw: [],
	sprite: [],
});

export const makeResolution = (now: number): Resolution => ({
	coarser: 0,
	stretch: freshStretch(now),
	said: [],
	since: now,
	changes: 0,
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

const median = (xs: number[]) => {
	const s = [...xs].sort((a, b) => a - b);
	return s[s.length >> 1] ?? 0;
};

// A frame took `drawMs` to draw - `spriteMs` of it on new pictures of the
// players - and came `frameMs` after the last: true if the picture should
// now change size.
export const adjust = (
	res: Resolution,
	deviceRows: number,
	frameMs: number,
	drawMs: number,
	now: number,
	spriteMs = 0,
): boolean => {
	res.drawMs =
		res.drawMs === undefined ? drawMs : res.drawMs * 0.94 + drawMs * 0.06;
	if (now - res.since < SETTLE_MS || res.changes >= MAX_CHANGES) {
		res.stretch = freshStretch(now);
		return false;
	}
	const st = res.stretch;
	st.frame.push(frameMs);
	st.draw.push(drawMs);
	st.sprite.push(Math.min(spriteMs, drawMs));
	if (now - st.from < STRETCH_MS) {
		return false;
	}
	const frame = median(st.frame);
	const draw = median(st.draw);
	const sprite = Math.min(draw, median(st.sprite));
	res.stretch = freshStretch(now);
	const art = artFor(deviceRows, res);
	const { finest, coarsest } = bounds(deviceRows);
	let verdict = 0;
	if (frame > 26 && draw > 12 && art < coarsest) {
		// Under about 40 frames a second - and the picture the reason: if
		// drawing it is not what takes the time (something else on the page,
		// the browser cleaning up after itself), a coarser one would only look
		// worse, no faster.
		verdict = 1;
	} else if (
		art > finest &&
		frame < 18.5 &&
		// A size finer, the players' new pictures have that many more pixels
		// to them; the rest of the drawing - working out where everyone is,
		// handing the floor and the stands to the graphics card - hardly
		// changes.
		draw - sprite + sprite * (art / (art - 1)) ** 2 < 11
	) {
		verdict = -1;
	}
	res.said.push(verdict);
	const last = res.said.slice(-AGREE);
	if (verdict === 0 || last.length < AGREE || last.some((v) => v !== verdict)) {
		return false;
	}
	res.coarser += verdict;
	res.since = now;
	res.changes += 1;
	res.said = [];
	res.drawMs = undefined;
	return true;
};
