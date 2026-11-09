// THE TIMELINE, as every sport's director stages a game into it (see
// court3d/director.ts, field3d/director.ts): what each line of the
// play-by-play comes to - a beat - and the end of the game; and where the
// picture stands for the page's playback cursor (events consumed).

// A line of the play-by-play (`i` its index among the events): from when
// its lead-in starts, to when it happens (its text appears), to when what
// it was is over.
export type Beat = {
	i: number;
	type: string;
	preStart: number;
	actionStart: number;
	end: number;
};

export type Beats = { beats: Beat[]; end: number };

// Where the animation should stand while the playback cursor (events
// consumed) is at `cursor`: the moment the next unshown line happens. Past
// the last line, the end of the game.
export const targetForCursor = (tl: Beats, cursor: number): number => {
	const beat = tl.beats.find((b) => b.i >= cursor);
	return beat ? beat.actionStart : tl.end;
};

// Where to cut to when playback jumps (a rewind, a fast-forward, joining a
// broadcast late): the end of the last line already shown.
export const snapForCursor = (tl: Beats, cursor: number): number => {
	let t = 0;
	for (const b of tl.beats) {
		if (b.i >= cursor) {
			break;
		}
		t = b.end;
	}
	return t;
};

// How many play-by-play lines the page showed going from one cursor to
// another. A line can come with events that are not lines - a basket with the
// points, rebounds and assists it adds up - so this, not how far the cursor
// moved, says whether the page jumped ahead.
export const linesBetween = (tl: Beats, from: number, to: number): number => {
	let n = 0;
	for (const b of tl.beats) {
		if (b.i >= to) {
			break;
		}
		if (b.i >= from) {
			n += 1;
		}
	}
	return n;
};

// A clock read off readings of it laid along the timeline (ascending `t`):
// at t, run straight on from one reading to the next - down, never up, and
// not on into the next period - and held at the last; before the first, the
// first.
export type ClockMark = { t: number; clock: number; period?: number };
export const clockAt = (marks: ClockMark[], t: number): number | undefined => {
	let lo = 0;
	let hi = marks.length - 1;
	let i = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (marks[mid]!.t <= t) {
			i = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	if (i < 0) {
		return marks[0]?.clock;
	}
	const a = marks[i]!;
	const b = marks[i + 1];
	if (!b || b.period !== a.period || b.clock > a.clock || b.t <= a.t) {
		return a.clock;
	}
	const u = Math.min(1, (t - a.t) / (b.t - a.t));
	return a.clock + (b.clock - a.clock) * u;
};
