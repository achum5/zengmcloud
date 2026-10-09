import { benchX, type Pt, type Side } from "./geometry.ts";

// THE STARTING LINEUPS, before the opening tip of a whole game (never a
// highlight reel): the lights go down and each side's starters are called
// out one at a time - the road team's first, quickly, then the home team's,
// each with longer and more noise - every one running out from in front of
// his bench, under a spotlight, to his place in his team's line across the
// middle of the floor, slapping hands with the man called before him and
// turning to face the cameras. Once all ten are out the lights come back up
// and they walk out for the tip. A playoff game makes more of all of it.

export type IntroCall = {
	pid: number;
	team: Side;
	// When he is called, and when the next man is (or, the last, when the
	// lights start back up).
	t0: number;
	t1: number;
};

export type Intro = {
	t0: number;
	t1: number;
	big: boolean;
	calls: IntroCall[];
};

// How long the lights take to go down, and to come back up (ms).
export const INTRO_DARK = 1100;
export const INTRO_LIGHT = 900;
// The first name called, after the lights start down; each one after it, by
// team (road, home); and all ten out before the lights come up -
// [regular season, playoffs].
export const INTRO_FIRST: [number, number] = [1300, 2600];
export const INTRO_CALL: Record<Side, [number, number]> = {
	0: [1400, 1700],
	1: [1900, 2400],
};
export const INTRO_HOLD: [number, number] = [1100, 2000];

// The order they are called in: forwards, then the center, then the guards -
// the way the arena announcer does it.
const roleOf = (pos: string | undefined): number => {
	const p = (pos ?? "").toUpperCase();
	if (p === "C") {
		return 1;
	}
	if (p === "PG" || p === "SG" || p === "G") {
		return 2;
	}
	return 0;
};
export const callOrder = <T extends { pos?: string }>(starters: T[]): T[] =>
	starters
		.map((p, i) => ({ p, i }))
		.sort((a, b) => roleOf(a.p.pos) - roleOf(b.p.pos) || a.i - b.i)
		.map(({ p }) => p);

// Waiting to be called: standing in a line in front of his own bench, facing
// the floor - in the order they are called, the first at the far end.
export const TUNNEL_Y = -3.4;
export const TUNNEL_GAP = 2.9;
export const tunnelSpot = (t: Side, j: number): Pt => ({
	x: benchX(t) + (t === 0 ? -1 : 1) * (2 - j) * TUNNEL_GAP,
	y: TUNNEL_Y,
});
// The coach, out of their way at the end of the line, toward his own end.
export const coachSpot = (t: Side): Pt => ({
	x: benchX(t) + (t === 0 ? -1 : 1) * (2 * TUNNEL_GAP + 3.2),
	y: TUNNEL_Y + 0.6,
});

// His place in his team's line, out past the free throw line on his own
// bench's side: the first called furthest out, the last nearest the middle
// of the floor - one team's line each side of it.
export const ROW_Y = 13;
export const ROW_GAP = 4.2;
export const rowSpot = (t: Side, k: number): Pt => ({
	x: t === 0 ? 26.4 + k * ROW_GAP : 67.6 - k * ROW_GAP,
	y: ROW_Y,
});

// How dark the building is at t: none outside the intro, all the way down
// between the lights going down and coming back up.
const smooth = (u: number) => {
	const c = Math.min(1, Math.max(0, u));
	return c * c * (3 - 2 * c);
};
export const darkAt = (intro: Intro | undefined, t: number): number => {
	if (!intro || t < intro.t0 || t >= intro.t1) {
		return 0;
	}
	return Math.min(
		smooth((t - intro.t0) / INTRO_DARK),
		smooth((intro.t1 - t) / INTRO_LIGHT),
	);
};

// The man being called at t, if any.
export const callAt = (
	intro: Intro | undefined,
	t: number,
): IntroCall | undefined => {
	if (!intro || t < intro.t0 || t >= intro.t1) {
		return undefined;
	}
	for (const c of intro.calls) {
		if (t >= c.t0 && t < c.t1) {
			return c;
		}
	}
	return undefined;
};

// Everyone called out by t - in their lines, lit.
export const calledBy = (intro: Intro | undefined, t: number): IntroCall[] =>
	intro ? intro.calls.filter((c) => c.t0 <= t) : [];
