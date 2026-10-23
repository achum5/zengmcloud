import type { CourtTimeline, RawEvent } from "./director.ts";

// TEAM FOULS, FOR THE SCORE BUG.
//
// The play-by-play carries every foul (a "pf..." line, `t` the team that
// committed it) and the game clock it came at, so the count on screen is
// rebuilt from it the way the sim keeps it: per period, from the line that
// starts it, plus a second count over the last two minutes of each. A team is
// in the bonus once the OTHER team has committed enough - the sim's own rule
// (see GameSim.getNumFoulsUntilBonus): a limit per regulation period, another
// per overtime, and a lower one over the last two minutes.

// In box score order: 0 the home team, 1 the visitors.
export type TeamFouls = {
	fouls: [number, number];
	bonus: [boolean, boolean];
};

export type FoulMark = TeamFouls & { t: number };

// The league's limits, as the game attribute holds them: [regulation,
// overtime, last two minutes].
export const DEFAULT_FOULS_UNTIL_BONUS: [number, number, number] = [5, 4, 2];

const LAST_TWO = 2 * 60;

const NONE: TeamFouls = { fouls: [0, 0], bonus: [false, false] };

export const buildFouls = (
	tl: Pick<CourtTimeline, "beats">,
	events: RawEvent[],
	foulsUntilBonus: readonly number[] = DEFAULT_FOULS_UNTIL_BONUS,
	// Periods in regulation: past them, it is overtime.
	numPeriods = 4,
): FoulMark[] => {
	const [regular = 5, overtime = 4, lastTwo = 2] = foulsUntilBonus;
	const marks: FoulMark[] = [];
	let period = 1;
	let fouls: [number, number] = [0, 0];
	let late: [number, number] = [0, 0];
	let clock = Infinity;
	const push = (t: number) => {
		const limit = period > numPeriods ? overtime : regular;
		const over = (k: 0 | 1) =>
			fouls[k] >= limit || (clock <= LAST_TWO && late[k] >= lastTwo);
		marks.push({
			t,
			fouls: [fouls[0], fouls[1]],
			// In the bonus: the other team over its limit.
			bonus: [over(1), over(0)],
		});
	};
	for (const b of tl.beats) {
		const e = events[b.i];
		if (!e) {
			continue;
		}
		if (e.type === "period" || e.type === "overtime") {
			period = typeof e.period === "number" ? e.period : period + 1;
			fouls = [0, 0];
			late = [0, 0];
			clock = Infinity;
			push(b.actionStart);
			continue;
		}
		if (typeof e.clock === "number") {
			clock = e.clock;
		}
		if (/^pf/.test(e.type) && (e.t === 0 || e.t === 1)) {
			const k: 0 | 1 = e.t;
			fouls[k] += 1;
			if (clock <= LAST_TWO) {
				late[k] += 1;
			}
			// (Only fouls in the last two minutes count toward its lower limit,
			// so the bonus only ever changes here.)
			push(b.actionStart);
		}
	}
	return marks;
};

// The fouls at timeline time t.
export const foulsAt = (marks: FoulMark[], t: number): TeamFouls => {
	let lo = 0;
	let hi = marks.length - 1;
	let found = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (marks[mid]!.t <= t) {
			found = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	return found >= 0 ? marks[found]! : NONE;
};

// THE FINAL HORN: from it on, the bug says the game is over - FINAL, or
// FINAL/OT after overtime - where the clock was, not the last period and a
// clock stopped at 0:00 through the handshakes.
export const isFinal = (
	tl: Pick<CourtTimeline, "beats">,
	t: number,
): boolean => {
	const k = tl.beats.findLastIndex((b) => b.type === "gameOver");
	if (k < 0) {
		return false;
	}
	// (The horn itself: the end of the last period, just before.)
	const before = tl.beats[k - 1];
	const horn = before?.type === "endOfPeriod" ? before : tl.beats[k]!;
	return t >= horn.actionStart;
};
// (The last period, as the bug carries it: "Q4", "OT", "2OT".)
export const finalText = (period: string): string =>
	/OT$/.test(period) ? `FINAL/${period}` : "FINAL";

// BETWEEN PERIODS: from the horn that ends one (not the last) until the
// next one starts, the bug says so where the clock was - END Q1, HALF at
// halftime, END Q3, END OT - not the period just ended and 0.0.
export const isBreak = (
	tl: Pick<CourtTimeline, "beats">,
	t: number,
): boolean => {
	const { beats } = tl;
	let lo = 0;
	let hi = beats.length - 1;
	let k = -1;
	while (lo <= hi) {
		const mid = (lo + hi) >> 1;
		if (beats[mid]!.actionStart <= t) {
			k = mid;
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}
	const next = beats[k + 1];
	return (
		k >= 0 &&
		beats[k]!.type === "endOfPeriod" &&
		next !== undefined &&
		next.type !== "gameOver"
	);
};
// (The period just ended, as the bug carries it, and how many a game has.)
export const breakText = (period: string, numPeriods: number): string => {
	const n = /^[A-Z](\d+)$/.exec(period);
	return n && numPeriods % 2 === 0 && Number(n[1]) === numPeriods / 2
		? "HALF"
		: `END ${period}`;
};

// THE SHOT CLOCK ON THE BUG: whole seconds, the way it counts - and in its
// last five, tenths and in red, the way the clock in the building shows them
// and a broadcast's bug carries them as it runs out.
export const SHOT_LOW = 5;
export const SHOT_LOW_INK = "#ff5040";
export const shotClockText = (shot: number): string => {
	const tenths = Math.max(0, Math.ceil(shot * 10 - 1e-6) / 10);
	return tenths < SHOT_LOW ? tenths.toFixed(1) : String(Math.ceil(shot - 1e-6));
};
