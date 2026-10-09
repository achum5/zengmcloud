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
