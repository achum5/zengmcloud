import { helpers } from "../../common/helpers.ts";

// Projected draft picks without the team ratings.
//
// The projection used to rank teams by overall rating and read the pick off
// the rank. In a league that hides team ratings that printed them in disguise:
// before a game has been played the projected pick IS the rating rank, so a
// pick "projected 27th" told you that you were the fourth-best roster in the
// league. The ordering here is built from performance instead.
//
// A team's score is an expected winning percentage:
//
//   - Last season's record, regressed a third of the way to .500 - which is
//     about how much records regress from one season to the next.
//   - Adjusted for the roster moves since: the change in team strength from
//     the end of last season to now, at three points of winning percentage
//     per point of margin (the rating scale is 100 points to 30 of margin).
//     Bounded, so one number can never dominate a season of results.
//   - For seasons further out, tilted by the roster's age: a young core is
//     expected to improve and an old one to fade, a little per year.
//
// In-season, getEstPicks blends this ordering with the actual record as the
// season goes on, exactly as it blended the rating rank before.

export type PerformanceInput = {
	tid: number;
	// Last season's record; undefined for an expansion team or a fresh league.
	lastSeason?: { won: number; lost: number; tied?: number; otl?: number };
	// Team overall now, and at the end of last season (0-100 scale).
	ovrNow: number;
	ovrThen?: number;
	avgAge?: number;
};

export type PerformanceScore = {
	tid: number;
	// Expected winning percentage this season.
	score: number;
	// Change per season further out.
	tilt: number;
};

// Winning percentage per point of team overall: 100 rating points span 30 of
// margin, and a point of margin is worth about 3% of winning percentage.
const WINP_PER_OVR = 0.03 * (30 / 100);
const MAX_MOVES_SHIFT = 0.15;
const MAX_AGE_TILT = 0.02;
const AVERAGE_AGE = 27;

export const performanceScore = (t: PerformanceInput): PerformanceScore => {
	const ls = t.lastSeason;
	const gp = ls ? ls.won + ls.lost + (ls.tied ?? 0) + (ls.otl ?? 0) : 0;
	const winp = ls && gp > 0 ? (ls.won + 0.5 * (ls.tied ?? 0)) / gp : 0.5;
	const prior = 0.5 + (winp - 0.5) * (2 / 3);

	const delta = typeof t.ovrThen === "number" ? t.ovrNow - t.ovrThen : 0;
	const moves = helpers.bound(
		delta * WINP_PER_OVR,
		-MAX_MOVES_SHIFT,
		MAX_MOVES_SHIFT,
	);

	const tilt =
		typeof t.avgAge === "number"
			? helpers.bound(
					(AVERAGE_AGE - t.avgAge) * 0.01,
					-MAX_AGE_TILT,
					MAX_AGE_TILT,
				)
			: 0;

	return { tid: t.tid, score: prior + moves, tilt };
};

// Where each team is expected to pick, `seasonsAhead` seasons from now.
//
// This season, the score is blended with the record so far in the proportion
// of the season that has been played - the same blend getEstPicks makes with
// the rating rank. Further out, the blended expectation regresses toward .500
// the way records do, and the roster's age tilts it a little per year. Ties
// share the average of the positions they cover: a fresh league with no
// history projects everyone in the middle of the round, not in team-id
// order.
export const projectPicks = (
	scores: PerformanceScore[],
	records: Map<
		number,
		{ won: number; lost: number; tied?: number; otl?: number }
	>,
	numGames: number,
	seasonsAhead: number,
): Record<number, number> => {
	const expected = scores.map((s) => {
		const rec = records.get(s.tid);
		const gp = rec ? rec.won + rec.lost + (rec.tied ?? 0) + (rec.otl ?? 0) : 0;
		const fraction = numGames > 0 ? Math.min(1, gp / numGames) : 0;
		const recordWinp =
			rec && gp > 0 ? (rec.won + 0.5 * (rec.tied ?? 0)) / gp : 0.5;
		const thisSeason = fraction * recordWinp + (1 - fraction) * s.score;
		if (seasonsAhead <= 0) {
			return { tid: s.tid, wp: thisSeason };
		}
		const regressed = 0.5 + (thisSeason - 0.5) * (2 / 3);
		return { tid: s.tid, wp: regressed + seasonsAhead * s.tilt };
	});
	// Weakest first, as the draft goes.
	expected.sort((a, b) => a.wp - b.wp);

	const picks: Record<number, number> = {};
	let i = 0;
	while (i < expected.length) {
		let j = i;
		while (
			j + 1 < expected.length &&
			Math.abs(expected[j + 1]!.wp - expected[i]!.wp) < 1e-9
		) {
			j += 1;
		}
		const shared = Math.round((i + 1 + (j + 1)) / 2);
		for (let k = i; k <= j; k++) {
			picks[expected[k]!.tid] = shared;
		}
		i = j + 1;
	}
	return picks;
};
