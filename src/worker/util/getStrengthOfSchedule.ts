import { idb } from "../db/index.ts";
import { g, helpers } from "./index.ts";

// How hard is the rest of the year?
//
// The measure is the one every sport uses: the average winning percentage of
// the opponents a team still has to play. Each remaining game counts
// separately, so facing the best team in the league three more times weighs
// three times as much as facing them once - which is the whole reason two teams
// with the same games left can have very different roads.
//
// Opponent records are SHRUNK toward .500 before averaging. Twelve games in, a
// 3-0 team is not a 1.000 opponent, and without shrinkage the column swings
// wildly in October and reads as noise for the month when people are most
// curious about it. The prior fades as the season fills in, so by midseason it
// is very nearly the raw opponent win percentage.
//
// Deliberately built from RECORDS only, never team ratings: this is a visible
// column, and a league that hides team ratings would otherwise have them leak
// out through it.

// Games of regression toward .500. Roughly a fortnight of an 82-game season -
// enough to stop early noise, small enough to be irrelevant by the All-Star
// break.
const PRIOR_GAMES = 10;

export const shrunkWinp = (winp: number, gp: number) =>
	(winp * gp + 0.5 * PRIOR_GAMES) / (gp + PRIOR_GAMES);

export type StrengthOfSchedule = {
	// Average shrunk opponent win percentage over the remaining games.
	sos: number;
	// How many games that average is over.
	gamesRemaining: number;
};

// Only meaningful for the season in progress - a finished season has no
// remaining games, and past seasons don't keep their schedule at all. Returns
// an empty map in those cases, which the UI renders as a blank column.
export const getStrengthOfSchedule = async (
	season: number,
): Promise<Map<number, StrengthOfSchedule>> => {
	if (season !== g.get("season")) {
		return new Map();
	}

	const [schedule, teamSeasons] = await Promise.all([
		idb.cache.schedule.getAll(),
		idb.getCopies.teamSeasons({ season }, "noCopyCache"),
	]);

	const strengthByTid = new Map<number, number>();
	for (const teamSeason of teamSeasons) {
		const gp = helpers.getTeamSeasonGp(teamSeason);
		strengthByTid.set(
			teamSeason.tid,
			shrunkWinp(helpers.calcWinp(teamSeason), gp),
		);
	}

	return computeStrengthOfSchedule(schedule, strengthByTid);
};

// The averaging itself, split out from the database reads so it can be tested.
export const computeStrengthOfSchedule = (
	schedule: { homeTid: number; awayTid: number }[],
	strengthByTid: Map<number, number>,
): Map<number, StrengthOfSchedule> => {
	const totals = new Map<number, { sum: number; games: number }>();
	const add = (tid: number, opponentTid: number) => {
		const strength = strengthByTid.get(opponentTid);
		if (strength === undefined) {
			return;
		}
		let entry = totals.get(tid);
		if (!entry) {
			entry = { sum: 0, games: 0 };
			totals.set(tid, entry);
		}
		entry.sum += strength;
		entry.games += 1;
	};

	for (const game of schedule) {
		// All-Star and trade-deadline rows are sentinels with negative tids, not
		// games anyone has to play.
		if (game.homeTid < 0 || game.awayTid < 0) {
			continue;
		}
		add(game.homeTid, game.awayTid);
		add(game.awayTid, game.homeTid);
	}

	const out = new Map<number, StrengthOfSchedule>();
	for (const [tid, entry] of totals) {
		if (entry.games > 0) {
			out.set(tid, {
				sos: entry.sum / entry.games,
				gamesRemaining: entry.games,
			});
		}
	}
	return out;
};
