import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import type {
	ScheduleGame,
	ScheduleGameWithoutKey,
} from "../../../common/types.ts";
import addDaysToSchedule from "./addDaysToSchedule.ts";
import { PHASE } from "../../../common/constants.ts";
import { isFinals } from "./isFinals.ts";
import { recomputeLocalUITeamOvrs } from "../../util/recomputeLocalUITeamOvrs.ts";
import { isOffDay, OFF_DAY_TIDS } from "../../../common/specialGames.ts";
import { daysBetween } from "../../../common/calendarDates.ts";
import { offDaysThisSeason, playoffOffDates } from "./offDays.ts";

// (A day with no games, by its date.)
const makePlayoffsKey = (game: ScheduleGameWithoutKey) =>
	isOffDay(game.homeTid, game.awayTid)
		? JSON.stringify(["off", game.date])
		: JSON.stringify([game.homeTid, game.awayTid]);

/**
 * Save the schedule to the database, overwriting what's currently there.
 *
 * @param {Array} tids A list of lists, each containing the team IDs of the home and
        away teams, respectively, for every game in the season, respectively.
 * @return {Promise}
 */
const setSchedule = async (
	tids: [number, number][],
	// The night of each game, parallel to tids, on a calendar schedule.
	dates?: (string | undefined)[],
) => {
	const playoffs = g.get("phase") === PHASE.PLAYOFFS;

	const oldPlayoffGames: Record<string, ScheduleGame> = {};
	if (playoffs) {
		// If live simming an individual playoff game, setSchedule gets called afterwards with the remaining games that day. But that means it forgets forceWin! So we need to keep track of old forceWin values. Same with gid, which messes with LeagueTopBar if it changes.
		const oldSchedule = await idb.cache.schedule.getAll();

		for (const game of oldSchedule) {
			const key = makePlayoffsKey(game);
			oldPlayoffGames[key] = game;
		}
	}

	const existingGames = await idb.cache.games.getAll();

	// In the playoffs on the calendar, the days before this night with no
	// games are days too (offDays.ts), numbered by their dates from the last
	// game played, so a day already lived is never numbered again. (The
	// playoffs' first night is set before the phase is.)
	let dayOfDate: ((date: string) => number) | undefined;
	if (
		dates !== undefined &&
		dates.length > 0 &&
		offDaysThisSeason() &&
		(playoffs ||
			(await idb.cache.playoffSeries.get(g.get("season"))) !== undefined)
	) {
		let firstLeft: string | undefined;
		for (const game of await idb.cache.schedule.getAll()) {
			if (
				game.date !== undefined &&
				(firstLeft === undefined || game.date < firstLeft)
			) {
				firstLeft = game.date;
			}
		}
		const dated = dates.filter((date): date is string => date !== undefined);
		const season = g.get("season");
		let last: { day: number; date: string } | undefined;
		for (const game of existingGames) {
			if (
				game.season === season &&
				game.date !== undefined &&
				game.day !== undefined &&
				(last === undefined || game.day > last.day)
			) {
				last = { day: game.day, date: game.date };
			}
		}
		if (dated.length === dates.length && last !== undefined) {
			const offDates = playoffOffDates({
				lastPlayed: last.date,
				firstLeft,
				firstNew: dated.toSorted()[0]!,
			});
			tids = [
				...offDates.map(() => [...OFF_DAY_TIDS] as [number, number]),
				...tids,
			];
			dates = [...offDates, ...dated];
			const anchor = last;
			dayOfDate = (date) => anchor.day + daysBetween(anchor.date, date);
		}
	}

	await idb.cache.schedule.clear();

	const finals = playoffs && (await isFinals());

	const schedule = addDaysToSchedule(
		tids.map(([homeTid, awayTid], i) => ({
			homeTid,
			awayTid,
			date: dates?.[i],
		})),
		existingGames,
	);
	for (const game of schedule) {
		if (dayOfDate && game.date !== undefined) {
			game.day = dayOfDate(game.date);
		}
		if (playoffs) {
			const key = makePlayoffsKey(game);
			if (oldPlayoffGames[key]) {
				game.day = oldPlayoffGames[key].day;
				game.gid = oldPlayoffGames[key].gid;
				game.forceWin = oldPlayoffGames[key].forceWin;
				if (oldPlayoffGames[key].date !== undefined) {
					game.date = oldPlayoffGames[key].date;
				} else {
					delete game.date;
				}
			}

			// Track if game is a finals game or not, used along with neutralSite
			if (finals) {
				game.finals = true;
			}
		}
	}

	await idb.cache.schedule.addAll(schedule);

	await recomputeLocalUITeamOvrs();
};

export default setSchedule;
