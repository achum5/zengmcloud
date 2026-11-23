import { addDays } from "../../common/calendarDates.ts";
import type { LocalStateUI } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import { getUpcoming } from "../views/schedule.ts";
import { nationalTags } from "../views/nationalBroadcast.ts";
import g from "./g.ts";
import toUI from "./toUI.ts";
import { getLiveGate } from "./liveGameGate.ts";

// How many nights past today the top bar shows on the NBA calendar.
const TOP_BAR_NIGHTS_AHEAD = 5;

export const getOneUpcomingGame = async (): Promise<
	LocalStateUI["games"][number] | undefined
> => {
	const game = (
		await getUpcoming({
			tid: g.get("userTid"),
			onlyOneGame: true,
		})
	)[0];
	// (Not the next playoff day's while a game of today's is still being
	// played live - see liveGameGate.ts.)
	const dayCutoff = game ? (await getLiveGate()).dayCutoff : undefined;
	if (game && (dayCutoff === undefined || game.day <= dayCutoff)) {
		return {
			date: game.date,
			finals: game.finals,
			gid: game.gid,
			national: game.national,
			// Worked out in the worker off fresh overalls, so the top bar can't
			// quote a different number than the schedule pages for the same game.
			spread: game.spread,
			teams: [
				{
					ovr: game.teams[0].ovr,
					tid: game.teams[0].tid,
					playoffs: game.teams[0].playoffs,
				},
				{
					ovr: game.teams[1].ovr,
					tid: game.teams[1].tid,
					playoffs: game.teams[1].playoffs,
				},
			],
		};
	}
};

// The user's next game, and on the NBA calendar the rest of their games in
// the top bar's coming nights. Only the next one gets overalls and a spread.
export const getUpcomingGamesForUI = async (): Promise<
	LocalStateUI["games"]
> => {
	const next = await getOneUpcomingGame();
	if (!next) {
		return [];
	}
	if (next.date === undefined) {
		return [next];
	}

	const userTid = g.get("userTid");
	const schedule = await idb.cache.schedule.getAll();
	let today = next.date;
	for (const game of schedule) {
		if (game.date !== undefined && game.date < today) {
			today = game.date;
		}
	}
	const last = addDays(today, TOP_BAR_NIGHTS_AHEAD);
	const national = await nationalTags(schedule);
	const later = schedule
		.filter(
			(game) =>
				game.gid !== next.gid &&
				(game.homeTid === userTid || game.awayTid === userTid) &&
				game.date !== undefined &&
				game.date <= last,
		)
		.sort((a, b) => (a.date! < b.date! ? -1 : a.date! > b.date! ? 1 : 0))
		.map((game) => ({
			date: game.date,
			gid: game.gid,
			national: national.get(game.gid),
			teams: [{ tid: game.homeTid }, { tid: game.awayTid }] as [
				{ tid: number },
				{ tid: number },
			],
		}));
	return [next, ...later];
};

export const recomputeLocalUITeamOvrs = async () => {
	// Only the user's upcoming games need recomputing, no other ones are shown in UI
	const upcomingGames = await getUpcomingGamesForUI();
	if (upcomingGames.length > 0) {
		await toUI("mergeGames", [upcomingGames]);
	}
};
