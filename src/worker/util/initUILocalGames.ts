import g from "./g.ts";
import { getProcessedGames } from "./getProcessedGames.ts";
import { toUI } from "./toUI.ts";
import type { LocalStateUI } from "../../common/types.ts";
import { getUpcomingGamesForUI } from "./recomputeLocalUITeamOvrs.ts";
import { updateTickerItems } from "./updateTickerItems.ts";
import { hideLiveResults } from "./liveSpoilers.ts";
import { getLiveGate } from "./liveGameGate.ts";
import { nationalTagsPlayed } from "../views/nationalBroadcast.ts";

export const initUILocalGames = async ({
	freshTicker = false,
}: {
	// The ticker's slate and award races read again rather than from their
	// memo (see updateTickerItems).
	freshTicker?: boolean;
} = {}) => {
	const userTid = g.get("userTid");

	// Start with completed games
	const games: LocalStateUI["games"] = (
		await getProcessedGames({
			tid: userTid,
			season: g.get("season"),
			includeAllStarGame: true,
		})
	).map((game) => ({
		date: game.date,
		forceWin: game.forceWin,
		gid: game.gid,
		overtimes: game.overtimes,
		numPeriods: game.numPeriods,
		teams: [
			{
				ovr: game.teams[0].ovr,
				pts: game.teams[0].pts,
				sPts: game.teams[0].sPts,
				tid: game.teams[0].tid,
				playoffs: game.teams[0].playoffs,
			},
			{
				ovr: game.teams[1].ovr,
				pts: game.teams[1].pts,
				sPts: game.teams[1].sPts,
				tid: game.teams[1].tid,
				playoffs: game.teams[1].playoffs,
			},
		],
	}));
	games.reverse();

	// On the NBA calendar, the network each was on, for its day on the bar.
	if (games.some((game) => game.date !== undefined)) {
		const national = await nationalTagsPlayed(games.map((game) => game.gid));
		for (const game of games) {
			const tag = national.get(game.gid);
			if (tag) {
				game.national = tag;
			}
		}
	}

	// Add the next game - and on the NBA calendar, the coming nights' games
	games.push(...(await getUpcomingGamesForUI()));

	await toUI("updateLocal", [
		{
			// (A game still playing out live reads as not played yet - see
			// liveSpoilers.ts.)
			games: hideLiveResults(games, (await getLiveGate()).hidden),
		},
	]);

	// The bottom ticker rides the score bar's feed deliberately. Every
	// path that refreshes scores - a league load, a view change, and crucially
	// the sync layer's post-apply refresh - then refreshes the ticker too, which
	// means the ticker inherits the spoiler hold that path already has: while a
	// live game is being watched, refreshAfterApply is banked and neither feed
	// moves. Feeding it from its own hook would have meant getting that right a
	// second time.
	await updateTickerItems({ fresh: freshTicker });
};
