import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getLines } from "./getLines.ts";

// WHAT THE BOOK SAID BEFORE THE SEASON. Every team's title price and win
// total, written onto its team season as the regular season begins, so the
// team page can show them for any year the way Basketball Reference does.
// The futures board itself is priced live and moves all season, so without
// this the preseason lines are gone the moment a game is played.
//
// Called once the schedule is set and before the first game - the same board
// the sportsbook was showing on the last day of the preseason.
export const savePreseasonOdds = async () => {
	if (__SPORT !== "basketball" || g.get("college")) {
		return;
	}

	const season = g.get("season");
	const { championship, winTotals } = await getLines();
	const title = new Map(championship.map((row) => [row.tid, row.americanOdds]));
	const winTotal = new Map(winTotals.map((row) => [row.tid, row.line]));

	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	);
	for (const teamSeason of teamSeasons) {
		const odds = {
			title: title.get(teamSeason.tid),
			winTotal: winTotal.get(teamSeason.tid),
		};
		if (odds.title === undefined && odds.winTotal === undefined) {
			continue;
		}
		teamSeason.preseasonOdds = odds;
		await idb.cache.teamSeasons.put(teamSeason);
	}
};
