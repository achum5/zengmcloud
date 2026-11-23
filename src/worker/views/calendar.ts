import { PHASE } from "../../common/constants.ts";
import { isAllStarGame, isTradeDeadline } from "../../common/specialGames.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { season as seasonCore } from "../core/index.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { defineView } from "../util/defineView.ts";
import { validateSeason, validateTeam } from "../util/processInputs.ts";

// THE CALENDAR PAGE: a season's nights, month by month, for a team or the
// whole league. Every game of the season goes to the UI, which picks out the
// team's.

const processInputs = (params: RouteParams<"calendar">) => ({
	season: validateSeason(params.season),
	tid:
		params.team === undefined
			? g.get("userTid")
			: validateTeam(params.team, ["all"]),
});

export type CalendarGame = {
	gid: number;
	date: string;
	// For the Daily Schedule link.
	day: number | undefined;
	homeTid: number;
	awayTid: number;
	playoffs: boolean;
	// The final, once it's played.
	homePts?: number;
	awayPts?: number;
	overtimes?: number;
};

export type CalendarKeyDate = {
	date: string;
	day: number | undefined;
	type: "allStar" | "tradeDeadline";
	// The All-Star Game's box score, once it's played.
	gid?: number;
};

export default defineView({
	id: "calendar",
	processInputs,
	load: async ({ inputs, updateEvents, prevInputs }) => {
		const currentSeason = g.get("season");
		if (
			!updateEvents.has("firstRun") &&
			!updateEvents.has("gameAttributes") &&
			!updateEvents.has("newPhase") &&
			!(inputs.season === currentSeason && updateEvents.has("gameSim")) &&
			inputs.season === prevInputs?.season &&
			inputs.tid === prevInputs.tid
		) {
			return;
		}

		const games: CalendarGame[] = [];
		const keyDates: CalendarKeyDate[] = [];

		// Games on classic numbered days, before the league went on the
		// calendar.
		let undated = false;

		const played = await idb.merged.games.getAll({ season: inputs.season });
		for (const game of played) {
			if (game.date === undefined) {
				undated = true;
				continue;
			}
			const [home, away] = game.teams;
			if (isAllStarGame(home.tid, away.tid)) {
				keyDates.push({
					date: game.date,
					day: game.day,
					type: "allStar",
					gid: game.gid,
				});
				continue;
			}
			games.push({
				gid: game.gid,
				date: game.date,
				day: game.day,
				homeTid: home.tid,
				awayTid: away.tid,
				playoffs: !!game.playoffs,
				homePts: home.pts,
				awayPts: away.pts,
				overtimes: game.overtimes,
			});
		}

		let today: string | undefined;
		if (inputs.season === currentSeason) {
			today = await seasonCore.getCalendarDate();
			const playoffs = g.get("phase") === PHASE.PLAYOFFS;
			for (const game of await seasonCore.getSchedule()) {
				if (game.date === undefined) {
					undated = true;
					continue;
				}
				if (isTradeDeadline(game.homeTid, game.awayTid)) {
					keyDates.push({
						date: game.date,
						day: game.day,
						type: "tradeDeadline",
					});
				} else if (isAllStarGame(game.homeTid, game.awayTid)) {
					keyDates.push({
						date: game.date,
						day: game.day,
						type: "allStar",
					});
				} else {
					games.push({
						gid: game.gid,
						date: game.date,
						day: game.day,
						homeTid: game.homeTid,
						awayTid: game.awayTid,
						playoffs,
					});
				}
			}
		}

		games.sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));

		return {
			games,
			keyDates,
			nbaCalendar: g.get("nbaCalendar"),
			season: inputs.season,
			tid: inputs.tid,
			today,
			undated,
		};
	},
});
