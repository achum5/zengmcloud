import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { defineView } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import {
	validateAbbrev,
	validateSeason,
	validateSeasonType,
} from "../util/processInputs.ts";
import { seasonLineups } from "../util/gameTimeline.ts";

// A team's fives over a season, from the lineups each of its games kept (see
// util/gameTimeline.ts). Games from before the sim logged time have no
// minutes; games whose box scores are gone take their lineups with them.

const processInputs = (params: RouteParams<"lineups">) => {
	const [tid, abbrev] = validateAbbrev(params.abbrev);
	return {
		abbrev,
		tid,
		season: validateSeason(params.season),
		playoffs: validateSeasonType(params.playoffs),
	};
};

export default defineView({
	id: "lineups",
	processInputs,
	load: async ({ inputs, updateEvents, prevInputs }) => {
		const { abbrev, tid, season, playoffs } = inputs;
		if (
			!(season === g.get("season") && updateEvents.has("gameSim")) &&
			season === prevInputs?.season &&
			tid === prevInputs?.tid &&
			playoffs === prevInputs?.playoffs &&
			!updateEvents.has("firstRun")
		) {
			return;
		}

		const games = await idb.merged.games.getAll(
			{ season },
			{
				filter: (game) =>
					(game.teams[0].tid === tid || game.teams[1].tid === tid) &&
					(playoffs === "combined" ||
						(playoffs === "playoffs") === !!game.playoffs),
			},
		);

		const { names, lineups } = seasonLineups(games as any, tid);

		return {
			abbrev,
			tid,
			season,
			playoffs,
			numGames: games.length,
			names,
			lineups,
		};
	},
});
