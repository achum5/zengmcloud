import { g } from "../util/index.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";
import getAwardRaceOdds from "../core/season/getAwardRaceOdds.ts";

const updateAwardRaces = async (
	inputs: ViewInput<"awardRaces">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		updateEvents.includes("firstRun") ||
		(inputs.season === g.get("season") &&
			(updateEvents.includes("gameSim") ||
				updateEvents.includes("playerMovement"))) ||
		inputs.season !== state.season
	) {
		// Live odds come from the shared award-race model (getAwardRaceOdds), the
		// exact same source the Sportsbook prices its award futures from, so the two
		// pages always agree.
		const awardCandidates = (await getAwardRaceOdds(inputs.season)).map(
			(row) => ({
				...row,
				players: addFirstNameShort(row.players),
			}),
		);

		const teams = await idb.getCopies.teamsPlus(
			{
				attrs: ["tid"],
				seasonAttrs: ["won", "lost", "tied", "otl"],
				season: inputs.season,
			},
			"noCopyCache",
		);

		return {
			awardCandidates,
			season: inputs.season,
			teams,
		};
	}
};

export default updateAwardRaces;
