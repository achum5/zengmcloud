import { g } from "../util/index.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";
import { season } from "../core/index.ts";
import { idb } from "../db/index.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";
import { strengthProbs } from "../../common/sportsbookOdds.ts";
import { probToAmerican } from "../../common/sportsbook.ts";

// How sharply award odds follow the formula's score gaps (matches the
// Sportsbook's award futures).
const AWARD_POWER = 0.9;

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
		const awardCandidates = (
			await season.getAwardCandidates(inputs.season)
		).map((row) => {
			// Sportsbook-style live odds per candidate, priced off the actual award
			// formula's scores (same model the Sportsbook page uses).
			const probs = strengthProbs(
				row.players.map((p: any) =>
					typeof p.awardScore === "number" ? p.awardScore : 0,
				),
				AWARD_POWER,
			);
			const players = addFirstNameShort(row.players).map(
				(p: any, i: number) => ({
					...p,
					odds: probToAmerican(probs[i] ?? 0),
				}),
			);
			return { ...row, players };
		});

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
