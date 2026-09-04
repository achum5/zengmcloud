import { g } from "../util/index.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import { getAwardCandidates } from "../core/awards/getAwardCandidates.ts";
import { groupByUnique } from "../../common/utils.ts";
import getAwardRaceOdds, { raceKey } from "../core/season/getAwardRaceOdds.ts";

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
			await getAwardCandidates(inputs.season)
		).awardCandidates.flat();

		// Live odds come from the shared award-race model (getAwardRaceOdds), the
		// exact same source the Sportsbook prices its award futures from, so the
		// two pages always agree. Past seasons have no race to price.
		if (inputs.season === g.get("season")) {
			try {
				const odds = new Map<string, Map<number, number>>();
				for (const race of await getAwardRaceOdds(inputs.season)) {
					odds.set(
						raceKey(race),
						new Map(race.players.map((p) => [p.pid, p.odds])),
					);
				}
				for (const award of awardCandidates) {
					if (award.numTeams !== undefined) {
						continue;
					}
					const byPid = odds.get(raceKey(award));
					for (const p of award.players) {
						p.odds = byPid?.get(p.pid);
					}
				}
			} catch (error) {
				console.error("Award race odds unavailable", error);
			}
		}

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
			confs: g.get("confs", inputs.season),
			divs: g.get("divs", inputs.season),
			season: inputs.season,
			teams: groupByUnique(teams, "tid"),
		};
	}
};

export default updateAwardRaces;
