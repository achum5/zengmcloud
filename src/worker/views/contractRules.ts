import { defineView } from "../util/defineView.ts";
import { g } from "../util/index.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { validateSeason, validateTeam } from "../util/processInputs.ts";
import {
	contractRules as rulesData,
	leagueCapTracker,
	signingsLog,
} from "../core/realisticFrontOffice/capSheet.ts";
import { stagesOn } from "../core/realisticFrontOffice/stages.ts";

// The Contract Rules page (realistic front offices' CBA-lite): the rules as
// this league's numbers, every team's cap status, and the season's signings
// with how each was made.
const TABS = ["rules", "teams", "signings"] as const;

const processInputs = (params: RouteParams<"contractRules">) => {
	const tab = TABS.find((x) => x === params.tab) ?? "rules";
	const team = validateTeam(params.team, ["all"]);
	const season = params.season
		? validateSeason(params.season)
		: g.get("season");
	return { tab, team, season };
};

export default defineView({
	id: "contractRules",
	processInputs,
	load: async ({ inputs }) => {
		if (!stagesOn()) {
			return {
				errorMessage: "Contract rules are not on in this league.",
			};
		}
		const userTid = g.get("userTid");
		return {
			tab: inputs.tab,
			team: inputs.team,
			season: inputs.season,
			userTid,
			rules: rulesData(),
			teams: inputs.tab === "teams" ? await leagueCapTracker() : [],
			signings:
				inputs.tab === "signings"
					? await signingsLog(
							inputs.season,
							inputs.team === "all" ? undefined : inputs.team,
						)
					: [],
		};
	},
});
