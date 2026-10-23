import { idb } from "../db/index.ts";
import { helpers } from "../util/index.ts";
import { defineView, type ViewInput } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";

const processInputs = (params: RouteParams<"editTeamStadium">) => {
	const tid =
		typeof params.tid === "string" ? Number.parseInt(params.tid) : Number.NaN;
	if (Number.isNaN(tid) || tid < 0) {
		return {
			redirectUrl: helpers.leagueUrl(["manage_teams"]),
		};
	}
	return { tid };
};

// Data for the football stadium page: the team's stadium and helmet as it
// has set them, and what the 3D game picks the rest from.
const editTeamStadium = async (inputs: ViewInput<typeof processInputs>) => {
	const t = await idb.cache.teams.get(inputs.tid);
	if (!t || t.disabled) {
		return {
			redirectUrl: helpers.leagueUrl(["manage_teams"]),
		};
	}

	return {
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		colors: t.colors,
		imgURL: t.imgURL,
		stadium: t.stadium,
		helmet: t.helmet,
	};
};

export default defineView({
	id: "editTeamStadium",
	processInputs,
	load: ({ inputs }) => editTeamStadium(inputs),
});
