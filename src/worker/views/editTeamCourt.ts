import { idb } from "../db/index.ts";
import { helpers } from "../util/index.ts";
import type { ViewInput } from "../../common/types.ts";

// Data for the court editor page: the team's current court style plus the
// defaults (colors + logo) the renderer falls back to when a field is unset.
const editTeamCourt = async (inputs: ViewInput<"editTeamCourt">) => {
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
		court: t.court,
	};
};

export default editTeamCourt;
