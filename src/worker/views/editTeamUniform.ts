import { idb } from "../db/index.ts";
import { helpers } from "../util/index.ts";
import type { ViewInput } from "../../common/types.ts";

// Data for the uniform editor page: the team's identity, colors and current
// jersey string (a preset id, or a custom uniform spec encoded behind a
// prefix - the page tells them apart).
const editTeamUniform = async (inputs: ViewInput<"editTeamUniform">) => {
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
		jersey: t.jersey,
	};
};

export default editTeamUniform;
