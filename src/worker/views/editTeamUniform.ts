import { idb } from "../db/index.ts";
import { helpers } from "../util/index.ts";
import { defineView, type ViewInput } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";

const processInputs = (params: RouteParams<"editTeamUniform">) => {
	const tid =
		typeof params.tid === "string" ? Number.parseInt(params.tid) : Number.NaN;
	if (Number.isNaN(tid) || tid < 0) {
		return {
			redirectUrl: helpers.leagueUrl(["manage_teams"]),
		};
	}
	return { tid };
};

// Data for the uniform editor page: the team's identity, colors and current
// jersey string (a preset id, or a custom uniform spec encoded behind a
// prefix - the page tells them apart).
const editTeamUniform = async (inputs: ViewInput<typeof processInputs>) => {
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

export default defineView({
	id: "editTeamUniform",
	processInputs,
	load: ({ inputs }) => editTeamUniform(inputs),
});
