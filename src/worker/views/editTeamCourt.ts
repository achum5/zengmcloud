import { idb } from "../db/index.ts";
import { helpers } from "../util/index.ts";
import { courtPictures } from "../util/courtPictures.ts";
import { defineView, type ViewInput } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";

const processInputs = (params: RouteParams<"editTeamCourt">) => {
	const tid =
		typeof params.tid === "string" ? Number.parseInt(params.tid) : Number.NaN;
	if (Number.isNaN(tid) || tid < 0) {
		return {
			redirectUrl: helpers.leagueUrl(["manage_teams"]),
		};
	}
	return { tid };
};

// Data for the court editor page: the team's current court style plus the
// defaults (colors + logo) the renderer falls back to when a field is unset.
const editTeamCourt = async (inputs: ViewInput<typeof processInputs>) => {
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
		// The pictures uploaded for it, by id (see courtPictures.ts).
		pictures: await courtPictures(t.court),
	};
};

export default defineView({
	id: "editTeamCourt",
	processInputs,
	load: ({ inputs }) => editTeamCourt(inputs),
});
