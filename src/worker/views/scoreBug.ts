import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import { pictureById, resolveScoreBug } from "../util/courtPictures.ts";
import { scoreBugPictureIds } from "../../common/scoreBug.ts";
import { defineView, type ViewInput } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";

const processInputs = (params: RouteParams<"scoreBug">) => {
	if (params.tid === undefined) {
		return { tid: undefined };
	}
	const tid =
		typeof params.tid === "string" ? Number.parseInt(params.tid) : Number.NaN;
	if (Number.isNaN(tid) || tid < 0) {
		return { redirectUrl: helpers.leagueUrl(["manage_teams"]) };
	}
	return { tid };
};

// Data for the score bug page - the league's, or (`tid`) a team's: the bug
// (null for the default - for a team, the league's), the pictures and
// fonts uploaded for it (by id), and two of the league's teams to preview it
// with - the team's own at home, or the user's.
const scoreBug = async ({ tid }: ViewInput<typeof processInputs>) => {
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const owner =
		tid === undefined ? undefined : teams.find((t) => t.tid === tid);
	if (tid !== undefined && !owner) {
		return { redirectUrl: helpers.leagueUrl(["manage_teams"]) };
	}
	const bug = (owner ? owner.scoreBug : g.get("scoreBug")) ?? null;
	const pictures: Record<string, string> = {};
	for (const id of scoreBugPictureIds(bug)) {
		try {
			const row = await pictureById(id);
			if (row) {
				pictures[id] = row.url;
			}
		} catch {
			// That piece goes without, here.
		}
	}
	const userTid = g.get("userTid");
	const home = owner ?? teams.find((t) => t.tid === userTid) ?? teams[0];
	const away = teams.find((t) => t !== home);
	const look = (t: (typeof teams)[number] | undefined) =>
		t
			? {
					abbrev: t.abbrev,
					region: t.region,
					name: t.name,
					colors: t.colors,
					imgURL: t.imgURL,
					imgURLSmall: t.imgURLSmall,
				}
			: undefined;
	return {
		tid,
		teamName: owner ? `${owner.region} ${owner.name}` : undefined,
		// What a team without its own carries: the league's (null, the
		// default).
		league: owner ? await resolveScoreBug(g.get("scoreBug")) : null,
		bug,
		pictures,
		home: look(home),
		away: look(away),
	};
};

export default defineView({
	id: "scoreBug",
	processInputs,
	load: ({ inputs }) => scoreBug(inputs),
});
