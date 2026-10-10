import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import {
	decalPictureIds,
	pictureById,
	resolveCourt,
} from "../util/courtPictures.ts";
import { defineView } from "../util/defineView.ts";

// Data for the league's court decals page: the decals, the pictures uploaded
// for them (by id), and the user's team's court to preview them on.
const courtDecals = async () => {
	const decals = g.get("courtDecals") ?? [];
	const pictures: Record<string, string> = {};
	for (const id of decalPictureIds(decals)) {
		try {
			const row = await pictureById(id);
			if (row) {
				pictures[id] = row.url;
			}
		} catch {
			// That decal goes without, here.
		}
	}
	const t = await idb.cache.teams.get(g.get("userTid"));
	return {
		decals,
		pictures,
		season: g.get("season"),
		team: t
			? {
					tid: t.tid,
					abbrev: t.abbrev,
					region: t.region,
					name: t.name,
					colors: t.colors,
					imgURL: t.imgURL,
					court: await resolveCourt(t.court),
				}
			: undefined,
	};
};

export default defineView({
	id: "courtDecals",
	load: () => courtDecals(),
});
