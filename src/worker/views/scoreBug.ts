import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { pictureById } from "../util/courtPictures.ts";
import { scoreBugPictureIds } from "../../common/scoreBug.ts";
import { defineView } from "../util/defineView.ts";

// Data for the league's score bug page: the bug (null for the default), the
// pictures uploaded for it (by id), and two of the league's teams to preview
// it with - the user's at home.
const scoreBug = async () => {
	const bug = g.get("scoreBug") ?? null;
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
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const userTid = g.get("userTid");
	const home = teams.find((t) => t.tid === userTid) ?? teams[0];
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
	return { bug, pictures, home: look(home), away: look(away) };
};

export default defineView({
	id: "scoreBug",
	load: () => scoreBug(),
});
