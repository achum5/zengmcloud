import { idb } from "../db/index.ts";
import { courtOf } from "../../common/stockCourts.ts";
import { resolveCourt } from "./courtPictures.ts";

// A team as it looked in a season: its colors, logo and floor.
export const teamThen = async (tid: number, season: number) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		return undefined;
	}
	const then = (await idb.merged.teamSeasons.getAll({ tid: t.tid })).find(
		(ts) => ts.season === season,
	);
	return {
		tid: t.tid,
		abbrev: then?.abbrev ?? t.abbrev,
		region: then?.region ?? t.region,
		name: then?.name ?? t.name,
		colors: then?.colors ?? t.colors,
		jersey: then?.jersey ?? t.jersey,
		imgURL: then?.imgURL ?? t.imgURL,
		imgURLSmall: then?.imgURLSmall ?? t.imgURLSmall,
		jerseySkins: t.jerseySkins,
		// (Its own court, or its stock floor - see stockCourts - with its
		// uploaded pictures filled in, to draw.)
		court: await resolveCourt(courtOf(t)),
	};
};
