import { idb } from "../db/index.ts";
import { helpers } from "../../common/helpers.ts";
import { courtOf } from "../../common/stockCourts.ts";
import type { HeadlineSceneSpec } from "../../common/headlineScene.ts";
import { takeArenaLooks } from "../core/game/replayLooks.ts";
import { resolveCourt } from "./courtPictures.ts";

// WHAT A HEADLINE'S SCENE IS MADE FROM (see HeadlineScene.tsx): for a draft
// pick, the team that took him as it looked that year - its colors, logo
// and floor, its building - and where he went. Nothing, if there is no scene
// to make of it (he went undrafted, his team is gone).
export const headlineScene = async (spec: HeadlineSceneSpec) => {
	if (__SPORT !== "basketball" || spec.type !== "draftPick") {
		return undefined;
	}
	const p = await idb.merged.players.get(spec.pid);
	// (A player whose face is a photo is a silhouette on the court: no
	// scene of him.)
	if (!p || p.draft.round < 1 || p.draft.tid < 0 || p.imgURL) {
		return undefined;
	}
	const t = await idb.cache.teams.get(p.draft.tid);
	if (!t) {
		return undefined;
	}
	const season = p.draft.year;
	const then = (await idb.merged.teamSeasons.getAll({ tid: t.tid })).find(
		(ts) => ts.season === season,
	);
	const ratings = p.ratings.find((r) => r.season === season) ?? p.ratings[0];
	return {
		type: "draftPick" as const,
		season,
		round: p.draft.round,
		pick: p.draft.pick,
		arena: await takeArenaLooks(t.tid, season),
		team: {
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
		},
		player: {
			pid: p.pid,
			name: `${p.firstName} ${p.lastName}`,
			pos: ratings?.pos,
			jerseyNumber: helpers.getJerseyNumber(p),
		},
	};
};
