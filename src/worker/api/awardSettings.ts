import { defaultGameAttributes } from "../../common/defaultGameAttributes.ts";
import type { GameAttributesLeague } from "../../common/types.ts";
import { getAwardCandidates as getAwardCandidatesRaw } from "../core/awards/getAwardCandidates.ts";
import {
	AWARD_STATS_ALL,
	PLAYOFF_SERIES_AWARD_STATS_ALL,
} from "../core/awards/getPlayers.ts";
import g from "../util/g.ts";
import { league } from "../core/index.ts";
import { toUI } from "../util/index.ts";
import {
	applyAwardRenames,
	awardRenames,
} from "../core/awards/renameAwards.ts";

export const getAwardCandidates = async (
	info:
		| {
				type: "season";
				season: number;
		  }
		| {
				type: "default";
				season: number;
		  }
		| {
				type: "custom";
				season: number;
				awards: GameAttributesLeague["awards"];
		  },
) => {
	const season = info.season;
	const awards =
		info.type === "season"
			? g.get("awards")
			: info.type === "default"
				? defaultGameAttributes.awards
				: info.awards;
	return await getAwardCandidatesRaw(season, awards);
};

// Saving the award settings, with any rename carried back through the seasons
// already played (see core/awards/renameAwards).
export const save = async (awards: GameAttributesLeague["awards"]) => {
	const renames = awardRenames(g.get("awards"), awards);

	await league.setGameAttributes({ awards });

	const renamed = await applyAwardRenames(renames);

	await toUI("realtimeUpdate", [["gameAttributes"]]);

	return renamed;
};

export const getVariables = () => {
	const normalSet = new Set(AWARD_STATS_ALL);
	const playoffSeriesSet = new Set(PLAYOFF_SERIES_AWARD_STATS_ALL);

	const common = Array.from(normalSet.intersection(playoffSeriesSet));
	const normalOnly = Array.from(normalSet.difference(playoffSeriesSet));
	const playoffSeriesOnly = Array.from(playoffSeriesSet.difference(normalSet));

	return { common, normalOnly, playoffSeriesOnly };
};
