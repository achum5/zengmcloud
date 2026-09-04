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
	awardRenamesFromSettings,
	type AwardRename,
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
	// Two things at once, and they cover different gaps. The diff is the only
	// thing that can follow an award whose ABBREV changed, since the seasons
	// carry the old one. The settings themselves catch everything else,
	// including a rename made before any of this existed - by then there is no
	// "before" left for a diff to find.
	const renames = new Map<string, AwardRename>();
	for (const rename of awardRenamesFromSettings(awards)) {
		renames.set(rename.fromShortName, rename);
	}
	for (const rename of awardRenames(g.get("awards"), awards)) {
		renames.set(rename.fromShortName, rename);
	}

	await league.setGameAttributes({ awards });

	const renamed = await applyAwardRenames([...renames.values()]);

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
