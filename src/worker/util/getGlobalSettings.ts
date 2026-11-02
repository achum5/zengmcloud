import {
	DEFAULT_PHASE_CHANGE_REDIRECTS,
	DEFAULT_RECAP_MAX_GAMES,
	DEFAULT_RECAP_MAX_DAYS,
	DEFAULT_RECAP_MAX_PLAYERS,
} from "../../common/constants.ts";
import { DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS } from "../../common/ownGameSim.ts";
import { DEFAULT_ACHIEVEMENT_DRAFT_PICKS } from "../../common/achievementCards.ts";
import { getMetaAttribute } from "../db/metaAttributes.ts";

export const getGlobalSettings = async () => {
	const globalSettings = (await getMetaAttribute("options")) ?? {};

	return {
		...globalSettings,
		phaseChangeRedirects:
			globalSettings.phaseChangeRedirects ?? DEFAULT_PHASE_CHANGE_REDIRECTS,
		recapMaxGames: globalSettings.recapMaxGames ?? DEFAULT_RECAP_MAX_GAMES,
		recapMaxDays: globalSettings.recapMaxDays ?? DEFAULT_RECAP_MAX_DAYS,
		recapMaxPlayers:
			globalSettings.recapMaxPlayers ?? DEFAULT_RECAP_MAX_PLAYERS,
		ownGameSimCutoffSeconds:
			globalSettings.ownGameSimCutoffSeconds ??
			DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS,
		achievementCardsDraftPicks:
			globalSettings.achievementCardsDraftPicks ??
			DEFAULT_ACHIEVEMENT_DRAFT_PICKS,
	};
};
