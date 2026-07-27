import { idb } from "../db/index.ts";
import type { Options } from "../../common/types.ts";
import {
	DEFAULT_PHASE_CHANGE_REDIRECTS,
	DEFAULT_RECAP_MAX_GAMES,
	DEFAULT_RECAP_MAX_DAYS,
	DEFAULT_RECAP_MAX_PLAYERS,
} from "../../common/constants.ts";

export const getGlobalSettings = async () => {
	const globalSettings = ((await idb.meta.get("attributes", "options")) ??
		{}) as unknown as Options;

	globalSettings.phaseChangeRedirects ??= DEFAULT_PHASE_CHANGE_REDIRECTS;
	globalSettings.recapMaxGames ??= DEFAULT_RECAP_MAX_GAMES;
	globalSettings.recapMaxDays ??= DEFAULT_RECAP_MAX_DAYS;
	globalSettings.recapMaxPlayers ??= DEFAULT_RECAP_MAX_PLAYERS;

	return globalSettings;
};
