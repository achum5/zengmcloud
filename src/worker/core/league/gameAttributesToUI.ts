import { toUI } from "../../util/index.ts";
import type {
	GameAttributesLeagueWithHistory,
	GameAttributesLeague,
	LocalStateUI,
} from "../../../common/types.ts";
import { unwrapGameAttribute } from "../../../common/unwrapGameAttribute.ts";
import { gameAttributesSyncedToUi } from "../../../common/gameAttributesSyncedToUi.ts";
import { stageSettings } from "../realisticFrontOffice/stages.ts";

const gameAttributesToUI = async (
	gameAttributes: GameAttributesLeagueWithHistory,
) => {
	const update: Partial<GameAttributesLeague> = {};
	let updated = false;
	for (const key of gameAttributesSyncedToUi) {
		if (Object.hasOwn(gameAttributes, key)) {
			(update as any)[key] = unwrapGameAttribute(gameAttributes, key);
			updated = true;
		}
	}

	let flagOverrides: LocalStateUI["flagOverrides"] | undefined;
	// Check the key rather than the value, so removing playerBioInfo also clears flagOverrides
	if (Object.hasOwn(gameAttributes, "playerBioInfo")) {
		flagOverrides = {};
		updated = true;
		const countries = gameAttributes.playerBioInfo?.countries;
		if (countries) {
			for (const [country, { flag }] of Object.entries(countries)) {
				if (flag !== undefined) {
					flagOverrides[country] = flag;
				}
			}
		}
	}

	if (updated) {
		await toUI("setGameAttributes", [update, flagOverrides]);
	}

	// Realistic front offices' contract rules (stagesOn), for the menu.
	if (Object.hasOwn(gameAttributes, "realisticFrontOffices")) {
		await toUI("updateLocal", [
			{
				contractRules:
					__SPORT === "basketball" &&
					!!unwrapGameAttribute(gameAttributes, "realisticFrontOffices") &&
					stageSettings.on,
			},
		]);
	}
};

export default gameAttributesToUI;
