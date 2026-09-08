import { idb } from "../index.ts";
import type { Awards, GetCopyType } from "../../../common/types.ts";
import { mergeByPk } from "./helpers.ts";
import { normalizeAwardsRow } from "../normalizeAwardsRow.ts";
import { relabelAwardsFromSettings } from "../../../common/awards.ts";
import { g } from "../../util/index.ts";

// A season keeps the label its awards were given at the time; the settings say
// what they are called now. See relabelAwardsFromSettings.
const relabel = (row: Awards): Awards => {
	const awards = relabelAwardsFromSettings(row?.awards, g.get("awards"));
	return awards === row?.awards ? row : { ...row, awards };
};

const getCopies = async (
	{
		season,
	}: {
		season?: number;
	} = {},
	type?: GetCopyType,
): Promise<Awards[]> => {
	if (season !== undefined) {
		const awards = mergeByPk(
			await idb.league.getAll("awards", season),
			(await idb.cache.awards.getAll()).filter((event) => {
				return event.season === season;
			}),
			"awards",
			type,
		);
		return awards.map((row) => relabel(normalizeAwardsRow(row)));
	}

	return (
		mergeByPk(
			await idb.league.getAll("awards"),
			await idb.cache.awards.getAll(),
			"awards",
			type,
		)
			// A row from before the custom-awards upgrade can still be sitting in the
			// store; anything reading award history walks `awards.awards` and would
			// die on it. See normalizeAwardsRow.
			.map((row) => relabel(normalizeAwardsRow(row)))
	);
};

export default getCopies;
