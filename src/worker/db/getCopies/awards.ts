import { idb } from "../index.ts";
import type { Awards, GetCopyType } from "../../../common/types.ts";
import { mergeByPk } from "./helpers.ts";
import { normalizeAwardsRow } from "../normalizeAwardsRow.ts";

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
		return awards.map(normalizeAwardsRow);
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
			.map(normalizeAwardsRow)
	);
};

export default getCopies;
