import { coarsenRatingDrop } from "../../../common/coarsenRating.ts";
import type { Player } from "../../../common/types.ts";
import fuzzRating from "./fuzzRating.ts";

export type ShownInjuryDrop = number | "-";

// One injury's ovr and pot drops as coarse ratings mode shows them: how far the
// tens digit fell, or "-" for a loss that stayed inside the decade.
//
// A rating-losing injury opened a new ratings row tagged with its index, so the
// row before that one holds the ratings going in. `fuzz` puts them on the scale
// the page's own Ovr/Pot columns use. `isExactSeason` leaves a drop exact when
// the page shows that season's ratings exact (a prospect year on his own page).
const coarsenInjuryDrops = (
	p: Pick<Player, "injuries" | "ratings">,
	injuryIndex: number,
	{
		fuzz,
		isExactSeason,
	}: {
		fuzz: boolean;
		isExactSeason?: (season: number) => boolean;
	},
): { ovrDrop?: ShownInjuryDrop; potDrop?: ShownInjuryDrop } => {
	const injury = p.injuries[injuryIndex];
	if (!injury) {
		return {};
	}

	const rowIndex = p.ratings.findIndex(
		(row) => row.injuryIndex === injuryIndex,
	);
	const before = rowIndex > 0 ? p.ratings[rowIndex - 1] : undefined;

	const shown = (drop: number | undefined, rating: "ovr" | "pot") => {
		if (drop === undefined) {
			return undefined;
		}
		if (before === undefined) {
			// No ratings to measure against, so no tens digit to report.
			return drop > 0 ? "-" : drop;
		}
		if (isExactSeason?.(before.season)) {
			return drop;
		}
		const scale = (value: number) =>
			fuzz ? fuzzRating(value, before.fuzz) : value;
		return coarsenRatingDrop(
			scale(before[rating]),
			scale(before[rating] - drop),
		);
	};

	return {
		ovrDrop: shown(injury.ovrDrop, "ovr"),
		potDrop: shown(injury.potDrop, "pot"),
	};
};

export default coarsenInjuryDrops;
