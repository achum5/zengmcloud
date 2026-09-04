// A LEAGUE THAT WAS RENAMED BEFORE ANY OF THIS EXISTED.
//
// Renaming an award relabels the seasons already played (see renameAwards),
// but only from the moment of the rename onward: it works off the difference
// between the old settings and the new ones. A league that changed All-League
// to All-NBA before that shipped has no difference left to find. Both settings
// say All-NBA and every season still says All-League, and nothing the user can
// do in the settings will ever produce a diff that fixes it.
//
// So the labels are also checked against the settings on league load, which is
// the one moment that needs no history at all. The check is one small record
// per season and finds nothing in a league that was never renamed; the sweep
// that repairs it is only paid the once.

import { idb } from "../../db/index.ts";
import { g, logEvent } from "../../util/index.ts";
import { normalizeAwardsRow } from "../../db/normalizeAwardsRow.ts";
import {
	applyAwardRenames,
	awardLabelsOutOfDate,
	awardRenamesFromSettings,
} from "./renameAwards.ts";

export const repairAwardLabels = async () => {
	const settings = g.get("awards");
	if (!settings || settings.length === 0) {
		return;
	}

	// Straight from the database, because this has to see every season and the
	// cache only holds the current one.
	const rows = (await idb.league.getAll("awards")).map((row) =>
		normalizeAwardsRow(row),
	);

	if (!awardLabelsOutOfDate(rows, settings)) {
		return;
	}

	const result = await applyAwardRenames(awardRenamesFromSettings(settings));

	if (result.seasons > 0) {
		// Say it happened. A league's whole history silently changing what its
		// awards are called is exactly the kind of thing that should not arrive
		// unannounced.
		logEvent({
			type: "info",
			text: `Updated award names in ${result.seasons} past season${
				result.seasons === 1 ? "" : "s"
			} to match your award settings.`,
			saveToDb: false,
		});
	}

	return result;
};
