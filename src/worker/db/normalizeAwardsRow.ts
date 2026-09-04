import { PLAYER } from "../../common/constants.ts";
import type { Awards } from "../../common/types.ts";
import { oldAwardsToNewAwards } from "./migrations/78/oldAwardsToNewAwards.ts";

// AN AWARDS ROW FROM BEFORE THE CUSTOM-AWARDS UPGRADE, CONVERTED ON SIGHT.
//
// The upgrade rewrites every awards row in the database, so a league that goes
// through it comes out entirely in the new format - a list of awards, each with
// its own name, formula and winners. Nothing is left in the old shape, where
// each award was a fixed field on the row (`mvp`, `roy`, `allLeague`).
//
// Except that a row can arrive AFTER the upgrade, with no migration between it
// and the store. In a synced league a league-mate still running the old build
// sims a season and the changeset carries their old-format row over; a room
// snapshot taken on an old build gets restored onto an upgraded one.
//
// One such row is enough to take down every page that reads award history.
// `awards.awards` is undefined, and the season summary, the all-time list and
// the awards records all walk it - so the League History page died on a
// TypeError with nothing but a URL change to show for it.
//
// So convert on sight: on the way in from sync, and on every read, so a row
// already sitting in a database renders instead of poisoning the page. The
// conversion is the migration's own. A row too damaged even for that becomes a
// season with no awards, because a blank year in the history is worth more than
// a page that will not open.
export const normalizeAwardsRow = (row: any): Awards => {
	if (row?.awards !== undefined || row?.season === undefined) {
		return row;
	}

	try {
		return oldAwardsToNewAwards(row);
	} catch (error) {
		console.error(`Unreadable awards row for ${row.season}`, error);
		return {
			season: row.season,
			bestRecord: PLAYER.DOES_NOT_EXIST,
			bestRecordConfs: {},
			bestRecordDivs: {},
			awards: [],
		};
	}
};
