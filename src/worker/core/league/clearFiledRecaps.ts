// Remove every FILED game and day recap in the league, so the automatic ones
// show everywhere again.
//
// Auto recaps are generated fresh on each view and never stored (see
// getDayGamesForRecap), so they always reflect the current generator - there is
// nothing to "regenerate". What can be stale is a recap somebody FILED: the AI
// and manual recap flows write Game.note (a box score's recap) and
// Game.dayNote (a whole day's), and a filed recap always wins over the
// automatic one. Clearing them is what actually puts every box score back on
// the current system.
//
// Irreversible: filed recaps are the user's own text and are not backed up
// anywhere, which is why this lives in the Danger Zone behind a confirmation.

import { idb } from "../../db/index.ts";

export type ClearedRecapCounts = {
	games: number;
	days: number;
};

export const clearFiledRecaps = async (): Promise<ClearedRecapCounts> => {
	// A cursor, not getCopies(): a long league holds tens of thousands of full
	// box scores and this only needs one row in memory at a time. There is an
	// index on noteBool but none on dayNoteBool, so the sweep has to see every
	// game anyway.
	//
	// Written straight to the database rather than through idb.cache, so flush
	// first (or pending cache writes would land on top of the clean rows
	// afterwards) and refill after (or the in-memory copies of this season's
	// games would still carry the notes and write them back on the next flush).
	await idb.cache.flush();

	const counts: ClearedRecapCounts = { games: 0, days: 0 };

	const transaction = idb.league.transaction("games", "readwrite");
	for await (const cursor of transaction.store) {
		const game = cursor.value;
		let changed = false;

		if (game.note !== undefined || game.noteBool !== undefined) {
			delete game.note;
			delete game.noteBool;
			counts.games += 1;
			changed = true;
		}
		if (game.dayNote !== undefined || game.dayNoteBool !== undefined) {
			delete game.dayNote;
			delete game.dayNoteBool;
			counts.days += 1;
			changed = true;
		}

		if (changed) {
			await cursor.update(game);
		}
	}
	await transaction.done;

	await idb.cache.fill();

	return counts;
};
