import { idb } from "../../db/index.ts";

// HOW LONG A HELD DEAL STAYS OPEN (owner's call): not by real time, nor by
// sim days - a sim of a week passes as many days as one of a day - but by
// sims, so whoever is simming, however far, everyone gets to answer. A deal
// closes as the third sim since it was held or last bid on starts: two full
// sims between for anyone to bid. It stays open while games are played; only
// its assets are off the table.
export const SIMS_TO_CLOSE = 3;
// With one person in the league there is no one else to wait for: a deal
// also closes after this many trading days, so one long sim does not keep its
// players off the table for months.
export const DAYS_ALONE = 14;

// A sim of days, or a phase advanced on its own, has started.
export const pendingDealsSimStarted = async () => {
	for (const t of await idb.cache.teams.getAll()) {
		if (t.pendingTrade) {
			t.pendingTrade.sims = (t.pendingTrade.sims ?? 0) + 1;
			await idb.cache.teams.put(t);
		}
	}
};
