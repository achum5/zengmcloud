import { idb } from "../../db/index.ts";
import g from "../../util/g.ts";
import toUI from "../../util/toUI.ts";

// The header's count of deals held for bids (windows.ts), on every device:
// refreshed with the status line - after each sim day, on each page, and as a
// league-mate's sim lands.
export const updatePendingDeals = async () => {
	if (g.get("lid") === undefined) {
		return;
	}
	const pendingDeals = g.get("realisticFrontOffices")
		? (await idb.cache.teams.getAll()).filter((t) => t.pendingTrade).length
		: 0;
	await toUI("updateLocal", [{ pendingDeals }]);
};
