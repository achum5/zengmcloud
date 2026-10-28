import { idb } from "../../db/index.ts";
import g from "../../util/g.ts";
import toUI from "../../util/toUI.ts";
import { proposalsTo } from "./proposalStore.ts";

// The header's count of deals held for bids (windows.ts) and of CPU teams'
// trade proposals to this device's team, on every device: refreshed with the
// status line - after each sim day, on each page, and as a league-mate's sim
// lands.
export const updatePendingDeals = async () => {
	if (g.get("lid") === undefined) {
		return;
	}
	const on = g.get("realisticFrontOffices");
	const pendingDeals = on
		? (await idb.cache.teams.getAll()).filter((t) => t.pendingTrade).length
		: 0;
	const tradeProposals = on ? (await proposalsTo(g.get("userTid"))).length : 0;
	await toUI("updateLocal", [{ pendingDeals, tradeProposals }]);
};
