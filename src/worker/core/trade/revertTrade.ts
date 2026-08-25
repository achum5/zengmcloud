import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import type { EventBBGM } from "../../../common/types.ts";
import processTrade from "./processTrade.ts";

type TradeEvent = Extract<EventBBGM, { type: "trade" }>;

// A trade can only be taken back while everything it moved is still where it
// landed. The moment an asset has moved on - a player re-traded, released or
// retired, a pick spent on draft night or flipped again - unwinding this trade
// would no longer restore the world before it, just scramble the world after
// it. So the plan is all-or-nothing: either every asset can go home, or the
// trade is not revertable and the reason names what moved.
//
// The revert itself is deliberately just another trade, pointed backwards.
// Players move by pid and picks by dpid - the same writes, on the same stores,
// as the trade being undone - and a new trade event records the reversal. That
// is what makes it safe in a synced league: every one of those writes already
// syncs, while the alternatives (deleting or editing the original event) are
// exactly the operations the sync layer cannot reconcile, because event ids
// are not stable across devices. The original event stays; the transaction log
// shows a trade and its reversal, which is also simply the truth.
export const planTradeRevert = async (
	event: TradeEvent,
): Promise<
	| {
			// What each side sends BACK: the assets it received in `event`, in
			// event.tids order. Feed straight to processTrade.
			pids: [number[], number[]];
			dpids: [number[], number[]];
	  }
	| {
			error: string;
	  }
> => {
	if (!event.teams || event.phase === undefined) {
		return { error: "This trade is too old to have the data needed." };
	}

	const pids: [number[], number[]] = [[], []];
	const dpids: [number[], number[]] = [[], []];

	for (const i of [0, 1] as const) {
		const tid = event.tids[i];
		for (const asset of event.teams[i].assets) {
			if ("pid" in asset) {
				// Retired and deleted players are not in the cache, and neither can
				// come back anyway, so one lookup answers both questions.
				const p = await idb.cache.players.get(asset.pid);
				if (!p || p.tid !== tid) {
					return {
						error: `${asset.name} is no longer on the team that traded for him.`,
					};
				}
				pids[i].push(asset.pid);
			} else {
				// A used pick is deleted, so existence is the "not yet drafted" check.
				const dp = await idb.cache.draftPicks.get(asset.dpid);
				if (!dp || dp.tid !== tid) {
					return {
						error: "A traded draft pick has since been used or moved.",
					};
				}
				dpids[i].push(asset.dpid);
			}
		}
	}

	return { pids, dpids };
};

// Undo the trade behind an event, God Mode only. Returns an error message, or
// undefined on success.
const revertTrade = async (eid: number): Promise<string | undefined> => {
	if (!g.get("godMode")) {
		return "God Mode is required to revert a trade.";
	}

	const event = await idb.getCopy.events({ eid }, "noCopyCache");
	if (!event || event.type !== "trade") {
		return "Trade not found.";
	}

	const plan = await planTradeRevert(event);
	if ("error" in plan) {
		return plan.error;
	}

	await processTrade(event.tids, plan.pids, plan.dpids, undefined, {
		revert: true,
	});
};

export default revertTrade;
