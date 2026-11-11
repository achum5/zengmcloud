import type { TradeTeams } from "../../../common/types.ts";
import type { Lane } from "./lanes.ts";

// What vanilla+'s front offices did, for whoever reports on it (news.ts).

// A team's lane changing.
export type LaneChange = {
	tid: number;
	from: Lane | undefined;
	to: Lane;
	review: string;
};

// What the market did.
export type MarketEvent =
	| {
			kind: "trade";
			teams: TradeTeams;
			initiator: number;
			role: "buyer" | "seller";
			// The player the deal was about.
			pid: number;
			// Offers the seller heard.
			offers: number;
			// How well he fit the buyer (tweaks' factor).
			fit: number;
	  }
	| { kind: "shopped"; seller: number; pid: number; offers: number }
	| { kind: "hunt"; buyer: number; pids: number[] };

export const laneWatchers: ((change: LaneChange) => void)[] = [];
export const marketWatchers: ((event: MarketEvent) => void)[] = [];
