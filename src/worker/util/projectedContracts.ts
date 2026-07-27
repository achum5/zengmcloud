import { PHASE, PLAYER } from "../../common/constants.ts";
import normalizeContractDemands, {
	getContractYears,
} from "../core/freeAgents/normalizeContractDemands.ts";
import genContract from "../core/player/genContract.ts";
import { idb } from "../db/index.ts";
import { g } from "./index.ts";
import type { Player } from "../../common/types.ts";

// What a player's NEXT contract is likely to be, for pages that show money the
// team has not actually committed yet (Upcoming Free Agents, and the projected
// years on Team Finances).
//
// The price comes from the SAME auction the game uses to price free agents in
// basketball, not from genContract. genContract is explicitly not the pricing
// model there (see normalizeContractDemands: basketball runs 60 rounds of
// bidding against real team cap space, and only the other sports fall back to
// the formula), so a genContract projection disagreed with what players went on
// to sign for - badly at the top and bottom of the market, since bidding spreads
// salaries much further than the formula does.
//
// Cached per league state, for two reasons: the auction is not cheap, and it is
// stochastic, so a number that changes every time you open the page reads as
// broken even when it is closer to the truth. One cache serves every page, so
// the same player is never quoted two different prices in two places.

let cache: { key: string; amounts: Map<number, number> } | undefined;

// Whoever is actually reaching the market at the end of this season: during
// re-signing that is the free agent pool, before then it is everyone whose
// contract expires this season.
const getExpiringPids = async (): Promise<number[]> => {
	const season = g.get("season");
	const players =
		g.get("phase") === PHASE.RESIGN_PLAYERS
			? await idb.getCopies.players({ tid: PLAYER.FREE_AGENT })
			: await idb.getCopies.players({
					tid: [0, Infinity],
					filter: (p) => p.contract.exp === season,
				});
	return players.map((p) => p.pid);
};

export const getProjectedContractAmounts = async (): Promise<
	Map<number, number>
> => {
	const key = `${g.get("lid")}|${g.get("season")}|${g.get("phase")}`;
	if (cache?.key === key) {
		return cache.amounts;
	}

	try {
		const pids = await getExpiringPids();
		const amounts =
			pids.length > 0
				? await normalizeContractDemands({
						type: "includeExpiringContracts",
						pids,
						dryRun: true,
					})
				: new Map<number, number>();
		cache = { key, amounts: amounts ?? new Map() };
		return cache.amounts;
	} catch (error) {
		// A projection is a nicety - never break the page over it.
		console.error("Failed to project contracts", error);
		return new Map();
	}
};

// The auction only prices players who are actually reaching the market this
// season. For anyone under contract past that - the 2007 column for a player
// signed through 2006 - there is nothing to bid on yet, so fall back to the
// formula. Either way this is today's player being priced, which is why these
// numbers are marked as projections wherever they are shown.
export const projectNextContract = (
	p: Player,
	amounts: Map<number, number>,
): { amount: number; years: number } => ({
	amount: amounts.get(p.pid) ?? genContract(p, false).amount,
	years: getContractYears(p, {
		season: Math.max(g.get("season"), p.contract.exp),
	}),
});
