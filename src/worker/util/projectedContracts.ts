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
// The auction is STOCHASTIC: teams are shuffled every round and each pick is a
// softmax draw, so two runs on identical data disagree, sometimes wildly. One
// run of it was being shown as a precise number. A player who happened to draw
// no bids in the early rounds gets scaled down repeatedly and lands near the
// minimum; the same player in the next run lands three times higher. That is the
// single biggest source of error in the projection, and it is not a modelling
// problem - it is sampling noise being reported as a point estimate.
//
// So the auction runs several times. The MEDIAN is the projection (robust to one
// run collapsing), and the spread across runs is reported alongside it, because
// a range is the honest shape of this answer: nobody goes into an offseason
// knowing a player will command exactly $31.78M, only that he'll land somewhere
// in a band.

// Enough runs for a median to mean something and for the band to reflect real
// spread, without making the page load cost several full auctions more than it
// already does. Cached per league state, so this is paid once.
const RUNS = 5;

export type ProjectedAmount = {
	// Median across runs - the number to show and sort by.
	amount: number;
	// Lowest and highest the auction priced him at.
	low: number;
	high: number;
};

let cache: { key: string; amounts: Map<number, ProjectedAmount> } | undefined;

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

const median = (sorted: number[]) => {
	const mid = Math.floor(sorted.length / 2);
	return sorted.length % 2 === 0
		? Math.round((sorted[mid - 1]! + sorted[mid]!) / 2)
		: sorted[mid]!;
};

export const getProjectedContractAmounts = async (): Promise<
	Map<number, ProjectedAmount>
> => {
	const key = `${g.get("lid")}|${g.get("season")}|${g.get("phase")}`;
	if (cache?.key === key) {
		return cache.amounts;
	}

	const amounts = new Map<number, ProjectedAmount>();
	try {
		const pids = await getExpiringPids();
		if (pids.length > 0) {
			const runs: Map<number, number>[] = [];
			for (let i = 0; i < RUNS; i++) {
				const result = await normalizeContractDemands({
					type: "includeExpiringContracts",
					pids,
					dryRun: true,
				});
				if (result) {
					runs.push(result);
				}
			}

			for (const pid of pids) {
				const values = runs
					.map((run) => run.get(pid))
					.filter((x): x is number => x !== undefined)
					.sort((a, b) => a - b);
				if (values.length > 0) {
					amounts.set(pid, {
						amount: median(values),
						low: values[0]!,
						high: values.at(-1)!,
					});
				}
			}
		}
		cache = { key, amounts };
	} catch (error) {
		// A projection is a nicety - never break the page over it.
		console.error("Failed to project contracts", error);
	}
	return amounts;
};

// The auction only prices players who are actually reaching the market this
// season. For anyone under contract past that - the 2007 column for a player
// signed through 2006 - there is nothing to bid on yet, so fall back to the
// formula. Either way this is today's player being priced, which is why these
// numbers are marked as projections wherever they are shown.
export const projectNextContract = (
	p: Player,
	amounts: Map<number, ProjectedAmount>,
): { amount: number; years: number } => ({
	amount: amounts.get(p.pid)?.amount ?? genContract(p, false).amount,
	years: getContractYears(p, {
		season: Math.max(g.get("season"), p.contract.exp),
	}),
});
