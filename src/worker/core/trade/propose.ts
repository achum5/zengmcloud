import { PHASE } from "../../../common/constants.ts";
import { team } from "../index.ts";
import { g } from "../../util/index.ts";
import clear from "./clear.ts";
import processTrade from "./processTrade.ts";
import summary from "./summary.ts";
import get from "./get.ts";
import { idb } from "../../db/index.ts";
import { hashSavedTrade } from "../../../common/hashSavedTrade.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { getLeagueTradeContext, getTradePosture } from "./tradePosture.ts";
import { isBadRental, sellerAcquiresVet } from "./tradeMotivation.ts";
import moodInfo from "../player/moodInfo.ts";

/**
 * Proposes the current trade in the database.
 *
 * Before proposing the trade, the trade is validated to ensure that all player IDs match up with team IDs.
 *
 * @memberOf core.trade
 * @param {boolean} forceTrade When true (like in God Mode), this trade is accepted regardless of the AI
 * @return {Promise.<boolean, string>} Resolves to an array. The first argument is a boolean for whether the trade was accepted or not. The second argument is a string containing a message to be dispalyed to the user.
 */
const propose = async (
	forceTrade: boolean = false,
): Promise<[boolean, string | null]> => {
	if (
		g.get("phase") >= PHASE.AFTER_TRADE_DEADLINE &&
		g.get("phase") <= PHASE.PLAYOFFS
	) {
		return [
			false,
			`Error! You're not allowed to make trades ${
				g.get("phase") === PHASE.AFTER_TRADE_DEADLINE
					? "after the trade deadline"
					: "now"
			}.`,
		];
	}

	const { teams } = await get();
	const tids: [number, number] = [teams[0].tid, teams[1].tid];
	const pids: [number[], number[]] = [teams[0].pids, teams[1].pids];
	const dpids: [number[], number[]] = [teams[0].dpids, teams[1].dpids];

	// The summary will return a warning if (there is a problem. In that case,
	// that warning will already be pushed to the user so there is no need to
	// return a redundant message here.
	const s = await summary(teams);

	if (s.warning && !forceTrade) {
		return [false, null];
	}

	let outcome = "rejected"; // Default

	// A front office is more than a calculator: some deals are refused no
	// matter what the value math says. Only for AI teams with the smart front
	// office on - another human's team decides for itself, and God Mode's
	// forceTrade skips straight past. Best-effort: if a posture cannot be
	// computed, the deal falls through to the plain value decision.
	if (
		!forceTrade &&
		g.get("smartAiFrontOffice") &&
		!g.get("userTids").includes(teams[1].tid)
	) {
		try {
			const context = await getLeagueTradeContext();
			const posture = await getTradePosture(teams[1].tid, context);
			const refusal = async (): Promise<string | undefined> => {
				// Its building blocks are simply not for sale.
				const blocks = new Set(posture.buildingBlockPids);
				for (const pid of teams[1].pids) {
					if (blocks.has(pid)) {
						const p = await idb.cache.players.get(pid);
						const name = p ? `${p.firstName} ${p.lastName}` : "He";
						return `${name} isn't going anywhere.`;
					}
				}

				const season = g.get("season");
				for (const pid of teams[0].pids) {
					const p = await idb.cache.players.get(pid);
					if (!p) {
						continue;
					}
					// A rebuilder does not absorb a veteran unless it is being
					// paid in draft capital to do it.
					if (
						sellerAcquiresVet({
							acquirerTier: posture.tier,
							age: season - p.born.year,
							value: p.value,
							receivesPicks: teams[0].dpids.length > 0,
						})
					) {
						return "A veteran doesn't fit our timeline.";
					}
					// An expiring player who will not re-sign is a rental, and
					// only a real win-now team takes one on.
					if (p.contract.exp === season) {
						const { probWilling } = await moodInfo(p, teams[1].tid);
						if (
							isBadRental({
								isExpiring: true,
								probWillingAcquirer: probWilling,
								acquirerTier: posture.tier,
							})
						) {
							const name = `${p.firstName} ${p.lastName}`;
							return `${name} would walk after the season. No thanks.`;
						}
					}
				}
				return undefined;
			};
			const message = await refusal();
			if (message !== undefined) {
				return [false, `Trade rejected! "${message}"`];
			}
		} catch (error) {
			console.error("propose: posture guard failed", error);
		}
	}

	const dv = await new ValueChangeCalculator().evaluate({
		tid: teams[1].tid,
		pidsAdd: teams[0].pids,
		pidsRemove: teams[1].pids,
		dpidsAdd: teams[0].dpids,
		dpidsRemove: teams[1].dpids,
		tradingPartnerTid: g.get("userTid"),
	});

	if (dv > 0 || forceTrade) {
		// Compute hash now, since teams is mutated in processTrade somehow
		const hash = hashSavedTrade(teams);

		// Trade players
		outcome = "accepted";
		await processTrade(tids, pids, dpids);

		// Delete from saved trades, if applicable
		await idb.cache.savedTrades.delete(hash);
	}

	if (outcome === "accepted") {
		await clear();

		// Auto-sort team rosters
		for (const tid of tids) {
			const t = await idb.cache.teams.get(tid);
			const onlyNewPlayers =
				g.get("userTids").includes(tid) &&
				!g.get("spectator") &&
				t &&
				!t.keepRosterSorted;

			await team.rosterAutoSort(tid, onlyNewPlayers);
		}

		return [true, 'Trade accepted! "Nice doing business with you!"'];
	}

	// Return a different rejection message based on how close we are to a deal. When dv < 0, the closer to 0, the better the trade for the AI.
	let message;

	if (dv > -2) {
		message = "Close, but not quite good enough.";
	} else if (dv > -5) {
		message = "That's not a good deal for me.";
	} else {
		message = "What, are you crazy?!";
	}

	return [false, `Trade rejected! "${message}"`];
};

export default propose;
