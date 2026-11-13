import { PHASE } from "../../../common/constants.ts";
import { team } from "../index.ts";
import { g } from "../../util/index.ts";
import clear from "./clear.ts";
import processTrade from "./processTrade.ts";
import summary from "./summary.ts";
import get from "./get.ts";
import { idb } from "../../db/index.ts";
import { hashSavedTrade } from "../../../common/hashSavedTrade.ts";
import { getTradeBlockedReason } from "../../../common/transactionPhases.ts";
import { tradeValueCalculator } from "../vanillaPlus/index.ts";
import type { TradeTeams } from "../../../common/types.ts";
import { realisticTradesOn, smartCpuOn } from "../realisticFrontOffice/isOn.ts";
import { smartJudgeOffer, smartRejection } from "../smartCpu/humanTrades.ts";
import {
	judgeHumanOffer as realisticFrontOfficeJudgeOffer,
	verdictMessage as realisticFrontOfficeVerdictMessage,
} from "../realisticFrontOffice/humanTrades.ts";

const accept = async (teams: TradeTeams, tids: [number, number]) => {
	const hash = hashSavedTrade(teams);
	const undo = await processTrade(teams, hash);

	await idb.cache.savedTrades.delete(hash);

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

	return {
		accepted: true as const,
		message: 'Trade accepted! "Nice doing business with you!"',
		undo,
	};
};

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
): Promise<
	| Awaited<ReturnType<typeof accept>>
	| { accepted: false; message: string | null; undo?: undefined }
> => {
	const tradeBlockedReason = getTradeBlockedReason({
		challengeNoTrades: g.get("challengeNoTrades"),
		gameOver: g.get("gameOver"),
		otherTeamsWantToHire: g.get("otherTeamsWantToHire"),
		phase: g.get("phase"),
		spectator: g.get("spectator"),
		tradesAfterDeadline: g.get("tradesAfterDeadline"),
	});
	if (tradeBlockedReason) {
		return {
			accepted: false as const,
			message: `Error! You're not allowed to make trades ${
				tradeBlockedReason === "phase" &&
				g.get("phase") === PHASE.AFTER_TRADE_DEADLINE
					? "after the trade deadline"
					: "now"
			}.`,
		};
	}

	const { teams } = await get();
	const tids: [number, number] = [teams[0].tid, teams[1].tid];

	// The summary will return a warning if (there is a problem. In that case,
	// that warning will already be pushed to the user so there is no need to
	// return a redundant message here.
	const s = await summary(teams);

	if (s.warning && !forceTrade) {
		return { accepted: false as const, message: null };
	}

	// The smart CPU judges a human's offer by the GM's worth, the market rule
	// on (smartCpu/humanTrades.ts).
	if (
		smartCpuOn() &&
		!forceTrade &&
		!g.get("userTids").includes(teams[1].tid)
	) {
		const verdict = await smartJudgeOffer(
			teams[1].tid,
			teams[0].tid,
			{ pids: teams[1].pids, dpids: teams[1].dpids },
			{ pids: teams[0].pids, dpids: teams[0].dpids },
		);
		if (verdict.gain > 0) {
			return accept(teams, tids);
		}
		return { accepted: false as const, message: smartRejection(verdict) };
	}

	// Realistic front offices judge a human's offer by their own valuation
	// (vanilla+ by vanilla's, below).
	if (
		realisticTradesOn() &&
		!forceTrade &&
		!g.get("userTids").includes(teams[1].tid)
	) {
		const verdict = await realisticFrontOfficeJudgeOffer(teams[1], teams[0]);
		if (verdict.kind !== "yes") {
			return {
				accepted: false as const,
				message: realisticFrontOfficeVerdictMessage(verdict),
			};
		}
		return accept(teams, tids);
	}

	const dv = await (
		await tradeValueCalculator()
	).evaluate({
		tid: teams[1].tid,
		pidsAdd: teams[0].pids,
		pidsRemove: teams[1].pids,
		dpidsAdd: teams[0].dpids,
		dpidsRemove: teams[1].dpids,
		tradingPartnerTid: g.get("userTid"),
	});

	if (dv > 0 || forceTrade) {
		return accept(teams, tids);
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

	return { accepted: false as const, message: `Trade rejected! "${message}"` };
};

export default propose;
