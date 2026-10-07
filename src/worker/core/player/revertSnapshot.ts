import { PHASE } from "../../../common/constants.ts";
import type { Player, TransactionRevert } from "../../../common/types.ts";
import { g, helpers } from "../../util/index.ts";

// The player as a signing, release or draft pick found him, for the revert
// snapshot on its event. Copies, never references: the player record goes on
// changing, and the event must not change with it - a synced event is deleted
// on other devices by matching its content, so one that drifted in memory
// could never be found there.
export const revertBefore = (
	p: Pick<
		Player,
		| "tid"
		| "contract"
		| "numDaysFreeAgent"
		| "gamesUntilTradable"
		| "ptModifier"
		| "yearsFreeAgent"
		| "jerseyNumber"
	>,
): TransactionRevert["before"] => {
	const before: TransactionRevert["before"] = {
		tid: p.tid,
		contract: helpers.deepCopy(p.contract),
		numDaysFreeAgent: p.numDaysFreeAgent,
		gamesUntilTradable: p.gamesUntilTradable,
		ptModifier: p.ptModifier,
		yearsFreeAgent: p.yearsFreeAgent,
	};
	if (p.jerseyNumber !== undefined) {
		before.jerseyNumber = p.jerseyNumber;
	}
	return before;
};

// The first season setContract writes a salary row for, when a contract is
// signed right now.
export const salaryStartSeason = () =>
	g.get("season") + (g.get("phase") > PHASE.AFTER_TRADE_DEADLINE ? 1 : 0);
