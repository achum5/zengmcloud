import type { Player, TransactionRevert } from "../../../common/types.ts";
import { getContractSummary } from "../contract/index.ts";
import { g } from "../../util/index.ts";

// The player as a signing, release or draft pick found him, for the revert
// snapshot on its event. Copies, never references: the player record goes on
// changing, and the event must not change with it - a synced event is deleted
// on other devices by matching its content, so one that drifted in memory
// could never be found there.
export const revertBefore = (
	p: Pick<
		Player,
		| "tid"
		| "contractDemand"
		| "contracts"
		| "numDaysFreeAgent"
		| "gamesUntilTradable"
		| "ptModifier"
		| "yearsFreeAgent"
		| "jerseyNumber"
	>,
): TransactionRevert["before"] => {
	// What he was asking for (a free agent or prospect), or the contract he was
	// under (a player on a team). getContractSummary returns a fresh object.
	const contract = getContractSummary(p) ?? {
		amount: g.get("minContract"),
		exp: g.get("season"),
	};
	const before: TransactionRevert["before"] = {
		tid: p.tid,
		contract,
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
