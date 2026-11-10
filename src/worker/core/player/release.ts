import addToFreeAgents from "./addToFreeAgents.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import type { Player, TransactionRevert } from "../../../common/types.ts";
import { getCurrentContract } from "../../../common/contracts.ts";
import { isContractFullyPaid } from "../contract/index.ts";
import { getNumPlayersTradedAwayNormalizedAll } from "./getNumPlayersTradedAwayNormalized.ts";
import { revertBefore } from "./revertSnapshot.ts";

/**
 * Release player.
 *
 * This keeps track of what the player's current team owes him, and then calls addToFreeAgents.
 *
 * @memberOf core.player
 * @param {Object} p Player object.
 * @param {boolean} justDrafted True if the player was just drafted by his current team and the regular season hasn't started yet. False otherwise. If True, then the player can be released without paying his salary.
 * @return {Promise}
 */
const release = async (p: Player, justDrafted: boolean) => {
	// Everything this release changes, so God Mode can take it back.
	const revert: TransactionRevert = {
		phase: g.get("phase"),
		before: revertBefore(p),
		numTransactions: p.transactions?.length ?? 0,
	};

	// Just drafted players can be released without paying anything. Otherwise, the team still owes the rest of his contract, see addToFreeAgents.
	const contract = getCurrentContract(p);
	if (contract && justDrafted) {
		p.contracts = p.contracts.filter((row) => row !== contract);
		revert.droppedContract = helpers.deepCopy(contract);
	}

	// College: an NIL deal ends when he leaves, and nobody owes the rest of it.
	const releasedByTid = g.get("college") ? undefined : p.tid;
	if (
		contract &&
		!justDrafted &&
		releasedByTid !== undefined &&
		!isContractFullyPaid(contract)
	) {
		revert.deadMoney = true;
	}

	logEvent({
		type: "release",
		text: `The <a href="${helpers.leagueUrl([
			"roster",
			`${g.get("teamInfoCache")[p.tid]?.abbrev}_${p.tid}`,
			g.get("season"),
		])}">${
			g.get("teamInfoCache")[p.tid]?.name
		}</a> released <a href="${helpers.leagueUrl(["player", p.pid])}">${
			p.firstName
		} ${p.lastName}</a>.`,
		showNotification: false,
		pids: [p.pid],
		tids: [p.tid],
		// College moves can't be reverted, so they carry nothing to revert with.
		...(g.get("college") ? {} : { revert }),
	});
	addToFreeAgents(
		p,
		await getNumPlayersTradedAwayNormalizedAll(),
		releasedByTid,
	);
	await idb.cache.players.put(p);
};

export default release;
