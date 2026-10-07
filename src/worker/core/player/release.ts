import addToFreeAgents from "./addToFreeAgents.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import type { Player, TransactionRevert } from "../../../common/types.ts";
import { PHASE } from "../../../common/constants.ts";
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
	const salariesBefore = helpers.deepCopy(p.salaries);

	// College: an NIL deal ends when he leaves.
	if (g.get("college")) {
		p.salaries = p.salaries.filter((row) => row.season < g.get("season"));
	}

	// Keep track of player salary even when he's off the team, but make an exception for players who were just drafted.
	if (!justDrafted && !g.get("college")) {
		// ...and of course for players whose contracts have already expired.
		if (
			p.contract.exp > g.get("season") ||
			(p.contract.exp === g.get("season") && g.get("phase") < PHASE.PLAYOFFS)
		) {
			await idb.cache.releasedPlayers.add({
				pid: p.pid,
				tid: p.tid,
				contract: helpers.deepCopy(p.contract),
			});
			revert.deadMoney = true;
		}
	}

	if (justDrafted) {
		// Clear player salary log if just drafted, because this won't be paid.
		p.salaries = [];
	}

	if (p.salaries.length !== salariesBefore.length) {
		revert.before.salaries = salariesBefore;
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
	addToFreeAgents(p, await getNumPlayersTradedAwayNormalizedAll());
	await idb.cache.players.put(p);
};

export default release;
