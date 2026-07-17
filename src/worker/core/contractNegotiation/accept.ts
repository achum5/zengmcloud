import { player, team } from "../index.ts";
import cancel from "./cancel.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import type {
	Negotiation,
	PlayerContract,
	UndoableAction,
} from "../../../common/types.ts";
import { PHASE } from "../../../common/constants.ts";
import { actualPhase } from "../../util/actualPhase.ts";
import { getHardCap } from "../../util/getHardCap.ts";

/**
 * Accept the player's offer.
 *
 * If successful, then the team's current roster will be displayed.
 *
 * @memberOf core.contractNegotiation
 * @param {number} pid An integer that must correspond with the player ID of a player in an ongoing negotiation.
 * @return {Promise.<string=>} If an error occurs, resolves to a string error message.
 */
const accept = async ({
	negotiation,
	amount,
	exp,
	dryRun,
}: {
	negotiation: Negotiation;
	amount: number;
	exp: number;
	dryRun?: boolean;
}) => {
	const salaryCapType = g.get("salaryCapType");
	const hardCap = getHardCap(g.get("userTid"));

	if (salaryCapType !== "none" || Number.isFinite(hardCap)) {
		const payroll = await team.getPayroll(g.get("userTid"));

		if (salaryCapType !== "none") {
			const birdException = negotiation.resigning && salaryCapType === "soft";

			// If this contract brings team over the salary cap, it's not a minimum contract, and it's not re-signing a current
			// player with the Bird exception, ERROR!
			if (
				!birdException &&
				payroll + amount - 1 > g.get("salaryCap") &&
				amount - 1 > g.get("minContract")
			) {
				return `You cannot go over the salary cap to sign ${
					salaryCapType === "hard" ? "players" : "free agents"
				} to contracts higher than the minimum salary.`;
			}
		}

		// Secondary hard cap: an absolute ceiling for bound teams that overrides
		// even the soft-cap Bird exception. Above it, only minimum-salary signings
		// are allowed, and only until the roster is full — enough to field a bench,
		// but the trade rule still stops those minimum guys being used to take on
		// salary over the cap, so they can't be stockpiled as trade fodder.
		if (Number.isFinite(hardCap) && payroll + amount - 1 > hardCap) {
			const isMinContract = amount - 1 <= g.get("minContract");
			let allowed = false;
			if (isMinContract) {
				const roster = await idb.cache.players.indexGetAll(
					"playersByTid",
					g.get("userTid"),
				);
				allowed = roster.length < g.get("maxRosterSize");
			}
			if (!allowed) {
				return `This team is at its hard cap (${helpers.formatCurrency(
					hardCap / 1000,
					"M",
				)}). You can only add minimum-salary players, and only until your roster is full.`;
			}
		}
	}

	// This error is for sanity checking in multi team mode. Need to check for existence of negotiation.tid because it
	// wasn't there originally and I didn't write upgrade code. Can safely get rid of it later.
	if (negotiation.tid !== undefined && negotiation.tid !== g.get("userTid")) {
		return `This negotiation was started by the ${
			g.get("teamInfoCache")[negotiation.tid]?.region
		} ${g.get("teamInfoCache")[negotiation.tid]?.name} but you are the ${
			g.get("teamInfoCache")[g.get("userTid")]?.region
		} ${
			g.get("teamInfoCache")[g.get("userTid")]?.name
		}. Either switch teams or cancel this negotiation.`;
	}

	const p = await idb.cache.players.get(negotiation.pid);
	if (!p) {
		return "Invalid pid";
	}

	// Make sure the user didn't do something in another tab to change the willingness to negotiate, such as trading away players
	const mood = await player.moodInfo(p, g.get("userTid"));
	if (!mood.willing) {
		return "Player is no longer willing to negotiate.";
	}

	const phase = actualPhase();

	const undo: UndoableAction = {
		type: "sign",
		phase,
		tid: g.get("userTid"),
		eid: undefined,
		numDaysFreeAgent: p.numDaysFreeAgent,
		numPlayersTradedAwayNormalized: helpers.deepCopy(
			p.numPlayersTradedAwayNormalized,
		),
		jerseyNumber: p.jerseyNumber,
		contract: helpers.deepCopy(p.contract),
		salaries: helpers.deepCopy(p.salaries),
		transactions: helpers.deepCopy(p.transactions),
	};

	const contract: PlayerContract = {
		amount,
		exp,
	};
	if (p.contract.rookie && phase === PHASE.RESIGN_PLAYERS) {
		// Not sure if the phase condition is necessary. The purpose of this is for hard cap rookies with rookie contract scale.
		contract.rookie = true;
	}

	if (!dryRun) {
		undo.eid = await player.sign(p, g.get("userTid"), contract, phase);
		await idb.cache.players.put(p);
		await cancel(negotiation.pid);
	}

	return undo;
};

export default accept;
