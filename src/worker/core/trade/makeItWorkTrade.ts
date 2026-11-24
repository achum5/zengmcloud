import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import makeItWork from "./makeItWork.ts";
import summary from "./summary.ts";
import get from "./get.ts";
import { tradeValueCalculator } from "../vanillaPlus/index.ts";
import {
	realisticTradesOn,
	smartCpuOn,
	smartVanillaOn,
} from "../realisticFrontOffice/isOn.ts";
import { ownReadValuer, smartGuardOffer } from "../smartCpu/humanTrades.ts";
import { smartMakeItWork } from "../smartCpu/screens.ts";
import { counterTeams } from "../realisticFrontOffice/proposals.ts";

/**
 * Make a trade work
 *
 * This should be called for a trade negotiation, as it will update the trade objectStore.
 *
 * @memberOf core.trade
 * @return {Promise.string} Resolves to a string containing a message to be displayed to the user, as if it came from the AI GM.
 */
const makeItWorkTrade = async () => {
	const tr = await get();
	const teams0 = tr.teams;
	// Realistic front offices: a CPU team's own read of the deal
	// (realisticFrontOffice/proposals.ts).
	const frontOffice =
		realisticTradesOn() && !g.get("userTids").includes(teams0[1].tid);
	const vanilla = await tradeValueCalculator();
	// The smart CPU on vanilla's rules: what the team would take by its own
	// read too, so the search asks for more (smartCpu/humanTrades.ts).
	const valueChangeCalculator =
		smartVanillaOn() && !g.get("userTids").includes(teams0[1].tid)
			? await ownReadValuer(vanilla)
			: vanilla;
	// The smart CPU: what it would take, by its own worth
	// (smartCpu/screens.ts).
	const teams = frontOffice
		? await counterTeams(helpers.deepCopy(teams0))
		: smartCpuOn() && !g.get("userTids").includes(teams0[1].tid)
			? await smartMakeItWork(helpers.deepCopy(teams0))
			: await makeItWork(helpers.deepCopy(teams0), {
					holdUserConstant: false,
					valueChangeCalculator,
				});

	// The smart CPU on vanilla's rules: not a deal it would then turn down
	// (smartCpu/humanTrades.ts).
	if (teams && smartVanillaOn() && !g.get("userTids").includes(teams[1].tid)) {
		const guard = await smartGuardOffer(
			teams[1].tid,
			teams[0].tid,
			{ pids: teams[1].pids, dpids: teams[1].dpids },
			{ pids: teams[0].pids, dpids: teams[0].dpids },
		);
		if (!guard.ok) {
			return {
				changed: false,
				message: `${g.get("teamInfoCache")[teams0[1].tid]?.region} GM: ${guard.message.replace("Trade rejected! ", "")}`,
			};
		}
	}

	if (!teams) {
		return {
			changed: false,
			message: `${g.get("teamInfoCache")[teams0[1].tid]?.region} GM: "${
				frontOffice
					? "I don't see a deal there."
					: "I can't afford to give up so much."
			}"`,
		};
	}

	const s = await summary(teams); // Store AI's proposed trade in database, if it's different

	let updated = false;

	for (const i of [0, 1] as const) {
		if (teams[i].tid !== teams0[i].tid) {
			updated = true;
			break;
		}

		if (teams[i].pids.toString() !== teams0[i].pids.toString()) {
			updated = true;
			break;
		}

		if (teams[i].dpids.toString() !== teams0[i].dpids.toString()) {
			updated = true;
			break;
		}
	}

	if (updated) {
		const tr2 = await get();
		tr2.teams = teams;
		await idb.cache.trade.put(tr2);
	}

	if (s.warning) {
		return {
			changed: updated,
			message: `${
				g.get("teamInfoCache")[teams[1].tid]?.region
			} GM: "Something like this would work if you can figure out how to get it done without breaking the salary cap rules."`,
		};
	}

	return {
		changed: updated,
		message: `${
			g.get("teamInfoCache")[teams[1].tid]?.region
		} GM: "How does this sound?"`,
	};
};

export default makeItWorkTrade;
