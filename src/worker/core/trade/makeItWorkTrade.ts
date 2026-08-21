import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import makeItWork from "./makeItWork.ts";
import summary from "./summary.ts";
import get from "./get.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { getLeagueTradeContext, getTradePosture } from "./tradePosture.ts";
import { wasTradedThisSeason } from "./tradeMotivation.ts";

/**
 * Make a trade work
 *
 * This should be called for a trade negotiation, as it will update the trade objectStore.
 *
 * @memberOf core.trade
 * @return {Promise.string} Resolves to a string containing a message to be dispalyed to the user, as if it came from the AI GM.
 */
const makeItWorkTrade = async () => {
	const tr = await get();
	const teams0 = tr.teams;

	// The counter-offer comes from a front office with a plan: its building
	// blocks and just-acquired players never go in the package, even if the
	// user dragged one in - the counter IS the GM saying "not him, but how
	// about this". Best-effort; without a posture the old anything-goes
	// counter still works. Another human's team is left alone.
	const teamsInput = helpers.deepCopy(teams0);
	if (
		g.get("smartAiFrontOffice") &&
		!g.get("userTids").includes(teamsInput[1].tid)
	) {
		try {
			const context = await getLeagueTradeContext();
			const posture = await getTradePosture(teamsInput[1].tid, context);
			const season = g.get("season");
			const aiPlayers = await idb.cache.players.indexGetAll(
				"playersByTid",
				teamsInput[1].tid,
			);
			const offLimits = new Set([
				...posture.buildingBlockPids,
				...aiPlayers
					.filter((p) => wasTradedThisSeason(p.transactions, season))
					.map((p) => p.pid),
			]);
			teamsInput[1].pids = teamsInput[1].pids.filter(
				(pid) => !offLimits.has(pid),
			);
			teamsInput[1].pidsExcluded = [
				...new Set([...teamsInput[1].pidsExcluded, ...offLimits]),
			];
		} catch (error) {
			console.error("makeItWorkTrade: posture guard failed", error);
		}
	}

	const valueChangeCalculator = new ValueChangeCalculator();
	const teams = await makeItWork(teamsInput, {
		holdUserConstant: false,
		valueChangeCalculator,
	});

	if (!teams) {
		return {
			changed: false,
			message: `${
				g.get("teamInfoCache")[teams0[1].tid]?.region
			} GM: "I can't afford to give up so much."`,
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
