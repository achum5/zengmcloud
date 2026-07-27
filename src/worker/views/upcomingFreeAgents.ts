import { PHASE, PLAYER } from "../../common/constants.ts";
import { freeAgents, player, team } from "../core/index.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import type { ViewInput } from "../../common/types.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";
import { bySport, isSport } from "../../common/sportFunctions.ts";

// Projected asking prices, from the SAME auction the game uses to actually
// price free agents in basketball. The page used to show a raw genContract
// number, which for basketball is explicitly not the pricing model (see
// normalizeContractDemands: basketball runs 60 rounds of bidding against real
// team cap space, and only the other sports fall back to the formula). So the
// projection disagreed with what players went on to sign for - usually badly
// for the players at the top and bottom of the market, since bidding spreads
// salaries far more than the formula does.
//
// Cached per league state so the numbers do not jump every time the page is
// opened: the auction is stochastic, and a projection that changes on refresh
// reads as broken even when it is closer to the truth. Invalidated whenever the
// season or phase moves.
let projectionCache: { key: string; amounts: Map<number, number> } | undefined;

const getAuctionProjections = async (
	pids: number[],
): Promise<Map<number, number> | undefined> => {
	if (pids.length === 0) {
		return undefined;
	}

	const key = `${g.get("lid")}|${g.get("season")}|${g.get("phase")}|${pids.length}`;
	if (projectionCache?.key === key) {
		return projectionCache.amounts;
	}

	try {
		const amounts = await freeAgents.normalizeContractDemands({
			type: "includeExpiringContracts",
			pids,
			dryRun: true,
		});
		if (amounts) {
			projectionCache = { key, amounts };
		}
		return amounts;
	} catch (error) {
		// A projection is a nicety - fall back to the formula rather than break
		// the page.
		console.error("Failed to project free agent contracts", error);
		return undefined;
	}
};

const updateUpcomingFreeAgents = async (
	inputs: ViewInput<"upcomingFreeAgents">,
) => {
	const stats = bySport({
		baseball: ["gp", "keyStats", "war"],
		basketball: ["min", "pts", "trb", "ast", "per"],
		football: ["gp", "keyStats", "av"],
		hockey: ["gp", "keyStats", "ops", "dps", "ps"],
	});

	const showActualFreeAgents =
		g.get("phase") === PHASE.RESIGN_PLAYERS &&
		g.get("season") === inputs.season;

	let players: any[] = showActualFreeAgents
		? await idb.getCopies.players({
				tid: PLAYER.FREE_AGENT,
			})
		: await idb.getCopies.players({
				tid: [0, Infinity],
				filter: (p) => p.contract.exp === inputs.season,
			});

	// The auction only knows about contracts expiring THIS season (that is the
	// pool it builds), so it can price the season we are actually heading into.
	// For a season further out, nothing has expired yet and the formula is the
	// only thing available.
	const auctionAmounts =
		isSport("basketball") && inputs.season === g.get("season")
			? await getAuctionProjections(players.map((p) => p.pid))
			: undefined;

	// Done before filter so full player object can be passed to player.genContract.
	for (const p of players) {
		p.contractDesired = player.genContract(p, false); // No randomization
		const projected = auctionAmounts?.get(p.pid);
		if (projected !== undefined) {
			p.contractDesired.amount = projected;
		}
		p.contractDesired.exp += inputs.season - g.get("season");

		p.mood = await player.moodInfos(p, {
			contractAmount: p.contractDesired.amount,
		});
	}

	players = addFirstNameShort(
		await idb.getCopies.playersPlus(players, {
			attrs: [
				"pid",
				"firstName",
				"lastName",
				"abbrev",
				"tid",
				"age",
				"contract",
				"injury",
				"contractDesired",
				"watch",
				"jerseyNumber",
				"mood",
			],
			ratings: ["ovr", "pot", "skills", "pos"],
			stats,
			season: g.get("season"),
			showNoStats: true,
			showRookies: true,
			fuzz: true,
		}),
	);

	// Apply mood
	for (const p of players) {
		p.contractDesired.amount = p.mood.user.contractAmount / 1000;
	}

	const projectedPayroll = await team.getPayroll(
		g.get("userTid"),
		inputs.season,
	);
	const projectedCapSpace = g.get("salaryCap") - projectedPayroll;

	return {
		players,
		projectedCapSpace,
		season: inputs.season,
		stats,
	};
};

export default updateUpcomingFreeAgents;
