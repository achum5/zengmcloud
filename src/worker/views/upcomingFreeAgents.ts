import { PHASE, PLAYER } from "../../common/constants.ts";
import { player, team } from "../core/index.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import type { ViewInput } from "../../common/types.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";
import { bySport } from "../../common/sportFunctions.ts";
import { getProjectedContractAmounts } from "../util/projectedContracts.ts";

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
		// Only where the game actually runs the bidding - elsewhere the auction
		// falls back to genContract, which is what this page already computes.
		bySport({
			baseball: false,
			basketball: true,
			football: false,
			hockey: true,
		}) && inputs.season === g.get("season")
			? await getProjectedContractAmounts()
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

	// Apply mood.
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
