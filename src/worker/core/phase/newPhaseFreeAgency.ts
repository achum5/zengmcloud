import { contractNegotiation, freeAgents, league, player } from "../index.ts";
import { openStages, stagesOn } from "../realisticFrontOffice/stages.ts";
import { g, helpers } from "../../util/index.ts";
import type { Conditions, PhaseReturn } from "../../../common/types.ts";
import { collegeOpenPortal } from "../college/portal.ts";
import { idb } from "../../db/index.ts";
import { PLAYER } from "../../../common/constants.ts";
import { getNumPlayersTradedAwayNormalizedAll } from "../player/getNumPlayersTradedAwayNormalized.ts";
import { realisticFreeAgencyOn } from "../realisticFrontOffice/isOn.ts";
import {
	decideReSignings as realisticFrontOfficeDecideReSignings,
	pickUpTeamOptions,
} from "../realisticFrontOffice/freeAgency.ts";

const newPhaseFreeAgency = async (
	conditions: Conditions,
): Promise<PhaseReturn> => {
	// In case some weird situation results in games still in the schedule, clear them
	await idb.cache.schedule.clear();

	// Realistic front offices: human teams' re-signing offers not yet decided
	// are decided now.
	if (realisticFreeAgencyOn()) {
		for (const tid of g.get("userTids")) {
			await realisticFrontOfficeDecideReSignings(tid);
		}
	}
	// And their team options not yet decided are picked up.
	if (stagesOn()) {
		await pickUpTeamOptions();
	}

	// Delete all current negotiations to resign players
	await contractNegotiation.cancelAll();

	if (g.get("college")) {
		await collegeOpenPortal(conditions);
		return {
			redirect: {
				url: helpers.leagueUrl(["recruiting"]),
				text: "View the transfer portal",
			},
			updateEvents: ["playerMovement"],
		};
	}

	await freeAgents.ensureEnoughPlayers();

	await freeAgents.normalizeContractDemands({ type: "freeAgentsOnly" });

	// Realistic front offices: free agency in stages, not days.
	if (stagesOn()) {
		await league.setGameAttributes({
			daysLeft: await openStages(),
			faInStages: true,
		});
	} else if (g.get("faInStages")) {
		await league.setGameAttributes({ faInStages: false });
	}

	// For any players who were free agents last year too, reset numPlayersTradedAwayNormalized
	const numPlayersTradedAwayNormalized =
		await getNumPlayersTradedAwayNormalizedAll();
	const players = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
	).filter((p) => p.yearsFreeAgent > 0);
	for (const p of players) {
		player.addToFreeAgents(p, numPlayersTradedAwayNormalized, null);
	}
	await idb.cache.players.putAll(players);

	return {
		redirect: {
			url: helpers.leagueUrl(["free_agents"]),
			text: "View free agents",
		},
		updateEvents: ["playerMovement"],
	};
};

export default newPhaseFreeAgency;
