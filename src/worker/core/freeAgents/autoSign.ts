import { PHASE, PLAYER } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import getBest from "./getBest.ts";
import { getContractDemand } from "../contract/index.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getHardCap } from "../../util/getHardCap.ts";
import { orderBy } from "../../../common/utils.ts";
import { shuffle } from "../../../common/random.ts";
import { userControlsTeam } from "../../util/userControlsTeam.ts";
import { revertBefore } from "../player/revertSnapshot.ts";
import {
	realisticFreeAgencyOn,
	smartVanillaOn,
} from "../realisticFrontOffice/isOn.ts";
import { noteVanillaPool, smartVanillaSigning } from "../smartCpu/vanilla.ts";
import { smartDecidesHere } from "../smartCpu/decidesHere.ts";
import { freeAgencyDay as realisticFrontOfficeFreeAgencyDay } from "../realisticFrontOffice/freeAgency.ts";

/**
 * AI teams sign free agents.
 *
 * Each team (in random order) will sign free agents up to their salary cap or roster size limit. This should eventually be made smarter
 *
 * @memberOf core.freeAgents
 * @return {Promise}
 */
const autoSign = async ({
	// Every team looks, as on the day the preseason opens (the smart CPU's
	// signings run now from the Danger Zone, api smartMarketNow).
	everyTeam = false,
}: { everyTeam?: boolean } = {}) => {
	// The smart CPU on vanilla's rules signs only where it decides: a league
	// mate's own game that ends a day leaves the signings to the device in
	// charge of simming (smartCpu/decidesHere.ts).
	if (smartVanillaOn() && !smartDecidesHere()) {
		return;
	}
	// Realistic front offices sign by their own valuation, in the offer market
	// or by stock's rules.
	if (realisticFreeAgencyOn()) {
		await realisticFrontOfficeFreeAgencyDay();
		return;
	}

	const players = await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	);

	if (players.length === 0) {
		return;
	}

	// The smart CPU prices a team's room by what is left to buy with it, so
	// the pool is measured before anyone signs (smartCpu/vanilla.ts).
	await noteVanillaPool();
	// In free agency the smart CPU's teams look every day, as a human GM
	// does: whether a free agent is worth signing now or worth waiting on is
	// its worth's call. Vanilla's random days off (three days in four, nine in
	// ten rebuilding) left teams with room out on the days the best free
	// agents signed (League 5: rebuilders sat out days 1-5 more often than
	// not).
	const looksEveryDay =
		smartVanillaOn() && (everyTeam || g.get("phase") === PHASE.FREE_AGENCY);

	// List of free agents, sorted by value
	let playersSorted = orderBy(players, "value", "desc");

	// Randomly order teams
	const teams = await idb.cache.teams.getAll();
	shuffle(teams);

	for (const t of teams) {
		// Skip the user's team
		if (userControlsTeam(t.tid)) {
			continue;
		}

		if (t.disabled) {
			continue;
		}

		let probSkip;
		if (looksEveryDay) {
			probSkip = 0;
		} else if (__SPORT === "basketball") {
			probSkip = t.strategy === "rebuilding" ? 0.9 : 0.75;
		} else {
			probSkip = 0.5;
		}

		// Skip teams sometimes
		if (Math.random() < probSkip) {
			continue;
		}

		const playersOnRoster = await idb.cache.players.indexGetAll(
			"playersByTid",
			t.tid,
		);

		// With forceHistoricalRosters, only sign FAs if we have to
		if (
			playersOnRoster.length >= g.get("minRosterSize") &&
			g.get("forceHistoricalRosters")
		) {
			continue;
		}

		// Ignore roster size, will drop bad player if necessary in checkRosterSizes, and getBest won't sign min contract player unless under the roster limit
		const payroll = await team.getPayroll(t.tid);
		// The smart CPU on vanilla's rules: the one it gains most from
		// (smartCpu/vanilla.ts).
		const p = smartVanillaOn()
			? await smartVanillaSigning(
					t.tid,
					playersSorted,
					payroll,
					getHardCap(t.tid),
				)
			: getBest(playersOnRoster, playersSorted, payroll, getHardCap(t.tid));
		if (p) {
			// Remove from list of free agents
			playersSorted = playersSorted.filter((p2) => p2 !== p);

			// Him as he was before signing, so reverting the signing hands back the
			// free agent he was.
			const before = revertBefore(p);

			await player.sign(p, t.tid, getContractDemand(p), g.get("phase"), before);
			await idb.cache.players.put(p);
			await team.rosterAutoSort(t.tid);
		}
	}
};

export default autoSign;
