// League-side plumbing for the contract-value model in common/contractValue.ts.
//
// EVERYTHING HERE IS IN MILLIONS, not the thousands the league file stores
// contracts in. That is because playersPlus already hands back `salary` in
// millions, and it is what the tables display - converting once, here, beats
// converting at four call sites and getting one of them wrong.

import { PHASE, PLAYER } from "../../common/constants.ts";
import {
	getContractValue,
	getDollarsPerWin,
	type ContractValue,
} from "../../common/contractValue.ts";
import { idb } from "../db/index.ts";
import { g } from "./index.ts";

export type ContractValueContext = {
	dollarsPerWin: number;
	minContract: number;
};

// One priced player, as the views see them after playersPlus.
type PricedPlayer = {
	pid: number;
	salary?: number;
	stats?: { vorp?: number; gp?: number };
};

const settings = () => ({
	minContract: g.get("minContract") / 1000,
	salaryCap: g.get("salaryCap") / 1000,
});

// Only players actually being PAID that season set the price of a win.
//
// Free agents are the trap: they show up in the current-season player list with
// no salary for the season, so each one would contribute a full negative
// minimum to the above-the-floor budget. A league with a big free-agent pool
// could drive that budget to zero and price every win in the league at nothing.
const isUnderContract = (p: PricedPlayer) =>
	p.salary !== undefined && p.salary > 0;

export const contractValueInputs = (players: readonly PricedPlayer[]) =>
	players.filter(isUnderContract).map((p) => ({
		vorp: p.stats?.vorp,
		salary: p.salary!,
	}));

// Price a win from a set of players that has ALREADY been loaded league-wide.
// The views that show every player in a season call this, so the extra load
// below is only paid by the ones that don't (team pages).
//
// It must be the league-wide set, never a filtered one: calibrating off a
// single team would re-price wins against that team's own payroll, so a
// tanking roster would make its own scrubs look like bargains.
export const contractValueContextFrom = (
	players: readonly PricedPlayer[],
): ContractValueContext => {
	const { minContract, salaryCap } = settings();
	return {
		minContract,
		dollarsPerWin: getDollarsPerWin(contractValueInputs(players), {
			minContract,
			salaryCap,
		}),
	};
};

// Same thing for callers holding only part of the league (team finances), which
// have to go and read the rest of it.
export const loadContractValueContext = async (
	season: number,
): Promise<ContractValueContext> => {
	const playersAll =
		season === g.get("season") && g.get("phase") <= PHASE.PLAYOFFS
			? await idb.cache.players.indexGetAll("playersByTid", [
					PLAYER.FREE_AGENT,
					Infinity,
				])
			: await idb.getCopies.players({ activeSeason: season }, "noCopyCache");

	const players = await idb.getCopies.playersPlus(playersAll, {
		attrs: ["pid", "salary"],
		stats: ["vorp", "gp"],
		season,
		showNoStats: true,
		showRookies: true,
	});

	return contractValueContextFrom(players);
};

// Undefined - rendered as a blank cell rather than a number - whenever there is
// no production to price. A rookie who has not played is not a bad contract,
// and a preseason table of every player showing a large negative would be
// actively misleading.
export const valueForPlayer = (
	p: PricedPlayer,
	{ dollarsPerWin, minContract }: ContractValueContext,
): ContractValue | undefined => {
	if (!isUnderContract(p) || !p.stats || (p.stats.gp ?? 0) <= 0) {
		return undefined;
	}
	return getContractValue(
		{ vorp: p.stats.vorp, salary: p.salary! },
		minContract,
		dollarsPerWin,
	);
};
