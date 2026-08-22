import { PLAYER, POSITION_COUNTS } from "../../../common/constants.ts";
import { player, freeAgents } from "../index.ts";
import rosterAutoSort from "./rosterAutoSort.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, local } from "../../util/index.ts";
import type { Player } from "../../../common/types.ts";
import { KEY_POSITIONS_NEEDED } from "../freeAgents/getBest.ts";
import { bySport } from "../../../common/sportFunctions.ts";
import { last } from "../../../common/utils.ts";
import { cutOrder } from "./rosterCuts.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
	type TradePosture,
} from "../trade/tradePosture.ts";

export const dropPlayers = async (
	players: Player[],
	numToDrop: number,
	// What the franchise is trying to do, when it is known. Decides the ORDER
	// players are let go in - see team/rosterCuts.ts. Absent (an unsmart league,
	// or a caller with no posture in hand) keeps the old lowest-value-first
	// ordering exactly.
	tier?: TradePosture["tier"],
) => {
	// Automatically drop lowest value players until we reach g.get("maxRosterSize")

	// Only drop player from a position there is an excess of (no dropping your only kicker)
	let counts: typeof POSITION_COUNTS | undefined;
	let countsHealthyKey: Record<string, number> | undefined;
	if (
		bySport({
			baseball: true,
			basketball: false,
			football: true,
			hockey: true,
		})
	) {
		counts = { ...POSITION_COUNTS };
		for (const pos of Object.keys(counts)) {
			counts[pos] = 0;
		}

		if (KEY_POSITIONS_NEEDED) {
			countsHealthyKey = {};
			for (const pos of Object.keys(KEY_POSITIONS_NEEDED)) {
				countsHealthyKey[pos] = 0;
			}
		}

		for (const p of players) {
			const pos = last(p.ratings).pos;

			if (counts[pos] !== undefined) {
				counts[pos] += 1;
			}

			if (
				countsHealthyKey?.[pos] !== undefined &&
				p.injury.gamesRemaining === 0
			) {
				countsHealthyKey[pos] += 1;
			}
		}

		let validPositions = [];
		for (const [pos, count] of Object.entries(counts)) {
			if (count > POSITION_COUNTS[pos]!) {
				validPositions.push(pos);
			}
		}

		// Should be impossible, but just in case, include all players except K/P
		if (validPositions.length === 0) {
			validPositions = Object.keys(POSITION_COUNTS).filter(
				(pos) => pos !== "K" && pos !== "P",
			);
		}
	}

	// First to be let go, first. Anchored on value either way; the posture only
	// decides between comparable players - see team/rosterCuts.ts for why the
	// old raw-value ordering cut a rebuilding team's youngest player.
	const ordered = cutOrder(
		players.map((p) => ({
			pid: p.pid,
			value: p.value,
			age: g.get("season") - p.born.year,
			pos: last(p.ratings).pos,
			contractAmount: p.contract.amount,
			contractExp: p.contract.exp,
		})),
		tier,
		{ season: g.get("season"), salaryCap: g.get("salaryCap") },
	);
	const byPid = new Map(players.map((p) => [p.pid, p]));
	players = ordered.map((o) => byPid.get(o.pid)!).filter(Boolean);

	const releasedPIDs = [];
	for (const p of players) {
		if (
			counts &&
			bySport({
				baseball: true,
				basketball: false,
				football: true,
				hockey: true,
			})
		) {
			const pos = last(p.ratings).pos;

			if (countsHealthyKey) {
				// If this is a key position and there is only one healthy player, keep the healthy player
				if (
					countsHealthyKey[pos]! <= (KEY_POSITIONS_NEEDED?.[pos] ?? 1) &&
					p.injury.gamesRemaining === 0
				) {
					continue;
				}
			}

			// Use 1 as fallback limit rather than POSITION_COUNTS[pos], just to be sure it's not some weird league where POSITION_COUNTS don't apply
			if (counts[pos]! <= (KEY_POSITIONS_NEEDED?.[pos] ?? 1)) {
				continue;
			}

			counts[pos]! -= 1;

			if (countsHealthyKey?.[pos] !== undefined) {
				countsHealthyKey[pos] -= 1;
			}
		}

		await player.release(p, false);
		releasedPIDs.push(p.pid);

		if (releasedPIDs.length >= numToDrop) {
			break;
		}
	}

	return releasedPIDs;
};

/**
 * Check roster size limits
 *
 * If any AI team is over the maximum roster size, cut their worst players.
 * If any AI team is under the minimum roster size, sign minimum contract
 * players until the limit is reached. If the user's team is breaking one of
 * these roster size limits, display a warning.
 *
 * @memberOf core.team
 * @return {Promise.?string} Resolves to null if there is no error, or a string with the error message otherwise.
 */
const checkRosterSizes = async (
	userOrOther: "user" | "other",
): Promise<string | undefined> => {
	// Built at most once, and only for a team that is actually over the limit -
	// the context walks every player in the league, so a league where nobody
	// needs cutting pays nothing for this.
	const smart = g.get("smartAiFrontOffice");
	let leagueContext:
		| Awaited<ReturnType<typeof getLeagueTradeContext>>
		| undefined;
	const tiers = new Map<number, TradePosture["tier"] | undefined>();
	const tierFor = async (tid: number) => {
		if (!smart) {
			return undefined;
		}
		if (tiers.has(tid)) {
			return tiers.get(tid);
		}
		let tier: TradePosture["tier"] | undefined;
		try {
			leagueContext ??= await getLeagueTradeContext();
			tier = (await getTradePosture(tid, leagueContext)).tier;
		} catch (error) {
			// A roster that has to get legal is not the place to fail; without a
			// posture the ordering is the one it always was.
			console.error("Failed to read a posture for roster cuts", error);
			tier = undefined;
		}
		tiers.set(tid, tier);
		return tier;
	};

	const minFreeAgents: Player[] = [];
	let userTeamSizeError: string | undefined;

	const releasedPIDs: number[] = [];

	const checkRosterSize = async (tid: number, userTeamAndActive: boolean) => {
		const players = await idb.cache.players.indexGetAll("playersByTid", tid);
		let numPlayersOnRoster = players.length;

		if (numPlayersOnRoster > g.get("maxRosterSize")) {
			if (userTeamAndActive) {
				if (g.get("userTids").length <= 1) {
					userTeamSizeError = "Your team has ";
				} else {
					userTeamSizeError = `The ${g.get("teamInfoCache")[tid]?.region} ${
						g.get("teamInfoCache")[tid]?.name
					} have `;
				}

				userTeamSizeError += `more than the maximum number of players (${g.get(
					"maxRosterSize",
				)}). You must remove players (by <a href="${helpers.leagueUrl([
					"roster",
				])}">releasing them from your roster</a> or through <a href="${helpers.leagueUrl(
					["trade"],
				)}">trades</a>) before continuing.`;
			} else {
				const releasedPIDsTemp = await dropPlayers(
					players,
					numPlayersOnRoster - g.get("maxRosterSize"),
					await tierFor(tid),
				);
				releasedPIDs.push(...releasedPIDsTemp);
			}
		} else if (numPlayersOnRoster < g.get("minRosterSize")) {
			if (userTeamAndActive) {
				if (g.get("userTids").length <= 1) {
					userTeamSizeError = "Your team has ";
				} else {
					userTeamSizeError = `The ${g.get("teamInfoCache")[tid]?.region} ${
						g.get("teamInfoCache")[tid]?.name
					} have `;
				}

				userTeamSizeError += `less than the minimum number of players (${g.get(
					"minRosterSize",
				)}). You must add players (through <a href="${helpers.leagueUrl([
					"free_agents",
				])}">free agency</a> or <a href="${helpers.leagueUrl([
					"trade",
				])}">trades</a>) before continuing.<br><br>Reminder: you can always sign free agents to ${helpers.formatCurrency(
					g.get("minContract") / 1000,
					"M",
					2,
				)}/yr contracts, even if you're over the cap!`;
			} else {
				// Auto-add players
				while (numPlayersOnRoster < g.get("minRosterSize")) {
					// See also core.phase
					let p: any = minFreeAgents.shift();

					if (!p) {
						p = await player.genRandomFreeAgent();
					}

					await player.sign(p, tid, p.contract, g.get("phase"));
					await idb.cache.players.put(p);
					numPlayersOnRoster += 1;
				}
			}
		}

		// Auto sort rosters (except player's team)
		// This will sort all AI rosters before every game. Excessive? It could change some times, but usually it won't
		const t = await idb.cache.teams.get(tid);
		if (!userTeamAndActive || (t && t.keepRosterSorted)) {
			await rosterAutoSort(tid);
		}
	};

	const players = await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	);

	// List of free agents looking for minimum contracts, sorted by value. This is used to bump teams up to the minimum roster size.
	for (const p of players) {
		if (p.contract.amount === g.get("minContract")) {
			minFreeAgents.push(p);
		}
	}

	minFreeAgents.sort((a, b) => b.value - a.value); // Make sure teams are all within the roster limits

	const teams = await idb.cache.teams.getAll();
	for (const t of teams) {
		if (t.disabled) {
			continue;
		}

		const userTeamAndActive =
			g.get("userTids").includes(t.tid) &&
			!local.autoPlayUntil &&
			!g.get("spectator");

		if (
			(userTeamAndActive && userOrOther === "user") ||
			(!userTeamAndActive && userOrOther === "other")
		) {
			await checkRosterSize(t.tid, userTeamAndActive);
		}

		if (userTeamSizeError) {
			break;
		}
	}

	if (releasedPIDs.length > 0) {
		await freeAgents.normalizeContractDemands({
			type: "dummyExpiringContracts",
			pids: releasedPIDs,
		});
	}

	return userTeamSizeError;
};

export default checkRosterSizes;
