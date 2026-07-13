import { g } from "../util/index.ts";
import { idb } from "../db/index.ts";
import type { UpdateEvents } from "../../common/types.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
	type TradeTier,
} from "../core/trade/tradePosture.ts";

// A one-line summary of what a team is shopping FOR, from its posture.
const seekingText = (
	tier: TradeTier,
	starGap: boolean,
	needs: { pos: string }[],
): string => {
	if (tier === "seller" || tier === "teardown") {
		return "Youth + draft picks";
	}
	if (tier === "allIn" && starGap) {
		return "A star (any position)";
	}
	const positions = needs.slice(0, 2).map((n) => n.pos);
	if (positions.length === 0) {
		return "Depth / opportunistic";
	}
	return `Starter at ${positions.join(" / ")}`;
};

// Read-only "Franchise Outlook": every team's trade posture (buy/sell tier,
// positional needs, star gap, who they're shopping, cap posture). This does not
// drive any trades yet - it's a window into the strategy engine so its
// classifications can be eyeballed against a real league.
const updateFranchiseOutlook = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		updateEvents.includes("newPhase") ||
		updateEvents.includes("playerMovement") ||
		updateEvents.includes("g.userTids")
	) {
		const season = g.get("season");
		const context = await getLeagueTradeContext();

		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);

		// pid → readable player info, for turning posture pid lists into names.
		const allPlayers = await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		]);
		const playerById = new Map(allPlayers.map((p) => [p.pid, p]));
		const nameOf = (pid: number) => {
			const p = playerById.get(pid);
			return p ? `${p.firstName} ${p.lastName}` : "?";
		};
		const infoOf = (pid: number) => {
			const p = playerById.get(pid);
			if (!p) {
				return undefined;
			}
			const ratings = p.ratings.at(-1);
			return {
				pid,
				name: `${p.firstName} ${p.lastName}`,
				ovr: ratings?.ovr ?? 0,
				age: season - p.born.year,
			};
		};

		const rows = [];
		for (const t of teams) {
			const posture = await getTradePosture(t.tid, context);

			const teamSeason = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsBySeasonTid",
				[season, t.tid],
			);

			rows.push({
				tid: t.tid,
				region: t.region,
				name: t.name,
				abbrev: t.abbrev,
				imgURL: t.imgURL,
				imgURLSmall: t.imgURLSmall,
				tier: posture.tier,
				aggression: posture.aggression,
				ovrRank: posture.ovrRank,
				avgAge: posture.avgAge,
				starGap: posture.starGap,
				won: teamSeason?.won ?? 0,
				lost: teamSeason?.lost ?? 0,
				needs: posture.needs,
				surpluses: posture.surpluses,
				seeking: seekingText(posture.tier, posture.starGap, posture.needs),
				// The veterans this team should move before they waste away.
				shopping: posture.shopVeteranPids
					.map(infoOf)
					.filter((x): x is NonNullable<typeof x> => x !== undefined)
					.slice(0, 5),
				buildingBlocks: posture.buildingBlockPids.map(nameOf).slice(0, 5),
				buildingBlockCount: posture.buildingBlockPids.length,
				cap: {
					payroll: posture.cap.payroll,
					overLuxury: posture.cap.overLuxury,
					wantsRelief: posture.cap.wantsRelief,
					canAbsorb: posture.cap.canAbsorb,
				},
			});
		}

		// Best teams first, so it reads like a power ranking of intentions.
		rows.sort((a, b) => a.ovrRank - b.ovrRank);

		return {
			teams: rows,
			userTid: g.get("userTid"),
			userTids: g.get("userTids"),
			season,
			salaryCap: g.get("salaryCap"),
			luxuryPayroll: g.get("luxuryPayroll"),
		};
	}
};

export default updateFranchiseOutlook;
