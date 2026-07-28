import { g } from "../util/index.ts";
import { idb } from "../db/index.ts";
import { player } from "../core/index.ts";
import { coarsenRating } from "../../common/coarsenRating.ts";
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
	targetPos: string | undefined,
): string => {
	if (tier === "seller" || tier === "teardown") {
		return "Youth + draft picks";
	}
	if (tier === "allIn" && starGap) {
		return "A star (any position)";
	}
	if (needs.length > 0) {
		return `Starter at ${needs
			.slice(0, 2)
			.map((n) => n.pos)
			.join(" / ")}`;
	}
	if (targetPos) {
		return `Upgrade at ${targetPos}`;
	}
	// No hole and no soft spot: a contender/buyer just wants the best upgrade.
	if (tier === "allIn" || tier === "buyer") {
		return "Best player available";
	}
	return "Depth / opportunistic";
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
		updateEvents.includes("gameAttributes")
	) {
		const season = g.get("season");
		const hideRatings = g.get("challengeNoRatings");
		const coarse = g.get("hideRatingsOnesDigit");
		const context = await getLeagueTradeContext();

		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);

		// pid → readable player info, for turning posture pid lists into names.
		const allPlayers = await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		]);
		const playerById = new Map(allPlayers.map((p) => [p.pid, p]));
		// pid → full player info (with value), for the diagnostic dump.
		const infoOf = (pid: number) => {
			const p = playerById.get(pid);
			if (!p) {
				return undefined;
			}
			const ratings = p.ratings.at(-1);
			// This page reads players straight out of the cache rather than through
			// playersPlus, so nothing has fuzzed or rounded these yet. Do both here
			// or the diagnostics window becomes a hole in every ratings setting the
			// league has turned on.
			const show = (value: number | undefined) => {
				if (value === undefined || hideRatings) {
					return undefined;
				}
				const fuzzed = player.fuzzRating(value, ratings?.fuzz ?? 0);
				return coarse ? coarsenRating(fuzzed) : fuzzed;
			};
			return {
				pid,
				name: `${p.firstName} ${p.lastName}`,
				ovr: show(ratings?.ovr),
				pot: show(ratings?.pot),
				pos: ratings?.pos ?? "?",
				age: season - p.born.year,
				// Value is talent on the same 0-100 scale and is never fuzzed, so
				// left alone it hands back what the ratings above just hid.
				value: hideRatings
					? undefined
					: coarse
						? coarsenRating(p.value)
						: Math.round(p.value * 10) / 10,
				contract: Math.round(p.contract.amount),
				exp: p.contract.exp,
				// Ordering only - never rendered, so it stays exact.
				sortValue: p.value,
			};
		};
		const resolve = (pids: number[], limit: number) =>
			pids
				.map(infoOf)
				.filter((x): x is NonNullable<typeof x> => x !== undefined)
				.slice(0, limit);

		const rows = [];
		for (const t of teams) {
			const posture = await getTradePosture(t.tid, context);

			const teamSeason = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsBySeasonTid",
				[season, t.tid],
			);

			// The team's best player by value, for judging star gap at a glance.
			const roster = resolve(
				(allPlayers.filter((p) => p.tid === t.tid) ?? []).map((p) => p.pid),
				999,
			).sort((a, b) => b.sortValue - a.sortValue);

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
				ovrRankPct: posture.ovrRankPct,
				contention: posture.contention,
				winp: posture.winp,
				avgAge: posture.avgAge,
				youngCoreCount: posture.youngCoreCount,
				starGap: posture.starGap,
				targetPos: posture.targetPos,
				won: teamSeason?.won ?? 0,
				lost: teamSeason?.lost ?? 0,
				needs: posture.needs,
				surpluses: posture.surpluses,
				seeking: seekingText(
					posture.tier,
					posture.starGap,
					posture.needs,
					posture.targetPos,
				),
				topPlayer: roster[0],
				// The veterans this team should move before they waste away.
				shopping: resolve(posture.shopVeteranPids, 8),
				buildingBlocks: resolve(posture.buildingBlockPids, 12),
				buildingBlockCount: posture.buildingBlockPids.length,
				cap: {
					payroll: posture.cap.payroll,
					capSpace: posture.cap.capSpace,
					overCap: posture.cap.overCap,
					overLuxury: posture.cap.overLuxury,
					underFloor: posture.cap.underFloor,
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
			// League-wide reference points the postures were scored against.
			context: {
				numActiveTeams: context.numActiveTeams,
				starterOvr: context.starterOvr,
				rotationOvr: context.rotationOvr,
				starValue: Math.round(context.starValue * 10) / 10,
				coreValue: Math.round(context.coreValue * 10) / 10,
				minPayroll: context.minPayroll,
			},
		};
	}
};

export default updateFranchiseOutlook;
