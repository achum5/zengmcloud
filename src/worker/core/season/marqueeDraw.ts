import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { ovrsIn } from "../game/primeTime.ts";
import type { MarqueeDraw } from "./marqueeNights.ts";
import { playoffHistory } from "./rivalries.ts";

// HOW MUCH EACH TEAM DRAWS, for the season's marquee nights (see
// marqueeNights.ts): how good it is as the season begins, its hype, its
// market, and last season's finals - the champion most, the team it beat
// next. And for Rivals Week, the user teams and the playoff series played
// (rivalries.ts).
export const marqueeDraw = async (season: number): Promise<MarqueeDraw> => {
	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	);
	const ovrs = await ovrsIn(season);
	const rank = (values: Map<number, number>) => {
		const order = [...values.entries()].sort((a, b) => a[1] - b[1]);
		return new Map(
			order.map(([tid], i) => [
				tid,
				order.length > 1 ? i / (order.length - 1) : 0.5,
			]),
		);
	};
	const ovrRank = rank(ovrs);
	const popRank = rank(new Map(teamSeasons.map((ts) => [ts.tid, ts.pop])));

	const last = await idb.merged.teamSeasons.getAll({ season: season - 1 });
	const mostRounds = Math.max(0, ...last.map((ts) => ts.playoffRoundsWon));
	const champion =
		mostRounds > 0
			? last.find((ts) => ts.playoffRoundsWon === mostRounds)?.tid
			: undefined;
	const finalists = new Set(
		mostRounds > 0
			? last
					.filter((ts) => ts.playoffRoundsWon >= mostRounds - 1)
					.map((ts) => ts.tid)
			: [],
	);

	const draw = new Map<number, number>();
	for (const ts of teamSeasons) {
		if (!ovrs.has(ts.tid)) {
			continue;
		}
		draw.set(
			ts.tid,
			0.45 * (ovrRank.get(ts.tid) ?? 0) +
				0.3 * ts.hype +
				0.25 * (popRank.get(ts.tid) ?? 0) +
				(ts.tid === champion ? 0.2 : finalists.has(ts.tid) ? 0.1 : 0),
		);
	}
	return {
		draw,
		champion,
		users: g.get("userTids"),
		// (A garnish: without it, rivalries are as the teams stand.)
		playoffs: await playoffHistory(season).catch(() => undefined),
	};
};
