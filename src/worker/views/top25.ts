import type { UpdateEvents } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";

// The weekly Top 25, with each team's movement since last week.
const updateTop25 = async (inputs: unknown, updateEvents: UpdateEvents) => {
	if (
		!updateEvents.includes("firstRun") &&
		!updateEvents.includes("gameSim") &&
		!updateEvents.includes("newPhase")
	) {
		return;
	}
	if (!g.get("college")) {
		return { college: false as const };
	}

	const season = g.get("season");
	const polls = g.get("collegePolls");
	const weeks = polls?.season === season ? polls.weeks : [];
	const current = weeks.at(-1) ?? [];
	const previous = weeks.length > 1 ? weeks.at(-2)! : undefined;
	const teamInfoCache = g.get("teamInfoCache");
	const confs = g.get("confs", "current");

	const rows = [];
	for (const [i, tid] of current.entries()) {
		const ts = await idb.cache.teamSeasons.indexGet("teamSeasonsByTidSeason", [
			tid,
			season,
		]);
		const prevIndex = previous ? previous.indexOf(tid) : -1;
		const t = await idb.cache.teams.get(tid);
		rows.push({
			rank: i + 1,
			tid,
			abbrev: teamInfoCache[tid]?.abbrev ?? "",
			region: teamInfoCache[tid]?.region ?? "",
			name: teamInfoCache[tid]?.name ?? "",
			conf: confs.find((c) => c.cid === t?.cid)?.abbrev ?? "",
			won: ts?.won ?? 0,
			lost: ts?.lost ?? 0,
			prevRank: previous ? (prevIndex >= 0 ? prevIndex + 1 : null) : undefined,
		});
	}

	return {
		college: true as const,
		season,
		week: weeks.length - 1,
		rows,
		userTid: g.get("userTid"),
	};
};

export default updateTop25;
