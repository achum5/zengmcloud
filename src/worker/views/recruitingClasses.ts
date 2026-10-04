import { classPoints } from "../../common/college.ts";
import { PLAYER } from "../../common/constants.ts";
import type { Player, UpdateEvents } from "../../common/types.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";

// Team recruiting class rankings: this cycle's commitments, and last year's
// signed class.
const updateRecruitingClasses = async (
	inputs: unknown,
	updateEvents: UpdateEvents,
) => {
	if (
		!updateEvents.includes("firstRun") &&
		!updateEvents.includes("playerMovement") &&
		!updateEvents.includes("gameSim") &&
		!updateEvents.includes("newPhase")
	) {
		return;
	}
	if (!g.get("college")) {
		return { college: false as const };
	}

	const season = g.get("season");
	const teamInfoCache = g.get("teamInfoCache");

	const rank = (byTid: Map<number, number[]>) => {
		const rows = [...byTid].map(([tid, stars]) => ({
			tid,
			abbrev: teamInfoCache[tid]?.abbrev ?? "",
			region: teamInfoCache[tid]?.region ?? "",
			name: teamInfoCache[tid]?.name ?? "",
			count: stars.length,
			fiveStars: stars.filter((s) => s === 5).length,
			fourStars: stars.filter((s) => s === 4).length,
			threeStars: stars.filter((s) => s === 3).length,
			avgStars:
				stars.length > 0 ? stars.reduce((a, b) => a + b, 0) / stars.length : 0,
			points: Math.round(classPoints(stars) * 10) / 10,
		}));
		rows.sort((a, b) => b.points - a.points);
		return rows.map((row, i) => ({ ...row, rank: i + 1 }));
	};

	// This cycle: high schoolers committed so far.
	const current = new Map<number, number[]>();
	for (const p of await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.UNDRAFTED,
	)) {
		const tid = p.recruiting?.committed;
		if (p.draft.year === season && tid !== undefined) {
			current.set(tid, [...(current.get(tid) ?? []), p.recruiting!.stars]);
		}
	}

	// Last class: this season's freshmen who came out of high school.
	const last = new Map<number, number[]>();
	const rostered: Player[] = await idb.cache.players.indexGetAll(
		"playersByTid",
		[0, Infinity],
	);
	for (const p of rostered) {
		if (p.collegeYear0 === season && p.draft.year === season - 1) {
			last.set(p.tid, [...(last.get(p.tid) ?? []), p.collegeStars ?? 1]);
		}
	}

	return {
		college: true as const,
		season,
		current: rank(current),
		last: rank(last),
		userTid: g.get("userTid"),
	};
};

export default updateRecruitingClasses;
