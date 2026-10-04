import {
	collegeClassLabel,
	collegeTopPriorities,
} from "../../common/college.ts";
import { PHASE } from "../../common/constants.ts";
import type { UpdateEvents } from "../../common/types.ts";
import { seasonLine } from "../core/college/retention.ts";
import { getTeamCtxs } from "../core/college/teams.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";

// The retention period, after the season: your returning players' NIL asks
// and how likely each is to enter the transfer portal.
const updateRetention = async (inputs: unknown, updateEvents: UpdateEvents) => {
	if (
		!updateEvents.includes("firstRun") &&
		!updateEvents.includes("playerMovement") &&
		!updateEvents.includes("newPhase")
	) {
		return;
	}

	if (!g.get("college")) {
		return { college: false as const };
	}

	const userTid = g.get("userTid");
	const season = g.get("season");
	const roster = (
		await idb.cache.players.indexGetAll("playersByTid", userTid)
	).filter((p) => p.collegeRetention?.season === season);
	const ctx = (await getTeamCtxs([])).get(userTid);

	const players = roster
		.sort((a, b) => b.value - a.value)
		.map((p) => {
			const r = p.collegeRetention!;
			const ratings = p.ratings.at(-1)!;
			const promise = (p.collegePromises ?? []).find(
				(row) => row.season === season + 1 && row.tid === userTid,
			);
			return {
				pid: p.pid,
				firstName: p.firstName,
				lastName: p.lastName,
				pos: ratings.pos,
				skills: ratings.skills,
				ovr: ratings.ovr,
				pot: ratings.pot,
				// His class next season.
				classLabel: collegeClassLabel(p, season + 1),
				mpg: Math.round(seasonLine(p, season).mpg * 10) / 10,
				priorities: p.collegeProfile
					? collegeTopPriorities(p.collegeProfile)
					: [],
				nil: p.contract.amount,
				demandRange: r.demandRange,
				settled: r.settled ?? false,
				counter: r.talks?.counter,
				patience: r.talks?.patience,
				walked: r.talks?.walked ?? false,
				risk: r.risk,
				reasons: r.reasons,
				promise,
			};
		});

	return {
		college: true as const,
		open: g.get("phase") === PHASE.RESIGN_PLAYERS,
		season,
		players,
		nilBudget: ctx?.nilBudget ?? 0,
		nilCommitted: ctx?.nilCommitted ?? 0,
	};
};

export default updateRetention;
