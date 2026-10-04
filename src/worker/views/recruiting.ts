import {
	homeState,
	RECRUITING_HOURS_PER_WEEK,
	RECRUITING_MAX_HOURS,
	RECRUITING_VISITS,
} from "../../common/college.ts";
import type { UpdateEvents } from "../../common/types.ts";
import {
	getRecruits,
	getTeamCtxs,
	interestFor,
} from "../core/college/recruiting.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";

// The recruiting board: this year's high school class (and, after the season,
// the transfer portal), with your school's standing for each player and the
// schools leading for him.
const updateRecruiting = async (
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

	const userTid = g.get("userTid");
	const recruits = await getRecruits();
	const ctxs = await getTeamCtxs(recruits);
	const ctx = ctxs.get(userTid);
	const teamInfoCache = g.get("teamInfoCache");

	const players = await idb.getCopies.playersPlus(recruits, {
		attrs: ["pid", "firstName", "lastName", "age", "born", "hgt", "tid"],
		ratings: ["ovr", "pot", "pos", "skills"],
		season: g.get("season"),
		showNoStats: true,
		showRookies: true,
		fuzz: true,
	});
	const byPid = new Map(recruits.map((p) => [p.pid, p]));

	let hoursUsed = 0;
	let offers = 0;
	const rows = players.map((p) => {
		const full = byPid.get(p.pid)!;
		const rec = full.recruiting!;
		const hours = rec.hours[userTid] ?? 0;
		hoursUsed += hours;
		if (rec.offers[userTid] !== undefined) {
			offers += 1;
		}
		const top = Object.entries(rec.interest)
			.map(([tid, interest]) => ({ tid: Number(tid), interest }))
			.filter((row) => row.tid !== userTid || rec.offers[userTid] !== undefined)
			.sort((a, b) => b.interest - a.interest)
			.slice(0, 3)
			.map((row) => ({
				...row,
				abbrev: teamInfoCache[row.tid]?.abbrev ?? "",
				offered: rec.offers[row.tid] !== undefined,
			}));
		return {
			pid: p.pid,
			firstName: p.firstName,
			lastName: p.lastName,
			age: p.age,
			hgt: p.hgt,
			bornLoc: p.born.loc,
			state: homeState(p.born.loc),
			ratings: p.ratings,
			stars: rec.stars,
			rank: rec.rank,
			ask: rec.ask,
			interest: ctx ? Math.round(interestFor(full, ctx)) : 0,
			top,
			committed: rec.committed,
			committedAbbrev:
				rec.committed !== undefined
					? (teamInfoCache[rec.committed]?.abbrev ?? "")
					: undefined,
			offer: rec.offers[userTid],
			hours,
			visited: rec.visits.includes(userTid),
			portalFrom:
				rec.portalFrom !== undefined
					? (teamInfoCache[rec.portalFrom]?.abbrev ?? "")
					: undefined,
		};
	});

	rows.sort((a, b) => a.rank - b.rank);

	return {
		college: true as const,
		userTid,
		phase: g.get("phase"),
		season: g.get("season"),
		recruits: rows,
		team: {
			hoursUsed,
			hoursMax: RECRUITING_HOURS_PER_WEEK,
			maxHoursPerRecruit: RECRUITING_MAX_HOURS,
			visitsUsed: ctx?.visitsUsed ?? 0,
			visitsMax: RECRUITING_VISITS,
			open: ctx?.open ?? 0,
			offers,
			nilBudget: ctx?.nilBudget ?? 0,
			nilCommitted: ctx?.nilCommitted ?? 0,
			auto: ctx?.auto ?? false,
		},
	};
};

export default updateRecruiting;
