import {
	collegeTopPriorities,
	homeState,
	RECRUITING_HOURS_PER_WEEK,
	RECRUITING_MAX_HOURS,
	RECRUITING_VISITS,
	scoutedRange,
	scoutingProgress,
} from "../../common/college.ts";
import { PHASE } from "../../common/constants.ts";
import { getRecruits } from "../core/college/recruiting.ts";
import { getTeamCtxs, interestFor } from "../core/college/teams.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";

// The recruiting board: this year's high school class (and, during the
// offseason weeks, the transfer portal), as your school sees them - ratings
// only as sharp as your scouting - with your standing for each player and
// the schools leading for him.
const updateRecruiting = async ({ updateEvents }: ViewArgs) => {
	if (
		!updateEvents.has("firstRun") &&
		!updateEvents.has("playerMovement") &&
		!updateEvents.has("gameSim") &&
		!updateEvents.has("newPhase")
	) {
		return;
	}

	if (!g.get("college")) {
		return { college: false as const };
	}

	const userTid = g.get("userTid");
	const season = g.get("season");
	const recruits = await getRecruits();
	const ctxs = await getTeamCtxs(recruits);
	const ctx = ctxs.get(userTid);
	const teamInfoCache = g.get("teamInfoCache");

	const players = await idb.getCopies.playersPlus(recruits, {
		attrs: ["pid", "firstName", "lastName", "age", "born", "hgt", "tid"],
		ratings: ["ovr", "pot", "pos", "skills"],
		season,
		showNoStats: true,
		showRookies: true,
		fuzz: false,
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
		const progress = scoutingProgress(rec, userTid);
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
		const talks = rec.talks[userTid];
		return {
			pid: p.pid,
			firstName: p.firstName,
			lastName: p.lastName,
			age: p.age,
			hgt: p.hgt,
			bornLoc: p.born.loc,
			state: homeState(p.born.loc),
			pos: p.ratings.pos as string,
			skills: progress >= 0.5 ? (p.ratings.skills as string[]) : [],
			ovr: scoutedRange(p.ratings.ovr, rec.fuzz, progress),
			pot: scoutedRange(p.ratings.pot, rec.fuzz, progress),
			scouted: Math.round(progress * 100),
			priorities: full.collegeProfile
				? collegeTopPriorities(full.collegeProfile)
				: [],
			stars: rec.stars,
			rank: rec.rank,
			askRange: rec.askRange,
			interest: ctx ? Math.round(interestFor(full, ctx)) : 0,
			top,
			committed: rec.committed,
			committedAbbrev:
				rec.committed !== undefined
					? (teamInfoCache[rec.committed]?.abbrev ?? "")
					: undefined,
			offer: rec.offers[userTid],
			promises: rec.promises[userTid] ?? [],
			counter: talks?.counter,
			patience: talks?.patience,
			walked: talks?.walked ?? false,
			hours,
			visited: rec.visits.includes(userTid),
			portalFrom:
				rec.portalFrom !== undefined
					? (teamInfoCache[rec.portalFrom]?.abbrev ?? "")
					: undefined,
		};
	});

	rows.sort((a, b) =>
		a.portalFrom !== undefined && b.portalFrom === undefined
			? -1
			: a.portalFrom === undefined && b.portalFrom !== undefined
				? 1
				: a.rank - b.rank,
	);

	return {
		college: true as const,
		userTid,
		phase: g.get("phase"),
		portalOpen: g.get("phase") === PHASE.FREE_AGENCY,
		season,
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
			rep: ctx?.rep ?? 0.8,
		},
	};
};

export default defineView({
	id: "recruiting",
	load: updateRecruiting,
});
