import { collegeClassLabel } from "../../common/college.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";
import {
	collegeDraftees,
	collegeLinkableLeagues,
} from "../core/college/handoff.ts";
import { g } from "../util/index.ts";

// Players who left for the draft, by season, and where they go next.
const updateProProspects = async (
	{ season }: ViewInput<"proProspects">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	if (
		!updateEvents.includes("firstRun") &&
		!updateEvents.includes("playerMovement") &&
		!updateEvents.includes("newPhase") &&
		!updateEvents.includes("gameAttributes") &&
		season === state.season
	) {
		return;
	}
	if (!g.get("college")) {
		return { college: false as const };
	}

	const teamInfoCache = g.get("teamInfoCache");
	const draftees = await collegeDraftees(season);
	const players = draftees.map((p) => {
		const r = p.ratings.at(-1)!;
		const tid = p.stats.at(-1)?.tid;
		return {
			pid: p.pid,
			firstName: p.firstName,
			lastName: p.lastName,
			age: season - p.born.year,
			pos: r.pos,
			ovr: r.ovr,
			pot: r.pot,
			skills: r.skills,
			classLabel: collegeClassLabel(p, season),
			abbrev: tid !== undefined ? (teamInfoCache[tid]?.abbrev ?? "") : "",
			tid,
		};
	});

	const linkedLid = g.get("collegeLinkedLid");
	return {
		college: true as const,
		season,
		currentSeason: g.get("season"),
		startingSeason: g.get("startingSeason"),
		players,
		leagues: await collegeLinkableLeagues(),
		linkedLid,
	};
};

export default updateProProspects;
