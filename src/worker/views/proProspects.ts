import { collegeClassLabel } from "../../common/college.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { PHASE } from "../../common/constants.ts";
import { validateSeason } from "../util/processInputs.ts";
import {
	collegeDraftees,
	collegeLinkableLeagues,
} from "../core/college/handoff.ts";
import { g } from "../util/index.ts";

// Players who left for the draft, by season, and where they go next.
// College: players leave after the season, so before then show last year's.
const processInputs = (params: RouteParams<"proProspects">) => {
	const defaultSeason =
		g.get("phase") >= PHASE.DRAFT_LOTTERY
			? g.get("season")
			: g.get("season") - 1;
	return {
		season:
			params.season === undefined
				? defaultSeason
				: validateSeason(params.season),
	};
};

const updateProProspects = async ({
	inputs: { season },
	updateEvents,
	prevInputs,
}: ViewArgs<typeof processInputs>) => {
	if (
		!updateEvents.has("firstRun") &&
		!updateEvents.has("playerMovement") &&
		!updateEvents.has("newPhase") &&
		!updateEvents.has("gameAttributes") &&
		season === prevInputs?.season
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

export default defineView({
	id: "proProspects",
	processInputs,
	load: updateProProspects,
});
