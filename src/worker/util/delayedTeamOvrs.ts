import { idb } from "../db/index.ts";
import {
	teamOvrDisplay,
	teamOvrDisplayForSeason,
	type TeamOvrDisplay,
	type TeamRatingsSettings,
} from "../../common/teamRatings.ts";
import g from "./g.ts";

// The "Team Ratings Delay" setting, on the worker side: what each team's
// overall rating was N seasons ago.
//
// The number comes out of the RECORDED team-season history rather than being
// recomputed from whoever was on the roster back then. teamSeasons carries
// ovrEnd, written once when that season's playoffs began, and ovrStart, written
// at its first game. Recomputing instead would mean loading every player active
// in that season for every team on every page load, and it would answer a
// subtly different question anyway: what those players rate under today's
// formula, rather than what the team's rating actually was at the time.
//
// ovrEnd first because a finished season's closing rating is the honest summary
// of it; ovrStart is the fallback for a season still in progress, or an old one
// from before either was recorded.

// The league's own settings, read once so a view does not have to remember
// which attributes make up the rule.
const settings = (): TeamRatingsSettings => ({
	challengeNoRatings: g.get("challengeNoRatings"),
	hideTeamRatings: g.get("hideTeamRatings"),
	teamRatingsDelaySeasons: g.get("teamRatingsDelaySeasons"),
	season: g.get("season"),
});

export const getTeamOvrDisplay = (): TeamOvrDisplay =>
	teamOvrDisplay(settings());

export const getTeamOvrDisplayForSeason = (
	pageSeason: number,
): TeamOvrDisplay => teamOvrDisplayForSeason(settings(), pageSeason);

export const getDelayedTeamOvrs = async (
	season: number,
): Promise<Map<number, number>> => {
	const ovrs = new Map<number, number>();

	// A delay longer than the league is old points at a season nobody played, so
	// there is nothing to look up and every team simply has no rating to show.
	if (season < g.get("startingSeason")) {
		return ovrs;
	}

	const teamSeasons = await idb.getCopies.teamSeasons(
		{ season },
		"noCopyCache",
	);
	for (const teamSeason of teamSeasons) {
		const ovr = teamSeason.ovrEnd ?? teamSeason.ovrStart;
		if (typeof ovr === "number") {
			ovrs.set(teamSeason.tid, ovr);
		}
	}

	return ovrs;
};

// The common shape every view needs: the display mode plus, when it is
// "delayed", the ratings to substitute in. Views ask for this once and then
// look each team up.
export const getTeamOvrOverride = async (
	// A screen showing one season's ratings passes that season; one showing "the
	// team's rating" with no season attached leaves it off.
	pageSeason?: number,
): Promise<{
	display: TeamOvrDisplay;
	ovrs: Map<number, number>;
}> => {
	const display =
		pageSeason === undefined
			? getTeamOvrDisplay()
			: getTeamOvrDisplayForSeason(pageSeason);
	return {
		display,
		ovrs:
			display.type === "delayed"
				? await getDelayedTeamOvrs(display.season)
				: new Map(),
	};
};
