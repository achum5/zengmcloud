// TWO settings hide a team's overall rating, and every screen that shows one
// has to honour both:
//
//   challengeNoRatings - "No Visible Player Ratings". A team rating is just its
//                        players', so hiding theirs hides it too.
//   hideTeamRatings    - "No Visible Team Ratings", which hides every team's
//                        overall rating and nothing else.
//
// That pair was spelled out separately at each call site, which is how
// Frivolities > Team Seasons came to check only the first and kept printing
// team overalls in a league running the second. One named rule instead, so a
// new screen can't get half of it.
//
// A THIRD setting then softens the hiding rather than adding to it:
//
//   teamRatingsDelaySeasons - show each team's rating as it was N seasons ago
//                             instead of hiding it. In 2007 with a delay of 5
//                             you see the 2002 ratings.
//
// The delay is scouting-with-old-information, not a second way to hide: it only
// does anything when the rating WOULD have been hidden, and what it reveals is
// a historical number that leaks nothing about the present.

export const hideTeamOvr = (settings: {
	challengeNoRatings: boolean;
	hideTeamRatings: boolean;
}): boolean => settings.challengeNoRatings || settings.hideTeamRatings;

export const showTeamOvr = (settings: {
	challengeNoRatings: boolean;
	hideTeamRatings: boolean;
}): boolean => !hideTeamOvr(settings);

export type TeamOvrDisplay =
	// Show today's rating.
	| { type: "current" }
	// Show the rating from `season`, which is in the past. Screens must SAY so -
	// an unlabelled old number is worse than no number, because it reads as the
	// current one.
	| { type: "delayed"; season: number }
	// Show nothing.
	| { type: "hidden" };

export type TeamRatingsSettings = {
	challengeNoRatings: boolean;
	hideTeamRatings: boolean;
	// Seasons of delay. 0 (or missing, or negative) means no delay, so a hidden
	// rating stays hidden - the behaviour before this setting existed.
	teamRatingsDelaySeasons?: number;
	// The league's current season, which the delay counts back from.
	season: number;
};

export const teamOvrDisplay = (
	settings: TeamRatingsSettings,
): TeamOvrDisplay => {
	if (!hideTeamOvr(settings)) {
		return { type: "current" };
	}

	// Fractions and junk would produce a season that no team-season row exists
	// for, which reads on screen as "this team has no rating" rather than as a
	// bad setting. Floor it and require at least one whole season of delay.
	const delay = Math.floor(settings.teamRatingsDelaySeasons ?? 0);
	if (!Number.isFinite(delay) || delay < 1) {
		return { type: "hidden" };
	}

	return { type: "delayed", season: settings.season - delay };
};

// For a screen that shows ONE season's ratings - Power Rankings, a roster as of
// a season. A season's ratings become knowable once that season is old enough.
//
// The page for the CURRENT season has no knowable rating of its own, so it
// falls back to the newest one there is, which is the whole point of the
// setting. A page for a season in between - too old to be current, too new to
// be unlocked - shows nothing: its own answer exists and just isn't available
// yet, and putting a different season's number there would only be wrong.
export const teamOvrDisplayForSeason = (
	settings: TeamRatingsSettings,
	pageSeason: number,
): TeamOvrDisplay => {
	const display = teamOvrDisplay(settings);
	if (display.type !== "delayed") {
		return display;
	}
	if (pageSeason <= display.season) {
		return { type: "current" };
	}
	if (pageSeason >= settings.season) {
		return display;
	}
	return { type: "hidden" };
};

// For a table with a row per historical season (Frivolities > Team Seasons):
// which rows may show their rating. A delay of 5 in 2007 reveals 2002 and
// everything before it, and keeps 2003 onward covered - the same rule as the
// single-number case, applied per row.
export const teamOvrVisibleForSeason = (
	settings: TeamRatingsSettings,
	rowSeason: number,
): boolean => {
	const display = teamOvrDisplay(settings);
	if (display.type === "current") {
		return true;
	}
	if (display.type === "hidden") {
		return false;
	}
	return rowSeason <= display.season;
};

// How a delayed rating is labelled, everywhere it is shown. One string so the
// screens cannot drift apart on what the number means.
export const delayedTeamOvrNote = (season: number): string =>
	`${season} rating`;
