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

// WHEN A POWER RANKING IS NOTHING BUT THE TEAM RATINGS IN ORDER.
//
// A power ranking combines recent performance, margin of victory and team
// rating. Before any games are played the first two are empty, so all that is
// left is the ratings, sorted - which in a league that hides team ratings is
// the entire secret, published as a list. The Power Rankings page has always
// closed itself in that state; the draft-pick tables print the same rank in a
// column and did not, so a preseason league with "No Visible Team Ratings" on
// could read every team's standing straight off the Draft Picks page.
//
// One rule, so the two screens cannot drift apart again. A delay is not
// covered: it means the owner opted into seeing ratings from N seasons ago, and
// the page that does that says outright that the rankings still use today's
// rosters.
export const powerRankingIsJustTeamOvr = ({
	display,
	noGamesYet,
}: {
	display: TeamOvrDisplay;
	noGamesYet: boolean;
}): boolean => display.type === "hidden" && noGamesYet;

// HOW MUCH A TRADE MOVES A TEAM, WITHOUT HANDING BACK THE RATINGS.
//
// With team ratings hidden, the trade screens already showed the CHANGE rather
// than the resulting rating - you should be able to tell whether a deal helps
// you without knowing how good anyone is. But an EXACT change is a measuring
// instrument. Offer one player at a time and read the delta off the screen and
// you can solve for that player's rating to within a point or two, which is
// precisely the number the setting exists to hide. A league found this and
// reported it: swap in one guard, team goes +13; swap in another, +11; a third,
// +5. That orders and near-enough prices three players nobody was supposed to
// be able to rate.
//
// So the change is reported in bands of five, as a run of + or - signs. It
// still answers the question the number was there for - is this a small
// upgrade or a huge one - and it stops being an oracle: every rating inside a
// five-point window now reads identically.
export const TEAM_OVR_DELTA_BAND = 5;

// Five bands, so a genuinely enormous swing still reads as bigger than a large
// one rather than saturating early.
export const MAX_TEAM_OVR_DELTA_SYMBOLS = 5;

// "+++" for +11 to +15, "--" for -6 to -10, "0" for no change.
export const teamOvrDeltaSymbols = (diff: number): string => {
	if (!Number.isFinite(diff)) {
		return "0";
	}
	// team.ovr is already rounded, so this only ever matters for an imported
	// league or a God Mode edit - and rounding first is what keeps a half-point
	// from being promoted to a full band.
	const rounded = Math.round(diff);
	if (rounded === 0) {
		return "0";
	}
	const bands = Math.min(
		MAX_TEAM_OVR_DELTA_SYMBOLS,
		Math.ceil(Math.abs(rounded) / TEAM_OVR_DELTA_BAND),
	);
	return (rounded > 0 ? "+" : "-").repeat(bands);
};

// What a run of signs stands for, for the tooltip. Naming the band is not a
// leak - the band IS what is being disclosed - and without it the reader has to
// guess whether "++" is twice "+" or the second of five.
export const teamOvrDeltaBandLabel = (diff: number): string => {
	const symbols = teamOvrDeltaSymbols(diff);
	if (symbols === "0") {
		return "No change";
	}
	const sign = symbols[0]!;
	const bands = symbols.length;
	if (bands === MAX_TEAM_OVR_DELTA_SYMBOLS) {
		return `${sign}${(bands - 1) * TEAM_OVR_DELTA_BAND + 1} or more`;
	}
	const low = (bands - 1) * TEAM_OVR_DELTA_BAND + 1;
	return `${sign}${low} to ${sign}${bands * TEAM_OVR_DELTA_BAND}`;
};

// How a delayed rating is labelled, everywhere it is shown. One string so the
// screens cannot drift apart on what the number means.
export const delayedTeamOvrNote = (season: number): string =>
	`${season} rating`;
