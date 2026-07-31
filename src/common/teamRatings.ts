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
export const hideTeamOvr = (settings: {
	challengeNoRatings: boolean;
	hideTeamRatings: boolean;
}): boolean => settings.challengeNoRatings || settings.hideTeamRatings;

export const showTeamOvr = (settings: {
	challengeNoRatings: boolean;
	hideTeamRatings: boolean;
}): boolean => !hideTeamOvr(settings);
