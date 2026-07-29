import { gradientStyleFactory } from "../../util/gradientStyleFactory.ts";
import { local } from "../../util/local.ts";
import {
	exemptFromCoarseRatings,
	prospectRatingsSeason,
} from "../../../common/coarsenRating.ts";

const gradient = gradientStyleFactory(25, 45, 55, 75);

// Are the ratings on screen for this player coarse (floored to the tens digit)?
// Not simply "is coarse mode on": a draft class can be exempt from it, and
// colouring true 0-100 ratings on the 0-10 scale would paint every one of them
// bright green.
//
// The exemption is per SEASON as well as per player, so this has to be asked
// about the row being drawn: a player's draft-class year stays exact after he's
// drafted, while the seasons that follow are coarsened.
export const ratingsAreCoarse = (
	tid?: number,
	// The row's season and the player's draft year. Both are needed to tell a
	// prospect-era row from a later one; omitting them just falls back to the
	// per-player answer.
	season?: number,
	draftYear?: number,
): boolean => {
	const { hideRatingsOnesDigit, hideRatingsOnesDigitExceptProspects } =
		local.getState();
	return (
		hideRatingsOnesDigit &&
		!exemptFromCoarseRatings(tid, hideRatingsOnesDigitExceptProspects) &&
		!prospectRatingsSeason(
			draftYear,
			season,
			hideRatingsOnesDigitExceptProspects,
		)
	);
};

// Colors a rating on the usual 0-100 gradient. Coarse values are 0-10, so scale
// them back to the middle of their decile (5 -> 55) before coloring - otherwise
// every rating would fall below the low threshold and show up uniformly red.
export const ratingsGradientStyle = (rating: number, coarse?: boolean) => {
	const isCoarse = coarse ?? ratingsAreCoarse();
	return gradient(isCoarse ? rating * 10 + 5 : rating);
};
