import { gradientStyleFactory } from "../../util/gradientStyleFactory.ts";
import { local } from "../../util/local.ts";
import { exemptFromCoarseRatings } from "../../../common/coarsenRating.ts";

const gradient = gradientStyleFactory(25, 45, 55, 75);

// Are the ratings on screen for this player coarse (floored to the tens digit)?
// Not simply "is coarse mode on": an undrafted prospect can be exempt from it,
// and colouring his true 0-100 ratings on the 0-10 scale would paint every one
// of them bright green.
export const ratingsAreCoarse = (tid?: number): boolean => {
	const { hideRatingsOnesDigit, hideRatingsOnesDigitExceptProspects } =
		local.getState();
	return (
		hideRatingsOnesDigit &&
		!exemptFromCoarseRatings(tid, hideRatingsOnesDigitExceptProspects)
	);
};

// Colors a rating on the usual 0-100 gradient. Coarse values are 0-10, so scale
// them back to the middle of their decile (5 -> 55) before coloring - otherwise
// every rating would fall below the low threshold and show up uniformly red.
export const ratingsGradientStyle = (rating: number, coarse?: boolean) => {
	const isCoarse = coarse ?? ratingsAreCoarse();
	return gradient(isCoarse ? rating * 10 + 5 : rating);
};
