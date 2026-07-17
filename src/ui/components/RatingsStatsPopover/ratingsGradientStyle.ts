import { gradientStyleFactory } from "../../util/gradientStyleFactory.ts";
import { local } from "../../util/local.ts";

const gradient = gradientStyleFactory(25, 45, 55, 75);

// Colors a rating on the usual 0-100 gradient. In "Coarse Ratings" mode the
// values passed in are floored to the tens digit (0-10), so scale them back to
// the middle of their decile (5 -> 55) before coloring — otherwise every rating
// would fall below the low threshold and show up uniformly red.
export const ratingsGradientStyle = (rating: number) => {
	const value = local.getState().hideRatingsOnesDigit ? rating * 10 + 5 : rating;
	return gradient(value);
};
