import RatingsStatsBaseball from "./RatingsStats.baseball.tsx";
import RatingsStatsBasketball from "./RatingsStats.basketball.tsx";
import RatingsStatsFootball from "./RatingsStats.football.tsx";
import RatingsStatsHockey from "./RatingsStats.hockey.tsx";
import { useLocal } from "../../util/local.ts";
import { bySport } from "../../../common/sportFunctions.ts";
import { exemptFromCoarseRatings } from "../../../common/coarsenRating.ts";

export const RatingsStats = (props: {
	ratings: any;
	stats: any;
	type?: "career" | "current" | "draft" | number;
	// The subject's current team, so an undrafted prospect exempted from coarse
	// ratings is coloured on the scale his numbers are actually on.
	tid?: number;
	// Whether these ratings actually came back coarsened. The producer knows -
	// the exemption is per SEASON as well as per player, so `tid` alone can't
	// tell you. Supplied wherever it's known; the tid rule is the fallback.
	coarseRatings?: boolean;
}) => {
	const {
		challengeNoRatings,
		hideRatingsOnesDigit,
		hideRatingsOnesDigitExceptProspects,
	} = useLocal([
		"challengeNoRatings",
		"hideRatingsOnesDigit",
		"hideRatingsOnesDigitExceptProspects",
	]);
	const coarseRatings =
		props.coarseRatings ??
		(hideRatingsOnesDigit &&
			!exemptFromCoarseRatings(props.tid, hideRatingsOnesDigitExceptProspects));

	return bySport({
		baseball: RatingsStatsBaseball({
			...props,
			challengeNoRatings,
			coarseRatings,
		}),
		basketball: RatingsStatsBasketball({
			...props,
			challengeNoRatings,
			coarseRatings,
		}),
		football: RatingsStatsFootball({
			...props,
			challengeNoRatings,
			coarseRatings,
		}),
		hockey: RatingsStatsHockey({
			...props,
			challengeNoRatings,
			coarseRatings,
		}),
	});
};
