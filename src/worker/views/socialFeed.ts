import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import {
	buildFeedDay,
	feedDaysForSeason,
	resolveFeedAccounts,
	type FeedDay,
} from "../util/socialFeed.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";

// How many days of the timeline to build at once. A feed is scrolled, not
// paged, but each day is real work (the roster is resolved and every account
// is scored against every event), so this is the batch a scroll asks for.
const DAYS_PER_PAGE = 4;

const updateSocialFeed = async (
	inputs: ViewInput<"socialFeed">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	const season = inputs.season ?? g.get("season");
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		updateEvents.includes("newPhase") ||
		state.season !== season ||
		state.days !== inputs.days
	) {
		if (!g.get("socialFeed")) {
			// The feed is opt-in, and a direct link should say so rather than
			// render an empty page or a stale one.
			return {
				errorMessage:
					"The League Feed is turned off for this league. Turn it on in League Settings under UI.",
			};
		}

		const allDays = await feedDaysForSeason(season);
		const wanted = allDays.slice(0, inputs.days ?? DAYS_PER_PAGE);

		const feed: FeedDay[] = [];
		for (const day of wanted) {
			feed.push(await buildFeedDay({ season, day }));
		}

		const teams = (await idb.cache.teams.getAll()).map((t) => ({
			tid: t.tid,
			abbrev: t.abbrev,
			region: t.region,
			name: t.name,
			imgURL: t.imgURL,
			imgURLSmall: t.imgURLSmall,
			colors: t.colors,
		}));

		return {
			feed,
			season,
			days: inputs.days ?? DAYS_PER_PAGE,
			hasMore: allDays.length > wanted.length,
			accountCount: (await resolveFeedAccounts()).length,
			teams,
			userTid: g.get("userTid"),
		};
	}
};

export default updateSocialFeed;
