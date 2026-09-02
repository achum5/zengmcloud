import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import {
	buildAccountDay,
	getFeedSnapshot,
	type FeedPost,
} from "../util/socialFeed.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";

// How far back one account's page reaches in a single load. Most accounts post
// on a minority of nights, so this is a window rather than a history.
const DAYS_SCANNED = 30;

const updateSocialAccount = async (
	inputs: ViewInput<"socialAccount">,
	updateEvents: UpdateEvents,
	state: any,
) => {
	const season = g.get("season");
	if (
		updateEvents.includes("firstRun") ||
		updateEvents.includes("gameSim") ||
		state.handle !== inputs.handle
	) {
		if (!g.get("socialFeed")) {
			// The feed is opt-in, and a direct link should say so rather than
			// render an empty page or a stale one.
			return {
				errorMessage:
					"The League Feed is turned off for this league. Turn it on in League Settings under UI.",
			};
		}

		const snapshot = await getFeedSnapshot(season);
		const account = snapshot.accounts.find(
			(a) => a.handle.toLowerCase() === inputs.handle.toLowerCase(),
		);
		if (!account) {
			return {
				errorMessage: `There is no account called "${inputs.handle}" in this league.`,
			};
		}

		// A profile is the account's own posts - what it said, whether or not
		// the day's timeline had room for it - newest first. Replies live under
		// the posts they answer, on the feed.
		const posts: (FeedPost & { day: number })[] = [];
		for (
			let dayIndex = snapshot.days.length - 1;
			dayIndex >= 0 && dayIndex >= snapshot.days.length - DAYS_SCANNED;
			dayIndex--
		) {
			const day = snapshot.days[dayIndex]!;
			for (const post of await buildAccountDay({
				snapshot,
				account,
				dayIndex,
			})) {
				posts.push({ ...post, day });
			}
		}

		const team =
			account.tid !== undefined && account.tid >= 0
				? await idb.cache.teams.get(account.tid)
				: undefined;

		return {
			account: {
				id: account.id,
				handle: account.handle,
				name: account.name,
				bio: account.bio,
				kind: account.kind,
				tid: account.tid,
				pid: account.pid,
				archetypeId: account.archetypeId,
				avatarUrl: account.avatarUrl,
				coverUrl: account.coverUrl,
				implicit: account.implicit,
				tone: account.personality.tone,
			},
			team: team
				? {
						tid: team.tid,
						abbrev: team.abbrev,
						region: team.region,
						name: team.name,
						imgURL: team.imgURL,
						colors: team.colors,
					}
				: undefined,
			posts,
			season,
			userTid: g.get("userTid"),
		};
	}
};

export default updateSocialAccount;
