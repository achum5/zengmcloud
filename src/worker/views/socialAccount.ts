import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import {
	buildAccountDay,
	buildFeedDay,
	feedDaysForSeason,
	resolveFeedAccounts,
	type FeedPost,
} from "../util/socialFeed.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";

// How far back one account's page reaches in a single load. An account page
// has to walk days looking for that account's own posts, and most accounts
// post on a minority of nights, so this is a window rather than a history.
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

		const accounts = await resolveFeedAccounts();
		const account = accounts.find(
			(a) => a.handle.toLowerCase() === inputs.handle.toLowerCase(),
		);
		if (!account) {
			return {
				errorMessage: `There is no account called "${inputs.handle}" in this league.`,
			};
		}

		const days = (await feedDaysForSeason(season)).slice(0, DAYS_SCANNED);
		const posts: (FeedPost & { day: number })[] = [];
		for (const day of days) {
			const feed = await buildFeedDay({ season, day });
			// Everything of this account's that reached the timeline, plus the
			// replies it left under other people's posts.
			const fromFeed = new Set<string>();
			for (const post of feed.posts) {
				if (post.accountId === account.id) {
					fromFeed.add(post.id);
					posts.push({ ...post, day });
				}
				for (const reply of post.replies) {
					if (reply.accountId === account.id) {
						posts.push({
							...post,
							day,
							id: reply.id,
							accountId: reply.accountId,
							handle: reply.handle,
							name: reply.name,
							kind: reply.kind,
							tid: reply.tid,
							pid: reply.pid,
							text: reply.text,
							replies: [],
						});
					}
				}
			}

			// And what it posted that did not make the day's forty-five. The
			// timeline is a highlight reel; a profile is not, and a profile
			// that is empty because its owner lost a popularity contest is the
			// worst possible answer to "let me look through this account".
			for (const post of await buildAccountDay({
				season,
				day,
				accountId: account.id,
			})) {
				if (!fromFeed.has(post.id)) {
					posts.push({ ...post, day });
				}
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
