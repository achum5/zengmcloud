import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import {
	buildFeedDay,
	getFeedSnapshot,
	picturesFor,
	playerNamesFor,
	suggestedAccounts,
	type FeedDay,
} from "../util/socialFeed.ts";
import { isVerified } from "../../common/socialMetrics.ts";
import { defineView, type ViewArgs } from "../util/defineView.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { validateSeason } from "../util/processInputs.ts";

// How many days of the timeline to build at once. A feed is scrolled, not
// paged, but each day is real work (the roster is resolved and every account
// is scored against every event), so this is the batch a scroll asks for.
const DAYS_PER_PAGE = 4;

const processInputs = (params: RouteParams<"socialFeed">) => ({
	season: validateSeason(params.season),
	// How far back the timeline has been scrolled, in days. Carried in the URL
	// so a reload lands where the reader was rather than at the top.
	days: params.days === undefined ? undefined : Number.parseInt(params.days),
});

const updateSocialFeed = async ({
	inputs,
	updateEvents,
	prevInputs,
}: ViewArgs<typeof processInputs>) => {
	const season = inputs.season ?? g.get("season");
	if (
		updateEvents.has("firstRun") ||
		updateEvents.has("gameSim") ||
		updateEvents.has("newPhase") ||
		prevInputs?.season !== season ||
		prevInputs?.days !== inputs.days
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
		const newestFirst = [...snapshot.days].reverse();
		const wanted = newestFirst.slice(0, inputs.days ?? DAYS_PER_PAGE);

		const feed: FeedDay[] = [];
		for (const day of wanted) {
			feed.push(
				await buildFeedDay({
					snapshot,
					dayIndex: snapshot.days.indexOf(day),
				}),
			);
		}

		// Only the accounts actually on the page: a face config is a kilobyte
		// of JSON and the roster is five hundred players.
		const onPage = new Set<string>();
		for (const day of feed) {
			for (const post of day.posts) {
				onPage.add(post.accountId);
				for (const reply of post.replies) {
					onPage.add(reply.accountId);
				}
			}
		}
		const suggestedRaw = suggestedAccounts(snapshot, g.get("userTid"));
		for (const a of suggestedRaw) {
			onPage.add(a.id);
		}
		const pictures = await picturesFor(
			snapshot,
			snapshot.accounts.filter((a) => onPage.has(a.id)),
		);
		const suggested = suggestedRaw.map((a) => ({
			accountId: a.id,
			handle: a.handle,
			name: a.name,
			kind: a.kind,
			archetypeId: a.archetypeId,
			tid: a.tid,
			pid: a.pid,
			avatarUrl: a.avatarUrl,
			verified: isVerified(a),
		}));

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
			playerNames: playerNamesFor(
				snapshot,
				feed.flatMap((day: any) => day.posts),
			),
			season,
			days: inputs.days ?? DAYS_PER_PAGE,
			hasMore: newestFirst.length > wanted.length,
			accountCount: snapshot.accounts.length,
			pictures,
			suggested,
			teams,
			userTid: g.get("userTid"),
		};
	}
};

export default defineView({
	id: "socialFeed",
	processInputs,
	load: updateSocialFeed,
});
