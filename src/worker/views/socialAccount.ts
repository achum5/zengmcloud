import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import {
	buildAccountDay,
	buildFeedDay,
	getFeedSnapshot,
	picturesFor,
	suggestedAccounts,
	type FeedPost,
} from "../util/socialFeed.ts";
import {
	formatReach,
	isVerified,
	reachOf,
} from "../../common/socialMetrics.ts";
import { hashSeed, rngFromSeed } from "../../common/phrasePool.ts";
import type { UpdateEvents, ViewInput } from "../../common/types.ts";

// How far back one account's page reaches in a single load. Most accounts post
// on a minority of nights, so this is a window rather than a history.
const DAYS_SCANNED = 30;
// Replies live on the day's timeline rather than on the account, and a day's
// timeline is the expensive thing to build, so the replies tab reaches back
// a week rather than a month.
const REPLY_DAYS_SCANNED = 7;

// How many accounts this one follows. Derived and stored nowhere, like every
// other number on a profile: a stable draw per account, sized by what kind
// of account it is - a wire service follows everyone, a fan follows a few
// hundred, a franchise follows its own people.
const followingOf = (account: {
	id: string;
	kind: string;
	archetypeId: string;
}): number => {
	const rng = rngFromSeed(hashSeed(`following|${account.id}`));
	rng();
	const base =
		account.kind === "team"
			? 120
			: account.kind === "player"
				? 350
				: account.archetypeId === "aggregator"
					? 4000
					: account.archetypeId === "homerFan" ||
						  account.archetypeId === "doomerFan" ||
						  account.archetypeId === "casualFan" ||
						  account.archetypeId === "troll"
						? 600
						: 1500;
	return Math.round(base * (0.5 + rng() * 1.2));
};

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

		// The replies it left under other people's posts, with the post each
		// one answers, so the tab reads as a conversation rather than a list
		// of one-liners without their context.
		const replies: (FeedPost["replies"][number] & {
			day: number;
			parent: Omit<FeedPost, "replies">;
		})[] = [];
		for (
			let dayIndex = snapshot.days.length - 1;
			dayIndex >= 0 && dayIndex >= snapshot.days.length - REPLY_DAYS_SCANNED;
			dayIndex--
		) {
			const feedDay = await buildFeedDay({ snapshot, dayIndex });
			for (const post of feedDay.posts) {
				for (const reply of post.replies) {
					if (reply.accountId === account.id) {
						const { replies: _drop, ...parent } = post;
						replies.push({ ...reply, day: feedDay.day, parent });
					}
				}
			}
		}

		const team =
			account.tid !== undefined && account.tid >= 0
				? await idb.cache.teams.get(account.tid)
				: undefined;
		const suggestedRaw = suggestedAccounts(snapshot, g.get("userTid")).filter(
			(a) => a.id !== account.id,
		);
		const parentIds = new Set(replies.map((r) => r.parent.accountId));
		const pictures = await picturesFor(snapshot, [
			account,
			...suggestedRaw,
			...snapshot.accounts.filter((a) => parentIds.has(a.id)),
		]);
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
			colors: t.colors,
		}));

		// When the account "joined": a player the year he was drafted, and
		// everyone else when the league began.
		const playerRow =
			account.pid === undefined
				? undefined
				: await idb.getCopy.players({ pid: account.pid }, "noCopyCache");
		const joined =
			playerRow && playerRow.draft.year > 0
				? playerRow.draft.year
				: g.get("startingSeason");
		// A profile shows how big this account is, which is the one number a
		// profile page always has and the feed never does.
		const notability =
			account.pid === undefined
				? 0.5
				: Math.max(
						0,
						Math.min(1, ((playerRow?.ratings.at(-1)?.ovr ?? 40) - 38) / 34),
					);

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
				verified: isVerified(account),
				followers: formatReach(reachOf(account, notability)),
				following: formatReach(followingOf(account)),
				joined,
				postCount: posts.length,
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
			replies,
			pictures,
			season,
			suggested,
			teams,
			userTid: g.get("userTid"),
		};
	}
};

export default updateSocialAccount;
