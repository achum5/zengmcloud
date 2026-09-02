// THE FEED, ASSEMBLED.
//
// Everything above this file is pure and knows nothing about a database. This
// is where the league is read and handed to it: accounts resolved from the
// roster, events derived from a day's games and the news log, a cast chosen, a
// line written for each slot, and replies hung underneath.
//
// NOTHING IS STORED. A day is recomputed on demand, every time, from data the
// league already has plus a seed built from the season and day. That is what
// keeps five hundred accounts and twenty seasons of history out of the room
// checkpoint - and it means two people looking at the same league day see the
// same feed without anything having to sync.
//
// The cost is that a day is real work: resolving accounts walks the roster and
// casting scores every account against every event. So the results are cached
// per (season, day) for as long as the league stays open, which is enough -
// scrolling a feed re-reads the same handful of days over and over, and a
// simmed day invalidates only itself.

import { idb } from "../db/index.ts";
import { g } from "./index.ts";
import {
	createPhrasePool,
	hashSeed,
	rngFromSeed,
} from "../../common/phrasePool.ts";
import {
	resolveAccounts,
	type ImplicitPlayer,
	type ImplicitTeam,
	type ResolvedSocialAccount,
} from "../../common/socialAccounts.ts";
import {
	assignEventDays,
	eventFromLeagueEvent,
	eventsFromGame,
	isFeedableLeagueEvent,
	trimDayEvents,
	type GameForEvents,
	type SocialEvent,
} from "../../common/socialEvents.ts";
import { castDay, castReplies } from "../../common/socialCasting.ts";
import { feudHeat, rivalryFrom } from "../../common/socialFeuds.ts";
import { writePost, writeReply } from "../../common/socialWriting.ts";
import { recapGamesForDay } from "./getDayGamesForRecap.ts";

export type FeedPost = {
	id: string;
	accountId: string;
	handle: string;
	name: string;
	kind: "player" | "team" | "media";
	tid?: number;
	pid?: number;
	text: string;
	eventId: string;
	// Replies and quotes hanging off this post, in the order they should read.
	replies: {
		id: string;
		accountId: string;
		handle: string;
		name: string;
		kind: "player" | "team" | "media";
		tid?: number;
		pid?: number;
		text: string;
		quote: boolean;
	}[];
};

export type FeedDay = {
	season: number;
	day: number;
	posts: FeedPost[];
};

// How busy a day is, matching the reactive-volume choice: accounts post when
// they have a reason and the feed is short on a quiet night.
const POSTS_PER_DAY = 45;
const REPLIES_PER_DAY = 14;
const EVENTS_PER_DAY = 26;

// ---------------------------------------------------------------- ACCOUNTS

// Everyone in the league, with the personality inputs the resolver needs.
// Retired players are included: people do not delete their accounts when they
// stop playing, and their pages are half the point of browsing back.
const readAccounts = async (): Promise<ResolvedSocialAccount[]> => {
	const season = g.get("season");
	const rawTeams = await idb.cache.teams.getAll();
	const teams: ImplicitTeam[] = rawTeams.map((t) => ({
		tid: t.tid,
		region: t.region,
		name: t.name,
		abbrev: t.abbrev,
		imgURL: t.imgURL,
		disabled: t.disabled,
	}));

	const rawPlayers = await idb.getCopies.players({ activeAndRetired: true });
	const players: ImplicitPlayer[] = rawPlayers.map((p) => {
		const ratings = p.ratings.at(-1);
		return {
			pid: p.pid,
			name: `${p.firstName} ${p.lastName}`,
			tid: p.tid,
			pos: ratings?.pos,
			age: season - p.born.year,
			ovr: ratings?.ovr ?? 0,
			experience: p.stats.filter((row) => !row.playoffs).length,
			moodTraits: p.moodTraits ?? [],
			retired: p.tid === -2 || p.retiredYear <= season,
		};
	});

	const stored = await idb.cache.socialAccounts.getAll();
	return resolveAccounts({ players, teams, stored });
};

// ---------------------------------------------------------------- EVENTS

const gameForEvents = (game: {
	gid: number;
	day: number;
	overtimes: number;
	winnerTid: number;
	playoffs: boolean;
	teams: any[];
	spread?: { favTid: number; points: number };
}): GameForEvents => ({
	gid: game.gid,
	day: game.day,
	season: g.get("season"),
	overtimes: game.overtimes,
	winnerTid: game.winnerTid,
	playoffs: game.playoffs,
	spread: game.spread,
	teams: [0, 1].map((i) => {
		const t = game.teams[i];
		return {
			tid: t.tid,
			region: t.region,
			name: t.name,
			abbrev: t.abbrev,
			pts: t.pts,
			streak: t.streak,
			record: t.record,
			players: (t.players ?? []).map((p: any) => ({
				pid: p.pid,
				name: p.name,
				min: p.min ?? 0,
				pts: p.pts ?? 0,
				reb: p.reb ?? 0,
				ast: p.ast ?? 0,
				stl: p.stl ?? 0,
				blk: p.blk ?? 0,
				tov: p.tov ?? 0,
				fga: p.fga ?? 0,
				fta: p.fta ?? 0,
			})),
		};
	}) as GameForEvents["teams"],
});

// League-log events for a season, placed onto the days that have games. The
// log records no day and the per-game entries that would have anchored one are
// not persisted, so this is a placement rather than a recovery - see
// assignEventDays for why that is the honest framing.
const readLeagueEvents = async (
	season: number,
	daysWithGames: number[],
): Promise<Map<number, SocialEvent[]>> => {
	const byDay = new Map<number, SocialEvent[]>();
	let rows: any[] = [];
	try {
		rows = await idb.getCopies.events({ season }, "noCopyCache");
	} catch {
		return byDay;
	}
	const feedable = rows
		.filter((row) => isFeedableLeagueEvent(row.type))
		.sort((a, b) => a.eid - b.eid);
	const dayFor = assignEventDays(
		feedable.map((row) => row.eid),
		daysWithGames,
	);
	for (const row of feedable) {
		const event = eventFromLeagueEvent(
			{
				eid: row.eid,
				type: row.type,
				season: row.season,
				text: row.text,
				pids: row.pids,
				tids: row.tids,
				score: row.score,
			},
			dayFor.get(row.eid) ?? 0,
		);
		if (!event) {
			continue;
		}
		const list = byDay.get(event.day);
		if (list) {
			list.push(event);
		} else {
			byDay.set(event.day, [event]);
		}
	}
	return byDay;
};

// ---------------------------------------------------------------- FEUDS

// The season's games as team pairs, which is all the rivalry rule needs, plus
// the teams that have swapped a player. Read once per day rather than per
// account pair - there are hundreds of thousands of pairs and one list.
const readRivalryInputs = async (season: number) => {
	const games = (await idb.getCopies.games({ season }, "noCopyCache")).map(
		(game: any) => ({
			tids: [game.teams[0].tid, game.teams[1].tid],
			winnerTid:
				game.teams[0].pts > game.teams[1].pts
					? game.teams[0].tid
					: game.teams[1].tid,
		}),
	);

	const swappedPairs: [number, number][] = [];
	try {
		const rows = await idb.getCopies.events({ season }, "noCopyCache");
		for (const row of rows as any[]) {
			if (row.type === "trade" && row.tids && row.tids.length === 2) {
				swappedPairs.push([row.tids[0], row.tids[1]]);
			}
		}
	} catch {
		// A league with no event log still gets rivalries from the schedule.
	}
	return { games, swappedPairs };
};

// ---------------------------------------------------------------- ASSEMBLY

const cache = new Map<string, FeedDay>();
const accountDayCache = new Map<string, FeedPost[]>();

export const clearSocialFeedCache = () => {
	cache.clear();
	accountDayCache.clear();
};

// What happened on one day, trimmed to the events worth posting about. Shared
// by the feed and by a single account's profile, so both are looking at the
// same day rather than at two slightly different ones.
const eventsForDay = async ({
	season,
	day,
}: {
	season: number;
	day: number;
}) => {
	const recapGames = await recapGamesForDay({ season, day });
	const gameEvents = recapGames.flatMap((game) =>
		eventsFromGame(gameForEvents(game as any)),
	);

	const allGames = await idb.getCopies.games({ season }, "noCopyCache");
	const daysWithGames = [
		...new Set(allGames.map((game: any) => game.day ?? 0)),
	].sort((a, b) => a - b);
	const leagueEvents = await readLeagueEvents(season, daysWithGames);

	return trimDayEvents([...gameEvents, ...(leagueEvents.get(day) ?? [])], {
		limit: EVENTS_PER_DAY,
	});
};

export const buildFeedDay = async ({
	season,
	day,
}: {
	season: number;
	day: number;
}): Promise<FeedDay> => {
	const key = `${season}|${day}`;
	const cached = cache.get(key);
	if (cached) {
		return cached;
	}

	const accounts = await readAccounts();
	const events = await eventsForDay({ season, day });

	const seed = `${season}|${day}`;
	const posts = castDay({
		accounts,
		events,
		seed,
		limits: { target: POSTS_PER_DAY },
	});

	// Feuds are derived, so the inputs are read once and the pair rule is
	// memoized - castReplies asks about the same handful of accounts many times.
	const { games: seasonGames, swappedPairs } = await readRivalryInputs(season);
	const accountById = new Map(accounts.map((a) => [a.id, a]));
	const feudCache = new Map<string, number>();
	const feudBetween = (firstId: string, secondId: string) => {
		const cacheKey =
			firstId < secondId ? `${firstId}|${secondId}` : `${secondId}|${firstId}`;
		const hit = feudCache.get(cacheKey);
		if (hit !== undefined) {
			return hit;
		}
		const first = accountById.get(firstId);
		const second = accountById.get(secondId);
		let heat = 0;
		if (first && second) {
			const firstTid = first.personality.loyaltyTid ?? first.tid;
			const secondTid = second.personality.loyaltyTid ?? second.tid;
			if (firstTid !== undefined && secondTid !== undefined) {
				heat = feudHeat({
					firstTid,
					secondTid,
					firstOptimism: first.personality.optimism,
					secondOptimism: second.personality.optimism,
					rivalry: rivalryFrom({
						firstTid,
						secondTid,
						games: seasonGames,
						swappedPairs,
						declaredRivals: first.personality.rivalTids ?? [],
					}),
				});
			}
		}
		feudCache.set(cacheKey, heat);
		return heat;
	};

	const replies = castReplies({
		posts,
		accounts,
		events,
		feudBetween,
		seed,
		target: REPLIES_PER_DAY,
	});

	// ONE pool for the whole day, which is what makes the rotation and the
	// template ledger span it. A pool per post would put the machinery back
	// exactly where it started.
	const pool = createPhrasePool();
	pool.beginBatch();

	const eventById = new Map(events.map((event) => [event.id, event]));
	const out: FeedPost[] = [];

	// EVERY LINE SAID TODAY, normalised. The template ledger already stops one
	// template repeating, but two banks can land on the same words once voice
	// has lowercased and trimmed them, and a reader scrolling one day only
	// sees the sentence. Measured at 37% of a fortnight's posts before this
	// existed, which is exactly the "cheap and redundant" the feed cannot be.
	const said = new Set<string>();
	const normalise = (text: string) =>
		text
			.toLowerCase()
			.replaceAll(/[^\d a-z]/g, "")
			.replaceAll(/\s+/g, " ")
			.trim();
	const alreadySaid = (text: string) => said.has(normalise(text));

	for (const slot of posts) {
		const account = accountById.get(slot.accountId);
		const event = eventById.get(slot.eventId);
		if (!account || !event) {
			continue;
		}
		const text = writePost({
			account,
			event,
			pool,
			rng: rngFromSeed(hashSeed(`${seed}|${slot.accountId}|${slot.eventId}`)),
			avoid: alreadySaid,
		});
		if (text === undefined) {
			continue;
		}
		said.add(normalise(text));

		const post: FeedPost = {
			id: `${slot.accountId}|${slot.eventId}`,
			accountId: account.id,
			handle: account.handle,
			name: account.name,
			kind: account.kind,
			tid: account.tid,
			pid: account.pid,
			text,
			eventId: event.id,
			replies: [],
		};

		for (const reply of replies) {
			if (
				reply.parentAccountId !== slot.accountId ||
				reply.parentEventId !== slot.eventId
			) {
				continue;
			}
			const replier = accountById.get(reply.accountId);
			if (!replier) {
				continue;
			}
			const replyText = writeReply({
				account: replier,
				parent: account,
				event,
				heat: reply.heat,
				quote: reply.kind === "quote",
				pool,
				rng: rngFromSeed(
					hashSeed(`${seed}|re|${reply.accountId}|${slot.accountId}`),
				),
				avoid: alreadySaid,
			});
			if (replyText === undefined) {
				continue;
			}
			said.add(normalise(replyText));
			post.replies.push({
				id: `${reply.accountId}|${post.id}`,
				accountId: replier.id,
				handle: replier.handle,
				name: replier.name,
				kind: replier.kind,
				tid: replier.tid,
				pid: replier.pid,
				text: replyText,
				quote: reply.kind === "quote",
			});
		}

		out.push(post);
	}

	pool.endBatch();

	const feed: FeedDay = { season, day, posts: out };
	// Bounded: a long browse back through a season should not pin every day it
	// touched in memory for the rest of the session.
	if (cache.size > 40) {
		cache.clear();
	}
	cache.set(key, feed);
	return feed;
};

// The days worth showing, newest first: every day that has games, so the feed
// scrolls back through the season the way a timeline does.
// ONE ACCOUNT'S OWN DAY.
//
// The feed holds the day's loudest forty-five posts, which is what a timeline
// is. A PROFILE is a different question: it asks what this account said, not
// whether what it said beat seven hundred other accounts for a slot. Asking
// the first question of a profile page leaves almost every profile empty,
// which is fatal for a feature whose whole point is that each account is worth
// clicking into.
//
// So a profile runs the same casting restricted to one account. Every choice
// in castDay is seeded on the account and the day, so this returns exactly the
// posts that account would have made - the ones that reached the feed, plus
// the ones that lost the competition for a slot. Days where it did reach the
// feed are taken from the feed itself, so no post ever exists in two versions.
export const buildAccountDay = async ({
	season,
	day,
	accountId,
}: {
	season: number;
	day: number;
	accountId: string;
}): Promise<FeedPost[]> => {
	const key = `${season}|${day}|${accountId}`;
	const cached = accountDayCache.get(key);
	if (cached) {
		return cached;
	}

	const accounts = await readAccounts();
	const account = accounts.find((a) => a.id === accountId);
	if (!account) {
		return [];
	}

	const events = await eventsForDay({ season, day });
	const seed = `${season}|${day}`;
	const casting = castDay({
		accounts: [account],
		events,
		seed,
		// Only this account, so the day-wide target never binds; the per-account
		// cap inside castDay is what limits it, exactly as in the feed.
		limits: { target: POSTS_PER_DAY },
	});

	const pool = createPhrasePool();
	pool.beginBatch();
	const eventById = new Map(events.map((event) => [event.id, event]));
	const out: FeedPost[] = [];
	const said = new Set<string>();

	for (const slot of casting) {
		const event = eventById.get(slot.eventId);
		if (!event) {
			continue;
		}
		const text = writePost({
			account,
			event,
			pool,
			rng: rngFromSeed(hashSeed(`${seed}|${slot.accountId}|${slot.eventId}`)),
			avoid: (candidate) => said.has(candidate),
		});
		if (text === undefined) {
			continue;
		}
		said.add(text);
		out.push({
			id: `${slot.accountId}|${slot.eventId}`,
			accountId: account.id,
			handle: account.handle,
			name: account.name,
			kind: account.kind,
			tid: account.tid,
			pid: account.pid,
			text,
			eventId: event.id,
			replies: [],
		});
	}

	pool.endBatch();
	if (accountDayCache.size > 400) {
		accountDayCache.clear();
	}
	accountDayCache.set(key, out);
	return out;
};

export const feedDaysForSeason = async (season: number): Promise<number[]> => {
	const games = await idb.getCopies.games({ season }, "noCopyCache");
	return [...new Set(games.map((game: any) => game.day ?? 0))].sort(
		(a, b) => b - a,
	);
};

export const resolveFeedAccounts = readAccounts;
