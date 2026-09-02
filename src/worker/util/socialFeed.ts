// THE FEED, ASSEMBLED.
//
// Everything above this file is pure and knows nothing about a database. This
// is where the league is read and handed to it: accounts resolved from the
// roster, events derived from a day's games and the news log, a cast chosen, a
// line written for each slot, and replies hung underneath.
//
// NOTHING IS STORED. A day is recomputed on demand, every time, from data the
// league already has plus a seed built from the season and day. That is what
// keeps seven hundred accounts and twenty seasons of history out of the room
// checkpoint - and it means two people looking at the same league day see the
// same feed without anything having to sync.
//
// TWO THINGS ARE TRUE OF EVERY POST HERE, and both are what "derived" has to
// mean for a timeline rather than a report:
//
//   1. An account's posts are generated ON THEIR OWN, from the account and the
//      day, and the feed SELECTS from them. The timeline is the day's loudest
//      forty-five; a profile is everything the account said. Because the feed
//      takes the account's own text rather than writing its own, a post reads
//      identically on the timeline and on the profile, always.
//
//   2. An account REMEMBERS what it said recently. Before it writes today, it
//      re-derives its last dozen days - each of those is a pure function of
//      the account and the day, so this is a lookup, not a chain - and refuses
//      any line, before or after voice, that it has already used. Within a
//      day the feed also refuses any sentence a different account has already
//      posted. Together with sign-offs and nicknames drawn fresh each time,
//      that is what makes an exact repeat something you have to go looking
//      for.
//
// Everything is cached per day, keyed on what that day actually depends on:
// the day's own box scores, the news placed on it, and the shape of the
// roster. A simmed day therefore invalidates itself and nothing else, and a
// re-simmed one (which multiplayer can produce) invalidates only itself too.

import { idb } from "../db/index.ts";
import { g } from "./index.ts";
import { PHASE } from "../../common/constants.ts";
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
	eventFromLeagueEvent,
	eventsFromGame,
	isFeedableLeagueEvent,
	OFFSEASON_DAY,
	placeLeagueEvents,
	trimDayEvents,
	type GameForEvents,
	type SocialEvent,
} from "../../common/socialEvents.ts";
import {
	castDay,
	castReplies,
	type SocialCasting,
} from "../../common/socialCasting.ts";
import { feudHeat, rivalryFrom } from "../../common/socialFeuds.ts";
import {
	writePostDetailed,
	writeReplyDetailed,
	type AvoidFn,
} from "../../common/socialWriting.ts";
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
// The offseason is one undated stretch holding a whole summer of news, so it
// gets a bigger window than a game night.
const OFFSEASON_EVENTS = 60;
// Most an account says in one day, whether or not the feed shows it.
const POSTS_PER_ACCOUNT_DAY = 2;
// How far back an account remembers its own lines.
export const MEMORY_DAYS = 12;

const normalise = (text: string) =>
	text
		.toLowerCase()
		.replaceAll(/[^\d a-z]/g, "")
		.replaceAll(/\s+/g, " ")
		.trim();

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

export const resolveFeedAccounts = readAccounts;

// ---------------------------------------------------------------- SNAPSHOT
//
// One read of the league per view update, shared by every day that update
// touches. Rebuilt only when its fingerprint changes.

export type FeedSnapshot = {
	season: number;
	fingerprint: string;
	// Ascending. Day 0 is the offseason and is present only when there is
	// news that belongs to it.
	days: number[];
	accounts: ResolvedSocialAccount[];
	accountById: Map<string, ResolvedSocialAccount>;
	leagueEventsByDay: Map<number, SocialEvent[]>;
	rivalry: {
		games: { tids: number[]; winnerTid: number }[];
		swappedPairs: [number, number][];
	};
	// What one day's cache entries depend on, and nothing else.
	dayKey: (day: number) => string;
};

let snapshotMemo: FeedSnapshot | undefined;

export const getFeedSnapshot = async (
	season: number,
): Promise<FeedSnapshot> => {
	const games = (await idb.getCopies.games({ season }, "noCopyCache")).filter(
		(game: any) => game.won && game.lost,
	);
	let rows: any[] = [];
	try {
		rows = await idb.getCopies.events({ season }, "noCopyCache");
	} catch {
		// A league with no event log still gets a feed from its games.
	}
	const stored = await idb.cache.socialAccounts.getAll();
	const offseason = season < g.get("season") || g.get("phase") > PHASE.PLAYOFFS;

	// Every score in the season, so a re-simmed game changes the fingerprint
	// even though the count did not.
	const gamesSig = games
		.map(
			(game: any) =>
				`${game.gid}:${game.teams[0].pts}-${game.teams[1].pts}:${game.day ?? 0}`,
		)
		.join(",");
	const fingerprint = `${season}|${hashSeed(gamesSig)}|${games.length}|${rows.length}|${stored.length}|${offseason ? 1 : 0}`;
	if (snapshotMemo?.fingerprint === fingerprint) {
		return snapshotMemo;
	}

	const accounts = await readAccounts();
	const accountById = new Map(accounts.map((a) => [a.id, a]));
	// The roster's shape, as far as posts depend on it: who is on which team
	// and speaking in which voice. An injury changes none of this, so it does
	// not throw away a hundred cached days; a trade changes it and should.
	const accountsSig = hashSeed(
		accounts
			.map(
				(a) =>
					`${a.id}:${a.tid}:${a.personality.loyaltyTid}:${a.archetypeId}:${a.personality.tone}`,
			)
			.join(","),
	);

	// ---- League news, placed on days by the box scores.
	const feedable = rows
		.filter((row) => isFeedableLeagueEvent(row.type))
		.sort((a, b) => a.eid - b.eid);
	const placement = placeLeagueEvents({
		events: feedable.map((row) => ({
			eid: row.eid,
			type: row.type,
			text: row.text,
			pids: row.pids,
			tids: row.tids,
		})),
		games: games.map((game: any) => ({
			gid: game.gid,
			day: game.day ?? 0,
			playoffs: game.playoffs === true,
			teams: game.teams.map((t: any) => ({
				tid: t.tid,
				players: (t.players ?? []).map((p: any) => ({
					pid: p.pid,
					injuryNew: p.injury?.newThisGame === true,
				})),
			})),
		})),
		offseason,
	});
	const leagueEventsByDay = new Map<number, SocialEvent[]>();
	for (const row of feedable) {
		const day = placement.get(row.eid) ?? OFFSEASON_DAY;
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
			day,
		);
		if (!event) {
			continue;
		}
		const list = leagueEventsByDay.get(day);
		if (list) {
			list.push(event);
		} else {
			leagueEventsByDay.set(day, [event]);
		}
	}

	const gameDays = [...new Set(games.map((game: any) => game.day ?? 0))].sort(
		(a, b) => a - b,
	);
	const days =
		leagueEventsByDay.has(OFFSEASON_DAY) && !gameDays.includes(OFFSEASON_DAY)
			? [OFFSEASON_DAY, ...gameDays]
			: gameDays;

	// ---- Per-day signatures.
	const daySig = new Map<number, string>();
	for (const game of games) {
		const day = game.day ?? 0;
		daySig.set(
			day,
			`${daySig.get(day) ?? ""}${game.gid}:${game.teams[0].pts}-${game.teams[1].pts},`,
		);
	}
	const dayKeys = new Map<number, string>();
	const dayKey = (day: number) => {
		let key = dayKeys.get(day);
		if (key === undefined) {
			const eids = (leagueEventsByDay.get(day) ?? [])
				.map((event) => event.id)
				.join(",");
			key = `${season}|${day}|${hashSeed(daySig.get(day) ?? "")}|${hashSeed(eids)}|${accountsSig}`;
			dayKeys.set(day, key);
		}
		return key;
	};

	// ---- Rivalry inputs, once.
	const swappedPairs: [number, number][] = [];
	for (const row of rows) {
		if (row.type === "trade" && row.tids && row.tids.length === 2) {
			swappedPairs.push([row.tids[0], row.tids[1]]);
		}
	}
	const rivalry = {
		games: games.map((game: any) => ({
			tids: [game.teams[0].tid, game.teams[1].tid],
			winnerTid:
				game.teams[0].pts > game.teams[1].pts
					? game.teams[0].tid
					: game.teams[1].tid,
		})),
		swappedPairs,
	};

	snapshotMemo = {
		season,
		fingerprint,
		days,
		accounts,
		accountById,
		leagueEventsByDay,
		rivalry,
		dayKey,
	};
	return snapshotMemo;
};

// The days worth showing, newest first.
export const feedDaysForSeason = async (season: number): Promise<number[]> =>
	[...(await getFeedSnapshot(season)).days].reverse();

// ---------------------------------------------------------------- CACHES

const dayEventsCache = new Map<string, SocialEvent[]>();
const memorylessCache = new Map<string, AccountDayPost[]>();
const accountDayCache = new Map<string, FeedPost[]>();
const feedCache = new Map<string, FeedDay>();

// Bounded rather than evicted: these are small strings, the bounds are
// generous, and a long browse should not pin a whole career in memory.
const bounded = <V>(map: Map<string, V>, limit: number) => {
	if (map.size > limit) {
		map.clear();
	}
};

export const clearSocialFeedCache = () => {
	snapshotMemo = undefined;
	dayEventsCache.clear();
	memorylessCache.clear();
	accountDayCache.clear();
	feedCache.clear();
};

// ---------------------------------------------------------------- EVENTS

const gameForEvents = (
	season: number,
	game: {
		gid: number;
		day: number;
		overtimes: number;
		winnerTid: number;
		playoffs: boolean;
		teams: any[];
		spread?: { favTid: number; points: number };
	},
): GameForEvents => ({
	gid: game.gid,
	day: game.day,
	season,
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

// What happened on one day, trimmed to the events worth posting about. The
// expensive part is the box-score read, which is why this is cached on the
// day's own key.
const eventsForDay = async (
	snapshot: FeedSnapshot,
	day: number,
): Promise<SocialEvent[]> => {
	const key = snapshot.dayKey(day);
	const cached = dayEventsCache.get(key);
	if (cached) {
		return cached;
	}
	const news = snapshot.leagueEventsByDay.get(day) ?? [];
	let events: SocialEvent[];
	if (day === OFFSEASON_DAY) {
		events = trimDayEvents(news, { limit: OFFSEASON_EVENTS });
	} else {
		const recapGames = await recapGamesForDay({ season: snapshot.season, day });
		const gameEvents = recapGames.flatMap((game) =>
			eventsFromGame(gameForEvents(snapshot.season, game as any)),
		);
		events = trimDayEvents([...gameEvents, ...news], { limit: EVENTS_PER_DAY });
	}
	bounded(dayEventsCache, 600);
	dayEventsCache.set(key, events);
	return events;
};

// ---------------------------------------------------------------- ONE ACCOUNT, ONE DAY

type AccountDayPost = {
	eventId: string;
	core: string;
	text: string;
};

const seedFor = (snapshot: FeedSnapshot, day: number) =>
	`${snapshot.season}|${day}`;

// What this account would say today with no memory of yesterday: a pure
// function of the account and the day. This is what memory is BUILT FROM, and
// it is never shown - the shown version is the one that has checked itself
// against these.
const memorylessDay = async (
	snapshot: FeedSnapshot,
	account: ResolvedSocialAccount,
	day: number,
): Promise<AccountDayPost[]> => {
	const key = `${snapshot.dayKey(day)}|${account.id}`;
	const cached = memorylessCache.get(key);
	if (cached) {
		return cached;
	}
	const events = await eventsForDay(snapshot, day);
	const out = writeAccountDay({
		snapshot,
		account,
		day,
		events,
		seen: new Set(),
	});
	bounded(memorylessCache, 80_000);
	memorylessCache.set(key, out);
	return out;
};

const writeAccountDay = ({
	snapshot,
	account,
	day,
	events,
	seen,
}: {
	snapshot: FeedSnapshot;
	account: ResolvedSocialAccount;
	day: number;
	events: SocialEvent[];
	seen: Set<string>;
}): AccountDayPost[] => {
	const seed = seedFor(snapshot, day);
	const casting = castDay({
		accounts: [account],
		events,
		seed,
		limits: {
			target: POSTS_PER_ACCOUNT_DAY,
			maxPerAccount: POSTS_PER_ACCOUNT_DAY,
			maxPerEvent: 1,
		},
	});
	if (casting.length === 0) {
		return [];
	}

	const eventById = new Map(events.map((event) => [event.id, event]));
	const pool = createPhrasePool();
	pool.beginBatch();
	const avoid: AvoidFn = (core, text) =>
		seen.has(normalise(core)) || seen.has(normalise(text));
	const out: AccountDayPost[] = [];
	for (const slot of casting) {
		const event = eventById.get(slot.eventId);
		if (!event) {
			continue;
		}
		const written = writePostDetailed({
			account,
			event,
			pool,
			rng: rngFromSeed(hashSeed(`${seed}|${account.id}|${slot.eventId}`)),
			avoid,
		});
		if (!written) {
			continue;
		}
		seen.add(normalise(written.core));
		seen.add(normalise(written.text));
		out.push({ eventId: event.id, core: written.core, text: written.text });
	}
	pool.endBatch();
	return out;
};

// Everything this account said in its last MEMORY_DAYS days of feed, as the
// normalised cores and texts a writer has to steer around.
const recentLines = async (
	snapshot: FeedSnapshot,
	account: ResolvedSocialAccount,
	dayIndex: number,
): Promise<Set<string>> => {
	const seen = new Set<string>();
	for (let j = 1; j <= MEMORY_DAYS; j++) {
		const day = snapshot.days[dayIndex - j];
		if (day === undefined) {
			break;
		}
		for (const post of await memorylessDay(snapshot, account, day)) {
			seen.add(normalise(post.core));
			seen.add(normalise(post.text));
		}
	}
	return seen;
};

const toFeedPost = (
	account: ResolvedSocialAccount,
	post: AccountDayPost,
): FeedPost => ({
	id: `${account.id}|${post.eventId}`,
	accountId: account.id,
	handle: account.handle,
	name: account.name,
	kind: account.kind,
	tid: account.tid,
	pid: account.pid,
	text: post.text,
	eventId: post.eventId,
	replies: [],
});

// ONE ACCOUNT'S OWN DAY, as shown: what it would post today, having checked
// itself against what it posted recently. The feed and the profile both read
// from here, which is why a post is the same text in both places.
export const buildAccountDay = async ({
	snapshot,
	account,
	dayIndex,
}: {
	snapshot: FeedSnapshot;
	account: ResolvedSocialAccount;
	dayIndex: number;
}): Promise<FeedPost[]> => {
	const day = snapshot.days[dayIndex];
	if (day === undefined) {
		return [];
	}
	const key = `${snapshot.dayKey(day)}|${account.id}|m`;
	const cached = accountDayCache.get(key);
	if (cached) {
		return cached;
	}
	const events = await eventsForDay(snapshot, day);
	const seen = await recentLines(snapshot, account, dayIndex);
	const out = writeAccountDay({ snapshot, account, day, events, seen }).map(
		(post) => toFeedPost(account, post),
	);
	bounded(accountDayCache, 80_000);
	accountDayCache.set(key, out);
	return out;
};

// ---------------------------------------------------------------- THE DAY

export const buildFeedDay = async ({
	snapshot,
	dayIndex,
}: {
	snapshot: FeedSnapshot;
	dayIndex: number;
}): Promise<FeedDay> => {
	const day = snapshot.days[dayIndex];
	if (day === undefined) {
		return { season: snapshot.season, day: 0, posts: [] };
	}
	const key = snapshot.dayKey(day);
	const cached = feedCache.get(key);
	if (cached) {
		return cached;
	}

	const { accounts, accountById } = snapshot;
	const events = await eventsForDay(snapshot, day);
	const seed = seedFor(snapshot, day);

	// Every account decides what it would post; the feed ranks those. The
	// candidates come back in score order with the per-account cap already
	// applied, exactly as each account applied it to itself, so the slots the
	// feed picks always exist on the account's own day.
	const everyone = castDay({
		accounts,
		events,
		seed,
		limits: {
			target: Number.POSITIVE_INFINITY,
			maxPerAccount: POSTS_PER_ACCOUNT_DAY,
			maxPerEvent: Number.POSITIVE_INFINITY,
		},
	});

	const perEvent = new Map<string, number>();
	const said = new Set<string>();
	const slots: SocialCasting[] = [];
	const out: FeedPost[] = [];
	for (const candidate of everyone) {
		if (out.length >= POSTS_PER_DAY) {
			break;
		}
		if ((perEvent.get(candidate.eventId) ?? 0) >= 4) {
			continue;
		}
		const account = accountById.get(candidate.accountId);
		if (!account) {
			continue;
		}
		const own = await buildAccountDay({ snapshot, account, dayIndex });
		const post = own.find((p) => p.eventId === candidate.eventId);
		if (!post) {
			continue;
		}
		// Two accounts landing on the same sentence about the same game is
		// the one repeat memory cannot see, because memory is per account.
		const line = normalise(post.text);
		if (said.has(line)) {
			continue;
		}
		said.add(line);
		perEvent.set(candidate.eventId, (perEvent.get(candidate.eventId) ?? 0) + 1);
		slots.push(candidate);
		out.push({ ...post, replies: [] });
	}

	// ---- Replies. Feuds are derived, so the pair rule is memoized here -
	// castReplies asks about the same handful of accounts many times.
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
						games: snapshot.rivalry.games,
						swappedPairs: snapshot.rivalry.swappedPairs,
						declaredRivals: first.personality.rivalTids ?? [],
					}),
				});
			}
		}
		feudCache.set(cacheKey, heat);
		return heat;
	};

	const replies = castReplies({
		posts: slots,
		accounts,
		events,
		feudBetween,
		seed,
		target: REPLIES_PER_DAY,
	});

	// One pool for the day's replies, so two of them do not open the same way.
	const pool = createPhrasePool();
	pool.beginBatch();
	const eventById = new Map(events.map((event) => [event.id, event]));
	const postByKey = new Map(out.map((post) => [post.id, post]));
	for (const reply of replies) {
		const parent = postByKey.get(
			`${reply.parentAccountId}|${reply.parentEventId}`,
		);
		const replier = accountById.get(reply.accountId);
		const poster = accountById.get(reply.parentAccountId);
		const event = eventById.get(reply.parentEventId);
		if (!parent || !replier || !poster || !event) {
			continue;
		}
		// A replier steers around its own recent posts too, so "Fair." under
		// a post is not also its post from the day before yesterday.
		const recent = await recentLines(snapshot, replier, dayIndex);
		const written = writeReplyDetailed({
			account: replier,
			parent: poster,
			event,
			heat: reply.heat,
			quote: reply.kind === "quote",
			pool,
			rng: rngFromSeed(
				hashSeed(`${seed}|re|${reply.accountId}|${reply.parentAccountId}`),
			),
			avoid: (core, text) =>
				said.has(normalise(text)) ||
				recent.has(normalise(core)) ||
				recent.has(normalise(text)),
		});
		if (!written) {
			continue;
		}
		said.add(normalise(written.text));
		parent.replies.push({
			id: `${reply.accountId}|${parent.id}`,
			accountId: replier.id,
			handle: replier.handle,
			name: replier.name,
			kind: replier.kind,
			tid: replier.tid,
			pid: replier.pid,
			text: written.text,
			quote: reply.kind === "quote",
		});
	}
	pool.endBatch();

	const feed: FeedDay = { season: snapshot.season, day, posts: out };
	bounded(feedCache, 400);
	feedCache.set(key, feed);
	return feed;
};
