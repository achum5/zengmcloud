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
	playoffSeriesEvents,
	seasonStateEvents,
	standingsThrough,
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
	engagementFor,
	isVerified,
	reachOf,
	timeOf,
	type AccountPicture,
	type Engagement,
} from "../../common/socialMetrics.ts";
import {
	writePostDetailed,
	writeReplyDetailed,
	type AvoidFn,
} from "../../common/socialWriting.ts";
import { recapGamesForDay } from "./getDayGamesForRecap.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";

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
	verified: boolean;
	// Clock time and engagement, derived rather than stored - see socialMetrics.
	time: string;
	minutes: number;
	engagement: Engagement;
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
		verified: boolean;
		time: string;
		engagement: Engagement;
		// The handle this answers, when it is answering a reply rather than
		// the post. A one-level thread is a comment section; the back-and-
		// forth is what makes it read like an argument between two people.
		replyTo?: string;
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
// How much of a day is about the league rather than about tonight. Small on
// purpose: an account that posts the standings every night is a bot.
const SEASON_EVENTS_PER_DAY = 5;
// The offseason is one undated stretch holding a whole summer of news, so it
// gets a bigger window than a game night.
const OFFSEASON_EVENTS = 60;
// Most an account says in one day, whether or not the feed shows it.
const POSTS_PER_ACCOUNT_DAY = 2;
// How far back an account remembers its own lines.
export const MEMORY_DAYS = 12;

// What counts as "the same line". Hashtags and emoji come off FIRST: two
// accounts posted an identical stat line and only one of them signed it with
// a team hashtag, which was enough to slip past a comparison that merely
// stripped punctuation.
const normalise = (text: string) =>
	text
		.toLowerCase()
		.replaceAll(/[#@][\d_a-z]+/g, "")
		.replaceAll(/[^\d a-z]/g, "")
		.replaceAll(/\s+/g, " ")
		.trim();

// ---------------------------------------------------------------- ACCOUNTS

// Everyone in the league, with the personality inputs the resolver needs.
// Retired players are included: people do not delete their accounts when they
// stop playing, and their pages are half the point of browsing back.
// The roster, read once per snapshot. Resolving accounts, sizing followings
// and building avatars all need it, and each was loading all five hundred
// players for itself.
let rosterMemo: { fingerprint: string; players: any[] } | undefined;
const readRoster = async (fingerprint: string): Promise<any[]> => {
	if (rosterMemo?.fingerprint === fingerprint) {
		return rosterMemo.players;
	}
	const players = await idb.getCopies.players(
		{ activeAndRetired: true },
		"noCopyCache",
	);
	rosterMemo = { fingerprint, players };
	return players;
};

const readAccounts = async (
	rawPlayers: any[],
): Promise<ResolvedSocialAccount[]> => {
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

	const players: ImplicitPlayer[] = rawPlayers.map((p) => {
		const ratings = p.ratings.at(-1);
		return {
			pid: p.pid,
			name: `${p.firstName} ${p.lastName}`,
			tid: p.tid,
			pos: ratings?.pos,
			age: season - p.born.year,
			ovr: ratings?.ovr ?? 0,
			experience: p.stats.filter((row: any) => !row.playoffs).length,
			moodTraits: p.moodTraits ?? [],
			retired: p.tid === -2 || p.retiredYear <= season,
		};
	});

	const stored = await idb.cache.socialAccounts.getAll();
	return resolveAccounts({ players, teams, stored });
};

// How well known a player is, 0 to 1, for the size of his following. Rating
// is most of it; a long career is the rest, because a fourteen-year starter is
// a household name in a way a rookie with the same ovr is not.
const notabilityByPid = new Map<number, number>();
const readNotability = (rawPlayers: any[]) => {
	notabilityByPid.clear();
	const season = g.get("season");
	for (const p of rawPlayers) {
		const ovr = p.ratings.at(-1)?.ovr ?? 0;
		const years = p.stats.filter((row: any) => !row.playoffs).length;
		const retiredFor = p.tid === -2 ? season - (p.retiredYear ?? season) : 0;
		const fame =
			Math.max(0, Math.min(1, (ovr - 38) / 34)) * 0.8 +
			Math.min(1, years / 12) * 0.2;
		// Fame fades once someone stops playing, but never to nothing.
		notabilityByPid.set(p.pid, fame * Math.max(0.35, 1 - retiredFor * 0.05));
	}
};

export const resolveFeedAccounts = async () =>
	readAccounts(await readRoster("accounts-only"));

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
	// Every game's teams and score, so any day can rebuild the table as it
	// stood that night without re-reading the database.
	standingsInput: {
		day: number;
		gid: number;
		playoffs: boolean;
		teams: {
			tid: number;
			region: string;
			name: string;
			abbrev: string;
			pts: number;
		}[];
	}[];
	playedByDay: Map<number, Set<number>>;
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

	const roster = await readRoster(fingerprint);
	const accounts = await readAccounts(roster);
	readNotability(roster);
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

	// ---- What each day needs to talk about the season rather than only about
	// the night: the table as of that night, and who played in it.
	//
	// A stored game row carries tids and scores but NOT team names - the recap
	// builder resolves those separately, and skipping that step here printed
	// "down 2-3 to the undefined undefined" into the feed. Resolved once, by
	// season, so a franchise that has since moved is still named the way it
	// was named at the time.
	const teamsBySeason = new Map<
		number,
		{ region: string; name: string; abbrev: string }
	>();
	for (const tid of new Set(
		games.flatMap((game: any) => game.teams.map((t: any) => t.tid)),
	)) {
		const info = await getTeamInfoBySeason(tid as number, season);
		teamsBySeason.set(tid as number, {
			region: info?.region ?? "",
			name: info?.name ?? "",
			abbrev: info?.abbrev ?? "???",
		});
	}
	const standingsInput = games.map((game: any) => ({
		day: game.day ?? 0,
		gid: game.gid,
		playoffs: game.playoffs === true,
		teams: game.teams.map((t: any) => ({
			tid: t.tid,
			...(teamsBySeason.get(t.tid) ?? {
				region: "",
				name: "",
				abbrev: "???",
			}),
			pts: t.pts,
		})),
	}));
	const playedByDay = new Map<number, Set<number>>();
	for (const game of standingsInput) {
		const set = playedByDay.get(game.day) ?? new Set<number>();
		for (const t of game.teams) {
			set.add(t.tid);
		}
		playedByDay.set(game.day, set);
	}

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
		standingsInput,
		playedByDay,
		dayKey,
	};
	return snapshotMemo;
};

// ---------------------------------------------------------------- AVATARS
//
// What an account's picture actually is. Seven hundred accounts cannot each be
// given an image by hand, so every one is derived: a player shows the face the
// league already generated for him (or his photo, if he has one), a franchise
// shows its logo, and everyone else gets a monogram - tinted with their team's
// colour when they have one, so the local beat writer sits visually next to
// the team he covers.
//
// Sent as a map keyed by account id and built only for the accounts actually
// on the page, because a face config is a kilobyte of JSON and the roster is
// five hundred players.

export const picturesFor = async (
	snapshot: FeedSnapshot,
	accounts: readonly ResolvedSocialAccount[],
): Promise<Record<string, AccountPicture>> => {
	const teams = await idb.cache.teams.getAll();
	const teamByTid = new Map(teams.map((t) => [t.tid, t]));
	const out: Record<string, AccountPicture> = {};

	const wanted = new Set(
		accounts
			.filter((a) => a.kind === "player" && a.pid !== undefined)
			.map((a) => a.pid!),
	);
	const players = new Map<number, any>();
	for (const p of await readRoster(snapshot.fingerprint)) {
		if (wanted.has(p.pid)) {
			players.set(p.pid, p);
		}
	}

	for (const account of accounts) {
		const team =
			account.tid !== undefined && account.tid >= 0
				? teamByTid.get(account.tid)
				: undefined;
		const colors = team?.colors;

		if (account.kind === "player" && account.pid !== undefined) {
			const p = players.get(account.pid);
			if (p) {
				out[account.id] = {
					face: p.face,
					imgURL: p.imgURL,
					jersey: p.stats?.at(-1)?.jerseyNumber ?? p.jerseyNumber,
					colors,
				};
				continue;
			}
		}
		if (account.kind === "team" && team) {
			out[account.id] = {
				logoURL: team.imgURL,
				colors,
			};
			continue;
		}
		if (colors) {
			out[account.id] = { colors };
		}
	}
	return out;
};

// The days worth showing, newest first.
export const feedDaysForSeason = async (season: number): Promise<number[]> =>
	[...(await getFeedSnapshot(season)).days].reverse();

// ---------------------------------------------------------------- CACHES

const dayEventsCache = new Map<string, SocialEvent[]>();
const accountDayCache = new Map<string, FeedPost[]>();
// What each cached day SAID, normalised, so the next day can remember it
// without the posts having to carry their pre-voice text to the UI.
const lineCache = new Map<string, Set<string>>();
// One post's pre-voice line, by post id. Two accounts reaching the same
// sentence and dressing it differently is invisible to a comparison of the
// finished text - "honest answer: could go either way" showed up twice in one
// day, once with an opener in front of it.
const coreByPostId = new Map<string, string>();
// Which TEMPLATES each cached day used. Refusing a repeated sentence is not
// enough on its own: two different sentences off the same template say the
// same thing in the same shape, and a reader notices the shape.
const shapeCache = new Map<string, Set<string>>();
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
	accountDayCache.clear();
	lineCache.clear();
	coreByPostId.clear();
	shapeCache.clear();
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
		// The state of the league as of tonight, which is what people actually
		// argue about between box scores - and what stops a two-game playoff
		// day being eight accounts describing the same man's rebounds.
		const tonight = snapshot.standingsInput.filter((game) => game.day === day);
		const season = [
			...seasonStateEvents({
				standings: standingsThrough(snapshot.standingsInput, day),
				day,
				season: snapshot.season,
				playedToday: snapshot.playedByDay.get(day) ?? new Set(),
				regularSeasonToday: tonight.some((game) => !game.playoffs),
			}),
			...(tonight.some((game) => game.playoffs)
				? playoffSeriesEvents({
						games: snapshot.standingsInput,
						day,
						season: snapshot.season,
					})
				: []),
		];
		// Trimmed SEPARATELY and then merged. Season notes score lower on
		// salience than a forty-point night, quite correctly, so throwing them
		// into one pool meant they lost every cut and the feature may as well
		// not have existed - one post in a hundred and seventy-five. Reserving
		// a few slots is the honest fix: a day always carries some of the
		// league's state, and it never carries much.
		events = [
			...trimDayEvents([...gameEvents, ...news], { limit: EVENTS_PER_DAY }),
			...trimDayEvents(season, { limit: SEASON_EVENTS_PER_DAY }),
		].sort((a, b) => a.order - b.order);
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
	templateId: string;
	// Where the event sat in the day, so the clock can put a post about the
	// late game later than a post about the early one.
	eventIndex: number;
	eventCount: number;
	isGame: boolean;
	salience: number;
};

const seedFor = (snapshot: FeedSnapshot, day: number) =>
	`${snapshot.season}|${day}`;

const writeAccountDay = ({
	snapshot,
	account,
	day,
	events,
	seen,
	staleTemplates,
}: {
	snapshot: FeedSnapshot;
	account: ResolvedSocialAccount;
	day: number;
	events: SocialEvent[];
	seen: Set<string>;
	staleTemplates: Set<string>;
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
	const eventIndexById = new Map(events.map((event, i) => [event.id, i]));
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
			staleTemplates,
		});
		if (!written) {
			continue;
		}
		seen.add(normalise(written.core));
		seen.add(normalise(written.text));
		staleTemplates.add(written.templateId);
		out.push({
			eventId: event.id,
			core: written.core,
			text: written.text,
			templateId: written.templateId,
			eventIndex: eventIndexById.get(event.id) ?? 0,
			eventCount: events.length,
			isGame: event.type === "gameResult" || event.type === "performance",
			salience: event.salience,
		});
	}
	pool.endBatch();
	return out;
};

const toFeedPost = (
	account: ResolvedSocialAccount,
	post: AccountDayPost,
	seed: string,
): FeedPost => {
	const reach = reachOf(
		account,
		account.pid === undefined ? 0.5 : (notabilityByPid.get(account.pid) ?? 0.4),
	);
	const time = timeOf({
		eventIndex: post.eventIndex,
		eventCount: post.eventCount,
		isGame: post.isGame,
		seed: `${seed}|${account.id}|${post.eventId}`,
	});
	return {
		id: `${account.id}|${post.eventId}`,
		accountId: account.id,
		handle: account.handle,
		name: account.name,
		kind: account.kind,
		tid: account.tid,
		pid: account.pid,
		text: post.text,
		eventId: post.eventId,
		verified: isVerified(account),
		time: time.label,
		minutes: time.minutes,
		engagement: engagementFor({
			account,
			reach,
			salience: post.salience,
			seed: `${seed}|${account.id}|${post.eventId}`,
		}),
		replies: [],
	};
};

// ONE ACCOUNT'S OWN DAY.
//
// An account remembers what it ACTUALLY POSTED on its last MEMORY_DAYS days,
// not what it would have posted with no memory. The difference is not
// academic: the first version compared today against a memoryless re-render of
// last week, and on a day where memory had pushed the account off its first
// choice, the two disagreed - so a line it really had posted three days ago
// was invisible, and it posted it again. That showed up in the very first
// screenshot as one player saying "Effort was there. Execution was not." twice
// in four days.
//
// So days chain. Each is cached under its own day key, which depends only on
// that day's box scores, that day's news and the shape of the roster, so
// simming tomorrow never invalidates today and the chain is walked once.
// LOOKBACK bounds the cold cost of walking the chain. Only the last
// MEMORY_DAYS days have to be exactly right; days before that reach today only
// through their effect on those, so a couple of windows is plenty and the
// seam is never visible. It is the difference between a page that opens in
// three seconds and one that opens in nine.
const LOOKBACK = MEMORY_DAYS * 2;

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
	const keyFor = (i: number) =>
		`${snapshot.dayKey(snapshot.days[i]!)}|${account.id}`;

	const wanted = keyFor(dayIndex);
	const hit = accountDayCache.get(wanted);
	if (hit) {
		return hit;
	}

	// Walk forward from the oldest day that could still be remembered, filling
	// the cache as it goes. Cached days are skipped, so this is linear the
	// first time an account is opened and free afterwards.
	const start = Math.max(0, dayIndex - LOOKBACK);
	const cores: Set<string>[] = [];
	const shapes: Set<string>[] = [];
	for (let i = start; i <= dayIndex; i++) {
		const key = keyFor(i);
		let posts = accountDayCache.get(key);
		if (!posts) {
			const seen = new Set<string>();
			for (const older of cores.slice(-MEMORY_DAYS)) {
				for (const line of older) {
					seen.add(line);
				}
			}
			const staleTemplates = new Set<string>();
			for (const older of shapes.slice(-MEMORY_DAYS)) {
				for (const id of older) {
					staleTemplates.add(id);
				}
			}
			const events = await eventsForDay(snapshot, snapshot.days[i]!);
			const written = writeAccountDay({
				snapshot,
				account,
				day: snapshot.days[i]!,
				events,
				seen,
				staleTemplates,
			});
			posts = written.map((post) =>
				toFeedPost(account, post, seedFor(snapshot, snapshot.days[i]!)),
			);
			for (const [n, post] of written.entries()) {
				coreByPostId.set(posts[n]!.id, normalise(post.core));
			}
			bounded(accountDayCache, 200_000);
			accountDayCache.set(key, posts);
			lineCache.set(
				key,
				new Set(
					written.flatMap((post) => [
						normalise(post.core),
						normalise(post.text),
					]),
				),
			);
			shapeCache.set(key, new Set(written.map((post) => post.templateId)));
		}
		cores.push(lineCache.get(key) ?? new Set());
		shapes.push(shapeCache.get(key) ?? new Set());
	}

	return accountDayCache.get(wanted) ?? [];
};

// What one account has said in the days it can still remember. Reads the
// caches the chain filled, so it is a lookup rather than more generation.
const memoryOf = (
	snapshot: FeedSnapshot,
	account: ResolvedSocialAccount,
	dayIndex: number,
): { lines: Set<string>; shapes: Set<string> } => {
	const lines = new Set<string>();
	const shapes = new Set<string>();
	for (let j = 0; j < MEMORY_DAYS; j++) {
		const older = snapshot.days[dayIndex - j];
		if (older === undefined) {
			break;
		}
		const key = `${snapshot.dayKey(older)}|${account.id}`;
		for (const line of lineCache.get(key) ?? []) {
			lines.add(line);
		}
		for (const id of shapeCache.get(key) ?? []) {
			shapes.add(id);
		}
	}
	return { lines, shapes };
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
		const core = coreByPostId.get(post.id);
		if (said.has(line) || (core !== undefined && said.has(core))) {
			continue;
		}
		said.add(line);
		if (core !== undefined) {
			said.add(core);
		}
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
		await buildAccountDay({ snapshot, account: replier, dayIndex });
		await buildAccountDay({ snapshot, account: poster, dayIndex });
		const { lines: recent, shapes: recentShapes } = memoryOf(
			snapshot,
			replier,
			dayIndex,
		);
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
			staleTemplates: recentShapes,
			avoid: (core, text) =>
				said.has(normalise(text)) ||
				said.has(normalise(core)) ||
				recent.has(normalise(core)) ||
				recent.has(normalise(text)),
		});
		if (!written) {
			continue;
		}
		said.add(normalise(written.text));
		said.add(normalise(written.core));
		// A reply lands after the post it answers, which is the one ordering
		// rule a thread cannot break.
		const replyMinutes = Math.min(
			24 * 60 - 1,
			parent.minutes +
				1 +
				Math.floor(
					rngFromSeed(hashSeed(`t|${parent.id}|${replier.id}`))() * 40,
				),
		);
		const h = Math.floor(replyMinutes / 60);
		const m = replyMinutes % 60;
		const replyRecord = {
			id: `${reply.accountId}|${parent.id}`,
			accountId: replier.id,
			handle: replier.handle,
			name: replier.name,
			kind: replier.kind,
			tid: replier.tid,
			pid: replier.pid,
			text: written.text,
			quote: reply.kind === "quote",
			verified: isVerified(replier),
			time: `${h % 12 === 0 ? 12 : h % 12}:${String(m).padStart(2, "0")} ${h >= 12 ? "PM" : "AM"}`,
			engagement: engagementFor({
				account: replier,
				reach: reachOf(
					replier,
					replier.pid === undefined
						? 0.5
						: (notabilityByPid.get(replier.pid) ?? 0.4),
				),
				salience: event.salience,
				seed: `${seed}|re|${replier.id}|${parent.id}`,
				isReply: true,
				parentLikes: parent.engagement.likes,
			}),
		};
		parent.replies.push(replyRecord);

		// THE ANSWER BACK. When there is real history between them, the
		// original poster does not let it go - which is the whole point of
		// deriving feuds in the first place, and was invisible while every
		// thread stopped after one reply.
		if (reply.heat < 0.5 || poster.personality.replyiness < 0.15) {
			continue;
		}
		const own = memoryOf(snapshot, poster, dayIndex);
		const back = writeReplyDetailed({
			account: poster,
			parent: replier,
			event,
			heat: reply.heat,
			pool,
			rng: rngFromSeed(hashSeed(`${seed}|back|${poster.id}|${replier.id}`)),
			// The POSTER's memory, not the replier's. Getting this wrong put
			// self-repeats back into a feed that had none.
			staleTemplates: own.shapes,
			avoid: (core, text) =>
				said.has(normalise(text)) ||
				said.has(normalise(core)) ||
				own.lines.has(normalise(core)) ||
				own.lines.has(normalise(text)),
		});
		if (!back) {
			continue;
		}
		said.add(normalise(back.text));
		said.add(normalise(back.core));
		const backMinutes = Math.min(
			24 * 60 - 1,
			replyMinutes +
				2 +
				Math.floor(
					rngFromSeed(
						hashSeed(`tb|${parent.id}|${poster.id}|${replier.id}`),
					)() * 25,
				),
		);
		const bh = Math.floor(backMinutes / 60);
		const bm = backMinutes % 60;
		parent.replies.push({
			// Keyed by WHO is being answered: a post that draws two heated
			// replies draws two answers back, and without the replier in the
			// key they were the same row twice.
			id: `${poster.id}|back|${replier.id}|${parent.id}`,
			accountId: poster.id,
			handle: poster.handle,
			name: poster.name,
			kind: poster.kind,
			tid: poster.tid,
			pid: poster.pid,
			text: back.text,
			quote: false,
			verified: isVerified(poster),
			time: `${bh % 12 === 0 ? 12 : bh % 12}:${String(bm).padStart(2, "0")} ${bh >= 12 ? "PM" : "AM"}`,
			engagement: engagementFor({
				account: poster,
				reach: reachOf(
					poster,
					poster.pid === undefined
						? 0.5
						: (notabilityByPid.get(poster.pid) ?? 0.4),
				),
				salience: event.salience,
				seed: `${seed}|back|${poster.id}|${replier.id}|${parent.id}`,
				isReply: true,
				parentLikes: replyRecord.engagement.likes,
			}),
			replyTo: replier.handle,
		});
	}
	pool.endBatch();

	// A timeline is newest-first, and the clock is what says which is newest.
	out.sort((a, b) => b.minutes - a.minutes || a.id.localeCompare(b.id));

	const feed: FeedDay = { season: snapshot.season, day, posts: out };
	bounded(feedCache, 400);
	feedCache.set(key, feed);
	return feed;
};
