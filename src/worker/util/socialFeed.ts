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
	type SocialEventType,
	streakFlipsForDay,
} from "../../common/socialEvents.ts";
import {
	castDay,
	castReplies,
	type SocialCasting,
} from "../../common/socialCasting.ts";
import { feudHeat, rivalryFrom } from "../../common/socialFeuds.ts";
import { socialAccountPicture } from "./socialFaces.ts";
import {
	engagementFor,
	isVerified,
	reachOf,
	timeOf,
	type AccountPicture,
	type Engagement,
} from "../../common/socialMetrics.ts";
import {
	playerReceiptReplyText,
	playerReceiptText,
	receiptReplyText,
	receiptText,
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
	// Which voice this is (beat writer, homer fan, insider...), so a page can
	// style by role and the timeline can be measured by who is on it.
	archetypeId: string;
	tid?: number;
	pid?: number;
	text: string;
	eventId: string;
	// WHAT THE POST IS ABOUT, so a player page can find the posts about him
	// and a box score the posts about that game without reading the text.
	eventType: SocialEventType;
	tids: number[];
	pids: number[];
	gid?: number;
	verified: boolean;
	// A RECEIPT: this post quotes something another account (or this one)
	// said on an earlier day, re-derived rather than stored. The day is the
	// league day it was said, for the stamp on the embed.
	quoted?: {
		accountId: string;
		handle: string;
		name: string;
		kind: "player" | "team" | "media";
		verified: boolean;
		text: string;
		day: number;
	};
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
		archetypeId: string;
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
// League news - a trade, an injury, a signing - gets its own reserved slots.
// Trimmed in one pool with fifteen box scores it lost every cut: a sprained
// ankle scores below a forty-point night, quite correctly, and so the
// insiders and the wire never had anything to break. Four game days of a
// real league produced two hundred and sixty posts and not one from either.
const NEWS_EVENTS_PER_DAY = 6;
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

		// EVERYONE ELSE IS A PERSON. A franchise is a logo and a player is the
		// face the league already drew, but the beat writer, the insider and
		// the fans are people, and a pair of initials on a tinted circle is
		// what made a timeline read as a roster with captions. The face is
		// derived from the account id, so it costs nothing to store and every
		// device computes the same one - the same contract the posts keep.
		out[account.id] = socialAccountPicture(
			account.id,
			account.archetypeId,
			colors,
		);
	}
	return out;
};

// WHAT WAS SAID ABOUT THIS ACCOUNT, which is the other half of a profile.
//
// The posts tab is what an account SAID and the replies tab is what it said
// under other people. Neither answers the question a profile is usually opened
// to ask - who is talking about him - and for a player that is most of the
// reason to be on the page at all.
//
// A mention is anything somebody ELSE published that points here, and there
// are four ways to point:
//
//   SUBJECT   the post is about this player or this team. Posts already carry
//             pids and tids for exactly this, so it costs no text search.
//   QUOTED    somebody pulled up something this account said on an earlier
//             day - the receipts.
//   BY NAME   the handle appears in the text. Replies carry the handle of what
//             they answer, so a reply under this account's post is an @mention
//             of it and lands here without a special case.
//   ANSWERED  somebody replied under this account's own post. The post is this
//             account's, so it is not itself a mention, but the conversation
//             under it is - and the thread is the only unit that reads
//             properly, so the post comes along to carry its replies.
//
// The unit is the POST, not the sentence, because a reply without the thing it
// answers is a one-liner with no context - the same reason the replies tab
// carries its parent.
export type MentionTarget = {
	id: string;
	handle: string;
	kind: "player" | "team" | "media";
	pid?: number;
	tid?: number;
};

// Word-bounded, so @ram does not match @rambis, and escaped because handles
// are cut from real names and a name can carry a dot.
const handleMention = (handle: string) =>
	new RegExp(`@${handle.replaceAll(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");

// The predicate on its own, so it can be tested against hand-written posts
// without a league behind it. The walk below is just "apply this to the last
// fortnight".
export const isMentionOf = (
	post: Pick<
		FeedPost,
		"accountId" | "text" | "pid" | "pids" | "tid" | "tids" | "quoted" | "replies"
	>,
	account: MentionTarget,
): boolean => {
	const handle = handleMention(account.handle);
	if (post.accountId === account.id) {
		// Not a mention of itself. It earns a place only when somebody else
		// turned up underneath it, and then the post is here to carry them.
		return post.replies.some((r) => r.accountId !== account.id);
	}
	// BEING THE SUBJECT, which only a player's or a franchise's account can be.
	// A media or fan account carries a tid too - it is the club they cover or
	// support - and matching on that made a fan's mentions tab an exact copy of
	// their team's feed, every post about the Cheesesteaks filed as somebody
	// talking about @CasualCheeseste. Supporting a team is not being one.
	const aboutThem =
		(account.kind === "player" &&
			account.pid !== undefined &&
			(post.pid === account.pid || post.pids.includes(account.pid))) ||
		(account.kind === "team" &&
			account.tid !== undefined &&
			account.tid >= 0 &&
			(post.tid === account.tid || post.tids.includes(account.tid)));
	return (
		aboutThem ||
		post.quoted?.accountId === account.id ||
		handle.test(post.text) ||
		post.replies.some(
			(r) => r.accountId !== account.id && handle.test(r.text),
		)
	);
};

export const mentionsOf = async ({
	snapshot,
	account,
	daysBack = 14,
	limit = 40,
}: {
	snapshot: FeedSnapshot;
	account: MentionTarget;
	daysBack?: number;
	limit?: number;
}): Promise<(FeedPost & { day: number })[]> => {
	const out: (FeedPost & { day: number })[] = [];
	const start = snapshot.days.length - 1;
	const stop = Math.max(0, start - daysBack + 1);
	for (
		let dayIndex = start;
		dayIndex >= stop && out.length < limit;
		dayIndex--
	) {
		const feedDay = await buildFeedDay({ snapshot, dayIndex });
		for (const post of feedDay.posts) {
			if (out.length >= limit) {
				break;
			}
			if (isMentionOf(post, account)) {
				out.push({ ...post, day: feedDay.day });
			}
		}
	}
	return out;
};

// The days worth showing, newest first.
// THE FEED ABOUT ONE THING - a player, a team or a game - for the pages that
// embed it. Walks the timeline newest-first and keeps the posts whose subject
// matches, up to a limit, so a player page shows what was said about him
// this week and a box score shows the reactions to that night.
export const feedAbout = async ({
	season,
	tid,
	pid,
	gid,
	day,
	limit = 6,
	daysBack = 6,
}: {
	season: number;
	tid?: number;
	pid?: number;
	gid?: number;
	// Restrict to one day of the timeline (a game's night).
	day?: number;
	limit?: number;
	daysBack?: number;
}): Promise<{
	posts: (FeedPost & { day: number })[];
	pictures: Record<string, AccountPicture>;
	teams: {
		tid: number;
		abbrev: string;
		region: string;
		name: string;
		imgURL?: string;
		colors?: [string, string, string];
	}[];
	handle?: string;
}> => {
	const snapshot = await getFeedSnapshot(season);
	const matches = (post: FeedPost) =>
		(pid !== undefined && (post.pid === pid || post.pids.includes(pid))) ||
		(tid !== undefined && (post.tid === tid || post.tids.includes(tid))) ||
		(gid !== undefined && post.gid === gid);

	const posts: (FeedPost & { day: number })[] = [];
	const start = snapshot.days.length - 1;
	const stop = day === undefined ? Math.max(0, start - daysBack + 1) : 0;
	for (
		let dayIndex = start;
		dayIndex >= stop && posts.length < limit;
		dayIndex--
	) {
		const d = snapshot.days[dayIndex]!;
		if (day !== undefined && d !== day) {
			continue;
		}
		const feedDay = await buildFeedDay({ snapshot, dayIndex });
		for (const post of feedDay.posts) {
			if (posts.length >= limit) {
				break;
			}
			if (matches(post)) {
				posts.push({ ...post, day: feedDay.day });
			}
		}
		if (day !== undefined) {
			break;
		}
	}

	const onPage = new Set<string>();
	for (const post of posts) {
		onPage.add(post.accountId);
		for (const reply of post.replies) {
			onPage.add(reply.accountId);
		}
	}
	const pictures = await picturesFor(
		snapshot,
		snapshot.accounts.filter((a) => onPage.has(a.id)),
	);
	const teams = (await idb.cache.teams.getAll()).map((t) => ({
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		imgURL: t.imgURL,
		colors: t.colors,
	}));
	// The subject's own handle, so a page can link to ITS profile rather than
	// to the whole timeline. A team has an account exactly as a player does,
	// and a team page had been sending people to the league timeline instead.
	const handle =
		pid !== undefined
			? snapshot.accounts.find((a) => a.pid === pid)?.handle
			: tid !== undefined && tid >= 0
				? snapshot.accounts.find((a) => a.kind === "team" && a.tid === tid)
						?.handle
				: undefined;
	return { posts, pictures, teams, handle };
};

// WHO TO FOLLOW: the national insider, and the user's own beat writer and
// loudest fan - the three accounts a person opening this feed for the first
// time would actually want, and the ones that make the sidebar read as
// theirs rather than as a random sample of seven hundred.
// THE CHATTER UNDER A NEWS STORY. News pages and transaction logs list
// league events by eid; every feed post written about a league event carries
// that eid inside its eventId ("e:123"), so the posts about a story can be
// collected without reading any text. Only days the feed has ALREADY built,
// plus the most recent stretch, are generated here - a season of history must
// not cost a season of feed generation the first time the news page opens.
// Chatter fading off old stories is also just true to life.
export const feedAboutLeagueEvents = async ({
	season,
	eids,
	limitPerEvent = 2,
	daysBack = 14,
	textByEid,
}: {
	season: number;
	eids: number[];
	limitPerEvent?: number;
	daysBack?: number;
	// The story text each eid renders with, so a post that merely restates it
	// word for word (the wire accounts do) is not hung underneath as if it
	// were a reaction.
	textByEid?: Map<number, string>;
}): Promise<{
	postsByEid: Record<number, (FeedPost & { day: number })[]>;
	pictures: Record<string, AccountPicture>;
	teams: {
		tid: number;
		abbrev: string;
		region: string;
		name: string;
		imgURL?: string;
		colors?: [string, string, string];
	}[];
}> => {
	const snapshot = await getFeedSnapshot(season);
	const wanted = new Set(eids);

	// Which day each wanted event was placed on.
	const dayByEid = new Map<number, number>();
	for (const [day, events] of snapshot.leagueEventsByDay) {
		for (const event of events) {
			if (event.id.startsWith("e:")) {
				const eid = Number(event.id.slice(2));
				if (wanted.has(eid)) {
					dayByEid.set(eid, day);
				}
			}
		}
	}

	const recent = new Set(snapshot.days.slice(-daysBack));
	const days = new Set<number>();
	for (const day of dayByEid.values()) {
		if (recent.has(day) || feedCache.has(snapshot.dayKey(day))) {
			days.add(day);
		}
	}

	const postsByEid: Record<number, (FeedPost & { day: number })[]> = {};
	const onPage = new Set<string>();
	for (const day of days) {
		const dayIndex = snapshot.days.indexOf(day);
		if (dayIndex < 0) {
			continue;
		}
		const feedDay = await buildFeedDay({ snapshot, dayIndex });
		for (const post of feedDay.posts) {
			if (!post.eventId.startsWith("e:")) {
				continue;
			}
			const eid = Number(post.eventId.slice(2));
			if (!wanted.has(eid)) {
				continue;
			}
			const story = textByEid?.get(eid);
			if (story !== undefined) {
				const a = normalise(post.text);
				const b = normalise(story);
				if (a.length > 0 && b.length > 0 && (a.includes(b) || b.includes(a))) {
					continue;
				}
			}
			const list = (postsByEid[eid] ??= []);
			if (list.length >= limitPerEvent) {
				continue;
			}
			list.push({ ...post, day });
			onPage.add(post.accountId);
			for (const reply of post.replies) {
				onPage.add(reply.accountId);
			}
		}
	}

	const pictures = await picturesFor(
		snapshot,
		snapshot.accounts.filter((a) => onPage.has(a.id)),
	);
	const teams = (await idb.cache.teams.getAll()).map((t) => ({
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		imgURL: t.imgURL,
		colors: t.colors,
	}));
	return { postsByEid, pictures, teams };
};

export const suggestedAccounts = (
	snapshot: FeedSnapshot,
	userTid: number,
): ResolvedSocialAccount[] => {
	const ids = [
		"m:cast:nat0",
		`m:cast:beat:${userTid}`,
		`m:cast:homer:${userTid}`,
		`m:cast:casual:${userTid}`,
	];
	const out: ResolvedSocialAccount[] = [];
	for (const id of ids) {
		const account = snapshot.accountById.get(id);
		if (account) {
			out.push(account);
		}
	}
	return out.slice(0, 3);
};

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
const templateByPostId = new Map<string, string>();
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
	templateByPostId.clear();
	flipsCache.clear();
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
			...trimDayEvents(gameEvents, { limit: EVENTS_PER_DAY }),
			...trimDayEvents(news, { limit: NEWS_EVENTS_PER_DAY }),
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
	eventType: SocialEventType;
	tids: number[];
	pids: number[];
	gid?: number;
};

// The game an event belongs to, read off the id the event builder wrote.
const gidOf = (event: SocialEvent): number | undefined => {
	const m = /^(?:g|perf):(\d+)/.exec(event.id);
	return m ? Number(m[1]) : undefined;
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
			eventType: event.type,
			tids: event.tids,
			pids: event.pids,
			gid: gidOf(event),
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
		archetypeId: account.archetypeId,
		tid: account.tid,
		pid: account.pid,
		text: post.text,
		eventId: post.eventId,
		eventType: post.eventType,
		tids: post.tids,
		pids: post.pids,
		gid: post.gid,
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
				templateByPostId.set(posts[n]!.id, post.templateId);
			}
			bounded(accountDayCache, 200_000);
			bounded(coreByPostId, 400_000);
			bounded(templateByPostId, 400_000);
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

	const own = accountDayCache.get(wanted) ?? [];
	// The author's profile carries his receipts too - a post that only existed
	// on the timeline would break the rule that a profile is everything the
	// account said.
	const extras: FeedPost[] = [];
	const receipt = await receiptPostFor(snapshot, dayIndex);
	if (receipt && receipt.accountId === account.id) {
		extras.push({ ...receipt, replies: [] });
	}
	const playerReceipt = await playerReceiptPostFor(snapshot, dayIndex);
	if (playerReceipt && playerReceipt.accountId === account.id) {
		extras.push({ ...playerReceipt, replies: [] });
	}
	return extras.length > 0 ? [...own, ...extras] : own;
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

// ---------------------------------------------------------------- RECEIPTS
//
// THE PROMISE OF MEMORY, KEPT. The cast spends whole weeks saying
// "screenshotting this for April" and "revisiting this in a month", and for
// as long as none of it ever came back, the feed was gesturing at a memory it
// did not have. A receipt is the payoff: on the day a team's story flips -
// four straight wins where there was a four-game skid, or the reverse - the
// rival mood account digs up what the other one actually said during the old
// run and quotes it, word for word, with the day it was said.
//
// Nothing is stored to make this work. The old post is re-derived exactly the
// way the profile page would derive it, which is what guarantees the receipt
// quotes something the reader can go and find.

const flipsCache = new Map<number, ReturnType<typeof streakFlipsForDay>>();

const flipsForDay = (snapshot: FeedSnapshot, day: number) => {
	let flips = flipsCache.get(day);
	if (!flips) {
		flips = streakFlipsForDay(snapshot.standingsInput, day);
		flipsCache.set(day, flips);
		bounded(flipsCache as any, 2000);
	}
	return flips;
};

const fanOf = (
	accounts: ResolvedSocialAccount[],
	tid: number,
	archetypeId: string,
): ResolvedSocialAccount | undefined =>
	accounts.find(
		(a) =>
			a.archetypeId === archetypeId &&
			(a.personality.loyaltyTid ?? a.tid) === tid,
	);

export const receiptPostFor = async (
	snapshot: FeedSnapshot,
	dayIndex: number,
): Promise<FeedPost | undefined> => {
	const day = snapshot.days[dayIndex];
	if (day === undefined) {
		return undefined;
	}
	for (const flip of flipsForDay(snapshot, day)) {
		// risen: the homer quotes the doomer, who spent the skid selling the
		// team. fallen: the doomer quotes the homer, who spent the streak
		// crowning it. When only the quoted account exists, it quotes itself,
		// which is the rarer and better joke.
		const targetArch = flip.kind === "risen" ? "doomerFan" : "homerFan";
		const authorArch = flip.kind === "risen" ? "homerFan" : "doomerFan";
		const target = fanOf(snapshot.accounts, flip.tid, targetArch);
		if (!target) {
			continue;
		}
		const author = fanOf(snapshot.accounts, flip.tid, authorArch) ?? target;
		const self = author.id === target.id;

		// What he actually said, most recent regret first. Only plain posts
		// about the team qualify - a receipt of a receipt is a hall of mirrors.
		let old: FeedPost | undefined;
		let oldDay: number | undefined;
		for (const rd of flip.regretDays.slice(0, 4)) {
			const di = snapshot.days.indexOf(rd);
			if (di < 0) {
				continue;
			}
			const targetDay = await buildAccountDay({
				snapshot,
				account: target,
				dayIndex: di,
			});
			old = targetDay.find(
				(post) =>
					post.quoted === undefined &&
					(post.eventType === "gameResult" || post.eventType === "standings") &&
					post.tids.includes(flip.tid),
			);
			if (old) {
				oldDay = rd;
				break;
			}
		}
		if (!old || oldDay === undefined) {
			continue;
		}

		const seed = seedFor(snapshot, day);
		const rng = rngFromSeed(hashSeed(`${seed}|receipt|${flip.tid}`));
		const pool = createPhrasePool();
		const text = receiptText({
			kind: flip.kind,
			self,
			rng,
			pick: pool.pick,
		});
		const reach = reachOf(author, 0.5);
		// Late morning, when yesterday has sunk in - before tonight's games,
		// after the box scores.
		const minutes = 10 * 60 + Math.floor(rng() * 150);
		const h = Math.floor(minutes / 60);
		const m = minutes % 60;
		const label = `${h % 12 === 0 ? 12 : h % 12}:${String(m).padStart(2, "0")} ${h >= 12 ? "PM" : "AM"}`;
		const post: FeedPost = {
			id: `receipt|${flip.tid}|${day}`,
			accountId: author.id,
			handle: author.handle,
			name: author.name,
			kind: author.kind,
			archetypeId: author.archetypeId,
			tid: author.tid,
			pid: author.pid,
			text,
			eventId: `receipt|${flip.tid}|${day}`,
			eventType: "standings",
			tids: [flip.tid],
			pids: [],
			verified: isVerified(author),
			quoted: {
				accountId: target.id,
				handle: target.handle,
				name: target.name,
				kind: target.kind,
				verified: isVerified(target),
				text: old.text,
				day: oldDay,
			},
			time: label,
			minutes,
			// A receipt travels: it is the kind of post a whole fanbase passes
			// around, so it earns big-moment engagement whatever the author's
			// usual reach is.
			engagement: engagementFor({
				account: author,
				reach,
				salience: 0.9,
				seed: `${seed}|receipt|${flip.tid}`,
			}),
			replies: [],
		};
		if (!self) {
			// He never concedes gracefully.
			const replyMinutes = Math.min(24 * 60 - 1, minutes + 9);
			const rh = Math.floor(replyMinutes / 60);
			const rm = replyMinutes % 60;
			post.replies.push({
				id: `${target.id}|receipt|${post.id}`,
				accountId: target.id,
				handle: target.handle,
				name: target.name,
				kind: target.kind,
				archetypeId: target.archetypeId,
				tid: target.tid,
				pid: target.pid,
				text: receiptReplyText({ kind: flip.kind, rng, pick: pool.pick }),
				quote: false,
				verified: isVerified(target),
				time: `${rh % 12 === 0 ? 12 : rh % 12}:${String(rm).padStart(2, "0")} ${rh >= 12 ? "PM" : "AM"}`,
				engagement: engagementFor({
					account: target,
					reach: reachOf(target, 0.5),
					salience: 0.9,
					seed: `${seed}|receiptreply|${flip.tid}`,
					isReply: true,
					parentLikes: post.engagement.likes,
				}),
			});
		}
		return post;
	}
	return undefined;
};

// THE PLAYER'S RECEIPT, same promise in the singular. The cold-night events
// give the snark accounts a record of writing a player off; on the night he
// answers with forty, the feed digs that post back up. The player quotes it
// himself when the cast gave him an account - the internet's favorite genre
// - and his team's homer does the honors otherwise.
//
// Cached per day: buildAccountDay re-derives earlier days through here, so
// without the cache every profile walk would re-run the search.
const playerReceiptCache = new Map<string, FeedPost | undefined>();

export const playerReceiptPostFor = async (
	snapshot: FeedSnapshot,
	dayIndex: number,
): Promise<FeedPost | undefined> => {
	const day = snapshot.days[dayIndex];
	if (day === undefined) {
		return undefined;
	}
	const cacheKey = snapshot.dayKey(day);
	if (playerReceiptCache.has(cacheKey)) {
		return playerReceiptCache.get(cacheKey);
	}
	// Set the key BEFORE the search: the search walks earlier days, and if one
	// of those walks somehow re-entered this day it would recurse forever.
	playerReceiptCache.set(cacheKey, undefined);
	bounded(playerReceiptCache as any, 2000);

	const events = await eventsForDay(snapshot, day);
	const monsters = events
		.filter(
			(event) =>
				event.type === "performance" &&
				!event.id.endsWith(":cold") &&
				typeof event.facts.pts === "number" &&
				event.facts.pts >= 40,
		)
		.sort((a, b) => b.salience - a.salience);
	for (const event of monsters) {
		const pid = event.pids[0];
		const tid = event.tids[0];
		if (pid === undefined || tid === undefined) {
			continue;
		}
		// Only somebody who would actually have written him off: the league
		// troll, his own team's doomer, an analytics account that watches his
		// team (or the national one).
		const doubters = snapshot.accounts.filter(
			(a) =>
				a.archetypeId === "troll" ||
				(a.archetypeId === "analytics" &&
					(a.tid === undefined ||
						(a.personality.loyaltyTid ?? a.tid) === tid)) ||
				(a.archetypeId === "doomerFan" &&
					(a.personality.loyaltyTid ?? a.tid) === tid),
		);
		let old: FeedPost | undefined;
		let oldDay: number | undefined;
		let target: ResolvedSocialAccount | undefined;
		outer: for (
			let di = dayIndex - 1;
			di >= Math.max(0, dayIndex - MEMORY_DAYS);
			di--
		) {
			// A cold post older than the player's LAST forty is already
			// answered. Without this stop, two monster nights a week apart
			// both dug up the same day-one quote, word for word.
			const priorEvents = await eventsForDay(snapshot, snapshot.days[di]!);
			if (
				priorEvents.some(
					(e) =>
						e.type === "performance" &&
						!e.id.endsWith(":cold") &&
						e.pids.includes(pid) &&
						typeof e.facts.pts === "number" &&
						e.facts.pts >= 40,
				)
			) {
				break outer;
			}
			for (const account of doubters) {
				const posts = await buildAccountDay({
					snapshot,
					account,
					dayIndex: di,
				});
				const hit = posts.find(
					(post) =>
						post.quoted === undefined &&
						/^perf:\d+:\d+:cold$/.test(post.eventId) &&
						post.pids.includes(pid),
				);
				if (hit) {
					old = hit;
					oldDay = snapshot.days[di];
					target = account;
					break outer;
				}
			}
		}
		if (!old || oldDay === undefined || !target) {
			continue;
		}

		const author =
			snapshot.accounts.find((a) => a.pid === pid) ??
			fanOf(snapshot.accounts, tid, "homerFan");
		if (!author) {
			continue;
		}

		const seed = seedFor(snapshot, day);
		const rng = rngFromSeed(hashSeed(`${seed}|preceipt|${pid}`));
		const pool = createPhrasePool();
		const text = playerReceiptText({
			self: author.pid === pid,
			pts: event.facts.pts as number,
			rng,
			pick: pool.pick,
		});
		const reach = reachOf(
			author,
			author.pid === undefined ? 0.5 : (notabilityByPid.get(author.pid) ?? 0.4),
		);
		// Late night, once the box score is real and the quote is unbearable.
		const minutes = 22 * 60 + 30 + Math.floor(rng() * 85);
		const h = Math.floor(minutes / 60);
		const m = minutes % 60;
		const post: FeedPost = {
			id: `preceipt|${pid}|${day}`,
			accountId: author.id,
			handle: author.handle,
			name: author.name,
			kind: author.kind,
			archetypeId: author.archetypeId,
			tid: author.tid,
			pid: author.pid,
			text,
			eventId: `preceipt|${pid}|${day}`,
			eventType: "performance",
			tids: [tid],
			pids: [pid],
			verified: isVerified(author),
			quoted: {
				accountId: target.id,
				handle: target.handle,
				name: target.name,
				kind: target.kind,
				verified: isVerified(target),
				text: old.text,
				day: oldDay,
			},
			time: `${h % 12 === 0 ? 12 : h % 12}:${String(m).padStart(2, "0")} ${h >= 12 ? "PM" : "AM"}`,
			minutes,
			engagement: engagementFor({
				account: author,
				reach,
				salience: 0.9,
				seed: `${seed}|preceipt|${pid}`,
			}),
			replies: [],
		};
		// The doubter never concedes.
		const replyMinutes = Math.min(24 * 60 - 1, minutes + 11);
		const rh = Math.floor(replyMinutes / 60);
		const rm = replyMinutes % 60;
		post.replies.push({
			id: `${target.id}|preceipt|${post.id}`,
			accountId: target.id,
			handle: target.handle,
			name: target.name,
			kind: target.kind,
			archetypeId: target.archetypeId,
			tid: target.tid,
			pid: target.pid,
			text: playerReceiptReplyText({ rng, pick: pool.pick }),
			quote: false,
			verified: isVerified(target),
			time: `${rh % 12 === 0 ? 12 : rh % 12}:${String(rm).padStart(2, "0")} ${rh >= 12 ? "PM" : "AM"}`,
			engagement: engagementFor({
				account: target,
				reach: reachOf(target, 0.5),
				salience: 0.9,
				seed: `${seed}|preceiptreply|${pid}`,
				isReply: true,
				parentLikes: post.engagement.likes,
			}),
		});
		playerReceiptCache.set(cacheKey, post);
		return post;
	}
	return undefined;
};

// ---------------------------------------------------------------- LANES
//
// WHO THE TIMELINE IS MADE OF. Left to interest alone, a day was a quarter
// local radio, a quarter homer fans and a seventh official team accounts,
// with players at one post in twenty - the loudest and the most corporate
// voices, because both post every night about everything. A real timeline
// is mostly fans and the beat, the news breakers when there is news, and
// the players themselves now and then. So each day is filled by lane, in
// score order within the lane, and a lane that runs out hands its slots to
// the others - except the franchise and radio lanes, which are capped for
// good: nobody wants more press releases.

export type FeedLane =
	| "fan"
	| "beat"
	| "radio"
	| "national"
	| "player"
	| "team";

export const LANE_SHARE: Record<FeedLane, number> = {
	fan: 0.36,
	beat: 0.2,
	radio: 0.07,
	national: 0.12,
	player: 0.17,
	team: 0.08,
};

const FAN_ARCHETYPES = new Set(["homerFan", "doomerFan", "casualFan", "troll"]);

export const laneOf = (account: {
	kind: "player" | "team" | "media";
	archetypeId: string;
	tid?: number;
}): FeedLane => {
	if (account.kind === "player") {
		return "player";
	}
	if (account.kind === "team") {
		return "team";
	}
	if (FAN_ARCHETYPES.has(account.archetypeId)) {
		return "fan";
	}
	if (account.archetypeId === "localRadio") {
		return "radio";
	}
	// The beat is everyone covering ONE team for a living: the writer and
	// the local film-room account. The national lane is the accounts with
	// no team at all - the insiders, the wire, the columnists - which the
	// thirty film rooms were crowding out of it.
	if (account.tid !== undefined) {
		return "beat";
	}
	return "national";
};

// Lanes that may take the slots another lane could not fill.
const OVERFLOW_LANES = new Set<FeedLane>(["fan", "beat", "national", "player"]);

export const laneCaps = (total: number): Record<FeedLane, number> => {
	const out = {} as Record<FeedLane, number>;
	for (const lane of Object.keys(LANE_SHARE) as FeedLane[]) {
		out[lane] = Math.max(1, Math.ceil(total * LANE_SHARE[lane]));
	}
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
	const caps = laneCaps(POSTS_PER_DAY);
	const perLane = new Map<FeedLane, number>();
	const taken = new Set<string>();
	const dayTemplates = new Set<string>();

	// Two passes: first every lane up to its share, then the lanes allowed to
	// overflow take whatever is left, still in score order.
	for (const pass of ["lanes", "overflow"] as const) {
		for (const candidate of everyone) {
			if (out.length >= POSTS_PER_DAY) {
				break;
			}
			const key = `${candidate.accountId}|${candidate.eventId}`;
			if (taken.has(key)) {
				continue;
			}
			if ((perEvent.get(candidate.eventId) ?? 0) >= 4) {
				continue;
			}
			const account = accountById.get(candidate.accountId);
			if (!account) {
				continue;
			}
			const lane = laneOf(account);
			const inLane = perLane.get(lane) ?? 0;
			if (pass === "lanes" ? inLane >= caps[lane] : !OVERFLOW_LANES.has(lane)) {
				continue;
			}
			const own = await buildAccountDay({ snapshot, account, dayIndex });
			const post = own.find((p) => p.eventId === candidate.eventId);
			if (!post) {
				taken.add(key);
				continue;
			}
			// Two accounts landing on the same sentence about the same game is
			// the one repeat memory cannot see, because memory is per account.
			const line = normalise(post.text);
			const core = coreByPostId.get(post.id);
			if (said.has(line) || (core !== undefined && said.has(core))) {
				taken.add(key);
				continue;
			}
			// ONE RUN OF A JOKE PER NIGHT. Exact-line dedupe cannot see "another
			// one. 116-88" and "another one. 124-94" as the same post, but a
			// reader can, and one sample night carried it three times from three
			// fanbases. Flavor lanes get one use of a template per day across
			// every account; the wire lanes are exempt, because eight team
			// accounts posting eight finals in the same shape IS the texture.
			const templateId = templateByPostId.get(post.id);
			if (
				templateId !== undefined &&
				(lane === "fan" || lane === "radio") &&
				dayTemplates.has(templateId)
			) {
				taken.add(key);
				continue;
			}
			said.add(line);
			if (core !== undefined) {
				said.add(core);
			}
			if (templateId !== undefined && (lane === "fan" || lane === "radio")) {
				dayTemplates.add(templateId);
			}
			taken.add(key);
			perEvent.set(
				candidate.eventId,
				(perEvent.get(candidate.eventId) ?? 0) + 1,
			);
			perLane.set(lane, inLane + 1);
			slots.push(candidate);
			out.push({ ...post, replies: [] });
		}
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
			archetypeId: replier.archetypeId,
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
			archetypeId: poster.archetypeId,
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

	// The day's receipts, when their moment has come. Outside the forty-five
	// on purpose: they are rare, and the best posts of the day when they exist.
	const receipt = await receiptPostFor(snapshot, dayIndex);
	if (receipt) {
		out.push(receipt);
	}
	const playerReceipt = await playerReceiptPostFor(snapshot, dayIndex);
	if (playerReceipt) {
		out.push(playerReceipt);
	}

	// A timeline is newest-first, and the clock is what says which is newest.
	out.sort((a, b) => b.minutes - a.minutes || a.id.localeCompare(b.id));

	const feed: FeedDay = { season: snapshot.season, day, posts: out };
	bounded(feedCache, 400);
	feedCache.set(key, feed);
	return feed;
};
