// WHAT HAPPENED, BEFORE ANYONE HAS AN OPINION ABOUT IT.
//
// The feed is generated in two stages that are kept strictly apart. This file
// is the first: turn a day of league activity into a flat list of typed FACTS.
// The second stage decides who cares and what they say about it.
//
// The split is not tidiness. Every number a post is allowed to state has to
// come from here, because a feed that misreports a score is broken rather than
// characterful - see the accuracy dial in socialPersonality. Keeping the facts
// in their own layer means the generator physically cannot invent one: it is
// handed `facts` and nothing else to quote. An account with low accuracy gets
// to be WRONG ABOUT WHAT IT MEANS, never wrong about what happened.
//
// It also means salience is decided once, on the merits, rather than by each
// archetype separately. A 40-point game is a big deal whether the account
// covering it is a homer or a wire service; what changes is the adjective.

import type { SocialTopicWeights } from "./socialPersonality.ts";

export type SocialEventType =
	| "gameResult"
	| "performance"
	| "injury"
	| "trade"
	| "signing"
	| "release"
	| "draft"
	| "award"
	| "milestone"
	| "retirement"
	| "playoffs"
	| "standings";

export type SocialEvent = {
	// Deterministic, and unique within a season. Doubles as the seed for
	// anything generated about this event, so it must not depend on iteration
	// order or wall-clock time.
	id: string;
	type: SocialEventType;
	// Which personality weight decides whether an account cares. Several event
	// types map to one topic on purpose: an account that likes transactions
	// likes trades, signings and releases alike.
	topic: keyof SocialTopicWeights;
	season: number;
	// The schedule day this belongs to. Games know theirs exactly; league
	// events do not record one and are placed by assignEventDays.
	day: number;
	// Position within the day, ascending. Ties are broken here rather than by
	// sort stability, so two devices order a day identically.
	order: number;

	// HOW BIG A DEAL, 0 to 1, decided once and on the merits.
	salience: number;

	tids: number[];
	pids: number[];

	// THE ONLY NUMBERS A POST MAY STATE. Anything not in here is not a fact
	// this event supports, and the generator has no other source.
	facts: Record<string, string | number | boolean>;
};

// ---------------------------------------------------------------- SALIENCE
//
// One scale for everything, so a trade and a 45-point night can be compared
// when the day is trimmed to a readable size. Anchored at the boring end:
// 0.2 is "a thing that happened", 0.5 is "worth a post", 0.8 is "the story of
// the night", 1 is reserved for the genuinely rare.

const clamp01 = (n: number) => Math.max(0, Math.min(1, n));

// A blowout and a one-possession game are both interesting; a nine-point win
// is not. So margin contributes at BOTH ends, which a linear term would miss
// and which is why this is a curve rather than a slope.
export const gameSalience = ({
	margin,
	overtimes,
	playoffs,
	elimination,
	streak,
	upset,
}: {
	margin: number;
	overtimes: number;
	playoffs: boolean;
	// A game that ends a series.
	elimination?: boolean;
	// Length of the winner's streak after this game.
	streak?: number;
	// The underdog won, per the pregame line.
	upset?: boolean;
}): number => {
	const m = Math.abs(margin);
	// Close games peak at 0, blowouts peak past 25, the dead zone is ~10.
	const drama = Math.max(clamp01((8 - m) / 8), clamp01((m - 14) / 16));
	let score = 0.25 + 0.3 * drama;
	if (overtimes > 0) {
		score += 0.12 + 0.04 * Math.min(overtimes - 1, 3);
	}
	if (playoffs) {
		score += 0.2;
	}
	if (elimination) {
		score += 0.15;
	}
	if (upset) {
		score += 0.12;
	}
	if (streak !== undefined && streak >= 5) {
		score += clamp01((streak - 4) / 12) * 0.12;
	}
	return clamp01(score);
};

// A line is interesting because of its size, its shape, or its rarity. Points
// alone would rank a 30-point night on 28 shots over a triple-double.
export const performanceSalience = ({
	pts,
	reb,
	ast,
	stl,
	blk,
	tsp,
	playoffs,
}: {
	pts: number;
	reb: number;
	ast: number;
	stl: number;
	blk: number;
	// True shooting percentage, 0-100. Undefined when it cannot be computed.
	tsp?: number;
	playoffs?: boolean;
}): number => {
	const doubles = [pts, reb, ast, stl, blk].filter((v) => v >= 10).length;
	let score = 0;

	// Scoring, with the interesting range starting where a good night starts.
	score += clamp01((pts - 24) / 26) * 0.5;
	// Shape. A triple-double is a different KIND of night, not a bigger one, so
	// it jumps rather than scaling.
	if (doubles >= 3) {
		score += 0.35;
	} else if (doubles === 2) {
		score += pts >= 20 ? 0.18 : 0.12;
	}
	if (doubles >= 4) {
		score += 0.25;
	}
	// Volume in the other columns, weighted to stand on its own. An earlier
	// version scaled these so gently that a 25-rebound night scored below the
	// threshold to be posted about at all, which is plainly wrong: a man with
	// 25 boards and 6 points is a story even though nobody scored.
	score += clamp01((reb - 12) / 6) * 0.35;
	score += clamp01((ast - 10) / 6) * 0.35;
	score += clamp01((stl - 4) / 3) * 0.2;
	score += clamp01((blk - 4) / 3) * 0.2;
	// Efficiency only counts when the volume is there; a 9-point night on
	// perfect shooting is not a story.
	if (tsp !== undefined && pts >= 20) {
		score += clamp01((tsp - 62) / 20) * 0.1;
	}
	if (playoffs) {
		score += 0.08;
	}
	return clamp01(score);
};

// ---------------------------------------------------------------- FROM GAMES

export type GameForEvents = {
	gid: number;
	day: number;
	season: number;
	overtimes: number;
	winnerTid: number;
	playoffs: boolean;
	teams: [GameTeamForEvents, GameTeamForEvents];
	spread?: { favTid: number; points: number };
	// A game that ended a playoff series.
	elimination?: boolean;
};

export type GameTeamForEvents = {
	tid: number;
	region: string;
	name: string;
	abbrev: string;
	pts: number;
	streak?: { won: boolean; count: number };
	record?: { won: number; lost: number };
	players: {
		pid: number;
		name: string;
		min: number;
		pts: number;
		reb: number;
		ast: number;
		stl: number;
		blk: number;
		tov: number;
		fga: number;
		fta: number;
	}[];
};

// True shooting, the one derived number worth precomputing here so a post can
// quote it without doing arithmetic the accuracy checker would have to redo.
const trueShooting = (p: {
	pts: number;
	fga: number;
	fta: number;
}): number | undefined => {
	const denom = 2 * (p.fga + 0.44 * p.fta);
	if (denom <= 0) {
		return undefined;
	}
	return Math.round(((100 * p.pts) / denom) * 10) / 10;
};

// How many players from one game are worth talking about. More than two and a
// single game floods the day; the trimmer downstream would drop them anyway,
// and generating them costs work for nothing.
const PERFORMANCES_PER_GAME = 2;

// Below this, a line is just a line.
const PERFORMANCE_FLOOR = 0.3;

export const eventsFromGame = (game: GameForEvents): SocialEvent[] => {
	const [home, away] = game.teams;
	const winner = home.tid === game.winnerTid ? home : away;
	const loser = home.tid === game.winnerTid ? away : home;
	const margin = winner.pts - loser.pts;
	const upset =
		game.spread !== undefined &&
		game.spread.points > 0 &&
		game.spread.favTid === loser.tid;

	const out: SocialEvent[] = [];

	out.push({
		id: `g:${game.gid}`,
		type: "gameResult",
		topic: "gameResult",
		season: game.season,
		day: game.day,
		order: game.gid,
		salience: gameSalience({
			margin,
			overtimes: game.overtimes,
			playoffs: game.playoffs,
			elimination: game.elimination,
			streak: winner.streak?.won ? winner.streak.count : undefined,
			upset,
		}),
		tids: [winner.tid, loser.tid],
		pids: [],
		facts: {
			winnerTid: winner.tid,
			loserTid: loser.tid,
			winnerName: `${winner.region} ${winner.name}`,
			loserName: `${loser.region} ${loser.name}`,
			// The nickname alone, for the voices that would never say "the
			// Boston Celtics" in full. Facts rather than string surgery in a
			// template, so a region like "Los Angeles" cannot be mangled.
			winnerNick: winner.name,
			loserNick: loser.name,
			winnerAbbrev: winner.abbrev,
			loserAbbrev: loser.abbrev,
			winnerPts: winner.pts,
			loserPts: loser.pts,
			margin,
			// The combined total is a real fact, so it is stated here rather than
			// added up inside a template - where the number checker would
			// correctly refuse it for having no source.
			combined: winner.pts + loser.pts,
			overtimes: game.overtimes,
			playoffs: game.playoffs,
			upset,
			...(winner.streak?.won ? { winnerStreak: winner.streak.count } : {}),
			...(loser.streak && !loser.streak.won
				? { loserSkid: loser.streak.count }
				: {}),
			...(winner.record
				? { winnerRecord: `${winner.record.won}-${winner.record.lost}` }
				: {}),
			...(loser.record
				? { loserRecord: `${loser.record.won}-${loser.record.lost}` }
				: {}),
		},
	});

	// Performances, ranked across BOTH teams so a losing team's 40-point night
	// beats the winner's quiet 18.
	const ranked = game.teams
		.flatMap((t) =>
			t.players
				.filter((p) => p.min > 0)
				.map((p) => ({
					p,
					tid: t.tid,
					won: t.tid === game.winnerTid,
					tsp: trueShooting(p),
				})),
		)
		.map((row) => ({
			...row,
			salience: performanceSalience({
				pts: row.p.pts,
				reb: row.p.reb,
				ast: row.p.ast,
				stl: row.p.stl,
				blk: row.p.blk,
				tsp: row.tsp,
				playoffs: game.playoffs,
			}),
		}))
		.filter((row) => row.salience >= PERFORMANCE_FLOOR)
		// Deterministic: pid breaks a salience tie, never array order.
		.sort((a, b) => b.salience - a.salience || a.p.pid - b.p.pid)
		.slice(0, PERFORMANCES_PER_GAME);

	for (const [i, row] of ranked.entries()) {
		const doubles = [
			row.p.pts,
			row.p.reb,
			row.p.ast,
			row.p.stl,
			row.p.blk,
		].filter((v) => v >= 10).length;
		out.push({
			id: `perf:${game.gid}:${row.p.pid}`,
			type: "performance",
			topic: "playerPerformance",
			season: game.season,
			day: game.day,
			order: game.gid * 100 + i + 1,
			salience: row.salience,
			tids: [row.tid],
			pids: [row.p.pid],
			facts: {
				name: row.p.name,
				tid: row.tid,
				won: row.won,
				min: row.p.min,
				pts: row.p.pts,
				reb: row.p.reb,
				ast: row.p.ast,
				stl: row.p.stl,
				blk: row.p.blk,
				tov: row.p.tov,
				// Shot volume. Carried as real facts rather than left out,
				// because "31 on 27 shots" is the whole story of some nights
				// and the number checker refuses any numeral it cannot source.
				fga: row.p.fga,
				fta: row.p.fta,
				doubles,
				tripleDouble: doubles >= 3,
				...(row.tsp !== undefined ? { tsp: row.tsp } : {}),
				opponentAbbrev:
					row.tid === game.teams[0].tid
						? game.teams[1].abbrev
						: game.teams[0].abbrev,
			},
		});
	}

	return out;
};

// ------------------------------------------------------- FROM LEAGUE EVENTS
//
// The league already keeps a news log with a type, the players and teams
// involved, and a rough importance score. That is most of a social event
// already, so transactions, awards and milestones are read from it rather than
// re-derived - which also means anything a future version of the game logs
// shows up in the feed for free.

export type LeagueEventForEvents = {
	eid: number;
	type: string;
	season: number;
	text?: string;
	pids?: number[];
	tids?: number[];
	// The league's own importance: under 10 is minor, 20 and up is major.
	score?: number;
};

// The league's log types this feed knows how to talk about, and what each one
// counts as. Anything not listed is skipped rather than posted about
// generically - a feed that says "something happened" about a database upgrade
// notice is exactly the cheapness this is trying to avoid.
const LEAGUE_EVENT_MAP: Record<
	string,
	{ type: SocialEventType; topic: keyof SocialTopicWeights }
> = {
	trade: { type: "trade", topic: "trade" },
	freeAgent: { type: "signing", topic: "freeAgency" },
	reSigned: { type: "signing", topic: "freeAgency" },
	release: { type: "release", topic: "trade" },
	refuseToSign: { type: "signing", topic: "freeAgency" },
	draft: { type: "draft", topic: "draft" },
	draftLottery: { type: "draft", topic: "draft" },
	award: { type: "award", topic: "awards" },
	hallOfFame: { type: "milestone", topic: "milestone" },
	retired: { type: "retirement", topic: "milestone" },
	retiredJersey: { type: "milestone", topic: "milestone" },
	playerFeat: { type: "milestone", topic: "milestone" },
	injured: { type: "injury", topic: "injury" },
	madePlayoffs: { type: "playoffs", topic: "standings" },
	playoffs: { type: "playoffs", topic: "standings" },
	tragedy: { type: "milestone", topic: "milestone" },
	ageFraud: { type: "milestone", topic: "rumor" },
	teamExpansion: { type: "milestone", topic: "milestone" },
	teamRelocation: { type: "milestone", topic: "milestone" },
	teamRename: { type: "milestone", topic: "milestone" },
	luxuryTax: { type: "milestone", topic: "money" },
	minPayroll: { type: "milestone", topic: "money" },
};

export const isFeedableLeagueEvent = (type: string): boolean =>
	Object.hasOwn(LEAGUE_EVENT_MAP, type);

// The league scores importance on an open-ended scale where 20 is "very
// important". Map it onto the same 0-1 the game events use, so the day can be
// trimmed by comparing them directly.
export const leagueEventSalience = (score: number | undefined): number => {
	if (score === undefined) {
		return 0.3;
	}
	return clamp01(0.2 + score / 30);
};

// Strip the league's HTML links so a post can quote the text as prose. The log
// is written for a web page; the feed is not.
export const plainEventText = (text: string | undefined): string =>
	text === undefined
		? ""
		: text
				.replaceAll(/<[^>]*>/g, "")
				.replaceAll(/\s+/g, " ")
				.trim()
				// The log shouts its own news ("Player was injured!") because it
				// is a notification. Posts quote it as prose, and the templates
				// add their own reaction after it, so the excitement belongs to
				// the account rather than to the sentence it is passing along.
				// Mid-string too: the injury line puts the mark before its own
				// parenthetical, which is where it was first spotted.
				.replaceAll("!", ".")
				.replace(/\.\s*\(/, " (");

export const eventFromLeagueEvent = (
	event: LeagueEventForEvents,
	day: number,
): SocialEvent | undefined => {
	const mapped = LEAGUE_EVENT_MAP[event.type];
	if (!mapped) {
		return undefined;
	}
	return {
		id: `e:${event.eid}`,
		type: mapped.type,
		topic: mapped.topic,
		season: event.season,
		day,
		// Offset well past any gid-derived order so league events sort after the
		// day's games rather than interleaving unpredictably with them.
		order: 1_000_000 + event.eid,
		salience: leagueEventSalience(event.score),
		tids: event.tids ?? [],
		pids: event.pids ?? [],
		facts: {
			summary: plainEventText(event.text),
			leagueType: event.type,
		},
	};
};

// WHICH DAY A TRANSACTION BELONGS TO.
//
// The league's news log records a season but never a schedule day, and the
// per-game events that would have anchored one are logged with saveToDb false,
// so they are not there to read. There is therefore NO GROUND TRUTH to recover
// here - only a placement to choose.
//
// So the requirement is not accuracy, which is unavailable, but two things
// that are: every device must choose the same day, and events must keep their
// relative order. Spreading a season's events evenly across the days that
// actually have games does both, and reads plausibly because transactions
// really are spread through a season.
//
// Events are handed over already sorted by eid; the result preserves that.
export const assignEventDays = (
	eids: readonly number[],
	days: readonly number[],
): Map<number, number> => {
	const out = new Map<number, number>();
	if (eids.length === 0) {
		return out;
	}
	if (days.length === 0) {
		// No games this season yet (a pure offseason view). Everything lands on
		// day 0, which the caller renders as a single undated stretch.
		for (const eid of eids) {
			out.set(eid, 0);
		}
		return out;
	}
	const sorted = [...days].sort((a, b) => a - b);
	for (const [i, eid] of eids.entries()) {
		const slot = Math.floor((i * sorted.length) / eids.length);
		out.set(eid, sorted[Math.min(slot, sorted.length - 1)]!);
	}
	return out;
};

// ---------------------------------------------------------------- PLACEMENT, STABLY
//
// assignEventDays above spreads a season's news evenly across its game days.
// That is deterministic, but it is not STABLE: both denominators grow as the
// season goes on, so an event placed on day 40 today sits on day 43 next week.
// A timeline whose past keeps moving is not a timeline.
//
// There is ground truth for the news that matters most, and it lives in the
// box scores. A box score records, per player, the team he suited up for and
// whether he was hurt in that game. So:
//
//   - an injury is anchored to the game where his row says newThisGame;
//   - a trade is anchored to the first game a traded player appears for his
//     new team;
//   - a playoff series result is anchored to the last playoff game between
//     the two teams, and a playoff moment ("in game 3 of the ...") to that
//     game;
//   - the champion's coronation is anchored to the last playoff game, and is
//     also the boundary: everything logged after it is the offseason.
//
// Everything else is interpolated by eid between the nearest anchors, and an
// anchor never moves once its game is in the books. The only news that can
// still shift is what comes after the season's LAST anchor - the trailing
// few days - and it settles as soon as the next injury or trade lands.

export type PlacementGame = {
	gid: number;
	day: number;
	playoffs: boolean;
	teams: {
		tid: number;
		players: { pid: number; injuryNew?: boolean }[];
	}[];
};

export type PlacementEvent = {
	eid: number;
	type: string;
	text?: string;
	pids?: number[];
	tids?: number[];
};

// Day 0 is the undated stretch: the offseason, and anything that predates the
// first game. The schedule starts at day 1, so nothing else can claim it.
export const OFFSEASON_DAY = 0;

export const placeLeagueEvents = ({
	events,
	games,
	offseason,
}: {
	// Sorted by eid ascending.
	events: readonly PlacementEvent[];
	games: readonly PlacementGame[];
	// True once the league has moved past the playoffs: news after the title
	// then belongs to the offseason rather than to the last day of the finals.
	offseason: boolean;
}): Map<number, number> => {
	const out = new Map<number, number>();
	if (events.length === 0) {
		return out;
	}

	const sortedGames = [...games].sort((a, b) => a.day - b.day || a.gid - b.gid);
	const days = [...new Set(sortedGames.map((game) => game.day))].sort(
		(a, b) => a - b,
	);
	if (days.length === 0) {
		for (const event of events) {
			out.set(event.eid, OFFSEASON_DAY);
		}
		return out;
	}
	const firstDay = days[0]!;
	const latestDay = days.at(-1)!;
	const lastRegularDay = Math.max(
		firstDay,
		...sortedGames.filter((game) => !game.playoffs).map((game) => game.day),
	);
	const lastPlayoffDay = Math.max(
		lastRegularDay,
		...sortedGames.filter((game) => game.playoffs).map((game) => game.day),
	);

	// ---- Indexes over the box scores, built once.
	const injuryDaysByPid = new Map<number, number[]>();
	const appearancesByPid = new Map<number, { day: number; tid: number }[]>();
	const playoffGamesByPair = new Map<string, number[]>();
	const pairKey = (a: number, b: number) => (a < b ? `${a}|${b}` : `${b}|${a}`);
	for (const game of sortedGames) {
		for (const team of game.teams) {
			for (const player of team.players) {
				if (player.injuryNew) {
					const arr = injuryDaysByPid.get(player.pid) ?? [];
					arr.push(game.day);
					injuryDaysByPid.set(player.pid, arr);
				}
				const seen = appearancesByPid.get(player.pid) ?? [];
				seen.push({ day: game.day, tid: team.tid });
				appearancesByPid.set(player.pid, seen);
			}
		}
		if (game.playoffs && game.teams.length === 2) {
			const key = pairKey(game.teams[0]!.tid, game.teams[1]!.tid);
			const arr = playoffGamesByPair.get(key) ?? [];
			arr.push(game.day);
			playoffGamesByPair.set(key, arr);
		}
	}

	// The days a player changed teams, in order: the first game he played for
	// each new team after having played for a different one.
	const changeDaysByPid = new Map<number, { day: number; tid: number }[]>();
	for (const [pid, seen] of appearancesByPid) {
		const changes: { day: number; tid: number }[] = [];
		for (let i = 1; i < seen.length; i++) {
			if (seen[i]!.tid !== seen[i - 1]!.tid) {
				changes.push(seen[i]!);
			}
		}
		changeDaysByPid.set(pid, changes);
	}

	// ---- Anchors. Each type keeps a per-key counter so the k-th event of a
	// kind pairs with the k-th thing in the box scores.
	const injuryCount = new Map<number, number>();
	const tradeCount = new Map<number, number>();
	const anchors: { eid: number; day: number }[] = [];
	let championEid: number | undefined;

	for (const event of events) {
		let day: number | undefined;
		const text = event.text ?? "";

		if (event.type === "injured" && event.pids?.length) {
			const pid = event.pids[0]!;
			const k = injuryCount.get(pid) ?? 0;
			injuryCount.set(pid, k + 1);
			day = injuryDaysByPid.get(pid)?.[k];
		} else if (event.type === "trade" && event.pids?.length) {
			let best: number | undefined;
			for (const pid of event.pids) {
				const k = tradeCount.get(pid) ?? 0;
				tradeCount.set(pid, k + 1);
				const change = changeDaysByPid.get(pid)?.[k];
				if (
					change &&
					(event.tids === undefined || event.tids.includes(change.tid))
				) {
					best = best === undefined ? change.day : Math.min(best, change.day);
				}
			}
			day = best;
		} else if (event.type === "playoffs") {
			if (/league champions/.test(text)) {
				day = lastPlayoffDay;
				championEid = event.eid;
			} else if (event.tids?.length === 2) {
				const series = playoffGamesByPair.get(
					pairKey(event.tids[0]!, event.tids[1]!),
				);
				if (series?.length) {
					const gameNumber = /game (\d+)/i.exec(text);
					if (gameNumber) {
						day = series[Number(gameNumber[1]) - 1];
					} else if (/defeated the/.test(text)) {
						day = series.at(-1);
					}
				}
			}
		} else if (event.type === "madePlayoffs") {
			day = lastRegularDay;
		}

		if (day !== undefined) {
			// Anchors must not run backwards in time. A mismatched pairing
			// (a player hurt twice, logged once) would otherwise drag every
			// interpolated event around it the wrong way.
			const previous = anchors.at(-1);
			if (previous === undefined || day >= previous.day) {
				anchors.push({ eid: event.eid, day });
			}
		}
	}

	// ---- Interpolation. Snap to a real game day so nothing lands on a day
	// with no games, which the feed would not show.
	const snap = (target: number, lo: number, hi: number) => {
		let best = lo;
		for (const day of days) {
			if (day < lo) {
				continue;
			}
			if (day > hi) {
				break;
			}
			if (Math.abs(day - target) < Math.abs(best - target)) {
				best = day;
			}
		}
		return best;
	};

	let a = 0;
	for (const event of events) {
		if (championEid !== undefined && event.eid > championEid) {
			out.set(event.eid, offseason ? OFFSEASON_DAY : latestDay);
			continue;
		}
		while (a < anchors.length && anchors[a]!.eid < event.eid) {
			a += 1;
		}
		const next = anchors[a];
		const prev = a > 0 ? anchors[a - 1] : undefined;
		if (next && next.eid === event.eid) {
			out.set(event.eid, next.day);
			continue;
		}
		const loEid = prev?.eid ?? Math.min(event.eid, events[0]!.eid) - 1;
		const loDay = prev?.day ?? firstDay;
		const hiEid = next?.eid ?? events.at(-1)!.eid + 1;
		const hiDay = next?.day ?? latestDay;
		const t = hiEid === loEid ? 0 : (event.eid - loEid) / (hiEid - loEid);
		out.set(event.eid, snap(loDay + t * (hiDay - loDay), loDay, hiDay));
	}
	return out;
};

// ---------------------------------------------------------------- TRIMMING
//
// A full day of a 30-team league produces far more events than anyone will
// read. Trim to the ones worth reacting to, but never let one game's blowout
// crowd out every other game: a feed that is fifteen posts about one result is
// the redundancy this whole design is trying to avoid.
export const trimDayEvents = (
	events: readonly SocialEvent[],
	{ limit, maxPerGame = 3 }: { limit: number; maxPerGame?: number },
): SocialEvent[] => {
	const perGame = new Map<string, number>();
	const gameKey = (event: SocialEvent) => {
		const match = /^(?:g|perf):(\d+)/.exec(event.id);
		return match ? match[1]! : undefined;
	};

	return (
		[...events]
			.sort((a, b) => b.salience - a.salience || a.order - b.order)
			.filter((event) => {
				const key = gameKey(event);
				if (key === undefined) {
					return true;
				}
				const used = perGame.get(key) ?? 0;
				if (used >= maxPerGame) {
					return false;
				}
				perGame.set(key, used + 1);
				return true;
			})
			.slice(0, limit)
			// Back into stream order once the cut is made, so the day reads
			// chronologically rather than by importance.
			.sort((a, b) => a.order - b.order)
	);
};
