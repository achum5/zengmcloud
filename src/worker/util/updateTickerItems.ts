import { idb } from "../db/index.ts";
import g from "./g.ts";
import toUI from "./toUI.ts";
import { formatEventText } from "./formatEventText.ts";
import { types } from "../../common/transactionInfo.ts";
import { formatAmerican } from "../../common/sportsbook.ts";
import { PHASE } from "../../common/constants.ts";
import { bySport, isSport } from "../../common/sportFunctions.ts";
import {
	TICKER_LIMITS,
	type TickerItem,
	type TickerRaceEntry,
} from "../../common/ticker.ts";
import { getUpcoming } from "../views/schedule.ts";
import getAwardRaceOdds from "../core/season/getAwardRaceOdds.ts";
import type { EventBBGM, Game } from "../../common/types.ts";

// Everything the bottom ticker shows, built league-wide.
//
// Five feeds, and they are not equally cheap. Scores, performances and news
// come from bounded reads that can run after every simulated day. The award
// races and the point spreads load and score every player in the league, so
// they are memoized on a short timer - without that, simming to the playoffs
// would recompute the entire award model eighty times over.

const abbrevOf = (tid: number): string =>
	g.get("teamInfoCache")[tid]?.abbrev ?? "???";

// ---------------------------------------------------------------- most recent
// The last day that actually has games, league-wide, without reading a season.
//
// `gid` is an autoincrement, so walking it backwards is newest-first: take the
// day off the newest completed game and then keep collecting while the day
// matches. Games simmed this session are still in the cache, so they come off
// the top first.
const recentGames = async (): Promise<Game[]> => {
	const collected: Game[] = [];
	const seen = new Set<number>();
	let day: number | undefined;
	let scanned = 0;

	const consider = (game: Game): boolean => {
		if (game.season !== g.get("season") || seen.has(game.gid)) {
			return true;
		}
		if (day === undefined) {
			day = game.day;
		} else if (game.day !== day) {
			// Walked off the end of the day.
			return false;
		}
		seen.add(game.gid);
		collected.push(game);
		return collected.length < 40;
	};

	const cached = (await idb.cache.games.getAll()) as Game[];
	for (const game of [...cached].sort((a, b) => b.gid - a.gid)) {
		if (!consider(game)) {
			return collected;
		}
	}

	let cursor = await idb.league
		.transaction("games")
		.store.openCursor(null, "prev");
	while (cursor && scanned < 120) {
		scanned += 1;
		if (!consider(cursor.value as Game)) {
			break;
		}
		cursor = await cursor.continue();
	}

	return collected;
};

const scoreItems = (games: Game[]): TickerItem[] =>
	games.map((game) => {
		// A game record's teams are [home, away].
		const [home, away] = game.teams;
		const allStar = home.tid === -1 && away.tid === -2;
		return {
			type: "score" as const,
			key: `score-${game.gid}`,
			gid: game.gid,
			season: game.season,
			boxScoreTeam: allStar ? "special" : `${abbrevOf(home.tid)}_${home.tid}`,
			away: { tid: away.tid, abbrev: abbrevOf(away.tid), pts: away.pts },
			home: { tid: home.tid, abbrev: abbrevOf(home.tid), pts: home.pts },
			overtimes: game.overtimes > 0 ? game.overtimes : undefined,
		};
	});

// ---------------------------------------------------------- day's performances
// The stat lines worth stopping the scroll for, ranked by a rough game score so
// a 40-point night and a triple-double both qualify.
//
// GROUPED BY GAME, and returned that way. The ticker turns each game's lines
// into a block with the score held in the left pane beside them, so what is
// wanted is the best few lines from each of the best few games - not a flat top
// six, which in practice came from six different games and left every one of
// them without its context.
const LINES_PER_GAME = 3;
const GAMES_WITH_LINES = 6;

const performanceItems = (games: Game[]): TickerItem[] => {
	type Row = { score: number; gid: number; item: TickerItem };
	const rows: Row[] = [];

	for (const game of games) {
		const [home, away] = game.teams;
		const allStar = home.tid === -1 && away.tid === -2;
		for (const team of game.teams) {
			for (const p of team.players ?? []) {
				const line = bySport({
					basketball: () => {
						const pts = p.pts ?? 0;
						const trb = (p.orb ?? 0) + (p.drb ?? 0);
						const ast = p.ast ?? 0;
						const parts = [`${pts} PTS`];
						if (trb >= 8) {
							parts.push(`${trb} REB`);
						}
						if (ast >= 8) {
							parts.push(`${ast} AST`);
						}
						if ((p.blk ?? 0) >= 4) {
							parts.push(`${p.blk} BLK`);
						}
						if ((p.stl ?? 0) >= 4) {
							parts.push(`${p.stl} STL`);
						}
						return {
							score:
								pts + 1.4 * trb + 1.6 * ast + 2 * ((p.blk ?? 0) + (p.stl ?? 0)),
							text: parts.join(", "),
						};
					},
					default: () => {
						const keyStats = p.keyStats ?? p.keyStatsShort;
						return keyStats
							? { score: p.pts ?? p.gp ?? 0, text: String(keyStats) }
							: undefined;
					},
				})();

				// A quiet night is not news. In basketball the bar is a real one; in
				// the other sports there is no comparable single number, so anyone
				// with a key-stats line is a candidate and the cap does the filtering.
				if (!line || (isSport("basketball") && line.score < 34)) {
					continue;
				}

				rows.push({
					score: line.score,
					gid: game.gid,
					item: {
						type: "performance",
						key: `perf-${game.gid}-${p.pid}`,
						gid: game.gid,
						season: game.season,
						boxScoreTeam: allStar
							? "special"
							: `${abbrevOf(home.tid)}_${home.tid}`,
						pid: p.pid,
						name: p.name,
						stat: line.text,
						game: `${abbrevOf(away.tid)} ${away.pts}-${home.pts} ${abbrevOf(home.tid)}`,
					},
				});
			}
		}
	}

	rows.sort((a, b) => b.score - a.score);

	// Best lines first, so each game keeps its best and the games rank by their
	// single best line. Emitted game by game, contiguously, because that grouping
	// is what the segment builder reads.
	const byGame = new Map<number, TickerItem[]>();
	for (const row of rows) {
		const lines = byGame.get(row.gid);
		if (lines) {
			if (lines.length < LINES_PER_GAME) {
				lines.push(row.item);
			}
		} else if (byGame.size < GAMES_WITH_LINES) {
			byGame.set(row.gid, [row.item]);
		}
	}

	return [...byGame.values()].flat().slice(0, TICKER_LIMITS.performance);
};

// A ticker says MVP, not Most Valuable Player - the full names are longer than
// the entry they label. Anything not listed keeps its name.
const SHORT_AWARD: Record<string, string> = {
	"Most Valuable Player": "MVP",
	"Defensive Player of the Year": "DPOY",
	"Offensive Player of the Year": "OPOY",
	"Defensive Forward of the Year": "DFOY",
	"Rookie of the Year": "ROY",
	"Defensive Rookie of the Year": "DROY",
	"Offensive Rookie of the Year": "OROY",
	"Sixth Man of the Year": "6MOY",
	"Most Improved Player": "MIP",
	"Goalie of the Year": "GOTY",
	"Protector of the Year": "POTY",
};

// ------------------------------------------------------------------ the news
// tradingCard is your own card generator reporting back, not league news, and
// newLeague is a one-line greeting that would otherwise get a whole block of the
// ticker to itself for as long as the league is young.
const IGNORE_TYPES = new Set([
	"retiredList",
	"newTeam",
	"tradingCard",
	"newLeague",
]);
const MIN_NEWS_SCORE = 10;
const MAX_NEWS_SCANNED = 400;

const newsItems = async (): Promise<TickerItem[]> => {
	const qualifies = (event: EventBBGM) =>
		!IGNORE_TYPES.has(event.type) &&
		event.score !== undefined &&
		event.score >= MIN_NEWS_SCORE;

	const picked: EventBBGM[] = [];
	const seen = new Set<number>();

	const cached = await idb.cache.events.getAll();
	for (const event of [...cached].reverse()) {
		if (qualifies(event) && !seen.has(event.eid)) {
			seen.add(event.eid);
			picked.push(event);
		}
		if (picked.length >= TICKER_LIMITS.news) {
			break;
		}
	}

	if (picked.length < TICKER_LIMITS.news) {
		let scanned = 0;
		let cursor = await idb.league
			.transaction("events")
			.store.openCursor(null, "prev");
		while (
			cursor &&
			picked.length < TICKER_LIMITS.news &&
			scanned < MAX_NEWS_SCANNED
		) {
			scanned += 1;
			const event = cursor.value as EventBBGM;
			if (qualifies(event) && !seen.has(event.eid)) {
				seen.add(event.eid);
				picked.push(event);
			}
			cursor = await cursor.continue();
		}
	}

	return Promise.all(
		picked.map(async (event) => ({
			type: "news" as const,
			key: `news-${event.eid}`,
			eid: event.eid,
			text: await formatEventText(event),
			category: types[event.type]?.category,
		})),
	);
};

// ------------------------------------------------- the expensive half, memoized
//
// Both of these walk every player in the league. Recomputing them per simulated
// day would dominate a long sim, and neither moves fast enough to need it.
//
// The key matters as much as the timer. Both sections are gated on the phase, so
// a result computed in the preseason is an empty list - and if that empty list
// were allowed to answer for the league that just tipped off, the ticker would
// carry no slate and no award race for the first minute of every season. Phase
// and season are therefore part of the identity, not just the age.
const MEMO_MS = 60_000;
let memo: { at: number; key: string; items: TickerItem[] } | undefined;

const upcomingAndRaces = async (fresh: boolean): Promise<TickerItem[]> => {
	const now = Date.now();
	const phase = g.get("phase");
	const key = `${g.get("lid")}|${g.get("season")}|${phase}`;
	if (!fresh && memo && memo.key === key && now - memo.at < MEMO_MS) {
		return memo.items;
	}

	const items: TickerItem[] = [];

	// The next games up, with the point spread every other page shows for them -
	// same helper, so the ticker cannot quote a different number. The playoffs
	// count: that is when the slate matters most.
	if (phase >= PHASE.REGULAR_SEASON && phase <= PHASE.PLAYOFFS) {
		try {
			const upcoming = await getUpcoming({});
			for (const game of upcoming.slice(0, TICKER_LIMITS.upcoming)) {
				const [home, away] = game.teams;
				if (home.tid < 0 || away.tid < 0) {
					continue;
				}
				// A spread is quoted on the favourite, the way a book quotes it.
				let line: string | undefined;
				if (game.spread !== undefined && game.spread !== 0) {
					const favourite = game.spread > 0 ? home : away;
					line = `${abbrevOf(favourite.tid)} ${-Math.abs(game.spread)}`;
				} else if (game.spread === 0) {
					line = "PK";
				}
				items.push({
					type: "upcoming",
					key: `up-${game.gid}`,
					away: { tid: away.tid, abbrev: abbrevOf(away.tid) },
					home: { tid: home.tid, abbrev: abbrevOf(home.tid) },
					line,
				});
			}
		} catch (error) {
			console.error("Ticker: upcoming games failed", error);
		}
	}

	// Where the award races stand, from the same model the Award Races page and
	// the sportsbook read.
	if (phase >= PHASE.REGULAR_SEASON && phase <= PHASE.PLAYOFFS) {
		try {
			const races = await getAwardRaceOdds(g.get("season"));
			for (const race of races.slice(0, TICKER_LIMITS.race)) {
				const top = (race.players ?? []).slice(0, 3);
				if (top.length === 0) {
					continue;
				}
				items.push({
					type: "race",
					key: `race-${race.name}`,
					// "Most Valuable Player" is too long to scroll past; the short form
					// is what a ticker would say.
					label: SHORT_AWARD[race.name] ?? race.name,
					// Annotated on the way out. getAwardRaceOdds hands back `any`, so a
					// missing field here is not a compile error - it is a name in the
					// ticker that quietly stops being a link.
					entries: top.map(
						(p: any): TickerRaceEntry => ({
							pid: p.pid,
							name: p.name,
							odds:
								typeof p.odds === "number" ? formatAmerican(p.odds) : undefined,
						}),
					),
				});
			}
		} catch (error) {
			console.error("Ticker: award races failed", error);
		}
	}

	memo = { at: now, key, items };
	return items;
};

// `fresh` skips the memo. The sim passes it once, at the end of a run: during
// the run the memo is doing its job, but the moment the user is looking at the
// result they should see the slate and the odds as they now stand, not as they
// stood when the sim started.
export const updateTickerItems = async ({ fresh = false } = {}) => {
	let items: TickerItem[] = [];

	try {
		const games = await recentGames();
		items = [
			...scoreItems(games),
			...performanceItems(games),
			...(await newsItems()),
			...(await upcomingAndRaces(fresh)),
		];
	} catch (error) {
		// A ticker is decoration. It must never be the reason a sim or a league
		// load fails, so a bad read leaves it as it was.
		console.error("Failed to build ticker items", error);
		return;
	}

	await toUI("updateLocal", [{ tickerItems: items }]);
};

// A new league, or one being closed, must not inherit the last one's ticker.
export const clearTickerMemo = () => {
	memo = undefined;
};
