import { idb } from "../db/index.ts";
import g from "./g.ts";
import toUI from "./toUI.ts";
import { formatEventText } from "./formatEventText.ts";
import { types } from "../../common/transactionInfo.ts";
import { formatAmerican } from "../../common/sportsbook.ts";
import { PHASE } from "../../common/constants.ts";
import { bySport, isSport } from "../../common/sportFunctions.ts";
import { TICKER_LIMITS, type TickerItem } from "../../common/ticker.ts";
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
const performanceItems = (games: Game[]): TickerItem[] => {
	type Row = { score: number; item: TickerItem };
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
					item: {
						type: "performance",
						key: `perf-${game.gid}-${p.pid}`,
						gid: game.gid,
						season: game.season,
						boxScoreTeam: allStar
							? "special"
							: `${abbrevOf(home.tid)}_${home.tid}`,
						text: `${p.name} ${line.text} — ${abbrevOf(away.tid)} ${away.pts}-${home.pts} ${abbrevOf(home.tid)}`,
					},
				});
			}
		}
	}

	rows.sort((a, b) => b.score - a.score);
	return rows.slice(0, TICKER_LIMITS.performance).map((row) => row.item);
};

// ------------------------------------------------------------------ the news
// tradingCard is your own card generator reporting back, not league news.
const IGNORE_TYPES = new Set(["retiredList", "newTeam", "tradingCard"]);
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
					label: race.name,
					text: top
						.map(
							(p: any) =>
								`${p.name}${
									typeof p.odds === "number" ? ` ${formatAmerican(p.odds)}` : ""
								}`,
						)
						.join(" · "),
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
