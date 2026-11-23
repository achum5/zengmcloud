// WHAT A LIVE GAME HIDES, ON SCREEN ONLY.
//
// In a shared league a game being live simmed is played in full the moment it
// tips off: the result is saved and goes up to the cloud like any other, and
// every device has it within seconds. Nothing here changes that. What this
// decides is what the pages SHOW while the game is still being played out on
// somebody's screen, so it looks like a game in progress everywhere: LIVE on
// the schedules, no final in the box score, standings and bracket as they were
// going in, and nothing the game gave rise to (the next playoff day, a series
// ending) until the final buzzer on the screen playing it.
//
// Which games, from liveSpoilers.ts:
// - `watching`: this device's own live game, or the one it is watching. Hidden
//   till it has played out on this screen, whatever else happens.
// - `room`: every other game a league mate is playing live. Hidden till it is
//   over on theirs - except in the regular season once the league has played
//   past its day (a day sim doesn't wait for a live game, and the day after's
//   results would give it away anyway). In the playoffs nothing is simmed past
//   a live game (see liveGameHoldingDay in sync/connect.ts).

import { idb } from "../db/index.ts";
import g from "./g.ts";
import { liveSpoilerSets, mentionsHiddenGame } from "./liveSpoilers.ts";
import getWinner from "../../common/getWinner.ts";
import type { Game } from "../../common/types.ts";

type GameTeam = Game["teams"][number];

export type LiveGate = {
	hidden: ReadonlySet<number>;
	// The playoff day a hidden game is on. What the app works out at tip-off of
	// a day's last game - the next day's games - is on a later day, so nothing
	// past this one is shown yet.
	dayCutoff: number | undefined;
	// This season's rows of the hidden games played here.
	games: ReadonlyMap<number, Game>;
};

const NO_GATE: LiveGate = {
	hidden: new Set(),
	dayCutoff: undefined,
	games: new Map(),
};

// The rule above, given the games' rows (where this device has them) and the
// latest day of the season played. Pure, for the tests.
export const decideLiveGate = ({
	watching,
	room,
	rows,
	maxPlayedDay,
}: {
	watching: ReadonlySet<number>;
	room: ReadonlySet<number>;
	rows: ReadonlyMap<number, Pick<Game, "day" | "playoffs">>;
	maxPlayedDay: () => number | undefined;
}): { hidden: Set<number>; dayCutoff: number | undefined } => {
	const hidden = new Set(watching);
	for (const gid of room) {
		if (hidden.has(gid)) {
			continue;
		}
		const row = rows.get(gid);
		if (row && !row.playoffs && row.day !== undefined) {
			const maxDay = maxPlayedDay();
			if (maxDay !== undefined && maxDay > row.day) {
				continue;
			}
		}
		hidden.add(gid);
	}
	let dayCutoff: number | undefined;
	for (const gid of hidden) {
		const row = rows.get(gid);
		if (row?.playoffs && row.day !== undefined) {
			dayCutoff =
				dayCutoff === undefined ? row.day : Math.min(dayCutoff, row.day);
		}
	}
	return { hidden, dayCutoff };
};

// Never throws: it is asked in the middle of a sim (game/play.ts) and by
// every page. On any trouble every live game is simply hidden, rows unknown.
export const getLiveGate = async (): Promise<LiveGate> => {
	const { watching, room } = liveSpoilerSets();
	if (watching.size === 0 && room.size === 0) {
		return NO_GATE;
	}
	try {
		return await readLiveGate(watching, room);
	} catch (error) {
		console.error("Live game gate failed", error);
		return {
			hidden: new Set([...watching, ...room]),
			dayCutoff: undefined,
			games: new Map(),
		};
	}
};

const readLiveGate = async (
	watching: ReadonlySet<number>,
	room: ReadonlySet<number>,
): Promise<LiveGate> => {
	const season = g.get("season");
	const rows = new Map<number, Game>();
	for (const gid of new Set([...watching, ...room])) {
		const game = await idb.cache.games.get(gid);
		if (game && game.season === season) {
			// (A copy: the pages working from these can never touch the
			// cache's own row, which is what gets saved and synced.)
			rows.set(gid, structuredClone(game));
		}
	}

	// (The whole season's games only when a regular season game could be
	// played past.)
	let maxDay: number | undefined;
	let maxDayLoaded = false;
	const needsMaxDay = [...room].some((gid) => {
		const row = rows.get(gid);
		return !watching.has(gid) && row && !row.playoffs && row.day !== undefined;
	});
	const seasonGames = needsMaxDay ? await idb.cache.games.getAll() : [];
	const maxPlayedDay = () => {
		if (!maxDayLoaded) {
			maxDayLoaded = true;
			for (const game of seasonGames) {
				if (
					game.season === season &&
					game.day !== undefined &&
					(maxDay === undefined || game.day > maxDay)
				) {
					maxDay = game.day;
				}
			}
		}
		return maxDay;
	};

	const { hidden, dayCutoff } = decideLiveGate({
		watching,
		room,
		rows,
		maxPlayedDay,
	});
	for (const gid of rows.keys()) {
		if (!hidden.has(gid)) {
			rows.delete(gid);
		}
	}
	return { hidden, dayCutoff, games: rows };
};

// A team in a game row as it stood going in: the row stores records and the
// series standing as they were AFTER the game, its result already counted.
export const teamGoingIn = <
	T extends Pick<GameTeam, "won" | "lost" | "tied" | "otl" | "playoffs">,
>(
	game: Pick<Game, "teams" | "overtimes">,
	i: 0 | 1,
	t: T,
	otlSetting: boolean,
): T => {
	const winner = getWinner(game.teams);
	const out = { ...t };
	if (winner === -1) {
		if (out.tied !== undefined) {
			out.tied = Math.max(0, out.tied - 1);
		}
	} else if (winner === i) {
		if (out.won !== undefined) {
			out.won = Math.max(0, out.won - 1);
		}
	} else if (otlSetting && game.overtimes > 0 && out.otl !== undefined) {
		out.otl = Math.max(0, out.otl - 1);
	} else if (out.lost !== undefined) {
		out.lost = Math.max(0, out.lost - 1);
	}
	if (out.playoffs) {
		out.playoffs = {
			...out.playoffs,
			won: Math.max(0, out.playoffs.won - (winner === i ? 1 : 0)),
			lost: Math.max(
				0,
				out.playoffs.lost - (winner !== -1 && winner !== i ? 1 : 0),
			),
		};
	}
	return out;
};

// A team's regular season record with the hidden games taken back out, for
// pages that show standings. The per-team deltas only: the season row itself
// is never touched.
export type RecordDelta = {
	won: number;
	lost: number;
	tied: number;
	otl: number;
	wonHome: number;
	lostHome: number;
	tiedHome: number;
	otlHome: number;
	wonAway: number;
	lostAway: number;
	tiedAway: number;
	otlAway: number;
	wonDiv: number;
	lostDiv: number;
	tiedDiv: number;
	otlDiv: number;
	wonConf: number;
	lostConf: number;
	tiedConf: number;
	otlConf: number;
	gp: number;
	pts: number;
	oppPts: number;
};

const emptyDelta = (): RecordDelta => ({
	won: 0,
	lost: 0,
	tied: 0,
	otl: 0,
	wonHome: 0,
	lostHome: 0,
	tiedHome: 0,
	otlHome: 0,
	wonAway: 0,
	lostAway: 0,
	tiedAway: 0,
	otlAway: 0,
	wonDiv: 0,
	lostDiv: 0,
	tiedDiv: 0,
	otlDiv: 0,
	wonConf: 0,
	lostConf: 0,
	tiedConf: 0,
	otlConf: 0,
	gp: 0,
	pts: 0,
	oppPts: 0,
});

export const recordDeltas = (
	games: Iterable<Game>,
	{
		otlSetting,
		divByTid,
		confByTid,
	}: {
		otlSetting: boolean;
		divByTid: ReadonlyMap<number, number>;
		confByTid: ReadonlyMap<number, number>;
	},
): Map<number, RecordDelta> => {
	const deltas = new Map<number, RecordDelta>();
	for (const game of games) {
		if (game.playoffs) {
			continue;
		}
		const winner = getWinner(game.teams);
		for (const i of [0, 1] as const) {
			const t = game.teams[i];
			const other = game.teams[i === 0 ? 1 : 0];
			if (t.tid < 0) {
				continue;
			}
			const d = deltas.get(t.tid) ?? emptyDelta();
			deltas.set(t.tid, d);
			const result =
				winner === -1
					? "tied"
					: winner === i
						? "won"
						: otlSetting && game.overtimes > 0
							? "otl"
							: "lost";
			const where = i === 0 && !game.neutralSite ? "Home" : "Away";
			d[result] += 1;
			d[`${result}${where}`] += 1;
			if (divByTid.get(t.tid) === divByTid.get(other.tid)) {
				d[`${result}Div`] += 1;
			}
			if (confByTid.get(t.tid) === confByTid.get(other.tid)) {
				d[`${result}Conf`] += 1;
			}
			d.gp += 1;
			d.pts += t.pts;
			d.oppPts += other.pts;
		}
	}
	return deltas;
};

type ResultRow = { tid: number; result: 1 | 0 | -1 | "OTL" };

// "Won 3" / "Lost 1" / "None", and "7-3", from a team's games newest first -
// the same text team.getFormatted makes from the season row, for when the
// season row counts a game that isn't to be shown yet.
export const streakAndLastTen = (
	results: ResultRow["result"][],
): { streak: string; lastTen: string } => {
	let streak = 0;
	for (const r of results) {
		if (r === 0) {
			break;
		}
		const won = r === 1;
		if (streak === 0) {
			streak = won ? 1 : -1;
		} else if (won && streak > 0) {
			streak += 1;
		} else if (!won && streak < 0) {
			streak -= 1;
		} else {
			break;
		}
	}
	let w = 0;
	let l = 0;
	let otl = 0;
	let t = 0;
	for (const r of results.slice(0, 10)) {
		if (r === 1) {
			w += 1;
		} else if (r === -1) {
			l += 1;
		} else if (r === "OTL") {
			otl += 1;
		} else {
			t += 1;
		}
	}
	let lastTen = `${w}-${l}`;
	if (otl > 0) {
		lastTen += `-${otl}`;
	}
	if (t > 0) {
		lastTen += `-${t}`;
	}
	return {
		streak:
			streak === 0
				? "None"
				: streak > 0
					? `Won ${streak}`
					: `Lost ${Math.abs(streak)}`,
		lastTen,
	};
};

// One team's regular season results, newest first, leaving out hidden games.
export const teamResultsNewestFirst = (
	games: readonly Game[],
	tid: number,
	hidden: ReadonlySet<number>,
	otlSetting: boolean,
): ResultRow["result"][] =>
	games
		.filter(
			(game) =>
				!game.playoffs &&
				!hidden.has(game.gid) &&
				(game.teams[0].tid === tid || game.teams[1].tid === tid),
		)
		.sort((a, b) => (b.day ?? 0) - (a.day ?? 0) || b.gid - a.gid)
		.map((game) => {
			const i = game.teams[0].tid === tid ? 0 : 1;
			const winner = getWinner(game.teams);
			if (winner === -1) {
				return 0;
			}
			if (winner === i) {
				return 1;
			}
			return otlSetting && game.overtimes > 0 ? "OTL" : -1;
		});

// A hidden game as a schedule row: the teams as they stood going in, the
// spread it was quoted at, and `live` so the page shows it in progress.
export const liveScheduleRow = (game: Game) => {
	const otlSetting = !!g.get("otl", game.season);
	const team = (i: 0 | 1) => {
		const t = game.teams[i];
		return teamGoingIn(
			game,
			i,
			{
				ovr: t.ovr ?? 0,
				tid: t.tid,
				won: t.won ?? 0,
				lost: t.lost ?? 0,
				tied: t.tied ?? 0,
				otl: t.otl ?? 0,
				playoffs: t.playoffs,
			},
			otlSetting,
		);
	};
	return {
		date: game.date,
		day: game.day ?? 0,
		finals: game.finals,
		forceWin: undefined,
		gid: game.gid,
		live: true as const,
		season: game.season,
		spread: game.spread,
		teams: [team(0), team(1)] as [
			ReturnType<typeof team>,
			ReturnType<typeof team>,
		],
	};
};

// The bracket as it stood before the hidden playoff games: each one's win and
// points out of its series, and every later round - drawn up only once this
// one was over, which took that game - empty again. A copy; the stored
// bracket is never touched. (Play-in games are left as they are.)
export const unplaySeries = <
	S extends {
		home: { tid: number; won: number; pts?: number; sPts?: number };
		away?: { tid: number; won: number; pts?: number; sPts?: number };
		gids?: number[];
	},
>(
	series: S[][],
	games: Iterable<Pick<Game, "gid" | "teams">>,
): S[][] => {
	let out = series;
	let copied = false;
	for (const game of games) {
		for (const [r, round] of out.entries()) {
			const i = round.findIndex((s) => s.gids?.includes(game.gid));
			if (i < 0) {
				continue;
			}
			if (!copied) {
				out = structuredClone(series);
				copied = true;
			}
			const s = out[r]![i]!;
			const winner = getWinner(game.teams);
			for (const t of game.teams) {
				const side =
					s.home.tid === t.tid
						? s.home
						: s.away?.tid === t.tid
							? s.away
							: undefined;
				if (!side) {
					continue;
				}
				if (winner !== -1 && game.teams[winner].tid === t.tid) {
					side.won = Math.max(0, side.won - 1);
				}
				if (side.pts !== undefined) {
					side.pts -= t.pts;
				}
				if (side.sPts !== undefined && t.sPts !== undefined) {
					side.sPts -= t.sPts;
				}
			}
			s.gids = s.gids!.filter((gid) => gid !== game.gid);
			if (s.gids.length === 0) {
				s.home.pts = undefined;
				s.home.sPts = undefined;
				if (s.away) {
					s.away.pts = undefined;
					s.away.sPts = undefined;
				}
			}
			for (let later = r + 1; later < out.length; later++) {
				out[later] = [];
			}
			break;
		}
	}
	return out;
};

// A news story that gives away a game still being played live: one about the
// game itself (game stories link its box score), or the series it ended.
export const eventGivesAwayLiveGame = (
	event: { text?: string; type: string; tids?: number[]; season?: number },
	gate: Pick<LiveGate, "hidden" | "games">,
): boolean => {
	if (gate.hidden.size === 0) {
		return false;
	}
	if (mentionsHiddenGame(event.text, gate.hidden)) {
		return true;
	}
	if (event.type === "playoffs" && event.tids && event.tids.length === 2) {
		for (const game of gate.games.values()) {
			if (
				game.playoffs &&
				(event.season === undefined || event.season === game.season) &&
				event.tids.includes(game.teams[0].tid) &&
				event.tids.includes(game.teams[1].tid)
			) {
				return true;
			}
		}
	}
	return false;
};
