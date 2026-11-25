// WHAT A LIVE GAME LEAVES OUT OF THE NUMBERS, ON SCREEN ONLY.
//
// liveGameGate.ts says which games are still being played live on somebody's
// screen, and the schedules, box score, standings and bracket already show
// those as in progress. But a game is played in full the moment it tips off,
// so its result is also in every total it fed: the teams' records, splits,
// streaks and team stats, each player's season line and season highs, an
// injury picked up in it, the head-to-head record, a series it won. This takes
// the hidden games back out of COPIES of those rows, for the pages showing
// them. The rows themselves - what the sim reads and what syncs - are never
// touched.
//
// Pages only: team.getFormattedAll's and player.format's `liveShown` option,
// or the methods directly on rows a page reads itself. Never hand it to
// anything that decides something - in a shared league every device has to
// reach the same decisions from the same data, and only some devices are
// holding a game back.
//
// What it can't take back out: the advanced stats worked out from the season
// totals after each day (PER, win shares and the like), and money (revenue,
// hype). Neither says who won.

import { idb } from "../db/index.ts";
import g from "./g.ts";
import {
	eventGivesAwayLiveGame,
	getLiveGate,
	recordDeltas,
	unplaySeries,
	type RecordDelta,
} from "./liveGameGate.ts";
import { processPlayerStats } from "./processPlayerStats.ts";
import playerStatsInfo from "../core/player/stats.ts";
import teamStatsInfo from "../core/team/stats.ts";
import getWinner from "../../common/getWinner.ts";
import { bySport } from "../../common/sportFunctions.ts";
import type {
	Game,
	HeadToHead,
	LiveShown,
	PlayerInjury,
	PlayoffSeries,
} from "../../common/types.ts";

type Box = Record<string, any>;

// The regular season record keys a game adds to a team season row.
const RECORD_KEYS = [
	"won",
	"lost",
	"tied",
	"otl",
	"wonHome",
	"lostHome",
	"tiedHome",
	"otlHome",
	"wonAway",
	"lostAway",
	"tiedAway",
	"otlAway",
	"wonDiv",
	"lostDiv",
	"tiedDiv",
	"otlDiv",
	"wonConf",
	"lostConf",
	"tiedConf",
	"otlConf",
] as const satisfies readonly (keyof RecordDelta)[];

// As in writePlayerStats.
const DERIVED_MAX_STATS = bySport({
	baseball: ["ab", "ip", "tb", "gmsc"],
	basketball: ["2p", "2pa", "trb", "gmsc"],
	hockey: ["g", "a", "pts"],
	football: undefined,
});

// Football's "longest" stats are kept as a maximum, which can't be taken back.
const isLongest = (key: string) =>
	__SPORT === "football" && key.endsWith("Lng");

const lowerFirst = (s: string) => s.charAt(0).toLowerCase() + s.slice(1);

// row[key] minus value, for a number or a per-position array.
const minus = (row: Record<string, any>, key: string, value: unknown) => {
	const current = row[key];
	if (typeof current === "number" && typeof value === "number") {
		row[key] = current - value;
	} else if (Array.isArray(current) && Array.isArray(value)) {
		row[key] = current.map((x, i) =>
			typeof x === "number" && typeof value[i] === "number" ? x - value[i] : x,
		);
	}
};

// Newest first, as in liveGameGate's teamResultsNewestFirst, in the stored
// lastTen's own values (1 won, 0 lost, "OTL", -1 tied).
const lastTenAndStreak = (
	games: readonly Game[],
	tid: number,
	hides: (gid: number) => boolean,
	otlSetting: boolean,
) => {
	const results = games
		.filter(
			(game) =>
				!game.playoffs &&
				!hides(game.gid) &&
				(game.teams[0].tid === tid || game.teams[1].tid === tid),
		)
		.sort((a, b) => (b.day ?? 0) - (a.day ?? 0) || b.gid - a.gid)
		.map((game): 1 | 0 | "OTL" | -1 => {
			const i = game.teams[0].tid === tid ? 0 : 1;
			const winner = getWinner(game.teams);
			if (winner === -1) {
				return -1;
			}
			if (winner === i) {
				return 1;
			}
			return otlSetting && game.overtimes > 0 ? "OTL" : 0;
		});

	let streak = 0;
	for (const r of results) {
		if (r === -1) {
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

	return { lastTen: results.slice(0, 10), streak };
};

// Where each hidden playoff game sits: its round, or a play-in game.
export const playoffPlaceOf = (
	gid: number,
	playoffSeries: Pick<PlayoffSeries, "series" | "playIns"> | undefined,
): number | "playIn" | undefined => {
	if (!playoffSeries) {
		return;
	}
	for (const [round, matchups] of playoffSeries.series.entries()) {
		if (matchups.some((s) => s.gids?.includes(gid))) {
			return round;
		}
	}
	if (playoffSeries.playIns && playoffSeries.playIns.length > 0) {
		return "playIn";
	}
};

// Pure, for the tests: everything but the reads.
export const buildLiveShown = ({
	games,
	season,
	otlSetting,
	divByTid,
	confByTid,
	seasonGames,
	playoffSeries,
}: {
	// This season's rows of the hidden games.
	games: readonly Game[];
	season: number;
	otlSetting: boolean;
	divByTid: ReadonlyMap<number, number>;
	confByTid: ReadonlyMap<number, number>;
	// Every game of the season (read only if a page needs a streak, last ten
	// or season high worked out again).
	seasonGames: () => readonly Game[];
	playoffSeries: Pick<PlayoffSeries, "series" | "playIns"> | undefined;
}): LiveShown => {
	const hiddenGids = new Set(games.map((game) => game.gid));
	const hides = (gid: number | undefined) =>
		gid !== undefined && hiddenGids.has(gid);

	const recordByTid = recordDeltas(games, { otlSetting, divByTid, confByTid });

	// Home games: attendance (writeTeamStats counts every home game, playoffs
	// too).
	const homeByTid = new Map<number, { gp: number; att: number }>();
	// How many playoff rounds a team had won going in.
	const roundsWonByTid = new Map<number, number>();
	// Each team's side of each game, by team stats row.
	const teamSides = new Map<string, { game: Game; i: 0 | 1 }[]>();
	// Each player's box score line, by pid.
	const playerSides = new Map<
		number,
		{ game: Game; tid: number; box: Box }[]
	>();
	for (const game of games) {
		for (const i of [0, 1] as const) {
			const t = game.teams[i];
			if (t.tid < 0) {
				continue;
			}
			const key = `${t.tid}:${game.playoffs ? 1 : 0}`;
			const sides = teamSides.get(key) ?? [];
			sides.push({ game, i });
			teamSides.set(key, sides);

			for (const box of (t.players ?? []) as Box[]) {
				const list = playerSides.get(box.pid) ?? [];
				list.push({ game, tid: t.tid, box });
				playerSides.set(box.pid, list);
			}

			if (game.playoffs) {
				const place = playoffPlaceOf(game.gid, playoffSeries);
				if (place !== undefined) {
					const wonGoingIn = place === "playIn" ? -1 : place;
					roundsWonByTid.set(
						t.tid,
						Math.min(roundsWonByTid.get(t.tid) ?? Infinity, wonGoingIn),
					);
				}
			}
		}
		const homeTid = game.teams[0].tid;
		if (homeTid >= 0) {
			const home = homeByTid.get(homeTid) ?? { gp: 0, att: 0 };
			home.gp += 1;
			home.att += typeof game.att === "number" ? game.att : 0;
			homeByTid.set(homeTid, home);
		}
	}

	let seasonGamesSorted: readonly Game[] | undefined;
	const getSeasonGames = () => {
		if (!seasonGamesSorted) {
			seasonGamesSorted = seasonGames()
				.filter((game) => game.season === season)
				.sort((a, b) => (a.day ?? 0) - (b.day ?? 0) || a.gid - b.gid);
		}
		return seasonGamesSorted;
	};

	const lastTenByTid = new Map<number, ReturnType<typeof lastTenAndStreak>>();

	// A player's box score lines in the season's games still shown.
	const appearancesByPid = new Map<
		number,
		{ game: Game; tid: number; box: Box }[]
	>();
	const appearances = (pid: number) => {
		let list = appearancesByPid.get(pid);
		if (!list) {
			list = [];
			for (const game of getSeasonGames()) {
				if (hides(game.gid)) {
					continue;
				}
				for (const t of game.teams) {
					const box = (t.players as Box[] | undefined)?.find(
						(p) => p.pid === pid,
					);
					if (box) {
						list.push({ game, tid: t.tid, box });
					}
				}
			}
			appearancesByPid.set(pid, list);
		}
		return list;
	};

	// Whether he had dressed for this team, since he last played for another,
	// in a game still shown - so the row was there going in.
	const dressedGoingIn = (pid: number, tid: number, playoffs: boolean) => {
		const list = appearances(pid).filter((x) => !!x.game.playoffs === playoffs);
		const lastElsewhere = list.findLastIndex((x) => x.tid !== tid);
		return list.some((x, i) => x.tid === tid && i > lastElsewhere);
	};

	// A season high set in a hidden game: the best of his other games, oldest
	// first like writePlayerStats (a later game has to beat it, not tie it).
	const seasonHigh = (
		pid: number,
		tid: number,
		playoffs: boolean,
		maxKey: string,
	): [number, number] | null => {
		const stat = maxKey.replace("Max", "");
		// (null, as addStatsRow has it before there's a high.)
		let best: [number, number] | null = null;
		for (const { game, tid: gameTid, box } of appearances(pid)) {
			if (gameTid !== tid || !!game.playoffs !== playoffs || !(box.gp > 0)) {
				continue;
			}
			const value =
				box[stat] ??
				(DERIVED_MAX_STATS
					? (processPlayerStats(box as any, DERIVED_MAX_STATS) as Box)[stat]
					: undefined);
			if (typeof value === "number" && (!best || value > best[0])) {
				best = [value, game.gid];
			}
		}
		return best;
	};

	const teamSeason: LiveShown["teamSeason"] = (ts) => {
		if (ts.season !== season) {
			return ts;
		}
		const record = recordByTid.get(ts.tid);
		const home = homeByTid.get(ts.tid);
		const roundsWon = roundsWonByTid.get(ts.tid);
		if (!record && !home && roundsWon === undefined) {
			return ts;
		}
		const out: Record<string, any> = { ...ts };
		if (record) {
			for (const key of RECORD_KEYS) {
				if (typeof out[key] === "number") {
					out[key] = Math.max(0, out[key] - record[key]);
				}
			}
			let recent = lastTenByTid.get(ts.tid);
			if (!recent) {
				recent = lastTenAndStreak(getSeasonGames(), ts.tid, hides, otlSetting);
				lastTenByTid.set(ts.tid, recent);
			}
			if (Array.isArray(out.lastTen)) {
				out.lastTen = recent.lastTen;
			}
			if (typeof out.streak === "number") {
				out.streak = recent.streak;
			}
		}
		if (home) {
			if (typeof out.gpHome === "number") {
				out.gpHome = Math.max(0, out.gpHome - home.gp);
			}
			if (typeof out.att === "number") {
				out.att = Math.max(0, out.att - home.att);
			}
		}
		if (roundsWon !== undefined && typeof out.playoffRoundsWon === "number") {
			out.playoffRoundsWon = Math.min(out.playoffRoundsWon, roundsWon);
		}
		return out as typeof ts;
	};

	const teamStats: LiveShown["teamStats"] = (ts) => {
		if (ts.season !== season) {
			return ts;
		}
		const sides = teamSides.get(`${ts.tid}:${ts.playoffs ? 1 : 0}`);
		if (!sides) {
			return ts;
		}
		const out: Record<string, any> = { ...ts };
		for (const { game, i } of sides) {
			const own = game.teams[i] as Box;
			const other = game.teams[i === 0 ? 1 : 0] as Box;
			for (const key of teamStatsInfo.raw) {
				if (key === "gp") {
					if (typeof out.gp === "number") {
						out.gp = Math.max(0, out.gp - 1);
					}
					continue;
				}
				if (isLongest(key)) {
					continue;
				}
				const isOpp = /^opp[\dA-Z]/.test(key);
				minus(out, key, isOpp ? other[lowerFirst(key.slice(3))] : own[key]);
			}
			if (__SPORT === "hockey") {
				// Kept by writeTeamStats apart from the box score.
				if (other.pts === 0) {
					minus(out, "so", 1);
				}
				if (own.pts === 0) {
					minus(out, "oppSo", 1);
				}
			}
		}
		return out as typeof ts;
	};

	const player: LiveShown["player"] = (p) => {
		const sides = playerSides.get(p.pid);
		if (!sides) {
			return p;
		}
		const out: Record<string, any> = { ...p };
		let stats = p.stats;
		for (const { game, tid, box } of sides) {
			const played = box.gp > 0;
			// Minutes he was available for count for everybody healthy at tip-off
			// and still healthy at the end, played or not (writePlayerStats).
			const injury = box.injury as
				| (PlayerInjury & { newThisGame?: boolean })
				| undefined;
			const ownTeam = game.teams[0].tid === tid ? game.teams[0] : game.teams[1];
			const minAvailable =
				!injury?.newThisGame &&
				(injury?.gamesRemaining ?? 0) === 0 &&
				typeof ownTeam.min === "number"
					? ownTeam.min / 5
					: 0;
			const playoffs = !!game.playoffs;
			if (!played && minAvailable === 0 && !playoffs) {
				continue;
			}
			const index = stats.findLastIndex(
				(row) =>
					row.season === season &&
					row.tid === tid &&
					!!row.playoffs === playoffs,
			);
			if (index < 0) {
				continue;
			}
			if (stats === p.stats) {
				stats = [...stats];
			}
			const row: Record<string, any> = { ...stats[index] };
			if (typeof row.minAvailable === "number") {
				minus(row, "minAvailable", minAvailable);
			}
			if (played) {
				for (const key of playerStatsInfo.raw) {
					if (!isLongest(key) && key !== "minAvailable") {
						minus(row, key, box[key]);
					}
				}
				for (const key of playerStatsInfo.max) {
					const high = row[key];
					if (Array.isArray(high) && hides(high[1])) {
						row[key] = seasonHigh(p.pid, tid, playoffs, key);
					}
				}
			}
			if (!(row.gp > 0) && !dressedGoingIn(p.pid, tid, playoffs)) {
				// His first game for this team (writePlayerStats adds the row as he
				// dresses for it): the row didn't exist going in.
				stats.splice(index, 1);
			} else {
				stats[index] = row as (typeof stats)[number];
			}
		}
		out.stats = stats;

		// An injury he picked up in it: not hurt yet (or still playing through
		// what he had at tip-off).
		for (const { box } of sides) {
			const injury = box.injury as
				| (PlayerInjury & { newThisGame?: boolean })
				| undefined;
			if (!injury?.newThisGame) {
				continue;
			}
			// (Unless it's healed already: a one game injury is over by the next
			// day.)
			if (out.injury?.type === injury.type && out.injury.gamesRemaining > 0) {
				out.injury = box.injuryAtStart
					? {
							type: box.injuryAtStart.type,
							gamesRemaining: box.injuryAtStart.gamesRemaining,
						}
					: { type: "Healthy", gamesRemaining: 0 };
			}
			const injuries = out.injuries as
				| { season: number; type: string }[]
				| undefined;
			const lastInjury = injuries?.at(-1);
			if (
				injuries &&
				lastInjury &&
				lastInjury.season === season &&
				lastInjury.type === injury.type
			) {
				const injuryIndex = injuries.length - 1;
				out.injuries = injuries.slice(0, -1);
				// And the ratings it cost him (writePlayerStats' doInjury).
				const ratings = out.ratings as { injuryIndex?: number }[] | undefined;
				if (
					ratings &&
					ratings.length > 1 &&
					ratings.at(-1)!.injuryIndex === injuryIndex
				) {
					out.ratings = ratings.slice(0, -1);
				}
			}
		}

		return out as typeof p;
	};

	const headToHead: LiveShown["headToHead"] = (row) => {
		if (row.season !== season) {
			return row;
		}
		let out: HeadToHead | undefined;
		for (const game of games) {
			const tids = [game.teams[0].tid, game.teams[1].tid];
			if (tids[0]! < 0 || tids[1]! < 0) {
				continue;
			}
			const t0 = Math.min(tids[0]!, tids[1]!);
			const t1 = Math.max(tids[0]!, tids[1]!);
			const i = tids.indexOf(t0) as 0 | 1;
			const j = i === 0 ? 1 : 0;
			const winner = getWinner(game.teams);
			const type = game.playoffs ? "playoffs" : "regularSeason";
			const entry = row[type][t0]?.[t1];
			if (!entry) {
				continue;
			}
			out ??= structuredClone(row);
			const e: Record<string, any> = out[type][t0]![t1]!;
			const overtime = game.overtimes > 0 && otlSetting;
			if (type === "regularSeason") {
				const key =
					winner === -1
						? "tied"
						: winner === i
							? overtime
								? "otw"
								: "won"
							: overtime
								? "otl"
								: "lost";
				e[key] = Math.max(0, e[key] - 1);
			} else {
				const key = winner === i ? "won" : "lost";
				e[key] = Math.max(0, e[key] - 1);
				// A series is decided in its last game.
				e.result = undefined;
			}
			e.pts -= game.teams[i].pts;
			e.oppPts -= game.teams[j].pts;
			const played =
				(e.won ?? 0) +
				(e.lost ?? 0) +
				(e.tied ?? 0) +
				(e.otl ?? 0) +
				(e.otw ?? 0);
			if (played <= 0) {
				delete out[type][t0]![t1];
				if (Object.keys(out[type][t0]!).length === 0) {
					delete out[type][t0];
				}
			}
		}
		return out ?? row;
	};

	const series: LiveShown["series"] = (rows) =>
		unplaySeries(
			rows,
			games.filter((game) => game.playoffs),
		);

	// Who was hurt in one of these games, and how.
	const injuredHere = new Map<number, string>();
	for (const [pid, sides] of playerSides) {
		for (const { box } of sides) {
			if (box.injury?.newThisGame && typeof box.injury.type === "string") {
				injuredHere.set(pid, box.injury.type);
			}
		}
	}
	const gate = {
		hidden: hiddenGids,
		games: new Map(games.map((game) => [game.gid, game])),
	};
	const events: LiveShown["events"] = (list) => {
		// The newest story of each injury picked up in one (its text names it;
		// an older one of the same kind is from before).
		const injuryEids = new Set<number>();
		for (const [pid, type] of injuredHere) {
			let latest: (typeof list)[number] | undefined;
			for (const event of list) {
				if (
					event.type === "injured" &&
					event.eid !== undefined &&
					(event.season === undefined || event.season === season) &&
					event.pids?.[0] === pid &&
					event.text?.includes(type) &&
					(latest === undefined || event.eid > latest.eid!)
				) {
					latest = event;
				}
			}
			if (latest) {
				injuryEids.add(latest.eid!);
			}
		}
		return list.filter(
			(event) =>
				!(event.eid !== undefined && injuryEids.has(event.eid)) &&
				!eventGivesAwayLiveGame(event, gate),
		);
	};

	return { player, teamSeason, teamStats, headToHead, series, events, hides };
};

// undefined when no game is being held back here: pages pass it on as is.
// Never throws; on any trouble the page just shows the stored numbers.
export const getLiveShown = async (): Promise<LiveShown | undefined> => {
	const gate = await getLiveGate();
	if (gate.games.size === 0) {
		return;
	}
	try {
		const season = g.get("season");
		const teams = await idb.cache.teams.getAll();
		// (The cache holds this season's games, and hands back its own rows:
		// only ever read here.)
		const seasonGames = await idb.cache.games.getAll();
		return buildLiveShown({
			games: [...gate.games.values()],
			season,
			otlSetting: !!g.get("otl", season),
			divByTid: new Map(teams.map((t) => [t.tid, t.did])),
			confByTid: new Map(teams.map((t) => [t.tid, t.cid])),
			seasonGames: () => seasonGames,
			playoffSeries: [...gate.games.values()].some((game) => game.playoffs)
				? await idb.cache.playoffSeries.get(season)
				: undefined,
		});
	} catch (error) {
		console.error("Live game shown numbers failed", error);
		return;
	}
};
