// ONE PLAYOFF SERIES, AS ITS OWN STATS.
//
// Player Stats for a team in the playoffs can be narrowed to one of its series:
// the games against that one opponent. Nothing stores stats that finely, so
// they are added up here from the series' box scores - the same lines the
// season rows are built from, game by game - and the advanced stats are worked
// out the same way the season's are, with the series as the whole league: its
// two teams, everyone who played in it.

import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import { season, team } from "../core/index.ts";
import stats from "../core/player/stats.ts";
import { processPlayerStats } from "../util/processPlayerStats.ts";
import { PLAYER_STATS } from "../util/advStatsCommon.ts";
import {
	ADV_TEAM_STATS,
	computeAdvStats,
} from "../util/advStats.basketball.ts";
import { makeResponsiveDropdownOption } from "../../common/makeResponsiveDropdownOption.tsx";
import type { Game, Player } from "../../common/types.ts";
import { last } from "../../common/utils.ts";

export type TeamSeries = {
	// "r1", "r2", ... - the dropdown key and the URL segment.
	key: string;
	round: number;
	oppTid: number;
	gids: number[] | undefined;
};

export const seriesKey = (round: number) => `r${round + 1}`;

export const roundFromSeriesKey = (key: string | undefined) => {
	const m = key === undefined ? null : /^r(\d+)$/.exec(key);
	return m ? Number(m[1]) - 1 : undefined;
};

// The series this team has played (or is playing) in a season, first round
// first - those with box scores to add up: started, and not from a season
// whose box scores the league has since dropped.
export const teamPlayoffSeries = async (
	tid: number,
	seasonNum: number,
): Promise<TeamSeries[]> => {
	const playoffSeries = await idb.merged.playoffSeries.get(seasonNum);
	const out: TeamSeries[] = [];
	if (!playoffSeries) {
		return out;
	}
	for (const [round, matchups] of playoffSeries.series.entries()) {
		for (const { home, away, gids } of matchups) {
			// (A bye, or a seed still waiting on its play-in.)
			if (!away || away.pendingPlayIn) {
				continue;
			}
			const oppTid =
				home.tid === tid ? away.tid : away.tid === tid ? home.tid : undefined;
			if (oppTid === undefined) {
				continue;
			}
			// Not started: nothing to show yet.
			if (home.won + away.won === 0 && home.pts === undefined) {
				continue;
			}
			// (No gids: a league from before series kept them - try the games.)
			if (
				gids === undefined ||
				(gids.length > 0 &&
					(await idb.merged.games.get(gids[0]!)) !== undefined)
			) {
				out.push({ key: seriesKey(round), round, oppTid, gids });
			}
		}
	}
	return out;
};

// The menu entries for those series: "R1 vs WAS", or on a wide screen
// "1st round vs WAS".
export const seriesDropdownOptions = async (
	series: TeamSeries[],
	seasonNum: number,
) => {
	if (series.length === 0) {
		return [];
	}
	const playoffSeries = await idb.merged.playoffSeries.get(seasonNum);
	const numRounds = playoffSeries?.series.length ?? 0;
	const byConf = await season.getPlayoffsByConf(seasonNum);
	const teamInfoCache = g.get("teamInfoCache");
	return series.map(({ key, round, oppTid }) => {
		const opp = teamInfoCache[oppTid]?.abbrev ?? "?";
		return {
			key,
			value: makeResponsiveDropdownOption(
				`R${round + 1} vs ${opp}`,
				`${helpers.upperCaseFirstLetter(
					helpers.playoffRoundName(round, numRounds, byConf),
				)} vs ${opp}`,
			),
		};
	});
};

const RAW = stats.raw.filter(
	(key) =>
		key !== "minAvailable" &&
		key !== "dd" &&
		key !== "td" &&
		key !== "qd" &&
		key !== "fxf",
);
const MAX = stats.max.map((key) => [key, key.replace("Max", "")] as const);
const DERIVED_MAX = ["2p", "2pa", "trb", "gmsc"];

// One game's line, added to a running row the way a season row takes it (see
// writePlayerStats): totals, double-doubles and the like, and game highs.
const addLine = (row: any, line: any, gid: number) => {
	for (const key of RAW) {
		row[key] =
			(row[key] ?? 0) + (typeof line[key] === "number" ? line[key] : 0);
	}
	if (!(line.gp > 0)) {
		return;
	}
	let numDoubles = 0;
	let numFives = 0;
	for (const stat of ["pts", "ast", "stl", "blk", "trb"]) {
		const value =
			stat === "trb" ? (line.orb ?? 0) + (line.drb ?? 0) : (line[stat] ?? 0);
		if (value >= 5) {
			numFives += 1;
			if (value >= 10) {
				numDoubles += 1;
			}
		}
	}
	row.dd += numDoubles >= 2 ? 1 : 0;
	row.td += numDoubles >= 3 ? 1 : 0;
	row.qd += numDoubles >= 4 ? 1 : 0;
	row.fxf += numFives >= 5 ? 1 : 0;

	const derived = processPlayerStats(line, DERIVED_MAX);
	for (const [key, stat] of MAX) {
		const value = line[stat] ?? derived[stat];
		if (typeof value === "number" && (!row[key] || value > row[key][0])) {
			row[key] = [value, gid];
		}
	}
};

const sideOf = (game: Game, tid: number) =>
	game.teams[0].tid === tid ? 0 : game.teams[1].tid === tid ? 1 : undefined;

// Per player who played in the series, for the team asked about: a stats row
// for the series, shaped like a season's playoff row, advanced stats included.
export const seriesStatsRows = async ({
	tid,
	series,
	seasonNum,
}: {
	tid: number;
	series: TeamSeries;
	seasonNum: number;
}): Promise<Map<number, any>> => {
	const isSeriesGame = (game: Game | undefined): game is Game =>
		game !== undefined &&
		!!game.playoffs &&
		sideOf(game, tid) !== undefined &&
		sideOf(game, series.oppTid) !== undefined;
	const games = (
		series.gids
			? (
					await Promise.all(series.gids.map((gid) => idb.merged.games.get(gid)))
				).filter(isSeriesGame)
			: await idb.merged.games.getAll(
					{ season: seasonNum },
					{ filter: isSeriesGame },
				)
	).sort((a, b) => a.gid - b.gid);

	const rowsByPid = new Map<number, any>();
	const tidOf = new Map<number, number>();
	const teamRows = new Map<number, Record<string, number>>();
	for (const game of games) {
		for (const side of [0, 1] as const) {
			const t: any = game.teams[side];
			const o: any = game.teams[side === 0 ? 1 : 0];
			let tr = teamRows.get(t.tid);
			if (!tr) {
				tr = { gp: 0 };
				teamRows.set(t.tid, tr);
			}
			tr.gp! += 1;
			for (const [key, value] of Object.entries(t)) {
				if (typeof value === "number" && key !== "tid" && key !== "gp") {
					tr[key] = (tr[key] ?? 0) + value;
				}
			}
			for (const [key, value] of Object.entries(o)) {
				if (typeof value === "number" && key !== "tid" && key !== "gp") {
					const oppKey = `opp${key[0]!.toUpperCase()}${key.slice(1)}`;
					tr[oppKey] = (tr[oppKey] ?? 0) + value;
				}
			}
			for (const line of t.players as any[]) {
				let row = rowsByPid.get(line.pid);
				if (!row) {
					row = {
						season: seasonNum,
						tid: t.tid,
						playoffs: true,
						dd: 0,
						td: 0,
						qd: 0,
						fxf: 0,
					};
					rowsByPid.set(line.pid, row);
					tidOf.set(line.pid, t.tid);
				}
				if (line.jerseyNumber !== undefined) {
					row.jerseyNumber = line.jerseyNumber;
				}
				addLine(row, line, game.gid);
			}
		}
	}

	// Advanced: everyone in the series, the series as the league.
	const pids = [...rowsByPid.keys()].filter((pid) => rowsByPid.get(pid).gp > 0);
	if (pids.length > 0 && teamRows.size === 2) {
		const playersRaw = await idb.merged.players.getAll({ pids });
		const posOf = new Map<number, string>();
		for (const p of playersRaw as Player[]) {
			const ratings =
				p.ratings.findLast((r) => r.season <= seasonNum) ?? last(p.ratings);
			posOf.set(p.pid, ratings?.pos ?? "F");
		}
		const players = pids.map((pid) => ({
			pid,
			tid: tidOf.get(pid)!,
			ratings: { pos: posOf.get(pid) ?? "F" },
			stats: processPlayerStats(rowsByPid.get(pid), PLAYER_STATS, "totals"),
		}));
		const teams = [...teamRows].map(([teamTid, tr]) => ({
			tid: teamTid,
			stats: team.processStats(
				tr as any,
				ADV_TEAM_STATS as any,
				true,
				"totals",
			) as any,
		}));
		const adv = computeAdvStats(players, teams);
		if (adv) {
			for (const [i, { pid }] of players.entries()) {
				const row = rowsByPid.get(pid);
				for (const [key, values] of Object.entries(adv)) {
					const value = (values as number[])[i];
					if (value !== undefined && !Number.isNaN(value)) {
						row[key] = value;
					}
				}
			}
		}
	}

	for (const [pid, row] of rowsByPid) {
		if (row.tid !== tid) {
			rowsByPid.delete(pid);
		}
	}
	return rowsByPid;
};
