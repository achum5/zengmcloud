import { idb } from "../../db/index.ts";
import { g, helpers, logEvent } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { syncDebugLog } from "./debugLog.ts";

// ---------------------------------------------------------------------------
// LEAGUE HISTORY SELF-REPAIR
//
// Every "league champion" display in the app - the season History page, the
// all-time list, trivia - derives from teamSeasons.playoffRoundsWon. Under
// whole-record last-write-wins sync, that field is exactly the kind of thing a
// messy recovery can leave stale: a replay applies an old vintage of one
// team's row, nothing later touches it, and a finished season suddenly shows
// no champion (???) on one device - which then looks like the season itself
// broke.
//
// But the field is DERIVED data. The playoff bracket (playoffSeries) is the
// ground truth: who appeared in which round and who won each series says
// precisely how many rounds every team won. So instead of chasing each new way
// a recovery can smudge it, recompute it from the bracket and fix what
// disagrees. Deterministic, local, no network - every device heals itself.
//
// Runs after the paths that can land stale rows (snapshot restore, connect
// after a rough session) - and before the sim authority publishes a room
// snapshot, so a device with a hole it cannot repair never becomes the room's
// source of truth.
// ---------------------------------------------------------------------------

type SeriesTeam = {
	tid: number;
	won?: number;
	pendingPlayIn?: boolean;
};

type SeriesMatchup = {
	home: SeriesTeam;
	away?: SeriesTeam;
};

// How many playoff rounds each team in the bracket won, derived from the
// bracket alone. Appearing in round r proves r rounds won; winning a decided
// round-r series proves r+1 (the champion has no later round to appear in, so
// the final needs this). Teams not in the bracket (missed playoffs, lost the
// play-in) are simply absent - the caller must leave their rows alone.
export const roundsWonFromSeries = (
	series: SeriesMatchup[][],
	numGamesPlayoffSeries: number[],
): Map<number, number> => {
	const out = new Map<number, number>();
	const claim = (tid: number, rounds: number) => {
		if (rounds > (out.get(tid) ?? -1)) {
			out.set(tid, rounds);
		}
	};

	for (const [r, round] of series.entries()) {
		const numGamesToWin = helpers.numGamesToWinSeries(numGamesPlayoffSeries[r]);
		for (const matchup of round) {
			const { home, away } = matchup;
			// A pending play-in slot holds a placeholder, not a team that has made
			// the playoffs.
			if (home && !home.pendingPlayIn) {
				claim(home.tid, r);
			}
			if (away && !away.pendingPlayIn) {
				claim(away.tid, r);
			}

			if (!home || home.pendingPlayIn || away?.pendingPlayIn) {
				continue;
			}
			if (!away) {
				// A bye: home advances without playing.
				claim(home.tid, r + 1);
			} else if ((home.won ?? 0) >= numGamesToWin) {
				claim(home.tid, r + 1);
			} else if ((away.won ?? 0) >= numGamesToWin) {
				claim(away.tid, r + 1);
			}
		}
	}

	return out;
};

// The playoffSeries record for one season, from the cache when it's the
// current season, else from the league DB (the cache only holds the current
// season's bracket).
const getSeries = async (season: number): Promise<any | undefined> => {
	try {
		const fromCache = await idb.cache.playoffSeries.get(season);
		if (fromCache) {
			return fromCache;
		}
	} catch {
		// Fall through to the league DB.
	}
	try {
		return await (idb.league as any)?.get("playoffSeries", season);
	} catch {
		return undefined;
	}
};

// One team's season row: cache first (recent seasons live there, and a direct
// DB write would be clobbered by the next flush), league DB for older seasons.
const getTeamSeasonRow = async (
	season: number,
	tid: number,
): Promise<{ row: any; inCache: boolean } | undefined> => {
	try {
		const fromCache = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[season, tid],
		);
		if (fromCache) {
			return { row: fromCache, inCache: true };
		}
	} catch {
		// Fall through.
	}
	try {
		const fromDb = await (idb.league as any)?.getFromIndex(
			"teamSeasons",
			"season, tid",
			[season, tid],
		);
		if (fromDb) {
			return { row: fromDb, inCache: false };
		}
	} catch {
		// Missing store/index - treated as absent.
	}
	return undefined;
};

export type HistoryRepairResult = {
	// teamSeasons rows whose playoffRoundsWon was corrected.
	repaired: number;
	// Human-readable descriptions of what could NOT be made right locally -
	// a torn bracket, a missing champion row. A device reporting any of these
	// needs a snapshot from a healthy league-mate, and must not publish one.
	problems: string[];
};

// Recompute playoffRoundsWon from the bracket for one COMPLETED season and fix
// any row that disagrees.
export const repairSeasonHistory = async (
	season: number,
): Promise<HistoryRepairResult> => {
	const result: HistoryRepairResult = { repaired: 0, problems: [] };

	let numRounds: number;
	try {
		numRounds = g.get("numGamesPlayoffSeries", season).length;
	} catch {
		return result;
	}
	if (numRounds === 0) {
		// A league without playoffs has no champion to check.
		return result;
	}

	const playoffSeries = await getSeries(season);
	if (!playoffSeries || !Array.isArray(playoffSeries.series)) {
		// No bracket at all. That can be legitimate (Delete Old Data, manual
		// imports), so it's only a PROBLEM when partial team data exists yet no
		// champion does - checked cheaply via the same rows the History page uses.
		// Without a bracket there is nothing to repair from either way.
		return result;
	}

	const expected = roundsWonFromSeries(
		playoffSeries.series,
		g.get("numGamesPlayoffSeries", season),
	);
	if (expected.size === 0) {
		return result;
	}

	let champions = 0;
	for (const [tid, rounds] of expected) {
		if (rounds === numRounds) {
			champions += 1;
		}

		const found = await getTeamSeasonRow(season, tid);
		if (!found) {
			// The row itself is gone. Nothing local can rebuild a whole teamSeason,
			// so say so - the cure is a snapshot from a device that still has it.
			result.problems.push(
				`teamSeasons ${season} for tid ${tid} is missing entirely`,
			);
			continue;
		}

		if (found.row.playoffRoundsWon !== rounds) {
			found.row.playoffRoundsWon = rounds;
			if (found.inCache) {
				await idb.cache.teamSeasons.put(found.row);
			} else {
				try {
					await (idb.league as any)?.put("teamSeasons", found.row);
				} catch {
					result.problems.push(
						`could not write repaired teamSeasons ${season} for tid ${tid}`,
					);
					continue;
				}
			}
			result.repaired += 1;
		}
	}

	if (champions !== 1) {
		// The bracket itself cannot name a champion (torn finals). Local data is
		// not trustworthy enough to broadcast.
		result.problems.push(
			`season ${season}: bracket yields ${champions} champions`,
		);
	}

	return result;
};

// True when the season's playoffs have finished, so its bracket is final and
// safe to reconcile against. The current season qualifies once past the
// playoffs; earlier seasons always do.
const lastCompletedSeason = () =>
	g.get("phase") > PHASE.PLAYOFFS ? g.get("season") : g.get("season") - 1;

// Every completed season, oldest first. Returns what was fixed and what
// cannot be fixed locally.
export const repairLeagueHistory = async (
	reason: string,
): Promise<HistoryRepairResult> => {
	const total: HistoryRepairResult = { repaired: 0, problems: [] };

	let startingSeason: number;
	try {
		startingSeason = g.get("startingSeason");
	} catch {
		return total;
	}
	const last = lastCompletedSeason();

	for (let season = startingSeason; season <= last; season++) {
		try {
			const result = await repairSeasonHistory(season);
			total.repaired += result.repaired;
			total.problems.push(...result.problems);
		} catch (error) {
			syncDebugLog("historyRepair:season-failed", { season, error });
		}
	}

	if (total.repaired > 0 || total.problems.length > 0) {
		syncDebugLog("historyRepair:done", { reason, ...total });
	}
	if (total.repaired > 0) {
		// Say it happened: the alternative is a champion that silently changes
		// from ??? back to real and nobody knowing why.
		try {
			logEvent({
				type: "success",
				text: `Repaired playoff history for ${total.repaired} team season${
					total.repaired === 1 ? "" : "s"
				}.`,
				saveToDb: false,
			});
		} catch {
			// The repair itself already happened; the toast is a courtesy.
		}
	}

	return total;
};
