import { PHASE } from "../../common/constants.ts";
import { betterSeedHome } from "../../common/sportsbookOdds.ts";
import { ifNecessary } from "../core/season/playoffCalendar.ts";
import { idb } from "../db/index.ts";
import g from "./g.ts";
import helpers from "./helpers.ts";
import { getLiveGate, unplaySeries } from "./liveGameGate.ts";

export type PlannedPlayoffGame = {
	date: string;
	homeTid: number;
	awayTid: number;
	playIn?: true;
	ifNecessary?: true;
	// For the series standing beside it.
	home: { tid: number; seed: number; won: number };
	away: { tid: number; seed: number; won: number };
};

// THE PLAYOFF GAMES STILL TO COME ON THE NBA CALENDAR'S PLAN (see
// playoffCalendar.ts) - the current round's, past the games already played,
// with "if necessary" on the ones that may not be. As the bracket stands on
// screen: a game still being played live hasn't happened yet (see
// liveGameGate.ts), so neither has a series it ended or the round after.
// Includes the ones already on the schedule; callers leave those out.
export const getPlannedPlayoffGames = async (): Promise<
	PlannedPlayoffGame[]
> => {
	if (g.get("phase") !== PHASE.PLAYOFFS) {
		return [];
	}
	const playoffSeries = await idb.cache.playoffSeries.get(g.get("season"));
	if (!playoffSeries) {
		return [];
	}

	const gate = await getLiveGate();
	const gated = gate.games.size > 0;
	const series = gated
		? unplaySeries(playoffSeries.series, gate.games.values())
		: playoffSeries.series;
	let rnd = playoffSeries.currentRound;
	while (rnd > 0 && (series[rnd]?.length ?? 0) === 0) {
		rnd -= 1;
	}

	const out: PlannedPlayoffGame[] = [];
	if (rnd === -1) {
		// The play-in's last games are drawn up when its first ones end, so
		// none while one is still being played live.
		if (gated) {
			return [];
		}
		for (const playIn of playoffSeries.playIns ?? []) {
			for (const matchup of playIn) {
				const date = matchup.dates?.[0];
				if (date !== undefined && matchup.home.won + matchup.away.won === 0) {
					out.push({
						date,
						homeTid: matchup.home.tid,
						awayTid: matchup.away.tid,
						playIn: true,
						home: matchup.home,
						away: matchup.away,
					});
				}
			}
		}
		return out;
	}

	const numGames = g.get("numGamesPlayoffSeries", "current")[rnd];
	if (numGames === undefined) {
		return [];
	}
	const numGamesToWin = helpers.numGamesToWinRound(rnd);
	for (const [i, { home, away, dates }] of (series[rnd] ?? []).entries()) {
		if (!away || !dates || Math.max(home.won, away.won) >= numGamesToWin) {
			continue;
		}
		// Past every game played, the live one too.
		const stored = playoffSeries.series[rnd]?.[i];
		const played =
			stored?.away && stored.home.tid === home.tid
				? stored.home.won + stored.away.won
				: home.won + away.won;
		for (let k = played; k < dates.length; k++) {
			const homeFirst = betterSeedHome(numGames, k);
			out.push({
				date: dates[k]!,
				homeTid: homeFirst ? home.tid : away.tid,
				awayTid: homeFirst ? away.tid : home.tid,
				...(ifNecessary(k, home.won, away.won, numGamesToWin)
					? { ifNecessary: true as const }
					: {}),
				home,
				away,
			});
		}
	}
	return out;
};
