import setSchedule from "./setSchedule.ts";
import { PHASE } from "../../../common/constants.ts";
import { idb } from "../../db/index.ts";
import { g, helpers, local, sim } from "../../util/index.ts";
import type { PlayoffSeriesTeam } from "../../../common/types.ts";
import { season, team } from "../index.ts";
// Play 2 home (true) then 2 away (false) and repeat, but ensure that the better
// team always gets the last game. betterSeedHome lives in
// common/sportsbookOdds.ts so the sportsbook prices playoff series with the
// exact same home pattern this file schedules.
import { betterSeedHome } from "../../../common/sportsbookOdds.ts";
import { groupByUnique } from "../../../common/utils.ts";
import {
	ORDER_TEAMS_SEASON_ATTRS,
	ORDER_TEAMS_STATS,
	orderTeams,
} from "../../util/orderTeams.ts";
import { ALL_STAR_GAME_TIDS } from "../../../common/specialGames.ts";
import { collegeNitDay } from "../college/tournaments.ts";
import { nextPlayoffDate } from "./playoffDates.ts";
import {
	planPlayIn,
	planRound,
	playInFinalNight,
	playoffsAllStarNight,
	roundKind,
	roundStartNight,
} from "./playoffCalendar.ts";
import { addDays } from "../../../common/calendarDates.ts";
import { getNextRoundMatchups, getPlayInFinalGame } from "./playoffBracket.ts";
import { refreshCalendarDaysNow } from "../game/refreshCalendarDays.ts";

const seriesIsNotOver = (
	home: PlayoffSeriesTeam,
	away: PlayoffSeriesTeam | undefined,
	numGamesToWin: number,
): away is PlayoffSeriesTeam =>
	!!(away && home.won < numGamesToWin && away.won < numGamesToWin);

// On the NBA calendar: the night of this season's last game so far, and each
// team's. Undefined on classic numbered days.
const playedNights = async () => {
	const season = g.get("season");
	let last: string | undefined;
	const byTid = new Map<number, string>();
	for (const game of await idb.cache.games.getAll()) {
		if (game.season !== season || game.date === undefined) {
			continue;
		}
		if (last === undefined || game.date > last) {
			last = game.date;
		}
		for (const { tid } of game.teams) {
			const prev = byTid.get(tid);
			if (prev === undefined || game.date > prev) {
				byTid.set(tid, game.date);
			}
		}
	}
	return { last, byTid };
};

// The games on the first of these nights.
const firstNight = (tids: [number, number][], dates: string[]) => {
	const night = [...dates].sort()[0];
	const tonight = tids.filter((_, i) => dates[i] === night);
	return { tids: tonight, dates: tonight.map(() => night) };
};

const getTeamsForOrderTeams = () => {
	return team.getFormattedAll({
		attrs: ["tid"],
		seasonAttrs: ORDER_TEAMS_SEASON_ATTRS,
		stats: ORDER_TEAMS_STATS,
		season: g.get("season"),
		showNoStats: true,
	});
};

/**
 * Create a single day's schedule for an in-progress playoffs.
 *
 * @memberOf core.season
 * @return {Promise.boolean} Resolves to true if the playoffs are over. Otherwise, false.
 */
const newSchedulePlayoffsDay = async (): Promise<boolean> => {
	const over = await scheduleNextNight();
	// On the NBA calendar: the days injuries have left count from the day
	// before the night just drawn up, and their games come from the round's
	// plan (refreshCalendarDays.ts). (While the playoffs are starting, the
	// phase change does it, newPhase/finalize.ts.)
	if (g.get("phase") === PHASE.PLAYOFFS) {
		await refreshCalendarDaysNow();
	}
	return over;
};

const scheduleNextNight = async (): Promise<boolean> => {
	const playoffSeries = await idb.cache.playoffSeries.get(g.get("season"));
	if (!playoffSeries) {
		throw new Error("No playoff series");
	}

	const playIns = playoffSeries.playIns;
	if (playoffSeries.currentRound === -1 && playIns && playIns.length > 0) {
		// On the calendar, the play-in's nights are set before its first game
		// (see playoffCalendar.ts). One that started without them (from before
		// they were) keeps the old every-other-night dates.
		if (
			playIns.every((playIn) =>
				playIn.every(
					(matchup) =>
						matchup.dates === undefined &&
						matchup.home.won + matchup.away.won === 0,
				),
			)
		) {
			const { last } = await playedNights();
			if (last !== undefined) {
				const firstNights = planPlayIn(playIns.length, last);
				for (const [i, playIn] of playIns.entries()) {
					for (const matchup of playIn) {
						matchup.dates = [firstNights[i]!];
					}
				}
			}
		}
		const finalNight = playIns.every((playIn) => playIn[0].dates)
			? playInFinalNight(playIns.map((playIn) => playIn[0].dates![0]!))
			: undefined;

		const tids: [number, number][] = [];
		const dates: string[] = [];

		// Is play-in tournament still going on?
		for (const playIn of playIns) {
			// If length is 3, that means the final game in the play-in was already scheduled, so no need to do any more
			const needsSecondRound =
				playIn.length === 2 &&
				playIn[0].home.won + playIn[0].away.won === 1 &&
				playIn[1].home.won + playIn[1].away.won === 1;
			if (needsSecondRound) {
				const matchup = {
					...getPlayInFinalGame(playIn),
					...(finalNight === undefined ? {} : { dates: [finalNight] }),
				};
				playIn.push(matchup);

				tids.push([matchup.home.tid, matchup.away.tid]);
				if (finalNight !== undefined) {
					dates.push(finalNight);
				}
			} else {
				for (const matchup of playIn) {
					if (matchup.home.won === 0 && matchup.away.won === 0) {
						tids.push([matchup.home.tid, matchup.away.tid]);
						const night = matchup.dates?.[0];
						if (night !== undefined) {
							dates.push(night);
						}
					}
				}
			}
		}

		if (tids.length > 0) {
			await idb.cache.playoffSeries.put(playoffSeries);
			if (finalNight !== undefined && dates.length === tids.length) {
				// One night at a time.
				const tonight = firstNight(tids, dates);
				await setSchedule(tonight.tids, tonight.dates);
				return false;
			}
			const date = await nextPlayoffDate({
				kind: playIns.some((playIn) =>
					playIn.some((matchup) => matchup.home.won + matchup.away.won > 0),
				)
					? "playInFinal"
					: "playInStart",
			});
			await setSchedule(
				tids,
				tids.map(() => date),
			);
			return false;
		} else {
			// playIn is over, so proceed to next round
			playoffSeries.currentRound = 0;
		}
	}

	const series = playoffSeries.series;

	if (series.length === 0) {
		return true;
	}

	const rnd = playoffSeries.currentRound;
	const tids: [number, number][] = [];
	const numGamesToWin = helpers.numGamesToWinRound(rnd);
	const numRounds = g.get("numGamesPlayoffSeries", "current").length;
	const finals = rnd === numRounds - 1;

	// On the calendar, every game of the round gets its night before Game 1
	// (see playoffCalendar.ts). A round that started without them (from before
	// they were) keeps the old every-other-night dates.
	const roundSeries = series[rnd]!;
	if (
		roundSeries.every(
			(matchup) =>
				matchup.dates === undefined &&
				(!matchup.away || matchup.home.won + matchup.away.won === 0),
		)
	) {
		const nights = await playedNights();
		if (nights.last !== undefined) {
			const active = roundSeries.filter((matchup) => matchup.away);
			const planned = planRound({
				kind: roundKind(rnd, numRounds),
				firstNight: roundStartNight({
					rnd,
					kind: roundKind(rnd, numRounds),
					lastNight: nights.last,
					afterPlayIn: !!playIns && playIns.length > 0,
					random: Math.random,
				}),
				series: active.map((matchup) => ({
					tids: [matchup.home.tid, matchup.away!.tid],
					numGames: g.get("numGamesPlayoffSeries", "current")[rnd]!,
				})),
				lastPlayed: nights.byTid,
			});
			for (const [i, matchup] of active.entries()) {
				matchup.dates = planned[i];
			}
			await idb.cache.playoffSeries.put(playoffSeries);
		}
	}

	// Tonight on the calendar: the first night any series has a game left.
	let tonight: string | undefined;
	let calendarRound = roundSeries.some((matchup) => matchup.dates);
	for (const { away, home, dates } of roundSeries) {
		if (seriesIsNotOver(home, away, numGamesToWin)) {
			const night = dates?.[home.won + away.won];
			if (night === undefined) {
				// Not on the plan (the series got longer?): the old dates.
				calendarRound = false;
			} else if (tonight === undefined || night < tonight) {
				tonight = night;
			}
		}
	}

	let minGamesPlayedThisRound = Infinity;
	for (const { away, home } of series[rnd]!) {
		if (seriesIsNotOver(home, away, numGamesToWin)) {
			const numGames = home.won + away.won;
			if (numGames < minGamesPlayedThisRound) {
				minGamesPlayedThisRound = numGames;
			}
		}
	}

	// Try to schedule games if there are active series
	for (const { away, home, dates } of series[rnd]!) {
		if (seriesIsNotOver(home, away, numGamesToWin)) {
			const numGames = home.won + away.won;

			if (calendarRound) {
				if (dates?.[numGames] !== tonight) {
					continue;
				}
			} else if (numGames > minGamesPlayedThisRound) {
				// Because live game sim is an individual game now, not a whole day, need to check if some series are ahead of others and therefore should not get a game today.
				continue;
			}

			// Make sure to set home/away teams correctly! Home for the lower seed is 1st, 2nd, 5th, and 7th games.
			if (
				betterSeedHome(
					g.get("numGamesPlayoffSeries", "current")[rnd]!,
					numGames,
				)
			) {
				tids.push([home.tid, away.tid]);
			} else {
				tids.push([away.tid, home.tid]);
			}
		}
	}

	// If series are still in progress, write games and short circuit
	if (tids.length > 0) {
		// Check if we need a playoffs All-Star Game
		if (
			g.get("allStarGame") === -1 &&
			series[rnd]!.length === 1 &&
			series[rnd]![0]!.home.won === 0 &&
			series[rnd]![0]!.away?.won === 0
		) {
			// Make sure we didn't just play the All-Star Game - only schedule once
			const allStar = await idb.merged.allStars.get(g.get("season"));
			if (!allStar?.score) {
				if (calendarRound && tonight !== undefined) {
					// On the calendar, a night of its own before Game 1.
					const { last } = await playedNights();
					let night = playoffsAllStarNight(tonight);
					if (last !== undefined && night <= last) {
						night = addDays(last, 1);
					}
					await setSchedule([[...ALL_STAR_GAME_TIDS]], [night]);
					return false;
				}
				tids.unshift([...ALL_STAR_GAME_TIDS]);
			}
		}

		if (g.get("college")) {
			tids.push(...(await collegeNitDay(true)));
		}
		if (calendarRound && tonight !== undefined) {
			await setSchedule(
				tids,
				tids.map(() => tonight),
			);
			return false;
		}
		const date = await nextPlayoffDate(
			minGamesPlayedThisRound === 0
				? {
						kind: "roundStart",
						round: rnd,
						finals,
						afterPlayIn: !!playIns && playIns.length > 0,
					}
				: { kind: "game", finals, gameIndex: minGamesPlayedThisRound },
		);
		await setSchedule(
			tids,
			tids.map(() => date),
		);
		return false;
	}

	// If playoffs are over, update winner and go to next phase
	if (rnd === g.get("numGamesPlayoffSeries", "current").length - 1) {
		const { away, home } = series[rnd]![0]!;
		let key;

		if (home.won >= numGamesToWin || !away) {
			key = home.tid;
		} else {
			key = away.tid;
		}

		const teamSeason = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[g.get("season"), key],
		);
		if (!teamSeason) {
			throw new Error("No team season");
		}
		teamSeason.playoffRoundsWon = g.get(
			"numGamesPlayoffSeries",
			"current",
		).length;
		teamSeason.hype += 0.05;

		if (teamSeason.hype > 1) {
			teamSeason.hype = 1;
		}

		await idb.cache.teamSeasons.put(teamSeason);

		// Playoffs are over! Return true!
		return true;
	}

	// Playoffs are not over! Make another round
	// Handle "Play until end of round"
	if (local.playingUntilEndOfRound) {
		sim.requestStop();
		local.playingUntilEndOfRound = false;
	}

	// Set matchups for next round

	// Which teams won?
	const teamsWon: PlayoffSeriesTeam[] = [];
	for (const { home, away } of series[rnd]!) {
		let teamWon;
		if (home.won >= numGamesToWin || !away) {
			teamWon = helpers.deepCopy(home);
		} else {
			teamWon = helpers.deepCopy(away);
		}
		teamsWon.push(teamWon);

		// In hockey, if team won by more than 1 game, goalie fatigue resets
		if (__SPORT === "hockey") {
			if (!away || Math.abs(home.won - away.won) > 1) {
				const players = await idb.cache.players.indexGetAll(
					"playersByTid",
					teamWon.tid,
				);
				for (const p of players) {
					if (
						p.numConsecutiveGamesG !== undefined &&
						p.numConsecutiveGamesG !== 0
					) {
						p.numConsecutiveGamesG = 0;
						await idb.cache.players.put(p);
					}
				}
			}
		}
	}

	// Can't just look in playoffSeries.byConf because of upgraded leagues and real players leagues started in the playoffs
	const playoffsByConf = await season.getPlayoffsByConf(g.get("season"));

	let teamsForOrderTeams: Awaited<ReturnType<typeof getTeamsForOrderTeams>>;
	const orderAcrossConfs = async (teams: PlayoffSeriesTeam[]) => {
		teamsForOrderTeams ??= await getTeamsForOrderTeams();
		const tids = new Set(teams.map((t) => t.tid));
		const orderedTeams = await orderTeams(
			teamsForOrderTeams.filter((t) => tids.has(t.tid)),
			teamsForOrderTeams,
			{
				skipDivisionLeaders: true,
			},
		);

		// Should never happen, but just in case a team is missing, seeds are better than nothing
		if (orderedTeams.length !== teams.length) {
			return teams.toSorted((a, b) => a.seed - b.seed);
		}

		const teamsByTid = groupByUnique(teams, "tid");
		return orderedTeams.map((t) => teamsByTid[t.tid]!);
	};

	const matchups = await getNextRoundMatchups({
		orderAcrossConfs,
		playoffsByConf,
		reseed: g.get("playoffsReseed"),
		teamsWon,
	});
	for (const [i, matchup] of matchups.entries()) {
		matchup.home.pts = undefined;
		matchup.away.pts = undefined;
		matchup.home.sPts = undefined;
		matchup.away.sPts = undefined;
		matchup.home.won = 0;
		matchup.away.won = 0;
		series[rnd + 1]![i] = matchup;
	}

	// Previously was just `playoffSeries.currentRound += 1` but some people reported that playoffSeries.currentRound was somehow getting too high, resulting in errors in other parts of the code. Not sure how that happened, but maybe this fixes it? I don't see anywhere else where playoffSeries.currentRound is updated.
	// This works because any rounds that are not yet current will have no matchups in them
	playoffSeries.currentRound =
		playoffSeries.series.filter((row) => row.length > 0).length - 1;
	await idb.cache.playoffSeries.put(playoffSeries);

	// Update hype for winning a series
	const teamSeasonsToSave = [];
	for (const { tid } of teamsWon) {
		const teamSeason = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[g.get("season"), tid],
		);
		if (!teamSeason) {
			throw new Error("No team season");
		}
		teamSeason.playoffRoundsWon = playoffSeries.currentRound;
		teamSeason.hype += 0.05;

		if (teamSeason.hype > 1) {
			teamSeason.hype = 1;
		}

		teamSeasonsToSave.push(teamSeason);
	}
	await idb.cache.teamSeasons.putAll(teamSeasonsToSave);

	// Next time, the schedule for the first day of the next round will be set
	return scheduleNextNight();
};

export default newSchedulePlayoffsDay;
