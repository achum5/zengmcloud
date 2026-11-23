import { PHASE } from "../../common/constants.ts";
import { isAllStarGame, isTradeDeadline } from "../../common/specialGames.ts";
import type { RouteParams } from "../../ui/router/types.ts";
import { season as seasonCore } from "../core/index.ts";
import { idb } from "../db/index.ts";
import { g, helpers } from "../util/index.ts";
import { defineView } from "../util/defineView.ts";
import { validateSeason, validateTeam } from "../util/processInputs.ts";
import { betterSeedHome } from "../../common/sportsbookOdds.ts";
import { ifNecessary } from "../core/season/playoffCalendar.ts";

// THE CALENDAR PAGE: a season's nights, month by month, for a team or the
// whole league. The same month calendar sits on Daily Schedule, Schedule and
// the dashboard, from getCalendarData.

const processInputs = (params: RouteParams<"calendar">) => ({
	season: validateSeason(params.season),
	tid:
		params.team === undefined
			? g.get("userTid")
			: validateTeam(params.team, ["all"]),
});

export type CalendarGame = {
	gid: number;
	date: string;
	// For the Daily Schedule link.
	day: number | undefined;
	homeTid: number;
	awayTid: number;
	playoffs: boolean;
	// A play-in game.
	playIn?: boolean;
	// A playoff game on the plan, not yet on the schedule (no gid of its own,
	// a negative one), and whether it may not be needed.
	planned?: boolean;
	ifNecessary?: boolean;
	// The final, once it's played.
	homePts?: number;
	awayPts?: number;
	overtimes?: number;
};

export type CalendarKeyDate = {
	date: string;
	day: number | undefined;
	type: "allStar" | "tradeDeadline";
	// The All-Star Game's box score, once it's played.
	gid?: number;
};

export type CalendarData = {
	games: CalendarGame[];
	keyDates: CalendarKeyDate[];
	// The next night to be played, in the current season.
	today: string | undefined;
	// Games on classic numbered days, before the league went on the
	// calendar.
	undated: boolean;
};

// A season's dated games and key dates: every game, or one team's.
export const getCalendarData = async (
	season: number,
	tid: number | "all",
): Promise<CalendarData> => {
	const games: CalendarGame[] = [];
	const keyDates: CalendarKeyDate[] = [];
	let undated = false;
	const forTeam = (homeTid: number, awayTid: number) =>
		tid === "all" || homeTid === tid || awayTid === tid;

	const playoffSeries = await idb.merged.playoffSeries.get(season);
	const playInGids = new Set(
		(playoffSeries?.playIns ?? []).flatMap((playIn) =>
			playIn.flatMap((matchup) => matchup.gids ?? []),
		),
	);

	const played = await idb.merged.games.getAll({ season });
	for (const game of played) {
		if (game.date === undefined) {
			undated = true;
			continue;
		}
		const [home, away] = game.teams;
		if (isAllStarGame(home.tid, away.tid)) {
			keyDates.push({
				date: game.date,
				day: game.day,
				type: "allStar",
				gid: game.gid,
			});
			continue;
		}
		if (!forTeam(home.tid, away.tid)) {
			continue;
		}
		games.push({
			gid: game.gid,
			date: game.date,
			day: game.day,
			homeTid: home.tid,
			awayTid: away.tid,
			playoffs: !!game.playoffs,
			...(playInGids.has(game.gid) ? { playIn: true } : {}),
			homePts: home.pts,
			awayPts: away.pts,
			overtimes: game.overtimes,
		});
	}

	let today: string | undefined;
	if (season === g.get("season")) {
		today = await seasonCore.getCalendarDate();
		const playoffs = g.get("phase") === PHASE.PLAYOFFS;
		for (const game of await seasonCore.getSchedule()) {
			if (game.date === undefined) {
				undated = true;
				continue;
			}
			if (isTradeDeadline(game.homeTid, game.awayTid)) {
				keyDates.push({
					date: game.date,
					day: game.day,
					type: "tradeDeadline",
				});
			} else if (isAllStarGame(game.homeTid, game.awayTid)) {
				keyDates.push({
					date: game.date,
					day: game.day,
					type: "allStar",
				});
			} else if (forTeam(game.homeTid, game.awayTid)) {
				games.push({
					gid: game.gid,
					date: game.date,
					day: game.day,
					homeTid: game.homeTid,
					awayTid: game.awayTid,
					playoffs,
					...(playoffs && playoffSeries?.currentRound === -1
						? { playIn: true }
						: {}),
				});
			}
		}

		// The rest of the round's games, on the nights they're planned for
		// (see playoffCalendar.ts).
		if (playoffs && playoffSeries) {
			const scheduled = new Set(
				games
					.filter((game) => game.playoffs)
					.map((game) => `${game.date}:${game.homeTid}:${game.awayTid}`),
			);
			let gid = -1;
			const plan = (
				homeTid: number,
				awayTid: number,
				date: string,
				extra: Partial<CalendarGame>,
			) => {
				if (
					!forTeam(homeTid, awayTid) ||
					scheduled.has(`${date}:${homeTid}:${awayTid}`)
				) {
					return;
				}
				games.push({
					gid: gid--,
					date,
					day: undefined,
					homeTid,
					awayTid,
					playoffs: true,
					planned: true,
					...extra,
				});
			};

			const rnd = playoffSeries.currentRound;
			if (rnd === -1) {
				for (const playIn of playoffSeries.playIns ?? []) {
					for (const matchup of playIn) {
						const date = matchup.dates?.[0];
						if (
							date !== undefined &&
							matchup.home.won + matchup.away.won === 0
						) {
							plan(matchup.home.tid, matchup.away.tid, date, { playIn: true });
						}
					}
				}
			} else {
				const numGames = g.get("numGamesPlayoffSeries", season)[rnd];
				const numGamesToWin = helpers.numGamesToWinRound(rnd);
				for (const { home, away, dates } of playoffSeries.series[rnd] ?? []) {
					if (
						!away ||
						!dates ||
						numGames === undefined ||
						Math.max(home.won, away.won) >= numGamesToWin
					) {
						continue;
					}
					for (let k = home.won + away.won; k < dates.length; k++) {
						const homeFirst = betterSeedHome(numGames, k);
						plan(
							homeFirst ? home.tid : away.tid,
							homeFirst ? away.tid : home.tid,
							dates[k]!,
							ifNecessary(k, home.won, away.won, numGamesToWin)
								? { ifNecessary: true }
								: {},
						);
					}
				}
			}
		}
	}

	games.sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));

	return { games, keyDates, today, undated };
};

// For pages that show the calendar only when the season has dates.
export const getCalendarIfDated = async (
	season: number,
	tid: number | "all",
) => {
	const data = await getCalendarData(season, tid);
	return data.games.length > 0 || data.keyDates.length > 0 ? data : undefined;
};

export default defineView({
	id: "calendar",
	processInputs,
	load: async ({ inputs, updateEvents, prevInputs }) => {
		if (
			!updateEvents.has("firstRun") &&
			!updateEvents.has("gameAttributes") &&
			!updateEvents.has("newPhase") &&
			!(inputs.season === g.get("season") && updateEvents.has("gameSim")) &&
			inputs.season === prevInputs?.season &&
			inputs.tid === prevInputs.tid
		) {
			return;
		}

		return {
			...(await getCalendarData(inputs.season, inputs.tid)),
			nbaCalendar: g.get("nbaCalendar"),
			season: inputs.season,
			tid: inputs.tid,
		};
	},
});
