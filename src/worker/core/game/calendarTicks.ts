import { PHASE } from "../../../common/constants.ts";
import { isAllStarGame, isAllStarTid } from "../../../common/specialGames.ts";
import type { Game } from "../../../common/types.ts";

// WHAT A NIGHT COUNTS FOR. The end-of-day rules (injuries healing, trade
// cooldowns, AI trades and signings, free agents' asking prices, tragic
// deaths) were written for classic days, when nearly every team plays every
// day and BBGM fits a season into about 87 of them. On a calendar a sim day
// is one night, a team plays about every other one, and a season has about
// 165 - counted per day, all of those would run about twice as often per game.
//
// So on a calendar night they count games instead:
// - injuries and trade cooldowns tick for the players whose team played
//   that night ("out 3 games" is three of his team's games);
// - the league-wide rules run once per round of games, a round being as many
//   games as half the league (so over a season, about once per game each team
//   plays, as on classic days).
// A night with no team games (the All-Star Game) counts as one classic day.
// Classic days, the playoffs and other sports are unchanged.

export type DayTick = {
	// Players on these teams tick. Undefined: everyone ticks, as on a classic
	// day.
	tidsPlayed: Set<number> | undefined;
	// How many times the league-wide rules run.
	leagueRuns: number;
};

const CLASSIC: DayTick = { tidsPlayed: undefined, leagueRuns: 1 };

const isRegularTeamGame = (game: Game) =>
	!game.playoffs &&
	!isAllStarTid(game.teams[0].tid) &&
	!isAllStarTid(game.teams[1].tid);

export const dayTickFromGames = ({
	games,
	season,
	day,
	phase,
	numActiveTeams,
}: {
	// This season's games, including tonight's.
	games: Game[];
	season: number;
	day: number | undefined;
	phase: number;
	numActiveTeams: number;
}): DayTick => {
	if (
		day === undefined ||
		(phase !== PHASE.REGULAR_SEASON && phase !== PHASE.AFTER_TRADE_DEADLINE)
	) {
		return CLASSIC;
	}

	let tonight = 0;
	let calendar = false;
	let played = 0;
	const tidsPlayed = new Set<number>();
	for (const game of games) {
		if (game.season !== season || !isRegularTeamGame(game)) {
			continue;
		}
		played += 1;
		if (game.day === day) {
			tonight += 1;
			tidsPlayed.add(game.teams[0].tid);
			tidsPlayed.add(game.teams[1].tid);
			if (game.date !== undefined) {
				calendar = true;
			}
		}
	}

	if (!calendar || tonight === 0) {
		return CLASSIC;
	}

	const round = Math.max(1, numActiveTeams / 2);
	const before = played - tonight;
	return {
		tidsPlayed,
		leagueRuns: Math.floor(played / round) - Math.floor(before / round),
	};
};

// How many injury ticks come before a scheduled game, for healing a roster
// forward to it: on a calendar, the team's own games before it (plus one for
// the All-Star night, which ticks everyone); on classic days, the days.
// `schedule` is the schedule still to play, in order.
export const injuryTicksUntil = (
	schedule: {
		gid: number;
		day: number;
		homeTid: number;
		awayTid: number;
		date?: string;
	}[],
) => {
	const todayDay = schedule[0]?.day ?? 0;
	const byDays = (day: number) => Math.max(0, day - todayDay);
	if (!schedule.some((game) => game.date !== undefined && game.homeTid >= 0)) {
		return (_tid: number, _gid: number | undefined, day: number) => byDays(day);
	}

	const ticks = new Map<string, number>();
	const teamGames = new Map<number, number>();
	let everyone = 0;
	for (const game of schedule) {
		if (isAllStarGame(game.homeTid, game.awayTid)) {
			everyone += 1;
			continue;
		}
		if (game.homeTid < 0 || game.awayTid < 0) {
			// The trade deadline: no night of its own.
			continue;
		}
		for (const tid of [game.homeTid, game.awayTid]) {
			const n = teamGames.get(tid) ?? 0;
			ticks.set(`${game.gid}:${tid}`, n + everyone);
			teamGames.set(tid, n + 1);
		}
	}
	return (tid: number, gid: number | undefined, day: number) =>
		(gid !== undefined ? ticks.get(`${gid}:${tid}`) : undefined) ?? byDays(day);
};
