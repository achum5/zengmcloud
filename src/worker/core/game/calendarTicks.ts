import { PHASE } from "../../../common/constants.ts";
import { isAllStarGame, isAllStarTid } from "../../../common/specialGames.ts";
import type { Game } from "../../../common/types.ts";
import addDaysToSchedule from "../season/addDaysToSchedule.ts";

// WHAT A NIGHT COUNTS FOR. The end-of-day rules (injuries healing, trade
// cooldowns, AI trades and signings, free agents' asking prices, tragic
// deaths) were written for classic days: BBGM packs a season into days where
// nearly every team plays, about 89 of them for 82 games (each team has 6-9
// days off), and every rule ticks once a day for everyone. On a calendar a
// sim day is one night, a team plays about every other one, and a season has
// about 165 - counted per day, all of those would run about twice as often.
//
// So a calendar season keeps the classic count. Each season stores how many
// classic days its league would have played in (classicDays, from BBGM's own
// schedule packed the classic way), and those days are spread evenly over the
// games:
// - injuries and trade cooldowns tick for the players whose team played that
//   night, a team's season of games carrying the season's classic days (so a
//   tick of 2 about every 12th game at 89 days for 82 games);
// - the league-wide rules run as the league's games pass the same pace.
// A night with no team games (the All-Star Game) counts as one classic day,
// as the All-Star day does. Classic days, the playoffs and other sports are
// unchanged.

// How many classic days the season's schedule (BBGM's matchups in its own
// order, with the All-Star Game and trade deadline) would take: the days with
// games, packed the way addDaysToSchedule packs them.
export const countClassicDays = (tids: [number, number][]) => {
	const days = new Set<number>();
	for (const row of addDaysToSchedule(
		tids.map(([homeTid, awayTid]) => ({ homeTid, awayTid })),
	)) {
		if (row.homeTid >= 0 && row.awayTid >= 0) {
			days.add(row.day);
		}
	}
	return days.size;
};

// Ticks through a team's (or the league's) kth game of `games`, at the
// classic pace: `classicDays` spread evenly over them. Without a classic
// count, one a game.
export const ticksThrough = (
	k: number,
	games: number,
	classicDays: number | undefined,
) =>
	classicDays === undefined || games <= 0
		? k
		: Math.floor((k * classicDays) / games);

export type DayTick = {
	// How many times players on each team tick (teams not in it: none).
	// Undefined: everyone ticks once, as on a classic day.
	ticksByTid: Map<number, number> | undefined;
	// How many times the league-wide rules run.
	leagueRuns: number;
};

const CLASSIC: DayTick = { ticksByTid: undefined, leagueRuns: 1 };

const isRegularTeamGame = (game: Game) =>
	!game.playoffs &&
	!isAllStarTid(game.teams[0].tid) &&
	!isAllStarTid(game.teams[1].tid);

export const dayTickFromGames = ({
	games,
	schedule,
	season,
	day,
	phase,
	numActiveTeams,
	classicDays,
}: {
	// This season's games, including tonight's.
	games: Game[];
	// The games still to play this season (tonight's already off it).
	schedule: { homeTid: number; awayTid: number }[];
	season: number;
	day: number | undefined;
	phase: number;
	numActiveTeams: number;
	// The season's classic days (see countClassicDays), if known.
	classicDays: number | undefined;
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
	const playedBy = new Map<number, number>();
	for (const game of games) {
		if (game.season !== season || !isRegularTeamGame(game)) {
			continue;
		}
		played += 1;
		for (const t of game.teams) {
			playedBy.set(t.tid, (playedBy.get(t.tid) ?? 0) + 1);
		}
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

	let remaining = 0;
	const remainingBy = new Map<number, number>();
	for (const game of schedule) {
		if (game.homeTid < 0 || game.awayTid < 0) {
			continue;
		}
		remaining += 1;
		for (const tid of [game.homeTid, game.awayTid]) {
			remainingBy.set(tid, (remainingBy.get(tid) ?? 0) + 1);
		}
	}

	const ticksByTid = new Map<number, number>();
	for (const tid of tidsPlayed) {
		const k = playedBy.get(tid)!;
		const n = k + (remainingBy.get(tid) ?? 0);
		ticksByTid.set(
			tid,
			ticksThrough(k, n, classicDays) - ticksThrough(k - 1, n, classicDays),
		);
	}

	const before = played - tonight;
	let leagueRuns;
	if (classicDays === undefined) {
		// A round of games, as many as half the league.
		const round = Math.max(1, numActiveTeams / 2);
		leagueRuns = Math.floor(played / round) - Math.floor(before / round);
	} else {
		const total = played + remaining;
		leagueRuns =
			ticksThrough(played, total, classicDays) -
			ticksThrough(before, total, classicDays);
	}

	return { ticksByTid, leagueRuns };
};

// How many injury ticks come before a scheduled game, for healing a roster
// forward to it: on a calendar, the team's own games before it at the classic
// pace (plus one for the All-Star night, which ticks everyone); on classic
// days, the days. `schedule` is the schedule still to play, in order; `pace`
// is the season's classic days and each team's games played so far.
export const injuryTicksUntil = (
	schedule: {
		gid: number;
		day: number;
		homeTid: number;
		awayTid: number;
		date?: string;
	}[],
	pace?: { classicDays: number; played: Map<number, number> },
) => {
	const todayDay = schedule[0]?.day ?? 0;
	const byDays = (day: number) => Math.max(0, day - todayDay);
	if (!schedule.some((game) => game.date !== undefined && game.homeTid >= 0)) {
		return (_tid: number, _gid: number | undefined, day: number) => byDays(day);
	}

	const remainingBy = new Map<number, number>();
	for (const game of schedule) {
		if (game.homeTid >= 0 && game.awayTid >= 0) {
			for (const tid of [game.homeTid, game.awayTid]) {
				remainingBy.set(tid, (remainingBy.get(tid) ?? 0) + 1);
			}
		}
	}
	const ticksBefore = (tid: number, n: number) => {
		if (!pace) {
			return n;
		}
		const k0 = pace.played.get(tid) ?? 0;
		const total = k0 + (remainingBy.get(tid) ?? 0);
		return (
			ticksThrough(k0 + n, total, pace.classicDays) -
			ticksThrough(k0, total, pace.classicDays)
		);
	};

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
			ticks.set(`${game.gid}:${tid}`, ticksBefore(tid, n) + everyone);
			teamGames.set(tid, n + 1);
		}
	}
	return (tid: number, gid: number | undefined, day: number) =>
		(gid !== undefined ? ticks.get(`${gid}:${tid}`) : undefined) ?? byDays(day);
};
