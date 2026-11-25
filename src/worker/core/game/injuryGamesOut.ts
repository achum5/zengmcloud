import { PHASE } from "../../../common/constants.ts";
import { addDays } from "../../../common/calendarDates.ts";
import type {
	GamesOut,
	PlayoffSeries,
	ScheduleGame,
} from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { ifNecessary } from "../season/playoffCalendar.ts";
import type { CalendarClock } from "./calendarTicks.ts";
import { getInjuryClock } from "./classicDays.ts";

// THE GAMES AN INJURY TAKES IN, on the NBA calendar. The game shows an
// injury's length in calendar days (daysRemaining, refreshCalendarDays.ts),
// which say little on their own when a team plays every other night, some
// weeks four times and some twice. Beside them it shows the games on his
// team's own schedule those days take in: he is out through the day `days`
// after the day they count from, so every game of his team's on a date in
// between. In the regular season, from the schedule; past its last night
// the playoffs may add more ("12+"). In the playoffs, from the round's plan
// (playoffCalendar.ts): the games sure to be played, "+" when an "if
// necessary" game or the next round falls in his days. In the offseason,
// before next season's schedule is made, an estimate at the season's pace
// ("~19"). Nothing for a free agent, a team whose season is over, or a team
// waiting on its next round. Display only, like the days: nothing reads it
// but the pages.

// His team's games after the day the days count from.
type TeamGames = {
	// Sure to be played, by date.
	sure: readonly string[];
	// Might not be ("if necessary").
	maybe?: readonly string[];
	// Past this date, games not known yet may come (the playoffs, the next
	// round). Undefined: none can.
	openAfter: string | undefined;
};

export const countGamesOut = (
	from: string,
	days: number,
	{ sure, maybe = [], openAfter }: TeamGames,
): GamesOut | undefined => {
	const through = addDays(from, days);
	const within = (date: string) => date > from && date <= through;
	const n = sure.filter(within).length;
	const more =
		maybe.some(within) || (openAfter !== undefined && through > openAfter);
	if (n === 0 && more) {
		// Nothing known to say.
		return undefined;
	}
	return more ? { n, more: true } : { n };
};

// Before a season's schedule is made: a team's games in the first `days` of
// a regular season `span` days long.
export const estimateGamesOut = (
	days: number,
	numGames: number,
	span: number,
): GamesOut =>
	days >= span
		? { n: numGames, more: true }
		: { n: Math.round((days * numGames) / span), about: true };

export type GamesOutCounter = (
	tid: number,
	days: number | undefined,
) => GamesOut | undefined;

const NONE: GamesOutCounter = () => undefined;

// The regular season's games still to play after `from`, by team.
export const regularSeason = (
	schedule: readonly ScheduleGame[],
	from: string,
): GamesOutCounter => {
	const byTid = new Map<number, string[]>();
	let last: string | undefined;
	for (const { homeTid, awayTid, date } of schedule) {
		if (date === undefined || date <= from || homeTid < 0 || awayTid < 0) {
			continue;
		}
		for (const tid of [homeTid, awayTid]) {
			let dates = byTid.get(tid);
			if (!dates) {
				dates = [];
				byTid.set(tid, dates);
			}
			dates.push(date);
		}
		if (last === undefined || date > last) {
			last = date;
		}
	}
	// Past the last night, the playoffs.
	const openAfter = last ?? from;
	return (tid, days) =>
		tid < 0 || days === undefined || days <= 0
			? undefined
			: countGamesOut(from, days, {
					sure: byTid.get(tid) ?? [],
					openAfter,
				});
};

// The playoffs' games on the current round's plan, by team: the play-in's
// games, or each series' still to play.
export const playoffGamesByTid = (
	playoffSeries: Pick<PlayoffSeries, "currentRound" | "series" | "playIns">,
	numGamesToWinRound: (rnd: number) => number,
) => {
	const byTid = new Map<number, TeamGames>();
	const { series } = playoffSeries;
	let rnd = playoffSeries.currentRound;
	while (rnd > 0 && (series[rnd]?.length ?? 0) === 0) {
		rnd -= 1;
	}
	if (rnd === -1) {
		// The play-in: a game each, and after it maybe another, or round 1.
		for (const playIn of playoffSeries.playIns ?? []) {
			for (const { home, away, dates } of playIn) {
				const date = dates?.[0];
				if (date !== undefined && home.won + away.won === 0) {
					for (const tid of [home.tid, away.tid]) {
						byTid.set(tid, { sure: [date], openAfter: date });
					}
				}
			}
		}
		return byTid;
	}
	const numGamesToWin = numGamesToWinRound(rnd);
	for (const { home, away, dates } of series[rnd] ?? []) {
		if (!away || Math.max(home.won, away.won) >= numGamesToWin || !dates) {
			// A bye, a series over (the winner waits on the next round), or one
			// not on the plan: nothing known to count.
			continue;
		}
		const sure: string[] = [];
		const maybe: string[] = [];
		for (let k = home.won + away.won; k < dates.length; k++) {
			(ifNecessary(k, home.won, away.won, numGamesToWin) ? maybe : sure).push(
				dates[k]!,
			);
		}
		// After the series' last night, the next round, if they win it.
		const games = { sure, maybe, openAfter: dates.at(-1) };
		byTid.set(home.tid, games);
		byTid.set(away.tid, games);
	}
	return byTid;
};

const playoffs = async (from: string): Promise<GamesOutCounter> => {
	const playoffSeries = await idb.cache.playoffSeries.get(g.get("season"));
	if (!playoffSeries) {
		return NONE;
	}
	const byTid = playoffGamesByTid(playoffSeries, (rnd) =>
		helpers.numGamesToWinRound(rnd),
	);
	return (tid, days) => {
		const games = byTid.get(tid);
		return games === undefined || days === undefined || days <= 0
			? undefined
			: countGamesOut(from, days, games);
	};
};

// Counts games out for days counted from `from` (the day the days left count
// from, see refreshCalendarDays.ts; "" before a stretch's first night) on
// this clock. Nothing off the calendar.
export const loadGamesOut = async (
	from: string,
	clock: CalendarClock | undefined,
): Promise<GamesOutCounter> => {
	if (!clock) {
		return NONE;
	}
	const phase = g.get("phase");
	const schedule = await idb.cache.schedule.getAll();

	if (from === "") {
		// Before the regular season's first night: from the day before it, or
		// with no schedule yet (the offseason), an estimate.
		let first: string | undefined;
		for (const { homeTid, awayTid, date } of schedule) {
			if (
				date !== undefined &&
				homeTid >= 0 &&
				awayTid >= 0 &&
				(first === undefined || date < first)
			) {
				first = date;
			}
		}
		if (first !== undefined) {
			return regularSeason(schedule, addDays(first, -1));
		}
		const span = clock.then?.at;
		if (span === undefined || phase === PHASE.PLAYOFFS) {
			return NONE;
		}
		const numGames = g.get("numGames");
		return (tid, days) =>
			tid < 0 || days === undefined || days <= 0
				? undefined
				: estimateGamesOut(days, numGames, span);
	}

	// The playoffs: once they've started (their first night is drawn up while
	// the phase is still the regular season's).
	if (
		phase === PHASE.PLAYOFFS ||
		(phase >= PHASE.REGULAR_SEASON &&
			phase < PHASE.PLAYOFFS &&
			(await idb.cache.playoffSeries.get(g.get("season"))) !== undefined)
	) {
		return playoffs(from);
	}
	if (phase >= PHASE.REGULAR_SEASON && phase < PHASE.PLAYOFFS) {
		return regularSeason(schedule, from);
	}
	return NONE;
};

// A player hurt in tonight's game (on the calendar, `days` out from tonight):
// the games those days take in.
export const gamesOutForNewInjury = async (
	tid: number,
	days: number | undefined,
	tonight: string | undefined,
) => {
	if (days === undefined || tonight === undefined) {
		return;
	}
	const now = await getInjuryClock();
	if (!now) {
		return;
	}
	return (await loadGamesOut(tonight, now.clock))(tid, days);
};
