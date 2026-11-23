import { PHASE } from "../../../common/constants.ts";
import { daysBetween } from "../../../common/calendarDates.ts";
import { isAllStarGame } from "../../../common/specialGames.ts";
import type { Game } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import addDaysToSchedule from "../season/addDaysToSchedule.ts";

// WHAT A NIGHT COUNTS FOR. The end-of-day rules (injuries healing, trade
// cooldowns, AI trades and signings, free agents' asking prices, tragic
// deaths) were written for classic days: BBGM packs a season into days where
// nearly every team plays, about 89 of them for 82 games, and every rule ticks
// once a day for everyone, off days included. On a calendar a sim day is one
// night, a team plays about every other one, and a season has about 165 -
// counted per night, all of those would run about twice as often.
//
// So on a calendar the rules run by the calendar's days, at the classic rate:
// a CLOCK turns calendar days into classic days. The regular season's clock
// spreads the season's classic days (classicDays, from BBGM's own schedule
// packed the classic way, plus one for the All-Star Game) evenly over the
// days from opening night to the last night - about one classic day every 1.9
// calendar days. The playoffs' runs at the classic postseason's rate (see
// PLAYOFF_CLASSIC_DAYS_PER_DAY). Each night, everyone ticks as many times as
// the clock passed since the night before - off nights, the All-Star break
// and the days before the play-in count like any other days. An injury is
// still a number of classic days (gamesRemaining) for everything that reads
// it; on a calendar it also carries the calendar days it has left
// (daysRemaining, see daysOut), which is what the game shows. Classic days
// and other sports are unchanged.

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
	// How many times players on each team tick (teams not in it: restTicks).
	// Undefined: everyone ticks once, as on a classic day.
	ticksByTid: Map<number, number> | undefined;
	// How many times players on teams not in ticksByTid tick (default none).
	restTicks?: number;
	// How many times the league-wide rules run.
	leagueRuns: number;
	// On a calendar: its clock and tonight, for the days injuries have left.
	clock?: CalendarClock;
	tonight?: string;
};

// Turns calendar days into classic days, for one stretch of the season.
export type CalendarClock = {
	// Classic days per calendar day.
	rate: number;
	// The stretch's calendar days through this date.
	at: (date: string) => number;
	// The regular season's: the playoffs that follow it, so an injury that
	// runs into them counts their days. After calendar day `at` (the last
	// night), with `ticks` classic days gone, it goes on at `rate`.
	then?: { at: number; ticks: number; rate: number };
};

const CLASSIC: DayTick = { ticksByTid: undefined, leagueRuns: 1 };

// Classic playoff days per calendar day. A classic postseason (the play-in's
// two days, then each round as long as its longest series) takes about 28
// days; the NBA's, from the last night of the regular season to the end of
// the Finals, about 71 (measured over whole simulated postseasons on the
// calendar's playoff plan, playoffCalendar.ts).
export const PLAYOFF_CLASSIC_DAYS_PER_DAY = 0.39;

// Without a stored classic count: the NBA's, about 89 classic days over the
// 170 from opening night to the last night.
export const DEFAULT_REGULAR_SEASON_RATE = 89 / 170;

// The league plays on the NBA calendar (see season/newCalendarSchedule.ts
// calendarOn, which this matches without importing the schedule maker).
export const nbaCalendarLeague = () =>
	__SPORT === "basketball" &&
	!g.get("college") &&
	!g.get("groupScheduleSeries") &&
	!!g.get("nbaCalendar");

const EPSILON = 1e-9;

type DatedRow = {
	homeTid: number;
	awayTid: number;
	date?: string;
};

type ClockGame = Pick<Game, "season" | "date" | "playoffs" | "day"> & {
	teams: readonly { tid: number }[];
};

// This stretch's clock: the regular season's (from its dates and classic
// days) or the playoffs' (from the regular season's last night). Undefined
// for a season without dates and in the offseason.
export const calendarClock = ({
	games,
	schedule,
	season,
	phase,
	classicDays,
}: {
	// This season's games so far.
	games: readonly ClockGame[];
	// The games still to play.
	schedule: readonly DatedRow[];
	season: number;
	phase: number;
	// The season's classic days (see countClassicDays), if known.
	classicDays: number | undefined;
}): CalendarClock | undefined => {
	if (phase === PHASE.PLAYOFFS) {
		let lastRegular: string | undefined;
		for (const game of games) {
			if (
				game.season === season &&
				!game.playoffs &&
				game.date !== undefined &&
				(lastRegular === undefined || game.date > lastRegular)
			) {
				lastRegular = game.date;
			}
		}
		if (lastRegular === undefined) {
			return;
		}
		const from = lastRegular;
		return {
			rate: PLAYOFF_CLASSIC_DAYS_PER_DAY,
			at: (date) => Math.max(0, daysBetween(from, date)),
		};
	}

	if (phase !== PHASE.REGULAR_SEASON && phase !== PHASE.AFTER_TRADE_DEADLINE) {
		return;
	}
	let first: string | undefined;
	let last: string | undefined;
	let allStar = false;
	const see = (date: string | undefined, homeTid: number, awayTid: number) => {
		if (date === undefined) {
			return;
		}
		if (first === undefined || date < first) {
			first = date;
		}
		if (last === undefined || date > last) {
			last = date;
		}
		if (isAllStarGame(homeTid, awayTid)) {
			allStar = true;
		}
	};
	for (const game of games) {
		if (game.season === season && !game.playoffs) {
			see(game.date, game.teams[0]!.tid, game.teams[1]!.tid);
		}
	}
	for (const row of schedule) {
		see(row.date, row.homeTid, row.awayTid);
	}
	if (first === undefined || last === undefined) {
		return;
	}
	const from = first;
	const span = daysBetween(from, last) + 1;
	const total =
		classicDays === undefined
			? span * DEFAULT_REGULAR_SEASON_RATE
			: classicDays + (allStar ? 1 : 0);
	return {
		rate: total / span,
		// Opening night is the first day.
		at: (date) => Math.max(0, daysBetween(from, date) + 1),
		then: {
			at: span,
			ticks: Math.floor(total + EPSILON),
			rate: PLAYOFF_CLASSIC_DAYS_PER_DAY,
		},
	};
};

// A stretch's clock before its first night (`at` 0 for any date).
export const clockBeforeFirstNight = (clock: CalendarClock): CalendarClock => ({
	...clock,
	at: () => 0,
});

// A regular season's clock before its first night when its dates aren't
// known: the NBA's.
export const DEFAULT_SEASON_CLOCK: CalendarClock = {
	rate: DEFAULT_REGULAR_SEASON_RATE,
	at: () => 0,
	then: { at: 170, ticks: 89, rate: PLAYOFF_CLASSIC_DAYS_PER_DAY },
};

// The playoffs' clock before their first night.
export const PLAYOFF_START_CLOCK: CalendarClock = {
	rate: PLAYOFF_CLASSIC_DAYS_PER_DAY,
	at: () => 0,
};

const ticksAt = (clock: CalendarClock, date: string | undefined) =>
	date === undefined ? 0 : Math.floor(clock.at(date) * clock.rate + EPSILON);

// How many classic days pass from the end of one night to the end of a later
// one (from undefined: the start of the stretch).
export const ticksBetween = (
	clock: CalendarClock,
	from: string | undefined,
	to: string,
) => Math.max(0, ticksAt(clock, to) - ticksAt(clock, from));

// The latest night before this one with games this season.
const nightBefore = (
	games: readonly ClockGame[],
	season: number,
	date: string,
) => {
	let before: string | undefined;
	for (const game of games) {
		if (
			game.season === season &&
			game.date !== undefined &&
			game.date < date &&
			(before === undefined || game.date > before)
		) {
			before = game.date;
		}
	}
	return before;
};

// What tonight counts for: on a calendar, the classic days the clock passed
// since the last night with games, for everyone. A classic day, once.
export const nightTick = ({
	games,
	schedule,
	season,
	day,
	phase,
	classicDays,
}: {
	// This season's games, including tonight's.
	games: readonly ClockGame[];
	// The games still to play this season (tonight's already off it).
	schedule: readonly DatedRow[];
	season: number;
	day: number | undefined;
	phase: number;
	// The season's classic days (see countClassicDays), if known.
	classicDays: number | undefined;
}): DayTick => {
	if (day === undefined) {
		return CLASSIC;
	}
	let tonight: string | undefined;
	for (const game of games) {
		if (game.season === season && game.day === day) {
			if (game.date === undefined) {
				return CLASSIC;
			}
			tonight = game.date;
		}
	}
	if (tonight === undefined) {
		return CLASSIC;
	}
	const clock = calendarClock({
		games,
		schedule,
		season,
		phase,
		classicDays,
	});
	if (!clock) {
		return CLASSIC;
	}
	const n = ticksBetween(clock, nightBefore(games, season, tonight), tonight);
	return {
		ticksByTid: new Map(),
		restTicks: n,
		leagueRuns: n,
		clock,
		tonight,
	};
};

// The calendar days a player with this many classic days left (after the
// night's countdown) is out: he's back once the clock has passed them all.
// The night's own progress toward its next tick counts, so the number goes
// down by one a day. In the regular season, days past its last night count
// at the playoffs' rate.
export const daysOut = (
	gamesRemaining: number,
	clock: CalendarClock,
	tonight: string,
) => {
	if (gamesRemaining <= 0) {
		return 0;
	}
	const now = clock.at(tonight);
	// The clock's classic days when he's back.
	const back = Math.floor(now * clock.rate + EPSILON) + gamesRemaining;
	const day =
		clock.then && back > clock.then.ticks + EPSILON
			? clock.then.at +
				Math.ceil((back - clock.then.ticks) / clock.then.rate - EPSILON)
			: Math.ceil(back / clock.rate - EPSILON);
	return Math.max(1, day - now);
};

// A player hurt tonight: the days he's out, as they'll read after tonight's
// countdown.
export const daysOutHurtTonight = ({
	gamesRemaining,
	clock,
	games,
	season,
	tonight,
}: {
	gamesRemaining: number;
	clock: CalendarClock;
	games: readonly ClockGame[];
	season: number;
	tonight: string;
}) =>
	Math.max(
		1,
		daysOut(
			gamesRemaining -
				ticksBetween(clock, nightBefore(games, season, tonight), tonight),
			clock,
			tonight,
		),
	);

// How many injury ticks come before a scheduled game, for healing a roster
// forward to it: on a calendar, the clock's classic days through the last
// night before it; on classic days, the days. `schedule` is the schedule
// still to play, in order; `now` is the stretch's clock and the last night
// played.
export const injuryTicksUntil = (
	schedule: {
		gid: number;
		day: number;
		homeTid: number;
		awayTid: number;
		date?: string;
	}[],
	now?: { clock: CalendarClock; lastNight: string | undefined },
) => {
	const todayDay = schedule[0]?.day ?? 0;
	const byDays = (day: number) => Math.max(0, day - todayDay);
	if (!now || !schedule.some((game) => game.date !== undefined)) {
		return (_tid: number, _gid: number | undefined, day: number) => byDays(day);
	}

	const nights = Array.from(
		new Set(
			schedule
				.map((game) => game.date)
				.filter((date): date is string => date !== undefined),
		),
	).sort();
	const ticksBefore = new Map<string, number>();
	for (const [i, night] of nights.entries()) {
		ticksBefore.set(
			night,
			i === 0 ? 0 : ticksBetween(now.clock, now.lastNight, nights[i - 1]!),
		);
	}
	const dateByGid = new Map(schedule.map((game) => [game.gid, game.date]));
	return (_tid: number, gid: number | undefined, day: number) => {
		const date = gid !== undefined ? dateByGid.get(gid) : undefined;
		return (
			(date !== undefined ? ticksBefore.get(date) : undefined) ?? byDays(day)
		);
	};
};
