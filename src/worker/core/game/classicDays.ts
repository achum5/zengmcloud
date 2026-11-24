import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import {
	type CalendarClock,
	calendarClock,
	clockBeforeFirstNight,
	daysOut,
	daysOutHurtTonight,
	DEFAULT_SEASON_CLOCK,
	nbaCalendarLeague,
	PLAYOFF_START_CLOCK,
} from "./calendarTicks.ts";

// THE CLASSIC DAY COUNT of this season, for a calendar season to tick at the
// classic pace (calendarTicks.ts). Stored with the league when the season's
// schedule is made (saveClassicDays.ts). Kept apart from the saving, which
// needs the schedule maker, so views can read it without importing that.

// This season's, if stored.
export const storedClassicDays = () => {
	const stored = g.get("calendarClassicDays");
	return stored?.season === g.get("season") ? stored.days : undefined;
};

// The date of this season's last day whose end of day was counted: a night
// with games, or a day without (season/offDays.ts).
export const countedThrough = () => {
	const last = g.get("lastDailyCountdownDay");
	return last?.season === g.get("season") ? last.date : undefined;
};

// This stretch's clock (calendarTicks.ts) and the last night played (or a
// later day with no games, once counted), for projecting injuries forward
// and for the days they have left. Undefined off the calendar.
export const getInjuryClock = async (phase: number = g.get("phase")) => {
	const season = g.get("season");
	const games = await idb.cache.games.getAll();
	const clock = calendarClock({
		games,
		schedule: await idb.cache.schedule.getAll(),
		season,
		phase,
		classicDays: storedClassicDays(),
	});
	if (!clock) {
		return;
	}
	let lastNight: string | undefined;
	for (const game of games) {
		if (
			game.season === season &&
			game.date !== undefined &&
			(lastNight === undefined || game.date > lastNight)
		) {
			lastNight = game.date;
		}
	}
	const counted = countedThrough();
	if (
		counted !== undefined &&
		(lastNight === undefined || counted > lastNight)
	) {
		lastNight = counted;
	}
	return { clock, lastNight, games, season };
};

// For a player hurt in tonight's game on the calendar: the days he's out.
export const daysOutForNewInjury = async (
	gamesRemaining: number,
	tonight: string | undefined,
) => {
	if (tonight === undefined) {
		return;
	}
	const now = await getInjuryClock();
	if (!now) {
		return;
	}
	return daysOutHurtTonight({
		gamesRemaining,
		clock: now.clock,
		games: now.games,
		season: now.season,
		tonight,
		countedThrough: countedThrough(),
	});
};

// The clock the days left are counted on now, and the night they count from:
// in a season on the calendar, its stretch's clock and the last night played;
// in the offseason on the calendar, next season's before its first night.
// Undefined off the calendar (and for a season that started before the
// calendar was turned on).
export const currentDayClock = async (): Promise<
	{ clock: CalendarClock; tonight: string } | undefined
> => {
	const phase = g.get("phase");
	if (phase >= PHASE.REGULAR_SEASON && phase <= PHASE.PLAYOFFS) {
		const now = await getInjuryClock(phase);
		if (!now) {
			return;
		}
		return now.lastNight === undefined
			? { clock: clockBeforeFirstNight(now.clock), tonight: "" }
			: { clock: now.clock, tonight: now.lastNight };
	}
	if (!nbaCalendarLeague()) {
		return;
	}
	return { clock: await seasonStartClock(), tonight: "" };
};

// The regular season's clock before its first night: from its schedule once
// it's made, before that the season just played standing in for it, else the
// NBA's.
export const seasonStartClock = async () => {
	const clock = calendarClock({
		games: await idb.cache.games.getAll(),
		// (In the playoffs, what's left is theirs.)
		schedule:
			g.get("phase") < PHASE.PLAYOFFS ? await idb.cache.schedule.getAll() : [],
		season: g.get("season"),
		phase: PHASE.REGULAR_SEASON,
		classicDays: storedClassicDays(),
	});
	return clock ? clockBeforeFirstNight(clock) : DEFAULT_SEASON_CLOCK;
};

// The highest Play Through Injuries setting.
const PLAY_THROUGH_MAX = 10;

// A Play Through Injuries slider on the NBA calendar: up to `max` days, a day
// at a time, the performance prorated by `rate` (see
// playThroughInjuriesDayFactor); `days`, the team's setting.
export type PlayThroughCalendar = { max: number; rate: number; days: number };

// For the Play Through Injuries sliders on the NBA calendar (regular season,
// playoffs): days instead of classic games, as far as the classic setting
// goes (10 classic days - about 19 days in the regular season). Before a team
// picks days, its classic setting as days: the most days left a player can
// show and still play. Undefined off the calendar.
export const playThroughCalendar = async (t: {
	playThroughInjuries: [number, number];
	playThroughInjuryDays?: [number, number];
}): Promise<[PlayThroughCalendar, PlayThroughCalendar] | undefined> => {
	const now = await currentDayClock();
	if (!now) {
		return;
	}
	const playoffs = g.get("phase") === PHASE.PLAYOFFS;
	const regular = playoffs
		? { clock: await seasonStartClock(), tonight: "" }
		: now;
	const post = playoffs ? now : { clock: PLAYOFF_START_CLOCK, tonight: "" };
	const slider = (
		{ clock, tonight }: { clock: CalendarClock; tonight: string },
		i: 0 | 1,
	): PlayThroughCalendar => {
		const asDays = (v: number) => daysOut(v, clock, tonight);
		const days =
			t.playThroughInjuryDays?.[i] ?? asDays(t.playThroughInjuries[i]);
		return {
			max: Math.max(asDays(PLAY_THROUGH_MAX), days),
			rate: clock.rate,
			days,
		};
	};
	return [slider(regular, 0), slider(post, 1)];
};

// The user's team's setting in days, for tonight's game (see processTeam):
// on the calendar, once it's been set in days. Its rate prorates the
// performance (playThroughInjuriesDayFactor).
export const playThroughDaysTonight = async (
	t: { tid: number; playThroughInjuryDays?: [number, number] },
	playoffs: boolean,
) => {
	const days = t.playThroughInjuryDays?.[playoffs ? 1 : 0];
	if (
		days === undefined ||
		g.get("spectator") ||
		!g.get("userTids").includes(t.tid)
	) {
		return;
	}
	const now = await getInjuryClock();
	if (!now) {
		return;
	}
	return { days, rate: now.clock.rate };
};
