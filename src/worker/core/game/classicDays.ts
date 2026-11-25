import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import {
	type CalendarClock,
	calendarClock,
	clockBeforeFirstNight,
	daysOutHurtTonight,
	DEFAULT_SEASON_CLOCK,
	nbaCalendarLeague,
	PLAYOFF_CLASSIC_DAYS_PER_DAY,
} from "./calendarTicks.ts";
import type { PlayThroughDays } from "../../../common/playThroughInjuriesFactor.ts";

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

// A classic setting as days, at a stretch's pace. A fixed conversion - not
// the days a classic count would take from tonight, which come out a day more
// or less from one day to the next - so a setting never moves on its own.
const classicAsDays = (classic: number, rate: number) =>
	classic <= 0 ? 0 : Math.max(1, Math.round(classic / rate));

// The classic days a calendar day is worth in the regular season or the
// playoffs, for games on this clock (getInjuryClock's). Undefined for the
// regular season once it's over.
export const playThroughRate = (
	playoffs: boolean,
	clock: CalendarClock,
	phase: number = g.get("phase"),
) =>
	playoffs
		? PLAYOFF_CLASSIC_DAYS_PER_DAY
		: phase >= PHASE.REGULAR_SEASON && phase < PHASE.PLAYOFFS
			? clock.rate
			: undefined;

type PlayThroughTeam = {
	tid: number;
	playThroughInjuries: [number, number];
	playThroughInjuryDays?: [number, number];
};

// A team's setting in days, at this pace: the user's choice in days, or
// before one is made, the classic setting as days.
const settingInDays = (
	t: Omit<PlayThroughTeam, "tid">,
	i: 0 | 1,
	rate: number,
) =>
	t.playThroughInjuryDays?.[i] ?? classicAsDays(t.playThroughInjuries[i], rate);

// For the Play Through Injuries sliders on the NBA calendar (regular season,
// playoffs): days instead of classic games, as far as the classic setting
// goes (10 classic days - about 19 days in the regular season). Undefined off
// the calendar.
export const playThroughCalendar = async (
	t: Omit<PlayThroughTeam, "tid">,
): Promise<[PlayThroughCalendar, PlayThroughCalendar] | undefined> => {
	const now = await currentDayClock();
	if (!now) {
		return;
	}
	const regularRate =
		g.get("phase") === PHASE.PLAYOFFS
			? (await seasonStartClock()).rate
			: now.clock.rate;
	const slider = (rate: number, i: 0 | 1): PlayThroughCalendar => {
		const days = settingInDays(t, i, rate);
		return {
			max: Math.max(classicAsDays(PLAY_THROUGH_MAX, rate), days),
			rate,
			days,
		};
	};
	return [slider(regularRate, 0), slider(PLAYOFF_CLASSIC_DAYS_PER_DAY, 1)];
};

// A team's setting in days for a game on the NBA calendar (see injuryAtGame),
// at the stretch's pace (playThroughRate): the user's teams', the same days
// their slider shows. A CPU team (or any, watching as a spectator) keeps its
// classic setting.
export const playThroughDays = (
	t: PlayThroughTeam,
	playoffs: boolean,
	rate: number | undefined,
): PlayThroughDays | undefined => {
	if (
		rate === undefined ||
		g.get("spectator") ||
		!g.get("userTids").includes(t.tid)
	) {
		return;
	}
	return { days: settingInDays(t, playoffs ? 1 : 0, rate), rate };
};

// The user's team's setting in days, for tonight's game (see processTeam).
// Its rate prorates the performance (playThroughInjuriesDayFactor).
export const playThroughDaysTonight = async (
	t: PlayThroughTeam,
	playoffs: boolean,
) => {
	const now = await getInjuryClock();
	if (!now) {
		return;
	}
	return playThroughDays(t, playoffs, playThroughRate(playoffs, now.clock));
};
