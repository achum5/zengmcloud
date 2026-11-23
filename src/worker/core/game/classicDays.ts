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

// This stretch's clock (calendarTicks.ts) and the last night played, for
// projecting injuries forward and for the days they have left. Undefined off
// the calendar.
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

// For the Play Through Injuries sliders on the NBA calendar: for each setting
// (0 to 10 classic days, for the regular season and the playoffs), the most
// days left a player can show and still play. The setting itself stays in
// classic days, so it plays exactly as in a classic league; everyone's days
// count on the same clock, so right now a player plays through exactly when
// his days are at most this. Undefined off the calendar.
export const playThroughDays = async (): Promise<
	[number[], number[]] | undefined
> => {
	const now = await currentDayClock();
	if (!now) {
		return;
	}
	const playoffs = g.get("phase") === PHASE.PLAYOFFS;
	const regular = playoffs
		? { clock: await seasonStartClock(), tonight: "" }
		: now;
	const post = playoffs ? now : { clock: PLAYOFF_START_CLOCK, tonight: "" };
	const labels = ({
		clock,
		tonight,
	}: {
		clock: CalendarClock;
		tonight: string;
	}) =>
		Array.from({ length: PLAY_THROUGH_MAX + 1 }, (_, v) =>
			daysOut(v, clock, tonight),
		);
	return [labels(regular), labels(post)];
};
