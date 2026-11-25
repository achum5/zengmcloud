import { PLAYER } from "../../../common/constants.ts";
import { addDays, daysBetween } from "../../../common/calendarDates.ts";
import { sameGamesOut } from "../../../common/gamesOut.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { type CalendarClock, daysOut } from "./calendarTicks.ts";
import { currentDayClock, seasonStartClock } from "./classicDays.ts";
import { type GamesOutCounter, loadGamesOut } from "./injuryGamesOut.ts";

// THE DAYS LEFT, on the NBA calendar. An injury and the wait after a signing
// count classic days (injury.gamesRemaining, gamesUntilTradable) for
// everything that reads them - the sim, the CPU, Play Through Injuries - so
// nothing plays differently from a classic league. What the game SHOWS is the
// calendar days they come to (daysOut, calendarTicks.ts), stored with the
// player so every page and every device shows the same number, and beside an
// injury's days the games on his team's schedule they take in
// (injuryGamesOut.ts). Refreshed after each night's countdown, when the
// playoffs draw up a night, when a player changes teams and when a season's
// clock starts; cleared when a season has no calendar.
//
// The days count from the day before the next night the game will play - the
// day it shows as today. That's the night just played, except in a season
// made before every date was a day (season/offDays.ts), which skips a date
// with no games (Christmas Eve, the All-Star break, a rest day in the
// playoffs): after the 23rd the game shows the 25th, so the days count from
// the 24th. The skipped days are counted when the next night's countdown
// runs, so the night he's back is the same either way.

// What a player's days left are counted on.
export type DayCount = {
	clock: CalendarClock;
	// The last night counted ("" before a stretch's first).
	tonight: string;
	// The day the days left count from: tonight, or the day before the next
	// night when dates are skipped before it.
	from: string;
	gamesOut: GamesOutCounter;
};

// The day before the next night with anything on it, when that's after
// tonight; else tonight.
export const countFrom = (
	tonight: string,
	schedule: readonly { date?: string }[],
) => {
	if (tonight === "") {
		return tonight;
	}
	let next: string | undefined;
	for (const { date } of schedule) {
		if (
			date !== undefined &&
			date > tonight &&
			(next === undefined || date < next)
		) {
			next = date;
		}
	}
	return next === undefined ? tonight : addDays(next, -1);
};

export const loadDayCount = async (
	clock: CalendarClock,
	tonight: string,
): Promise<DayCount> => {
	const from = countFrom(tonight, await idb.cache.schedule.getAll());
	return { clock, tonight, from, gamesOut: await loadGamesOut(from, clock) };
};

// What the days left are counted on now (see currentDayClock). Undefined off
// the calendar.
export const currentDayCount = async () => {
	const now = await currentDayClock();
	return now ? loadDayCount(now.clock, now.tonight) : undefined;
};

// The days shown for a wait of this many classic days.
export const daysLeft = (games: number, count: DayCount) => {
	const days = daysOut(games, count.clock, count.tonight);
	const skipped =
		count.from > count.tonight && count.tonight !== ""
			? daysBetween(count.tonight, count.from)
			: 0;
	// Out for the next night's game, at least.
	return Math.max(1, days - skipped);
};

// Sets (or clears) a player's day counts. True if anything changed.
export const applyCalendarDays = (
	p: Pick<
		Player,
		"injury" | "gamesUntilTradable" | "daysUntilTradable" | "tid"
	>,
	count: DayCount | undefined,
) => {
	let changed = false;

	const injuryDays =
		count && p.injury.gamesRemaining > 0
			? daysLeft(p.injury.gamesRemaining, count)
			: undefined;
	if (p.injury.daysRemaining !== injuryDays) {
		if (injuryDays === undefined) {
			delete p.injury.daysRemaining;
		} else {
			p.injury.daysRemaining = injuryDays;
		}
		changed = true;
	}

	const gamesOut = count?.gamesOut(p.tid, injuryDays);
	if (!sameGamesOut(p.injury.gamesOut, gamesOut)) {
		if (gamesOut === undefined) {
			delete p.injury.gamesOut;
		} else {
			p.injury.gamesOut = gamesOut;
		}
		changed = true;
	}

	const tradeDays =
		count && p.gamesUntilTradable > 0
			? daysLeft(p.gamesUntilTradable, count)
			: undefined;
	if (p.daysUntilTradable !== tradeDays) {
		if (tradeDays === undefined) {
			delete p.daysUntilTradable;
		} else {
			p.daysUntilTradable = tradeDays;
		}
		changed = true;
	}

	return changed;
};

// Everyone on a team or in free agency, after tonight on this clock (or
// cleared, without one).
export const refreshCalendarDays = async (
	clock: CalendarClock | undefined,
	tonight: string,
) => {
	const count = clock ? await loadDayCount(clock, tonight) : undefined;
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		PLAYER.FREE_AGENT,
		Infinity,
	]);
	const changed = players.filter((p) => applyCalendarDays(p, count));
	if (changed.length > 0) {
		await idb.cache.players.putAll(changed);
	}
};

// Everyone, on what the days are counted on now: after the playoffs draw up
// their next night (its date, and the round's plan, are known only then).
// Nothing off the calendar.
export const refreshCalendarDaysNow = async () => {
	const now = await currentDayClock();
	if (now) {
		await refreshCalendarDays(now.clock, now.tonight);
	}
};

// Before the regular season's first night: the season's clock with nothing
// on it yet, so a wait carried over from the offseason reads as the days of
// the season it takes (in the offseason, see seasonStartClock). Cleared for a
// season without dates.
export const refreshCalendarDaysForNewSeason = async (calendar: boolean) => {
	await refreshCalendarDays(
		calendar ? await seasonStartClock() : undefined,
		"",
	);
};
