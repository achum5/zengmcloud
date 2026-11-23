import { PLAYER } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { type CalendarClock, daysOut } from "./calendarTicks.ts";
import { seasonStartClock } from "./classicDays.ts";

// THE DAYS LEFT, on the NBA calendar. An injury and the wait after a signing
// count classic days (injury.gamesRemaining, gamesUntilTradable) for
// everything that reads them - the sim, the CPU, Play Through Injuries - so
// nothing plays differently from a classic league. What the game SHOWS is the
// calendar days they come to (daysOut, calendarTicks.ts), stored with the
// player so every page and every device shows the same number. Refreshed
// after each night's countdown and when a season's clock starts; cleared when
// a season has no calendar.

// Sets (or clears) a player's day counts. True if anything changed.
export const applyCalendarDays = (
	p: Pick<Player, "injury" | "gamesUntilTradable" | "daysUntilTradable">,
	clock: CalendarClock | undefined,
	tonight: string,
) => {
	let changed = false;

	const injuryDays =
		clock && p.injury.gamesRemaining > 0
			? daysOut(p.injury.gamesRemaining, clock, tonight)
			: undefined;
	if (p.injury.daysRemaining !== injuryDays) {
		if (injuryDays === undefined) {
			delete p.injury.daysRemaining;
		} else {
			p.injury.daysRemaining = injuryDays;
		}
		changed = true;
	}

	const tradeDays =
		clock && p.gamesUntilTradable > 0
			? daysOut(p.gamesUntilTradable, clock, tonight)
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
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		PLAYER.FREE_AGENT,
		Infinity,
	]);
	const changed = players.filter((p) => applyCalendarDays(p, clock, tonight));
	if (changed.length > 0) {
		await idb.cache.players.putAll(changed);
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
