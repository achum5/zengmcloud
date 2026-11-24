import { PHASE } from "../../../common/constants.ts";
import {
	addDays,
	daysBetween,
	makeDate,
} from "../../../common/calendarDates.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import {
	type CalendarClock,
	nbaCalendarLeague,
	ticksBetween,
} from "./calendarTicks.ts";
import { getInjuryClock, seasonStartClock } from "./classicDays.ts";

// THE NBA'S TRADE DATES, on the NBA calendar (owner, 2026-11-24: "research
// what the actual NBA rule on this is"). A player who signs can't be traded
// until (the CBA, as Hoops Rumors sets it out each summer):
//  - signed in the offseason: December 15, or three months after he signed
//    if that is later (a signing after September 15) - so a preseason
//    signing, at training camp (October 1), January 1;
//  - re-signed: January 15 (the NBA's is for a raise over 20% with his team
//    over the cap; the owner, 2026-11-24: "just simplify and make all
//    re-signs Jan 15");
//  - signed during the season: three months on.
// A classic league keeps the game's 14 games. The engine still counts the
// wait in classic days (gamesUntilTradable), so it plays the same on every
// device: the date is kept with the player (tradableOn) and turned into the
// classic days to it, which the season's dates fix once its schedule is made.

// The game's own wait (player/sign.ts), in classic days.
export const classicCoolOff = () => Math.round(0.17 * g.get("numGames"));

// The same day of the month three months on (the month's last, if shorter).
export const threeMonthsAfter = (date: string) => {
	const year = Number(date.slice(0, 4));
	const month = Number(date.slice(5, 7)) + 3;
	const y = month > 12 ? year + 1 : year;
	const m = month > 12 ? month - 12 : month;
	const last = Number(makeDate(y, m + 1, 0).slice(8, 10));
	return makeDate(y, m, Math.min(Number(date.slice(8, 10)), last));
};

// The year the season being played, or coming, starts in: in the offseason
// after the playoffs, the next one's.
const startYear = (phase: number) =>
	phase > PHASE.PLAYOFFS ? g.get("season") : g.get("season") - 1;

// A typical opening night, for a season whose schedule isn't made yet.
const openingNightGuess = (year: number) => makeDate(year, 10, 21);

// The night a player signed today can first be traded.
export const signedTradableOn = ({
	phase,
	signedOn,
	resigned,
}: {
	phase: number;
	// In season: the night he signed before (the next to be played).
	signedOn?: string;
	// Re-signed by his own team.
	resigned: boolean;
}) => {
	const year = startYear(phase);
	if (phase >= PHASE.REGULAR_SEASON && phase <= PHASE.PLAYOFFS) {
		return threeMonthsAfter(signedOn ?? openingNightGuess(year));
	}
	if (resigned) {
		return makeDate(year + 1, 1, 15);
	}
	if (phase === PHASE.PRESEASON) {
		return threeMonthsAfter(makeDate(year, 10, 1));
	}
	return makeDate(year, 12, 15);
};

// The classic days from tonight to that night (the wait ends with the night
// before it): on the season's clock, or before its schedule is made, from a
// typical opening night at the clock's pace - put right when the schedule
// is made (tradableOnForNewSeason).
const ticksTo = async (date: string) => {
	const now = await getInjuryClock();
	if (now) {
		return ticksBetween(now.clock, now.lastNight, addDays(date, -1));
	}
	const clock: CalendarClock = await seasonStartClock();
	const year =
		Number(date.slice(5, 7)) >= 7
			? Number(date.slice(0, 4))
			: Number(date.slice(0, 4)) - 1;
	return Math.floor(
		Math.max(0, daysBetween(openingNightGuess(year), date)) * clock.rate,
	);
};

// After a night on the calendar: the classic days left to a wait's date, at
// least one until the night before it is played, or until the next night
// with games is on or past it (a break in the schedule) - the classic days,
// nearly two nights each, alone would end it a night early or late.
export const classicDaysTo = (
	date: string,
	clock: CalendarClock,
	tonight: string,
	nextNight?: string,
) => {
	const last = addDays(date, -1);
	return tonight >= last || (nextNight !== undefined && nextNight >= date)
		? 0
		: Math.max(1, ticksBetween(clock, tonight, last));
};

// A signing's wait: on the NBA calendar the night it ends and the classic
// days to it, else the game's 14 games.
export const coolOffForSigning = async ({
	phase,
	resigned,
}: {
	phase: number;
	resigned: boolean;
}): Promise<{ games: number; on?: string }> => {
	if (!nbaCalendarLeague()) {
		return { games: classicCoolOff() };
	}
	let signedOn: string | undefined;
	if (phase >= PHASE.REGULAR_SEASON && phase <= PHASE.PLAYOFFS) {
		const now = await getInjuryClock();
		if (!now) {
			// A season begun before the calendar was on.
			return { games: classicCoolOff() };
		}
		const next = (await idb.cache.schedule.getAll())
			.map((game) => game.date)
			.filter((date): date is string => date !== undefined)
			.sort()[0];
		signedOn = next ?? (now.lastNight ? addDays(now.lastNight, 1) : undefined);
	}
	const on = signedTradableOn({ phase, signedOn, resigned });
	return { games: Math.max(1, await ticksTo(on)), on };
};

// A wait from before the NBA's dates (a classic count, no date) that hasn't
// begun, the summer's: to December 15 of the season starting in `year`, or a
// summer re-signing's (his deal, a re-signing's, starts that season) to
// January 15.
const summerTradableOn = (
	p: Pick<Player, "gamesUntilTradable" | "contracts">,
	year: number,
) => {
	if (p.gamesUntilTradable < classicCoolOff()) {
		return undefined;
	}
	const contract = p.contracts.at(-1);
	return contract?.type === "reSign" && contract.start === year + 1
		? makeDate(year + 1, 1, 15)
		: makeDate(year, 12, 15);
};

// The night a player's wait ends, as the game shows it: his date, or before
// the season's schedule is made, the one a wait from before the NBA's dates
// is going to get.
export const shownTradableOn = (
	p: Pick<Player, "gamesUntilTradable" | "contracts" | "tradableOn">,
) => {
	if (!(p.gamesUntilTradable > 0)) {
		return undefined;
	}
	if (p.tradableOn !== undefined) {
		return p.tradableOn;
	}
	const phase = g.get("phase");
	if (
		!nbaCalendarLeague() ||
		(phase >= PHASE.REGULAR_SEASON && phase <= PHASE.PLAYOFFS)
	) {
		return undefined;
	}
	return summerTradableOn(p, startYear(phase));
};

// As a season's schedule is made: each wait to a date, the classic days to
// it on this season's own dates. A wait from before the NBA's dates (a
// classic count, no date): one that hasn't started, the summer's, to
// December 15, or a re-signing's to January 15; one left over from last
// season, over - its three months ran out in the summer. Off the calendar,
// the dates go.
export const tradableOnForNewSeason = async (calendar: boolean) => {
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const now = calendar ? await getInjuryClock(PHASE.REGULAR_SEASON) : undefined;
	const year = g.get("season") - 1;
	const changed: Player[] = [];
	for (const p of players) {
		if (!now) {
			if (p.tradableOn !== undefined) {
				delete p.tradableOn;
				changed.push(p);
			}
			continue;
		}
		if (p.gamesUntilTradable <= 0 && p.tradableOn === undefined) {
			continue;
		}
		const on = p.tradableOn ?? summerTradableOn(p, year);
		const games =
			on === undefined
				? 0
				: ticksBetween(now.clock, undefined, addDays(on, -1));
		if (games <= 0) {
			p.gamesUntilTradable = 0;
			delete p.tradableOn;
		} else {
			p.gamesUntilTradable = games;
			p.tradableOn = on;
		}
		changed.push(p);
	}
	if (changed.length > 0) {
		await idb.cache.players.putAll(changed);
	}
};

// A new season's preseason: a wait whose night has come by training camp
// (a late-season signing's three months) is over.
export const tradableOnForPreseason = async () => {
	const camp = makeDate(g.get("season") - 1, 10, 1);
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const changed: Player[] = [];
	for (const p of players) {
		if (p.tradableOn !== undefined && p.tradableOn <= camp) {
			p.gamesUntilTradable = 0;
			delete p.tradableOn;
			changed.push(p);
		}
	}
	if (changed.length > 0) {
		await idb.cache.players.putAll(changed);
	}
};
