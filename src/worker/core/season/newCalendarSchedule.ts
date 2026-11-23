import { g } from "../../util/index.ts";
import type { Conditions } from "../../../common/types.ts";
import { TOO_MANY_TEAMS_TOO_SLOW } from "./getInitialNumGamesConfDivSettings.ts";
import {
	ALL_STAR_GAME_TIDS,
	TRADE_DEADLINE_TIDS,
	isAllStarGame,
	isTradeDeadline,
} from "../../../common/specialGames.ts";
import {
	addDays,
	dayNumber,
	weekdayOf,
} from "../../../common/calendarDates.ts";
import newSchedule from "./newSchedule.ts";
import { getRealSchedule } from "./getRealSchedule.basketball.ts";
import { generateCalendar } from "./generateCalendar.ts";
import { teamLocation, type HomeCity } from "../../../common/teamLocation.ts";
import { countClassicDays } from "../game/calendarTicks.ts";

// THE REGULAR SEASON ON REAL DATES (basketball): every game gets the night
// it's played on, and each night is one sim day. A league shaped like the
// NBA plays a real NBA season moved to its year; any other league gets a
// generated NBA-style calendar of its usual matchups. Without the calendar
// (setting off, college, series grouping, other sports) it's the classic
// numbered days, exactly as before.

export type DatedSchedule = {
	tids: [number, number][];
	// Parallel to tids, or absent for classic numbered days.
	dates?: string[];
	// With dates: how many classic days the season would have taken, for its
	// end-of-day rules to keep the classic pace (game/calendarTicks.ts).
	classicDays?: number;
};

type CalendarScheduleTeam = {
	tid: number;
	homeCity?: HomeCity;
	seasonAttrs: {
		cid: number;
		did: number;
		abbrev?: string;
		region?: string;
	};
};

// Default All-Star placement. Close to it, a real season keeps its own break.
const DEFAULT_ALL_STAR_GAME = 0.7;

export const calendarOn = () =>
	__SPORT === "basketball" &&
	!g.get("college") &&
	!g.get("groupScheduleSeries") &&
	!!g.get("nbaCalendar");

const allStarFraction = () => {
	const allStarGame = g.get("allStarGame");
	return allStarGame !== null && allStarGame >= 0 ? allStarGame : undefined;
};

// The Sunday inside the longest stretch without games from mid-January to
// mid-March: a real season's All-Star break.
const realAllStarSunday = (dates: string[]) => {
	const nights = Array.from(new Set(dates)).sort();
	let best: { from: string; to: string } | undefined;
	let bestGap = 0;
	for (let i = 1; i < nights.length; i++) {
		const month = Number(nights[i]!.slice(5, 7));
		if (month !== 2 && month !== 3 && month !== 1) {
			continue;
		}
		const gap = dayNumber(nights[i]!) - dayNumber(nights[i - 1]!);
		if (gap > bestGap) {
			bestGap = gap;
			best = { from: nights[i - 1]!, to: nights[i]! };
		}
	}
	if (!best || bestGap < 3) {
		return;
	}
	for (
		let date = addDays(best.from, 1);
		date !== best.to;
		date = addDays(date, 1)
	) {
		if (weekdayOf(date) === 0) {
			return date;
		}
	}
	return addDays(best.from, 1);
};

// The trade deadline at its usual point in the schedule, moved to the
// nearest Thursday with games when there is one within three nights before
// the All-Star Game.
const tradeDeadlineDate = (
	sortedDates: string[],
	fraction: number,
	allStarDate: string | undefined,
) => {
	if (sortedDates.length === 0) {
		return;
	}
	const index = Math.min(
		sortedDates.length - 1,
		Math.max(0, Math.round(fraction * sortedDates.length)),
	);
	const at = sortedDates[index]!;
	const nights = new Set(sortedDates);
	for (const offset of [0, -1, 1, -2, 2, -3, 3]) {
		const date = addDays(at, offset);
		if (
			weekdayOf(date) === 4 &&
			nights.has(date) &&
			(allStarDate === undefined || date < allStarDate)
		) {
			return date;
		}
	}
	return at;
};

// Regular games in date order, with the All-Star Game and the trade deadline
// placed on their nights (each still its own sim day, ahead of any games that
// night).
export const assembleDatedSchedule = (
	games: { homeTid: number; awayTid: number; date: string }[],
	allStarDate: string | undefined,
	tradeDeadline: number,
): Required<Omit<DatedSchedule, "classicDays">> => {
	const sorted = games
		.map((game, i) => ({ ...game, i }))
		.sort((a, b) => (a.date === b.date ? a.i - b.i : a.date < b.date ? -1 : 1));
	const deadline =
		tradeDeadline < 1
			? tradeDeadlineDate(
					sorted.map((game) => game.date),
					tradeDeadline,
					allStarDate,
				)
			: undefined;

	const tids: [number, number][] = [];
	const dates: string[] = [];
	let addedAllStar = allStarDate === undefined;
	let addedDeadline = deadline === undefined;
	const addSpecials = (beforeDate: string | undefined) => {
		if (
			!addedDeadline &&
			(beforeDate === undefined || deadline! <= beforeDate)
		) {
			tids.push([...TRADE_DEADLINE_TIDS]);
			dates.push(deadline!);
			addedDeadline = true;
		}
		if (
			!addedAllStar &&
			(beforeDate === undefined || allStarDate! <= beforeDate)
		) {
			tids.push([...ALL_STAR_GAME_TIDS]);
			dates.push(allStarDate!);
			addedAllStar = true;
		}
	};
	for (const game of sorted) {
		addSpecials(game.date);
		tids.push([game.homeTid, game.awayTid]);
		dates.push(game.date);
	}
	addSpecials(undefined);
	return { tids, dates };
};

const newCalendarSchedule = (
	teams: CalendarScheduleTeam[],
	conditions?: Conditions,
): DatedSchedule => {
	if (!calendarOn() || teams.length >= TOO_MANY_TEAMS_TOO_SLOW) {
		return { tids: newSchedule(teams, conditions) };
	}

	const season = g.get("season");
	const fraction = allStarFraction();
	const tradeDeadline = g.get("tradeDeadline");

	const locationOf = (t: CalendarScheduleTeam) =>
		teamLocation({ region: t.seasonAttrs.region, homeCity: t.homeCity });

	const real = getRealSchedule(
		teams.map((t) => ({
			tid: t.tid,
			cid: t.seasonAttrs.cid,
			did: t.seasonAttrs.did,
			abbrev: t.seasonAttrs.abbrev,
			location: locationOf(t),
		})),
		season,
	);
	if (real) {
		let allStarDate: string | undefined;
		if (fraction !== undefined) {
			const dates = real.map((game) => game.date).sort();
			allStarDate =
				Math.abs(fraction - DEFAULT_ALL_STAR_GAME) <= 0.05
					? realAllStarSunday(dates)
					: dates[
							Math.min(dates.length - 1, Math.round(fraction * dates.length))
						];
		}
		return {
			...assembleDatedSchedule(real, allStarDate, tradeDeadline),
			classicDays: countClassicDays(newSchedule(teams)),
		};
	}

	const classic = newSchedule(teams, conditions);
	const regular = classic.filter(
		([home, away]) =>
			!isAllStarGame(home, away) && !isTradeDeadline(home, away),
	);
	const calendar = generateCalendar({
		season,
		teams: teams.map((t) => ({ tid: t.tid, location: locationOf(t) })),
		games: regular,
		allStarFraction: fraction,
	});
	if (!calendar) {
		return { tids: classic };
	}
	return {
		...assembleDatedSchedule(
			regular.map(([homeTid, awayTid], i) => ({
				homeTid,
				awayTid,
				date: calendar.dates[i]!,
			})),
			calendar.allStarDate,
			tradeDeadline,
		),
		classicDays: countClassicDays(classic),
	};
};

export default newCalendarSchedule;
