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
import { featureMarqueeNights, type MarqueeDraw } from "./marqueeNights.ts";
import { rankRivalries } from "./rivalries.ts";

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
	// With dates: the rivalries Rivals Week has (marqueeNights.ts).
	rivals?: [number, number][];
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

// BBGM's default trade deadline. Close to it, the calendar has the NBA's:
// the Thursday a week and a half before the All-Star Game (Feb 9 in 2023,
// two thirds of the way through the games).
const DEFAULT_TRADE_DEADLINE = 0.6;

const nbaTradeDeadline = (nights: Set<string>, allStarDate: string) => {
	for (let back = 8; back <= 21; back++) {
		const date = addDays(allStarDate, -back);
		if (weekdayOf(date) === 4 && nights.has(date)) {
			return date;
		}
	}
};

// The trade deadline at its usual point in the schedule, moved to the
// nearest Thursday with games when there is one within three nights before
// the All-Star Game - or, with the default setting, the NBA's.
const tradeDeadlineDate = (
	sortedDates: string[],
	fraction: number,
	allStarDate: string | undefined,
) => {
	if (sortedDates.length === 0) {
		return;
	}
	if (
		allStarDate !== undefined &&
		Math.abs(fraction - DEFAULT_TRADE_DEADLINE) <= 0.05
	) {
		const nba = nbaTradeDeadline(new Set(sortedDates), allStarDate);
		if (nba !== undefined) {
			return nba;
		}
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
): Required<Omit<DatedSchedule, "classicDays" | "rivals">> => {
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

// `marquee`: who the league wants to see, for opening night, Christmas and
// Rivals Week (see marqueeNights.ts) - without it, they're as they fall.
const newCalendarSchedule = (
	teams: CalendarScheduleTeam[],
	conditions?: Conditions,
	marquee?: MarqueeDraw,
): DatedSchedule => {
	if (!calendarOn() || teams.length >= TOO_MANY_TEAMS_TOO_SLOW) {
		return { tids: newSchedule(teams, conditions) };
	}

	const season = g.get("season");
	const fraction = allStarFraction();
	const tradeDeadline = g.get("tradeDeadline");

	const locationOf = (t: CalendarScheduleTeam) =>
		teamLocation({ region: t.seasonAttrs.region, homeCity: t.homeCity });

	const locations = new Map(teams.map((t) => [t.tid, locationOf(t)]));
	let rivals: [number, number][] | undefined;
	const featured = <
		T extends { homeTid: number; awayTid: number; date: string },
	>(
		games: T[],
	) => {
		if (!marquee) {
			return games;
		}
		const out = featureMarqueeNights(
			games,
			{
				...marquee,
				rivals: rankRivalries({
					teams: teams.map((t) => ({
						tid: t.tid,
						cid: t.seasonAttrs.cid,
						did: t.seasonAttrs.did,
					})),
					locations,
					draw: marquee.draw,
					users: marquee.users,
					playoffs: marquee.playoffs,
				}),
			},
			locations,
		);
		rivals = out.rivals;
		return out.games;
	};

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
			...assembleDatedSchedule(featured(real), allStarDate, tradeDeadline),
			classicDays: countClassicDays(newSchedule(teams)),
			rivals,
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
			featured(
				regular.map(([homeTid, awayTid], i) => ({
					homeTid,
					awayTid,
					date: calendar.dates[i]!,
				})),
			),
			calendar.allStarDate,
			tradeDeadline,
		),
		classicDays: countClassicDays(classic),
		rivals,
	};
};

export default newCalendarSchedule;
