// WHERE THE SIM STOPS ON PURPOSE.
//
// A room of humans needs the league to pause at the moments worth pausing at.
// The trade deadline was hardcoded as the only one and it stopped whether the
// league wanted it or not; this makes it a list, and makes the deadline one
// entry in that list rather than a special case.
//
// Stored as a plain comma-separated string so it can be typed into League
// Settings and read back at a glance ("15, 41, deadline"), and so it syncs as
// one small game attribute rather than a structure that has to be merged.
//
// A day entry means: stop when day N is the NEXT thing to be played, before it
// is played. That is how the deadline already behaves - you stop on arrival,
// deal, and then cross - so a number and the word "deadline" mean the same
// kind of thing.
//
// On the NBA calendar a stop can be a date instead ("Dec 15", every season):
// the sim stops on arrival at it - or, in a season that skips the dates with
// no games (from before season/offDays.ts), before the first night on or
// after it.

import { makeDate } from "./calendarDates.ts";

export const SIM_STOP_DEADLINE = "deadline";

// A date stop: a month and day, the same each season.
export type SimStopDate = { month: number; day: number };

const MONTH_ABBREVS = [
	"jan",
	"feb",
	"mar",
	"apr",
	"may",
	"jun",
	"jul",
	"aug",
	"sep",
	"oct",
	"nov",
	"dec",
];

// Days in each month, February's leap day allowed.
const MONTH_DAYS = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

// "Dec 15", "december 15", "Dec. 15" - or undefined if it isn't a real date.
const parseStopDate = (token: string): SimStopDate | undefined => {
	const match = /^([a-z]{3})[a-z]*\.?\s*(\d{1,2})$/.exec(token);
	if (!match) {
		return;
	}
	const month = MONTH_ABBREVS.indexOf(match[1]!) + 1;
	const day = Number(match[2]);
	if (month === 0 || day < 1 || day > MONTH_DAYS[month - 1]!) {
		return;
	}
	return { month, day };
};

// The settings' own spelling, "Dec 15".
export const formatStopDate = ({ month, day }: SimStopDate) =>
	`${MONTH_ABBREVS[month - 1]![0]!.toUpperCase()}${MONTH_ABBREVS[month - 1]!.slice(1)} ${day}`;

// Season order: from July, the season's first months, through June.
const seasonOrder = ({ month, day }: SimStopDate) =>
	((month + 5) % 12) * 100 + day;

// The date in a season (named for the year it ends in, as the game's are).
export const stopDateIn = ({ month, day }: SimStopDate, season: number) =>
	makeDate(month >= 7 ? season - 1 : season, month, day);

export type SimStopDays = {
	// Stop when the trade deadline sentinel is next.
	deadline: boolean;
	// Regular-season day numbers to stop before, ascending and deduplicated.
	days: number[];
	// Dates to stop before, on the NBA calendar, in season order.
	dates: SimStopDate[];
};

export const EMPTY_SIM_STOP_DAYS: SimStopDays = {
	deadline: false,
	days: [],
	dates: [],
};

// Lenient on the way in - people type "15,deadline" and "Day 15, 41" - and
// strict about what it will accept as a day, since a typo that silently became
// day 0 would stop the league somewhere it can never leave.
export const parseSimStopDays = (raw: string | undefined): SimStopDays => {
	if (typeof raw !== "string" || raw.trim() === "") {
		return EMPTY_SIM_STOP_DAYS;
	}

	let deadline = false;
	const days = new Set<number>();
	const dates = new Map<number, SimStopDate>();
	for (const part of raw.split(",")) {
		const token = part
			.trim()
			.toLowerCase()
			.replace(/^day\s+/, "");
		if (token === "") {
			continue;
		}
		if (token === SIM_STOP_DEADLINE) {
			deadline = true;
			continue;
		}
		const date = parseStopDate(token);
		if (date) {
			dates.set(seasonOrder(date), date);
			continue;
		}
		const day = Number(token);
		if (Number.isInteger(day) && day > 0) {
			days.add(day);
		}
	}

	return {
		deadline,
		days: [...days].sort((a, b) => a - b),
		dates: [...dates].sort((a, b) => a[0] - b[0]).map(([, date]) => date),
	};
};

// What the settings field shows. Round-trips through parseSimStopDays.
export const formatSimStopDays = ({
	deadline,
	days,
	dates,
}: SimStopDays): string =>
	[
		...days.map(String),
		...dates.map(formatStopDate),
		...(deadline ? [SIM_STOP_DEADLINE] : []),
	].join(", ");

// Whether every token is something parseSimStopDays would keep, so the settings
// form can reject a typo instead of quietly dropping it.
export const invalidSimStopDayToken = (
	raw: string | undefined,
): string | undefined => {
	if (typeof raw !== "string") {
		return undefined;
	}
	for (const part of raw.split(",")) {
		const token = part
			.trim()
			.toLowerCase()
			.replace(/^day\s+/, "");
		if (
			token === "" ||
			token === SIM_STOP_DEADLINE ||
			parseStopDate(token) !== undefined
		) {
			continue;
		}
		const day = Number(token);
		if (!Number.isInteger(day) || day <= 0) {
			return part.trim();
		}
	}
	return undefined;
};

export const stopsOnDay = (
	stops: SimStopDays,
	day: number | undefined,
): boolean => typeof day === "number" && stops.days.includes(day);

// Whether a night stops for a date: one of the dates falls after the last
// night played (none yet: any before) and on or before this one - so a date
// with no games stops before the next night that has them.
export const stopsOnNight = (
	stops: SimStopDays,
	night: string | undefined,
	lastNight: string | undefined,
	season: number,
): boolean =>
	night !== undefined &&
	stops.dates.some((stop) => {
		const date = stopDateIn(stop, season);
		return date <= night && (lastNight === undefined || date > lastNight);
	});
