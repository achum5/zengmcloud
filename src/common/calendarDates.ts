// THE NIGHT A GAME IS PLAYED, as "YYYY-MM-DD". A date in no time zone: it is
// the league's calendar, not anyone's clock, so it is never turned into a
// Date in local time (west of Greenwich that prints the day before).

const MS_PER_DAY = 86_400_000;

const MONTHS = [
	"January",
	"February",
	"March",
	"April",
	"May",
	"June",
	"July",
	"August",
	"September",
	"October",
	"November",
	"December",
] as const;

const WEEKDAYS = [
	"Sunday",
	"Monday",
	"Tuesday",
	"Wednesday",
	"Thursday",
	"Friday",
	"Saturday",
] as const;

const parse = (date: string) => {
	const year = Number(date.slice(0, 4));
	const month = Number(date.slice(5, 7));
	const day = Number(date.slice(8, 10));
	return { year, month, day };
};

// Days since 1970-01-01.
export const dayNumber = (date: string) => {
	const { year, month, day } = parse(date);
	return Math.round(Date.UTC(year, month - 1, day) / MS_PER_DAY);
};

export const fromDayNumber = (n: number) =>
	new Date(n * MS_PER_DAY).toISOString().slice(0, 10);

export const makeDate = (year: number, month: number, day: number) =>
	fromDayNumber(Math.round(Date.UTC(year, month - 1, day) / MS_PER_DAY));

export const addDays = (date: string, n: number) =>
	fromDayNumber(dayNumber(date) + n);

export const daysBetween = (from: string, to: string) =>
	dayNumber(to) - dayNumber(from);

// 0 = Sunday.
export const weekdayOf = (date: string) =>
	new Date(dayNumber(date) * MS_PER_DAY).getUTCDay();

// The nth (1-based) given weekday of a month, e.g. Thanksgiving is the 4th
// Thursday (4) of November.
export const nthWeekday = (
	year: number,
	month: number,
	weekday: number,
	nth: number,
) => {
	const first = makeDate(year, month, 1);
	const offset = (weekday - weekdayOf(first) + 7) % 7;
	return addDays(first, offset + 7 * (nth - 1));
};

// BBGM's season is named for the year it ends in: the 2027 season tips off in
// October 2026.
export const seasonStartYear = (season: number) => season - 1;

// "Tue, Jan 13" / "Tuesday, January 13" / "Tuesday, January 13, 2027" /
// "Tue, Jan 13, 2027" / "Jan 13, 2027" / "Jan 13".
export const formatGameDate = (
	date: string,
	style:
		| "short"
		| "long"
		| "full"
		| "shortWithYear"
		| "withYear"
		| "monthDay" = "short",
) => {
	const { year, month, day } = parse(date);
	const monthName = MONTHS[month - 1]!;
	const weekdayName = WEEKDAYS[weekdayOf(date)]!;
	if (style === "long") {
		return `${weekdayName}, ${monthName} ${day}`;
	}
	if (style === "full") {
		return `${weekdayName}, ${monthName} ${day}, ${year}`;
	}
	if (style === "shortWithYear") {
		return `${weekdayName.slice(0, 3)}, ${monthName.slice(0, 3)} ${day}, ${year}`;
	}
	if (style === "withYear") {
		return `${monthName.slice(0, 3)} ${day}, ${year}`;
	}
	if (style === "monthDay") {
		return `${monthName.slice(0, 3)} ${day}`;
	}
	return `${weekdayName.slice(0, 3)}, ${monthName.slice(0, 3)} ${day}`;
};

// "Jan 13" or "Jan 13–15" or "Jan 30–Feb 2" for a span of nights.
export const formatDateRange = (from: string, to: string) => {
	if (from === to) {
		return formatGameDate(from, "monthDay");
	}
	const a = parse(from);
	const b = parse(to);
	if (a.month === b.month) {
		return `${formatGameDate(from, "monthDay")}–${b.day}`;
	}
	return `${formatGameDate(from, "monthDay")}–${formatGameDate(to, "monthDay")}`;
};

// "November 2025", or "Nov" short, for the month a date is in.
export const formatMonth = (date: string, short?: boolean) => {
	const { year, month } = parse(date);
	const monthName = MONTHS[month - 1]!;
	return short ? monthName.slice(0, 3) : `${monthName} ${year}`;
};

// The 2026 season as the NBA writes it, "2025-26".
export const formatSeasonSpan = (season: number) =>
	`${seasonStartYear(season)}-${String(season).slice(-2)}`;
