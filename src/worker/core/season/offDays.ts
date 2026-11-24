import { addDays, daysBetween } from "../../../common/calendarDates.ts";
import { OFF_DAY_TIDS } from "../../../common/specialGames.ts";
import { g } from "../../util/index.ts";

// EVERY DATE IS A DAY. On the NBA calendar a sim day used to be a night with
// games, so a date with none anywhere - Election Day, Christmas Eve, the
// All-Star break, the nights between playoff games - never happened: the
// league went from Monday straight to Wednesday. Now each such date is a row
// of the schedule of its own (OFF_DAY_TIDS, like the trade deadline's), so
// the sim lands on it like any other day: it's today on every page, its end
// of day counts (injuries heal, front offices deal), and the next day is the
// day after.
//
// A season gets them from its schedule's first build (calendarOffDays): one
// started before there were any keeps skipping empty dates to its end.

// This season was built with its empty dates in it.
export const offDaysThisSeason = () =>
	g.get("calendarOffDays") === g.get("season");

// A dated schedule (in date order) with a row for each date between its
// first and last with nothing on it.
export const withOffDays = (
	tids: [number, number][],
	dates: string[],
): { tids: [number, number][]; dates: string[] } => {
	const outTids: [number, number][] = [];
	const outDates: string[] = [];
	let prev: string | undefined;
	for (const [i, date] of dates.entries()) {
		if (prev !== undefined && date > prev) {
			for (let n = 1; n < daysBetween(prev, date); n++) {
				outTids.push([...OFF_DAY_TIDS]);
				outDates.push(addDays(prev, n));
			}
		}
		outTids.push(tids[i]!);
		outDates.push(date);
		if (prev === undefined || date > prev) {
			prev = date;
		}
	}
	return { tids: outTids, dates: outDates };
};

// In the playoffs, one night is scheduled at a time. The dates before it not
// yet lived - from the day after the last night played (or the first row
// still on the schedule, if the league is on one of them) - are its off days.
export const playoffOffDates = ({
	lastPlayed,
	firstLeft,
	firstNew,
}: {
	// The night of the season's last game played.
	lastPlayed: string | undefined;
	// The earliest date still on the schedule, before this one replaces it.
	firstLeft: string | undefined;
	// The earliest night of the new schedule.
	firstNew: string;
}) => {
	if (lastPlayed === undefined) {
		return [];
	}
	let from = addDays(lastPlayed, 1);
	if (firstLeft !== undefined && firstLeft > from) {
		from = firstLeft;
	}
	const out: string[] = [];
	for (let date = from; date < firstNew; date = addDays(date, 1)) {
		out.push(date);
	}
	return out;
};
