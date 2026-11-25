import getCalendarDate from "../core/season/getCalendarDate.ts";

// THE DAY A MOVE HAPPENED, ON THE LEAGUE'S CALENDAR: saved on each event
// (logEvent) while the league is on dates, so a trade or a signing can say
// the day it was made instead of just the season and phase.
//
// Today is the top bar's: the next night on the schedule. A night's own
// moves - the CPU's trades and signings, injuries healed, made in the sim
// after that night's games are off the schedule - are that night's, so the
// sim names it while they're made (duringNight). Between seasons there are
// no dates, and nothing is saved.

let night: string | undefined;

export const duringNight = async <T>(
	date: string | undefined,
	fn: () => Promise<T>,
): Promise<T> => {
	const before = night;
	night = date;
	try {
		return await fn();
	} finally {
		night = before;
	}
};

export const eventDate = async (): Promise<string | undefined> =>
	night ?? (await getCalendarDate());
