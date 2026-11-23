import { idb } from "../../db/index.ts";

// TODAY ON THE LEAGUE'S CALENDAR: the night of the next games, or undefined
// on classic numbered days and between seasons.
const getCalendarDate = async () => {
	let day = Infinity;
	let date: string | undefined;
	for (const game of await idb.cache.schedule.getAll()) {
		if (game.day < day) {
			day = game.day;
			date = game.date;
		} else if (game.day === day && date === undefined) {
			date = game.date;
		}
	}
	return date;
};

export default getCalendarDate;
