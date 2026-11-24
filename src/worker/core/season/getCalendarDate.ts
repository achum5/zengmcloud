import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";

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

// The season's opening night on the calendar: its first night of games,
// played or still to come (the scores bar starts there, the user's team
// playing that night or not). Undefined on classic numbered days.
export const getOpeningNight = async () => {
	const season = g.get("season");
	let first: string | undefined;
	const see = (date: string | undefined) => {
		if (date !== undefined && (first === undefined || date < first)) {
			first = date;
		}
	};
	for (const game of await idb.cache.games.getAll()) {
		if (game.season === season) {
			see(game.date);
		}
	}
	for (const game of await idb.cache.schedule.getAll()) {
		see(game.date);
	}
	return first;
};

export default getCalendarDate;
