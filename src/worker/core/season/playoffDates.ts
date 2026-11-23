import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { addDays } from "../../../common/calendarDates.ts";

// THE PLAYOFFS ON THE CALENDAR. Every series plays each playoff sim day, as
// ever; the days just get nights spaced like the NBA's: the play-in from the
// Tuesday after the season, a game every other night within a round, a few
// days off before the Finals, and the Finals every two or three nights. Only
// on a season that has a calendar.

export type PlayoffDay =
	| { kind: "playInStart" }
	| { kind: "playInFinal" }
	| { kind: "roundStart"; round: number; finals: boolean; afterPlayIn: boolean }
	| { kind: "game"; finals: boolean; gameIndex: number };

export const gapForPlayoffDay = (day: PlayoffDay) => {
	switch (day.kind) {
		case "playInStart":
			// Regular season ends on a Sunday, the play-in starts Tuesday.
			return 2;
		case "playInFinal":
			return 3;
		case "roundStart":
			if (day.round === 0) {
				return day.afterPlayIn ? 1 : 6;
			}
			return day.finals ? 5 : 2;
		case "game":
			// The Finals: Thursday, Sunday, Wednesday, Friday, Monday, Thursday,
			// Sunday.
			if (day.finals) {
				return day.gameIndex === 3 ? 2 : 3;
			}
			return 2;
	}
};

// The night of the next playoff day, or undefined when this season has no
// calendar.
export const nextPlayoffDate = async (day: PlayoffDay) => {
	const season = g.get("season");
	let last: string | undefined;
	for (const game of await idb.cache.games.getAll()) {
		if (game.season === season && game.date !== undefined) {
			if (last === undefined || game.date > last) {
				last = game.date;
			}
		}
	}
	if (last === undefined) {
		return;
	}
	return addDays(last, gapForPlayoffDay(day));
};
