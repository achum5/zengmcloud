import type { PlayThroughDays } from "../../../common/playThroughInjuriesFactor.ts";
import {
	type getInjuryClock,
	playThroughDays,
	playThroughRate,
} from "./classicDays.ts";
import { calendarDaysUntil } from "./refreshCalendarDays.ts";

// THE USER'S HURT PLAYERS IN A GAME AHEAD, on the NBA calendar: for the
// lines, team ratings and lineups shown before a game, judged as the game
// will judge them that night (injuryAtGame) - by the days they'll have left
// then, against the setting in days the user's slider shows.
export type CalendarPlayThrough = {
	byDays: (
		t: {
			tid: number;
			playThroughInjuries: [number, number];
			playThroughInjuryDays?: [number, number];
		},
		playoffs: boolean,
	) => PlayThroughDays | undefined;
	// Calendar days counted down before a scheduled game.
	daysUntil: (gid: number | undefined) => number;
};

// Undefined off the calendar. `now`: getInjuryClock's.
export const calendarPlayThrough = (
	schedule: readonly { gid: number; date?: string }[],
	now: Awaited<ReturnType<typeof getInjuryClock>>,
): CalendarPlayThrough | undefined => {
	if (!now) {
		return;
	}
	return {
		byDays: (t, playoffs) =>
			playThroughDays(t, playoffs, playThroughRate(playoffs, now.clock)),
		daysUntil: calendarDaysUntil(schedule, now.lastNight),
	};
};
