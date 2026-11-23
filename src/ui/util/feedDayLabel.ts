import { formatGameDate } from "../../common/calendarDates.ts";

// The day a feed post is from: its night on the NBA calendar ("Nov 12"), the
// league day without one ("Day 41"), or "Offseason".
export const feedDayLabel = (
	day: number,
	date: string | undefined,
	style: "short" | "monthDay" = "monthDay",
) =>
	day === 0
		? "Offseason"
		: date !== undefined
			? formatGameDate(date, style)
			: `Day ${day}`;
