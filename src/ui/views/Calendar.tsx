import type { View } from "../../common/types.ts";
import { MonthCalendar } from "../components/MonthCalendar.tsx";
import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { useLocal } from "../util/local.ts";

// THE LEAGUE'S CALENDAR: a month at a time, for a team or the whole league.

const Calendar = ({
	games,
	keyDates,
	nbaCalendar,
	season,
	tid,
	today,
	undated,
}: View<"calendar">) => {
	useTitleBar({
		title: "Calendar",
		dropdown: {
			fields: [
				{ key: "team", options: { type: "teams", all: true }, value: tid },
				{ key: "season", options: { type: "seasons" }, value: season },
			],
			view: "calendar",
		},
	});

	const { season: currentSeason } = useLocal(["season"]);

	const moreLinks =
		typeof tid === "number" ? (
			<MoreLinks type="team" page="calendar" tid={tid} season={season} />
		) : (
			<MoreLinks type="schedule" page="calendar" />
		);

	if (games.length === 0 && keyDates.length === 0) {
		return (
			<>
				{moreLinks}
				<p>
					{!nbaCalendar
						? "This league isn't on the NBA calendar."
						: season !== currentSeason
							? "No dates this season."
							: undated
								? "Dates start next season."
								: "No games scheduled yet."}
				</p>
			</>
		);
	}

	return (
		<>
			{moreLinks}
			<MonthCalendar
				games={games}
				keyDates={keyDates}
				monthChips
				season={season}
				tid={tid}
				today={today}
			/>
		</>
	);
};

export default Calendar;
