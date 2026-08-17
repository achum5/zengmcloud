import { useState } from "react";
import { type RecapLink, type SentenceGame } from "../util/linkifyRecap.ts";
import { RecapBanner } from "./RecapBanner.tsx";
import {
	isDayNoteExpanded,
	setDayNoteExpanded,
} from "../util/dailyScheduleUiState.ts";

// The "Day in the League" AI recap, shown full-width at the top of the Daily
// Schedule: the same headline + dropdown as a game's note, but opening in normal
// flow (pushing the schedule down) rather than as an overlay. Open state is
// remembered across in-app navigation, keyed by (season, day).
export const DayRecap = ({
	season,
	day,
	note,
	links,
	sentenceGames,
}: {
	season: number;
	day: number;
	note: string;
	links: RecapLink[];
	sentenceGames?: SentenceGame[];
}) => {
	const [expanded, setExpandedState] = useState(() =>
		isDayNoteExpanded(season, day),
	);

	const setExpanded = (value: boolean) => {
		setExpandedState(value);
		setDayNoteExpanded(season, day, value);
	};

	return (
		<div className="mb-3">
			<RecapBanner
				note={note}
				links={links}
				sentenceGames={sentenceGames}
				expanded={expanded}
				onToggle={setExpanded}
				flow
			/>
		</div>
	);
};

export default DayRecap;
