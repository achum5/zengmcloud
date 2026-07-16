import { useState } from "react";
import { type RecapLink } from "../util/linkifyRecap.ts";
import { RecapBanner } from "./RecapBanner.tsx";
import {
	isGameNoteExpanded,
	setGameNoteExpanded,
} from "../util/dailyScheduleUiState.ts";

// A game's note (AI recap) attached to the bottom of its card on the Daily
// Schedule. Overlay dropdown (see RecapBanner). Whether it's open is remembered
// across in-app navigation, keyed by gid.
export const GameNote = ({
	gid,
	note,
	links,
}: {
	gid: number;
	note: string;
	links: RecapLink[];
}) => {
	const [expanded, setExpandedState] = useState(() => isGameNoteExpanded(gid));

	const setExpanded = (value: boolean) => {
		setExpandedState(value);
		setGameNoteExpanded(gid, value);
	};

	return (
		<RecapBanner
			note={note}
			links={links}
			expanded={expanded}
			onToggle={setExpanded}
		/>
	);
};

export default GameNote;
