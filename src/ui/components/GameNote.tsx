import { useState } from "react";
import { type RecapLink } from "../util/linkifyRecap.ts";
import { RecapBanner } from "./RecapBanner.tsx";
import {
	isGameNoteExpanded,
	setGameNoteExpanded,
} from "../util/dailyScheduleUiState.ts";

// A game's note (AI recap): under its card on the Daily Schedule, and under the
// score on its box score. Whether it's open is remembered across in-app
// navigation, keyed by gid, so a recap opened in one place is open in the other.
//
// `flow` picks how the body opens - overlay (default, under a schedule card,
// where sitting ON TOP keeps the cards from shifting) or pushing content down
// (on the box score, where it's full width above the whole stats table and an
// overlay would bury it).
export const GameNote = ({
	gid,
	note,
	links,
	flow,
}: {
	gid: number;
	note: string;
	links: RecapLink[];
	flow?: boolean;
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
			flow={flow}
		/>
	);
};

export default GameNote;
