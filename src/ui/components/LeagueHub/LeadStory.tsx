import { useState } from "react";
import { Markdown } from "../Markdown.tsx";
import {
	linkifyRecap,
	linkRecapSegments,
	type RecapLink,
	type SentenceGame,
} from "../../util/linkifyRecap.ts";
import {
	isDayNoteExpanded,
	setDayNoteExpanded,
} from "../../util/dailyScheduleUiState.ts";

// THE NIGHT'S STORY, at the top of the hub: the night's written recap as an
// article - its headline, the first blocks, and the rest behind "Keep
// reading". (The night is picked in the strip over it, so no date here.)

const SHOWN = 2;

export const LeadStory = ({
	day,
	links,
	note,
	season,
	sentenceGames,
}: {
	day: number;
	links: RecapLink[];
	note: string;
	season: number;
	sentenceGames: SentenceGame[];
}) => {
	const [expanded, setExpandedState] = useState(() =>
		isDayNoteExpanded(season, day),
	);
	const setExpanded = (value: boolean) => {
		setExpandedState(value);
		setDayNoteExpanded(season, day, value);
	};

	// The headline plain, as a paper prints it; the links are in the story.
	const plain = note.trim().split("\n");
	const headIndex = plain.findIndex((line) => line.trim() !== "");
	const headline = (plain[headIndex] ?? "").replaceAll("**", "");
	const lines = linkifyRecap(note.trim(), links).split("\n");
	const blocks = lines
		.slice(headIndex + 1)
		.join("\n")
		.split(/\n\s*\n/)
		.map((p) => p.trim())
		.filter((p) => p !== "");
	const shown = expanded ? blocks : blocks.slice(0, SHOWN);
	const linkSegments =
		sentenceGames.length > 0
			? (text: string) => linkRecapSegments(text, sentenceGames)
			: undefined;

	return (
		<article className="lh-lead lh-article">
			<h2 className="lh-headline">
				<Markdown>{headline}</Markdown>
			</h2>
			<div className="lh-body">
				<Markdown linkSegments={linkSegments}>{shown.join("\n\n")}</Markdown>
			</div>
			{blocks.length > SHOWN ? (
				<button
					type="button"
					className="btn btn-link p-0 lh-more"
					onClick={() => setExpanded(!expanded)}
				>
					{expanded ? "Show less" : "Keep reading"}
				</button>
			) : null}
		</article>
	);
};
