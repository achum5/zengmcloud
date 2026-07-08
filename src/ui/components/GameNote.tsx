import { type MouseEvent, useState } from "react";
import clsx from "clsx";
import { Markdown } from "./Markdown.tsx";
import { linkifyRecap, type RecapLink } from "../util/linkifyRecap.ts";
import {
	isGameNoteExpanded,
	setGameNoteExpanded,
} from "../util/dailyScheduleUiState.ts";

// A game's note (AI recap) attached to the bottom of its card on the Daily
// Schedule. The headline (the note's first line) is always shown with a toggle
// arrow on the right; clicking the headline (or the arrow) expands/collapses the
// rest of the note. The expanded body drops down as an overlay banner - it sits
// ON TOP of whatever is below instead of pushing the page down - and slides in.
// Renders markdown and auto-links team/player names, the same as the note on the
// box-score page. Read-only here; whether it's open is remembered across in-app
// navigation.
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

	const text = note.trim();
	if (text === "") {
		return null;
	}

	const linked = links.length > 0 ? linkifyRecap(text, links) : text;
	const lines = linked.split("\n");
	const headlineIdx = lines.findIndex((line) => line.trim() !== "");
	const headline = headlineIdx >= 0 ? lines[headlineIdx]! : linked;
	const body =
		headlineIdx >= 0
			? lines
					.slice(headlineIdx + 1)
					.join("\n")
					.trim()
			: "";
	const hasMore = body !== "";
	const open = expanded && hasMore;

	const setExpanded = (value: boolean) => {
		setExpandedState(value);
		setGameNoteExpanded(gid, value);
	};

	// Click the header to toggle either way; ignore clicks on the auto-links so
	// tapping a name navigates instead of collapsing.
	const onHeaderClick = (event: MouseEvent) => {
		if ((event.target as HTMLElement).closest("a")) {
			return;
		}
		if (hasMore) {
			setExpanded(!expanded);
		}
	};

	return (
		<div className={clsx("game-note small position-relative", { open })}>
			<div
				className="game-note-header d-flex align-items-center gap-2"
				style={{ cursor: hasMore ? "pointer" : undefined }}
				onClick={onHeaderClick}
			>
				<div className="flex-grow-1">
					<Markdown>{headline}</Markdown>
				</div>
				{hasMore ? (
					<button
						type="button"
						className="btn btn-link p-0 text-decoration-none text-body-secondary lh-1"
						onClick={(event) => {
							event.stopPropagation();
							setExpanded(!expanded);
						}}
						title={expanded ? "Hide note" : "Show note"}
						aria-expanded={expanded}
					>
						{expanded ? "▾" : "▸"}
					</button>
				) : null}
			</div>
			{hasMore ? (
				<div className={clsx("game-note-body", { open })} aria-hidden={!open}>
					<Markdown>{body}</Markdown>
				</div>
			) : null}
		</div>
	);
};

export default GameNote;
