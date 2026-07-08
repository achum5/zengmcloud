import { type MouseEvent, useState } from "react";
import { Markdown } from "./Markdown.tsx";
import { linkifyRecap, type RecapLink } from "../util/linkifyRecap.ts";

// A game's note (AI recap) shown under its card on the Daily Schedule. Collapsed
// to just the headline (the note's first line) with a toggle arrow; expanded it
// shows the whole note. Renders markdown and auto-links team/player names, the
// same as the note on the box-score page. Read-only here - editing stays on the
// box score.
export const GameNote = ({
	note,
	links,
}: {
	note: string;
	links: RecapLink[];
}) => {
	const [expanded, setExpanded] = useState(false);

	const text = note.trim();
	if (text === "") {
		return null;
	}

	const linked = links.length > 0 ? linkifyRecap(text, links) : text;
	const headline = linked.split("\n").find((line) => line.trim() !== "") ?? linked;
	const hasMore = linked.trim() !== headline.trim();

	// Toggle on a click that isn't on one of the auto-links (so tapping a name
	// navigates instead of expanding/collapsing).
	const toggle = (event: MouseEvent) => {
		if ((event.target as HTMLElement).closest("a")) {
			return;
		}
		setExpanded((value) => !value);
	};

	return (
		<div className="border rounded p-2 mt-1">
			<div className="d-flex align-items-start gap-2">
				<button
					type="button"
					className="btn btn-link p-0 text-decoration-none text-body-secondary"
					style={{ lineHeight: 1.2 }}
					onClick={() => setExpanded((value) => !value)}
					title={expanded ? "Hide note" : "Show note"}
					aria-expanded={expanded}
				>
					{hasMore ? (expanded ? "▾" : "▸") : "•"}
				</button>
				{expanded ? (
					<div className="flex-grow-1">
						<Markdown>{linked}</Markdown>
					</div>
				) : (
					<div
						className="flex-grow-1"
						style={{ cursor: hasMore ? "pointer" : undefined }}
						onClick={hasMore ? toggle : undefined}
					>
						<Markdown>{headline}</Markdown>
					</div>
				)}
			</div>
		</div>
	);
};

export default GameNote;
