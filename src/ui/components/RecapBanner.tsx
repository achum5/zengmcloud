import { type MouseEvent } from "react";
import clsx from "clsx";
import { Markdown } from "./Markdown.tsx";
import { linkifyRecap, type RecapLink } from "../util/linkifyRecap.ts";

// The shared presentation for an AI recap on the Daily Schedule: the first line
// is the always-visible headline with a toggle arrow; clicking it expands the
// rest. Renders markdown and auto-links team/player names. Expansion state is
// owned by the caller (so a game keys it by gid, the day recap by season+day).
//
// `flow` controls how the body opens: overlay (default, for the per-game notes
// under each card, which sit ON TOP of what's below so cards don't shift) or in
// normal flow (for the full-width day recap at the top of the page, where there's
// nothing above to hide and pushing the schedule down reads better).
export const RecapBanner = ({
	note,
	links,
	expanded,
	onToggle,
	flow,
	centered,
}: {
	note: string;
	links: RecapLink[];
	expanded: boolean;
	onToggle: (value: boolean) => void;
	flow?: boolean;
	// Centered headline with no expand glyph, for the box score - it sits under
	// a centered score and reads as that game's headline rather than as a list
	// item. `flow` as well; this only restyles it.
	centered?: boolean;
}) => {
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

	// Click the header to toggle either way; ignore clicks on the auto-links so
	// tapping a name navigates instead of collapsing.
	const onHeaderClick = (event: MouseEvent) => {
		if ((event.target as HTMLElement).closest("a")) {
			return;
		}
		if (hasMore) {
			onToggle(!expanded);
		}
	};

	return (
		<div
			className={clsx("game-note small position-relative", {
				open,
				"game-note-flow": flow,
				"game-note-centered": centered,
			})}
		>
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
							onToggle(!expanded);
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
