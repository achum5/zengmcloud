import { type MouseEvent, useLayoutEffect, useRef, useState } from "react";
import clsx from "clsx";
import { Markdown } from "./Markdown.tsx";
import {
	linkifyRecap,
	linkRecapSegments,
	type RecapLink,
	type SentenceGame,
} from "../util/linkifyRecap.ts";

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
	sentenceGames,
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
	// The day's completed games, when this is a DAY recap: each body sentence
	// that resolves to exactly one of them links to that game's box score
	// (underlining whole on hover). The headline is left out - clicking it
	// toggles the note, and a headline that also navigated would fight that.
	sentenceGames?: SentenceGame[];
}) => {
	const text = note.trim();

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

	// ROOM TO READ IT. The overlay sits on top of the cards below, which is
	// what keeps the page from jumping - but under the LAST game on the page
	// there is nothing below it to cover, and the body ran off the end of the
	// document, behind the ticker, where no amount of scrolling reached it.
	// When it would reach the end, it opens in the page instead and pushes
	// what little is below down, so the whole recap can be scrolled to.
	const bodyRef = useRef<HTMLDivElement>(null);
	const [pushDown, setPushDown] = useState(false);
	useLayoutEffect(() => {
		if (!open || flow) {
			setPushDown(false);
			return;
		}
		const el = bodyRef.current;
		if (!el || typeof window === "undefined") {
			return;
		}
		// Measured as an overlay, so the decision does not feed back on itself.
		const bottom = el.getBoundingClientRect().bottom + window.scrollY;
		const pageEnd = document.documentElement.scrollHeight;
		// Anything fixed along the bottom of the screen (the ticker) hides
		// the last stretch of the page, so leave it a margin.
		setPushDown(bottom > pageEnd - 120);
	}, [open, flow]);

	if (text === "") {
		return null;
	}

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
				"game-note-push": pushDown && !flow,
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
				<div
					ref={bodyRef}
					className={clsx("game-note-body", { open })}
					aria-hidden={!open}
				>
					<Markdown
						linkSegments={
							sentenceGames && sentenceGames.length > 0
								? (text) => linkRecapSegments(text, sentenceGames)
								: undefined
						}
					>
						{body}
					</Markdown>
				</div>
			) : null}
		</div>
	);
};
