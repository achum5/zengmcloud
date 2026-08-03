import type { ReactNode } from "react";
import { Markdown } from "./Markdown.tsx";
import { ResponsivePopover } from "./ResponsivePopover.tsx";
import { linkifyRecap, type RecapLink } from "../util/linkifyRecap.ts";
import type { SeasonNoteSection } from "../../common/seasonNote.ts";

// A speech bubble that opens a piece of a player's writeup, hung off whatever
// the piece is about: a season's row in the stats or ratings table, his row on
// a roster, the draft line in his bio, his selection on the draft history page.
//
// It reads as "there is something written in here" - an arrow only said "this
// expands", which on a table full of numbers looks like another sort control.
//
// A career note is one long stack of "[YYYY]" sections, and reading it used to
// mean scrolling a fixed-height box hunting for the year you wanted. The rows
// are already in order on the page, so the row you are looking at is the
// natural place to hang its story off.
export const SeasonNoteButton = ({
	header,
	id,
	linksFor,
	sections,
	title,
}: {
	// Modal header - the season for a season writeup, "Draft" for the pick.
	header: ReactNode;
	// Unique on the page. The draft line and the draft season's ratings row are
	// both filed under the same year, so the season alone isn't enough.
	id: string;
	// Names to link, resolved per section so a 2001 section links to how teams
	// looked in 2001.
	linksFor: (season: number | undefined) => RecapLink[];
	// Everything to show, newest first.
	sections: SeasonNoteSection[];
	title: string;
}) => {
	const body = (
		<div
			className="text-wrap player-note-compact"
			style={{ maxHeight: "24em", overflowY: "auto" }}
		>
			{sections.map((section, i) => (
				<div className={i > 0 ? "mt-3" : undefined} key={i}>
					{section.headline ? (
						<div className="fw-bold mb-1">{section.headline}</div>
					) : null}
					<Markdown>
						{linkifyRecap(section.body, linksFor(section.season))}
					</Markdown>
				</div>
			))}
		</div>
	);

	return (
		<ResponsivePopover
			id={id}
			modalHeader={header}
			modalBody={body}
			popoverContent={
				<div style={{ minWidth: 260, maxWidth: 420 }}>{body}</div>
			}
			// No stopPropagation here: OverlayTrigger listens on the wrapper it puts
			// AROUND this button, so swallowing the event would mean the popover
			// never opens. A stats table only toggles a row when the click landed on
			// the cell itself (see DataTable's Row), so a button is already safe.
			renderTarget={({ forwardedRef, onClick }) => (
				<button
					aria-label={title}
					className="btn btn-link p-0 border-0 align-baseline text-decoration-none ms-1"
					onClick={() => {
						onClick?.();
					}}
					ref={forwardedRef as any}
					title={title}
					type="button"
				>
					<span className="glyphicon glyphicon-comment" />
				</button>
			)}
		/>
	);
};
