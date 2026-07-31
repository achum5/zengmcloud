import { Markdown } from "../../components/Markdown.tsx";
import { ResponsivePopover } from "../../components/ResponsivePopover.tsx";
import { linkifyRecap, type RecapLink } from "../../util/linkifyRecap.ts";
import type { SeasonNoteSection } from "../../../common/seasonNote.ts";

// A season's writeup, opened from that season's row in the stats table.
//
// A career note is one long stack of "[YYYY]" sections, and reading it meant
// scrolling a fixed-height box on the player page hunting for the year you
// wanted. The stats table already lists every season he played, in order, so
// the row you're looking at is the natural place to hang its story off.
export const SeasonNoteButton = ({
	links,
	sections,
	season,
}: {
	// Names to link, scoped to this season.
	links: RecapLink[];
	// Everything written about this year, newest first (a retirement writeup
	// sits above that year's season recap).
	sections: SeasonNoteSection[];
	season: number;
}) => {
	const body = (
		<div
			className="text-wrap player-note-compact"
			style={{ maxHeight: "24em", overflowY: "auto" }}
		>
			{sections.map((section, i) => (
				<div className={i > 0 ? "mt-3" : undefined} key={i}>
					{section.headline ? (
						<div className="fw-bold mb-1">
							{section.kind === "retirement"
								? `Retirement — ${section.headline}`
								: section.headline}
						</div>
					) : null}
					<Markdown>{linkifyRecap(section.body, links)}</Markdown>
				</div>
			))}
		</div>
	);

	return (
		<ResponsivePopover
			id={`season-note-${season}`}
			modalHeader={season}
			modalBody={body}
			popoverContent={
				<div style={{ minWidth: 260, maxWidth: 420 }}>{body}</div>
			}
			// No stopPropagation here: OverlayTrigger listens on the wrapper it puts
			// AROUND this button, so swallowing the event would mean the popover
			// never opens. The stats table only toggles a row when the click landed
			// on the cell itself (see DataTable's Row), so a button is already safe.
			renderTarget={({ forwardedRef, onClick }) => (
				<button
					aria-label={`Read the ${season} writeup`}
					className="btn btn-link p-0 border-0 align-baseline text-decoration-none ms-1"
					onClick={() => {
						onClick?.();
					}}
					ref={forwardedRef as any}
					title={`Read the ${season} writeup`}
					type="button"
				>
					<span className="glyphicon glyphicon-triangle-right" />
				</button>
			)}
		/>
	);
};
