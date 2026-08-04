import { useState } from "react";
import { helpers } from "../../util/helpers.ts";
import { toWorker } from "../../util/toWorker.ts";
import clsx from "clsx";
import { Markdown } from "../../components/Markdown.tsx";
import {
	linkifyRecap,
	linkifySeasonNote,
	type RecapLink,
} from "../../util/linkifyRecap.ts";
import { GameNote } from "../../components/GameNote.tsx";

const MAX_WIDTH = 600;

export type NoteInfo =
	| {
			type: "draftPick";
			dpid: number;
	  }
	| {
			type: "game";
			gid: number;
	  }
	| {
			type: "player";
			pid: number;
	  }
	| {
			type: "teamSeason";
			tid: number;
			season: number;
	  }
	| {
			// A whole-league-day recap. Stored on the day's anchor game (see
			// Game.dayNote); addressed by (season, day).
			type: "day";
			season: number;
			day: number;
	  };

const Note = (
	props:
		| {
				initialNote: string | undefined;
				note?: undefined;
				info: NoteInfo;
				infoLink?: boolean;
				xs?: boolean;
				// For game notes (AI recaps): names to auto-link, scoped to the game.
				autoLink?: RecapLink[];
				autoLinkBySeason?: (season: number | undefined) => RecapLink[];
				// Show a game note as the headline-plus-dropdown banner the Daily
				// Schedule uses, instead of a scrolling block of markdown. Same
				// component and the same per-gid open state, so a recap opened in one
				// place is open in the other.
				banner?: boolean;
				// What to RENDER, when that differs from what gets EDITED. The player
				// page shows only the part of a career note that isn't already
				// reachable from a season row in the stats table, but the editor has
				// to keep the whole thing or saving would delete the rest.
				displayNote?: string;
				hideSeasonLabels?: boolean;
				// Just the button (and the editor it opens) - no copy of the note.
				// For a page that already shows the note somewhere else and only wants
				// the way in to edit it, like the box score, where the recap sits under
				// the score and this stays at the foot of the page.
				editOnly?: boolean;
		  }
		| {
				initialNote?: undefined;
				note: string | undefined;
				info: NoteInfo;
				infoLink?: boolean;
				xs?: boolean;
				autoLink?: RecapLink[];
				autoLinkBySeason?: (season: number | undefined) => RecapLink[];
				banner?: boolean;
				editOnly?: boolean;
				displayNote?: string;
				hideSeasonLabels?: boolean;
		  },
) => {
	const {
		initialNote,
		note,
		info,
		infoLink,
		xs,
		autoLink,
		autoLinkBySeason,
		banner,
		editOnly,
		displayNote,
		hideSeasonLabels,
	} = props;

	const [editing, setEditing] = useState(false);
	const [editedNote, setEditedNote] = useState(initialNote ?? note ?? "");

	// Keep the displayed note in sync when the underlying note changes out from
	// under us - e.g. it was edited on another device and synced in, or the view
	// reloaded with fresh data. Without this, `editedNote` is frozen at its first
	// value (it only otherwise refreshes when the component is remounted via its
	// `key`), so a synced note never appears. Never clobber an in-progress edit.
	const externalNote = initialNote ?? note ?? "";
	const [syncedNote, setSyncedNote] = useState(externalNote);
	if (externalNote !== syncedNote && !editing) {
		setSyncedNote(externalNote);
		setEditedNote(externalNote);
	}

	if (editing) {
		return (
			<form
				onSubmit={async (event) => {
					event.preventDefault();
					await toWorker("main", "setNote", {
						...info,
						editedNote,
					});
					setEditing(false);
				}}
			>
				<textarea
					className="form-control"
					rows={5}
					onChange={(event) => {
						setEditedNote(event.target.value);
					}}
					style={{ maxWidth: MAX_WIDTH }}
					value={editedNote}
				/>

				<div className="mt-2 d-flex gap-2" style={{ maxWidth: MAX_WIDTH }}>
					<button type="submit" className="btn btn-primary btn-sm">
						Save
					</button>
					<button
						type="reset"
						className="btn btn-light-bordered btn-sm"
						onClick={async () => {
							setEditing(false);
						}}
					>
						Cancel
					</button>
					{infoLink ? (
						<div className="ms-auto">
							<a href={helpers.leagueUrl(["notes", info.type])}>View all</a>
						</div>
					) : null}
				</div>
			</form>
		);
	}

	const name =
		info.type === "draftPick"
			? "draft pick"
			: info.type === "game"
				? "game"
				: info.type === "player"
					? "player"
					: "team";

	const fullNote = Object.hasOwn(props, "initialNote") ? editedNote : note;
	// `displayNote` only ever narrows what's shown, so an empty string from it is
	// meaningful (nothing left to show here) while undefined means "show it all".
	const noteToShow = displayNote ?? fullNote;

	if (fullNote === undefined || fullNote === "") {
		return (
			<button
				type="button"
				className={clsx("btn btn-light-bordered", xs ? "btn-xs" : "btn-sm")}
				onClick={() => {
					setEditing(true);
				}}
			>
				Add {name} note
			</button>
		);
	}

	// Empty notes already returned the "Add" button above, so this is only ever
	// the edit path. `noteToShow` empty with a non-empty note means every section
	// is being shown somewhere else (a career whose writeups all sit on their
	// season rows) - there's nothing to print, but there is still plenty to edit.
	if (editOnly || noteToShow === undefined || noteToShow === "") {
		return (
			<button
				type="button"
				className={clsx("btn btn-light-bordered", xs ? "btn-xs" : "btn-sm")}
				onClick={() => {
					setEditing(true);
				}}
			>
				Edit {name} note
			</button>
		);
	}

	if (banner && info.type === "game") {
		return (
			<GameNote
				gid={info.gid}
				note={noteToShow}
				links={autoLink ?? []}
				flow
				centered
			/>
		);
	}

	return (
		<>
			<div
				className={"overflow-auto small-scrollbar"}
				style={{ maxHeight: 300, maxWidth: MAX_WIDTH }}
			>
				{/* All notes render markdown (player / team-season / game notes double
				    as AI writeups). When the caller supplies an autoLink map (game
				    notes scoped to that game's rosters, team-season notes scoped to the
				    league's teams + that season's roster), team/player names are linked
				    to their pages. A PLAYER note is a stack of "[YYYY]" sections rather
				    than one piece of writing, so it passes autoLinkBySeason instead and
				    each section is linked against its own year. Linking/rendering is
				    applied only to the view - the stored/edited text stays plain. */}
				{autoLinkBySeason ? (
					<Markdown>
						{linkifySeasonNote(noteToShow, autoLinkBySeason, hideSeasonLabels)}
					</Markdown>
				) : autoLink && autoLink.length > 0 ? (
					<Markdown>{linkifyRecap(noteToShow, autoLink)}</Markdown>
				) : (
					<Markdown>{noteToShow}</Markdown>
				)}
			</div>
			<button
				type="button"
				className="btn btn-light-bordered btn-sm mt-2"
				onClick={() => {
					setEditing(true);
				}}
			>
				Edit {name} note
			</button>
		</>
	);
};

export default Note;
