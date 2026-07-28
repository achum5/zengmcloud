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
		  }
		| {
				initialNote?: undefined;
				note: string | undefined;
				info: NoteInfo;
				infoLink?: boolean;
				xs?: boolean;
				autoLink?: RecapLink[];
				autoLinkBySeason?: (season: number | undefined) => RecapLink[];
		  },
) => {
	const { initialNote, note, info, infoLink, xs, autoLink, autoLinkBySeason } =
		props;

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

	const noteToShow = Object.hasOwn(props, "initialNote") ? editedNote : note;

	if (noteToShow === undefined || noteToShow === "") {
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
					<Markdown>{linkifySeasonNote(noteToShow, autoLinkBySeason)}</Markdown>
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
