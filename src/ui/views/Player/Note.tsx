import { useState } from "react";
import { helpers } from "../../util/helpers.ts";
import { toWorker } from "../../util/toWorker.ts";
import clsx from "clsx";
import { Markdown } from "../../components/Markdown.tsx";
import { linkifyRecap, type RecapLink } from "../../util/linkifyRecap.ts";

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
		  }
		| {
				initialNote?: undefined;
				note: string | undefined;
				info: NoteInfo;
				infoLink?: boolean;
				xs?: boolean;
				autoLink?: RecapLink[];
		  },
) => {
	const { initialNote, note, info, infoLink, xs, autoLink } = props;

	const [editing, setEditing] = useState(false);
	const [editedNote, setEditedNote] = useState(initialNote ?? note ?? "");

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
				{info.type === "game" ? (
					// Game notes double as AI recaps (markdown). Auto-link team/player
					// names to their pages, scoped to this game's two rosters. Applied
					// only to the rendered view - the stored/edited text stays plain.
					<Markdown>
						{autoLink && autoLink.length > 0
							? linkifyRecap(noteToShow, autoLink)
							: noteToShow}
					</Markdown>
				) : (
					<div style={{ whiteSpace: "pre-line" }}>{noteToShow}</div>
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
