import { useState } from "react";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import {
	countPlayerHighlights,
	filterPlayerHighlights,
} from "../util/filterPlayerHighlights.ts";

// Per-game "Highlights" button: routes the game's saved live play-by-play,
// filtered to this player's positive plays, through the normal live-game viewer
// (which pauses on each highlight). Shown only when a saved replay exists.
// Shared by the player game log and the box score player-stats table.
export const HighlightsButton = ({
	gid,
	pid,
}: {
	gid: number;
	pid: number;
}) => {
	const [loading, setLoading] = useState(false);
	const [note, setNote] = useState<string | undefined>();

	const watch = async () => {
		setLoading(true);
		setNote(undefined);
		try {
			const playByPlay = await toWorker("main", "getLiveGamePlayByPlay", gid);
			if (!Array.isArray(playByPlay) || playByPlay.length === 0) {
				setNote("No replay");
				return;
			}
			if (countPlayerHighlights(playByPlay, pid) === 0) {
				setNote("No highlights");
				return;
			}
			await realtimeUpdate([], helpers.leagueUrl(["live_game"]), {
				gidOneGame: gid,
				playByPlay: filterPlayerHighlights(playByPlay, pid),
				replay: true,
				fromAction: true,
			});
		} finally {
			setLoading(false);
		}
	};

	return (
		<>
			<button
				type="button"
				className="btn btn-secondary btn-sm p-0 px-1 ms-2"
				onClick={watch}
				disabled={loading}
				title="Watch this player's highlights from this game"
			>
				<span className="glyphicon glyphicon-film" />
			</button>
			{note ? (
				<span className="text-body-secondary small ms-1">{note}</span>
			) : null}
		</>
	);
};

export default HighlightsButton;
