import { useState } from "react";
import { PlayButton } from "./PlayButton.tsx";
import { askForHighlights } from "../util/highlightsAsk.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { toWorker } from "../util/toWorker.ts";

// A game's highlights: its saved replay, opened in the live game viewer as
// its key plays. For a game known to have a replay saved. New interface only.
export const GameHighlightsButton = ({
	gid,
	className,
}: {
	gid: number;
	className?: string;
}) => {
	const [loading, setLoading] = useState(false);
	const { newInterface } = useLocal(["newInterface"]);

	const watch = async () => {
		setLoading(true);
		try {
			const playByPlay = await toWorker("main", "getLiveGamePlayByPlay", gid);
			if (Array.isArray(playByPlay) && playByPlay.length > 0) {
				askForHighlights();
				await realtimeUpdate([], helpers.leagueUrl(["live_game"]), {
					gidOneGame: gid,
					playByPlay,
					replay: true,
					fromAction: true,
				});
			}
		} finally {
			setLoading(false);
		}
	};

	if (!newInterface) {
		return null;
	}

	return (
		<PlayButton
			className={className}
			onClick={watch}
			disabled={loading}
			title="Highlights"
		/>
	);
};
