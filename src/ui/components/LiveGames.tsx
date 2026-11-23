import clsx from "clsx";
import type { MpLiveGame } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { local, useLocal } from "../util/local.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { toWorker } from "../util/toWorker.ts";

// Games being played live in a shared league, as every page shows them: LIVE,
// the score and the clock as they stand on the screen playing it, and a way in.

// Into a game being played live, at the moment it's at. A league mate's goes
// through the room; this device's own is already on its live game page.
export const watchLiveGame = (gid: number) => {
	if (local.getState().mpLiveGames.some((game) => game.gid === gid)) {
		void toWorker("main", "watchLiveBroadcast", gid);
	} else {
		void realtimeUpdate([], helpers.leagueUrl(["live_game"]));
	}
};

export const useLiveGame = (gid: number): MpLiveGame | undefined => {
	const { mpLiveGames } = useLocal(["mpLiveGames"]);
	return mpLiveGames.find((game) => game.gid === gid);
};

const clockText = (game: MpLiveGame | undefined) => {
	if (!game) {
		return "";
	}
	if (game.paused) {
		return "Paused";
	}
	const score = game.score;
	if (!score) {
		return "";
	}
	return `${score.q} ${score.clock}`.trim();
};

// Under a game on a schedule: LIVE and where it's at.
export const LiveGameStatus = ({
	className,
	gid,
}: {
	className?: string;
	gid: number;
}) => {
	const game = useLiveGame(gid);
	const text = clockText(game);
	return (
		<div className={clsx("d-flex align-items-center gap-2", className)}>
			<span className="badge text-bg-danger">LIVE</span>
			{text ? (
				<span className="small text-body-secondary text-nowrap">{text}</span>
			) : null}
		</div>
	);
};

// The action on a game's score box.
export const watchLiveAction = (gid: number, byName?: string) => ({
	highlight: true,
	title: byName ? `${byName} is simming this game live` : undefined,
	text: (
		<>
			Watch
			<br />
			live
		</>
	),
	onClick: () => {
		watchLiveGame(gid);
	},
});

// In the header: every league mate's game being played live, its score and
// clock, tap to watch. There only while something is live: in the top bar on
// a wide screen, a thin row under it on anything narrower (where the bar has
// no room, and the sidebar isn't docked below it).
export const LiveScoresStrip = ({ className }: { className?: string }) => {
	const { mpLiveGames, teamInfoCache } = useLocal([
		"mpLiveGames",
		"teamInfoCache",
	]);

	if (mpLiveGames.length === 0) {
		return null;
	}

	return (
		<div
			className={clsx("live-scores-strip gap-2 overflow-auto", className)}
		>
			{mpLiveGames.map((game) => {
				const tids = game.tids;
				const abbrev = (i: 0 | 1) =>
					tids ? (teamInfoCache[tids[i]]?.abbrev ?? "") : "";
				const pts = game.score?.pts;
				const clock = clockText(game);
				const watchTitle = `${game.byName} is simming ${
					game.label || "this game"
				} live`;
				return (
					<button
						key={game.gid}
						type="button"
						className={clsx(
							"live-score-chip btn btn-sm text-nowrap d-flex align-items-center gap-2",
							game.watching ? "btn-danger" : "btn-outline-danger",
						)}
						title={watchTitle}
						onClick={() => {
							watchLiveGame(game.gid);
						}}
					>
						<span className="live-score-dot" aria-hidden />
						{tids ? (
							<span>
								{abbrev(1)} <b>{pts?.[1] ?? 0}</b>
								<span className="mx-1">@</span>
								{abbrev(0)} <b>{pts?.[0] ?? 0}</b>
							</span>
						) : (
							<span>{game.label || "Live game"}</span>
						)}
						{clock ? <span className="live-score-clock">{clock}</span> : null}
					</button>
				);
			})}
		</div>
	);
};
