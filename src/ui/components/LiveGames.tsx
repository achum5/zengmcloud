import clsx from "clsx";
import type { MpLiveGame } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { local, useLocal } from "../util/local.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { toWorker } from "../util/toWorker.ts";
import { TeamLogoInline } from "./TeamLogoInline.tsx";

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
	if (game.stalled) {
		return "Delayed";
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

// Where a game being played live stands, the way a scoreboard puts it: the
// period over the clock, Half or End Q3 on a break, Final at the buzzer,
// Delayed while the screen playing it has gone quiet.
const statusLines = (game: MpLiveGame): string[] => {
	if (game.gameOver) {
		return ["Final"];
	}
	const score = game.score;
	if (!score || !score.q) {
		return ["Live"];
	}
	const clock = score.clock.trim();
	if (/^[.0:]+$/.test(clock)) {
		return [score.q === "Q2" ? "Half" : `End ${score.q}`];
	}
	if (game.stalled) {
		return [`${score.q} ${clock}`.trim(), "Delayed"];
	}
	if (game.paused) {
		return [`${score.q} ${clock}`.trim(), "Paused"];
	}
	return clock ? [score.q, clock] : [score.q];
};

// In the header: every league mate's game being played live, as a scoreboard
// row like the networks' (each game a cell: both teams with logo and score,
// what period and how long left), tap one to watch. There only while
// something is live: in the top bar on a wide screen, its own row under it on
// anything narrower (where the bar has no room, and the sidebar isn't docked
// below it).
export const LiveScoresStrip = ({ className }: { className?: string }) => {
	const { mpLiveGames, ownLiveSim, teamInfoCache } = useLocal([
		"mpLiveGames",
		"ownLiveSim",
		"teamInfoCache",
	]);

	if (mpLiveGames.length === 0) {
		return null;
	}

	return (
		<div className={clsx("live-scores-strip", className)}>
			{mpLiveGames.map((game) => {
				const tids = game.tids;
				const pts = game.score?.pts ?? [0, 0];
				const status = statusLines(game);
				// (Not while playing your own live: leaving it would end it.)
				const title = ownLiveSim
					? "Finish your game first"
					: `${game.byName} is simming ${
							game.label || "this game"
						} live${game.stalled ? " (not heard from)" : game.paused ? " (paused)" : ""}`;
				return (
					<button
						key={game.gid}
						type="button"
						className={clsx(
							"live-score",
							game.watching && "live-score-watching",
						)}
						title={title}
						disabled={ownLiveSim}
						onClick={() => {
							watchLiveGame(game.gid);
						}}
					>
						{tids ? (
							<span className="live-score-teams">
								{([1, 0] as const).map((i) => {
									const t = teamInfoCache[tids[i]];
									return (
										<span
											key={i}
											className={clsx(
												"live-score-team",
												pts[i] < pts[i === 1 ? 0 : 1] && "live-score-trailing",
											)}
										>
											<TeamLogoInline
												imgURL={t?.imgURL}
												imgURLSmall={t?.imgURLSmall}
												includePlaceholderIfNoLogo
												size={16}
											/>
											<span className="live-score-abbrev">
												{t?.abbrev ?? ""}
											</span>
											<span className="live-score-pts">{pts[i]}</span>
										</span>
									);
								})}
							</span>
						) : (
							<span className="live-score-label">
								{game.label || "Live game"}
							</span>
						)}
						<span
							className={clsx(
								"live-score-status",
								(game.paused || game.stalled) && "live-score-status-paused",
								game.gameOver && "live-score-status-final",
							)}
						>
							{status.map((line) => (
								<span key={line}>{line}</span>
							))}
						</span>
					</button>
				);
			})}
		</div>
	);
};
