import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { decideOwnGameSim } from "../../common/ownGameSim.ts";
import { helpers } from "../util/helpers.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { useSimAuthorityLocked } from "../util/useSimAuthorityLocked.ts";
import { DAILY_SCHEDULE } from "../../common/constants.ts";
import { NoGamesMessage } from "./GameLog.tsx";
import allowForceTie from "../../common/allowForceTie.ts";
import { ForceWin } from "../components/ForceWin.tsx";
import { ScoreBox } from "../components/ScoreBox/index.tsx";
import { SimHereButton } from "../components/SimHereButton.tsx";
import { GameNote } from "../components/GameNote.tsx";
import { DayRecap } from "../components/DayRecap.tsx";
import { buildRecapLinksForGame } from "../util/linkifyRecap.ts";
import {
	getDailyScheduleScroll,
	setDailyScheduleScroll,
} from "../util/dailyScheduleUiState.ts";
import { useEffect, useState } from "react";

const DailySchedule = ({
	cid,
	cids,
	completed,
	day,
	dayNote,
	days,
	elam,
	elamASG,
	isToday,
	ownGameSimCutoffSeconds,
	season,
	ties,
	topPlayers,
	upcoming,
}: View<"dailySchedule">) => {
	// Prime the engine-corrected spreads for this day, AFTER the page has
	// rendered rather than as part of building it. Pricing reads every active
	// player, which is fine on the sportsbook but is not work worth adding to a
	// page that just lists games.
	//
	// Nothing is read back here. The spread each game shows comes from the view
	// itself (game.spread), so the schedule, the Schedule page and the league top
	// bar are all quoting one number; this just makes sure that number is the
	// refined one. When a background sim lands it emits a sportsbookLines update,
	// which rebuilds this view off the warmed cache.
	const upcomingKey = upcoming.map((g) => g.gid).join(",");
	useEffect(() => {
		if (upcoming.length === 0) {
			return;
		}
		(async () => {
			try {
				await toWorker("main", "syncDaySpreads", { season, day });
			} catch (error) {
				// A missing line is not worth breaking the page over - every game keeps
				// showing the closed-form spread it always did.
				console.error("Failed to refine spreads", error);
			}
		})();
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [season, day, upcomingKey]);

	useTitleBar({
		title: DAILY_SCHEDULE,
		dropdownView: "daily_schedule",
		dropdownFields: { seasons: season, days: day, cids: cid ?? "all" },
		dropdownCustomOptions: {
			cids,
			days,
		},
	});

	const {
		gameSimInProgress,
		mpAutoPlay,
		mpLiveBroadcast,
		mpLiveWatchable,
		mpSyncActive,
		mpSyncIsHost,
		mpSyncReady,
		mpSyncReconnecting,
		phase,
		season: currentSeason,
		teamInfoCache,
		userTid,
	} = useLocal([
		"gameSimInProgress",
		"mpAutoPlay",
		"mpLiveBroadcast",
		"mpLiveWatchable",
		"mpSyncActive",
		"mpSyncIsHost",
		"mpSyncReady",
		"mpSyncReconnecting",
		"phase",
		"season",
		"teamInfoCache",
		"userTid",
	]);

	// A league-mate's live sim that is still running. The game itself has already
	// been simmed and synced - watching is only the animation - so the score
	// beside this badge is the real one. Someone who left the broadcast has
	// already opted into seeing results; this is just the way back in.
	// The cutoff is a moving deadline, so the countdown has to advance on its own
	// rather than be read during render (which is impure, and would also freeze
	// the buttons in whatever state the last render happened to catch). Only ticks
	// while someone is actually auto-playing.
	const autoPlayNextRunAt = mpAutoPlay?.nextRunAt;
	const [now, setNow] = useState(() => Date.now());
	useEffect(() => {
		if (typeof autoPlayNextRunAt !== "number") {
			return;
		}
		setNow(Date.now());
		const interval = setInterval(() => {
			setNow(Date.now());
		}, 1000);
		return () => {
			clearInterval(interval);
		};
	}, [autoPlayNextRunAt]);

	// The same policy the worker enforces, so a button that would be refused is
	// greyed out with the reason in its tooltip rather than offered and rejected.
	const ownGameSim = (isOwnGame: boolean) =>
		decideOwnGameSim({
			isOwnGame,
			isAuthority: !mpSyncActive || !!mpSyncIsHost,
			connectedAndReady:
				!mpSyncActive || (!!mpSyncReady && !mpSyncReconnecting),
			// Watching one, or one is running in the room and this device walked
			// out of it - either way a second sim is what the worker would refuse.
			simInFlight: !!mpLiveBroadcast?.active || !!mpLiveWatchable,
			msUntilAutoSim:
				typeof autoPlayNextRunAt === "number"
					? autoPlayNextRunAt - now
					: undefined,
			cutoffSeconds: ownGameSimCutoffSeconds,
		});

	const liveBroadcastGid =
		mpLiveBroadcast?.active && !mpLiveBroadcast.isBroadcaster
			? mpLiveBroadcast.gid
			: undefined;

	// Can't sim/watch games from here unless this device is in charge of simming.
	const { locked: simAuthorityLocked } = useSimAuthorityLocked();

	// Remember the scroll position per day, so leaving the page (e.g. tapping a
	// player link) and coming back lands you where you were rather than at the top.
	// Restored on a macrotask so it runs after the router's firstRun scroll-to-top.
	useEffect(() => {
		const key = `${season}-${day}`;
		const saved = getDailyScheduleScroll(key);
		let restoreId: number | undefined;
		if (saved !== undefined) {
			restoreId = window.setTimeout(() => {
				window.scrollTo(window.scrollX, saved);
			}, 0);
		}
		return () => {
			if (restoreId !== undefined) {
				clearTimeout(restoreId);
			}
			setDailyScheduleScroll(key, window.scrollY);
		};
	}, [season, day]);

	let simToDay = null;
	if (upcoming.length > 0 && !isToday) {
		const minGid = Math.min(...upcoming.map((game) => game.gid));
		simToDay = (
			<div className="mb-3">
				<button
					className="btn btn-secondary"
					disabled={gameSimInProgress || simAuthorityLocked}
					onClick={() => {
						toWorker("actions", "simToGame", minGid);
					}}
				>
					Sim to day
				</button>
			</div>
		);
	}

	const upcomingAndCompleted = upcoming.length > 0 && completed.length > 0;

	const tradeDeadline =
		upcoming.length === 1 &&
		upcoming[0]!.teams[0].tid === -3 &&
		upcoming[0]!.teams[1].tid === -3;

	let noGamesMessage;
	if (days.length === 0) {
		noGamesMessage = (
			<NoGamesMessage warnAboutDelete={season < currentSeason} />
		);
	}

	return (
		<>
			<div className="d-flex flex-wrap align-items-center gap-3">
				<MoreLinks type="schedule" page="daily_schedule" />
			</div>

			{dayNote ? (
				<DayRecap
					season={season}
					day={day}
					note={dayNote}
					links={completed.flatMap((game) =>
						buildRecapLinksForGame(game, (tid) => teamInfoCache[tid]),
					)}
				/>
			) : null}

			{noGamesMessage ? (
				noGamesMessage
			) : (
				<>
					{simToDay}

					{tradeDeadline ? (
						<p>
							Sim one day to move past the trade deadline, and then the next
							day's games will be available here.
						</p>
					) : null}

					{upcoming.length > 0 ? (
						<>
							{upcomingAndCompleted ? <h2>Upcoming Games</h2> : null}
							<div className="d-flex flex-wrap" style={{ gap: "1rem 2rem" }}>
								{upcoming.map((game) => {
									const actions =
										isToday && !tradeDeadline
											? (() => {
													// Your own game stays available even when someone
													// else is in charge of simming: one gid is a
													// disjoint slice and the sim-day fence refuses any
													// overlap. The worker owns the full rule (cutoff
													// window, a sim already running) and names the
													// reason when it refuses, so this only has to stop
													// greying out the button.
													const isOwnGame =
														game.teams[0].tid === userTid ||
														game.teams[1].tid === userTid;
													const decision = ownGameSim(isOwnGame);
													const blocked =
														gameSimInProgress ||
														(simAuthorityLocked && !decision.allow);
													return [
														{
															disabled: blocked,
															highlight: isOwnGame,
															title:
																blocked && !decision.allow
																	? decision.reason
																	: undefined,
															text: (
																<>
																	Watch
																	<br />
																	game
																</>
															),
															onClick: () =>
																toWorker("actions", "liveGame", game.gid),
														},
														{
															disabled: blocked,
															highlight: isOwnGame,
															title:
																blocked && !decision.allow
																	? decision.reason
																	: undefined,
															text: (
																<>
																	Sim
																	<br />
																	game
																</>
															),
															onClick: () =>
																toWorker("actions", "simGame", game.gid),
														},
													];
												})()
											: undefined;

									const allowTie = allowForceTie({
										homeTid: game.teams[0].tid,
										awayTid: game.teams[1].tid,
										elam,
										elamASG,
										phase,
										ties,
									});

									let playersUpcoming: [any, any] | undefined;
									if (topPlayers.type === "byGid") {
										playersUpcoming = topPlayers.playersByGid[game.gid];
									} else {
										const x0 = topPlayers.playersByTid[game.teams[0].tid];
										const x1 = topPlayers.playersByTid[game.teams[1].tid];

										// Undefined for ASG
										if (x0 && x1) {
											playersUpcoming = [x0[0], x1[0]];
										}
									}

									return (
										<div
											className="flex-grow-1"
											key={game.gid}
											style={{ maxWidth: 510 }}
										>
											<ScoreBox
												game={{
													// Leave out forceTie, since ScoreBox wants the value for finished games
													finals: game.finals,
													gid: game.gid,
													season: game.season,
													spread: game.spread,
													teams: game.teams,
												}}
												playersUpcoming={playersUpcoming}
												actions={actions}
											/>
											<ForceWin allowTie={allowTie} game={game} />
										</div>
									);
								})}
							</div>
						</>
					) : null}

					{completed.length > 0 ? (
						<>
							{upcomingAndCompleted ? (
								<h2 className="mt-3">Completed Games</h2>
							) : null}

							<div className="d-flex flex-wrap" style={{ gap: "1rem 2rem" }}>
								{completed.map((game) => {
									return (
										<div
											className="flex-grow-1"
											key={game.gid}
											style={{ maxWidth: 510 }}
										>
											<div
												className={
													game.note ? "daily-game-with-note" : "daily-game"
												}
											>
												<ScoreBox game={game} />
												{game.gid === liveBroadcastGid ? (
													<div className="mt-1">
														<span className="badge text-bg-danger me-2">
															LIVE
														</span>
														<button
															className="btn btn-sm btn-link p-0 border-0"
															type="button"
															onClick={() => {
																void realtimeUpdate(
																	[],
																	helpers.leagueUrl(["live_game"]),
																);
															}}
														>
															Watch
														</button>
													</div>
												) : null}
												{game.note ? (
													<GameNote
														gid={game.gid}
														note={game.note}
														links={buildRecapLinksForGame(
															game,
															(tid) => teamInfoCache[tid],
														)}
													/>
												) : null}
											</div>
										</div>
									);
								})}
							</div>
						</>
					) : null}
				</>
			)}

			{/* Bottom of the page, where the AI-recap buttons used to sit. The
			    margin rides on the button itself rather than a wrapper, since the
			    component renders nothing at all when this device already holds
			    sim authority - an empty wrapper would leave a gap. */}
			<SimHereButton className="btn btn-primary btn-sm mt-3" />
		</>
	);
};

export default DailySchedule;
