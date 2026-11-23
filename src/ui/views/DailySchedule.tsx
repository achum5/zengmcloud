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
import {
	buildRecapLinksForGame,
	buildSentenceGamesForDay,
} from "../util/linkifyRecap.ts";
import {
	getDailyScheduleScroll,
	setDailyScheduleScroll,
} from "../util/dailyScheduleUiState.ts";
import { useEffect, useState } from "react";
import { isTradeDeadline } from "../../common/specialGames.ts";
import { formatGameDate } from "../../common/calendarDates.ts";
import { WeekStrip } from "../components/LeagueHub/WeekStrip.tsx";
import { LeadStory } from "../components/LeagueHub/LeadStory.tsx";
import {
	ComingUp,
	Headlines,
	HubStandings,
	InjuryReport,
	TopPerformers,
} from "../components/LeagueHub/HubRail.tsx";

const DailySchedule = ({
	calendar,
	cid,
	cids,
	completed,
	date,
	day,
	dayNote,
	days,
	elam,
	hub,
	nights,
	todayDay,
	elamASG,
	isToday,
	ownGameSimCutoffSeconds,
	season,
	ties,
	topPlayers,
	upcoming,
}: View<"dailySchedule">) => {
	useTitleBar({
		title: DAILY_SCHEDULE,
		dropdown: {
			fields: [
				{ key: "season", options: { type: "seasons" }, value: season },
				{ key: "day", options: days, value: day, buttons: "forward" },
				{ key: "cid", options: cids, value: cid ?? "all" },
			],
			view: "dailySchedule",
		},
	});

	const {
		gameSimInProgress,
		ownLiveSim,
		mpAutoPlay,
		mpLiveBroadcast,
		mpLiveOffer,
		mpLiveWatchable,
		mpSyncActive,
		mpSyncIsHost,
		mpSyncReady,
		mpSyncReconnecting,
		phase,
		season: currentSeason,
		teamInfoCache,
		userTid,
		userTids,
	} = useLocal([
		"gameSimInProgress",
		"ownLiveSim",
		"mpAutoPlay",
		"mpLiveBroadcast",
		"mpLiveOffer",
		"mpLiveWatchable",
		"mpSyncActive",
		"mpSyncIsHost",
		"mpSyncReady",
		"mpSyncReconnecting",
		"phase",
		"season",
		"teamInfoCache",
		"userTid",
		"userTids",
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
			// Only a live sim of this device's own game (e.g. in another tab) -
			// the worker's own test (liveSimGid). Not liveGameInProgress: that is
			// also held while a league-mate's broadcast is watched or offered,
			// which greyed out your game for no reason you could see. Their
			// game and yours run side by side.
			simInFlight: !!ownLiveSim,
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
	// The game a league-mate is simming live right now - watched here or not
	// (yet). Still upcoming here till their result lands, it is watched with
	// them: never played a second time on this device, at its own pace.
	const mateLive =
		liveBroadcastGid !== undefined
			? { gid: liveBroadcastGid, byName: mpLiveBroadcast?.byName }
			: (mpLiveWatchable ?? mpLiveOffer);

	// Can't sim/watch games from here unless this device is in charge of simming.
	const { locked: simAuthorityLocked, reason: simAuthorityReason } =
		useSimAuthorityLocked();

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

	// TODAY'S CPU GAMES, ALL AT ONCE: every game left today with no user team
	// in it (none of userTids - in multi team mode, and in a cloud room every
	// league mate's team), so the user games are what's left to play. Only
	// while there are both, and only on the device in charge of simming - in
	// the spot where every other device has Sim here (see SimHereButton).
	const [simmingCpu, setSimmingCpu] = useState(false);
	const simsHere = !mpSyncActive || !!mpSyncIsHost;
	const isUserGame = (game: (typeof upcoming)[number]) =>
		userTids.includes(game.teams[0].tid) ||
		userTids.includes(game.teams[1].tid);
	const showSimCpu =
		simsHere &&
		isToday &&
		upcoming.some((game) => !isUserGame(game)) &&
		upcoming.some(isUserGame);

	const tradeDeadline =
		upcoming.length === 1 &&
		isTradeDeadline(upcoming[0]!.teams[0].tid, upcoming[0]!.teams[1].tid);

	const simCpuButton =
		showSimCpu && !tradeDeadline ? (
			<button
				type="button"
				className="btn btn-primary btn-sm mt-3"
				disabled={gameSimInProgress || simAuthorityLocked || simmingCpu}
				title={
					simAuthorityLocked
						? simAuthorityReason
						: "Sim every game today without a user team"
				}
				onClick={async () => {
					setSimmingCpu(true);
					try {
						await toWorker("actions", "simCpuGames", undefined);
					} finally {
						setSimmingCpu(false);
					}
				}}
			>
				Sim CPU games
			</button>
		) : null;

	const dateHeading =
		date !== undefined ? (
			<h3 className="h5 mb-3">{formatGameDate(date, "full")}</h3>
		) : null;

	const recapLinks = completed.flatMap((game) =>
		buildRecapLinksForGame(game, (tid) => teamInfoCache[tid]),
	);
	const sentenceGames = buildSentenceGamesForDay(
		completed,
		(tid) => teamInfoCache[tid],
	);

	let noGamesMessage;
	if (days.length === 0) {
		noGamesMessage = (
			<NoGamesMessage warnAboutDelete={season < currentSeason} />
		);
	}

	// THE LEAGUE HUB (basketball, where the night's recaps are written): the
	// nights across the top, the night's top story leading, its scores under
	// it, and down the side its top performers, headlines, the standings, who
	// got hurt and what's next. One column on a phone, in that order of need.
	const hubOn = __SPORT === "basketball" && days.length > 0;

	// The players to watch in a game still to play: each team's best, as its
	// card has them.
	const playersToWatch = (game: (typeof upcoming)[number]) => {
		if (topPlayers.type === "byGid") {
			return topPlayers.playersByGid[game.gid] as [any, any] | undefined;
		}
		const x0 = topPlayers.playersByTid[game.teams[0].tid];
		const x1 = topPlayers.playersByTid[game.teams[1].tid];
		return x0 && x1 ? ([x0[0], x1[0]] as [any, any]) : undefined;
	};

	const scores = (
		<>
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
										mateLive?.gid === game.gid
											? [
													{
														highlight: true,
														title: `${mateLive.byName ?? "A league-mate"} is simming this game live`,
														text: (
															<>
																Watch
																<br />
																live
															</>
														),
														onClick: () =>
															toWorker("main", "watchLiveBroadcast", undefined),
													},
												]
											: isToday && !tradeDeadline
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
											className={hubOn ? "lh-game" : "flex-grow-1"}
											key={game.gid}
											style={hubOn ? undefined : { maxWidth: 510 }}
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
											className={hubOn ? "lh-game" : "flex-grow-1"}
											key={game.gid}
											style={hubOn ? undefined : { maxWidth: 510 }}
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
		</>
	);

	// The top story: the game the night's recap leads with - or, before the
	// night is played, its best matchup.
	const leadCompleted =
		completed.find((game) => game.gid === hub.leadGid) ?? completed[0];
	const leadUpcoming =
		leadCompleted === undefined
			? upcoming
					.filter((game) => game.teams[0].tid >= 0 && game.teams[1].tid >= 0)
					.map((game) => {
						const pct = (t: { won?: number; lost?: number }) => {
							const gp = (t.won ?? 0) + (t.lost ?? 0);
							return gp > 0 ? (t.won ?? 0) / gp : 0.5;
						};
						const ovr = (t: { ovr?: number }) => t.ovr ?? 0;
						return {
							game,
							score:
								pct(game.teams[0]) +
								pct(game.teams[1]) +
								(ovr(game.teams[0]) + ovr(game.teams[1])) / 200,
						};
					})
					.sort((a, b) => b.score - a.score)[0]?.game
			: undefined;

	const playedTids = new Set<number>();
	const wonTids = new Set<number>();
	for (const game of completed) {
		for (const t of game.teams) {
			playedTids.add(t.tid);
		}
		const [t0, t1] = game.teams;
		if (t0.pts > t1.pts) {
			wonTids.add(t0.tid);
		} else if (t1.pts > t0.pts) {
			wonTids.add(t1.tid);
		}
	}

	return (
		<>
			<div className="d-flex flex-wrap align-items-center gap-3">
				<MoreLinks type="schedule" page="daily_schedule" />
			</div>

			{hubOn ? (
				<>
					<WeekStrip
						calendar={calendar}
						date={date}
						day={day}
						nights={nights}
						season={season}
						todayDay={todayDay}
					/>
					<div className="lh-grid">
						<div className="lh-main">
							{leadCompleted || leadUpcoming ? (
								<div className="lh-o1">
									<LeadStory
										date={date}
										day={day}
										dayNote={dayNote}
										game={leadCompleted ?? leadUpcoming}
										links={recapLinks}
										season={season}
										sentenceGames={sentenceGames}
										upcoming={leadCompleted === undefined}
										playersToWatch={
											leadUpcoming ? playersToWatch(leadUpcoming) : undefined
										}
									/>
								</div>
							) : null}
							<div className="lh-o3 lh-scores">
								<h2 className="lh-section-title">
									{completed.length > 0 && upcoming.length === 0
										? "Scores"
										: "Games"}
								</h2>
								{scores}
							</div>
						</div>
						<div className="lh-rail">
							{hub.performers.length > 0 ? (
								<div className="lh-o2">
									<TopPerformers performers={hub.performers} season={season} />
								</div>
							) : null}
							{completed.length > 0 ? (
								<div className="lh-o4 d-none d-lg-block">
									<Headlines games={completed} season={season} />
								</div>
							) : null}
							{hub.standings ? (
								<div className="lh-o5">
									<HubStandings
										playedTids={playedTids}
										season={season}
										standings={hub.standings}
										userTid={userTid}
										wonTids={wonTids}
									/>
								</div>
							) : null}
							{hub.injuries.length > 0 ? (
								<div className="lh-o6">
									<InjuryReport injuries={hub.injuries} season={season} />
								</div>
							) : null}
							{hub.next ? (
								<div className="lh-o7">
									<ComingUp
										next={hub.next}
										season={season}
										todayDay={todayDay}
									/>
								</div>
							) : null}
						</div>
					</div>
				</>
			) : (
				<>
					{dateHeading}
					{dayNote ? (
						<DayRecap
							season={season}
							day={day}
							note={dayNote}
							links={recapLinks}
							sentenceGames={sentenceGames}
						/>
					) : null}
					{scores}
				</>
			)}

			{/* Bottom of the page, where the AI-recap buttons used to sit. The
			    margin rides on the button itself rather than a wrapper, since the
			    component renders nothing at all when this device already holds
			    sim authority - an empty wrapper would leave a gap. */}
			{/* (One or the other: Sim CPU games on the device in charge of
			    simming, Sim here on the rest.) */}
			{simCpuButton}
			<SimHereButton className="btn btn-primary btn-sm mt-3" />
		</>
	);
};

export default DailySchedule;
