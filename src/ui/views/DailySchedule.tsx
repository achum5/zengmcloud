import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { decideOwnGameSim } from "../../common/ownGameSim.ts";
import { useSimAuthorityLocked } from "../util/useSimAuthorityLocked.ts";
import { DAILY_SCHEDULE } from "../../common/constants.ts";
import { NoGamesMessage } from "./GameLog.tsx";
import allowForceTie from "../../common/allowForceTie.ts";
import { ForceWin } from "../components/ForceWin.tsx";
import { NATIONAL_ROOM, ScoreBox } from "../components/ScoreBox/index.tsx";
import { SimHereButton } from "../components/SimHereButton.tsx";
import { GameNote } from "../components/GameNote.tsx";
import { DayRecap } from "../components/DayRecap.tsx";
import NightlyRecap from "../components/NightlyRecap/index.tsx";
import {
	buildRecapLinksForGame,
	buildSentenceGamesForDay,
} from "../util/linkifyRecap.ts";
import {
	getDailyScheduleScroll,
	setDailyScheduleScroll,
} from "../util/dailyScheduleUiState.ts";
import { useEffect, useRef, useState } from "react";
import clsx from "clsx";
import { isAllStarGame, isTradeDeadline } from "../../common/specialGames.ts";
import { formatGameDate } from "../../common/calendarDates.ts";
import { WeekStrip } from "../components/LeagueHub/WeekStrip.tsx";
import { LeadStory } from "../components/LeagueHub/LeadStory.tsx";
import { GameCard } from "../components/LeagueHub/GameCard.tsx";
import { UpcomingCard } from "../components/LeagueHub/UpcomingCard.tsx";
import {
	HubTabs,
	useHubTab,
	type HubTabId,
} from "../components/LeagueHub/HubTabs.tsx";
import {
	ComingUp,
	Headlines,
	HubStandings,
	InjuryReport,
	TopPerformers,
} from "../components/LeagueHub/HubRail.tsx";
import {
	LiveGameStatus,
	watchLiveAction,
	watchLiveGame,
} from "../components/LiveGames.tsx";

// Each hub pane's place on a phone. (Whole class names: the build keeps only
// styles whose names it finds written out in the code.)
const PANE_ORDER = [
	"",
	"lh-o1",
	"lh-o2",
	"lh-o3",
	"lh-o4",
	"lh-o5",
	"lh-o6",
	"lh-o7",
];

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
	offDay,
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
		mpLiveGames,
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
		"mpLiveGames",
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

	// Every game being played live: LIVE, its score as it stands, Watch live.
	// One already played here reads as not played yet (the worker sends it as
	// `live`) till its final on the screen playing it.
	const liveGameByGid = new Map(mpLiveGames.map((game) => [game.gid, game]));
	const isLive = (game: (typeof upcoming)[number]) =>
		!!game.live || liveGameByGid.has(game.gid) || mateLive?.gid === game.gid;
	const playable = upcoming.filter((game) => !isLive(game));
	// (Room for a network's logo in every box, so they line up, if any's on one.)
	const nationalRoom = upcoming.some((game) => game.national);

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
	if (playable.length > 0 && !isToday) {
		const minGid = Math.min(...playable.map((game) => game.gid));
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
		playable.some((game) => !isUserGame(game)) &&
		playable.some(isUserGame);

	const tradeDeadline =
		playable.length === 1 &&
		upcoming.length === 1 &&
		isTradeDeadline(playable[0]!.teams[0].tid, playable[0]!.teams[1].tid);

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

	// (Nothing for a night with no recap plays.)
	const recapVideo =
		completed.length > 0 ? (
			<div style={{ maxWidth: 960 }}>
				{/* (Fetched again when a game played live goes final.) */}
				<NightlyRecap key={completed.length} season={season} day={day} />
			</div>
		) : null;

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

					{offDay ? (
						<p className="text-body-secondary">
							{isToday ? "No games today." : "No games."}
						</p>
					) : null}

					{upcoming.length > 0 ? (
						<>
							{upcomingAndCompleted ? <h2>Upcoming Games</h2> : null}
							<div
								className={hubOn ? "lh-games lh-finals" : "d-flex flex-wrap"}
								style={hubOn ? undefined : { gap: "1rem 2rem" }}
							>
								{upcoming.map((game) => {
									const live = isLive(game);
									const liveGame = liveGameByGid.get(game.gid);
									const actions = live
										? [
												watchLiveAction(
													game.gid,
													liveGame?.byName ??
														(mateLive?.gid === game.gid
															? mateLive.byName
															: undefined),
												),
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

									// On the card: watch it or sim it - only where this
									// device can sim it. (Someone else in charge of simming:
									// your own team's game only.)
									const isOwnGame =
										game.teams[0].tid === userTid ||
										game.teams[1].tid === userTid;
									const cardDecision = ownGameSim(isOwnGame);
									const cardActions =
										!live &&
										isToday &&
										!tradeDeadline &&
										!(simAuthorityLocked && !isOwnGame)
											? {
													own: isOwnGame,
													disabled:
														gameSimInProgress ||
														(simAuthorityLocked && !cardDecision.allow),
													title:
														simAuthorityLocked && !cardDecision.allow
															? cardDecision.reason
															: undefined,
													onWatch: () => {
														toWorker("actions", "liveGame", game.gid);
													},
													onSim: () => {
														toWorker("actions", "simGame", game.gid);
													},
												}
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
											style={
												hubOn
													? undefined
													: {
															maxWidth:
																510 + (nationalRoom ? NATIONAL_ROOM : 0),
														}
											}
										>
											{hubOn &&
											!live &&
											game.teams[0].tid >= 0 &&
											game.teams[1].tid >= 0 ? (
												<UpcomingCard
													game={game}
													players={playersUpcoming}
													when={
														isToday
															? "Today"
															: date
																? formatGameDate(date, "monthDay")
																: `Day ${game.day}`
													}
													actions={cardActions}
												/>
											) : (
												<ScoreBox
													className={hubOn ? "lh-box" : undefined}
													game={{
														// Leave out forceTie, since ScoreBox wants the value for finished games
														finals: game.finals,
														gid: game.gid,
														national: game.national,
														season: game.season,
														spread: game.spread,
														teams: game.teams,
													}}
													livePts={live ? liveGame?.score?.pts : undefined}
													playersUpcoming={playersUpcoming}
													actions={actions}
													nationalRoom={nationalRoom}
												/>
											)}
											{live ? (
												<LiveGameStatus className="mt-1" gid={game.gid} />
											) : (
												<ForceWin allowTie={allowTie} game={game} />
											)}
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

							<div
								className={hubOn ? "lh-games lh-finals" : "d-flex flex-wrap"}
								style={hubOn ? undefined : { gap: "1rem 2rem" }}
							>
								{completed.map((game) => {
									const liveHere =
										game.gid === liveBroadcastGid ||
										liveGameByGid.has(game.gid);
									// In the hub, a played game as NBA.com's scoreboard
									// shows one.
									const card =
										hubOn &&
										!liveHere &&
										!isAllStarGame(game.teams[0].tid, game.teams[1].tid);
									const note = game.note ? (
										<GameNote
											gid={game.gid}
											note={game.note}
											links={buildRecapLinksForGame(
												game,
												(tid) => teamInfoCache[tid],
											)}
										/>
									) : null;
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
												{card ? (
													<GameCard game={game}>{note}</GameCard>
												) : (
													<ScoreBox
														className={hubOn ? "lh-box" : undefined}
														game={game}
													/>
												)}
												{liveHere ? (
													<div className="mt-1 d-flex align-items-center gap-2">
														<LiveGameStatus gid={game.gid} />
														<button
															className="btn btn-sm btn-link p-0 border-0"
															type="button"
															onClick={() => {
																watchLiveGame(game.gid);
															}}
														>
															Watch
														</button>
													</div>
												) : null}
												{card ? null : note}
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

	// On a phone, the hub in tabs: the night's scores, its recap once it's
	// played, the standings.
	const hubTabs: HubTabId[] = ["scores"];
	if (
		completed.length > 0 &&
		(dayNote !== undefined ||
			hub.performers.length > 0 ||
			hub.injuries.length > 0)
	) {
		hubTabs.push("recap");
	}
	if (hub.standings) {
		hubTabs.push("standings");
	}
	const [hubTab, setHubTab] = useHubTab(hubTabs);
	const gridRef = useRef<HTMLDivElement>(null);
	const pane = (id: HubTabId, order: number, extra?: string) =>
		clsx(PANE_ORDER[order], "lh-pane", extra, {
			"lh-pane-shown": id === hubTab,
		});

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
			<div
				className={clsx(
					"flex-wrap align-items-center gap-3",
					// (Not over the hub on a phone: the room goes to the night.)
					hubOn ? "d-none d-sm-flex" : "d-flex",
				)}
			>
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
					<HubTabs
						contentRef={gridRef}
						onPick={setHubTab}
						tab={hubTab}
						tabs={hubTabs}
					/>
					<div
						className={clsx("lh-grid", { "lh-tabbed": hubTabs.length > 1 })}
						ref={gridRef}
					>
						<div className="lh-main">
							{completed.length > 0 && dayNote ? (
								<div className={pane("recap", 1)}>
									<LeadStory
										day={day}
										links={recapLinks}
										note={dayNote}
										season={season}
										sentenceGames={sentenceGames}
									/>
								</div>
							) : null}
							<div className={pane("scores", 3, "lh-scores")}>
								<h2 className="lh-section-title">
									{completed.length > 0 && upcoming.length === 0
										? "Scores"
										: "Games"}
								</h2>
								{scores}
							</div>
						</div>
						<div className="lh-rail">
							{/* The night's recap video: atop the side column, its
							    width, over the day's leaders - first of all on a
							    phone. (Fetched again when a game played live goes
							    final.) */}
							{completed.length > 0 ? (
								<div className={pane("recap", 0, "lh-video")}>
									<NightlyRecap
										key={completed.length}
										season={season}
										day={day}
									/>
								</div>
							) : null}
							{hub.performers.length > 0 ? (
								<div className={pane("recap", 2)}>
									<TopPerformers performers={hub.performers} season={season} />
								</div>
							) : null}
							{completed.length > 0 ? (
								<div className={pane("recap", 4, "lh-wide-only")}>
									<Headlines games={completed} season={season} />
								</div>
							) : null}
							{hub.standings ? (
								<div className={pane("standings", 5)}>
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
								<div className={pane("recap", 6)}>
									<InjuryReport injuries={hub.injuries} season={season} />
								</div>
							) : null}
							{hub.next ? (
								<div className={pane("scores", 7)}>
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
					{recapVideo}
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
