import { MoreLinks } from "../components/MoreLinks.tsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { useSimAuthorityLocked } from "../util/useSimAuthorityLocked.ts";
import { DAILY_SCHEDULE } from "../../common/constants.ts";
import { NoGamesMessage } from "./GameLog.tsx";
import allowForceTie from "../../common/allowForceTie.ts";
import { ForceWin } from "../components/ForceWin.tsx";
import { ScoreBox } from "../components/ScoreBox/index.tsx";
import { GameRecap } from "../components/GameRecap.tsx";
import { GameNote } from "../components/GameNote.tsx";
import { DayRecap } from "../components/DayRecap.tsx";
import { buildRecapLinksForGame } from "../util/linkifyRecap.ts";
import {
	getDailyScheduleScroll,
	setDailyScheduleScroll,
} from "../util/dailyScheduleUiState.ts";
import { useEffect } from "react";

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
	season,
	ties,
	topPlayers,
	upcoming,
}: View<"dailySchedule">) => {
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
		phase,
		season: currentSeason,
		teamInfoCache,
		userTid,
	} = useLocal([
		"gameSimInProgress",
		"phase",
		"season",
		"teamInfoCache",
		"userTid",
	]);

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
											? [
													{
														disabled: gameSimInProgress || simAuthorityLocked,
														highlight:
															game.teams[0].tid === userTid ||
															game.teams[1].tid === userTid,
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
														disabled: gameSimInProgress || simAuthorityLocked,
														highlight:
															game.teams[0].tid === userTid ||
															game.teams[1].tid === userTid,
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
												]
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

					{completed.length > 0 ? (
						<div className="mt-3">
							<GameRecap
								season={season}
								day={day}
								numCompleted={completed.length}
							/>
						</div>
					) : null}
				</>
			)}
		</>
	);
};

export default DailySchedule;
