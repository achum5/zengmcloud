import {
	ALL_STAR_GAME_ONLY,
	PHASE,
	SAVE_REPLAYS_ALL_PLAYOFFS,
	SAVE_REPLAYS_DRAMATIC,
} from "../../../common/constants.ts";
import {
	GameSim,
	allStar,
	freeAgents,
	phase,
	player,
	season,
	team,
	trade,
} from "../index.ts";
import loadTeams from "./loadTeams.ts";
import { dayAlreadyCounted } from "./dailyCountdownGate.ts";
import { recordInjuryForensics } from "../sync/injuryForensics.ts";
import setGameAttributes from "../league/setGameAttributes.ts";
import updatePlayoffSeries from "./updatePlayoffSeries.ts";
import writeGameStats from "./writeGameStats.ts";
import writePlayerStats, {
	P_FATIGUE_DAILY_REDUCTION,
} from "./writePlayerStats.ts";
import writeTeamStats from "./writeTeamStats.ts";
import {
	clearRosterBlockNotice,
	notifyRosterBlockedSim,
} from "../sync/simBlockedNotify.ts";
import {
	getPendingSimStop,
	isTradeDeadlineGame,
	isTradeDeadlineGateActive,
	notifySimStopArrived,
	shouldStopAtSimStop,
} from "../sync/tradeDeadlineGate.ts";
import { settleBets } from "../sportsbook/bets.ts";
import { idb } from "../../db/index.ts";
import { updateTickerItems } from "../../util/updateTickerItems.ts";
import {
	advStats,
	g,
	helpers,
	lock,
	logEvent,
	toUI,
	updatePlayMenu,
	updateStatus,
	local,
} from "../../util/index.ts";
import type {
	Conditions,
	GameResults,
	LocalStateUI,
	ScheduleGame,
	UpdateEvents,
} from "../../../common/types.ts";
import allowForceTie from "../../../common/allowForceTie.ts";
import getWinner from "../../../common/getWinner.ts";
import { setLiveSimRatingsStatsPopoverPlayers } from "./setLiveSimRatingsStatsPopoverPlayers.ts";
import {
	getOneUpcomingGame,
	recomputeLocalUITeamOvrs,
} from "../../util/recomputeLocalUITeamOvrs.ts";
import { isSport } from "../../../common/sportFunctions.ts";
import { last } from "../../../common/utils.ts";
import {
	runAfterActionHook,
	setSingleGameSimActive,
} from "../sync/afterActionHook.ts";
import { beginLiveSimNotificationHold } from "../sync/liveSimNotificationHold.ts";
import {
	claimSimDayFence,
	completeClaimedSimDayFence,
} from "../sync/simDayFence.ts";
import { syncDebugLog } from "../sync/debugLog.ts";
import { scheduleForSim } from "./singleGameSchedule.ts";
import { orphanedScheduleGids } from "./orphanedSchedule.ts";
import { runLiveBroadcastStart } from "../sync/liveBroadcastHook.ts";
import { changeTracker } from "../../db/changeTracker.ts";

/**
 * Play one or more days of games.
 *
 * This also handles the case where there are no more games to be played by switching the phase to either the playoffs or before the draft, as appropriate.
 *
 * @memberOf core.game
 * @param {number} numDays An integer representing the number of days to be simulated. If numDays is larger than the number of days remaining, then all games will be simulated up until either the end of the regular season or the end of the playoffs, whichever happens first.
 * @param {boolean} start Is this a new request from the user to play games (true) or a recursive callback to simulate another day (false)? If true, then there is a check to make sure simulating games is allowed. Default true.
 * @param {number?} gidOneGame Game ID number if we just want to sim one game rather than the whole day. Must be defined if playByPlay is true.
 * @param {boolean?} playByPlay When true, an array of strings representing the play-by-play game simulation are included in the api.realtimeUpdate raw call.
 */
const play = async (
	numDays: number,
	conditions: Conditions,
	start: boolean = true,
	gidOneGame?: number,
	playByPlay?: boolean,
) => {
	// DID A LIVE PLAYBACK ACTUALLY REACH THE SCREEN?
	//
	// The live game page is navigated to BEFORE this runs, so it sits on
	// "Loading..." until the play-by-play arrives - and there are five ways out
	// of here that never send one: the lock is held, the game is not on the
	// schedule any more, the roster is illegal, the trade deadline stops the
	// sim, or the room's fence refuses the day. Every one of them used to leave
	// that page loading forever. The caller needs a plain answer.
	let liveSimDelivered = false;
	// This is called when there are no more games to play, either due to the user's request (e.g. 1 week) elapsing or at the end of the regular season
	const cbNoGames = async (playoffsOver: boolean = false) => {
		await updateStatus("Saving...");
		await idb.cache.flush();

		// Settle any sportsbook game bets whose games just finished (no-op if
		// nothing is bet). Wallet changes ride the sim's sync window to the room.
		try {
			await settleBets(conditions);
		} catch (error) {
			console.error("Sportsbook settlement failed", error);
		}

		await updateStatus("Idle");
		await lock.set("gameSim", false);

		// Check to see if the season is over
		const schedule = await season.getSchedule();
		if (g.get("phase") < PHASE.PLAYOFFS) {
			if (schedule.length === 0) {
				await phase.newPhase(
					PHASE.PLAYOFFS,
					conditions,
					gidOneGame !== undefined,
				);
			}
		} else if (playoffsOver) {
			await phase.newPhase(
				PHASE.DRAFT_LOTTERY,
				conditions,
				gidOneGame !== undefined,
			);
		}

		if (schedule.length > 0 && !playoffsOver) {
			const allStarNext = await allStar.nextGameIsAllStar(schedule);

			if (allStarNext && gidOneGame === undefined) {
				toUI(
					"realtimeUpdate",
					[
						[],
						helpers.leagueUrl(
							ALL_STAR_GAME_ONLY ? ["all_star", "teams"] : ["all_star"],
						),
					],
					conditions,
				);
			}
		}

		await updatePlayMenu();

		// The sim has finished. Publish everything it changed to the cloud NOW.
		// Multi-day sims ("week", "until playoffs", "until end of round", …) run
		// game.play fire-and-forget, so the dispatched action already resolved and
		// its afterAction ran with nothing to send - without this, the sim's changes
		// sat unpublished until the next unrelated worker call happened to drain the
		// tracker. Routed through a hook to avoid a static import cycle between the
		// game engine and the sync layer. Drain is atomic, so for a blocking
		// day/week sim (whose dispatched afterAction also runs) this doesn't
		// double-send.
		//
		// A single-game sim (Sim one game / live game) publishes its results the
		// same way - the room must stay in sync - but must NOT push a notification:
		// you deliberately simmed just one game with the rest of the day still to
		// play, so pinging phones with a "game done" would be noise.
		const synced = await runAfterActionHook("playMenu", "sim", {
			silent: gidOneGame !== undefined,
		});
		if (synced) {
			// The sim's results are durably queued for the room, so the claimed
			// day's crash-recovery window can close. On !synced the claim is left
			// to its lease: completing it with unpublished results could fence the
			// day forever while the room never receives it.
			completeClaimedSimDayFence();
		}
		if (!synced) {
			logEvent(
				{
					type: "error",
					text: `Cloud sync did not finish uploading this sim. The sim is still queued locally and will be retried after your connection works again.`,
					persistent: true,
				},
				conditions,
			);
		}

		// The single-game-sim window is over (its changeset is drained). Clear the
		// force-silent flag so the next full day/week sim notifies normally.
		if (gidOneGame !== undefined) {
			setSingleGameSimActive(false);
		}

		// Last word on the ticker, after any phase change above has landed. The
		// per-day refreshes inside the sim read a memoized slate and award race;
		// this one is the fresh read the user is actually about to look at.
		try {
			await updateTickerItems({ fresh: true });
		} catch (error) {
			console.error("Ticker refresh after sim failed", error);
		}
	};

	// Saves a vector of results objects for a day, as is output from cbSimGames.
	// simmedDay identifies the schedule day for the once-per-day gate below.
	const cbSaveResults = async (
		results: GameResults[],
		dayOver: boolean,
		simmedDay: number | undefined,
	) => {
		// Before writeGameStats, so LeagueTopBar can not update with game result
		if (gidOneGame !== undefined && playByPlay) {
			// Remember WHICH game the playback is for, so only that game's page can
			// declare it over. onLiveSimOver used to clear unconditionally, and any
			// other live game page - a finished game in a second tab, a replay
			// hitting its final play - would clear the flag mid-playback and un-hide
			// everything it exists to hide (phase text, ready-up, the score ticker).
			local.liveSimGid = gidOneGame;
			await toUI("updateLocal", [{ liveGameInProgress: true }]);

			// Run this before writing player stats
			await setLiveSimRatingsStatsPopoverPlayers(results);
		} else {
			// In case a live sim is still open in another tab
			local.liveSimRatingsStatsPopoverPlayers = undefined;
		}

		// Before writeGameStats, so injury is set correctly
		const { injuryTexts, pidsInjuredOneGameOrLess, stopPlay } =
			await writePlayerStats(results, conditions);

		let gameToUi: LocalStateUI["games"][number] | undefined;
		const gidsFinished = await Promise.all(
			results.map(async (result) => {
				const att = await writeTeamStats(result);

				const maybeGameToUi = await writeGameStats(result, att, conditions);
				if (maybeGameToUi) {
					gameToUi = maybeGameToUi;
				}

				return result.gid;
			}),
		);

		// Delete finished games from schedule
		for (const gid of gidsFinished) {
			if (typeof gid === "number") {
				await idb.cache.schedule.delete(gid);
			}
		}

		// Invalidate leaders cache, if it exists
		local.seasonLeaders = undefined;

		if (g.get("phase") === PHASE.PLAYOFFS) {
			// Update playoff series W/L
			await updatePlayoffSeries(results, conditions);
		} else {
			// Update clinchedPlayoffs, only if there are games left in the schedule. Otherwise, this would be inaccruate (not correctly accounting for tiebreakers) and redundant (going to be called again on phase change)
			const schedule = await season.getSchedule();
			if (schedule.length > 0) {
				await team.updateClinchedPlayoffs(false, conditions);
			}
		}

		if (injuryTexts.length > 0) {
			logEvent(
				{
					type: "injuredList",
					text: injuryTexts.join("<br>"),
					showNotification: true,
					persistent: stopPlay,
					saveToDb: false,
				},
				conditions,
			);
		}

		const updateEvents: UpdateEvents = ["gameSim"];

		// See dailyCountdownGate.ts: everything in the dayOver block below has
		// per-day semantics, a shared league can end up deciding "the day is
		// over" on more than one device, and a double run is a player healing a
		// day early (a real incident), doubled tragic-death odds, and doubled AI
		// signing/trade evaluations. The day's identity is stamped into a
		// replicated game attribute in the same write as the countdown, so a
		// counted day is never counted again anywhere.
		const countdownDay =
			simmedDay === undefined
				? undefined
				: { season: g.get("season"), phase: g.get("phase"), day: simmedDay };
		const countdownAlreadyRan = dayAlreadyCounted(
			g.get("lastDailyCountdownDay"),
			countdownDay,
		);
		if (dayOver && countdownAlreadyRan) {
			syncDebugLog("sim:daily-countdown-skipped", { day: simmedDay });
			void recordInjuryForensics({
				source: "day-tick-skipped",
				detail: `day=${simmedDay} already counted, countdown not run`,
			});
		}

		if (dayOver && !countdownAlreadyRan) {
			local.minFractionDiffs = undefined;

			const healedTexts: string[] = [];
			const injuryTickNotes: string[] = [];

			// Injury countdown - This must be after games are saved, of there is a race condition involving new injury assignment in writeStats. Free agents are handled in decreaseDemands.
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);

			for (const p of players) {
				let changed = false;

				if (p.injury.gamesRemaining > 0) {
					injuryTickNotes.push(
						`p${p.pid}:${p.injury.gamesRemaining}>${p.injury.gamesRemaining - 1}`,
					);
					p.injury.gamesRemaining -= 1;
					changed = true;
				}

				if (isSport("baseball") && p.pFatigue !== undefined && p.pFatigue > 0) {
					p.pFatigue = helpers.bound(
						p.pFatigue - P_FATIGUE_DAILY_REDUCTION,
						0,
						80,
					);
					changed = true;
				}

				// Is it already over?
				if (p.injury.type !== "Healthy" && p.injury.gamesRemaining <= 0) {
					const score = p.injury.score;
					p.injury = {
						type: "Healthy",
						gamesRemaining: 0,
					};
					changed = true;
					const healedText = `${
						last(p.ratings).pos
					} <a href="${helpers.leagueUrl(["player", p.pid])}">${p.firstName} ${
						p.lastName
					}</a>`;

					if (
						p.tid === g.get("userTid") &&
						!pidsInjuredOneGameOrLess.has(p.pid)
					) {
						healedTexts.push(healedText);
					}

					logEvent(
						{
							type: "healed",
							text: `${healedText} has recovered from ${helpers.pronoun(
								g.get("gender"),
								"his",
							)} injury.`,
							showNotification: false,
							pids: [p.pid],
							tids: [p.tid],
							score,
						},
						conditions,
					);
				}

				// Also check for gamesUntilTradable
				if (p.gamesUntilTradable === undefined) {
					p.gamesUntilTradable = 0; // Initialize for old leagues

					changed = true;
				} else if (p.gamesUntilTradable > 0) {
					p.gamesUntilTradable -= 1;
					changed = true;
				}

				if (changed) {
					await idb.cache.players.put(p);
				}
			}

			if (healedTexts.length > 0) {
				logEvent(
					{
						type: "healedList",
						text: healedTexts.join("<br>"),
						showNotification: true,
						saveToDb: false,
					},
					conditions,
				);
			}

			// Tragic deaths only happen during the regular season!
			if (
				g.get("phase") !== PHASE.PLAYOFFS &&
				Math.random() < g.get("tragicDeathRate") &&
				!g.get("repeatSeason") &&
				!g.get("forceHistoricalRosters")
			) {
				await player.killOne(conditions);

				if (g.get("stopOnInjury")) {
					await lock.set("stopGameSim", true);
				}

				updateEvents.push("playerMovement");
			}

			// Do this stuff after injuries, so autoSign knows the injury status of players for the next game
			const phase = g.get("phase");
			if (
				phase === PHASE.REGULAR_SEASON ||
				phase === PHASE.AFTER_TRADE_DEADLINE
			) {
				await freeAgents.decreaseDemands();
				await freeAgents.autoSign();
			}
			if (phase === PHASE.REGULAR_SEASON) {
				await trade.betweenAiTeams();
			}

			// One compact durable line for the whole league's countdown this day -
			// the record that distinguishes "the tick ran twice" from "a remote row
			// wiped the injury" next time a countdown loses days it should not.
			if (injuryTickNotes.length > 0) {
				void recordInjuryForensics({
					source: "day-tick",
					detail: `day=${simmedDay ?? "?"} ${injuryTickNotes.join(" ")}`,
				});
			}

			// Stamped LAST, so a crash mid-block re-counts the day rather than
			// marking it counted without counting. Rides the same capture window
			// as the countdown itself, so the whole room learns of both together.
			if (countdownDay !== undefined) {
				await setGameAttributes({ lastDailyCountdownDay: countdownDay });
			}
		}

		// More stuff for LeagueTopBar - update ovrs based on injuries, and (if user just played a game) update the score of the user's last game and add their next game
		// This is safe to do down here because injuries have been processed (if necessay) and games have been deleted from the schedule
		if (gameToUi) {
			const gamesToUi = [gameToUi];

			// Also show next game
			const upcomingGame = await getOneUpcomingGame();
			if (upcomingGame) {
				gamesToUi.push(upcomingGame);
			}

			await toUI("mergeGames", [gamesToUi]);
		} else {
			// This loads next game and calls mergeGames internally
			await recomputeLocalUITeamOvrs();
		}

		// The bottom ticker is league-wide, so a day of games changes it even when
		// the user's team did not play. mergeGames only carries their own games, so
		// the ticker is rebuilt separately here. Best effort - decoration must
		// never fail a sim.
		try {
			await updateTickerItems();
		} catch (error) {
			console.error("Ticker refresh after sim failed", error);
		}

		await advStats();

		const playoffsOver =
			g.get("phase") === PHASE.PLAYOFFS &&
			(await season.newSchedulePlayoffsDay());

		let raw;
		let url;

		// Persist a rewatchable replay for every game that generated play-by-play
		// this batch and still qualifies. Keyed by each game's own gid, written
		// inside the sim's capture window so it syncs to the whole room like the
		// game itself. Best-effort: a replay is a nicety, never fail the sim over
		// it.
		//
		// "Still qualifies" is for the dramatic-games option, which generates
		// play-by-play for the whole slate and only KEEPS the games that earned
		// it: a statistical feat (the same standard as the Statistical Feats
		// page - results.team[t].playerFeat, set by checkStatisticalFeat), or a
		// game winner/tyer (clutchPlays - the shot that won it, tied it, or
		// forced overtime). Everything a pre-sim rule asked for is saved
		// unconditionally, exactly as before.
		{
			const saveReplaysTids = new Set(g.get("saveReplaysTids"));
			const saveAllPlayoffGames =
				saveReplaysTids.has(SAVE_REPLAYS_ALL_PLAYOFFS) &&
				g.get("phase") === PHASE.PLAYOFFS;
			for (const result of results) {
				if (result.playByPlay === undefined) {
					continue;
				}
				const wantedBeforeSim =
					result.gid === gidOneGame ||
					saveAllPlayoffGames ||
					saveReplaysTids.has(result.team[0].id) ||
					saveReplaysTids.has(result.team[1].id);
				const dramatic =
					saveReplaysTids.has(SAVE_REPLAYS_DRAMATIC) &&
					(result.team[0].playerFeat ||
						result.team[1].playerFeat ||
						result.clutchPlays.length > 0);
				if (!wantedBeforeSim && !dramatic) {
					continue;
				}
				try {
					await idb.cache.liveGamePlayByPlay.put({
						gid: result.gid,
						season: g.get("season"),
						playByPlay: result.playByPlay,
					});
				} catch (error) {
					console.error("Failed to save game play-by-play", error);
				}
			}
		}

		// If this was a live sim, route the UI to the live game and (in a sync
		// room) broadcast it so every follower watches in lockstep.
		if (gidOneGame !== undefined && playByPlay) {
			const liveResult = results.find((result) => result.gid === gidOneGame);
			if (liveResult?.playByPlay !== undefined) {
				raw = {
					gidOneGame,
					playByPlay: liveResult.playByPlay,
				};
				url = helpers.leagueUrl(["live_game"]);
				liveSimDelivered = true;
				runLiveBroadcastStart(gidOneGame, liveResult.playByPlay);
			}

			// This is not ideal... it means no event will be sent to other open tabs. But I don't have a way of saying "send this update to all tabs except X" currently
			await toUI("realtimeUpdate", [updateEvents, url, raw], conditions);
		} else {
			url = undefined;
			await toUI("realtimeUpdate", [updateEvents]);
		}

		if (numDays - 1 <= 0 || playoffsOver) {
			await cbNoGames(playoffsOver);
		} else {
			await play(numDays - 1, conditions, false);
		}
	};

	const getResult = ({
		gid,
		day,
		teams,
		doPlayByPlay = false,
		homeCourtFactor = 1,
		neutralSite = false,
	}: {
		gid: number;
		day: number | undefined;
		teams: [any, any];
		doPlayByPlay?: boolean;
		homeCourtFactor?: number;
		neutralSite?: boolean;
	}) => {
		let dh;
		if (isSport("baseball")) {
			const dhSetting = g.get("dh");
			const cidHome = teams[0].cid;
			dh =
				dhSetting === "all" ||
				(Array.isArray(dhSetting) && dhSetting.includes(cidHome));
		}

		// In FBGM, need to do depth chart generation here (after deepCopy in forceWin case) to maintain referential integrity of players (same object in depth and team).
		for (const t of teams) {
			if (t.depth !== undefined) {
				t.depth = team.getDepthPlayers(t.depth, t.player, dh);
			}
		}

		let baseInjuryRate;
		const allStarGame = teams[0].id === -1 && teams[1].id === -2;
		if (allStarGame) {
			// Fewer injuries in All-Star Game, and no injuries in playoffs All-Star Game
			if (g.get("phase") === PHASE.PLAYOFFS) {
				baseInjuryRate = 0;
			} else {
				baseInjuryRate = g.get("injuryRate") / 4;
			}
		} else {
			baseInjuryRate = g.get("injuryRate");
		}

		return new GameSim({
			gid,
			day,
			teams,
			doPlayByPlay,
			homeCourtFactor,
			neutralSite: neutralSite || allStarGame,
			allStarGame,
			baseInjuryRate,

			// @ts-expect-error
			dh,
		}).run();
	};

	// Simulates a day of games (whatever is in schedule) and passes the results to cbSaveResults
	const cbSimGames = async (
		schedule: ScheduleGame[],
		teams: Record<number, any>,
		dayOver: boolean,
	) => {
		const results: any[] = [];

		// Teams whose every game auto-saves a rewatchable replay. Generating
		// play-by-play is extra work, so this is only done for the flagged teams'
		// games (plus the one game being live-watched, as before). The -2 sentinel
		// means "every playoff game", regardless of team.
		//
		// The -3 sentinel is "any game with a statistical feat or a game
		// winner/tyer" - which can only be known AFTER a game is simmed, so it
		// works the other way round from the rules above: play-by-play is
		// generated for EVERY game, and the ones that turn out not to qualify are
		// simply not saved (see cbSaveResults). That is the whole day paying the
		// live-sim game's generation cost, which is the price of the option.
		const saveReplaysTids = new Set(g.get("saveReplaysTids"));
		const saveAllPlayoffGames =
			saveReplaysTids.has(-2) && g.get("phase") === PHASE.PLAYOFFS;
		const saveDramaticGames = saveReplaysTids.has(SAVE_REPLAYS_DRAMATIC);

		for (const game of schedule) {
			const doPlayByPlay =
				(gidOneGame === game.gid && playByPlay) ||
				saveAllPlayoffGames ||
				saveDramaticGames ||
				saveReplaysTids.has(game.homeTid) ||
				saveReplaysTids.has(game.awayTid);

			const teamsInput = [teams[game.homeTid], teams[game.awayTid]] as any;

			const forceTie = game.forceWin === "tie";
			const invalidForceTie =
				forceTie &&
				!allowForceTie({
					homeTid: game.homeTid,
					awayTid: game.awayTid,
					ties: season.hasTies("current"),
					phase: g.get("phase"),
					elam: g.get("elam"),
					elamASG: g.get("elamASG"),
				});

			if (g.get("godMode") && game.forceWin !== undefined && !invalidForceTie) {
				const NUM_TRIES = 2000;
				const START_CHANGING_HOME_COURT_ADVANTAGE = NUM_TRIES / 4;

				const forceWinHome = game.forceWin === game.homeTid;
				let homeCourtFactor = 1;

				let found = false;
				let homeWonLastGame = false;
				let homeWonCounter = 0;
				for (let i = 0; i < NUM_TRIES; i++) {
					if (i >= START_CHANGING_HOME_COURT_ADVANTAGE) {
						if (!forceTie) {
							// Scale from 1x to 3x linearly, after staying at 1x for some time
							homeCourtFactor =
								1 +
								(2 * (i - START_CHANGING_HOME_COURT_ADVANTAGE)) /
									(NUM_TRIES - START_CHANGING_HOME_COURT_ADVANTAGE);

							if (!forceWinHome) {
								homeCourtFactor = 1 / homeCourtFactor;
							}
						} else {
							// Keep track of homeWonCounter only after START_CHANGING_HOME_COURT_ADVANTAGE
							if (homeWonLastGame) {
								homeWonCounter += 1;
							} else {
								homeWonCounter -= 1;
							}

							// Scale from 1 to 3, where 3 happens when homeWonCounter is 1000
							homeCourtFactor =
								1 + Math.min(2, (Math.abs(homeWonCounter) * 2) / 1000);

							if (homeWonCounter > 0) {
								homeCourtFactor = 1 / homeCourtFactor;
							}
						}
					}

					const result = getResult({
						gid: game.gid,
						day: game.day,
						teams: helpers.deepCopy(teamsInput), // So stats start at 0 each time
						doPlayByPlay,
						homeCourtFactor,
					});

					const winner = getWinner([result.team[0].stat, result.team[1].stat]);
					let wonTid: number | undefined;
					if (winner === 0) {
						wonTid = result.team[0].id;
						homeWonLastGame = true;
					} else if (winner === 1) {
						wonTid = result.team[1].id;
						homeWonLastGame = false;
					}

					if (
						(forceTie && wonTid === undefined) ||
						(!forceTie && wonTid === game.forceWin)
					) {
						found = true;
						(result as any).forceWin = i + 1;
						results.push(result);
						break;
					}
				}

				if (!found) {
					const teamInfoCache = g.get("teamInfoCache");

					let suffix: string;
					if (game.forceWin === "tie") {
						const t = teamInfoCache[game.homeTid]!;
						const t2 = teamInfoCache[game.awayTid]!;

						suffix = `the ${t.region} ${t.name} tied the ${t2.region} ${
							t2.name
						}`;
					} else {
						const otherTid = forceWinHome ? game.awayTid : game.homeTid;
						const t = teamInfoCache[game.forceWin]!;
						const t2 = teamInfoCache[otherTid]!;

						suffix = `the ${t.region} ${t.name} beat the ${t2.region} ${
							t2.name
						}`;
					}

					logEvent(
						{
							type: "error",
							text: `Could not find a simulation in ${helpers.numberWithCommas(
								NUM_TRIES,
							)} tries where ${suffix}.`,
							showNotification: true,
							persistent: true,
							saveToDb: false,
						},
						conditions,
					);
					await lock.set("stopGameSim", true);
				}
			} else {
				// Only do neutralSite when not forcing a win, since forcing a win uses homeCourtFactor and I don't want to worry about how that interacts with neutralSite
				const neutralSite =
					g.get("phase") === PHASE.PLAYOFFS &&
					(g.get("neutralSite") === "playoffs" ||
						(g.get("neutralSite") === "finals" && game.finals));

				const result = getResult({
					gid: game.gid,
					day: game.day,
					teams: teamsInput,
					doPlayByPlay,
					neutralSite,
				});
				results.push(result);
			}
		}

		await cbSaveResults(results, dayOver, schedule[0]?.day);
	};

	// Simulates a day of games. If there are no games left, it calls cbNoGames.
	// Promise is resolved after games are run
	const cbPlayGames = async () => {
		await updateStatus(`Playing (${helpers.daysLeft(false, numDays)})`);

		let schedule = await season.getSchedule(true);

		// Which of the scheduled games already have a final result saved. The
		// games cache holds this season's games keyed by gid, so a hit means a
		// completed game.
		const playedGids = new Set<number>();
		for (const game of schedule) {
			if ((await idb.cache.games.get(game.gid)) !== undefined) {
				playedGids.add(game.gid);
			}
		}

		// SWEEP ORPHANS FIRST - scheduled games that already have a saved result
		// (see orphanedSchedule.ts for the field incident). Deleting the row is
		// what the sim that produced the result would have done; doing it here
		// un-wedges the day, and the deletions ride this sim's changeset so the
		// whole room heals from one device pressing Sim. Done BEFORE
		// scheduleForSim so a single-game sim of an orphan falls into the honest
		// requestedGameMissing path ("already been played") rather than the
		// fence's "someone else got there first, the result will appear in a
		// moment" - which, for an orphan, never comes true.
		const orphaned = orphanedScheduleGids(schedule, (gid) =>
			playedGids.has(gid),
		);
		if (orphaned.length > 0) {
			syncDebugLog("schedule:orphans-swept", {
				day: schedule[0]?.day,
				gids: orphaned,
			});
			for (const gid of orphaned) {
				await idb.cache.schedule.delete(gid);
			}
			// Tell the page. The sweep's most common trigger is someone pressing
			// Sim on the dead card from the Daily Schedule, a path that then bails
			// without simming anything - and with no update event, that page kept
			// rendering the card it had just deleted until something unrelated
			// refreshed it. A field report confirmed exactly that: the log showed
			// orphans-swept and the publish, and the screen showed no change.
			await toUI("realtimeUpdate", [["gameSim"]]);
			schedule = await season.getSchedule(true);
		}

		// If live game sim, only do that one game, not the whole day.
		const plan = scheduleForSim(schedule, gidOneGame);
		schedule = plan.games;
		const dayOver = plan.dayOver;

		// THE GAME ASKED FOR IS NOT ON THE SCHEDULE - already played, by this
		// device moments ago or by another one. Nothing to sim, and stopping is
		// the only safe move.
		//
		// Falling through was catastrophic in the playoffs. Below, an empty
		// schedule during PHASE.PLAYOFFS is read as "the next playoff day hasn't
		// been generated yet", so it generates one - and then re-reads the
		// schedule WITHOUT the single-game filter and sims every game on it. Ask
		// to watch one game that is already over and the whole next day of the
		// playoffs runs behind you: exactly the "I live-simmed game 5, and when I
		// left, game 6 had already been played" report. A request to play one
		// game must never be able to advance the timeline.
		if (plan.requestedGameMissing) {
			syncDebugLog("sim:live-game-already-played", { gid: gidOneGame });
			// Say so. This is the ordinary way a Watch click fails now that auto
			// play sims the day on a timer: the countdown ran out, the day went,
			// and the button on screen is a second stale. Silence here is what
			// left the live game page loading forever.
			logEvent(
				{
					type: "error",
					text: "That game has already been played, so there's nothing to watch. Its box score is in your game log.",
					saveToDb: false,
				},
				conditions,
			);
			return cbNoGames();
		}

		// Server-side fence: exactly one device per (season, day, games) may sim,
		// no matter what the advisory authority doc says. Without this, an
		// authority-handoff race lets two devices sim the same day - their game
		// records collide by gid (one sim survives) while every incremental
		// aggregate (team records, headToHeads, player stats) double-applies and
		// permanently diverges from the game log. Claim BEFORE any timeline
		// mutation; a rejection means someone else already simmed these games, so
		// skip and catch up instead. No-op outside a sync room.
		const claimDayOrBail = async (games: ScheduleGame[]) => {
			const granted = await claimSimDayFence(
				games[0]!.day,
				games.map((game) => game.gid),
			);
			if (!granted) {
				// Name what was actually refused. Now that a user can sim their own
				// single game while someone else runs the day, the common rejection is
				// two people reaching for the SAME GAME - and telling them "this day"
				// was already simmed describes something that did not happen.
				logEvent(
					{
						type: "error",
						text:
							gidOneGame === undefined
								? `Another device already simmed this day, so this sim was skipped. Catching up to the cloud now.`
								: `Someone else got to this game first, so it wasn't simmed here. Catching up to the cloud now — the result will appear in a moment.`,
						persistent: true,
					},
					conditions,
				);
			}
			return granted;
		};

		const stop = await getPendingSimStop();
		if (stop) {
			// A configured stop is the one place a sim deliberately does not run
			// straight through. Alone that costs one extra press, and in a shared
			// league it is the ready-up gate: the evaluator crosses once every team
			// has said they are done, so simming harder can't skip the room.
			// Checked BEFORE claiming the day, so bailing leaves the day unconsumed
			// and - at the deadline - the sentinel in place for whoever does cross.
			if (shouldStopAtSimStop(start)) {
				// Say why, or a press of Sim Day looks like it did nothing.
				const what =
					stop.kind === "deadline" ? "Trade deadline" : `Day ${stop.day}`;
				if (isTradeDeadlineGateActive()) {
					void notifySimStopArrived(what);
					logEvent(
						{
							type: "info",
							text: `${what}. The league sims on once every team has readied up.`,
							saveToDb: false,
						},
						conditions,
					);
				} else {
					logEvent(
						{
							type: "info",
							text: `${what}. Make your moves — simming again crosses it.`,
							saveToDb: false,
						},
						conditions,
					);
				}
				return cbNoGames();
			}
		}

		if (isTradeDeadlineGame(schedule[0])) {
			if (!(await claimDayOrBail(schedule))) {
				return cbNoGames();
			}
			await idb.cache.schedule.delete(schedule[0]!.gid);
			await phase.newPhase(PHASE.AFTER_TRADE_DEADLINE, conditions);
			await toUI("deleteGames", [[schedule[0]!.gid]]);
			await play(numDays - 1, conditions, false);
		} else {
			// This should also call cbNoGames after the playoffs end, because g.get("phase") will have been incremented by season.newSchedulePlayoffsDay after the previous day's games
			if (schedule.length === 0 && g.get("phase") !== PHASE.PLAYOFFS) {
				return cbNoGames();
			}

			const tids = new Set<number>();

			// Will loop through schedule and simulate all games
			if (schedule.length === 0 && g.get("phase") === PHASE.PLAYOFFS) {
				// Sometimes the playoff schedule isn't made the day before, so make it now
				// This works because there should always be games in the playoffs phase. The next phase will start before reaching this point when the playoffs are over.
				await season.newSchedulePlayoffsDay();
				schedule = await season.getSchedule(true);
			}

			if (schedule.length > 0 && !(await claimDayOrBail(schedule))) {
				return cbNoGames();
			}

			for (const matchup of schedule) {
				tids.add(matchup.homeTid);
				tids.add(matchup.awayTid);
			}

			const teams = await loadTeams(Array.from(tids), conditions); // Play games

			await cbSimGames(schedule, teams, dayOver);
		}
	};

	// This simulates a day, including game simulation and any other bookkeeping that needs to be done
	const cbRunDay = async () => {
		const userTeamSizeError = await team.checkRosterSizes("user");

		if (!userTeamSizeError) {
			// A sim ran (or is about to): reset the roster-block dedup so a future
			// block - even the same team going over again - announces fresh.
			clearRosterBlockNotice();
			await updatePlayMenu();

			if (numDays > 0) {
				// If we didn't just stop games, let's play
				// Or, if we are starting games (and already passed the lock), continue even if stopGameSim was just seen
				const stopGameSim = lock.get("stopGameSim");

				if (start || !stopGameSim) {
					// If start is set, then reset stopGames
					if (stopGameSim) {
						await lock.set("stopGameSim", false);
					}

					if (g.get("phase") !== PHASE.PLAYOFFS) {
						await team.checkRosterSizes("other");
					}

					await cbPlayGames();
				} else {
					// Update UI if stopped
					await cbNoGames();
				}
			} else {
				// Not sure why we get here sometimes, but we do
				const playoffsOver =
					g.get("phase") === PHASE.PLAYOFFS &&
					(await season.newSchedulePlayoffsDay());
				await cbNoGames(playoffsOver);
			}
		} else {
			await lock.set("gameSim", false); // Counteract auto-start in lock.canStartGames
			await updatePlayMenu();
			await updateStatus("Idle");
			logEvent(
				{
					type: "error",
					text: userTeamSizeError,
					saveToDb: false,
				},
				conditions,
			);
			// In a synced league the sim only runs on the simmer's device, so the
			// error above is local to it - a follower just sees the timer come and
			// go. Announce to the room why the sim was skipped (no-op offline).
			void notifyRosterBlockedSim();
		}
	};

	// If this is a request to start a new simulation... are we allowed to do
	// that? If so, set the lock and update the play menu
	if (start) {
		// A single-game sim (gidOneGame set: a live sim or "Sim one game") must
		// never push a phone notification - only a full day/week/month sim does.
		// Flag the whole window up front (before any game is written or the live
		// game navigates) so afterAction stays silent no matter what drains the
		// changeset; a day sim (gidOneGame undefined) clears it so it notifies.
		setSingleGameSimActive(gidOneGame !== undefined);

		// A game being WATCHED is different from one silently simmed: the room
		// should hear about it, just not while the watcher is still on Q1. Hold
		// its pushes here and release them when the playback ends. Without this
		// they were dropped outright, which is why a playoff run watched game by
		// game announced nothing to anyone.
		if (gidOneGame !== undefined && playByPlay) {
			beginLiveSimNotificationHold();
		}

		const canStartGames = lock.canStartGames();

		// A silently-refused sim reads as a broken button. Name the reason.
		//
		// Unconditional, not just in a synced room: the play menu disables itself
		// while a sim runs, but the Watch/Sim buttons on the daily schedule do
		// not, and auto play can take the lock between the page rendering and the
		// click landing. That is now the most likely press to be refused, and it
		// used to be refused in complete silence.
		if (!canStartGames) {
			syncDebugLog("sim:cannot-start", {
				gameSim: lock.get("gameSim"),
				newPhase: lock.get("newPhase"),
			});
			logEvent(
				{
					type: "error",
					text: lock.get("newPhase")
						? "Can't sim: a phase change is still finishing (or a previous one didn't finish cleanly). If this persists, reload the page."
						: "Can't sim: a sim is already running (or a previous one didn't finish cleanly). If this persists, reload the page.",
					persistent: true,
				},
				conditions,
			);
		}

		try {
			if (canStartGames) {
				// Bracket the whole sim (all days, including the recursive continuations
				// nested inside cbRunDay) so a concurrent runSuppressed call can't swallow
				// the sim's interleaved writes and leave its delta unpublished.
				changeTracker.beginSim();
				try {
					await cbRunDay();
				} finally {
					changeTracker.endSim();
				}
			}
		} finally {
			// The force-silent window must NEVER outlive this call. Its normal end
			// is in cbNoGames (right after the sim's silent drain), but a refused
			// start (canStartGames false) or an error before cbNoGames used to
			// leave the flag stuck on - silencing EVERY notification from this
			// device (phase changes included) until a page refresh, while sync
			// itself kept working so nothing looked wrong.
			if (gidOneGame !== undefined) {
				setSingleGameSimActive(false);
			}
		}
	} else {
		await cbRunDay();
	}

	return liveSimDelivered;
};

export default play;
