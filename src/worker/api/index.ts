import { sanitizeRotation, type TeamRotation } from "../../common/rotation.ts";
import { csvFormat, csvFormatRows } from "d3-dsv";
import type { FaceConfig } from "facesjs";
import {
	GAME_ACRONYM,
	PHASE,
	PHASE_TEXT,
	PLAYER,
	PLAYER_STATS_TABLES,
	RATINGS,
	DEFAULT_JERSEY,
	DEFAULT_TEAM_COLORS,
	POSITIONS,
	GRACE_PERIOD,
	LEAGUE_DATABASE_VERSION,
	REAL_PLAYERS_INFO,
	DEFAULT_RECAP_MAX_GAMES,
	DEFAULT_RECAP_MAX_DAYS,
	DEFAULT_RECAP_MAX_PLAYERS,
} from "../../common/constants.ts";
import { DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS } from "../../common/ownGameSim.ts";
import actions from "./actions.ts";
import * as awardSettings from "./awardSettings.ts";
import leagueFileUpload, {
	decompressStreamIfNecessary,
	emitProgressStream,
	parseJSON,
} from "./leagueFileUpload.ts";
import processInputs from "./processInputs.ts";
import {
	allStar,
	contractNegotiation,
	draft,
	finances,
	league,
	phase,
	player,
	team,
	trade,
	expansionDraft,
	realRosters,
	freeAgents,
	season,
} from "../core/index.ts";
import { idb } from "../db/index.ts";
import {
	coarsenPlayerForDisplay,
	exemptFromCoarseRatings,
	prospectRatingsSeason,
} from "../../common/coarsenRating.ts";
import {
	achievement,
	g,
	helpers,
	local,
	lock,
	updatePlayMenu,
	updateStatus,
	toUI,
	updatePhase,
	logEvent,
} from "../util/index.ts";
import * as views from "../views/index.ts";
import {
	type Conditions,
	type Env,
	type GameAttributesLeague,
	type Local,
	type LockName,
	type Player,
	type PlayerWithoutKey,
	type UpdateEvents,
	type TradeTeams,
	type MinimalPlayerRatings,
	type Relative,
	type Options,
	type ExpansionDraftSetupTeam,
	type GetLeagueOptions,
	type TeamSeasonWithoutKey,
	type ScheduledEvent,
	type ScheduledEventGameAttributes,
	type ScheduledEventTeamInfo,
	type ScheduleGameWithoutKey,
	type Conf,
	type Div,
	type DunkAttempt,
	type AllStarPlayer,
	type League,
	type View,
	type NonEmptyArray,
	type CourtStyle,
	type Image,
	type TradingCard,
	realPlayerPhotosSchema,
	realTeamInfoSchema,
	type Awards,
} from "../../common/types.ts";
import { getScore } from "../core/player/checkJerseyNumberRetirement.ts";
import {
	claimSyncAuthority,
	checkSyncReady,
	connectSharedLeague,
	deleteAllSyncRooms,
	deleteSyncRoom,
	pruneAllSyncRoomChanges,
	pruneSyncRoomChanges,
	endLiveBroadcast,
	disconnectSharedLeague,
	getConnectedLid,
	getSyncActivity,
	getSimSafety,
	getSyncDebugSnapshot,
	getSyncEngine,
	getSyncRequired,
	getSyncStatus,
	loadSyncDeviceName,
	refreshSyncLocalName,
	resolveSyncLocalName,
	beginLotteryReveal,
	flushDeferredRefreshAfterLive,
	listSyncRooms,
	leaveLiveBroadcast,
	markFollowedBroadcastOver,
	markSyncRequired,
	publishAutoPlayState,
	publishLotteryRevealState,
	refreshSyncUIState,
	syncNudge,
	watchLiveBroadcast,
	pushDay,
	pushUnsyncedDays,
	reportDayPush,
	reportUnsyncedDays,
	resyncSharedLeague,
	sendLiveChatMessage,
	setDraftReady,
	setFaBoard,
	teardownSharedLeague,
	updateLiveBroadcast,
} from "../core/sync/index.ts";
import { setSingleGameSimActive } from "../core/sync/afterActionHook.ts";
import { releaseLiveSimNotifications } from "../core/sync/liveSimNotificationHold.ts";
import { liveSimBlocksDaySim } from "../core/sync/liveSimDayCollision.ts";
import { setSyncDebugLogging, syncDebugLog } from "../core/sync/debugLog.ts";
import { getDayGamesForRecap } from "../util/getDayGamesForRecap.ts";
import { getSeasonRecapData } from "../util/getSeasonRecapData.ts";
import {
	getPlayerRecapData,
	getRecapPool,
	type RecapFilter,
} from "../util/getPlayerRecapData.ts";
import { removeSeasonNote, upsertSeasonNote } from "../../common/seasonNote.ts";
import { recordInjuryForensics } from "../core/sync/injuryForensics.ts";
import type { NewLeagueTeam } from "../../ui/views/NewLeague/types.ts";
import { PointsFormulaEvaluator } from "../core/team/evaluatePointsFormula.ts";
import type { Settings } from "../views/settings.ts";
import {
	getActualAttendance,
	getAutoTicketPriceByTid,
	getBaseAttendance,
} from "../core/game/attendance.ts";
import goatFormula from "../util/goatFormula.ts";
import getRandomTeams from "./getRandomTeams.ts";
import { withState } from "../core/player/name.ts";
import { initDefaults, loadNames } from "../util/loadNames.ts";
import type { PlayerRatings } from "../../common/types.basketball.ts";
import createStreamFromLeagueObject from "../core/league/create/createStreamFromLeagueObject.ts";
import type { IDBPIndex, IDBPObjectStore } from "@dumbmatter/idb";
import {
	upgradeGamesVersion65,
	type LeagueDB,
	type LeagueDBStoreNames,
} from "../db/connectLeague.ts";
import playMenu from "./playMenu.ts";
import toolsMenu from "./toolsMenu.ts";
import eightyTwoZeroDraft from "./eightyTwoZeroDraft.ts";
import addFirstNameShort from "../util/addFirstNameShort.ts";
import statsBaseball from "../core/team/stats.baseball.ts";
import { extraRatings } from "../views/playerRatings.ts";
import {
	groupByUnique,
	last,
	maxBy,
	omit,
	orderBy,
	range,
} from "../../common/utils.ts";
import {
	finalizePlayersRelativesList,
	formatPlayerRelativesList,
} from "../views/customizePlayer.ts";
import { TOO_MANY_TEAMS_TOO_SLOW } from "../core/season/getInitialNumGamesConfDivSettings.ts";
import { advancedPlayerSearch } from "./advancedPlayerSearch.ts";
import { getTradeHistoryDump } from "./tradeHistoryDump.ts";
import * as exhibitionGame from "./exhibitionGame.ts";
import { simIntrasquadGame } from "./intrasquad.ts";
import { getSummary } from "../views/trade.ts";
import { statTypes } from "../views/playerGraphs.ts";
import {
	getStats as teamGetStats,
	statTypes as teamStatTypes,
} from "../views/teamGraphs.ts";
import { DEFAULT_LEVEL } from "../../common/budgetLevels.ts";
import isUntradable from "../core/trade/isUntradable.ts";
import { offerPassesGuards } from "../core/trade/betweenAiTeams.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
	type TradePosture,
} from "../core/trade/tradePosture.ts";
import { wasTradedThisSeason } from "../core/trade/tradeMotivation.ts";
import getWinner from "../../common/getWinner.ts";
import formatScoreWithShootout from "../../common/formatScoreWithShootout.ts";
import { getStats } from "../../common/advancedPlayerSearch.ts";
import type { LookingFor } from "../core/trade/makeItWork.ts";
import type { LookingForState } from "../../ui/views/TradingBlock/useLookingForState.ts";
import { getPlayer } from "../views/player.ts";
import {
	placeBet as sportsbookPlaceBetCore,
	placeBetSlip as sportsbookPlaceBetSlipCore,
	cancelBet as sportsbookCancelBetCore,
	settleBetsIfAuthority as sportsbookSettleCore,
} from "../core/sportsbook/bets.ts";
import {
	buildCustomGrid,
	generateTriviaGrid,
	getGridCatalog,
	getTriviaFaces,
	getTriviaPlayerCard,
	type GridCriterionRef,
} from "../core/trivia/grid.ts";
import {
	getOptions,
	getPoolAndTeams,
	type EightyTwoZeroPosition,
} from "../core/trivia/eightyTwoZero.ts";
import { simulateEightyTwoZeroSeason } from "../core/trivia/eightyTwoZeroSim.ts";
import {
	generateTeamTriviaRound,
	getTeamTriviaCatalog,
	type TeamTriviaOptions,
} from "../core/trivia/teamTrivia.ts";
import { getTriviaPlayerProfile } from "../core/trivia/playerProfile.ts";
import {
	getRemoteTriviaScores,
	publishTriviaScores,
} from "../core/sync/triviaScores.ts";
import type { SportsbookMarket } from "../../common/types.ts";
import type { NoteInfo } from "../../ui/views/Player/Note.tsx";
import { beforeLeague, beforeNonLeague } from "../util/beforeView.ts";
import loadData from "../core/realRosters/loadData.basketball.ts";
import formatPlayerFactory from "../core/realRosters/formatPlayerFactory.ts";
import { applyRealPlayerPhotos } from "../core/league/processPlayerNewLeague.ts";
import {
	getTradingCardSeasons,
	getTradingCardSubject,
} from "../util/getTradingCardSubject.ts";
import {
	buildCardBackPrompt,
	buildCardFrontPrompt,
} from "../../common/tradingCardPrompt.ts";
import { cardTitle } from "../../common/tradingCards.ts";
import type { SocialAccount } from "../../common/socialAccounts.ts";
import { clearSocialFeedCache } from "../util/socialFeed.ts";
import {
	achievementPromptOverride,
	CHAMPION_CARD_PLAYERS,
	DEFAULT_ACHIEVEMENT_DRAFT_PICKS,
	deriveDraftAchievementCards,
	deriveSeasonAchievementCards,
	type AchievementCardSpec,
	type AchievementKind,
	type DraftCardScene,
} from "../../common/achievementCards.ts";
import { actualPhase } from "../util/actualPhase.ts";
import { getGlobalSettings } from "../util/getGlobalSettings.ts";
import { getCol } from "../../common/getCol.ts";
import { getCols } from "../../common/getCols.ts";
import { formatScheduleForEditor } from "../views/scheduleEditor.ts";
import type { KeyboardShortcutsLocal } from "../../ui/util/keyboardShortcuts.ts";
import { getNumPlayoffTeamsRaw } from "../core/season/getNumPlayoffTeams.ts";
import type { NewLeagueSettings } from "../views/newLeague.ts";
import { getNumPlayersTradedAwayNormalizedAll } from "../core/player/getNumPlayersTradedAwayNormalized.ts";
import { getAdjustedTicketPrice } from "../../common/getAdjustedTicketPrice.ts";
import { gameAttributesArrayToObject } from "../../common/gameAttributesArrayToObject.ts";
import { bySport, isSport } from "../../common/sportFunctions.ts";
import { generateContractOptions } from "../core/contractNegotiation/generateContractOptions.ts";
import getRealTeamPlayerData from "../core/league/create/getRealTeamPlayerData.ts";
import * as z from "zod";
import { defaultTragicDeaths } from "../util/defaultTragicDeaths.ts";
import { defaultInjuries } from "../util/defaultInjuries.ts";
import { checkNaNs } from "../util/checkNaNs.ts";
import { checkChanges } from "../util/checkChanges.ts";
import { checkAccount } from "../util/checkAccount.ts";
import { generateFace } from "../util/face.ts";
import { choice } from "../../common/random.ts";
import { getNewLeagueLid } from "../util/getNewLeagueLid.ts";
import { env } from "../util/env.ts";
import { recomputeLocalUITeamOvrs } from "../util/recomputeLocalUITeamOvrs.ts";
import { initUILocalGames } from "../util/initUILocalGames.ts";
import { ValueChangeCalculator } from "../core/team/ValueChangeCalculator.ts";
import type { GenOrderResult } from "../core/draft/genOrder.ts";
import { allowCrossingNextSimStop } from "../core/sync/tradeDeadlineGate.ts";
import { parseSimStopDays, stopsOnDay } from "../../common/simStopDays.ts";
import { revertAppearance } from "../../common/playerAppearance.ts";
import {
	getAwardsByPlayer,
	updatePlayerAwards,
} from "../core/awards/awardsByPlayer.ts";
import { legacyAwardsWithNames } from "../util/legacyAwards.ts";

const acceptContractNegotiation = async ({
	pid,
	amount,
	exp,
}: {
	pid: number;
	amount: number;
	exp: number;
}) => {
	const negotiation = await contractNegotiation.get(pid);
	if (typeof negotiation === "string") {
		return negotiation;
	}

	const response = await contractNegotiation.accept({
		negotiation,
		amount,
		exp,
	});

	// string response is an error message
	if (typeof response === "string") {
		return response;
	}

	// Only do this if there was no error, and don't await because it makes the UI slow
	void contractNegotiation.afterAccept(negotiation.tid);

	local.undoableActions[pid] = response;
};

const addTeam = async () => {
	const did = g.get("divs")[0].did;

	const t = await team.addNewTeamToExistingLeague({
		did,
		region: "Region",
		name: "Name",
		abbrev: "ZZZ",
		pop: 1,
		imgURL: undefined,
	});

	await idb.cache.flush();

	// Team format used in ManageTemas
	return {
		tid: t.tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		imgURL: t.imgURL,
		imgURLSmall: t.imgURLSmall ?? "",
		did: t.did,
		disabled: t.disabled,
		jersey: t.jersey ?? DEFAULT_JERSEY,
		pop: t.pop!, // See comment in types.ts about upgrade
		stadiumCapacity: t.stadiumCapacity!, // See comment in types.ts about upgrade
		colors: t.colors,
	};
};

const allStarDraftAll = async () => {
	const pids = await allStar.draftAll();
	return pids;
};

const allStarDraftOne = async () => {
	const { finalized, pid } = await allStar.draftOne();
	return {
		finalized,
		pid,
	};
};

const allStarDraftUser = async (pid: number) => {
	const finalized = await allStar.draftUser(pid);
	return finalized;
};

const allStarDraftReset = async () => {
	const allStars = await idb.cache.allStars.get(g.get("season"));
	if (allStars) {
		allStars.finalized = false;

		// Ideally it would put them back in the same order it started, but that's hard, so just assume draft was old order
		const oldRemaining = allStars.remaining;
		allStars.remaining = [];

		// Interleave teams
		const maxIndex = Math.max(
			allStars.teams[0].length,
			allStars.teams[1].length,
		);
		for (let i = 1; i < maxIndex; i++) {
			for (const t of [0, 1] as const) {
				const p = allStars.teams[t][i];
				if (p) {
					allStars.remaining.push(p);
				}
			}
		}

		allStars.remaining.push(...oldRemaining);

		allStars.teams = [[allStars.teams[0][0]!], [allStars.teams[1][0]!]];

		await idb.cache.allStars.put(allStars);

		await toUI("realtimeUpdate", [["playerMovement"]]);
	}
};

const allStarDraftSetPlayers = async (
	players: {
		teams: [AllStarPlayer[], AllStarPlayer[]];
		remaining: AllStarPlayer[];
	},
	conditions: Conditions,
) => {
	const allStars = await idb.cache.allStars.get(g.get("season"));
	if (allStars) {
		const prevPids = [
			...allStars.teams[0],
			...allStars.teams[1],
			...allStars.remaining,
		].map((p) => p.pid);

		const newPlayers = [
			...players.teams[0],
			...players.teams[1],
			...players.remaining,
		];

		const newPids = newPlayers.map((p) => p.pid);

		const pidsToDelete = prevPids.filter((pid) => !newPids.includes(pid));

		// Delete old awards
		const awardsToDelete = pidsToDelete.map((pid) => ({
			pid,
			award: { type: "All-Star" },
		}));

		// Add new awards
		const awardsToSave = newPlayers
			.filter((p) => !prevPids.includes(p.pid))
			.map((p) => ({
				pid: p.pid,
				tid: p.tid,
				name: p.name,
				award: { type: "All-Star" },
			}));
		await updatePlayerAwards({
			awardsToDelete,
			awardsToSave,
			logEventInfo: {
				conditions,
			},
			season: g.get("season"),
		});

		// Save new All-Stars
		allStars.teams = players.teams;
		allStars.remaining = players.remaining;
		if (allStars.type === "draft") {
			for (const i of [0, 1] as const) {
				const p = await idb.cache.players.get(allStars.teams[i][0]!.pid);
				if (p) {
					allStars.teamNames[i] = `Team ${p.firstName}`;
				}
			}
		}
		await idb.cache.allStars.put(allStars);

		await toUI("realtimeUpdate", [["playerMovement"]]);
	}
};

const allStarGameNow = async () => {
	const currentPhase = g.get("phase");
	if (
		currentPhase != PHASE.REGULAR_SEASON &&
		currentPhase !== PHASE.AFTER_TRADE_DEADLINE
	) {
		return;
	}

	let schedule = (await season.getSchedule()).map((game) => {
		const newGame: ScheduleGameWithoutKey = {
			...game,
		};
		// Delete gid, so ASG added to beginning will be in order
		delete newGame.gid;
		return newGame;
	});

	// Does ASG exist in schedule? If so, delete it.
	schedule = schedule.filter(
		(game) => game.awayTid !== -2 || game.homeTid !== -1,
	);

	// Add 1 to each day, so we can fit in ASG
	for (const game of schedule) {
		game.day += 1;
	}

	// Add new ASG to front of schedule, and adjust days
	schedule.unshift({
		awayTid: -2,
		homeTid: -1,
		day: schedule[0] ? schedule[0].day - 1 : 0,
	});

	await idb.cache.schedule.clear();
	for (const game of schedule) {
		await idb.cache.schedule.add(game);
	}

	await initUILocalGames();
	await updatePlayMenu();
	await toUI("realtimeUpdate", [["gameSim"]]);
};

const autoSortRoster = async ({
	pos,
	tids,
}: {
	pos?: string;
	tids?: number[];
} = {}) => {
	const tids2 = tids ?? [g.get("userTid")];

	for (const tid of tids2) {
		await team.rosterAutoSort(
			tid,
			false,
			typeof pos === "string" ? pos : undefined,
		);
	}
	await toUI("realtimeUpdate", [["playerMovement"]]);
};

const beforeView = async (
	{
		inLeague,
		lidCurrent,
		lidUrl,
	}: {
		inLeague: boolean;
		lidCurrent: number | undefined;
		lidUrl: number | undefined;
	},
	conditions: Conditions,
) => {
	if (inLeague) {
		// idb.league check is for Safari weirdness - seems we need to reinitialize state sometimes because it is lost? idk
		if (
			lidUrl !== undefined &&
			(lidUrl !== lidCurrent || idb.league === undefined)
		) {
			await beforeLeague(lidUrl, conditions);
		}
	} else {
		// TEMP DISABLE WITH ESLINT 9 UPGRADE eslint-disable-next-line no-lonely-if
		if (lidCurrent !== undefined) {
			await beforeNonLeague(conditions);
		}
	}
};

const cancelContractNegotiation = async (pid: number) => {
	await contractNegotiation.cancel(pid);

	local.undoableActions[pid] = {
		type: "release",
		tid: g.get("userTid"),
	};

	await toUI("realtimeUpdate", [["playerMovement"]]);
};

const checkAccount2 = (param: unknown, conditions: Conditions) =>
	checkAccount(conditions);

const checkParticipationAchievement = async (
	force: boolean,
	conditions: Conditions,
) => {
	if (force) {
		await achievement.add(["participation"], conditions, "normal");
	} else {
		const achievements = await achievement.getAll();
		const participationAchievement = achievements.find(
			({ slug }) => slug === "participation",
		);

		if (participationAchievement && participationAchievement.normal === 0) {
			await achievement.add(["participation"], conditions, "normal");
		}
	}
};

const clearInjuries = async (pids: number[] | "all") => {
	const players =
		pids === "all"
			? await idb.cache.players.getAll()
			: await idb.getCopies.players({ pids }, "noCopyCache");

	for (const p of players) {
		if (p.injury.gamesRemaining > 0) {
			// Adjust injuries log
			const lastInjuriesEntry = p.injuries.at(-1);
			if (lastInjuriesEntry?.type === p.injury.type) {
				lastInjuriesEntry.games -= p.injury.gamesRemaining;
				if (lastInjuriesEntry.games <= 0) {
					// Injury was cleared before any days were simmed
					p.injuries.pop();
				}
			}

			p.injury = {
				type: "Healthy",
				gamesRemaining: 0,
			};
			await idb.cache.players.put(p);
		}
	}

	await toUI("realtimeUpdate", [["playerMovement"]]);
	await recomputeLocalUITeamOvrs();
};

const noteUpdateEvents: Record<NoteInfo["type"], UpdateEvents> = {
	draftPick: ["notes", "playerMovement"],
	game: ["notes"],
	player: ["notes", "playerMovement"],
	teamSeason: ["notes", "team"],
	// A day recap lives on a game record but shows on the Daily Schedule; "notes"
	// is what that view refreshes on.
	day: ["notes"],
};

const clearNotes = async (type: NoteInfo["type"]) => {
	if (type === "day") {
		// Day recaps live on game records (Game.dayNote), not their own store, and
		// the Notes page never bulk-clears them, so there's nothing to sweep here.
		// An individual day recap is cleared by re-filing an empty note via setNote.
		return;
	}
	const storeName = `${type}s` as const;
	const rows = await idb.getCopies[storeName](
		{
			note: true,
		},
		"noCopyCache",
	);
	for (const row of rows) {
		delete row.note;
		delete row.noteBool;
		await idb.cache[storeName].put(row as any);
	}

	await toUI("realtimeUpdate", [noteUpdateEvents[type]]);
};

const getUpdateWatch = (players: Player[]) => {
	const updateWatch: Record<number, number> = {};
	for (const p of players) {
		updateWatch[p.pid] = p.watch ?? 0;
	}
	return updateWatch;
};

const clearWatchList = async (type: "all" | number) => {
	const players = await idb.getCopies.players(
		{
			watch: true,
		},
		"noCopyCache",
	);
	for (const p of players) {
		if (type === "all" || p.watch === type) {
			delete p.watch;
			await idb.cache.players.put(p);
		}
	}

	await Promise.all([
		toUI("crossTabEmit", [["updateWatch", getUpdateWatch(players)]]),
		toUI("realtimeUpdate", [["playerMovement", "watchList"]]),
	]);
};

const countNegotiations = async () => {
	const negotiations = await idb.cache.negotiations.getAll();
	return negotiations.length;
};

const createLeague = async (
	{
		name,
		tid,
		file,
		url,
		shuffleRosters,
		importLid,
		getLeagueOptions,
		keptKeys,
		confs,
		divs,
		teamsFromInput,
		settings,
		fromFile,
		startingSeasonFromInput,
		leagueCreationID,
	}: {
		name: string;
		tid: number;
		file: File | undefined;
		url: string | undefined;
		shuffleRosters: boolean;
		importLid: number | undefined | null;
		getLeagueOptions: GetLeagueOptions | undefined;
		keptKeys: string[];
		confs: Conf[];
		divs: Div[];
		teamsFromInput: NewLeagueTeam[];
		settings: NewLeagueSettings;
		fromFile: {
			gameAttributes: Record<string, unknown> | undefined;
			hasRookieContracts: boolean;
			// True when the file's meta carries a syncCheckpoint - a machine-generated
			// export of a synced league. createStream then PRESERVES autoincrement
			// primary keys instead of renumbering them, so this device addresses
			// records by the same keys as the rest of the sync room (renumbering is
			// what let synced writes land on - and overwrite - unrelated rows).
			hasSyncCheckpoint?: boolean;
			maxGid: number | undefined;
			startingSeason: number | undefined;
			teams: any[] | undefined;
			version: number | undefined;
		};
		startingSeasonFromInput: string | undefined;
		leagueCreationID: string;
	},
	conditions: Conditions,
): Promise<number> => {
	const keys = new Set([
		...keptKeys,
		"startingSeason",
		"version",
	]) as Set<LeagueDBStoreNames>;

	const setLeagueCreationStatus = (status: string) => {
		toUI(
			"updateLocal",
			[
				{
					leagueCreation: {
						id: leagueCreationID,
						status,
					},
				},
			],
			conditions,
		);
	};

	setLeagueCreationStatus("Initializing...");

	// A sync session belongs to ONE league file. Kill any live session BEFORE the
	// import starts - not when the new league later loads - or the old room keeps
	// applying its deltas into (and capturing changes from) the new file's cache
	// for the whole (possibly long) import. The old league's persisted session is
	// kept, so reopening it reconnects.
	await teardownSharedLeague({ clearPersisted: false });

	// Close the currently open league (if any) BEFORE streaming the new one in.
	// On a SharedWorker (desktop keeps ONE worker across every tab and league),
	// leaving it open means its cache auto-flush, background view loads, and -
	// when importing over the same league slot - the mid-import remove() all
	// keep transacting on a handle that then gets closed underneath them, which
	// surfaced as repeated "Failed to execute 'transaction' on 'IDBDatabase':
	// The database connection is closing" failures that killed the import. The
	// import ends by navigating to the new league anyway, so there is nothing
	// the old league still needs to do; close it cleanly (flushes dirty cache)
	// while it is still coherent.
	if (g.get("lid") !== undefined) {
		await league.close(true);
		await toUI("resetLeague", []);
	}

	let actualTid = tid;
	let stream: ReadableStream | undefined;

	// A file exported from a synced league carries a checkpoint in its meta
	// object: the room fingerprint it belongs to plus the change-log position
	// its data already includes. Sniff it (and which stores the file contains)
	// while the stream parses, and apply it to the new league's meta row below -
	// so re-importing an up-to-date export joins its room and catches up from
	// the checkpoint instead of replaying the entire room history from zero.
	let syncCheckpoint: { leagueId: string; watermark: number } | undefined;
	const fileStoreKeys = new Set<string>();
	const sniffSyncCheckpoint = new TransformStream<any, any>({
		transform(chunk, controller) {
			const key = chunk?.key;
			if (key === "meta") {
				const cp = chunk.value?.syncCheckpoint;
				if (
					cp &&
					typeof cp.leagueId === "string" &&
					typeof cp.watermark === "number"
				) {
					syncCheckpoint = { leagueId: cp.leagueId, watermark: cp.watermark };
				}
			} else if (typeof key === "string") {
				fileStoreKeys.add(key);
			}
			controller.enqueue(chunk);
		},
	});

	if (getLeagueOptions) {
		const realLeague = await realRosters.getLeague(getLeagueOptions);

		if (getLeagueOptions.type === "real") {
			if (getLeagueOptions.realStats === "all") {
				keys.add("awards");
				keys.add("playoffSeries");
			}

			if (getLeagueOptions.phase >= PHASE.PLAYOFFS) {
				keys.add("awards");
				keys.add("draftLotteryResults");
				keys.add("draftPicks");
				keys.add("playoffSeries");
			}
		}

		// Since inactive teams are included if realStats=="all", need to translate tid and overwrite fromFile.teams
		if (
			getLeagueOptions.type === "real" &&
			getLeagueOptions.realStats === "all"
		) {
			const srID = fromFile.teams![tid].srID;
			actualTid = realLeague.teams.findIndex((t) => t.srID === srID);
			if (!srID || actualTid < 0) {
				throw new Error("Error finding tid");
			}
		}

		// Definitley need this for realStats=="all", but maybe elsewhere too. This is needed because we don't know if we're keeping history or not when we call getLeagueInfo to display the team/settings in the UI.
		fromFile.gameAttributes = realLeague.gameAttributes;
		fromFile.startingSeason = realLeague.startingSeason;
		fromFile.teams = realLeague.teams;

		stream = createStreamFromLeagueObject(realLeague);
	} else if (file || url) {
		let baseStream: ReadableStream;
		let sizeInBytes: number | undefined;
		if (file) {
			baseStream = file.stream();
			sizeInBytes = file.size;
		} else {
			const response = await fetch(url!);
			if (!response.ok) {
				throw new Error(`HTTP error ${response.status}`);
			}
			baseStream = response.body as ReadableStream;
			const size = response.headers.get("content-length");
			if (size) {
				sizeInBytes = Number(size);
			}
		}

		const stream0 = baseStream;

		// I HAVE NO IDEA WHY THIS LINE IS NEEDED, but without this, Firefox seems to cut the stream off early
		(self as any).stream0 = stream0;

		stream = (
			await decompressStreamIfNecessary(
				stream0.pipeThrough(
					emitProgressStream(leagueCreationID, sizeInBytes, conditions),
				),
			)
		)
			.pipeThrough(new TextDecoderStream())
			.pipeThrough(parseJSON())
			.pipeThrough(sniffSyncCheckpoint);
	} else {
		stream = createStreamFromLeagueObject({});
	}

	if (!stream) {
		throw new Error("No stream");
	}

	const lid = importLid ?? (await getNewLeagueLid());

	await league.createStream(stream, {
		conditions,
		confs,
		divs,
		fromFile,
		getLeagueOptions,
		lid,
		keptKeys: keys,
		name,
		setLeagueCreationStatus,
		settings,
		shuffleRosters,
		startingSeasonFromInput,
		teamsFromInput,
		tid: actualTid,
	});

	delete (self as any).stream0;

	// A (re)created league is a NEW file: it must never inherit a previous file's
	// room session, watermark, or room binding. This lid can carry stale sync
	// state two ways - importing over an existing league (importLid keeps its
	// meta row), and lid reuse (new lid = newest lid + 1, so deleting the newest
	// league recycles its lid).
	const metaLeague = await idb.meta.get("leagues", lid);
	if (metaLeague) {
		delete metaLeague.syncCode;
		delete metaLeague.syncIsHost;
		delete metaLeague.syncWatermark;
		delete metaLeague.syncLeagueId;

		// If the file carried a sync checkpoint AND this import faithfully kept
		// everything the file contains (no dropped stores, no roster shuffling),
		// the new league IS the state that checkpoint describes - stamp the room
		// fingerprint and watermark so joining the room catches up from there
		// instead of replaying the whole history. Any deviation falls back to a
		// full replay, which is slower but always converges (idempotent).
		const missingStores = [...fileStoreKeys].filter(
			(key) =>
				key !== "gameAttributes" &&
				key !== "startingSeason" &&
				key !== "version" &&
				!keys.has(key as any),
		);
		const keptEverything = missingStores.length === 0;
		const applied =
			!!syncCheckpoint &&
			keptEverything &&
			!shuffleRosters &&
			!settings.giveMeWorstRoster;
		if (applied) {
			metaLeague.syncLeagueId = syncCheckpoint!.leagueId;
			metaLeague.syncWatermark = syncCheckpoint!.watermark;
		}

		// One-time line so a re-import that still catches up from zero shows
		// exactly why the checkpoint fast-forward was (not) applied.
		syncDebugLog("import:checkpoint", {
			applied,
			hasCheckpoint: !!syncCheckpoint,
			checkpointWatermark: syncCheckpoint?.watermark,
			keptEverything,
			missingStores,
			fileStores: [...fileStoreKeys],
			shuffleRosters,
			worstRoster: !!settings.giveMeWorstRoster,
		});

		await idb.meta.put("leagues", metaLeague);
	} else {
		// No meta row at this point means the checkpoint can't be stamped - the
		// re-import would replay from zero. Logged so this case is distinguishable.
		syncDebugLog("import:checkpoint", {
			applied: false,
			outcome: "no-meta-row",
			hasCheckpoint: !!syncCheckpoint,
			lid,
		});
	}

	if (settings.giveMeWorstRoster) {
		await league.swapWorstRoster(false);
	}

	toUI(
		"updateLocal",
		[
			{
				leagueCreation: undefined,
			},
		],
		conditions,
	);

	return lid;
};

const deleteOldData = async (options: {
	boxScores: boolean;
	events: boolean;
	teamStats: boolean;
	teamHistory: boolean;
	retiredPlayersUnnotable: boolean;
	retiredPlayers: boolean;
	playerStatsUnnotable: boolean;
	playerStats: boolean;
}) => {
	// This prunes via raw IndexedDB transactions, which the sync change tracker
	// cannot see - on a shared league the deletions would apply on this device
	// only and permanently fork the room. Refuse rather than diverge.
	if (getSyncRequired() || getSyncEngine() !== undefined) {
		throw new Error("Delete Old Data is not available in a synced league.");
	}

	const transaction = idb.league.transaction(
		[
			"allStars",
			"draftLotteryResults",
			"events",
			"games",
			"headToHeads",
			"liveGamePlayByPlay",
			"teams",
			"teamSeasons",
			"teamStats",
			"players",
		],
		"readwrite",
	);

	if (options.boxScores) {
		transaction.objectStore("games").clear();
		// Saved live-sim replays go with the box scores they belong to.
		transaction.objectStore("liveGamePlayByPlay").clear();
	}

	if (options.teamHistory) {
		for await (const cursor of transaction.objectStore("teamSeasons")) {
			if (cursor.value.season < g.get("season")) {
				await cursor.delete();
			}
		}

		transaction.objectStore("draftLotteryResults").clear();

		transaction.objectStore("headToHeads").clear();

		for await (const cursor of transaction.objectStore("allStars")) {
			if (cursor.value.season < g.get("season")) {
				await cursor.delete();
			}
		}

		for await (const cursor of transaction.objectStore("teams")) {
			const t = cursor.value;
			t.retiredJerseyNumbers = [];
			await cursor.update(t);
		}
	}

	if (options.teamStats) {
		for await (const cursor of transaction.objectStore("teamStats")) {
			if (cursor.value.season < g.get("season")) {
				await cursor.delete();
			}
		}
	}

	if (options.retiredPlayers) {
		for await (const cursor of transaction
			.objectStore("players")
			.index("tid")
			.iterate(PLAYER.RETIRED)) {
			await cursor.delete();
		}
	} else if (options.retiredPlayersUnnotable) {
		for await (const cursor of transaction
			.objectStore("players")
			.index("tid")
			.iterate(PLAYER.RETIRED)) {
			const p = cursor.value;
			if (p.awards.length === 0 && !p.statsTids.includes(g.get("userTid"))) {
				await cursor.delete();
			}
		}
	}

	const deletePlayerStats = (p: Player) => {
		let updated = false;
		if (p.ratings.length > 1) {
			updated = true;
			const latestSeason = last(p.ratings).season;
			p.ratings = p.ratings.filter((row) => row.season >= latestSeason) as any;
		}
		if (p.stats.length > 0) {
			updated = true;
			let latestSeason = g.get("season");
			if (g.get("phase") === PHASE.PRESEASON) {
				latestSeason -= 1;
			}
			p.stats = p.stats.filter((row) => row.season >= latestSeason);
		}
		if (p.injuries.length > 0) {
			if (
				p.injuries.length >= 1 &&
				(p.injury.gamesRemaining > 0 || p.injury.type !== "Healthy")
			) {
				if (p.injuries.length > 1) {
					p.injuries = [p.injuries.at(-1)!];
					updated = true;
				}
			} else {
				p.injuries = [];
				updated = true;
			}
		}
		if (p.salaries.length > 0) {
			if (p.tid < 0) {
				p.salaries = [];
			} else {
				const minSeasonKeep =
					g.get("phase") > PHASE.PLAYOFFS
						? g.get("season") + 1
						: g.get("season");
				let minIndexKeep = Infinity;
				for (const [i, row] of p.salaries.entries()) {
					if (row.season === minSeasonKeep) {
						// Keep latest contract that covers the current season - handles the case of old released contracts that would have also covered this season
						minIndexKeep = i;
					}
				}
				const lengthBefore = p.salaries.length;
				p.salaries = p.salaries.slice(minIndexKeep);
				if (lengthBefore > p.salaries.length) {
					updated = true;
				}
			}
		}

		if (updated) {
			return p;
		}
	};

	if (options.playerStats) {
		for await (const cursor of transaction.objectStore("players")) {
			const p = cursor.value;
			const p2 = deletePlayerStats(p);
			if (p2) {
				await cursor.update(p2);
			}
		}
	} else if (options.playerStatsUnnotable) {
		for await (const cursor of transaction.objectStore("players")) {
			const p = cursor.value;
			if (p.awards.length === 0 && !p.statsTids.includes(g.get("userTid"))) {
				const p2 = deletePlayerStats(p);
				if (p2) {
					await cursor.update(p2);
				}
			}
		}
	}

	if (options.events) {
		transaction.objectStore("events").clear();
	}

	await transaction.done;

	// Without this, cached values will still exist
	await idb.cache.fill();
};

const deleteFromGameAttributesScheduledEvent = async (
	keys: (keyof ScheduledEventGameAttributes["info"])[],
	event: ScheduledEventGameAttributes & { id: number },
) => {
	let updated = false;
	for (const key of keys) {
		if (event.info[key] !== undefined) {
			delete event.info[key];
			updated = true;
		}
	}

	if (Object.keys(event.info).length === 0) {
		await idb.cache.scheduledEvents.delete(event.id);
	} else if (updated) {
		await idb.cache.scheduledEvents.put(event);
	}
};

const deleteFromTeamInfoScheduledEvent = async (
	keys: (keyof ScheduledEventTeamInfo["info"])[],
	event: ScheduledEventTeamInfo & { id: number },
	invert: boolean,
) => {
	let updated = false;
	if (invert) {
		for (const key of helpers.keys(event.info)) {
			if (key !== "tid" && key !== "srID" && !keys.includes(key)) {
				delete event.info[key];
				updated = true;
			}
		}
	} else {
		for (const key of keys) {
			if (event.info[key] !== undefined) {
				delete event.info[key];
				updated = true;
			}
		}
	}

	const keys2 = helpers.keys(event.info);
	if (
		keys2.length <= 1 ||
		(keys2.length === 2 && keys2.includes("tid") && keys2.includes("srID"))
	) {
		await idb.cache.scheduledEvents.delete(event.id);
	} else if (updated) {
		await idb.cache.scheduledEvents.put(event);
	}
};

const deleteScheduledEvents = async (type: string) => {
	const scheduledEvents = await idb.getCopies.scheduledEvents(
		undefined,
		"noCopyCache",
	);

	const deletedExpansionTIDs: number[] = [];

	for (const event of scheduledEvents) {
		if (type === "all") {
			await idb.cache.scheduledEvents.delete(event.id);
		} else if (type === "expansionDraft") {
			if (event.type === "expansionDraft") {
				deletedExpansionTIDs.push(...event.info.teams.map((t) => t.tid));
				await idb.cache.scheduledEvents.delete(event.id);
			}

			if (
				(event.type === "contraction" || event.type === "teamInfo") &&
				deletedExpansionTIDs.includes(event.info.tid)
			) {
				await idb.cache.scheduledEvents.delete(event.id);
			}
		} else if (type === "contraction") {
			if (event.type === "contraction") {
				await idb.cache.scheduledEvents.delete(event.id);
			}
		} else if (type === "unretirePlayer") {
			if (event.type === "unretirePlayer") {
				await idb.cache.scheduledEvents.delete(event.id);
			}
		} else if (type === "teamInfo") {
			if (event.type === "teamInfo") {
				await deleteFromTeamInfoScheduledEvent(["cid", "did"], event, true);
			}
		} else if (type === "confs") {
			if (event.type === "teamInfo") {
				// cid is legacy
				await deleteFromTeamInfoScheduledEvent(["cid", "did"], event, false);
			}

			if (event.type === "gameAttributes") {
				await deleteFromGameAttributesScheduledEvent(["confs", "divs"], event);
			}
		} else if (type === "finance") {
			if (event.type === "gameAttributes") {
				await deleteFromGameAttributesScheduledEvent(
					[
						"luxuryPayroll",
						"maxContract",
						"minContract",
						"minPayroll",
						"salaryCap",
						"salaryCapType",
						"luxuryTax",
					],
					event,
				);
			}
		} else if (type === "rules") {
			if (event.type === "gameAttributes") {
				await deleteFromGameAttributesScheduledEvent(
					[
						"numGamesPlayoffSeries",
						"numPlayoffByes",
						"numGames",
						"draftType",
						"threePointers",
						"foulsUntilBonus",
						"playIn",
						"numGamesConf",
						"numGamesDiv",
						"allStarType",
						"elamASG",
						"allStarDunk",
						"allStarThree",
					],
					event,
				);
			}
		} else if (type === "styleOfPlay") {
			if (event.type === "gameAttributes") {
				await deleteFromGameAttributesScheduledEvent(
					[
						"pace",
						"threePointTendencyFactor",
						"threePointAccuracyFactor",
						"twoPointAccuracyFactor",
						"ftAccuracyFactor",
						"blockFactor",
						"stealFactor",
						"turnoverFactor",
						"orbFactor",
					],
					event,
				);
			}
		} else if (type === "awards") {
			if (event.type === "gameAttributes") {
				await deleteFromGameAttributesScheduledEvent(["awards"], event);
			}
		}
	}

	await toUI("realtimeUpdate", [["scheduledEvents"]]);
};

// Edit one scheduled event in place (when it fires, and its payload). The id
// must already exist. unretirePlayer's info is augmented with a name/skills for
// display, so only the stored { pid } is persisted back.
const updateScheduledEvent = async (event: ScheduledEvent) => {
	// The cache holds ONLY the current season's scheduled events - its loader
	// reads them through the `season` index for the season being filled - and a
	// scheduled event is by definition in the future. So looking one up there
	// found nothing for essentially every row the page lists, and editing any of
	// them failed with "Scheduled event not found".
	//
	// The page's own list comes from disk (views/scheduledEvents.ts uses
	// getCopies), which is why every event was visible but none was editable.
	// Read the cache first, for a row created this session that has not been
	// flushed yet, then fall back to disk. Every other write path in this file
	// already reads from disk for exactly this reason.
	//
	// The WRITE below needs no such change: a put with an explicit primary key
	// marks the record dirty and flushes to disk whether or not it was cached.
	const existing =
		(await idb.cache.scheduledEvents.get(event.id)) ??
		(await idb.league.get("scheduledEvents", event.id));
	if (!existing) {
		throw new Error("Scheduled event not found");
	}

	if (typeof event.season !== "number" || !Number.isFinite(event.season)) {
		throw new Error("Invalid season");
	}
	if (typeof event.phase !== "number" || !Number.isFinite(event.phase)) {
		throw new Error("Invalid phase");
	}

	// Never let the type change out from under the info shape.
	const toSave = { ...event, type: existing.type } as ScheduledEvent;
	if (toSave.type === "unretirePlayer") {
		toSave.info = { pid: (event.info as { pid: number }).pid };
	}

	await idb.cache.scheduledEvents.put(toSave);
	await toUI("realtimeUpdate", [["scheduledEvents"]]);
};

const deleteScheduledEvent = async (id: number) => {
	await idb.cache.scheduledEvents.delete(id);
	await toUI("realtimeUpdate", [["scheduledEvents"]]);
};

const discardUnsavedProgress = async () => {
	const lid = g.get("lid");
	await league.close(true);
	await beforeLeague(lid);
};

const draftLottery = async () => {
	// The lottery is a once-per-season event with side effects that compound if
	// repeated (COLA winners get their future chances penalized on EVERY run).
	// Guard against a double trigger - a double-click, or two devices racing -
	// by treating an existing result as authoritative.
	const existing = await idb.getCopy.draftLotteryResults(
		{ season: g.get("season") },
		"noCopyCache",
	);
	if (existing) {
		return existing as unknown as GenOrderResult<false>["draftLotteryResult"];
	}

	const { draftLotteryResult } = (await draft.genOrder(
		false,
	)) as unknown as GenOrderResult<false>;
	// In a synced league the result is revealed pick-by-pick on every device.
	// Mark the reveal as starting so the lottery push (built from this same
	// action's changeset) is held until the reveal finishes, rather than spoiling
	// it the instant the result is written. Released in publishLotteryRevealState.
	if (draftLotteryResult && getSyncEngine() !== undefined) {
		beginLotteryReveal();
		// Publish the reveal marker BEFORE this action's changeset can upload
		// (the api returns first, then the changeset publishes): followers'
		// lottery pages arm their reveal gate off this tiny doc, so the full
		// result can never flash on their screens in the gap before the slow
		// reveal starts. Awaited so the ordering is guaranteed, best-effort on
		// failure (the reveal heartbeats re-assert it).
		try {
			await publishLotteryRevealState({
				active: true,
				season: g.get("season"),
				revealed: -1,
				startedAt: Date.now(),
			});
		} catch (error) {
			console.error("Failed to pre-publish lottery reveal state", error);
		}
	}
	return draftLotteryResult;
};

const draftUser = async (pid: number, conditions: Conditions) => {
	if (lock.get("drafting")) {
		return;
	}

	const draftPicks = await draft.getOrder();
	const dp = draftPicks[0];

	// In a synced league, each person drafts only for the team THEIR device
	// manages - multi-team mode's "any user team" default would let one friend
	// draft for another.
	if (
		dp &&
		(getSyncRequired() || getSyncEngine() !== undefined) &&
		g.get("userTids").includes(dp.tid) &&
		dp.tid !== g.get("userTid")
	) {
		logEvent(
			{
				type: "error",
				text: "This pick belongs to a league-mate's team.",
				saveToDb: false,
			},
			conditions,
		);
		return;
	}

	if (dp && g.get("userTids").includes(dp.tid)) {
		draftPicks.shift();
		await draft.selectPlayer(dp, pid);
		await draft.afterPicks(draftPicks.length === 0, conditions);
	}
};

const dunkGetProjected = async ({
	dunkAttempt,
	index,
}: {
	dunkAttempt: DunkAttempt;
	index: number;
}) => {
	let score = 0;
	let prob = 0;

	const allStars = await idb.cache.allStars.get(g.get("season"));
	const dunk = allStars?.dunk;
	if (dunk?.players[index]) {
		const pid = dunk.players[index].pid;
		const p = await idb.cache.players.get(pid);
		if (p) {
			score = helpers.bound(
				allStar.dunkContest.getDunkScoreRaw(dunkAttempt),
				allStar.dunkContest.LOWEST_POSSIBLE_SCORE,
				allStar.dunkContest.HIGHEST_POSSIBLE_SCORE,
			);

			const difficulty = allStar.dunkContest.getDifficulty(dunkAttempt);
			prob = allStar.dunkContest.difficultyToProbability(
				difficulty,
				allStar.dunkContest.getDunkerRating(p.ratings.at(-1) as PlayerRatings),
			);
		}
	}

	return {
		score,
		prob,
	};
};

const dunkSetControlling = async (controlling: number[]) => {
	const allStars = await idb.cache.allStars.get(g.get("season"));
	const dunk = allStars?.dunk;
	if (dunk) {
		dunk.controlling = controlling;
		await idb.cache.allStars.put(allStars);
		await toUI("realtimeUpdate", [["allStarDunk"]]);
	}
};

const contestSetPlayers = async ({
	type,
	players,
}: {
	type: "dunk" | "three";
	players: AllStarPlayer[];
}) => {
	const allStars = await idb.cache.allStars.get(g.get("season"));
	const contest = allStars?.[type];
	if (contest) {
		contest.players = players;
		await idb.cache.allStars.put(allStars);
		await toUI("realtimeUpdate", [
			[`allStar${helpers.upperCaseFirstLetter(type)}`],
		]);
	}
};

const dunkSimNext = async (
	type: "event" | "dunk" | "round" | "all" | "your",
	conditions: Conditions,
) => {
	if (type === "your") {
		const allStars = await idb.cache.allStars.get(g.get("season"));
		const dunk = allStars?.dunk;
		if (dunk) {
			while (true) {
				const awaitingUserDunkIndex =
					allStar.dunkContest.getAwaitingUserDunkIndex(dunk);
				if (awaitingUserDunkIndex !== undefined) {
					// Found user dunk
					break;
				}

				const newType = await allStar.dunkContest.simNextDunkEvent(conditions);
				if (newType === "all") {
					// Contest over
					break;
				}
			}
		}
	} else {
		const types: (typeof type)[] = ["event", "dunk", "round", "all"];

		// Each call to simNextDunkEvent returns one of `type`. Stopping condition is satisfied if we hit the requested `type`, or any `type` that is after it in `types`.

		const targetIndex = types.indexOf(type);

		while (true) {
			const newType = await allStar.dunkContest.simNextDunkEvent(conditions);
			const newIndex = types.indexOf(newType);
			if (newIndex >= targetIndex) {
				break;
			}
		}
	}

	await toUI("realtimeUpdate", [["allStarDunk"]]);
};

const takeControlTeam = async (userTid: number) => {
	if (g.get("userTids").includes(userTid)) {
		await league.setGameAttributes({
			userTid,
		});
	} else {
		await league.setGameAttributes({
			userTid,
			userTids: [userTid],
		});
	}

	await toUI("realtimeUpdate", [["gameAttributes"]]);
};

const threeSimNext = async (
	type: "event" | "rack" | "player" | "round" | "all",
	conditions: Conditions,
) => {
	const types: (typeof type)[] = ["event", "rack", "player", "round", "all"];

	// Each call to simNextThreeEvent returns one of `type`. Stopping condition is satisfied if we hit the requested `type`, or any `type` that is after it in `types`.

	const targetIndex = types.indexOf(type);

	while (true) {
		const newType = await allStar.threeContest.simNextThreeEvent(conditions);
		const newIndex = types.indexOf(newType);
		if (newIndex >= targetIndex) {
			break;
		}
	}

	await toUI("realtimeUpdate", [["allStarThree"]]);
};

const dunkUser = async (
	{ dunkAttempt, index }: { dunkAttempt: DunkAttempt; index: number },
	conditions: Conditions,
) => {
	await allStar.dunkContest.simNextDunkEvent(conditions, {
		dunkAttempt,
		index,
	});
	await toUI("realtimeUpdate", [["allStarDunk"]]);
};

const evalOnWorker = async (code: string) => {
	const logOutput: (string | boolean | number)[] = [];

	const originalLog = console.log;
	const originalTable = console.table;

	const log = (x: unknown) => {
		if (x === undefined) {
			return;
		}

		if (
			typeof x === "string" ||
			typeof x === "boolean" ||
			typeof x === "number"
		) {
			logOutput.push(x);
		} else {
			try {
				const json = JSON.stringify(x);
				logOutput.push(json);
			} catch (error) {
				logOutput.push(
					`Can only log JSON-serializable variables: ${error.message}`,
				);
			}
		}
	};

	const table = (rows: any[], inputColumns?: string[]) => {
		const csv = csvFormat(rows, inputColumns);
		logOutput.push(csv);
	};

	console.log = log;
	console.table = table;

	try {
		// https://stackoverflow.com/a/63972569/786644
		// eslint-disable-next-line prefer-arrow-callback
		await Object.getPrototypeOf(async function () {}).constructor(code)();

		if (logOutput.length > 0) {
			return logOutput.join("\n");
		}
	} finally {
		console.log = originalLog;
		console.table = originalTable;
	}
};

// exportPlayerAveragesCsv(2015) - just 2015 stats
// exportPlayerAveragesCsv("all") - all stats
const exportPlayerAveragesCsv = async (season: number | "all") => {
	let players: Player[];

	if (g.get("season") === season && g.get("phase") <= PHASE.PLAYOFFS) {
		players = await idb.cache.players.indexGetAll("playersByTid", [
			PLAYER.FREE_AGENT,
			Infinity,
		]);
	} else if (season === "all") {
		players = await idb.getCopies.players(
			{
				activeAndRetired: true,
			},
			"noCopyCache",
		);
	} else {
		players = await idb.getCopies.players(
			{
				activeSeason: season,
			},
			"noCopyCache",
		);
	}

	// Array of seasons in stats, either just one or all of them
	let seasons;

	if (season === "all") {
		seasons = Array.from(
			new Set(players.flatMap((p) => p.ratings).map((pr) => pr.season)),
		);
	} else {
		seasons = [season];
	}

	const ratings = [...RATINGS, ...extraRatings];

	let stats: string[] = [];

	for (const table of Object.values(PLAYER_STATS_TABLES)) {
		if (table) {
			stats.push(
				...table.stats.filter((stat) => {
					if (stat.endsWith("Max")) {
						return false;
					}

					if (isSport("baseball")) {
						if (stat === "pos") {
							return false;
						}

						if (
							statsBaseball.byPos &&
							statsBaseball.byPos.includes(stat as any)
						) {
							return false;
						}
					}

					return true;
				}),
			);
		}
	}

	// Ugh
	const shotLocationsGetCols = (cols: string[]) => {
		const colNames: string[] = [];
		const overrides = {
			"stat:fgAtRim": "AtRimFG",
			"stat:fgaAtRim": "AtRimFGA",
			"stat:fgpAtRim": "AtRimFGP",
			"stat:fgLowPost": "LowPostFG",
			"stat:fgaLowPost": "LowPostFGA",
			"stat:fgpLowPost": "LowPostFGP",
			"stat:fgMidRange": "MidRangeFG",
			"stat:fgaMidRange": "MidRangeFGA",
			"stat:fgpMidRange": "MidRangeFGP",
		};
		for (const col of cols) {
			// @ts-expect-error
			if (overrides[col]) {
				// @ts-expect-error
				colNames.push(overrides[col]);
			} else {
				const col2 = getCol(col);
				colNames.push(col2.title);
			}
		}

		return colNames;
	};

	stats = Array.from(new Set(stats));
	const columns = [
		"pid",
		"Name",
		"Pos",
		"DraftPick",
		"Age",
		"Salary",
		"Team",
		"Season",
		...shotLocationsGetCols(stats.map((stat) => `stat:${stat}`)),
		"Ovr",
		"Pot",
		...getCols(RATINGS.map((rating) => `rating:${rating}`)).map(
			(col) => col.title,
		),
		...getCols(
			extraRatings.length
				? ["ovr", "pot"].flatMap((prefix) =>
						POSITIONS.map((pos) => `rating:${prefix}${pos}`),
					)
				: [],
		).map((col) => col.title),
	];
	const rows: any[] = [];

	for (const s of seasons) {
		console.log(s, new Date());
		const players2 = await idb.getCopies.playersPlus(players, {
			attrs: ["pid", "name", "age", "draft", "salary"],
			ratings: ["pos", "ovr", "pot", ...ratings],
			stats: ["abbrev", ...stats],
			season: s,
			mergeStats: "totOnly",
		});

		for (const p of players2) {
			rows.push([
				p.pid,
				p.name,
				p.ratings.pos,
				p.draft.round > 0 && p.draft.pick > 0
					? (p.draft.round - 1) * 30 + p.draft.pick
					: "",
				p.age,
				p.salary,
				p.stats.abbrev,
				s,
				...stats.map((stat) => p.stats[stat]),
				p.ratings.ovr,
				p.ratings.pot,
				...RATINGS.map((rating) => p.ratings[rating]),
				...(extraRatings.length
					? ["ovrs", "pots"].flatMap((type) =>
							POSITIONS.map((pos) => p.ratings[type][pos]),
						)
					: []),
			]);
		}
	}

	return csvFormatRows([columns, ...rows]);
};

// exportPlayerGamesCsv(2015) - just 2015 games
// exportPlayerGamesCsv("all") - all games
const exportPlayerGamesCsv = async (season: number | "all") => {
	const columns = [
		"gid",
		"pid",
		"Name",
		"Pos",
		"Team",
		"Opp",
		"Score",
		"WL",
		"Season",
		"Playoffs",
		"Min",
		"FGM",
		"FGA",
		"FG%",
		"3PM",
		"3PA",
		"3P%",
		"FTM",
		"FTA",
		"FT%",
		"ORB",
		"DRB",
		"TRB",
		"AST",
		"TO",
		"STL",
		"BLK",
		"BA",
		"PF",
		"PTS",
		"+/-",
	];

	await idb.cache.flush();

	let storeOrIndex:
		| IDBPObjectStore<LeagueDB, ["games"], "games", "readonly">
		| IDBPIndex<LeagueDB, ["games"], "games", "season", "readonly"> =
		idb.league.transaction("games").store;
	let keyRange = undefined;

	if (season !== "all") {
		storeOrIndex = storeOrIndex.index("season");
		keyRange = IDBKeyRange.only(season);
	}

	const rows: any[] = [];

	for await (const cursor of storeOrIndex.iterate(keyRange)) {
		const { gid, playoffs, season, teams } = cursor.value;

		for (const i of [0, 1] as const) {
			const j = i === 0 ? 1 : 0;
			const t = teams[i];
			const t2 = teams[j];

			for (const p of t.players) {
				const winner = getWinner([t, t2]);
				const result = winner === i ? "W" : winner === j ? "L" : "T";

				rows.push([
					gid,
					p.pid,
					p.name,
					p.pos,
					g.get("teamInfoCache")[t.tid]?.abbrev,
					g.get("teamInfoCache")[t2.tid]?.abbrev,
					formatScoreWithShootout(t, t2),
					result,
					season,
					playoffs,
					p.min,
					p.fg,
					p.fga,
					p.fgp,
					p.tp,
					p.tpa,
					p.tpp,
					p.ft,
					p.fta,
					p.ftp,
					p.orb,
					p.drb,
					p.drb + p.orb,
					p.ast,
					p.tov,
					p.stl,
					p.blk,
					p.ba,
					p.pf,
					p.pts,
					p.pm,
				]);
			}
		}
	}

	return csvFormatRows([columns, ...rows]);
};

const getExportFilename = async (type: "league" | "players") => {
	const leagueName = (await league.getName()).replace(/[^\da-z]/gi, "_");

	if (type === "league") {
		const phase = g.get("phase");
		const season = g.get("season");
		const userTid = g.get("userTid");

		let filename = `${GAME_ACRONYM}_${leagueName}_${g.get(
			"season",
		)}_${PHASE_TEXT[phase].replace(/[^\da-z]/gi, "_")}`;

		if (
			phase === PHASE.REGULAR_SEASON ||
			phase === PHASE.AFTER_TRADE_DEADLINE
		) {
			const teamSeason = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsByTidSeason",
				[userTid, season],
			);
			if (teamSeason) {
				filename += `_${teamSeason.won}-${teamSeason.lost}`;
			}
		}

		if (phase === PHASE.PLAYOFFS) {
			const playoffSeries = await idb.cache.playoffSeries.get(season);
			if (playoffSeries) {
				const rnd = playoffSeries.currentRound;
				if (rnd < 0) {
					filename += "_Play-In";
				} else if (playoffSeries.series.length > 0) {
					filename += `_Round_${playoffSeries.currentRound + 1}`;

					// Find the latest playoff series with the user's team in it
					const roundSeries = playoffSeries.series[rnd];
					if (roundSeries) {
						for (const series of roundSeries) {
							if (series.home.tid === userTid) {
								if (series.away) {
									filename += `_${series.home.won}-${series.away.won}`;
								} else {
									filename += "_bye";
								}
							} else if (series.away?.tid === userTid) {
								filename += `_${series.away.won}-${series.home.won}`;
							}
						}
					}
				}
			}
		}

		return `${filename}.json`;
	} else if (type === "players") {
		return `${GAME_ACRONYM}_players_${leagueName}_${g.get("season")}.json`;
	}

	throw new Error("Not implemented");
};

const exportDraftClass = async ({
	season,
	retiredPlayers,
}: {
	season: number;
	retiredPlayers?: boolean;
}) => {
	const onlyUndrafted =
		!retiredPlayers &&
		(season > g.get("season") ||
			(season === g.get("season") &&
				g.get("phase") >= 0 &&
				g.get("phase") <= PHASE.DRAFT_LOTTERY));

	let players = await idb.getCopies.players(
		retiredPlayers
			? {
					retiredYear: season,
				}
			: {
					draftYear: season,
				},
		"noCopyCache",
	);

	// For exporting future draft classes (most common use case), the user might have manually changed the tid of some players, in which case we need this check to ensure that the exported draft class matches the draft class shown in the UI
	if (onlyUndrafted) {
		players = players.filter((p) => p.tid === PLAYER.UNDRAFTED);
	}

	const data: any = {
		version: idb.league.version,
		startingSeason: season,
		players: players.map((p) => ({
			born: p.born,
			college: p.college,
			draft: {
				...p.draft,
				round: 0,
				pick: 0,
				tid: -1,
				originalTid: -1,
				year: season,
			},
			face: p.face,
			firstName: p.firstName,
			hgt: p.hgt,
			imgURL: p.imgURL,
			injury: p.injury,
			injuries: p.injuries,
			lastName: p.lastName,
			pid: p.pid,
			pos: p.pos,
			ratings: [p.ratings[retiredPlayers ? p.ratings.length - 1 : 0]],
			stats: p.stats,
			real: p.real,
			relatives: p.relatives,
			srID: p.srID,
			tid: PLAYER.UNDRAFTED,
			weight: p.weight,
		})),
	};

	// When exporting a past draft class, don't include current injuries
	if (
		season < g.get("season") ||
		(season === g.get("season") && g.get("phase") > PHASE.DRAFT)
	) {
		for (const p of data.players) {
			delete p.injury;
			delete p.injuries;
		}
	}

	const leagueName = (await league.getName()).replace(/[^\da-z]/gi, "_");
	const filename = `${GAME_ACRONYM}_${
		retiredPlayers ? "retired" : "draft"
	}_class_${leagueName}_${season}.json`;

	return {
		filename,
		json: JSON.stringify(data, null, 2),
	};
};

const generateFace2 = async (country: string | undefined) => {
	const { race } = await player.name(
		country ? helpers.getCountry(country) : undefined,
	);
	return generateFace({ race });
};

const getAutoPos = (ratings: any) => {
	const boundedRatings = {
		...ratings,
	};
	for (const key of RATINGS) {
		boundedRatings[key] = player.limitRating(boundedRatings[key]);
	}
	return player.pos(boundedRatings);
};

const getBornLoc = async (pid: number) => {
	const p = await idb.getCopy.players({ pid });
	if (p) {
		return p.born.loc;
	}
};

// Batch-fetch the face (or image URL) of a set of players PLUS the colors and
// jersey of the team they wore in the applicable season, so the UI can show a
// small, correctly-uniformed face next to a name in any table without every view
// baking face data into its rows. Each item is (pid, season): season is used to
// pick the team the player was actually on THAT year (teams change colors /
// relocate over time, and a player changes teams), falling back to their current
// team, then their last real team (for retirees). Keyed by "pid:season" so the
// same player can be cached differently per season; the UI caches per league.
const getPlayerFaces = async (
	items: { pid: number; season?: number }[],
): Promise<
	Record<
		string,
		{
			face?: FaceConfig;
			imgURL?: string;
			colors?: [string, string, string];
			jersey?: string; // uniform STYLE id (for facesjs), not the number
			jerseyNumber?: string; // the player's actual number, e.g. "3", "44"
			// Real height (inches) and weight (lbs), so a viewer can size a player
			// by his actual build - e.g. the live-game court scales each body.
			hgt?: number;
			weight?: number;
		}
	>
> => {
	const result: Record<
		string,
		{
			face?: FaceConfig;
			imgURL?: string;
			colors?: [string, string, string];
			jersey?: string;
			jerseyNumber?: string;
			hgt?: number;
			weight?: number;
		}
	> = {};
	if (items.length === 0) {
		return result;
	}

	const pids = Array.from(new Set(items.map((item) => item.pid)));
	const players = await idb.getCopies.players({ pids }, "noCopyCache");
	const byPid = new Map(players.map((p) => [p.pid, p]));
	const currentSeason = g.get("season");

	// Memoize team-season lookups: a roster is all the same team+season, so this
	// collapses ~30 lookups to 1.
	const tsCache = new Map<
		string,
		{ colors: [string, string, string]; jersey?: string } | undefined
	>();
	// Inlined team-season colors/jersey lookup (rather than importing
	// getTeamInfoBySeason). api/index.ts is loaded by the worker test harness, and
	// pulling that module into its import graph perturbs a latent init-order cycle
	// (player -> util helpers) that breaks the whole worker test suite. This uses
	// only idb + constants, already imported here, so it adds no module edge.
	const getTS = async (tid: number, season: number) => {
		const k = `${tid}:${season}`;
		if (tsCache.has(k)) {
			return tsCache.get(k);
		}

		let value:
			| { colors: [string, string, string]; jersey?: string }
			| undefined;
		if (tid >= 0) {
			const index = idb.league
				.transaction("teamSeasons")
				.store.index("tid, season");
			let ts = await index.get([tid, season]);
			if (!ts) {
				// No entry for that exact season - use the nearest one at or before it
				// (else the oldest that exists).
				for await (const cursor of index.iterate(
					IDBKeyRange.bound([tid, -Infinity], [tid, Infinity]),
				)) {
					if (cursor.value.season > season && ts) {
						break;
					}
					ts = cursor.value;
				}
			}
			const t = ts ?? (await idb.cache.teams.get(tid));
			if (t) {
				value = { colors: t.colors ?? DEFAULT_TEAM_COLORS, jersey: t.jersey };
			}
		}

		tsCache.set(k, value);
		return value;
	};

	for (const { pid, season } of items) {
		const key = `${pid}:${season ?? ""}`;
		if (result[key]) {
			continue;
		}
		const p = byPid.get(pid);
		if (!p) {
			continue;
		}

		// Which team's uniform to draw: the team the player was on that season, else
		// their current team, else (retired) their last real team. The jersey
		// NUMBER is season-accurate off the stats row where possible (a player can
		// change numbers), falling back to his current number.
		let jerseyTid: number | undefined;
		let jerseySeason: number | undefined;
		let jerseyNumber: string | undefined;
		if (season !== undefined) {
			const row = p.stats
				.filter((ps) => ps.season === season && ps.tid >= 0)
				.at(-1);
			if (row) {
				jerseyTid = row.tid;
				jerseySeason = season;
				jerseyNumber = row.jerseyNumber;
			}
		}
		if (jerseyTid === undefined) {
			if (p.tid >= 0) {
				jerseyTid = p.tid;
				jerseySeason = currentSeason;
			} else {
				const row = p.stats.filter((ps) => ps.tid >= 0).at(-1);
				if (row) {
					jerseyTid = row.tid;
					jerseySeason = row.season;
					jerseyNumber = row.jerseyNumber;
				}
			}
		}
		if (jerseyNumber === undefined) {
			jerseyNumber = p.jerseyNumber;
		}

		let colors: [string, string, string] | undefined;
		let jersey: string | undefined;
		if (jerseyTid !== undefined && jerseySeason !== undefined) {
			try {
				const ts = await getTS(jerseyTid, jerseySeason);
				if (ts) {
					colors = ts.colors;
					jersey = ts.jersey;
				}
			} catch {
				// Fall back to default colors/jersey (drawn by MyFace).
			}
		}

		result[key] = {
			face: p.face,
			imgURL: p.imgURL === "" ? undefined : p.imgURL,
			colors,
			jersey,
			jerseyNumber,
			hgt: p.hgt,
			weight: p.weight,
		};
	}
	return result;
};

const getDefaultInjuries = () => {
	return defaultInjuries;
};

const getDefaultNewLeagueSettings = async () => {
	const overrides = (await idb.meta.get(
		"attributes",
		"defaultSettingsOverrides",
	)) as Partial<Settings> | undefined;

	return overrides ?? {};
};

const getDefaultTragicDeaths = () => {
	return defaultTragicDeaths;
};

const getEightyTwoZeroDraftPlayer = (pid: number) => {
	const draft = local.eightyTwoZeroDraft;
	if (!draft) {
		return;
	}

	const hasPid = ({ p }: { p: PlayerWithoutKey }) => {
		return p.pid === pid;
	};

	return (
		draft.currentTeam?.players.find(hasPid)?.p ?? draft.picks.find(hasPid)?.p
	);
};

const getDiamondInfo = async (pid: number) => {
	let p;
	if (local.exhibitionGamePlayers) {
		p = local.exhibitionGamePlayers[pid];
	} else {
		p = await idb.cache.players.get(pid);
	}

	if (p) {
		return {
			name: `${p.firstName} ${p.lastName}`,
			spd: last(p.ratings).spd,
		};
	}
};

const getJerseyNumberConflict = async ({
	pid,
	tid,
	jerseyNumber,
}: {
	pid: number | undefined;
	tid: number;
	jerseyNumber: string;
}) => {
	const conflicts = (
		await idb.cache.players.indexGetAll("playersByTid", tid)
	).filter((p) => {
		// Can't conflict with self
		if (p.pid === pid) {
			return false;
		}

		return helpers.getJerseyNumber(p) === jerseyNumber;
	});

	if (conflicts.length === 0) {
		const t = await idb.cache.teams.get(tid);
		if (t?.retiredJerseyNumbers) {
			for (const row of t.retiredJerseyNumbers) {
				if (row.number === jerseyNumber) {
					return {
						type: "retiredJerseyNumber" as const,
					};
				}
			}
		}

		// No player or retired jersey conflicts
		return;
	}

	if (conflicts.length === 1) {
		const p = conflicts[0]!;

		return {
			type: "player" as const,
			name: `${p.firstName} ${p.lastName}`,
			pid: p.pid,
		};
	}

	return {
		type: "multiple" as const,
	};
};

const getLeagueInfo = async (
	options: Parameters<typeof realRosters.getLeagueInfo>[0],
) => {
	return realRosters.getLeagueInfo(options);
};

const getLeagueName = () => {
	return league.getName();
};

const getLeagues = async () => {
	return idb.meta.getAll("leagues");
};

const getNegotiationProps = async (pid: number) => {
	const userTid = g.get("userTid");

	const negotiation = await contractNegotiation.get(pid);
	if (typeof negotiation === "string") {
		return negotiation;
	}

	const p2 = await idb.cache.players.get(negotiation.pid);
	let p;
	if (p2) {
		p = await idb.getCopy.playersPlus(p2, {
			attrs: [
				"pid",
				"tid",
				"name",
				"age",
				"contract",
				"face",
				"imgURL",
				"watch",
			],
			ratings: ["ovr", "pot"],
			season: g.get("season"),
			showNoStats: true,
			showRookies: true,
			fuzz: true,
			// The contract options below are computed from this overall, and the
			// formula reads its ones digit - a display-rounded 5 would negotiate a
			// 58-overall player as though he were a 5. Rounded for display just
			// before it's returned.
			coarsenRatings: false,
		});
	}

	// This can happen if a negotiation is somehow started with a retired player, or a player was deleted
	if (!p || !p2) {
		await contractNegotiation.cancel(negotiation.pid);
		return "Invalid negotiation. Please try again.";
	}

	p.mood = await player.moodInfos(p2);

	const contractOptions = await generateContractOptions(
		negotiation,
		{
			amount: p.mood.user.contractAmount / 1000,
			exp: p.contract.exp,
		},
		p.ratings.ovr,
	);

	// Now that the arithmetic is done, show what this league shows.
	if (g.get("hideRatingsOnesDigit")) {
		p = coarsenPlayerForDisplay(
			p,
			["ovr", "pot"],
			g.get("hideRatingsOnesDigitExceptProspects"),
		);
	}

	if (contractOptions.length === 0 && g.get("phase") === PHASE.RESIGN_PLAYERS) {
		const t = await idb.cache.teams.get(userTid);
		if (
			t &&
			t.firstSeasonAfterExpansion !== undefined &&
			t.firstSeasonAfterExpansion - 1 === g.get("season")
		) {
			contractOptions.push({
				exp: g.get("season") + 1,
				years: 1,
				amount: p.mood.user.contractAmount / 1000,
				smallestAmount: true,
			});
		}
	}

	const payroll = await team.getPayroll(userTid);

	const t = await idb.getCopy.teamsPlus({
		tid: g.get("userTid"),
		attrs: ["colors", "jersey"],
	});
	if (!t) {
		throw new Error("Should never happen");
	}

	return {
		capSpace: (g.get("salaryCap") - payroll) / 1000,
		challengeNoRatings: g.get("challengeNoRatings"),
		contractOptions,
		salaryCapType: g.get("salaryCapType"),
		payroll: payroll / 1000,
		p,
		resigning: negotiation.resigning,
		salaryCap: g.get("salaryCap") / 1000,
		t,
	};
};

const getNumPlayoffTeams = ({
	confs,
	numRounds,
	numPlayoffByes,
	playIn,
	playoffsByConf,
}: {
	confs: NonEmptyArray<Conf>;
	numRounds: number;
	numPlayoffByes: number;
	playIn: boolean;
	playoffsByConf: boolean;
}) => {
	const byConf = playoffsByConf ? confs.length : false;

	const actualNumPlayoffByes = season.getNumPlayoffByes({
		numPlayoffByes,
		byConf,
	});

	const numTeams = getNumPlayoffTeamsRaw({
		byConf,
		numRounds,
		numPlayoffByes: actualNumPlayoffByes,
		playIn,
	});
	const numPlayoffTeams = numTeams.numPlayoffTeams + numTeams.numPlayInTeams;
	return numPlayoffTeams;
};

const getPlayerGraphStat = ({
	prev,
}: {
	prev?: { statType?: string; stat?: string };
}) => {
	const statType = prev?.statType ?? choice(statTypes);
	const stats = getStats(statType);
	const stat =
		prev?.stat !== undefined && stats.includes(prev.stat)
			? prev.stat
			: choice(stats);
	return {
		statType,
		stat,
	};
};

const getTeamGraphStat = ({
	prev,
	seasons,
}: {
	prev?: { statType?: string; stat?: string };
	seasons: [number, number];
}) => {
	const statType = prev?.statType ?? choice(teamStatTypes);
	const stats = teamGetStats(statType, seasons);

	const prevStat = prev?.stat;

	// opp logic is so switching between normal and opponent stats keeps the same stat selected (like pts and oppPts)
	let stat;
	if (prevStat !== undefined) {
		if (stats.includes(prevStat)) {
			stat = prevStat;
		} else if (prevStat.startsWith("opp")) {
			// Try removing opp
			const withoutOpp = prevStat.replace("opp", "");
			const withoutOppLower = `${withoutOpp.charAt(0).toLowerCase()}${withoutOpp.slice(1)}`;
			if (stats.includes(withoutOppLower)) {
				stat = withoutOppLower;
			}
		} else {
			// Try adding opp
			const withOpp = `opp${helpers.upperCaseFirstLetter(prevStat)}`;
			if (stats.includes(withOpp)) {
				stat = withOpp;
			}
		}
	}
	if (stat === undefined) {
		stat = choice(stats);
	}

	return {
		statType,
		stat,
	};
};

const getPlayersCommandPalette = async () => {
	const playersAll = await idb.cache.players.indexGetAll("playersByTid", [
		PLAYER.FREE_AGENT,
		Infinity,
	]);

	return idb.getCopies.playersPlus(playersAll, {
		attrs: ["pid", "firstName", "lastName", "abbrev", "age"],
		ratings: ["pos", "ovr", "pot"],
		season: g.get("season"),
		showNoStats: true,
		showRookies: true,
		fuzz: true,
	});
};

const getLocal = async (name: keyof Local) => {
	return local[name];
};

const getPlayerBioInfoDefaults = initDefaults;

// Aggregated career totals for an arbitrary set of a player's seasons, for the
// "selected rows" subtotal on the stat tables. Correct rate stats (re-derived
// from raw totals in playersPlus), for any non-contiguous selection.
const getPlayerSelectedStats = async ({
	pid,
	seasons,
}: {
	pid: number;
	seasons: number[];
}) => {
	if (!seasons || seasons.length === 0) {
		return;
	}
	const pRaw = await idb.getCopy.players(
		{
			pid,
		},
		"noCopyCache",
	);
	if (!pRaw) {
		return;
	}

	const p = await getPlayer(pRaw, undefined, undefined, seasons);

	if (p) {
		return {
			careerStatsCombined: p.careerStatsCombined,
			careerStatsPlayoffs: p.careerStatsPlayoffs,
			careerStats: p.careerStats,
		};
	}
};

// Per-team career totals for a player (like the team rows basketball-reference
// shows below a career line). Aggregates each team's seasons correctly in the
// worker - rate stats are re-derived from raw totals, not averaged - so the
// stat table can show a subtotal row per team. Returns nothing for a
// single-team career (the caller then shows no per-team rows).
const sportsbookPlaceBet = async (info: {
	tid: number;
	market: SportsbookMarket;
	stake: number;
	americanOdds: number;
	label: string;
}) => {
	return sportsbookPlaceBetCore(info);
};

// Place an entire bet slip (1+ picks) as one atomic operation - either every
// pick is placed, or (on any invalid pick) none are and no money moves. See
// core/sportsbook/bets.ts placeBetSlip for why this replaced placing each
// pick in its own separate call.
const sportsbookPlaceBetSlip = async (info: {
	tid: number;
	picks: {
		market: SportsbookMarket;
		stake: number;
		americanOdds: number;
		label: string;
	}[];
	// When true, the picks combine into one parlay staked `stake` (odds compound,
	// all legs must win). Otherwise each pick is its own straight bet.
	parlay?: boolean;
	stake?: number;
}) => {
	return sportsbookPlaceBetSlipCore(info);
};

const sportsbookCancelBet = async (info: { tid: number; betID: number }) => {
	return sportsbookCancelBetCore(info);
};

// Trivia games: fresh puzzle/round on demand. Pure reads - no league writes.
const triviaNewGrid = async () => {
	return generateTriviaGrid();
};

// 82-0: the players one rolled round can offer. Fetched a round at a time
// because every franchise-era combination at once is most of the league's
// history in one payload.
const trivia82Options = async ({
	tid,
	eraStart,
	position,
	excludePids,
}: {
	tid: number;
	eraStart: number;
	position: EightyTwoZeroPosition;
	excludePids: number[];
}) => {
	const { pool, eras } = await getPoolAndTeams();
	const era = eras.find((row) => row.start === eraStart);
	if (!era) {
		return [];
	}
	return getOptions(pool, tid, era, position, new Set(excludePids));
};

const trivia82Simulate = async (picks: { pid: number; season: number }[]) => {
	return simulateEightyTwoZeroSeason(picks);
};

// toWorker hands each api function exactly one argument, so the two-parameter
// prune helpers get object-shaped wrappers.
const pruneSyncRoomChangesApi = async ({
	code,
	olderThanDays,
}: {
	code: string;
	olderThanDays: number;
}) => {
	return pruneSyncRoomChanges(code, olderThanDays);
};

const pruneAllSyncRoomChangesApi = async ({
	olderThanDays,
}: {
	olderThanDays: number;
}) => {
	return pruneAllSyncRoomChanges(olderThanDays);
};

const triviaGridCatalog = async () => {
	return getGridCatalog();
};

const triviaCustomGrid = async (input: {
	rows: GridCriterionRef[];
	cols: GridCriterionRef[];
}) => {
	return buildCustomGrid(input);
};

const triviaPlayerCard = async ({
	pid,
	tid,
}: {
	pid: number;
	tid?: number;
}) => {
	return getTriviaPlayerCard(pid, tid);
};

const triviaNewTeamRound = async (options: TeamTriviaOptions | undefined) => {
	return generateTeamTriviaRound(options ?? {});
};

// The quizzable team-seasons, for the season and team dropdowns. Fetched once
// per visit and cached in the UI - it only moves when a season finishes.
const triviaTeamCatalog = async () => {
	return getTeamTriviaCatalog();
};

const triviaFaces = async ({ pids }: { pids: number[] }) => {
	return getTriviaFaces(pids);
};

// The player card the trivia games open in place, so looking someone up never
// costs you the board you're in the middle of.
const triviaPlayerProfile = async ({ pid }: { pid: number }) => {
	return getTriviaPlayerProfile(pid);
};

// Share this device's trivia results with the room, and read back everyone
// else's. Both no-op outside a shared league rather than throwing - the games
// work perfectly well solo, and a failed publish must never cost you the game
// you just finished.
const triviaPublishScores = async ({ entries }: { entries: any[] }) => {
	try {
		return await publishTriviaScores(entries);
	} catch (error) {
		console.error("Publishing trivia scores failed", error);
		return false;
	}
};

const triviaRemoteScores = async ({ game }: { game: string }) => {
	return getRemoteTriviaScores(game);
};

// Catch-up settlement, called when the Sportsbook page loads. A REAL captured
// `main` call (unlike the old in-view settle it replaced), so a payout
// actually gets published to the room instead of silently applying only to
// this device's local cache. No-op (and no error) on a device that isn't
// allowed to write shared state right now (a synced follower) - the sim
// authority's device settles instead.
const sportsbookSettle = async () => {
	return sportsbookSettleCore();
};

const getPlayerTeamStats = async ({ pid }: { pid: number }) => {
	const pRaw = await idb.getCopy.players({ pid }, "noCopyCache");
	if (!pRaw) {
		return;
	}

	const tids = Array.from(
		new Set(
			(pRaw.stats ?? [])
				.filter((row: any) => (row.gp ?? 0) > 0)
				.map((row: any) => row.tid)
				.filter((tid: any) => typeof tid === "number" && tid >= 0),
		),
	) as number[];

	if (tids.length <= 1) {
		return [];
	}

	const result: {
		tid: number;
		careerStats: any;
		careerStatsPlayoffs: any;
		careerStatsCombined: any;
	}[] = [];
	for (const tid of tids) {
		const p = await getPlayer(pRaw, undefined, tid);
		if (p) {
			result.push({
				tid,
				careerStats: p.careerStats,
				careerStatsPlayoffs: p.careerStatsPlayoffs,
				careerStatsCombined: p.careerStatsCombined,
			});
		}
	}
	return result;
};

const getPlayerWatch = async (pid: number) => {
	if (Number.isNaN(pid)) {
		return 0;
	}

	let p;
	if (local.exhibitionGamePlayers) {
		p = local.exhibitionGamePlayers[pid];
		if (!p) {
			return 0;
		}
	} else {
		p = getEightyTwoZeroDraftPlayer(pid);
		if (!p) {
			p = await idb.cache.players.get(pid);
		}
	}

	if (p) {
		return p.watch ?? 0;
	}
	const p2 = await idb.getCopy.players({ pid }, "noCopyCache");
	if (p2) {
		return p2.watch ?? 0;
	}

	return 0;
};

const getProjectedAttendance = async ({
	ticketPrice,
	tid,
}: {
	ticketPrice: number;
	tid: number;
}) => {
	if (Number.isNaN(ticketPrice)) {
		return 0;
	}

	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsByTidSeason",
		[
			[tid, g.get("season") - 2],
			[tid, g.get("season")],
		],
	);
	const teamSeason = teamSeasons.at(-1);
	if (!teamSeason) {
		return 0;
	}

	const baseAttendance = getBaseAttendance({
		hype: teamSeason.hype,
		pop: teamSeason.pop,
		playoffs: false,
	});
	const adjustedTicketPrice = getAdjustedTicketPrice(ticketPrice, false);
	const attendance = await getActualAttendance({
		baseAttendance,
		randomize: false,
		stadiumCapacity: teamSeason.stadiumCapacity,
		teamSeasons,
		tid: teamSeason.tid,
		adjustedTicketPrice,
	});

	return attendance;
};

const getRandomCollege = async () => {
	// Don't use real country, since most have no colleges by default
	const { college } = await player.name("None");
	return college;
};

const getRandomCountry = async () => {
	const playerBioInfo = local.playerBioInfo ?? (await loadNames());

	// Equal odds of every country, otherwise it's too commonly USA - no fun!
	return withState(choice(playerBioInfo.frequencies)[0]);
};

const getRandomInjury = () => {
	return player.injury(DEFAULT_LEVEL);
};

const getRandomJerseyNumber = async ({
	pid,
	pos,
	tid,
}: {
	pid: number | undefined;
	pos: string;
	tid: number;
}) => {
	const jerseyNumber = await player.genJerseyNumber(
		{
			pid,
			tid,
			ratings: [
				{
					pos,
				},
			],
			stats: [],
		},
		undefined,
		undefined,
		true,
	);

	return jerseyNumber;
};

const getRandomName = async (country: string) => {
	const { firstName, lastName } = await player.name(
		helpers.getCountry(country),
	);
	return { firstName, lastName };
};

const getRandomRatings = async ({
	age,
	pos,
}: {
	age: number;
	pos: string | undefined;
}) => {
	// 100 tries to find a matching position
	let p;
	for (let i = 0; i < 100; i++) {
		p = player.generate(
			PLAYER.UNDRAFTED,
			19,
			g.get("season"),
			false,
			g.get("numActiveTeams") / 2,
		);
		if (p.ratings[0].pos === pos || pos === undefined) {
			break;
		}
	}
	if (!p) {
		throw new Error("Should never happen");
	}

	await player.develop(p, age - 19);

	const ratings: Record<string, unknown> = {};
	for (const key of RATINGS) {
		ratings[key] = (p.ratings[0] as any)[key];
	}
	if (pos === undefined) {
		ratings.pos = p.ratings[0].pos;
	}
	return {
		hgt: p.hgt,
		ratings,
	};
};

const getOffers = async (
	userPids: number[],
	userDpids: number[],
	lookingFor: LookingFor,
) => {
	const teams = await idb.cache.teams.getAll();
	const tids = orderBy(
		teams.filter((t) => !t.disabled),
		["region", "name", "tid"],
	).map((t) => t.tid);
	const offers = [];

	const valueChangeCalculator = new ValueChangeCalculator();

	// The responding teams' plans, so a block response obeys the same rules an
	// AI-AI trade does: nobody offers up its young core because the raw math
	// says the values match, a rebuilder does not bid on the user's veteran,
	// and no offer takes on a rental it cannot keep. Best-effort - without
	// postures (or with the smart front office off) responses are unguarded,
	// which is the old behavior.
	const postures = new Map<number, TradePosture>();
	const season = g.get("season");
	try {
		if (g.get("smartAiFrontOffice")) {
			const context = await getLeagueTradeContext();
			for (const tid of tids) {
				if (tid !== g.get("userTid")) {
					postures.set(tid, await getTradePosture(tid, context));
				}
			}
		}
	} catch (error) {
		console.error("getOffers: posture computation failed", error);
		postures.clear();
	}

	for (const tid of tids) {
		if (tid === g.get("userTid")) {
			continue;
		}

		const posture = postures.get(tid);
		let responderExcluded: number[] = [];
		if (posture) {
			const responderPlayers = await idb.cache.players.indexGetAll(
				"playersByTid",
				tid,
			);
			responderExcluded = [
				...posture.buildingBlockPids,
				...responderPlayers
					.filter((p) => wasTradedThisSeason(p.transactions, season))
					.map((p) => p.pid),
			];
		}

		const teams: TradeTeams = [
			{
				tid: g.get("userTid"),
				pids: userPids,
				pidsExcluded: [],
				dpids: userDpids,
				dpidsExcluded: [],
			},
			{
				tid,
				pids: [],
				pidsExcluded: responderExcluded,
				dpids: [],
				dpidsExcluded: [],
			},
		];

		const teams2 = await trade.makeItWork(teams, {
			holdUserConstant: true,
			maxAssetsToAdd: 4 + userPids.length + userDpids.length,
			lookingFor,
			valueChangeCalculator,
		});

		if (
			teams2 &&
			(postures.size === 0 ||
				(await offerPassesGuards(teams2, postures, season)))
		) {
			offers.push(teams2);
		}
	}

	return offers;
};

export const augmentOffers = async (offers: TradeTeams[]) => {
	if (offers.length === 0) {
		return [];
	}

	const teams = await idb.getCopies.teamsPlus({
		attrs: ["abbrev", "region", "name", "tid"],
		seasonAttrs: ["won", "lost", "tied", "otl"],
		season: g.get("season"),
		addDummySeason: true,
	});
	const teamsByTid = groupByUnique(teams, "tid");
	const stats = bySport({
		baseball: ["gp", "keyStats", "war"],
		basketball: ["gp", "min", "pts", "trb", "ast", "per"],
		football: ["gp", "keyStats", "av"],
		hockey: ["gp", "keyStats", "ops", "dps", "ps"],
	});

	// Take the pids and dpids in each offer and get the info needed to display the offer
	return Promise.all(
		offers.map(async (offerRaw) => {
			const tid = offerRaw[1].tid;
			const t = teamsByTid[tid];
			if (!t) {
				throw new Error("No team found");
			}

			const formatPicks = async (tid: number, dpids: number[]) => {
				let picks = await idb.getCopies.draftPicks(
					{
						tid,
					},
					"noCopyCache",
				);
				picks = picks.filter((dp) => dpids.includes(dp.dpid));

				return await Promise.all(
					picks.map(async (dp) => {
						return {
							...dp,
							desc: await helpers.pickDesc(dp, "short"),
						};
					}),
				);
			};

			const formatPlayers = async (tid: number, pids: number[]) => {
				let playersAll = await idb.cache.players.indexGetAll(
					"playersByTid",
					tid,
				);
				playersAll = playersAll.filter(
					(p) => pids.includes(p.pid) && !isUntradable(p).untradable,
				);
				return addFirstNameShort(
					await idb.getCopies.playersPlus(playersAll, {
						attrs: [
							"pid",
							"firstName",
							"lastName",
							"age",
							"contract",
							"injury",
							"jerseyNumber",
							"draft",
						],
						ratings: ["ovr", "pot", "skills", "pos"],
						stats,
						season: g.get("season"),
						tid,
						showNoStats: true,
						showRookies: true,
						fuzz: true,
					}),
				);
			};

			const payroll = await team.getPayroll(tid);
			return {
				tid,
				won: t.seasonAttrs.won,
				lost: t.seasonAttrs.lost,
				tied: t.seasonAttrs.tied,
				otl: t.seasonAttrs.otl,
				pids: offerRaw[1].pids,
				dpids: offerRaw[1].dpids,
				pidsUser: offerRaw[0].pids,
				dpidsUser: offerRaw[0].dpids,
				payroll,
				picks: await formatPicks(tid, offerRaw[1].dpids),
				players: await formatPlayers(tid, offerRaw[1].pids),
				picksUser: await formatPicks(g.get("userTid"), offerRaw[0].dpids),
				playersUser: await formatPlayers(g.get("userTid"), offerRaw[0].pids),
				summary: await getSummary(offerRaw),
			};
		}),
	);
};

const toConciseLookingFor = (lookingForState: LookingForState) => {
	const output = {
		positions: new Set<string>(),
		skills: new Set<string>(),
		draftPicks: lookingForState.assets.draftPicks!,
		prospects: lookingForState.assets.prospects!,
		bestCurrentPlayers: lookingForState.assets.bestCurrentPlayers!,
	};

	for (const category of ["positions", "skills"] as const) {
		for (const [key, value] of Object.entries(lookingForState[category])) {
			if (value) {
				output[category].add(key);
			}
		}
	}

	return output;
};

const getTradingBlockOffers = async ({
	pids,
	dpids,
	lookingFor,
}: {
	pids: number[];
	dpids: number[];
	lookingFor: LookingForState;
}) => {
	let offers = await getOffers(pids, dpids, toConciseLookingFor(lookingFor));

	let saveLookingFor;
	let positionAndNotDraftPicks = false;
	let draftPicksAndNothingElse = lookingFor.assets.draftPicks;
	for (const type of helpers.keys(lookingFor)) {
		const obj = lookingFor[type];
		for (const [key, value] of Object.entries(obj)) {
			if (value) {
				saveLookingFor = true;

				if (!lookingFor.assets.draftPicks && type === "positions") {
					positionAndNotDraftPicks = true;
				}

				if (
					draftPicksAndNothingElse &&
					(type !== "assets" || key !== "draftPicks")
				) {
					draftPicksAndNothingElse = false;
				}
			}
		}
	}

	// If we're looking for a position and not draft picks, only keep offers that include that position
	if (positionAndNotDraftPicks) {
		offers = offers.filter((offer) => {
			return offer[1].pids.length > 0;
		});
	}

	// If we're looking for draft picks and nothing else, only keep offers that include picks
	if (draftPicksAndNothingElse) {
		offers = offers.filter((offer) => {
			return offer[1].dpids.length > 0;
		});
	}

	const savedTradingBlock = {
		rid: 0 as const,
		dpids,
		pids,
		tid: g.get("userTid"),
		offers: offers.map((offer) => {
			return {
				dpids: offer[1].dpids,
				pids: offer[1].pids,
				tid: offer[1].tid,
			};
		}),
		lookingFor: saveLookingFor ? lookingFor : undefined,
	};
	await idb.cache.savedTradingBlock.put(savedTradingBlock);

	return augmentOffers(offers);
};

const ping = async () => {
	return;
};

const handleUploadedDraftClass = async ({
	uploadedFile,
	draftYear,
}: {
	uploadedFile: any;
	draftYear: number;
}) => {
	// Find season from uploaded file, for age adjusting
	let uploadedSeason: number | undefined;

	if (uploadedFile.gameAttributes) {
		if (Array.isArray(uploadedFile.gameAttributes)) {
			uploadedFile.gameAttributes = gameAttributesArrayToObject(
				uploadedFile.gameAttributes,
			);
		}

		if (uploadedFile.gameAttributes.season !== undefined) {
			uploadedSeason = uploadedFile.gameAttributes.season;
		}
	}

	if (Object.hasOwn(uploadedFile, "startingSeason")) {
		uploadedSeason = uploadedFile.startingSeason;
	}

	// Get all players from uploaded files
	let players: any[] = uploadedFile.players;

	// Filter out any that are not draft prospects
	players = players.filter((p) => p.tid === PLAYER.UNDRAFTED);

	// Handle draft format change in version 33, where PLAYER.UNDRAFTED has multiple draft classes
	if (uploadedFile.version !== undefined && uploadedFile.version >= 33) {
		let filtered = players.filter(
			(p) =>
				p.draft === undefined ||
				p.draft.year === undefined ||
				p.draft.year === "" ||
				p.draft.year === uploadedSeason,
		);

		if (filtered.length === 0) {
			// Try the next season, in case draft already happened
			filtered = players.filter(
				(p) =>
					uploadedSeason !== undefined && p.draft.year === uploadedSeason + 1,
			);
		}

		players = filtered;
	}

	// Get scouting rank, which is used in a couple places below
	const scoutingLevel = await finances.getLevelLastThree("scouting", {
		tid: g.get("userTid"),
	});

	// Delete old players from draft class
	const oldPlayers = await idb.cache.players.indexGetAll(
		"playersByDraftYearRetiredYear",
		[[draftYear], [draftYear, Infinity]],
	);

	const toRemove = [];
	for (const p of oldPlayers) {
		if (p.tid === PLAYER.UNDRAFTED) {
			toRemove.push(p.pid);
		}
	}
	await player.remove(toRemove);

	// Add new players to database
	for (const p of players) {
		// Adjust age and seasons
		p.ratings[0].season = draftYear;

		const noDraftProperty = !p.draft;
		if (noDraftProperty) {
			// For college basketball imports
			p.draft = {
				round: 0,
				pick: 0,
				tid: -1,
				originalTid: -1,
				year: draftYear,
				pot: 0,
				ovr: 0,
				skills: [],
			};
		}

		if (uploadedSeason !== undefined) {
			p.born.year = draftYear - (uploadedSeason - p.born.year);
		} else if (noDraftProperty) {
			// Hopefully never happens
			p.born.year = draftYear - 19;
		}

		delete p.numPlayersTradedAwayNormalized;
		p.numDaysFreeAgent = 0;
		p.gamesUntilTradable = 0;
		p.ptModifier = 1;

		// Would be nice to allow keeping it, but it's kind of messy to duplicate the logic here and in importPlayers and to add a UI
		delete p.stats;

		// Make sure player object is fully defined
		const p2 = await player.augmentPartialPlayer(
			p,
			scoutingLevel,
			uploadedFile.version,
		);
		p2.draft.year = draftYear;
		p2.ratings.at(-1)!.season = draftYear;
		p2.tid = PLAYER.UNDRAFTED;

		if (Object.hasOwn(p2, "pid")) {
			// @ts-expect-error
			delete p2.pid;
		}

		await player.updateValues(p2);

		await idb.cache.players.add(p2);
	}

	// "Top off" the draft class if not enough players imported
	await draft.genPlayers(draftYear, scoutingLevel);

	await toUI("realtimeUpdate", [["playerMovement"]]);
};

const idbCacheFlush = async () => {
	await idb.cache.flush();
};

const importPlayers = async ({
	includeStats,
	leagueFileVersion,
	players,
}: {
	includeStats: boolean;
	leagueFileVersion: number | undefined;
	players: {
		p: any;
		contractAmount: string;
		contractExp: string;
		draftYear: string;
		season: number;
		seasonOffset: number;
		tid: number;
	}[];
}) => {
	const currentSeason = g.get("season");
	const currentPhase = g.get("phase");

	for (const {
		p,
		contractAmount,
		contractExp,
		draftYear,
		season,
		seasonOffset,
		tid,
	} of players) {
		const stats = (p.stats && includeStats ? p.stats : []) as any[];
		for (const row of stats) {
			// Not worth trying to match up tids - even with srID it's not the same league so those aren't actually the same teams
			row.tid = PLAYER.DOES_NOT_EXIST;
		}

		const p2 = {
			born: p.born,
			college: p.college,
			contract: {
				amount: helpers.localeParseFloat(contractAmount) * 1000,
				exp: Number.parseInt(contractExp),
			},
			draft: {
				...p.draft,
				round: 0,
				pick: 0,
				tid: -1,
				originalTid: -1,
			},
			face: p.face,
			firstName: p.firstName,
			hgt: p.hgt,
			imgURL: p.imgURL,
			injuries: p.injuries ?? [],
			lastName: p.lastName,
			ratings: p.ratings,
			salaries: p.salaries ?? [],
			srID: p.srID,
			stats,
			tid,
			transactions: [
				{
					season: currentSeason,
					phase: currentPhase,
					tid,
					type: "import",
				},
			],
			weight: p.weight,

			// Particularly important because stats are ignored, so jersey number is lost without this
			jerseyNumber: p.stats?.at(-1)?.jerseyNumber ?? p.jerseyNumber,
		};

		if (p.customMoodItems) {
			(p2 as any).customMoodItems = p.customMoodItems;
		}
		if (p.noteBool) {
			(p2 as any).note = p.note;
			(p2 as any).noteBool = p.noteBool;
		}
		if (p.real) {
			(p2 as any).real = p.real;
		}

		// Only add injury if the season wasn't chaned by the user. These variables copied from ImportPlayers init
		const exportedSeason: number | undefined =
			typeof p.exportedSeason === "number" ? p.exportedSeason : undefined;
		const season2 =
			(exportedSeason !== undefined
				? p.exportedSeason
				: p.ratings.at(-1).season) + seasonOffset;
		if (season === season2) {
			(p2 as any).injury = p.injury;
		}

		const adjustAndFilter = (
			key: "injuries" | "ratings" | "salaries" | "stats",
			seasonOffset: number,
			draftProspect: boolean,
		) => {
			for (const row of p2[key]) {
				row.season += seasonOffset;
			}

			let offset = 0;
			if (!draftProspect) {
				if (key === "injuries" && currentPhase < PHASE.REGULAR_SEASON) {
					// No injuries from current season, if current season has not started yet
					offset = -1;
				} else if (key === "salaries") {
					// Current season salary will be added later
					offset = -1;
				} else if (key === "stats" && currentPhase <= PHASE.PLAYOFFS) {
					// Don't include current season stats if the season has not started yet. Might be good to separate playoff stats and non-playoff stats and use differnet phase cutoffs, but whatever.
					offset = -1;
				}
			}

			p2[key] = p2[key].filter(
				(row: any) => row.season <= currentSeason + offset,
			);
		};

		if (tid === PLAYER.UNDRAFTED) {
			const draftYearInt = Number.parseInt(draftYear);
			if (
				Number.isNaN(draftYearInt) ||
				draftYearInt < currentSeason ||
				(currentPhase > PHASE.DRAFT && draftYearInt === currentSeason)
			) {
				throw new Error("Invalid draft year");
			}

			const ratingsSeason = season - seasonOffset;
			const ageAtDraft = ratingsSeason - p2.born.year;

			p2.draft.year = draftYearInt;
			p2.born.year = draftYearInt - ageAtDraft;

			const ratings = p2.ratings.find(
				(row: any) => row.season === ratingsSeason,
			);
			if (!ratings) {
				throw new Error(
					`Ratings not found for player ${p.pid} in season ${ratingsSeason}`,
				);
			}

			p2.salaries = [];
			p2.injuries = [];
			p2.ratings = [ratings];
			adjustAndFilter("stats", currentSeason - ratingsSeason, true);
			ratings.season = p2.draft.year;
		} else {
			// How many seasons to adjust player to bring him aligned with current season, as an active player at the selected age
			const seasonOffset2 = currentSeason - (season - seasonOffset);

			p2.born.year += seasonOffset2;
			p2.draft.year += seasonOffset2;

			adjustAndFilter("injuries", seasonOffset2, false);
			adjustAndFilter("ratings", seasonOffset2, false);
			adjustAndFilter("salaries", seasonOffset2, false);
			adjustAndFilter("stats", seasonOffset2, false);

			player.setContract(p2, p2.contract, tid >= 0);
		}

		const p3 = await player.augmentPartialPlayer(
			p2,
			DEFAULT_LEVEL,
			leagueFileVersion,
		);
		if (p3.jerseyNumber !== undefined && g.get("phase") <= PHASE.PLAYOFFS) {
			// Make sure there is no conflict with the team we're importing to
			player.setJerseyNumber(p3, await player.genJerseyNumber(p3));
		}
		await player.updateValues(p3);

		await idb.cache.players.put(p3);
	}

	await toUI("realtimeUpdate", [["playerMovement"]]);
};

const importPlayersGetReal = async (param: unknown, conditions: Conditions) => {
	const basketball = await loadData();
	const groupedRatings = Map.groupBy(basketball.ratings, (row) => row.slug);

	const formatPlayer = await formatPlayerFactory(
		basketball,
		{
			type: "real",
			season: REAL_PLAYERS_INFO!.MAX_SEASON,
			phase: g.get("phase"),
			randomDebuts: false,
			randomDebutsKeepCurrent: false,
			realDraftRatings: g.get("realDraftRatings") ?? "rookie",
			realStats: "all", // Maybe should default to "none" on mobile, but then I'd need a UI to change it, and most people probably want "all"  if they are using this feature
			includePlayers: true,
		},
		REAL_PLAYERS_INFO!.MAX_SEASON,
		[],
		-1,
	);

	const contract = {
		exp: g.get("season") + 2,
		amount: helpers.roundContract(
			Math.sqrt(g.get("minContract") * g.get("maxContract")),
		),
	};
	const salaries = range(g.get("season"), contract.exp + 1).map((season) => {
		return {
			season,
			amount: contract.amount,
		};
	});

	const { realPlayerPhotos } = await getRealTeamPlayerData(
		{ fileHasPlayers: true, fileHasTeams: false },
		conditions,
	);

	const players = [];
	for (const ratings of groupedRatings.values()) {
		const p = formatPlayer(ratings);
		applyRealPlayerPhotos(realPlayerPhotos, p);
		p.contract = { ...contract };
		p.salaries = helpers.deepCopy(salaries);

		const p2 = await player.augmentPartialPlayer(
			p,
			DEFAULT_LEVEL,
			LEAGUE_DATABASE_VERSION,
		);
		players.push(p2);
	}

	return players;
};

// What the Team Finances salary table is counting for one team. Cosmetic only -
// it moves the totals on that page and touches nothing the sim reads - but it's
// a game attribute rather than device state so the plan follows the manager
// between devices, which is the whole point of writing it down.
const setTeamFinancesPlan = async ({
	tid,
	droppedPids,
	keptPids,
	keptDpids,
}: {
	tid: number;
	droppedPids: number[];
	keptPids: number[];
	keptDpids: number[];
}) => {
	const previous = g.get("teamFinancesPlan");
	const empty =
		droppedPids.length === 0 && keptPids.length === 0 && keptDpids.length === 0;

	const teamFinancesPlan = { ...previous };
	if (empty) {
		// Back to the default view of the table, so stop storing anything for this
		// team rather than keeping three empty arrays around forever.
		delete teamFinancesPlan[tid];
	} else {
		teamFinancesPlan[tid] = { droppedPids, keptPids, keptDpids };
	}

	await league.setGameAttributes({ teamFinancesPlan });

	// The cache only auto-flushes every few seconds, and this is a preference
	// someone ticks and then immediately navigates away from - exactly the write
	// that would be lost. It's one small record, so pay for it now.
	await idb.cache.flush();
};

const incrementTradeProposalsSeed = async () => {
	await league.setGameAttributes({
		tradeProposalsSeed: g.get("tradeProposalsSeed") + 1,
	});

	await toUI("realtimeUpdate", [["g.tradeProposalsSeed"]]);
};

let initRan = false;
const init = async (inputEnv: Env, conditions: Conditions) => {
	Object.assign(env, inputEnv);

	// Kind of hacky, only run this for the first host tab
	if (!initRan) {
		initRan = true;
		checkNaNs();

		// Account and changes checks can be async
		(async () => {
			// Account check needs to complete before initAds, though
			await checkAccount(conditions);
			await toUI("initAds", ["accountChecked"], conditions);

			// This might make another HTTP request, and is less urgent than ads
			await checkChanges(conditions);
		})();
	} else {
		// No need to run checkAccount and make another HTTP request
		const currentTimestamp = Math.floor(Date.now() / 1000) - GRACE_PERIOD;
		await toUI("updateLocal", [
			{
				gold: local.goldUntil < Infinity && currentTimestamp <= local.goldUntil,
				email: local.email,
				username: local.username,
			},
		]);

		(async () => {
			await toUI("initAds", ["accountChecked"], conditions);
		})();
	}

	// Send options to all new tabs
	const attributesStore = (await idb.meta.transaction("attributes")).store;
	const options = ((await attributesStore.get("options")) ?? {}) as Options;
	const keyboardShortcuts = (await attributesStore.get(
		"keyboardShortcuts",
	)) as KeyboardShortcutsLocal;
	await toUI(
		"updateLocal",
		[
			{
				fullNames: options.fullNames,
				keyboardShortcuts,
				units: options.units,
				recapAIProvider: options.recapAIProvider ?? "claude",
			},
		],
		conditions,
	);
};

const initGold = async () => {
	await toUI("initGold", []);
};

const loadRetiredPlayers = async () => {
	const players = await idb.cache.players.getAll();
	const playersByPid = groupByUnique(players, "pid");

	const playerNames: {
		pid: number;
		firstName: string;
		lastName: string;
		firstSeason: number;
		lastSeason: number;
	}[] = [];

	for await (const { value: pTemp } of idb.league.transaction("players")
		.store) {
		// Make sure we have latest version of this player
		const p = playersByPid[pTemp.pid] ?? pTemp;

		playerNames.push(formatPlayerRelativesList(p));
	}

	return finalizePlayersRelativesList(playerNames);
};

const lockSet = async ([name, value]: [LockName, boolean]) => {
	await lock.set(name, value);
};

const ovr = async ({
	ratings,
	pos,
}: {
	ratings: MinimalPlayerRatings;
	pos: string;
}) => {
	return player.ovr(ratings, pos);
};

const ratingsStatsPopoverInfo = async ({
	pid,
	season,
}: {
	pid: number;
	season?: number;
}) => {
	const blankObj = {
		name: undefined,
		ratings: undefined,
		stats: undefined,
	};

	if (Number.isNaN(pid) || typeof pid !== "number") {
		return blankObj;
	}

	let p;
	let eightyTwoZeroDraftPlayer = false;
	if (local.exhibitionGamePlayers) {
		p = local.exhibitionGamePlayers[pid];
	} else if (local.liveSimRatingsStatsPopoverPlayers) {
		p = local.liveSimRatingsStatsPopoverPlayers[pid];
	} else {
		p = getEightyTwoZeroDraftPlayer(pid);
		eightyTwoZeroDraftPlayer = p !== undefined;
	}

	if (!p) {
		p = await idb.getCopy.players(
			{
				pid,
			},
			"noCopyCache",
		);
	}

	if (!p) {
		return blankObj;
	}

	const currentSeason = g.get("season");

	let actualSeason: number | undefined;
	let draftProspect = false;
	if (
		(local.exhibitionGamePlayers || eightyTwoZeroDraftPlayer) &&
		p.stats.length > 0
	) {
		actualSeason = p.stats.at(-1)!.season;
	} else {
		if (season !== undefined) {
			// For draft prospects, show their draft season, otherwise they will be skipped due to not having ratings in g.get("season")
			actualSeason = p.draft.year > season ? p.draft.year : season;
		} else {
			actualSeason =
				p.draft.year > currentSeason ? p.draft.year : currentSeason;
		}

		// If player has no stats that season and is not a draft prospect, show career stats
		if (
			p.draft.year < actualSeason &&
			!p.ratings.some((row) => row.season === actualSeason)
		) {
			actualSeason = undefined;
		}

		if (p.draft.year === actualSeason) {
			draftProspect = true;
			actualSeason = undefined;
		}
	}

	const stats = bySport({
		baseball: ["keyStats"],
		basketball: [
			"pts",
			"trb",
			"ast",
			"blk",
			"stl",
			"tov",
			"min",
			"per",
			"ewa",
			"tsp",
			"tpar",
			"ftr",
			"fgp",
			"tpp",
			"ftp",
		],
		football: ["keyStats"],
		hockey: ["keyStatsWithGoalieGP"],
	});

	// No "note" - the popover used to print the player's whole career writeup
	// under his ratings, which is a lot of prose to hang off a hover. Season
	// writeups are read from their own row in the stats table now.
	const attrs = ["name", "jerseyNumber", "tid", "age"];
	const ratings = ["pos", "ovr", "pot", "season", "tid", ...RATINGS];
	if (!local.exhibitionGamePlayers && !eightyTwoZeroDraftPlayer) {
		attrs.push("abbrev");
		ratings.push("abbrev");
	}

	const p2 = await idb.getCopy.playersPlus(p, {
		attrs,
		ratings,
		stats: ["tid", "season", "playoffs", ...stats],
		season: actualSeason,
		showNoStats: true,
		showRetired: true,
		oldStats: true,
		fuzz: true,
		// This popover is the ONLY place a draft class's ratings are read from
		// the Draft History and Draft Scouting tables, so it has to honour the
		// "prospects exempt" option or the exemption may as well not exist.
		prospectSeasonsExact: true,
	});
	if (actualSeason === undefined) {
		if (draftProspect) {
			p2.ratings = p2.ratings[0];
		} else {
			// Peak ratings. Which season peaked is decided from the TRUE ratings -
			// a coarsened ovr ties a whole decade together, so picking the max off
			// the processed rows would pick an arbitrary one - but the row that
			// gets DISPLAYED is the processed one. Reading it straight off `p`
			// showed raw ratings: no fuzz, and no coarsening either.
			const peak = maxBy(p.ratings, "ovr");
			p2.ratings =
				p2.ratings.find((row: any) => row.season === peak?.season) ??
				p2.ratings.at(-1);
		}
		p2.age = p2.ratings.season - p.born.year;

		p2.stats = p2.careerStats;
		delete p2.careerStats;
	}
	if (
		!eightyTwoZeroDraftPlayer &&
		(actualSeason === undefined || actualSeason < currentSeason)
	) {
		p2.abbrev = p2.ratings.abbrev;
		p2.tid = p2.ratings.tid;
	}
	delete p2.ratings.abbrev;
	delete p2.ratings.tid;
	delete p2.stats.playoffs;
	delete p2.stats.season;
	delete p2.stats.tid;

	let type: "career" | "current" | "draft" | number;
	if (draftProspect) {
		type = "draft";
	} else if (actualSeason === undefined) {
		type = "career";
	} else if (actualSeason >= currentSeason) {
		type = "current";
	} else {
		type = actualSeason;
	}

	// Whether the row above actually came back coarsened, decided here because
	// this is where it was (or wasn't) done. The UI colours ratings on a 0-10 or
	// a 0-100 gradient depending on this, and it used to re-derive it from `tid`
	// alone - which got it wrong the moment prospectSeasonsExact started handing
	// back a drafted player's prospect season at full resolution, painting every
	// number green. One source of truth so the two can't drift again.
	const exceptProspects = g.get("hideRatingsOnesDigitExceptProspects");
	const coarseRatings =
		g.get("hideRatingsOnesDigit") &&
		!exemptFromCoarseRatings(p.tid, exceptProspects) &&
		!prospectRatingsSeason(p.draft.year, p2.ratings?.season, exceptProspects);

	return {
		...p2,
		type,
		coarseRatings,
	};
};

// Why does this exist, just to send it back to the UI? So an action in one tab will trigger and update in all tabs! Never pass a URL here because it would apply to all tabs.
const realtimeUpdate = async (updateEvents: UpdateEvents) => {
	await toUI("realtimeUpdate", [updateEvents]);
};

const regenerateDraftClass = async (season: number, conditions: Conditions) => {
	const proceed = await toUI(
		"confirm",
		[
			"This will delete the existing draft class and replace it with a new one filled with randomly generated players. Are you sure you want to do that?",
			{
				okText: "Regenerate Draft Class",
			},
		],
		conditions,
	);

	if (proceed) {
		// Delete old players from draft class
		const oldPlayers = await idb.cache.players.indexGetAll(
			"playersByDraftYearRetiredYear",
			[[season], [season, Infinity]],
		);

		const toRemove = [];
		for (const p of oldPlayers) {
			if (p.tid === PLAYER.UNDRAFTED) {
				toRemove.push(p.pid);
			}
		}
		await player.remove(toRemove);

		// Generate new players
		await draft.genPlayers(season);
		await toUI("realtimeUpdate", [["playerMovement"]]);
	}
};

const regenerateSchedule = async (param: unknown, conditions: Conditions) => {
	const teams = await idb.getCopies.teamsPlus(
		{
			attrs: ["tid"],
			seasonAttrs: ["cid", "did", "abbrev"],
			season: g.get("season"),
			active: true,
		},
		"noCopyCache",
	);

	const tids = await season.newSchedule(teams, conditions);

	const schedule = season.addDaysToSchedule(
		tids.map(([homeTid, awayTid]) => ({
			homeTid,
			awayTid,
		})),
	);

	return formatScheduleForEditor(schedule, teams, []);
};

const releasePlayer = async ({ pids }: { pids: number[] }) => {
	if (pids.length === 0) {
		return;
	}

	const players = await idb.getCopies.players({ pids });
	if (players.length !== pids.length) {
		return "Player not found";
	}

	if (players.some((p) => p.tid !== g.get("userTid"))) {
		return "You aren't allowed to do this";
	}

	for (const p of players) {
		const justDrafted = helpers.justDrafted(p, g.get("phase"), g.get("season"));

		await player.release(p, justDrafted);
	}

	await toUI("realtimeUpdate", [["playerMovement"]]);
	await recomputeLocalUITeamOvrs();

	// Purposely after realtimeUpdate, so the UI update happens without waiting for this to complete
	await freeAgents.normalizeContractDemands({
		type: "dummyExpiringContracts",
		pids,
	});
};

const expandVote = (
	params: { override: boolean; userVote: boolean },
	conditions: Conditions,
) => {
	return team.expandVote(params, conditions);
};

const relocateVote = (params: {
	override: boolean;
	realign: boolean;
	rebrandTeam: boolean;
	userVote: boolean;
}) => {
	return team.relocateVote(params);
};

const removeLastTeam = async () => {
	const tid = g.get("numTeams") - 1;
	const players = await idb.cache.players.indexGetAll("playersByTid", tid);

	const numPlayersTradedAwayNormalized =
		await getNumPlayersTradedAwayNormalizedAll();
	for (const p of players) {
		player.addToFreeAgents(p, numPlayersTradedAwayNormalized);
		await idb.cache.players.put(p);
	}

	// Delete draft picks, and return traded ones to original owner
	await draft.genPicks();

	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsByTidSeason",
		[[tid], [tid, "Z"]],
	);

	for (const teamSeason of teamSeasons) {
		await idb.cache.teamSeasons.delete(teamSeason.rid);
	}

	const teamStats = [
		...(await idb.cache.teamStats.indexGetAll("teamStatsByPlayoffsTid", [
			[false, tid],
			[false, tid],
		])),
		...(await idb.cache.teamStats.indexGetAll("teamStatsByPlayoffsTid", [
			[true, tid],
			[true, tid],
		])),
	];

	for (const teamStat of teamStats) {
		await idb.cache.teamStats.delete(teamStat.rid);
	}

	await idb.cache.teams.delete(tid);
	const updatedGameAttributes: any = {
		numActiveTeams: g.get("numActiveTeams") - 1,
		numTeams: g.get("numTeams") - 1,
		teamInfoCache: g.get("teamInfoCache").slice(0, -1),
		userTids: g.get("userTids").filter((userTid) => userTid !== tid),
	};

	if (g.get("userTid") === tid && tid > 0) {
		updatedGameAttributes.userTid = tid - 1;

		if (!updatedGameAttributes.userTids.includes(tid - 1)) {
			updatedGameAttributes.userTids.push(tid - 1);
		}
	}

	await league.setGameAttributes(updatedGameAttributes);

	// Manually removing a new team can mess with scheduled events, because they are indexed on tid. Let's try to adjust them.
	// Delete future scheduledEvents for the deleted team, and decrement future tids for new teams
	const scheduledEvents = await idb.getCopies.scheduledEvents(
		undefined,
		"noCopyCache",
	);
	for (const scheduledEvent of scheduledEvents) {
		if (scheduledEvent.season < g.get("season")) {
			await idb.cache.scheduledEvents.delete(scheduledEvent.id);
		} else if (scheduledEvent.type === "expansionDraft") {
			let updated;
			let hasTid;
			for (const t2 of scheduledEvent.info.teams) {
				if (typeof t2.tid === "number" && tid < t2.tid) {
					t2.tid -= 1;
					updated = true;
				} else if (typeof t2.tid === "number" && tid === t2.tid) {
					hasTid = true;
				}
			}

			if (hasTid) {
				scheduledEvent.info.teams = scheduledEvent.info.teams.filter(
					(t2) => t2.tid !== tid,
				);
				updated = true;
			}

			if (updated) {
				await idb.cache.scheduledEvents.put(scheduledEvent);
			}
		} else if (
			scheduledEvent.type == "contraction" ||
			scheduledEvent.type === "teamInfo"
		) {
			if (tid === scheduledEvent.info.tid) {
				await idb.cache.scheduledEvents.delete(scheduledEvent.id);
			} else if (tid < scheduledEvent.info.tid) {
				scheduledEvent.info.tid -= 1;
				await idb.cache.scheduledEvents.put(scheduledEvent);
			}
		}
	}

	await idb.cache.flush();
};

const cloneLeague = async (lid: number) => {
	const name = await league.clone(lid);
	await toUI("realtimeUpdate", [["leagues"]]);
	return name;
};

const removeLeague = async (lid: number) => {
	await league.remove(lid);
	await toUI("realtimeUpdate", [["leagues"]]);
};

const removePlayers = async (pids: number[]) => {
	await player.remove(pids);
	await toUI("realtimeUpdate", [["playerMovement"]]);
};

const reorderDepthDrag = async ({
	pos,
	sortedPids,
}: {
	pos: string;
	sortedPids: number[];
}) => {
	const t = await idb.cache.teams.get(g.get("userTid"));
	if (!t) {
		throw new Error("Invalid tid");
	}
	const depth = t.depth;

	if (depth === undefined) {
		throw new Error("Missing depth");
	}

	if (Object.hasOwn(depth, pos)) {
		t.keepRosterSorted = false;

		// https://github.com/microsoft/TypeScript/issues/21732
		// @ts-expect-error
		depth[pos] = sortedPids;
		await idb.cache.teams.put(t);
		await toUI("realtimeUpdate", [["playerMovement"]]);
	}
};

const reorderDraftDrag = async (sortedDpids: number[]) => {
	const draftPicks = await draft.getOrder();
	for (const dp of draftPicks) {
		const sortedIndex = sortedDpids.indexOf(dp.dpid);
		const dpToTakeOrderFrom = draftPicks[sortedIndex];
		if (!dpToTakeOrderFrom) {
			throw new Error("Invalid dpid");
		}

		// Only need to update database if something changed
		if (dpToTakeOrderFrom.dpid !== dp.dpid) {
			await idb.cache.draftPicks.put({
				...dp,
				round: dpToTakeOrderFrom.round,
				pick: dpToTakeOrderFrom.pick,
			});
		}
	}

	await toUI("realtimeUpdate", [["playerMovement"]]);
};

const reorderRosterDrag = async (sortedPids: number[]) => {
	for (const [rosterOrder, pid] of sortedPids.entries()) {
		const p = await idb.cache.players.get(pid);
		if (!p) {
			throw new Error("Invalid pid");
		}

		if (p.rosterOrder !== rosterOrder) {
			p.rosterOrder = rosterOrder;
			await idb.cache.players.put(p);
		}
	}

	const t = await idb.cache.teams.get(g.get("userTid"));
	if (t) {
		t.keepRosterSorted = false;
		await idb.cache.teams.put(t);
	}

	await toUI("realtimeUpdate", [["gameAttributes", "playerMovement"]]);
};

const revertTrade = async (eid: number) => {
	return trade.revertTrade(eid);
};

const resetPlayingTime = async (tids: number[] | undefined) => {
	const tids2 = tids ?? [g.get("userTid")];

	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);

	for (const p of players) {
		if (tids2.includes(p.tid)) {
			p.ptModifier = 1;
			await idb.cache.players.put(p);
		}
	}

	await toUI("realtimeUpdate", [["playerMovement"]]);
};

const retiredJerseyNumberDelete = async ({
	tid,
	i,
}: {
	tid: number;
	i: number;
}) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error("Invalid tid");
	}

	if (t.retiredJerseyNumbers) {
		t.retiredJerseyNumbers = t.retiredJerseyNumbers.filter((row, j) => i !== j);
		await idb.cache.teams.put(t);
		await toUI("realtimeUpdate", [["retiredJerseys", "playerMovement"]]);
	}
};

const retiredJerseyNumberUpsert = async ({
	tid,
	i,
	info,
}: {
	tid: number;
	i?: number;
	info: {
		number: string;
		seasonRetired: number;
		seasonTeamInfo: number;
		pid: number | undefined;
		text: string;
	};
}) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error("Invalid tid");
	}

	if (Number.isNaN(info.seasonRetired)) {
		throw new Error("Invalid value for seasonRetired");
	}
	if (Number.isNaN(info.seasonTeamInfo)) {
		throw new Error("Invalid value for seasonTeamInfo");
	}
	if (Number.isNaN(info.pid)) {
		throw new Error("Invalid value for player ID number");
	}

	let playerText = "";
	let score: number | undefined;
	if (info.pid !== undefined) {
		const p = await idb.getCopy.players({ pid: info.pid }, "noCopyCache");
		if (p) {
			playerText = `<a href="${helpers.leagueUrl(["player", p.pid])}">${
				p.firstName
			} ${p.lastName}</a>'s `;

			score = getScore(p, tid);
		}
	}

	// Insert or update?
	let saveEvent = false;
	if (i === undefined) {
		saveEvent = true;

		if (!t.retiredJerseyNumbers) {
			t.retiredJerseyNumbers = [];
		}

		t.retiredJerseyNumbers.push({
			...info,
			score,
		});
	} else {
		if (!t.retiredJerseyNumbers) {
			throw new Error("Cannot edit when retiredJerseyNumbers is undefined");
		}

		if (i >= t.retiredJerseyNumbers.length) {
			throw new Error("Invalid index");
		}

		const prevNumber = t.retiredJerseyNumbers[i]?.number;
		if (prevNumber !== info.number) {
			saveEvent = true;
		}

		t.retiredJerseyNumbers[i] = {
			...info,
			score,
		};
	}

	if (saveEvent) {
		logEvent({
			type: "retiredJersey",
			text: `The ${t.region} ${t.name} retired ${playerText}#${info.number}.`,
			showNotification: false,
			pids: info.pid ? [info.pid] : [],
			tids: [t.tid],
			score: 20,
		});
	}

	await idb.cache.teams.put(t);

	// Handle players who have the retired jersey number
	if (actualPhase() <= PHASE.PLAYOFFS) {
		const players = await idb.cache.players.indexGetAll("playersByTid", tid);
		for (const p of players) {
			if (p.stats.length === 0) {
				continue;
			}

			const jerseyNumber = helpers.getJerseyNumber(p);
			if (jerseyNumber === info.number) {
				player.setJerseyNumber(p, await player.genJerseyNumber(p));
				await idb.cache.players.put(p);
			}
		}
	}

	await toUI("realtimeUpdate", [["retiredJerseys", "playerMovement"]]);
};

const runBefore = async (
	{
		viewId,
		params,
		ctxBBGM,
		updateEvents,
		prevData,
	}: {
		viewId: string;
		params: any;
		ctxBBGM: any;
		updateEvents: UpdateEvents;
		prevData: any;
	},
	conditions: Conditions,
): Promise<void | {
	[key: string]: any;
}> => {
	// Special case for errors, so that the condition right below (when league is loading) does not cause no update
	if (viewId === "error") {
		return {};
	}

	if (typeof g.get("lid") === "number" && !local.leagueLoaded) {
		return;
	}

	let inputs;
	if (Object.hasOwn(processInputs, viewId)) {
		// https://github.com/microsoft/TypeScript/issues/21732
		// @ts-expect-error
		inputs = processInputs[viewId](params, ctxBBGM);
	}
	if (inputs === undefined) {
		// Return empty object rather than undefined
		inputs = {};
	}

	if (typeof inputs.redirectUrl === "string") {
		// Short circuit from processInputs alone
		return {
			redirectUrl: inputs.redirectUrl,
		};
	}

	// https://github.com/microsoft/TypeScript/issues/21732
	// @ts-expect-error
	const view = views[viewId];

	if (view) {
		const data = await view(inputs, updateEvents, prevData, conditions);
		return data ?? {};
	}

	return {};
};

const setForceWin = async ({
	gid,
	tidOrTie,
}: {
	gid: number;
	tidOrTie?: number | "tie";
}) => {
	const game = await idb.cache.schedule.get(gid);
	if (!game) {
		throw new Error("Game not found");
	}

	game.forceWin = tidOrTie;
	await idb.cache.schedule.put(game);
};

const setForceWinAll = async ({
	tid,
	type,
}: {
	tid: number;
	type: "none" | "win" | "lose" | "tie";
}) => {
	const games = await idb.cache.schedule.getAll();
	for (const game of games) {
		if (game.homeTid !== tid && game.awayTid !== tid) {
			continue;
		}

		if (type === "win") {
			game.forceWin = tid;
		} else if (type === "lose") {
			game.forceWin = game.homeTid === tid ? game.awayTid : game.homeTid;
		} else if (type === "tie") {
			game.forceWin = "tie";
		} else {
			delete game.forceWin;
		}

		await idb.cache.schedule.put(game);
	}

	await toUI("realtimeUpdate", [["gameSim"]]);
};

const setGOATFormula = async ({
	formula,
	type,
}: {
	formula: string;
	type: "season" | "career";
}) => {
	// Arbitrary player for testing
	const players = await idb.cache.players.getAll();
	const p = players[0];
	if (!p) {
		throw new Error("No players found");
	}

	// Confirm it actually works
	goatFormula.evaluate(
		p,
		formula,
		type === "season"
			? {
					type,
					season: g.get("season"),
				}
			: {
					type,
				},
	);

	if (type === "career") {
		await league.setGameAttributes({
			goatFormula: formula,
		});
		await toUI("realtimeUpdate", [["g.goatFormula"]]);
	} else {
		await league.setGameAttributes({
			goatSeasonFormula: formula,
		});
		await toUI("realtimeUpdate", [["g.goatSeasonFormula"]]);
	}
};

const setLocal = async <T extends keyof Local>([key, value]: [T, Local[T]]) => {
	if (key === "autoSave" && value === false) {
		await idb.cache.flush();
	}

	// @ts-expect-error
	local[key] = value;

	if (key === "autoSave" && value === true) {
		await idb.cache.flush();
		await idb.cache.fill();

		await league.updateMeta({
			phaseText: `${g.get("season")} ${PHASE_TEXT[g.get("phase")]}`,
			difficulty: g.get("difficulty"),
		});
	}
};

const setNote = async (info: NoteInfo & { editedNote: string }) => {
	// A whole-league-day recap has no per-day record, so it's stored on the day's
	// ANCHOR game - the lowest-gid game of that (season, day) - which the Daily
	// Schedule view reads back deterministically. Stored in its own dayNote field
	// so it never collides with that game's own note.
	if (info.type === "day") {
		const seasonGames = await idb.getCopies.games(
			{ season: info.season },
			"noCopyCache",
		);
		const dayGames = seasonGames.filter((game) => (game.day ?? 0) === info.day);
		if (dayGames.length === 0) {
			throw new Error("No games on this league day to attach a recap to");
		}
		const anchor = dayGames.reduce((a, b) => (a.gid <= b.gid ? a : b));
		if (info.editedNote === "") {
			delete anchor.dayNote;
			delete anchor.dayNoteBool;
		} else {
			anchor.dayNote = info.editedNote;
			anchor.dayNoteBool = 1;
		}
		await idb.cache.games.put(anchor);
		await toUI("realtimeUpdate", [noteUpdateEvents.day]);
		return;
	}

	let cacheStore;
	let object;
	if (info.type === "draftPick") {
		cacheStore = idb.cache.draftPicks;
		object = await idb.cache.draftPicks.get(info.dpid);
	} else if (info.type === "game") {
		cacheStore = idb.cache.games;
		object = await idb.getCopy.games(
			{
				gid: info.gid,
			},
			"noCopyCache",
		);
	} else if (info.type === "player") {
		cacheStore = idb.cache.players;
		object = await idb.getCopy.players(
			{
				pid: info.pid,
			},
			"noCopyCache",
		);
	} else {
		cacheStore = idb.cache.teamSeasons;
		object = await idb.getCopy.teamSeasons(
			{
				tid: info.tid,
				season: info.season,
			},
			"noCopyCache",
		);
	}

	if (object) {
		if (info.editedNote === "") {
			delete object.note;
			delete object.noteBool;
		} else {
			object.note = info.editedNote;
			object.noteBool = 1;
		}
		await cacheStore.put(object as any);
	} else {
		throw new Error("Invalid object");
	}

	await toUI("realtimeUpdate", [noteUpdateEvents[info.type]]);
};

// File a whole season's team recaps in ONE call, for the same reason
// filePlayerSeasonRecaps exists: the UI used to loop setNote per team, and in a
// shared league every one of those worker calls waits on its own upload to the
// cloud. Thirty teams meant thirty versions in the change log and about twenty
// seconds of staring at a spinner. One call is one changeset and one version.
const fileTeamSeasonRecaps = async ({
	season,
	recaps,
}: {
	season: number;
	recaps: { tid: number; note: string }[];
}) => {
	let filed = 0;
	const missing: number[] = [];

	for (const { tid, note } of recaps) {
		const teamSeason = await idb.getCopy.teamSeasons(
			{ tid, season },
			"noCopyCache",
		);
		if (!teamSeason) {
			missing.push(tid);
			continue;
		}

		if (note === "") {
			delete teamSeason.note;
			delete teamSeason.noteBool;
		} else {
			teamSeason.note = note;
			teamSeason.noteBool = 1;
		}
		await idb.cache.teamSeasons.put(teamSeason);
		filed += 1;
	}

	await toUI("realtimeUpdate", [noteUpdateEvents.teamSeason]);

	return { filed, missing };
};

// File a batch of AI-written player season recaps. Merging happens HERE rather
// than in the UI because the merge needs each player's existing note, and doing
// it per-player from the UI would be one worker round trip per player (dozens
// per batch, hundreds per season).
//
// Each recap goes under a [season] heading in that player's single note,
// newest season on top, replacing any recap already written for that season.
// The player's own hand-written text is preserved below the year sections.
const filePlayerSeasonRecaps = async ({
	season,
	recaps,
}: {
	season: number;
	recaps: {
		pid: number;
		kind: "season" | "retirement";
		// Only retirement writeups carry one; a season recap is headed by its year.
		headline?: string;
		text: string;
	}[];
}) => {
	let filed = 0;
	const missing: number[] = [];
	const wrongKind: number[] = [];

	for (const { pid, kind, headline, text } of recaps) {
		const p = await idb.getCopy.players({ pid }, "noCopyCache");
		if (!p) {
			missing.push(pid);
			continue;
		}

		// A retirement writeup belongs only to the year a player actually retired.
		// Anything else is a misfiled paste, and it would sit in the note forever,
		// since re-running a season only ever replaces the SEASON section.
		if (kind === "retirement" && p.retiredYear !== season) {
			wrongKind.push(pid);
			continue;
		}

		let merged = upsertSeasonNote(p.note, {
			season,
			kind,
			headline: kind === "retirement" ? headline : "",
			body: text,
		});
		if (kind === "season" && p.retiredYear !== season) {
			merged = removeSeasonNote(merged, season, "retirement");
		}
		if (merged === "") {
			delete p.note;
			delete p.noteBool;
		} else {
			p.note = merged;
			p.noteBool = 1;
		}
		await idb.cache.players.put(p);
		filed += 1;
	}

	await toUI("realtimeUpdate", [noteUpdateEvents.player]);

	return { filed, missing, wrongKind };
};

// Undo for one recap pass: strip the writeups it filed, from exactly the
// players it covers, and leave everything else in their notes alone.
//
// A pass is easy to run at the wrong moment - a draft class written up before
// the draft has been run gets a writeup about being picked by nobody - and
// re-running only ever REPLACES a section, so without this the only way back
// was editing a note by hand on every player in the class.
const clearPlayerSeasonRecaps = async ({
	season,
	filter = "players",
}: {
	season: number;
	filter?: RecapFilter;
}) => {
	const pool = await getRecapPool({ season, filter });

	let cleared = 0;
	for (const p of pool) {
		// Both kinds, because both come from the same pass: a player's season
		// recap and, in the year he retires, his retirement piece.
		let note = removeSeasonNote(p.note, season, "season");
		note = removeSeasonNote(note, season, "retirement");
		if (note === (p.note ?? "")) {
			continue;
		}

		if (note === "") {
			delete p.note;
			delete p.noteBool;
		} else {
			p.note = note;
			p.noteBool = 1;
		}
		await idb.cache.players.put(p);
		cleared += 1;
	}

	await toUI("realtimeUpdate", [noteUpdateEvents.player]);

	return { cleared };
};

const reSignAll = async (players: any[]) => {
	const userTid = g.get("userTid");
	let negotiations = await idb.cache.negotiations.getAll();

	// For Multi Team Mode, might have other team's negotiations going on
	negotiations = negotiations.filter(
		(negotiation) => negotiation.tid === userTid,
	);

	if (negotiations.length > 0) {
		for (const negotiation of negotiations) {
			const p = players.find((p) => p.pid === negotiation.pid);

			if (p && p.mood.user.willing) {
				const response = await contractNegotiation.accept({
					negotiation,
					amount: p.mood.user.contractAmount,
					exp: p.contract.exp,
				});

				if (typeof response === "string") {
					return response;
				}
			}
		}

		await contractNegotiation.afterAccept(userTid);
	}
};

const updateExpansionDraftSetup = async (changes: {
	numProtectedPlayers?: string;
	numPerTeam?: string;
	teams?: ExpansionDraftSetupTeam[];
}) => {
	const expansionDraft = g.get("expansionDraft");
	if (expansionDraft.phase !== "setup") {
		throw new Error("Invalid expansion draft phase");
	}

	if (changes.teams) {
		for (const t of changes.teams) {
			for (const key of ["imgURL", "imgURLSmall"] as const) {
				if (typeof t[key] === "string") {
					t[key] = helpers.stripBbcode(t[key]);
				}
			}
		}
	}

	await league.setGameAttributes({
		expansionDraft: {
			...expansionDraft,
			...changes,
		},
	});
};

const advanceToPlayerProtection = async (
	param: unknown,
	conditions: Conditions,
) => {
	const errors = await expansionDraft.advanceToPlayerProtection(
		false,
		conditions,
	);

	if (errors) {
		return errors;
	}

	await phase.newPhase(PHASE.EXPANSION_DRAFT, conditions);
};

const autoProtect = async (tid: number) => {
	const pids = await expansionDraft.autoProtect(tid);
	await expansionDraft.updateProtectedPids(tid, pids);
	await toUI("realtimeUpdate", [["gameAttributes"]]);
};

const cancelExpansionDraft = async () => {
	const expansionDraft = g.get("expansionDraft");
	if (expansionDraft.phase !== "protection") {
		throw new Error("Invalid expansion draft phase");
	}
	for (let i = 0; i < expansionDraft.expansionTids.length; i++) {
		await removeLastTeam();
	}
	await league.setGameAttributes({
		expansionDraft: { phase: "setup" },
		phase: g.get("nextPhase"),
		nextPhase: undefined,
	});
	await updatePhase();
	await updatePlayMenu();
};

const updateProtectedPlayers = async ({
	tid,
	protectedPids,
}: {
	tid: number;
	protectedPids: number[];
}) => {
	await expansionDraft.updateProtectedPids(tid, protectedPids);
	await toUI("realtimeUpdate", [["gameAttributes"]]);
};

const startExpansionDraft = async () => {
	await expansionDraft.start();
	await toUI("realtimeUpdate", [["gameAttributes"]]);
};

const startFantasyDraft = async (tids: number[], conditions: Conditions) => {
	await phase.newPhase(PHASE.FANTASY_DRAFT, conditions, tids);
};

const switchTeam = async (tid: number, conditions: Conditions) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error("Invalid tid");
	}

	const userTid = g.get("userTid");
	if (userTid !== tid) {
		await team.switchTo(tid);
		await updateStatus("Idle");
		await updatePlayMenu();
	}

	if (g.get("otherTeamsWantToHire")) {
		await league.setGameAttributes({
			otherTeamsWantToHire: false,
		});
		await updateStatus("Idle");
		await updatePlayMenu();
	}

	const expansionDraft = g.get("expansionDraft");
	if (
		g.get("phase") === PHASE.EXPANSION_DRAFT &&
		expansionDraft.phase === "protection" &&
		expansionDraft.allowSwitchTeam
	) {
		await league.setGameAttributes({
			expansionDraft: {
				...expansionDraft,
				allowSwitchTeam: false,
			},
		});

		if (userTid !== tid) {
			logEvent(
				{
					saveToDb: false,
					text: `You are now the GM of a new expansion team, the ${t.region} ${t.name}!`,
					type: "info",
				},
				conditions,
			);
		}
	}
};

const onLiveSimOver = async (gid?: number) => {
	// Only the page playing THE CURRENT live sim gets to declare it over. Every
	// LiveGame page fires this - on unmount and on reaching its final play -
	// including finished games parked in other tabs and REPLAYS of old games,
	// none of which know a fresh sim is mid-playback. An unconditional clear
	// here is how a season-ending live sim had the draft lottery ready-up pop
	// over Game 4 of the finals at Q1: something else's "over" landed right
	// after play.ts set the flag.
	//
	// A report with no gid is allowed through: it can only come from a page that
	// never received its game data (user bailed on a pending live sim), and
	// swallowing that would leave the flag stuck on forever.
	if (
		gid !== undefined &&
		local.liveSimGid !== undefined &&
		gid !== local.liveSimGid
	) {
		return;
	}
	local.liveSimGid = undefined;

	local.liveSimRatingsStatsPopoverPlayers = undefined;

	// On a follower, this same signal means the FOLLOWED broadcast's game just
	// went final on this screen - release its spoiler gate now, not when the
	// broadcaster eventually leaves their live game page.
	markFollowedBroadcastOver(gid);

	// The show is over: paint everything remote applies held back during the
	// playback (final scores in the ticker, a phase flip, the status line).
	flushDeferredRefreshAfterLive();

	// Backstop: guarantee the single-game-sim force-silent flag is cleared once the
	// live game is done (normal clear is in play.ts). Prevents a stale flag from
	// silencing later notifications if a live sim errored before its normal clear.
	setSingleGameSimActive(false);

	// And now the room can be told. These were built when the sim ran and held
	// back so watching a game wouldn't broadcast its score mid-playback; the
	// playback is over, so they go out. Fires on the final play AND on leaving
	// the page, so bailing on a game still tells the room what happened in it.
	const heldNotifications = releaseLiveSimNotifications();
	if (heldNotifications.length > 0) {
		const engine = getSyncEngine();
		if (engine) {
			for (const notification of heldNotifications) {
				void engine.publishNotification(notification).catch((error) => {
					console.error(
						"[sync] Failed to publish held live sim notification",
						error,
					);
				});
			}
		}
	}

	await toUI("updateLocal", [{ liveGameInProgress: false }]);
};

// ---------------------------------------------------------------- LEAGUE FEED
//
// Editing accounts. The store is SPARSE by design (see socialAccounts.ts):
// every player and team already has an account derived from the league, so a
// row is written only when someone changes something. That shapes every
// operation here - saving writes an override, resetting DELETES the row rather
// than writing defaults back, and removing writes a tombstone because an
// absent row would simply be re-derived on the next read.

const socialAccountSave = async (row: SocialAccount) => {
	const existing = await idb.cache.socialAccounts.get(row.id);
	await idb.cache.socialAccounts.put({
		...row,
		createdAt: existing?.createdAt ?? row.createdAt ?? Date.now(),
		editedAt: Date.now(),
	});
	clearSocialFeedCache();
	await toUI("realtimeUpdate", [["gameSim"]]);
};

// Back to the league's own answer. Deleting is the whole operation: with no
// row, the resolver derives the account again from the player or team.
const socialAccountReset = async (id: string) => {
	await idb.cache.socialAccounts.delete(id);
	clearSocialFeedCache();
	await toUI("realtimeUpdate", [["gameSim"]]);
};

// A tombstone rather than a deletion, because an implicit account with no row
// is exactly what "derive it" looks like.
const socialAccountRemove = async ({
	id,
	kind,
}: {
	id: string;
	kind: SocialAccount["kind"];
}) => {
	await idb.cache.socialAccounts.put({ id, kind, removed: true });
	clearSocialFeedCache();
	await toUI("realtimeUpdate", [["gameSim"]]);
};

const socialAccountCreate = async (input: {
	name: string;
	handle?: string;
	bio?: string;
	archetypeId: string;
	tid?: number;
}) => {
	// Client-generated, so two devices adding an account independently never
	// collide - the same reason images and trading cards are keyed this way.
	const id = `m:${
		typeof crypto !== "undefined" && crypto.randomUUID
			? crypto.randomUUID()
			: `${Date.now()}-${Math.random().toString(36).slice(2)}`
	}`;
	await idb.cache.socialAccounts.put({
		id,
		kind: "media",
		name: input.name,
		handle: input.handle,
		bio: input.bio,
		archetypeId: input.archetypeId,
		tid: input.tid,
		createdAt: Date.now(),
		editedAt: Date.now(),
	});
	clearSocialFeedCache();
	await toUI("realtimeUpdate", [["gameSim"]]);
	return id;
};

// THE BATCH EDIT. Applies one change to many accounts at once, which is the
// only practical way to shape five hundred of them: set every player on a team
// to an archetype, quieten every fan account, and so on. Each target gets its
// own row, merged over whatever it already had, so a batch never discards an
// edit somebody made to one account by hand.
const socialAccountsBatch = async ({
	ids,
	patch,
}: {
	ids: string[];
	patch: Partial<
		Pick<SocialAccount, "archetypeId" | "bio" | "personality" | "removed">
	>;
}) => {
	for (const id of ids) {
		const existing = await idb.cache.socialAccounts.get(id);
		const kind: SocialAccount["kind"] = existing?.kind
			? existing.kind
			: id.startsWith("p:")
				? "player"
				: id.startsWith("t:")
					? "team"
					: "media";
		// Only touch personality when the batch actually carries one, so a
		// batch that just sets an archetype does not stamp an empty override
		// object onto every account it touched.
		const personality =
			patch.personality || existing?.personality
				? {
						...existing?.personality,
						...patch.personality,
						topics: {
							...existing?.personality?.topics,
							...patch.personality?.topics,
						},
					}
				: undefined;

		await idb.cache.socialAccounts.put({
			...existing,
			id,
			kind,
			...patch,
			personality,
			createdAt: existing?.createdAt ?? Date.now(),
			editedAt: Date.now(),
		});
	}
	clearSocialFeedCache();
	await toUI("realtimeUpdate", [["gameSim"]]);
};

const updateBudget = async ({
	budgetLevels,
	adjustForInflation,
	autoTicketPrice,
}: {
	budgetLevels: {
		coaching: number;
		facilities: number;
		health: number;
		scouting: number;
		ticketPrice: number;
	};
	adjustForInflation: boolean;
	autoTicketPrice: boolean;
}) => {
	const userTid = g.get("userTid");

	const t = await idb.cache.teams.get(userTid);
	if (!t) {
		throw new Error("Invalid tid");
	}

	for (const key of helpers.keys(budgetLevels)) {
		// Check for NaN before updating
		if (!Number.isNaN(budgetLevels[key])) {
			t.budget[key] = budgetLevels[key];
		}
	}

	if (autoTicketPrice && t.autoTicketPrice === false) {
		t.budget.ticketPrice = await getAutoTicketPriceByTid(userTid);
	}

	t.adjustForInflation = adjustForInflation;
	t.autoTicketPrice = autoTicketPrice;

	await idb.cache.teams.put(t);
	await toUI("realtimeUpdate", [["teamFinances"]]);
};

const updateDefaultSettingsOverrides = async (
	defaultSettingsOverrides: Partial<Settings>,
) => {
	if (Object.keys(defaultSettingsOverrides).length === 0) {
		await idb.meta.delete("attributes", "defaultSettingsOverrides");
	} else {
		await idb.meta.put(
			"attributes",
			defaultSettingsOverrides,
			"defaultSettingsOverrides",
		);
	}
};

const updateGameAttributes = async (
	gameAttributes: Partial<GameAttributesLeague>,
) => {
	await league.setGameAttributes(gameAttributes);
	await toUI("realtimeUpdate", [["gameAttributes"]]);
};

// Switch which of this device's multi-team-mode teams it controls. userTid is
// per-device and never synced, so this is deliberately SEPARATE from the general
// (sim-authority-locked) updateGameAttributes: every league-mate can pick their
// own team even while a league-mate is in charge of simming. See the
// SKIP_CHANGESET_CAPTURE entry that keeps it out of the sync guard + changeset.
const setUserTidLocal = async (userTid: number) => {
	await league.setGameAttributes({ userTid });
	await toUI("realtimeUpdate", [["firstRun"]]);
};
const updateGameAttributesGodMode = async (
	settings: Settings,
	conditions: Conditions,
) => {
	const gameAttributes: Partial<GameAttributesLeague> = omit(settings, [
		"repeatSeason",
	]);

	const currentRepeatSeasonType = g.get("repeatSeason")?.type ?? "disabled";
	const repeatSeason = settings.repeatSeason;

	if (repeatSeason !== "disabled" && repeatSeason !== currentRepeatSeasonType) {
		if (g.get("phase") < 0 || g.get("phase") > PHASE.DRAFT_LOTTERY) {
			throw new Error("Groundhog Day can only be enabled before the draft");
		}
	}

	if (
		gameAttributes.forceHistoricalRosters &&
		!g.get("forceHistoricalRosters")
	) {
		if (g.get("phase") < 0 || g.get("phase") > PHASE.DRAFT_LOTTERY) {
			throw new Error(
				"Force Historical Rosters can only be enabled before the draft",
			);
		}

		if (REAL_PLAYERS_INFO && g.get("season") >= REAL_PLAYERS_INFO.MAX_SEASON) {
			throw new Error(
				"Force Historical Rosters can only be enabled before the current season",
			);
		}
	}

	// Will be handled in setRepeatSeason, don't pass through a string
	delete gameAttributes.repeatSeason;

	// Check schedule, unless it'd be too slow
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	if (teams.length < TOO_MANY_TEAMS_TOO_SLOW) {
		await season.newSchedule(
			teams.map((t) => ({
				tid: t.tid,
				seasonAttrs: {
					cid: t.cid,
					did: t.did,
				},
			})),
			conditions,
		);
	}

	const currentRpdPot = g.get("rpdPot");
	const currentRealPlayerDeterminism = g.get("realPlayerDeterminism");

	await league.setGameAttributes(gameAttributes);

	if (repeatSeason !== currentRepeatSeasonType) {
		await league.setRepeatSeason(repeatSeason);
	}

	// Need to recompute pot for real players?
	if (
		(gameAttributes.rpdPot !== undefined &&
			currentRpdPot !== gameAttributes.rpdPot) ||
		(gameAttributes.realPlayerDeterminism !== undefined &&
			currentRealPlayerDeterminism !== gameAttributes.realPlayerDeterminism)
	) {
		const players = await idb.cache.players.getAll();
		for (const p of players) {
			if (p.real) {
				await player.develop(p, 0);
				await player.updateValues(p);
				await idb.cache.players.put(p);
			}
		}
	}

	await idb.cache.flush();

	await toUI("realtimeUpdate", [["gameAttributes"]]);

	// Confirmation that the settings actually landed.
	//
	// In a shared league every cloud-tracked call passes the multiplayer guard
	// first, and each of its nine refusal paths (not connected, still catching
	// up, sync intended but offline, cloud not ready...) returns undefined
	// WITHOUT running the action. This function used to return undefined on
	// success too, so the Settings page could not tell "saved" from "refused"
	// and reported success either way: the toast said the league settings were
	// updated, nothing had been written, and going back to the page showed the
	// old value. Returning a value is what makes a refusal visible.
	return true;
};

// A team's rotation plan, replaced whole. Reduced to what the sim can follow
// before it is kept, so the row never carries a player who has left or a
// period the league does not play. See common/rotation.ts.
const updateRotation = async ({
	tid,
	rotation,
}: {
	tid: number;
	rotation: TeamRotation;
}) => {
	if (!g.get("userTids").includes(tid)) {
		throw new Error("Not your team");
	}

	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error("Invalid tid");
	}

	const players = await idb.cache.players.indexGetAll("playersByTid", tid);
	t.rotation = sanitizeRotation(
		rotation,
		new Set(players.map((p) => p.pid)),
		g.get("numPeriods"),
	);
	await idb.cache.teams.put(t);
	await toUI("realtimeUpdate", [["team"]]);
};

const updateKeepRosterSorted = async ({
	tid,
	keepRosterSorted,
}: {
	tid: number;
	keepRosterSorted: boolean;
}) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error("Invalid tid");
	}

	t.keepRosterSorted = keepRosterSorted;
	await idb.cache.teams.put(t);
	await toUI("realtimeUpdate", [["team"]]);
};

const updateKeyboardShortcuts = async (
	keyboardShortcuts: NonNullable<KeyboardShortcutsLocal>,
) => {
	const attributesStore = (
		await idb.meta.transaction("attributes", "readwrite")
	).store;
	await attributesStore.put(keyboardShortcuts, "keyboardShortcuts");
	await toUI("updateLocal", [{ keyboardShortcuts }]);
};

const updateLeague = async ({
	lid,
	obj,
}: {
	lid: number;
	obj: Partial<League>;
}) => {
	await league.updateMeta(obj, lid, true);
	await toUI("realtimeUpdate", [["leagues"]]);
};

const updateMultiTeamMode = async (gameAttributes: {
	userTids: number[];
	userTid?: number;
}) => {
	await league.setGameAttributes(gameAttributes);

	await league.updateMeta();

	await toUI("realtimeUpdate", [["gameAttributes"]]);
};

const updateOptions = async (
	options: Options & {
		realPlayerPhotos: string;
		realTeamInfo: string;
	},
) => {
	let realPlayerPhotos;
	let realTeamInfo;
	if (options.realPlayerPhotos !== "") {
		let parsedJson;
		try {
			parsedJson = JSON.parse(options.realPlayerPhotos);
		} catch (error) {
			console.log(error);
			throw new Error("Invalid JSON in real player photos");
		}

		const result = realPlayerPhotosSchema.safeParse(parsedJson);
		if (result.success) {
			realPlayerPhotos = result.data;
		} else {
			throw new Error(
				`In real player photos:<br><span style="white-space: pre-wrap">${z.prettifyError(result.error)}</span>`,
			);
		}
	}
	if (options.realTeamInfo !== "") {
		let parsedJson;
		try {
			parsedJson = JSON.parse(options.realTeamInfo);
		} catch (error) {
			console.log(error);
			throw new Error("Invalid JSON in real team info");
		}

		const result = realTeamInfoSchema.safeParse(parsedJson);
		if (result.success) {
			realTeamInfo = result.data;
		} else {
			throw new Error(
				`In real team info:<br><span style="white-space: pre-wrap">${z.prettifyError(result.error)}</span>`,
			);
		}
	}

	// Recap caps must stay positive integers, or a recap run would bake in an
	// empty/garbage history. Anything invalid falls back to the default.
	const coerceRecapCap = (value: unknown, fallback: number) => {
		const num = Math.floor(Number(value));
		return Number.isFinite(num) && num >= 1 ? num : fallback;
	};

	const attributesStore = (
		await idb.meta.transaction("attributes", "readwrite")
	).store;
	await attributesStore.put(
		{
			units: options.units,
			fullNames: options.fullNames,
			phaseChangeRedirects: options.phaseChangeRedirects,
			recapAIProvider: options.recapAIProvider,
			recapMaxGames: coerceRecapCap(
				options.recapMaxGames,
				DEFAULT_RECAP_MAX_GAMES,
			),
			recapMaxDays: coerceRecapCap(
				options.recapMaxDays,
				DEFAULT_RECAP_MAX_DAYS,
			),
			recapMaxPlayers: coerceRecapCap(
				options.recapMaxPlayers,
				DEFAULT_RECAP_MAX_PLAYERS,
			),
			// Unlike the recap caps, 0 is a legitimate value here: it means "no
			// cutoff window", leaving the sim-day fence as the only guard.
			ownGameSimCutoffSeconds: (() => {
				const num = Math.floor(Number(options.ownGameSimCutoffSeconds));
				return Number.isFinite(num) && num >= 0
					? num
					: DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS;
			})(),
			// 0 is legitimate here too: it turns draft achievement cards off.
			achievementCardsDraftPicks: (() => {
				const num = Math.floor(Number(options.achievementCardsDraftPicks));
				return Number.isFinite(num) && num >= 0
					? num
					: DEFAULT_ACHIEVEMENT_DRAFT_PICKS;
			})(),
		},
		"options",
	);
	await attributesStore.put(realPlayerPhotos, "realPlayerPhotos");
	await attributesStore.put(realTeamInfo, "realTeamInfo");
	await toUI("updateLocal", [
		{
			units: options.units,
			fullNames: options.fullNames,
			recapAIProvider: options.recapAIProvider ?? "claude",
		},
	]);
	await toUI("realtimeUpdate", [["options"]]);
};

const updatePlayThroughInjuries = async ({
	tid,
	value,
	playoffs,
}: {
	tid: number;
	value: number;
	playoffs?: boolean;
}) => {
	const index = playoffs ? 1 : 0;

	const t = await idb.cache.teams.get(tid);
	if (t) {
		t.playThroughInjuries[index] = value;
		await idb.cache.teams.put(t);

		// So roster re-renders, which is needed to maintain state on mobile when the panel is closed
		await toUI("realtimeUpdate", [["playerMovement"]]);

		const phase = actualPhase();
		if (
			(!playoffs &&
				(phase === PHASE.REGULAR_SEASON ||
					phase === PHASE.AFTER_TRADE_DEADLINE)) ||
			(playoffs && phase === PHASE.PLAYOFFS)
		) {
			await recomputeLocalUITeamOvrs();
		}
	}
};

const updatePlayerWatch = async ({
	pid,
	watch,
}: {
	pid: number;
	watch: number;
}) => {
	let p;
	let eightyTwoZeroDraftPlayer = false;
	if (local.exhibitionGamePlayers) {
		p = local.exhibitionGamePlayers[pid];
		if (!p) {
			return;
		}
	} else {
		p = getEightyTwoZeroDraftPlayer(pid);
		eightyTwoZeroDraftPlayer = p !== undefined;
		if (!p) {
			p = await idb.getCopy.players({ pid }, "noCopyCache");
		}
	}

	if (p) {
		if (
			watch < 1 ||
			(!local.exhibitionGamePlayers &&
				!eightyTwoZeroDraftPlayer &&
				watch > g.get("numWatchColors"))
		) {
			delete p.watch;
		} else {
			p.watch = watch;
		}
		if (!local.exhibitionGamePlayers && !eightyTwoZeroDraftPlayer) {
			await idb.cache.players.put(p);
			await Promise.all([
				toUI("crossTabEmit", [["updateWatch", getUpdateWatch([p])]]),
				toUI("realtimeUpdate", [["playerMovement", "watchList"]]),
			]);
		}
	}
};

// Mark/unmark a player "untouchable" (protected from trade offers) for his
// CURRENT team. Stamping the tid makes it team-dependent: a trade to a new team
// leaves untouchableTid pointing at the old tid, so he's no longer untouchable
// there (see PlayerWithoutKey.untouchableTid).
const updatePlayerUntouchable = async ({
	pid,
	untouchable,
}: {
	pid: number;
	untouchable: boolean;
}) => {
	const p = await idb.getCopy.players({ pid }, "noCopyCache");
	if (!p) {
		return;
	}
	if (untouchable) {
		p.untouchableTid = p.tid;
	} else {
		delete p.untouchableTid;
	}
	await idb.cache.players.put(p);
	await toUI("realtimeUpdate", [["playerMovement"]]);
};

// Set a player's cartoon face from a faces.js config. Clears any image URL,
// because imgURL wins over face everywhere it's drawn - keeping it would save
// the face and change nothing on screen.
const updatePlayerFace = async ({
	pid,
	face,
}: {
	pid: number;
	face: FaceConfig;
}) => {
	const p = await idb.getCopy.players({ pid }, "noCopyCache");
	if (!p) {
		throw new Error("Player not found.");
	}

	p.face = face;
	// "" is how a player with no picture is stored (see player.generate).
	p.imgURL = "";
	await idb.cache.players.put(p);
	await toUI("realtimeUpdate", [["playerMovement"]]);
};

// PUT ONE SEASON'S FACE BACK, from the appearance gallery.
//
// Faces age on their own, most years change nothing, and the year one does is
// occasionally a year you would rather it had not. Everything needed to undo
// it is already in the history - see revertAppearance, which owns the rule -
// so this is the load, apply, save around it.
//
// Written through the cache like every other player edit, so the room sees it.
const revertPlayerFace = async ({
	pid,
	season,
}: {
	pid: number;
	season: number;
}) => {
	const p = await idb.getCopy.players({ pid }, "noCopyCache");
	if (!p) {
		throw new Error("Player not found.");
	}

	// The newest season the player has: the one his live face belongs to, so
	// reverting it moves p.face and reverting an older one does not.
	const latestSeason = Math.max(
		g.get("season"),
		...p.ratings.map((row) => row.season),
	);

	const reverted = revertAppearance({
		appearances: p.appearances,
		season,
		current: { face: p.face, imgURL: p.imgURL },
		latestSeason,
	});
	if (!reverted) {
		// Nothing changed that season - the button should not have been there.
		return;
	}

	p.appearances = reverted.appearances;
	if (reverted.current.face) {
		p.face = reverted.current.face;
	}
	// "" is how a player with no picture is stored (see player.generate), and
	// a look with no imgURL is exactly that rather than "leave the old one".
	p.imgURL = reverted.current.imgURL ?? "";
	await idb.cache.players.put(p);
	await toUI("realtimeUpdate", [["playerMovement"]]);
};

const getPlayersNextWatch = (players: Player[]) => {
	const watchCounts = new Map<number, number>();
	for (const p of players) {
		const watch = p.watch ?? 0;
		const count = watchCounts.get(watch) ?? 0;
		watchCounts.set(watch, count + 1);
	}
	const mostCommonCurrentWatch = maxBy(
		Array.from(watchCounts.entries()),
		1,
	)![0];

	const nextWatch =
		(mostCommonCurrentWatch + 1) % (g.get("numWatchColors") + 1);

	if (nextWatch === 0) {
		return undefined;
	}

	return nextWatch;
};

const updatePlayersWatch = async ({
	pids,
	watch,
}: {
	pids: number[];
	watch?: number;
}) => {
	// Need to get all players to see what the new watch value should be!
	const players = await idb.getCopies.players(
		{ pids: Array.from(new Set(pids)) },
		"noCopyCache",
	);

	if (players.length === 0) {
		return;
	}

	let nextWatch = watch ?? getPlayersNextWatch(players);
	if (nextWatch === 0) {
		// If we're clearing the watch list, watch value is 0, but we want to make it undefined in player object
		nextWatch = undefined;
	}

	for (const p of players) {
		// Only update players who changed
		if (p.watch !== nextWatch) {
			if (nextWatch === undefined) {
				delete p.watch;
			} else {
				p.watch = nextWatch;
			}
			await idb.cache.players.put(p);
		}
	}

	await Promise.all([
		toUI("crossTabEmit", [["updateWatch", getUpdateWatch(players)]]),
		toUI("realtimeUpdate", [["playerMovement", "watchList"]]),
	]);
};

const updatePlayingTime = async ({
	pid,
	ptModifier,
}: {
	pid: number;
	ptModifier: number;
}) => {
	const p = await idb.cache.players.get(pid);
	if (!p) {
		throw new Error("Invalid pid");
	}
	p.ptModifier = ptModifier;
	await idb.cache.players.put(p);
};

const updatePlayoffTeams = async (
	teams: {
		tid: number;
		cid: number;
		seed: number | undefined;
	}[],
) => {
	const playoffSeries = await idb.cache.playoffSeries.get(g.get("season"));
	if (playoffSeries) {
		const { playIns, series } = playoffSeries;
		const byConf = await season.getPlayoffsByConf(g.get("season"));

		const findTeam = (seed: number, cid: number) => {
			// If byConf, we need to find the seed in the same conference, cause multiple teams will have this seed. Otherwise, can just check seed.
			const t = teams.find(
				(t) => seed === t.seed && (!byConf || cid === t.cid),
			);

			if (!t) {
				throw new Error("Team not found");
			}

			return t;
		};

		const tidsPlayoffs = new Set();

		const checkMatchups = (matchups: (typeof series)[0]) => {
			for (const matchup of matchups) {
				const home = findTeam(matchup.home.seed, matchup.home.cid);
				matchup.home.tid = home.tid;
				matchup.home.cid = home.cid;
				tidsPlayoffs.add(home.tid);
				if (matchup.away) {
					const away = findTeam(matchup.away.seed, matchup.away.cid);
					matchup.away.tid = away.tid;
					matchup.away.cid = away.cid;
					tidsPlayoffs.add(away.tid);
				}
			}
		};

		checkMatchups(series[0]!);

		if (playIns) {
			checkMatchups(playIns.flatMap((playIn) => playIn.slice(0, 2)));
		}

		await idb.cache.playoffSeries.put(playoffSeries);

		// Update schedule, since games might have changed
		await season.newSchedulePlayoffsDay();

		// Update teamSeasons, since playoffRoundsWon might need to be updated
		const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
			"teamSeasonsBySeasonTid",
			[[g.get("season")], [g.get("season"), "Z"]],
		);
		for (const teamSeason of teamSeasons) {
			const playoffRoundsWon = tidsPlayoffs.has(teamSeason.tid) ? 0 : -1;
			if (playoffRoundsWon !== teamSeason.playoffRoundsWon) {
				teamSeason.playoffRoundsWon = playoffRoundsWon;
				await idb.cache.teamSeasons.put(teamSeason);
			}
		}

		await toUI("realtimeUpdate", [["playoffs"]]);
	}
};

const updateTeamInfo = async ({
	teams: newTeams,
	from,
}: {
	teams: {
		tid: number;
		cid?: number;
		did: number;
		region: string;
		name: string;
		abbrev: string;
		imgURL?: string;
		imgURLSmall?: string;
		pop: number | string;
		stadiumCapacity: number | string;
		colors: [string, string, string];
		jersey: string;
		disabled?: boolean;
	}[];
	from: "manageTeams" | "manageConfs";
}) => {
	const teams = await idb.cache.teams.getAll();

	const newTeamsByTid = groupByUnique(newTeams, "tid");

	const newTeamsIncludingDisabled = [];

	for (const t of teams) {
		const newTeam = newTeamsByTid[t.tid];
		if (!newTeam) {
			// manageConfs doesn't include disabled teams, on purpose
			if (from === "manageConfs" && t.disabled) {
				newTeamsIncludingDisabled.push(t);
				continue;
			} else {
				throw new Error(`New team not found for tid ${t.tid}`);
			}
		}
		newTeamsIncludingDisabled.push(newTeam);

		if (newTeam.did !== undefined) {
			const divs = g.get("divs");
			const newDiv = divs.find((div) => div.did === newTeam.did) ?? divs[0];
			t.did = newDiv.did;
			t.cid = newDiv.cid;
		}

		t.region = newTeam.region;
		t.name = newTeam.name;
		t.abbrev = newTeam.abbrev;

		for (const key of ["imgURL", "imgURLSmall"] as const) {
			if (Object.hasOwn(newTeam, key)) {
				t[key] = newTeam[key];
				if (typeof t[key] === "string") {
					t[key] = helpers.stripBbcode(t[key]);
				}
			}
		}

		t.colors = newTeam.colors;
		t.jersey = newTeam.jersey;

		t.pop =
			typeof newTeam.pop === "number"
				? newTeam.pop
				: helpers.localeParseFloat(newTeam.pop);
		t.stadiumCapacity =
			typeof newTeam.stadiumCapacity === "number"
				? Math.round(newTeam.stadiumCapacity)
				: Number.parseInt(newTeam.stadiumCapacity);

		const disableTeam = newTeam.disabled && !t.disabled;
		const enableTeam = !newTeam.disabled && t.disabled;

		t.disabled = !!newTeam.disabled;

		if (Number.isNaN(t.pop)) {
			throw new Error("Invalid pop");
		}

		if (Number.isNaN(t.stadiumCapacity)) {
			throw new Error("Invalid stadiumCapacity");
		}

		await idb.cache.teams.put(t);

		if (enableTeam) {
			await draft.genPicks();
			await draft.deleteLotteryResultIfNoDraftYet();

			if (t.tid === g.get("userTid")) {
				await league.setGameAttributes({
					gameOver: false,
				});
				await updateStatus();
				await updatePlayMenu();
			}
		} else if (disableTeam) {
			await team.disable(t.tid);
		}

		// Also apply team info changes to this season
		if (actualPhase() < PHASE.PLAYOFFS) {
			let teamSeason: TeamSeasonWithoutKey | undefined =
				await idb.cache.teamSeasons.indexGet("teamSeasonsByTidSeason", [
					t.tid,
					g.get("season"),
				]);

			if (enableTeam) {
				const prevSeason = await idb.cache.teamSeasons.indexGet(
					"teamSeasonsByTidSeason",
					[t.tid, g.get("season") - 1],
				);

				teamSeason = team.genSeasonRow(t, prevSeason);
			}

			if (teamSeason && !t.disabled) {
				teamSeason.cid = t.cid;
				teamSeason.did = t.did;
				teamSeason.region = t.region;
				teamSeason.name = t.name;
				teamSeason.abbrev = t.abbrev;
				teamSeason.imgURL = t.imgURL;
				teamSeason.imgURLSmall = t.imgURLSmall;
				teamSeason.colors = t.colors;
				teamSeason.jersey = t.jersey;
				teamSeason.pop = t.pop;
				teamSeason.stadiumCapacity = t.stadiumCapacity;

				if (teamSeason.imgURLSmall === "") {
					delete teamSeason.imgURLSmall;
				}

				await idb.cache.teamSeasons.put(teamSeason);
			}
		}

		if (t.imgURLSmall === "") {
			delete t.imgURLSmall;
		}
	}

	await league.setGameAttributes({
		teamInfoCache: orderBy(newTeamsIncludingDisabled, "tid").map((t) => ({
			abbrev: t.abbrev,
			disabled: t.disabled,
			imgURL: t.imgURL,
			imgURLSmall: t.imgURLSmall === "" ? undefined : t.imgURLSmall,
			name: t.name,
			region: t.region,
		})),

		// numActiveTeams is only needed when enabling a disabled team, and numTeams should never be needed. But might as well do these every time just to be sure, because it's easy.
		numActiveTeams: newTeamsIncludingDisabled.filter((t) => !t.disabled).length,
		numTeams: newTeamsIncludingDisabled.length,
	});

	await league.updateMeta();
};

// Save (or clear) a team's custom basketball court style. Writes the whole-team
// record through the cache, so the change is captured and synced to the room -
// every device draws the same custom court.
const updateTeamCourt = async ({
	tid,
	court,
}: {
	tid: number;
	court: CourtStyle | undefined;
}) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error(`Team not found for tid ${tid}`);
	}
	if (court === undefined || Object.keys(court).length === 0) {
		delete t.court;
	} else {
		t.court = court;
	}
	await idb.cache.teams.put(t);
	return { ok: true };
};

const updateConfsDivs = async ({
	confs,
	divs,
	teams,
}: {
	confs: Conf[];
	divs: Div[];
	teams: (Omit<Parameters<typeof updateTeamInfo>[0]["teams"][number], "cid"> & {
		cid: number;
	})[];
}) => {
	// First some sanity checks to make sure they're consistent
	for (const div of divs) {
		const conf = confs.find((c) => c.cid === div.cid);
		if (!conf) {
			throw new Error("div has invalid cid");
		}
	}
	for (const t of teams) {
		const div = divs.find((d) => d.did === t.did);
		if (!div) {
			throw new Error("team has invalid did");
		}
		if (div.cid !== t.cid) {
			throw new Error("team has invalid cid");
		}
	}

	const currentTeams = await idb.cache.teams.getAll();
	for (const t of currentTeams) {
		if (t.disabled) {
			continue;
		}

		const info = teams.find((row) => row.tid === t.tid);
		if (!info) {
			throw new Error("Inconsistent teams");
		}
	}

	await league.setGameAttributes({ confs: confs as any, divs: divs as any });

	await updateTeamInfo({ teams, from: "manageConfs" });
};

const undoAction = async (
	info: { type: "sign"; pid: number } | { type: "release"; pid: number },
) => {
	if (info.type === "sign") {
		const pid = info.pid;

		const undoInfo = local.undoableActions[pid];
		if (!undoInfo || undoInfo.type !== "sign") {
			return false;
		}

		const p = await idb.cache.players.get(pid);
		if (!p) {
			return false;
		}

		const phase = actualPhase();

		if (phase !== undoInfo.phase || p.tid !== undoInfo.tid) {
			return false;
		}

		p.numDaysFreeAgent = undoInfo.numDaysFreeAgent;
		p.numPlayersTradedAwayNormalized = undoInfo.numPlayersTradedAwayNormalized;
		p.jerseyNumber = undoInfo.jerseyNumber;
		p.contract = undoInfo.contract;
		p.salaries = undoInfo.salaries;
		p.transactions = undoInfo.transactions;
		p.tid = PLAYER.FREE_AGENT;

		if (phase === PHASE.RESIGN_PLAYERS) {
			await idb.cache.negotiations.add({
				pid,
				tid: undoInfo.tid,
				resigning: true,
			});
		}

		await idb.cache.players.put(p);

		if (undoInfo.eid !== undefined) {
			await idb.cache.events.delete(undoInfo.eid);
		}

		delete local.undoableActions[pid];
		void toUI("realtimeUpdate", [["playerMovement"]]);

		return true;
	} else if (info.type === "release") {
		const pid = info.pid;

		const undoInfo = local.undoableActions[pid];
		if (!undoInfo || undoInfo.type !== "release") {
			return false;
		}

		await idb.cache.negotiations.add({
			pid,
			tid: undoInfo.tid,
			resigning: true,
		});

		delete local.undoableActions[pid];
		void toUI("realtimeUpdate", [["playerMovement"]]);

		return true;
	}

	return false;
};

const updateAwards = async (
	newAwards: Pick<Awards, "awards" | "season">,
	conditions: Conditions,
): Promise<any> => {
	const oldAwards = await idb.getCopy.awards(
		{
			season: newAwards.season,
		},
		"noCopyCache",
	);

	if (!oldAwards) {
		throw new Error("oldAwards not found");
	}

	const playersAll = await idb.getCopies.players(
		{
			activeSeason: newAwards.season,
		},
		"noCopyCache",
	);
	const players = await idb.getCopies.playersPlus(playersAll, {
		attrs: ["name", "pid"],
	});

	const awardsToDelete = getAwardsByPlayer(oldAwards.awards, players);
	const awardsToSave = getAwardsByPlayer(newAwards.awards, players);

	await idb.cache.awards.put({
		...oldAwards,
		...newAwards,
	});

	await updatePlayerAwards({
		awardsToDelete,
		awardsToSave,
		logEventInfo: {
			conditions,
		},
		season: g.get("season"),
	});
};

const upgrade65Estimate = async () => {
	// cursor is null if there are no saved box scores. Using IDBObjectStore.count() is slower if there are a lot of games
	const cursor = await idb.league.transaction("games").store.openKeyCursor();
	if (!cursor) {
		return {
			numFeats: 0,
			numPlayoffSeries: 0,
		};
	}

	const [numFeats, numPlayoffSeries] = await Promise.all([
		idb.league.count("playerFeats"),
		idb.league.count("playoffSeries"),
	]);

	return {
		numFeats,
		numPlayoffSeries,
	};
};

const upgrade65 = async () => {
	console.time("upgrade65");
	const transaction = idb.league.transaction(
		["games", "playerFeats", "playoffSeries"],
		"readwrite",
	);
	await upgradeGamesVersion65({
		transaction,
		stopIfTooMany: false,
		lid: g.get("lid"),
	});
	console.timeEnd("upgrade65");
};

const upsertCustomizedPlayer = async (
	{
		p,
		originalTid,
		season,
		recomputePosOvrPot,
	}: {
		p: PlayerWithoutKey;
		originalTid: number | undefined;
		season: number;
		recomputePosOvrPot: boolean;
	},
	conditions: Conditions,
): Promise<number> => {
	if (p.tid >= 0) {
		const t = await idb.cache.teams.get(p.tid);
		if (!t) {
			throw new Error("Invalid tid");
		}

		if (t.retiredJerseyNumbers) {
			const retiredJerseyNumbers = t.retiredJerseyNumbers.map(
				(row) => row.number,
			);
			const jerseyNumber = helpers.getJerseyNumber(p);
			if (jerseyNumber && retiredJerseyNumbers.includes(jerseyNumber)) {
				throw new Error(
					`Jersey number "${jerseyNumber}" is retired by the ${t.region} ${t.name}. Either un-retire it at Team > History or pick a new number.`,
				);
			}
		}

		delete p.numPlayersTradedAwayNormalized;

		// When switching teams, reset some stuff, especially ptModifier
		if (p.tid !== originalTid) {
			p.numDaysFreeAgent = 0;
			p.gamesUntilTradable = 0;
			p.ptModifier = 1;
		}
	}

	// Handle making player a FA
	if (p.tid === PLAYER.FREE_AGENT && originalTid !== PLAYER.FREE_AGENT) {
		player.addToFreeAgents(p, await getNumPlayersTradedAwayNormalizedAll());
	}

	p.imgURL = helpers.stripBbcode(p.imgURL);

	// Fix draft and ratings season
	if (p.tid === PLAYER.UNDRAFTED) {
		if (p.draft.year < season) {
			p.draft.year = season;
		}

		// Once a new draft class is generated, if the next season hasn't started, need to bump up year numbers
		if (p.draft.year === season && actualPhase() >= PHASE.RESIGN_PLAYERS) {
			p.draft.year += 1;
		}

		last(p.ratings).season = p.draft.year;
	} else if (p.tid !== PLAYER.RETIRED) {
		p.retiredYear = Infinity;

		// If a player was a draft prospect (or some other weird shit happened), ratings season might be wrong
		last(p.ratings).season = g.get("season");
	}

	// If player was retired, add ratings (but don't develop, because that would change ratings)
	if (originalTid === PLAYER.RETIRED && p.tid !== PLAYER.RETIRED) {
		if (g.get("season") - last(p.ratings).season > 0) {
			player.addRatingsRow(p);
		}
	}

	// If player is now retired, check HoF eligibility
	if (
		typeof p.pid === "number" &&
		p.tid === PLAYER.RETIRED &&
		originalTid !== PLAYER.RETIRED
	) {
		await player.retire(p as Player, conditions, {
			forceHofNotification: true,
		});
	}

	// Recalculate player pos, ovr, pot, and values if necessary
	const originalPot = last(p.ratings).pot;
	await player.develop(p, 0);
	if (!recomputePosOvrPot) {
		// Make sure not to randomly change pot if it was not necessary (no ratings/age change, and in non-basketball sports no pos change).
		// Why do this here, rather than just calling develop only if this stuff changed? Because develop handles PlayerRatings.pos being set to the right value too, and that can change in BBGM even if no ratings change.
		last(p.ratings).pot = originalPot;
	}
	await player.updateValues(p);

	if (p.tid >= 0 && p.tid !== originalTid) {
		if (!p.transactions) {
			p.transactions = [];
		}
		p.transactions.push({
			season: g.get("season"),
			phase: g.get("phase"),
			tid: p.tid,
			type: "godMode",
		});
	}

	// Fill in player names for relatives
	const relatives: Relative[] = [];

	const getInverseType = (type: Player["relatives"][number]["type"]) => {
		let type2: typeof type;
		if (type === "father") {
			type2 = "son";
		} else if (type === "son") {
			type2 = "father";
		} else {
			type2 = "brother";
		}

		return type2;
	};

	const ensureRelationExists = async (
		p: Player,
		p2: Player,
		type: Player["relatives"][number]["type"],
	) => {
		const type2 = getInverseType(type);

		let name = p.firstName;
		if (p.lastName) {
			name += ` ${p.lastName}`;
		}

		const existingRelation = p2.relatives.find(
			(rel) => rel.type === type2 && rel.pid === p.pid,
		);
		if (existingRelation) {
			// We found relation! Make sure name is correct
			if (name !== existingRelation.name) {
				existingRelation.name = name;
				await idb.cache.players.put(p2);
			}
		} else {
			// Need to add this relation
			p2.relatives.push({
				type: type2,
				pid: p.pid,
				name,
			});
			await idb.cache.players.put(p2);
		}
	};

	for (const rel of p.relatives) {
		const p2 = await idb.getCopy.players(
			{
				pid: rel.pid,
			},
			"noCopyCache",
		);

		if (p2) {
			rel.name = p2.firstName;
			if (p2.lastName) {
				rel.name += ` ${p2.lastName}`;
			}
		}

		if (rel.name !== "") {
			// This will keep names of deleted players too, just not blank entries
			relatives.push(rel);
		}
	}

	p.relatives = relatives;

	const prevPlayer =
		p.pid !== undefined
			? await idb.getCopy.players({ pid: p.pid }, "noCopyCache")
			: undefined;
	if (prevPlayer) {
		// Any relation in here that is no longer in p should be deleted in the corresponding player too
		for (const prevRel of prevPlayer.relatives) {
			const currentRel = p.relatives.find(
				(rel) => rel.type === prevRel.type && rel.pid === prevRel.pid,
			);
			if (!currentRel) {
				// prevRel has been deleted!
				const p2 = await idb.getCopy.players(
					{
						pid: prevRel.pid,
					},
					"noCopyCache",
				);
				if (p2) {
					p2.relatives = p2.relatives.filter(
						(rel) =>
							!(
								rel.type === getInverseType(prevRel.type) &&
								rel.pid === prevPlayer.pid
							),
					);
					await idb.cache.players.put(p2);
				}
			}
		}

		// If the injury was added in this edit, do some stuff depending on what the previous injury was
		const editedInjuryType = p.injury.type !== prevPlayer.injury.type;
		const editedInjuryGames =
			p.injury.gamesRemaining !== prevPlayer.injury.gamesRemaining;
		if (editedInjuryType || editedInjuryGames) {
			void recordInjuryForensics({
				source: "edit",
				detail: `p${p.pid} ${p.firstName} ${p.lastName} ${prevPlayer.injury.gamesRemaining}(${prevPlayer.injury.type}) > ${p.injury.gamesRemaining}(${p.injury.type})`,
			});

			let lastInjuriesEntry = p.injuries.at(-1);
			if (lastInjuriesEntry?.type !== prevPlayer.injury.type) {
				// If somehow injuries does not contain the previous injury, ignore it
				lastInjuriesEntry = undefined;
			}

			// Was the injury type changed, or just the duration of injury?
			if (editedInjuryType) {
				// Adjust prevInjuriesEntry, since that old injury no longer applies and it healed prematurely
				if (lastInjuriesEntry) {
					lastInjuriesEntry.games -= prevPlayer.injury.gamesRemaining;
					if (lastInjuriesEntry.games <= 0) {
						// Injury was edited before any days were simmed
						p.injuries.pop();
					}
				}

				if (p.injury.type !== "Healthy") {
					p.injuries.push({
						season: g.get("season"),
						games: p.injury.gamesRemaining,
						type: p.injury.type,
					});
				}
			} else {
				// Only the duration of injury was changed, so adjust lastInjuriesEntry to reflect that
				if (lastInjuriesEntry) {
					const extraGames =
						p.injury.gamesRemaining - prevPlayer.injury.gamesRemaining;
					lastInjuriesEntry.games += extraGames;
					if (lastInjuriesEntry.games <= 0) {
						// Injury was edited before any days were simmed
						p.injuries.pop();
					}
				}
			}
		}
	}

	if (p.tid >= 0) {
		let jerseyNumber = p.jerseyNumber;
		if (jerseyNumber === undefined && actualPhase() <= PHASE.PLAYOFFS) {
			// If no specified jersey number and it's during the season
			jerseyNumber = await player.genJerseyNumber(p);
		}
		if (jerseyNumber !== undefined) {
			// Update stats row if necessary
			player.setJerseyNumber(p, jerseyNumber);

			// Extra write so genJerseyNumber sees it
			await idb.cache.players.put(p);

			// If jersey number is the same as a teammate, edit the teammate's
			const conflicts = (
				await idb.cache.players.indexGetAll("playersByTid", p.tid)
			).filter((p2) => p2.pid !== p.pid && p2.jerseyNumber === jerseyNumber);
			for (const conflict of conflicts) {
				const newJerseyNumber = await player.genJerseyNumber(conflict);
				player.setJerseyNumber(conflict, newJerseyNumber);
				await idb.cache.players.put(conflict);
			}
		}
	}

	// Save to database, adding pid if it doesn't already exist
	await idb.cache.players.put(p);

	// Only after pid is known - update current relatives
	for (const rel of p.relatives) {
		const p2 = await idb.getCopy.players(
			{
				pid: rel.pid,
			},
			"noCopyCache",
		);

		if (p2) {
			await ensureRelationExists(p as Player, p2, rel.type);
		}
	}

	// In case a player was injured or moved to another team
	await recomputeLocalUITeamOvrs();

	// @ts-expect-error
	return p.pid;
};

const clearTrade = async (
	type: "all" | "other" | "user" | "keepUntradeable",
) => {
	await trade.clear(type);
	await toUI("realtimeUpdate", []);
};

const createTrade = async (teams: TradeTeams) => {
	await trade.create(teams);
	await toUI("realtimeUpdate", []);
};

const proposeTrade = async (forceTrade: boolean, conditions: Conditions) => {
	const { teams } = await trade.get();
	const dv = await new ValueChangeCalculator().evaluate({
		tid: teams[1].tid,
		pidsAdd: teams[0].pids,
		pidsRemove: teams[1].pids,
		dpidsAdd: teams[0].dpids,
		dpidsRemove: teams[1].dpids,
		tradingPartnerTid: g.get("userTid"),
	});
	const aiWillAcceptTrade = dv > 0;
	if (
		aiWillAcceptTrade &&
		teams[1].pids.length === 0 &&
		teams[1].dpids.length === 0
	) {
		let assetsText;
		const numAssets = teams[0].pids.length + teams[0].dpids.length;
		if (teams[0].pids.length === 0) {
			assetsText = helpers.plural("Pick", numAssets);
		} else if (teams[0].dpids.length === 0) {
			assetsText = helpers.plural("Player", numAssets);
		} else {
			assetsText = helpers.plural("Asset", numAssets);
		}

		const proceed = await toUI(
			"confirm",
			[
				"Are you sure you want to propose a trade where you receive nothing?",
				{
					okText: `Give Away ${assetsText}`,
				},
			],
			conditions,
		);

		if (!proceed) {
			return;
		}
	}

	const output = await trade.propose(forceTrade);
	await toUI("realtimeUpdate", []);
	return output;
};

const toggleColaOptOut = async () => {
	const t = await idb.cache.teams.get(g.get("userTid"));
	if (!t) {
		throw new Error("Should never happen");
	}

	if (t.draftLottery?.type === "cola") {
		if (t.draftLottery.optOut) {
			delete t.draftLottery.optOut;
		} else {
			t.draftLottery.optOut = true;
		}
	} else {
		// Should never happen
		t.draftLottery = {
			type: "cola",
			chances: 0,
			optOut: true,
		};
	}
	await idb.cache.teams.put(t);

	await toUI("realtimeUpdate", [["draftLottery"]]);
};

const toggleTradeDeadline = async () => {
	const currentPhase = g.get("phase");
	if (currentPhase === PHASE.AFTER_TRADE_DEADLINE) {
		await league.setGameAttributes({
			phase: PHASE.REGULAR_SEASON,
		});

		await updatePlayMenu();
		await toUI("realtimeUpdate", [["newPhase"]]);
	} else if (currentPhase === PHASE.REGULAR_SEASON) {
		await league.setGameAttributes({
			phase: PHASE.AFTER_TRADE_DEADLINE,
		});

		// Delete scheduled trade deadline
		const schedule = await season.getSchedule();
		const tradeDeadline = schedule.find(
			(game) => game.homeTid === -3 && game.awayTid === -3,
		);
		if (tradeDeadline) {
			await idb.cache.schedule.delete(tradeDeadline.gid);
			await toUI("deleteGames", [[tradeDeadline.gid]]);
		}

		await updatePlayMenu();
		await toUI("realtimeUpdate", [["newPhase"]]);
	}
};

const tradeCounterOffer = async () => {
	const response = await trade.makeItWorkTrade();
	await toUI("realtimeUpdate", []);
	return response;
};

const updateTrade = async (teams: TradeTeams) => {
	await trade.updatePlayers(teams);
	await toUI("realtimeUpdate", []);
};

const validatePointsFormula = async (pointsFormula: string) => {
	if (pointsFormula !== "") {
		new PointsFormulaEvaluator(pointsFormula);
	}
};

const validatePlayoffSettings = async ({
	numRounds,
	numPlayoffByes,
	numActiveTeams,
	playIn,
	playoffsByConf,
	confs,
}: {
	numRounds: number;
	numPlayoffByes: number;
	numActiveTeams: number | undefined;
	playIn: boolean;
	playoffsByConf: boolean;
	confs: GameAttributesLeague["confs"];
}) => {
	// Season doesn't matter, since we provide overrides and skipPlayoffSeries
	const byConf = await season.getPlayoffsByConf(Infinity, {
		skipPlayoffSeries: true,
		playoffsByConf,
		confs,
	});

	season.validatePlayoffSettings({
		numRounds,
		numPlayoffByes: season.getNumPlayoffByes({ numPlayoffByes, byConf }),
		numActiveTeams,
		playIn,
		byConf,
	});
};

const getSavedTrade = async (hash: string) => {
	const value = await idb.cache.savedTrades.get(hash);

	// Use 1 and 0 rather than boolean for consistency with watch list, and in case we want to add more trade lists in the future
	return value ? 1 : 0;
};

const setSavedTrade = async ({
	saved,
	hash,
	tid,
}: {
	saved: number;
	hash: string;
	tid: number;
}) => {
	if (saved !== 0) {
		await idb.cache.savedTrades.put({ hash, tid });
	} else {
		await idb.cache.savedTrades.delete(hash);
	}

	await toUI("realtimeUpdate", [["savedTrades"]]);
};

const clearSavedTrades = async (hashes: string[]) => {
	for (const hash of hashes) {
		await idb.cache.savedTrades.delete(hash);
	}

	await toUI("realtimeUpdate", [["savedTrades"]]);
};

// Normally use season.setSchedule, but this skips various checks and saves exactly what the user has edited
const setScheduleFromEditor = async ({
	regenerated,
	schedule,
}: {
	regenerated: boolean;
	schedule: View<"scheduleEditor">["schedule"];
}) => {
	if (regenerated) {
		// It's the regular season with 0 games played and we're allowed to regenerate the schedule (see canRegenerateSchedule). In that case, season.newSchedule uses the latest settings for numGames/numGamesConf/numGamesDiv/divs, both because that's what the user would want (tweaking schedule settings) and because numGamesConf/numGamesDiv are currently not wrapped. So if we know numGames or divs has changed for next season, we need to update the setting for those (and also confs for consistency) to apply to this season.
		// Originally I added this so updateClinchedPlayoffs would work correctly, but now updateClinchedPlayoffs uses the actual upcoming schedule rather than (numGames - GP) so this shouldn't affect that now. TBH I'm not sure if this matters for other things, but probably it does for something at least!
		const season = g.get("season");
		const toAdjust = ["numGames", "divs", "confs"] as const;
		for (const key of toAdjust) {
			const value = g.getRaw(key);
			if (value.length > 1) {
				const updated = helpers.deepCopy(value);

				const lastValue = updated.at(-1)!;
				if (lastValue.start === season + 1) {
					// We need to update! Either change last entry to current season, or delete 2nd-last entry and overwrite 2nd-last one with current value (if 2nd-last entry was only for this season).

					const secondLastValue = updated.at(-2)!;
					if (secondLastValue.start === season) {
						updated.pop();
						secondLastValue.value = lastValue.value;
					} else {
						lastValue.start = season;
					}

					await idb.cache.gameAttributes.put({
						key,
						value: updated,
					});
					g.setWithoutSavingToDB(key, updated);
				}
			}
		}
	}

	await idb.cache.schedule.clear();

	for (const game of schedule) {
		if (game.type === "placeholder" || game.type === "completed") {
			continue;
		}
		await idb.cache.schedule.add(omit(game, ["gid", "type"]));
	}

	// This is needed in case the upcoming game was edited/deleted
	await initUILocalGames();
};

// For the Multiplayer Sync team picker: the teams under multi-team-mode control
// (from userTids), which one this device is currently acting as, and whether
// A preview of the upcoming season calendar for the Auto Play scheduler: one
// entry per scheduled day (games on it, plus whether it's the trade deadline or
// All-Star day), the game-days each "sim day/week/month" advances, and a note for
// when the current phase's schedule runs out. The UI overlays the real-clock fire
// schedule on this to show exactly which league days each auto-sim will cover.
const getAutoPlayPreview = async () => {
	const phase = g.get("phase");

	const schedule = await season.getSchedule();
	const byDay = new Map<
		number,
		{
			day: number;
			numGames: number;
			tradeDeadline: boolean;
			allStar: boolean;
			// The league has asked the sim to pause before this day - see
			// common/simStopDays.ts. Marked here rather than worked out in the UI so
			// the scheduler and the worker can never disagree about where a stop is.
			simStop: boolean;
		}
	>();
	const stops = parseSimStopDays(g.get("simStopDays"));
	for (const item of schedule) {
		if (item.day === undefined) {
			continue;
		}
		let entry = byDay.get(item.day);
		if (!entry) {
			entry = {
				day: item.day,
				numGames: 0,
				tradeDeadline: false,
				allStar: false,
				simStop: stopsOnDay(stops, item.day),
			};
			byDay.set(item.day, entry);
		}
		if (item.homeTid === -3 && item.awayTid === -3) {
			entry.tradeDeadline = true;
			if (stops.deadline) {
				entry.simStop = true;
			}
		} else if (item.homeTid === -1 && item.awayTid === -2) {
			entry.allStar = true;
		} else {
			entry.numGames += 1;
		}
	}
	const upcomingDays = [...byDay.values()].sort((a, b) => a.day - b.day);

	// How many game-days each Play Menu amount advances (mirrors playAmount for the
	// current, playable phase). The actual sim is still capped at the days left.
	const amountDays = {
		day: 1,
		week: !isSport("football") ? 7 : 1,
		month: bySport({ football: 4, default: 30 }),
	};

	let phaseEndNote: string | undefined;
	if (phase === PHASE.REGULAR_SEASON || phase === PHASE.AFTER_TRADE_DEADLINE) {
		phaseEndNote = "Regular season ends, playoffs begin";
	} else if (phase === PHASE.PLAYOFFS) {
		phaseEndNote = "Playoffs end";
	}

	return {
		phase,
		season: g.get("season"),
		upcomingDays,
		amountDays,
		phaseEndNote,
	};
};

// multi-team mode is even set up yet.
const getSyncTeams = async () => {
	const userTids = g.get("userTids");
	const allTeams = await idb.cache.teams.getAll();
	const teams = userTids
		.map((tid) => allTeams.find((t) => t.tid === tid))
		.filter((t) => t !== undefined)
		.map((t) => ({ tid: t.tid, region: t.region, name: t.name }));
	return {
		teams,
		userTid: g.get("userTid"),
		multiTeamMode: userTids.length > 1,
	};
};

// Set this device's draft ready state: ready through overall pick `untilPick`,
// or null to clear. Cloud-only write; the shared ready doc drives pick advance.
const draftSetReady = async (untilPick: number | null) => {
	await setDraftReady(untilPick);
	return { ok: true };
};

// Publish this team's free-agency board (ranked FA pids). Cloud-only write;
// boards resolve when the FA day advances (see faBoard.ts).
const faBoardSet = async (pids: number[]) => {
	await setFaBoard(pids);
	return { ok: true };
};

// User-attached images (see common/types.ts Image). Stored in the synced
// `images` cache store; writes are captured and shared to every device in a
// room automatically (the store has no sync-specific handling). A player's
// gallery is every image tagging that pid; a team's is every image with its tid.
const getImages = async (filter: {
	pid?: number;
	tid?: number;
}): Promise<Image[]> => {
	const all = await idb.cache.images.getAll();
	const filtered = all.filter((image) => {
		if (filter.pid !== undefined) {
			return image.playerIds.includes(filter.pid);
		}
		if (filter.tid !== undefined) {
			return image.tid === filter.tid;
		}
		return true;
	});
	// Newest first.
	return filtered.sort((a, b) => b.at - a.at);
};

const upsertImage = async (image: Image) => {
	await idb.cache.images.put(image);
};

const deleteImage = async (id: string) => {
	await idb.cache.images.delete(id);
};

// Trading cards (see common/tradingCards.ts). Stored in the synced
// `tradingCards` store, so a card made on one device shows up on every device
// in the room - the collection is shared, like the images gallery.
const getTradingCardOptions = async (pid: number) => {
	const p = await idb.getCopy.players({ pid }, "noCopyCache");
	if (!p) {
		return { seasons: [] as number[], name: "" };
	}
	return {
		seasons: getTradingCardSeasons(p),
		name: `${p.firstName} ${p.lastName}`.trim(),
	};
};

const getTradingCardPrompts = async ({
	pid,
	season,
	setId,
	variantId,
	includeName = true,
}: {
	pid: number;
	season: number;
	setId: string;
	variantId: string;
	includeName?: boolean;
}) => {
	const subject = await getTradingCardSubject(pid, season);
	if (!subject) {
		return undefined;
	}
	return {
		// A fresh seed per press, so "Build prompts" again on the same card gives
		// a different photograph instead of the identical one forever.
		front: buildCardFrontPrompt(
			setId,
			variantId,
			subject,
			Math.floor(Math.random() * 1e9),
			{ includeName },
		),
		back: buildCardBackPrompt(setId, variantId, subject, { includeName }),
		title: cardTitle(setId, variantId, season),
		playerName: subject.name,
	};
};

const upsertTradingCard = async (card: TradingCard) => {
	// Only a NEW card is news. This is an upsert, so re-saving an existing one
	// (a replaced image, a re-generated back) must not file a second headline
	// for the same card.
	const isNew = (await idb.cache.tradingCards.get(card.id)) === undefined;

	// Stamp the maker in a shared league, the same name the chat and the live
	// broadcast attribute with. It has to be recorded HERE rather than worked
	// out when the feed renders: the event syncs to everyone, so a headline
	// that said "you" would say it on all three devices.
	//
	// Devices resolve a real name at connect now, but an older room can still be
	// holding the "You" placeholder, and that reads as "You" on every device.
	// Record nobody in that case and take the neutral wording.
	const maker = getSyncEngine()?.localName;
	const saved: TradingCard =
		isNew && card.by === undefined && maker !== undefined && maker !== "You"
			? { ...card, by: maker }
			: card;

	await idb.cache.tradingCards.put(saved);

	if (isNew) {
		const p = await idb.getCopy.players({ pid: saved.pid }, "noCopyCache");
		if (p) {
			const playerLink = `<a href="${helpers.leagueUrl([
				"player",
				p.pid,
			])}">${p.firstName} ${p.lastName}</a>`;
			const cardsLink = `<a href="${helpers.leagueUrl(["create_cards"])}">${
				saved.title
			}</a>`;
			logEvent({
				type: "tradingCard",
				text: saved.by
					? `${saved.by} made a ${cardsLink} card of ${playerLink}.`
					: `A ${cardsLink} card of ${playerLink} was added.`,
				showNotification: false,
				pids: [saved.pid],
				tids: p.tid >= 0 ? [p.tid] : [],
				// Above the "normal" feed's threshold of 10 so it shows by default,
				// below the 20 that "big" filters on - a card someone made is worth
				// seeing in the feed, but it does not outrank a jersey retirement.
				score: 10,
			});
		}
	}

	// playerMovement so an open News page (and the dashboard headlines) picks the
	// new event up without a reload, the same way a jersey retirement does.
	await toUI("realtimeUpdate", [["tradingCards", "playerMovement"]]);
};

const deleteTradingCard = async (id: string) => {
	await idb.cache.tradingCards.delete(id);
	await toUI("realtimeUpdate", [["tradingCards"]]);
};

// Achievement cards (see common/achievementCards.ts): what a season's card set
// SHOULD contain, minus what the synced tradingCards store already holds. No
// queue is stored anywhere - the ids are deterministic, so every device in a
// room derives the same list and a card saved on one crosses it off on all.

// A championship is a team achievement; the cards go to whoever carried it on
// the floor, measured the only way a card can defend: playoff minutes.
const getChampionKeyPlayers = async (
	season: number,
): Promise<{ pid: number; name: string }[]> => {
	const teams = await idb.getCopies.teamsPlus(
		{
			attrs: ["tid"],
			seasonAttrs: ["playoffRoundsWon"],
			season,
		},
		"noCopyCache",
	);
	const numRounds = g.get("numGamesPlayoffSeries", season).length;
	const champ = teams.find((t) => t.seasonAttrs.playoffRoundsWon === numRounds);
	if (!champ) {
		return [];
	}

	const players = await idb.getCopies.players(
		{ statsTid: champ.tid },
		"noCopyCache",
	);
	const withMinutes = players
		.map((p) => {
			let min = 0;
			for (const row of p.stats) {
				if (row.season === season && row.playoffs && row.tid === champ.tid) {
					min += row.min ?? 0;
				}
			}
			return {
				pid: p.pid,
				name: `${p.firstName} ${p.lastName}`.trim(),
				min,
			};
		})
		.filter((p) => p.min > 0);
	withMinutes.sort((a, b) => b.min - a.min);
	return withMinutes
		.slice(0, CHAMPION_CARD_PLAYERS)
		.map(({ pid, name }) => ({ pid, name }));
};

const getAchievementCardData = async ({
	season,
	context,
}: {
	season: number;
	// "draft" derives the class's top picks (Draft History page); "season"
	// derives awards, All-Stars and champions (season History page).
	context: "draft" | "season";
}) => {
	let expected: AchievementCardSpec[];
	if (context === "draft") {
		const options = await getGlobalSettings();
		const numPicks =
			options.achievementCardsDraftPicks ?? DEFAULT_ACHIEVEMENT_DRAFT_PICKS;
		const players = await idb.getCopies.players(
			{ draftYear: season },
			"noCopyCache",
		);
		expected = deriveDraftAchievementCards({
			season,
			picks: players
				.filter((p) => p.draft.round === 1 && p.draft.pick >= 1)
				.map((p) => ({
					pid: p.pid,
					name: `${p.firstName} ${p.lastName}`.trim(),
					pick: p.draft.pick,
				})),
			numPicks,
		});
	} else {
		const awardsRow = await idb.getCopy.awards({ season });
		const awards = awardsRow
			? await legacyAwardsWithNames(awardsRow)
			: undefined;
		// No All-Stars. Twenty-odd selections a season against one MVP buried
		// the awards that are actually rare, so the season list is the awards,
		// the named teams and the champions (see deriveSeasonAchievementCards).
		expected = deriveSeasonAchievementCards({
			season,
			awards,
			champions: await getChampionKeyPlayers(season),
		});
	}

	const existing = new Set(
		(await idb.cache.tradingCards.getAll()).map((card) => card.id),
	);
	const pending = expected.filter((spec) => !existing.has(spec.id));
	return {
		pending,
		total: expected.length,
		done: expected.length - pending.length,
	};
};

const getAchievementCardPrompts = async ({
	pid,
	season,
	setId,
	variantId,
	kind,
	label,
	scene,
	includeName = true,
}: {
	pid: number;
	season: number;
	setId: string;
	variantId: string;
	kind: AchievementKind;
	label: string;
	scene?: DraftCardScene;
	includeName?: boolean;
}) => {
	const subject = await getTradingCardSubject(pid, season);
	if (!subject) {
		return undefined;
	}
	// One seed per press, shared: it picks the action on an ordinary card and the
	// defensive moment on a defensive one, so both re-roll rather than coming
	// back identical.
	const actionSeed = Math.floor(Math.random() * 1e9);
	const override = {
		...achievementPromptOverride(
			{ kind, label, season, pid },
			subject,
			scene,
			actionSeed,
		),
		includeName,
	};
	return {
		front: buildCardFrontPrompt(
			setId,
			variantId,
			subject,
			actionSeed,
			override,
		),
		back: buildCardBackPrompt(setId, variantId, subject, override),
		title: `${cardTitle(setId, variantId, season)} · ${label}`,
		playerName: subject.name,
	};
};

// Set a player's primary display image (imgURL) - e.g. "use this gallery image
// as the profile picture". getCopy + cache.put works for retired players too
// (they aren't held in the cache), mirroring updatePlayerWatch.
const setPlayerImage = async ({
	pid,
	imgURL,
}: {
	pid: number;
	imgURL: string;
}) => {
	const p = await idb.getCopy.players({ pid }, "noCopyCache");
	if (!p) {
		throw new Error("Invalid pid");
	}
	p.imgURL = helpers.stripBbcode(imgURL);
	await idb.cache.players.put(p);
	await toUI("realtimeUpdate", [["playerMovement"]]);
};

// Set a team's primary logo (imgURL or the small variant), mirroring how
// updateTeamInfo propagates a logo change: onto the team, the current-season
// row the roster page reads, and the teamInfoCache game attribute.
const setTeamImage = async ({
	tid,
	imgURL,
	small,
}: {
	tid: number;
	imgURL: string;
	small?: boolean;
}) => {
	const t = await idb.cache.teams.get(tid);
	if (!t) {
		throw new Error("Invalid tid");
	}
	const cleaned = helpers.stripBbcode(imgURL);
	if (small) {
		t.imgURLSmall = cleaned;
	} else {
		t.imgURL = cleaned;
	}
	await idb.cache.teams.put(t);

	if (actualPhase() < PHASE.PLAYOFFS) {
		const teamSeason = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsByTidSeason",
			[tid, g.get("season")],
		);
		if (teamSeason && !t.disabled) {
			if (small) {
				teamSeason.imgURLSmall = t.imgURLSmall;
			} else {
				teamSeason.imgURL = t.imgURL;
			}
			if (teamSeason.imgURLSmall === "") {
				delete teamSeason.imgURLSmall;
			}
			await idb.cache.teamSeasons.put(teamSeason);
		}
	}

	const teams = await idb.cache.teams.getAll();
	await league.setGameAttributes({
		teamInfoCache: orderBy(teams, "tid").map((t2) => ({
			abbrev: t2.abbrev,
			disabled: t2.disabled,
			imgURL: t2.imgURL,
			imgURLSmall: t2.imgURLSmall === "" ? undefined : t2.imgURLSmall,
			name: t2.name,
			region: t2.region,
		})),
	});
	await toUI("realtimeUpdate", [["team"]]);
};

// The current league's sync checkpoint, embedded in full league exports: the
// room fingerprint this file belongs to plus the change-log position its data
// already includes. A re-import that keeps everything can then join its room
// and catch up from here, instead of replaying the entire room history.
const getSyncCheckpoint = async (): Promise<
	{ leagueId: string; watermark: number } | undefined
> => {
	const lid = g.get("lid");
	if (typeof lid !== "number") {
		syncDebugLog("export:checkpoint", { outcome: "no-lid" });
		return undefined;
	}
	const metaLeague = await idb.meta.get("leagues", lid);
	if (!metaLeague?.syncLeagueId) {
		// The exporting league was never bound to a room, so there's no checkpoint
		// to embed - a re-import will replay from zero. This is the usual reason an
		// "already up to date" export still catches up from the beginning.
		syncDebugLog("export:checkpoint", {
			outcome: "no-syncLeagueId",
			hasMeta: !!metaLeague,
			syncWatermark: metaLeague?.syncWatermark ?? 0,
		});
		return undefined;
	}
	// The live engine's position is fresher than the (debounced) meta row.
	const engine = getSyncEngine();
	const connectedSeq =
		getConnectedLid() === lid ? (engine?.getPersistedSeq() ?? 0) : 0;
	const watermark = Math.max(metaLeague.syncWatermark ?? 0, connectedSeq);
	if (watermark <= 0) {
		syncDebugLog("export:checkpoint", {
			outcome: "zero-watermark",
			syncLeagueId: metaLeague.syncLeagueId,
			metaWatermark: metaLeague.syncWatermark ?? 0,
			connectedSeq,
		});
		return undefined;
	}
	syncDebugLog("export:checkpoint", {
		outcome: "ok",
		leagueId: metaLeague.syncLeagueId,
		watermark,
		metaWatermark: metaLeague.syncWatermark ?? 0,
		connectedSeq,
	});
	return { leagueId: metaLeague.syncLeagueId, watermark };
};

// The saved play-by-play of a live-simmed game (for rewatching), or undefined
// if none exists. Reads the cache first (a game live-simmed this session), then
// disk (older games, or a replay synced in from another device).
const getLiveGamePlayByPlay = async (gid: number) => {
	if (typeof gid !== "number" || Number.isNaN(gid)) {
		return undefined;
	}
	let row = await idb.cache.liveGamePlayByPlay.get(gid);
	if (!row) {
		try {
			row = await (idb.league as any).get("liveGamePlayByPlay", gid);
		} catch {
			// Store missing / read failed - no replay.
		}
	}
	return row?.playByPlay;
};

// The chat that happened while a game was live-simmed, saved alongside its
// replay so re-watching shows the conversation at the moments it happened.
const getLiveGameChat = async (gid: number) => {
	if (typeof gid !== "number" || Number.isNaN(gid)) {
		return [];
	}
	let row = await idb.cache.liveGamePlayByPlay.get(gid);
	if (!row) {
		try {
			row = await (idb.league as any).get("liveGamePlayByPlay", gid);
		} catch {
			// Store missing / read failed - no chat.
		}
	}
	return row?.chat ?? [];
};

// Cheap existence check for a saved replay (used to decide whether to show the
// "Watch replay" button) - avoids loading the whole play-by-play payload.
const hasLiveGameReplay = async (gid: number) => {
	if (typeof gid !== "number" || Number.isNaN(gid)) {
		return false;
	}
	if (await idb.cache.liveGamePlayByPlay.get(gid)) {
		return true;
	}
	try {
		const key = await (idb.league as any).getKey("liveGamePlayByPlay", gid);
		return key !== undefined;
	} catch {
		return false;
	}
};

// Toggle worker-side sync debug logging (see debugLog.ts). Driven by the UI at
// startup from the localStorage key "syncDebugLog".
const setSyncDebugLoggingApi = async (enabled: boolean) => {
	setSyncDebugLogging(!!enabled);
	return { ok: true };
};

// Heartbeat this device's live lottery-reveal position to the room (it just
// ran the lottery / revealed another pick). Cloud-only write.
const lotteryRevealUpdate = async (update: {
	active: boolean;
	season?: number;
	revealed?: number;
	startedAt?: number;
}) => {
	await publishLotteryRevealState(update);
	return { ok: true };
};

// Register this device for phone push notifications. The FCM token is obtained
// on the UI thread (Cloud Messaging can't run in a worker) and handed here so we
// can store it - alongside this device's team and display name - in the room's
// members list, which the Cloud Function reads to deliver pushes.
const registerPushToken = async ({
	token,
	name,
}: {
	token: string;
	name: string;
}) => {
	const engine = getSyncEngine();
	if (!engine) {
		throw new Error(
			"Connect to a shared league before enabling notifications.",
		);
	}
	await engine.registerMember({
		fcmToken: token,
		// The caller doesn't ask for a name, so don't let a blank one overwrite
		// the one this device already resolved.
		name: name.trim() === "" ? engine.localName : name,
		tid: g.get("userTid"),
	});
	return { ok: true };
};

// This device's display name in shared rooms. Blank means "use the team I
// manage", which is what an unconfigured device does.
const getSyncDeviceName = async () => {
	// Resolved rather than read off the engine, so the sync page can show what
	// the fallback WOULD be before this device has ever connected - which is
	// exactly when someone is looking at this field.
	return {
		stored: (await loadSyncDeviceName()) ?? "",
		effective: await resolveSyncLocalName(),
	};
};

const setSyncDeviceName = async (name: string) => {
	const trimmed = name.trim();
	if (trimmed === "") {
		await idb.meta.delete("attributes", "syncDeviceName");
	} else {
		await idb.meta.put("attributes", trimmed, "syncDeviceName");
	}
	// Applies immediately to a live room, and is picked up at connect otherwise.
	await refreshSyncLocalName();
	return { ok: true };
};

export default {
	actions,
	awardSettings,
	eightyTwoZeroDraft,
	exhibitionGame,
	leagueFileUpload,
	playMenu,
	toolsMenu,
	main: {
		acceptContractNegotiation,
		addTeam,
		advancedPlayerSearch,
		allStarDraftAll,
		allStarDraftOne,
		allStarDraftUser,
		allStarDraftReset,
		allStarDraftSetPlayers,
		allStarGameNow,
		allowCrossingNextSimStop,
		autoSortRoster,
		beforeView,
		cancelContractNegotiation,
		checkAccount: checkAccount2,
		checkSyncReady,
		checkParticipationAchievement,
		clearInjuries,
		clearSavedTrades,
		claimSyncAuthority,
		clearNotes,
		clearPlayerSeasonRecaps,
		clearTrade,
		clearWatchList,
		connectSharedLeague,
		countNegotiations,
		createLeague,
		createTrade,
		deleteOldData,
		deleteScheduledEvent,
		deleteScheduledEvents,
		disconnectSharedLeague,
		discardUnsavedProgress,
		draftLottery,
		draftSetReady,
		faBoardSet,
		getImages,
		upsertImage,
		deleteImage,
		getAchievementCardData,
		getAchievementCardPrompts,
		getTradingCardOptions,
		getTradingCardPrompts,
		upsertTradingCard,
		deleteTradingCard,
		setPlayerImage,
		setTeamImage,
		draftUser,
		dunkGetProjected,
		dunkSetControlling,
		contestSetPlayers,
		dunkSimNext,
		dunkUser,
		evalOnWorker,
		exportDraftClass,
		getExportFilename,
		exportPlayerAveragesCsv,
		exportPlayerGamesCsv,
		generateFace: generateFace2,
		getAutoPlayPreview,
		getAutoPos,
		getAutoSimSafety: getSimSafety,
		getBornLoc,
		getPlayerFaces,
		getDefaultInjuries,
		getDefaultNewLeagueSettings,
		getDefaultTragicDeaths,
		getDiamondInfo,
		getJerseyNumberConflict,
		getLeagueInfo,
		getLeagueName,
		getLeagues,
		getLiveGameChat,
		getLiveGamePlayByPlay,
		hasLiveGameReplay,
		liveSimBlocksDaySim,
		getNegotiationProps,
		getNumPlayoffTeams,
		getPlayerGraphStat,
		getPlayersCommandPalette,
		getLocal,
		getPlayerBioInfoDefaults,
		getPlayerSelectedStats,
		getPlayerTeamStats,
		sportsbookPlaceBet,
		sportsbookPlaceBetSlip,
		sportsbookCancelBet,
		sportsbookSettle,
		trivia82Options,
		trivia82Simulate,
		triviaNewGrid,
		triviaGridCatalog,
		triviaCustomGrid,
		triviaPlayerCard,
		triviaPlayerProfile,
		triviaPublishScores,
		triviaRemoteScores,
		triviaFaces,
		triviaNewTeamRound,
		triviaTeamCatalog,
		getPlayerWatch,
		getProjectedAttendance,
		getRandomCollege,
		getRandomCountry,
		getRandomInjury,
		getRandomJerseyNumber,
		getRandomName,
		getRandomRatings,
		getRandomTeams,
		getSavedTrade,
		getTradeHistoryDump,
		getDayGamesForRecap,
		getSeasonRecapData,
		getPlayerRecapData,
		filePlayerSeasonRecaps,
		fileTeamSeasonRecaps,
		getSyncActivity,
		getSyncCheckpoint,
		getSyncDebugSnapshot,
		getSyncStatus,
		getSyncTeams,
		getSyncDeviceName,
		setSyncDeviceName,
		listSyncRooms,
		deleteSyncRoom,
		deleteAllSyncRooms,
		pruneSyncRoomChangesApi,
		pruneAllSyncRoomChangesApi,
		lotteryRevealUpdate,
		publishAutoPlayState,
		resyncSharedLeague,
		reportUnsyncedDays,
		pushUnsyncedDays,
		reportDayPush,
		pushDay,
		getTeamGraphStat,
		getTradingBlockOffers,
		ping,
		handleUploadedDraftClass,
		idbCacheFlush,
		importPlayers,
		importPlayersGetReal,
		incrementTradeProposalsSeed,
		init,
		initGold,
		loadRetiredPlayers,
		lockSet,
		markSyncRequired,
		ovr,
		proposeTrade,
		ratingsStatsPopoverInfo,
		sendLiveChatMessage,
		reSignAll,
		realtimeUpdate,
		refreshSyncUIState,
		syncNudge,
		regenerateDraftClass,
		regenerateSchedule,
		registerPushToken,
		releasePlayer,
		expandVote,
		relocateVote,
		cloneLeague,
		removeLeague,
		removePlayers,
		reorderDepthDrag,
		reorderDraftDrag,
		reorderRosterDrag,
		resetPlayingTime,
		revertTrade,
		simIntrasquadGame,
		retiredJerseyNumberDelete,
		retiredJerseyNumberUpsert,
		runBefore,
		setForceWin,
		setForceWinAll,
		setGOATFormula,
		setLocal,
		setNote,
		setSavedTrade,
		setScheduleFromEditor,
		setSyncDebugLogging: setSyncDebugLoggingApi,
		setTeamFinancesPlan,
		setUserTidLocal,
		updateExpansionDraftSetup,
		advanceToPlayerProtection,
		autoProtect,
		cancelExpansionDraft,
		updateProtectedPlayers,
		startExpansionDraft,
		startFantasyDraft,
		switchTeam,
		takeControlTeam,
		threeSimNext,
		toggleColaOptOut,
		toggleTradeDeadline,
		tradeCounterOffer,
		onLiveSimOver,
		updateLiveBroadcast,
		endLiveBroadcast,
		watchLiveBroadcast,
		leaveLiveBroadcast,
		undoAction,
		updateAwards,
		updateBudget,
		updateConfsDivs,
		updateDefaultSettingsOverrides,
		socialAccountCreate,
		socialAccountRemove,
		socialAccountReset,
		socialAccountSave,
		socialAccountsBatch,
		updateGameAttributes,
		updateGameAttributesGodMode,
		updateKeepRosterSorted,
		updateRotation,
		updateKeyboardShortcuts,
		updateLeague,
		updateMultiTeamMode,
		updateOptions,
		updatePlayThroughInjuries,
		revertPlayerFace,
		updatePlayerFace,
		updatePlayerUntouchable,
		updatePlayerWatch,
		updatePlayersWatch,
		updatePlayingTime,
		updatePlayoffTeams,
		updateScheduledEvent,
		updateTeamCourt,
		updateTeamInfo,
		updateTrade,
		upgrade65,
		upgrade65Estimate,
		upsertCustomizedPlayer,
		validatePointsFormula,
		validatePlayoffSettings,
	},
};
