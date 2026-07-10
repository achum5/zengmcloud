import "../common/polyfills.ts";
import api from "./api/index.ts";
import * as common from "../common/constants.ts";
import * as core from "./core/index.ts";
import * as db from "./db/index.ts";
import * as util from "./util/index.ts";
import * as random from "../common/random.ts";
import { promiseWorker } from "./util/promiseWorker.ts";
import { defaultGameAttributes } from "../common/defaultGameAttributes.ts";
import { changeTracker } from "./db/changeTracker.ts";
import { afterAction } from "./core/sync/afterAction.ts";
import { setAfterActionHook } from "./core/sync/afterActionHook.ts";
import { setLiveBroadcastStartHook } from "./core/sync/liveBroadcastHook.ts";
import { getSyncEngine } from "./core/sync/engineHolder.ts";
import {
	getSyncRequired,
	restoreSyncRequiredFromMeta,
	startLiveBroadcast,
} from "./core/sync/connect.ts";

// Let the game engine trigger a publish when a multi-day (fire-and-forget) sim
// finishes, without a static import cycle from game core into the sync layer.
setAfterActionHook(afterAction);

// Same pattern for starting a live-sim broadcast the instant a live single-game
// sim's play-by-play is ready (a no-op unless connected and in charge of simming).
setLiveBroadcastStartHook((gid, playByPlay) => {
	void startLiveBroadcast(gid, playByPlay);
});

self.bbgm = {
	...common,
	...core,
	...db,
	...util,
	api,
	defaultGameAttributes,
	random,
};

if (process.env.NODE_ENV === "development") {
	import("./core/debug/index.ts").then(({ default: debug }) => {
		self.bbgm.debug = debug;
	});

	// Turn on cloud-sync change tracking so we can watch changesets in the
	// console as we play. Dev only - no effect on production.
	changeTracker.enable();
}

// Calls we never want to changeset-log: bulk league import, and read-only view
// fetches that run on every navigation and produce no changes anyway. These are
// also allowed while cloud sync is not ready because they do not publish shared
// league changes.
const SKIP_CHANGESET_CAPTURE = new Set([
	"createStream",
	// Creating/importing a league writes the WHOLE league (its entire baseline -
	// ~1000+ records). That is never a delta to sync: every device gets the initial
	// league by importing the same file, and the room only ever syncs changes made
	// AFTER that. Without this, creating/importing while a session is active would
	// publish the whole league to the room (spamming every follower) and fire a
	// bogus notification (e.g. "the bracket is set", from the imported phase). All
	// its writes are awaited inside the createLeague call, so suppressing the call
	// suppresses the entire creation.
	"createLeague",
	"beforeView",
	"runBefore",
	// Sync room control/status calls do not mutate the league DB. They must remain
	// available so reconnect/readiness recovery can work while the dot is red.
	"claimSyncAuthority",
	"checkSyncReady",
	"connectSharedLeague",
	"deleteAllSyncRooms",
	"deleteSyncRoom",
	"disconnectSharedLeague",
	"getSyncActivity",
	"getSyncEngine",
	"getSyncStatus",
	"listSyncRooms",
	"markSyncRequired",
	"publishAutoPlayState",
	"refreshSyncUIState",
	"resyncSharedLeague",
	// Draft ready-up: writes only the cloud ready doc, never the league DB. Must
	// stay available while catching up so a device can ready up mid-draft.
	"draftSetReady",
	// Debug-log toggle: no league writes.
	"setSyncDebugLogging",
	// Live lottery-reveal heartbeat: cloud-only write, fires per revealed pick.
	"lotteryRevealUpdate",
	// Play-menu stop commands just halt local autoplay; they don't publish a
	// shared league change and must remain available if sync is reconnecting.
	"stop",
	"stopAuto",
	// Generates AI trade offers and saves your personal shopping list
	// (savedTradingBlock) - local-only, not something to sync live.
	"getTradingBlockOffers",
	// Live-sim broadcast control. These only write the cloud broadcast docs - they
	// never mutate league state, so they must not run afterAction. Critically, the
	// broadcaster heartbeats updateLiveBroadcast every ~400ms; without this, one
	// landing in the window between a live sim writing its game and that sim's own
	// SILENT drain would drain the game changeset under a non-silent label and push
	// a "final score" notification - exactly what a live sim must never do.
	"updateLiveBroadcast",
	"endLiveBroadcast",
	// The watch/star list is a PERSONAL preference (note these fan out via
	// same-device crossTabEmit, not the league). It happens to live on the shared
	// `players` record, so it can't be excluded at the store level like `trade` -
	// but these actions only ever mutate the `watch` field, so suppressing their
	// capture keeps a star toggle from spamming the room a whole-player changeset
	// (and from forcing everyone else's UI to show your stars).
	"updatePlayerWatch",
	"updatePlayersWatch",
	"clearWatchList",
	// Read-only fetches the UI fires automatically while rendering (player faces
	// and watch flags in every table, popover lookups, recap panels). Treating
	// these as cloud-tracked mutations broke things two ways: the sync guard
	// refused them mid-catch-up (they resolved undefined and crashed the UI's
	// caches), and their capture window drained a CONCURRENT sim's pending
	// changes - publishing giant mid-sim changesets under labels like
	// "main.getPlayerWatch" while the sim was still running.
	"getPlayerFaces",
	"getPlayerWatch",
	"getBornLoc",
	"getDiamondInfo",
	"getSavedTrade",
	"getDayGamesForRecap",
	"getSeasonRecapData",
	"getLocal",
	"getLeagues",
	"getPlayersCommandPalette",
	"getLeagueName",
	"getExportFilename",
	// Read-only sync-checkpoint lookup for league exports.
	"getSyncCheckpoint",
	// Persists the in-memory cache to disk (the export calls it first). It
	// creates no new deltas, and it must neither steal a running sim's pending
	// changes nor be refused mid-catch-up (which would silently export stale
	// data).
	"idbCacheFlush",
]);

const isChangesetSuppressedCall = (type: string, name: string): boolean =>
	type === "leagueFileUpload" || SKIP_CHANGESET_CAPTURE.has(name);

const isCloudTrackedCall = (type: string, name: string): boolean =>
	!isChangesetSuppressedCall(type, name);

// Multiplayer "sim authority": while synced, only the device that is in charge of simming may
// advance the shared timeline. These sets classify which API calls count as
// "advancing" so the guard below can block them on non-authority devices.
//
// Play-menu items that DON'T need sim authority: "stop"/"stopAuto" just halt.
// Drafting your OWN player (main.draftUser) is a separate call that isn't
// sim-authority-locked, so every user can still make their own pick - but the draft
// ADVANCERS (sim one pick / to your next pick / to end) move the shared draft
// past other teams' picks, so only the simmer may run them.
const PLAY_MENU_SIM_AUTHORITY_EXEMPT = new Set(["stop", "stopAuto"]);
// "actions"-type calls that advance the season/live sim, or advance the shared
// draft past other teams' picks (untilPick = "Sim to this pick", same class as
// playMenu.onePick/untilYourNextPick).
const ACTIONS_SIM_AUTHORITY_LOCKED = new Set([
	"simGame",
	"liveGame",
	"simToGame",
	"untilPick",
]);

// "toolsMenu"-type calls that advance the shared timeline (auto play, skip-to
// phase jumps). Everything else in Tools (resetDb, dangerZone toggles) is
// local-only and stays open.
const TOOLS_MENU_SIM_AUTHORITY_LOCKED = new Set([
	"autoPlaySeasons",
	"skipToPlayoffs",
	"skipToBeforeDraft",
	"skipToAfterDraft",
	"skipToPreseason",
]);
// The All-Star weekend is a single shared event (one dunk contest, one 3pt
// contest, one All-Star draft) that the whole league watches - not something each
// device runs its own copy of. So only the sim authority may advance or set it up;
// otherwise a follower just opening the page (which auto-advances the contest on a
// timer) would race the simmer and fork the shared state. Kept as its own set so
// these can be sim-authority-locked WITHOUT driving the sim-busy lease (they fire every
// ~1s, which would flicker the "simming" indicator and spam the control doc).
const ALLSTAR_SIM_AUTHORITY_LOCKED = new Set([
	"dunkSimNext",
	"threeSimNext",
	"dunkUser",
	"dunkSetControlling",
	"contestSetPlayers",
	"allStarDraftAll",
	"allStarDraftOne",
	"allStarDraftUser",
	"allStarDraftReset",
	"allStarDraftSetPlayers",
]);

// "main"-type calls that restructure/advance the league. A single on-the-clock
// pick (draftUser) is deliberately NOT here - every user drafts their own team.
// Per-team expansion-draft protection (updateProtectedPlayers/autoProtect) is
// also open: each user protects their own roster. Everything below is a
// commissioner-class operation: it advances shared time, restructures the
// league, predetermines results, or bulk-rewrites records - so only the device
// in charge of simming may run it, or two devices editing at once would race
// and fork.
const MAIN_SIM_AUTHORITY_LOCKED = new Set([
	"draftLottery",
	"startExpansionDraft",
	"startFantasyDraft",
	"advanceToPlayerProtection",
	"cancelExpansionDraft",
	"updateExpansionDraftSetup",
	"updateGameAttributes",
	"updateGameAttributesGodMode",
	"setScheduleFromEditor",
	"toggleTradeDeadline",
	"allStarGameNow",
	"updatePlayoffTeams",
	"setForceWin",
	"setForceWinAll",
	"addTeam",
	"updateConfsDivs",
	"regenerateDraftClass",
	"importPlayers",
	"removePlayers",
	"clearInjuries",
	"updateAwards",
	...ALLSTAR_SIM_AUTHORITY_LOCKED,
]);

// Does this API call advance the shared timeline (and so require sim authority)?
const isSimAuthorityLockedCall = (type: string, name: string): boolean =>
	(type === "playMenu" && !PLAY_MENU_SIM_AUTHORITY_EXEMPT.has(name)) ||
	(type === "actions" && ACTIONS_SIM_AUTHORITY_LOCKED.has(name)) ||
	(type === "toolsMenu" && TOOLS_MENU_SIM_AUTHORITY_LOCKED.has(name)) ||
	(type === "main" && MAIN_SIM_AUTHORITY_LOCKED.has(name));

// Edits that rewrite shared game-state records (player rows, team-seasons) and so
// would COLLIDE with a sim if made on a stale copy while a sim is in flight. On a
// follower these are refused while the sim authority is advancing, or while this
// device hasn't yet caught up, so a whole-record overwrite can't silently clobber
// the sim's results (or vice versa). draftUser is intentionally excluded - making
// your own on-the-clock pick is expected to happen during the (sim-authority-advanced)
// draft and has its own turn logic.
const SIM_CONFLICT_GATED = new Set([
	"proposeTrade",
	"acceptContractNegotiation",
	"reSignAll",
	"releasePlayer",
	"reorderRosterDrag",
	"reorderDepthDrag",
]);

export type WorkerAPICategory =
	| "actions"
	| "eightyTwoZeroDraft"
	| "exhibitionGame"
	| "leagueFileUpload"
	| "main"
	| "playMenu"
	| "toolsMenu";

// API functions should have at most 2 arguments. First argument is passed here from toWorker. If you need to pass multiple variables, use an object/array. Second argument is Conditions.
promiseWorker.register(async ([type, name, param], hostID) => {
	const conditions = {
		hostID,
	};

	// @ts-expect-error
	if (!api[type] || !Object.hasOwn(api[type], name)) {
		throw new Error(
			`API call to nonexistant worker function "${type}.${name}"`,
		);
	}

	// Multiplayer guard. Before any cloud-tracked change, require that this device
	// is ACTUALLY ready to upload to the cloud - not just holding a stale engine
	// object. Timeline advances also require being the person in charge of simming.
	// Blocking here (before the action runs) is what stops a device from mutating
	// locally and diverging when it only looks connected.
	const simAuthorityLocked = isSimAuthorityLockedCall(type, name);
	const syncEngine = getSyncEngine();
	const cloudTracked = isCloudTrackedCall(type, name);
	if (cloudTracked) {
		await restoreSyncRequiredFromMeta();
	}
	const syncSessionIntended = syncEngine !== undefined || getSyncRequired();
	const needsConnection = syncSessionIntended && cloudTracked;

	if (needsConnection) {
		if (syncEngine) {
			if (simAuthorityLocked && !syncEngine.isAuthority()) {
				const holder =
					syncEngine.getAuthority()?.holderName ?? "Another device";
				util.logEvent(
					{
						type: "error",
						text: `${holder} is in charge of simming. Go to Multiplayer Sync to sim here.`,
						persistent: true,
					},
					conditions,
				);
				return undefined;
			}

			if (!syncEngine.isCaughtUp()) {
				util.logEvent(
					{
						type: "error",
						text: `Still catching up to the cloud, so this wasn't done. Try again in a moment.`,
						persistent: true,
					},
					conditions,
				);
				return undefined;
			}

			try {
				await syncEngine.ensureReady(true);
			} catch {
				util.logEvent(
					{
						type: "error",
						text: `Cloud sync is not ready right now, so this wasn't done. Check your connection and try again.`,
						persistent: true,
					},
					conditions,
				);
				return undefined;
			}

			// Confirm the connection is GENUINELY live with a real server round-trip
			// before EVERY shared-league mutation. This is intentionally slower: a
			// stale listener/expired token/resumed tab must fail before local state
			// changes, not after a roster edit or sim already diverged.
			const live = await syncEngine.verifyConnection(true);
			if (!live) {
				util.logEvent(
					{
						type: "error",
						text: `Not connected to the cloud right now, so this wasn't done — it wouldn't reach your league-mates. Check your connection and try again in a moment.`,
						persistent: true,
					},
					conditions,
				);
				return undefined;
			}
		} else if (getSyncRequired()) {
			// Meant to be synced but not connected (reconnecting after a refresh,
			// or offline). Pause so this device can't advance while offline and
			// diverge from everyone else.
			util.logEvent(
				{
					type: "error",
					text: `Reconnecting to the shared league — paused until you're back online. To play offline instead, disconnect on the Multiplayer Sync page.`,
					persistent: true,
				},
				conditions,
			);
			return undefined;
		}
	}

	// Concurrency gate: refuse a conflict-prone edit while the sim authority is
	// mid-advance (a sim/phase/draft running or still uploading), or while this
	// device hasn't caught up on what's already in the log. Both are windows where
	// acting would author a whole-record write on a stale world and clobber the
	// sim's results (or lose the edit) under last-write-wins. Skipped for the
	// sim authority itself, which is the one doing the advancing.
	if (type === "main" && SIM_CONFLICT_GATED.has(name)) {
		const syncEngine = getSyncEngine();
		if (
			syncEngine &&
			!syncEngine.isAuthority() &&
			(syncEngine.isRoomBusy() || !syncEngine.isCaughtUp())
		) {
			util.logEvent(
				{
					type: "error",
					text: `The league is simming right now, so this wasn't done — it would collide with the sim. Try again in a moment.`,
					persistent: true,
				},
				conditions,
			);
			return undefined;
		}
	}

	// https://github.com/microsoft/TypeScript/issues/21732
	// @ts-expect-error
	const call = () => api[type][name](param, conditions);

	// When change tracking is off (normal single-player, non-dev), behave
	// exactly as before - zero overhead. It's turned on in dev (for the console
	// logger) and once a shared-league sync session is connected.
	if (!changeTracker.isEnabled()) {
		return call();
	}

	// Bulk/local-only calls (league import, AI trade-offer generation, read-only
	// view fetches) never open a capture window, so on their own they record
	// nothing.
	if (isChangesetSuppressedCall(type, name)) {
		return changeTracker.runSuppressed(() => Promise.resolve(call()));
	}

	// A sim-authority-locked advance by the current holder marks the room "busy", so
	// followers hold off on colliding edits until the resulting changeset is
	// published (in afterAction) - after which the caught-up check takes over.
	const syncEngineForBusy = getSyncEngine();
	// All-Star contest steps fire ~once a second and are tiny; lock them to the
	// person in charge of simming without driving the busy lease (it would flicker
	// the follower's "simming" indicator and spam the control doc).
	const marksBusy =
		simAuthorityLocked &&
		!ALLSTAR_SIM_AUTHORITY_LOCKED.has(name) &&
		!!syncEngineForBusy?.isAuthority();
	if (marksBusy) {
		syncEngineForBusy!.markRoomBusy();
	}

	// Cloud-tracked call: open a capture window for its whole duration, so its
	// writes are recorded even while non-tracked calls (view loads, heartbeats)
	// run concurrently. Fire-and-forget continuations that outlive the call
	// (multi-day sims, autoplay, free agency) keep recording via their own
	// beginSim/endSim brackets.
	return changeTracker
		.runCaptured(() => Promise.resolve(call()))
		.then(
			async (value) => {
				// Wait for the sync handoff for every cloud-tracked mutation. The local
				// action has already happened; before reporting success, make sure its
				// changeset was either confirmed uploaded or durably retained for retry.
				// Otherwise a plain roster/depth edit can vanish while the connection dot
				// still looks green.
				const synced = await afterAction(type, name);
				if (marksBusy) {
					getSyncEngine()?.clearRoomBusy();
				}
				if (!synced) {
					util.logEvent(
						{
							type: "error",
							text: `This change is saved and queued for the cloud — it will upload automatically when the connection recovers.`,
							persistent: true,
						},
						conditions,
					);
				}
				return value;
			},
			(error) => {
				// The advance threw, so nothing will publish - drop the lease now rather
				// than making followers wait it out.
				if (marksBusy) {
					getSyncEngine()?.clearRoomBusy();
				}
				throw error;
			},
		);
});
