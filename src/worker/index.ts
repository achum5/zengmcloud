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
import {
	ALLSTAR_SIM_AUTHORITY_LOCKED,
	isSimAuthorityLockedCall,
} from "./core/sync/actionLabels.ts";
import {
	decideOwnGameSimCall,
	isOwnGameSimCall,
} from "./core/sync/ownGameSimGate.ts";
import { setCurrentAction } from "./util/actionContext.ts";
import { syncDebugLog } from "./core/sync/debugLog.ts";
import { setAfterActionHook } from "./core/sync/afterActionHook.ts";
import { setLiveBroadcastStartHook } from "./core/sync/liveBroadcastHook.ts";
import { getSyncEngine } from "./core/sync/engineHolder.ts";
import { getLeaguePosition } from "./core/sync/leaguePosition.ts";
import {
	getSimSafety,
	getSyncRequired,
	restoreSyncRequiredFromMeta,
	startLiveBroadcast,
} from "./core/sync/connect.ts";

// Let the game engine trigger a publish when a multi-day (fire-and-forget) sim
// finishes, without a static import cycle from game core into the sync layer.
setAfterActionHook(afterAction);

// Same pattern for starting a live-sim broadcast the instant a live single-game
// sim's play-by-play is ready (a no-op unless connected). The simmer's game
// broadcasts as the room's watch-party; anyone else's own-game live sim
// broadcasts as opt-in - a header pill on every other device.
setLiveBroadcastStartHook((gid, playByPlay) => {
	void startLiveBroadcast(gid, playByPlay);
});

self.bbgm = {
	...self.bbgm,
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
	// The debug snapshot is FOR diagnosing a broken device - it must never be
	// eaten by the not-caught-up guard (which is the exact state being diagnosed).
	"getSyncDebugSnapshot",
	// Pure read (builds a text report); no league writes.
	"getTradeHistoryDump",
	// The popover's fetch was the one read of its kind NOT listed further down,
	// so during any catch-up the guard answered undefined, the caller
	// dereferenced it, and the popover opened blank and never recovered.
	"ratingsStatsPopoverInfo",
	// Live game chat: a cloud write and a UI push, no league mutation. It must
	// also stay available WHILE catching up - chat during a live sim is exactly
	// when a device is busiest applying the sim it is watching.
	"sendLiveChatMessage",
	"getSyncEngine",
	"getSyncStatus",
	"listSyncRooms",
	"markSyncRequired",
	"publishAutoPlayState",
	"refreshSyncUIState",
	// Foreground sync kick from the UI (probe + drain): cloud reads/writes
	// only, no league mutations, and it must run even mid-catch-up.
	"syncNudge",
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
	"watchLiveBroadcast",
	// The watch/star list and the "untouchable" trade flag are PERSONAL
	// preferences (the watch ones fan out via same-device crossTabEmit, not the
	// league). They happen to live on the shared `players` record, so they can't
	// be excluded at the store level like `trade` - but these actions only ever
	// mutate the `watch` / `untouchableTid` field, so suppressing their capture
	// keeps a toggle from spamming the room a whole-player changeset (and from
	// forcing everyone else's UI to show your stars / untouchables).
	"updatePlayerWatch",
	"updatePlayersWatch",
	"clearWatchList",
	"updatePlayerUntouchable",
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
	// Read-only fetch of a player/team image gallery (see api getImages).
	"getImages",
	// Read-only lookup of a saved live-game replay.
	"getLiveGamePlayByPlay",
	"getLiveGameChat",
	"hasLiveGameReplay",
	// Read-only sync-checkpoint lookup for league exports.
	"getSyncCheckpoint",
	// Trivia games: pure reads that generate a puzzle/round from league
	// history. Never mutate the league, and must stay playable on a
	// multiplayer follower device (mid-catch-up included).
	"triviaNewGrid",
	"triviaGridCatalog",
	"triviaCustomGrid",
	"triviaPlayerCard",
	"triviaNewTeamRound",
	// Cloud-only write of this team's free-agency board (no league data).
	"faBoardSet",
	// Which of this device's teams it controls (userTid). userTid is per-device
	// and never synced (see the changeset NEVER_SYNC set), so - unlike the general
	// updateGameAttributes, which is sim-authority-locked - every league-mate must
	// be able to pick their own team even while someone else is in charge of
	// simming or this device is still catching up. Suppressing capture keeps it
	// out of both the authority guard and the changeset.
	"setUserTidLocal",
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

// Multiplayer "sim authority" classification lives in core/sync/actionLabels.ts,
// shared with the sync engines (which use it to tell a timeline advance from an
// ordinary edit on the publish path).

// Edits that rewrite shared game-state records (player rows, team-seasons) and so
// would COLLIDE with a sim if made on a stale copy while a sim is in flight. On a
// follower these are refused while the sim authority is advancing, or while this
// device hasn't yet caught up, so a whole-record overwrite can't silently clobber
// the sim's results (or vice versa). draftUser is intentionally excluded - making
// your own on-the-clock pick is expected to happen during the (sim-authority-advanced)
// draft and has its own turn logic.
const SIM_CONFLICT_GATED = new Set([
	"proposeTrade",
	"revertTrade",
	"acceptContractNegotiation",
	"reSignAll",
	"releasePlayer",
	"reorderRosterDrag",
	"reorderDepthDrag",
	// The rotation plan is read by the sim exactly as the depth chart is.
	"updateRotation",
]);

export type WorkerAPICategory = keyof typeof api;

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

	// Cheap lag tripwire (only logs when sync debug logging is opted in): any
	// call whose pre-action guard blocked noticeably gets named, so "the UI
	// feels slow" is diagnosable from a console paste instead of guesswork.
	const guardStart = Date.now();

	// Every timeline-advance click leaves a breadcrumb BEFORE any guard runs.
	// A sim click that produces nothing on screen and nothing in the log is
	// undiagnosable; with this, the capture always shows the click happened,
	// and whatever follows (or doesn't) names where it died.
	if (simAuthorityLocked && syncEngine) {
		syncDebugLog("api:sim-call", { type, name });
	}

	if (needsConnection) {
		if (syncEngine) {
			// One carve-out: your OWN team's single game. A one-gid sim is a
			// disjoint slice of the day and simDayClaimPolicy's fence refuses any
			// overlapping claim atomically, so this cannot double-sim a game
			// whatever the UI allows. See core/sync/ownGameSimGate.ts.
			let ownGameSimAllowed = false;
			if (
				simAuthorityLocked &&
				!syncEngine.isAuthority() &&
				isOwnGameSimCall(type, name)
			) {
				const decision = await decideOwnGameSimCall(param);
				ownGameSimAllowed = decision.allow;
				if (!decision.allow) {
					syncDebugLog("api:guard-refused", {
						type,
						name,
						step: "own-game",
					});
					util.logEvent(
						{ type: "error", text: decision.reason, persistent: true },
						conditions,
					);
					return undefined;
				}
			}

			if (
				simAuthorityLocked &&
				!syncEngine.isAuthority() &&
				!ownGameSimAllowed
			) {
				syncDebugLog("api:guard-refused", { type, name, step: "authority" });
				// "You" is the pre-rename placeholder some rooms still have stored;
				// reading it back at another device would say "You is in charge".
				const holderName = syncEngine.getAuthority()?.holderName;
				const holder =
					holderName === undefined || holderName === "You"
						? "Another device"
						: holderName;
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
				syncDebugLog("api:guard-refused", { type, name, step: "caught-up" });
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

			// Timeline advances (sims, phase changes, draft advancers) fork the room
			// if they run on a half-dead connection, so they pay for FORCED probes:
			// a fresh ping plus a genuine server round-trip, every time. Ordinary
			// edits don't: their deltas are durable-first (queued in the outbox and
			// guaranteed to upload), so they use the CACHED checks - instant while
			// recent contact is confirmed, probing only once contact goes stale.
			// Forcing both round-trips on every call made every interactive screen
			// (each Trade click, roster toggle, etc.) block on the network for
			// hundreds of ms and feel broken.
			try {
				await syncEngine.ensureReady(simAuthorityLocked);
			} catch (error) {
				syncDebugLog("api:guard-refused", {
					type,
					name,
					step: "ensure-ready",
					error: String(error),
				});
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

			// NOT forced, even for a timeline advance.
			//
			// Forcing it made every sim wait on a getDocFromServer round-trip, and
			// on a phone that read times out (6s) often enough to be the thing
			// standing between the user and a working Sim button - clicked, waited,
			// nothing, "Not connected to the cloud right now". The connection was
			// fine; one probe was slow.
			//
			// It was inherited from the old protocol, where a half-dead connection
			// could fork a room because a sim was merged from stale state. The chain
			// cannot fork that way: an advance is published as version N+1 by
			// compare-and-set, and one authored on a base the room has moved past is
			// discarded rather than merged. And the check immediately below is a
			// real server read that must reach the head before anything advances -
			// strictly better evidence than "a document was readable a moment ago".
			const live = await syncEngine.verifyConnection();
			if (!live) {
				syncDebugLog("api:guard-refused", { type, name, step: "verify" });
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

			// A timeline advance must run on the room's LATEST state, not on the
			// "seen-relative" caught-up flag checked above. isCaughtUp() is relative to
			// what THIS device has seen, so a silently-stalled changes listener (the
			// socket is live - verifyConnection passes - but the changes onSnapshot has
			// quietly stopped delivering) reports caught-up while game data stops
			// arriving. An advance run then reads a stale whole-record aggregate - most
			// dangerously playoffSeries, a single per-season record rewritten wholesale
			// on every game - and, under record-level last-write-wins, clobbers a result
			// another device just recorded. That's the mid-day simmer-handoff hazard:
			// device B takes over and live-sims ITS game off a playoffSeries that's
			// missing the series win device A just recorded, then publishes the stale
			// record over A's. So, exactly like the draft-advance preflight
			// (core/sync/draftReady.ts), force a real change-log drain to the head here.
			// catchUp() returns true ONLY when this pass reached the head; false means a
			// fetch failed, more pages remain, or another drain is already in flight - in
			// any of those the device isn't provably current, so refuse to advance and
			// let the user retry once caught up (the backstop poll keeps draining). Only
			// timeline advances pay this extra round-trip; ordinary edits are
			// durable-first deltas that can't re-derive shared state from stale reads.
			if (simAuthorityLocked) {
				const drained = await syncEngine.catchUp();
				if (!drained || !syncEngine.isCaughtUp()) {
					syncDebugLog("api:guard-refused", {
						type,
						name,
						step: "advance-catchup",
						drained,
					});
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

				// Everything above says the CONNECTION is healthy; none of it says
				// the DEVICE is. A device whose local state was corrupted (parked at
				// a phantom phase by a bad replay) has its watermark at the head and
				// drains clean - and then sims its corruption into the shared log
				// for the whole room. Health is judged against evidence the broken
				// state can't vouch for: a pending repair, or disagreeing with the
				// room's stamped position.
				// A THROW here used to be the one fully-silent death left in the
				// guard (it escaped every toast and log). Treat it as unsafe.
				let safety: Awaited<ReturnType<typeof getSimSafety>>;
				try {
					safety = await getSimSafety();
				} catch (error) {
					safety = {
						safe: false,
						reason: `the sim preflight failed (${
							error instanceof Error ? error.message : String(error)
						}). Try again in a moment.`,
					};
				}
				if (!safety.safe) {
					syncDebugLog("api:guard-refused", {
						type,
						name,
						step: "sim-safety",
						reason: safety.reason,
					});
					util.logEvent(
						{
							type: "error",
							text: `This wasn't done: ${safety.reason}`,
							persistent: true,
						},
						conditions,
					);
					return undefined;
				}
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

	const guardMs = Date.now() - guardStart;
	if (guardMs > 100) {
		syncDebugLog("api:guard-slow", { type, name, guardMs });
	}

	// https://github.com/microsoft/TypeScript/issues/21732
	// @ts-expect-error
	// Stamp which action is running so deep side effects (a phase change
	// several calls down) can attribute themselves to the click that caused
	// them. Cleared in a finally: a stale label would blame the wrong action.
	const call = async () => {
		setCurrentAction(`${type}.${name}`);
		try {
			return await api[type][name](param, conditions);
		} finally {
			setCurrentAction(undefined);
		}
	};

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
					if (synced) {
						// Stamp where the league now sits along with the lease release, so
						// followers have a second opinion on how far the room has got - one
						// that doesn't come from their own change log, which is exactly the
						// thing that fails silently when a day goes missing.
						getSyncEngine()?.clearRoomBusy(await getLeaguePosition());
					} else {
						// The advance is queued, NOT in the cloud. Stamping the new
						// position now would announce a day whose data followers cannot
						// fetch, and every one of them would grind recovery against a
						// gap that is not there to download. Release the lease without
						// a position; the outbox drain that finally lands the upload
						// restamps (SyncEngine.doDrain).
						getSyncEngine()?.clearRoomBusy();
					}
				}
				if (!synced) {
					// Held on purpose while the league is simming is not a failure,
					// and calling it one sends people looking for a connection
					// problem that isn't there.
					const held = getSyncEngine()?.isHeldForSim() ?? false;
					util.logEvent(
						{
							type: held ? "info" : "error",
							text: held
								? `The league is simming, so this will go to the room as soon as the sim finishes.`
								: `This change is saved and queued for the cloud — it will upload automatically when the connection recovers.`,
							persistent: true,
						},
						conditions,
					);
				}
				return value;
			},
			(error) => {
				// The advance threw, so nothing will publish - drop the lease now rather
				// than making followers wait it out. No position: nothing moved.
				if (marksBusy) {
					getSyncEngine()?.clearRoomBusy();
				}
				// A thrown action used to die silently (the UI never awaits these
				// rejections): no toast, no log, a button that "does nothing". Say
				// what happened in both places, then rethrow.
				if (getSyncEngine()) {
					syncDebugLog("api:call-failed", {
						type,
						name,
						error: String(error),
					});
					util.logEvent(
						{
							type: "error",
							text: `That didn't finish: ${
								error instanceof Error ? error.message : String(error)
							}`,
							persistent: true,
						},
						conditions,
					);
				}
				throw error;
			},
		);
});
