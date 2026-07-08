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
import { getSyncRequired, startLiveBroadcast } from "./core/sync/connect.ts";

// Let the game engine trigger a publish when a multi-day (fire-and-forget) sim
// finishes, without a static import cycle from game core into the sync layer.
setAfterActionHook(afterAction);

// Same pattern for starting a live-sim broadcast the instant a live single-game
// sim's play-by-play is ready (a no-op unless connected and holding the wheel).
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
// fetches that run on every navigation and produce no changes anyway.
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
]);

// Multiplayer "wheel": while synced, only the device that holds the wheel may
// advance the shared timeline. These sets classify which API calls count as
// "advancing" so the guard below can block them on non-wheel devices.
//
// Play-menu items that DON'T need the wheel: "stop"/"stopAuto" just halt.
// Drafting your OWN player (main.draftUser) is a separate call that isn't
// wheel-locked, so every user can still make their own pick - but the draft
// ADVANCERS (sim one pick / to your next pick / to end) move the shared draft
// past other teams' picks, so only the simmer may run them.
const PLAY_MENU_WHEEL_EXEMPT = new Set(["stop", "stopAuto"]);
// "actions"-type calls that advance the season/live sim. (runDraft/untilPick in
// actions.ts are draft helpers and stay exempt.)
const ACTIONS_WHEEL_LOCKED = new Set(["simGame", "liveGame", "simToGame"]);
// The All-Star weekend is a single shared event (one dunk contest, one 3pt
// contest, one All-Star draft) that the whole league watches - not something each
// device runs its own copy of. So only the wheel-holder may advance or set it up;
// otherwise a follower just opening the page (which auto-advances the contest on a
// timer) would race the simmer and fork the shared state. Kept as its own set so
// these can be wheel-locked WITHOUT driving the sim-busy lease (they fire every
// ~1s, which would flicker the "simming" indicator and spam the control doc).
const ALLSTAR_WHEEL_LOCKED = new Set([
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
const MAIN_WHEEL_LOCKED = new Set([
	"draftLottery",
	"startExpansionDraft",
	"startFantasyDraft",
	...ALLSTAR_WHEEL_LOCKED,
]);

// Does this API call advance the shared timeline (and so require the wheel)?
const isWheelLockedCall = (type: string, name: string): boolean =>
	(type === "playMenu" && !PLAY_MENU_WHEEL_EXEMPT.has(name)) ||
	(type === "actions" && ACTIONS_WHEEL_LOCKED.has(name)) ||
	(type === "main" && MAIN_WHEEL_LOCKED.has(name));

// "main"-type transactions that mutate the SHARED league (not the wheel, so any
// connected member may do them, but they must actually reach the room). These
// require a verified-live connection before running, so a transaction can't be
// made locally while the app only looks connected and then silently not sync.
const MAIN_CONNECTION_REQUIRED = new Set([
	"proposeTrade",
	"acceptContractNegotiation",
	"reSignAll",
	"releasePlayer",
	"draftUser",
	// Roster/lineup edits also rewrite shared records (player.rosterOrder / a
	// team's depth), so they must reach the room too.
	"reorderRosterDrag",
	"reorderDepthDrag",
]);

// Edits that rewrite shared game-state records (player rows, team-seasons) and so
// would COLLIDE with a sim if made on a stale copy while a sim is in flight. On a
// follower these are refused while the wheel-holder is advancing, or while this
// device hasn't yet caught up, so a whole-record overwrite can't silently clobber
// the sim's results (or vice versa). draftUser is intentionally excluded - making
// your own on-the-clock pick is expected to happen during the (wheel-advanced)
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

	// Multiplayer guard. Before advancing the timeline (sim/draft/phase) or making
	// a transaction (trade/signing/cut/draft pick), require that this device (a)
	// holds the wheel when the call advances the timeline, and (b) is ACTUALLY
	// connected to the cloud - not just holding a stale engine object. Blocking
	// here (before the action runs) is what stops a device from mutating locally
	// and diverging when it only looks connected.
	const wheelLocked = isWheelLockedCall(type, name);
	const needsConnection =
		wheelLocked || (type === "main" && MAIN_CONNECTION_REQUIRED.has(name));

	if (needsConnection) {
		const syncEngine = getSyncEngine();
		if (syncEngine) {
			if (wheelLocked && !syncEngine.isAuthority()) {
				const holder =
					syncEngine.getAuthority()?.holderName ?? "Another device";
				util.logEvent(
					{
						type: "error",
						text: `${holder} has the wheel. Take it on the Multiplayer Sync page to sim here.`,
						persistent: true,
					},
					conditions,
				);
				return undefined;
			}

			// Confirm the connection is GENUINELY live, so this change can actually
			// reach the room. This is what catches "looked connected but wasn't" - a
			// dropped listener, expired token, or resumed-from-suspend tab. For a
			// wheel-locked ADVANCE (a sim/draft/phase change), force a real server
			// round-trip rather than trusting recent contact: a sim that runs on a
			// silently-dead socket and then can't upload strands every other device,
			// so the round-trip's latency is a worthwhile price to never let that
			// happen. Transactions keep the cheap check (they're small and the outbox
			// guarantees eventual delivery), and the rapid All-Star contest steps are
			// excluded so they don't round-trip ~once a second.
			const forceLiveCheck = wheelLocked && !ALLSTAR_WHEEL_LOCKED.has(name);
			const live = await syncEngine.verifyConnection(forceLiveCheck);
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

	// Concurrency gate: refuse a conflict-prone edit while the wheel-holder is
	// mid-advance (a sim/phase/draft running or still uploading), or while this
	// device hasn't caught up on what's already in the log. Both are windows where
	// acting would author a whole-record write on a stale world and clobber the
	// sim's results (or lose the edit) under last-write-wins. Skipped for the
	// wheel-holder itself, which is the one doing the advancing.
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
	// view fetches) are suppressed so they don't capture or accumulate.
	if (type === "leagueFileUpload" || SKIP_CHANGESET_CAPTURE.has(name)) {
		return changeTracker.runSuppressed(() => Promise.resolve(call()));
	}

	// A wheel-locked advance by the current holder marks the room "busy", so
	// followers hold off on colliding edits until the resulting changeset is
	// published (in afterAction) - after which the caught-up check takes over.
	const syncEngineForBusy = getSyncEngine();
	// All-Star contest steps fire ~once a second and are tiny; wheel-lock them but
	// don't drive the busy lease with them (it would flicker the follower's
	// "simming" indicator and spam the control doc).
	const marksBusy =
		wheelLocked &&
		!ALLSTAR_WHEEL_LOCKED.has(name) &&
		!!syncEngineForBusy?.isAuthority();
	if (marksBusy) {
		syncEngineForBusy!.markRoomBusy();
	}

	return Promise.resolve(call()).then(
		(value) => {
			// Fire-and-forget: capture/log/push must never add latency to the
			// action's response. (Safe ordering: this microtask drains before the
			// next worker message is processed.)
			const done = afterAction(type, name);
			// Release the lease once the advance has actually been published.
			if (marksBusy) {
				void done.then(() => getSyncEngine()?.clearRoomBusy());
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
