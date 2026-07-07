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
import { getSyncEngine } from "./core/sync/engineHolder.ts";
import { getSyncRequired } from "./core/sync/connect.ts";

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
	"beforeView",
	"runBefore",
	// Generates AI trade offers and saves your personal shopping list
	// (savedTradingBlock) - local-only, not something to sync live.
	"getTradingBlockOffers",
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
// "main"-type calls that restructure/advance the league. A single on-the-clock
// pick (draftUser) is deliberately NOT here - every user drafts their own team.
const MAIN_WHEEL_LOCKED = new Set([
	"draftLottery",
	"startExpansionDraft",
	"startFantasyDraft",
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
			// reach the room. Cheap when we've had recent contact; a real, timed
			// server round-trip otherwise. This is what catches "looked connected but
			// wasn't" - a dropped listener, expired token, or resumed-from-suspend tab.
			const live = await syncEngine.verifyConnection();
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

	return Promise.resolve(call()).then((value) => {
		// Fire-and-forget: capture/log/push must never add latency to the
		// action's response. (Safe ordering: this microtask drains before the
		// next worker message is processed.)
		void afterAction(type, name);
		return value;
	});
});
