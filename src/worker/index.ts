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
// Play-menu items that DON'T need the wheel: "stop"/"stopAuto" just halt, and
// the draft-advancement items are turn-based (the game only enables them for
// whoever is on the clock), so any user may drive their own draft.
const PLAY_MENU_WHEEL_EXEMPT = new Set([
	"stop",
	"stopAuto",
	"onePick",
	"untilYourNextPick",
	"untilEnd",
]);
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

export type WorkerAPICategory =
	| "actions"
	| "eightyTwoZeroDraft"
	| "exhibitionGame"
	| "leagueFileUpload"
	| "main"
	| "playMenu"
	| "toolsMenu";

// API functions should have at most 2 arguments. First argument is passed here from toWorker. If you need to pass multiple variables, use an object/array. Second argument is Conditions.
promiseWorker.register(([type, name, param], hostID) => {
	const conditions = {
		hostID,
	};

	// @ts-expect-error
	if (!api[type] || !Object.hasOwn(api[type], name)) {
		throw new Error(
			`API call to nonexistant worker function "${type}.${name}"`,
		);
	}

	// Multiplayer wheel guard. While connected to a shared league, a device that
	// doesn't hold the wheel may not advance the timeline. We block BEFORE the
	// action runs (not just its broadcast), which is what stops a non-authority
	// device from simming locally and diverging. Draft picks are exempt (handled
	// by isWheelLockedCall), so everyone can still draft their own team.
	const syncEngine = getSyncEngine();
	if (
		syncEngine &&
		!syncEngine.isAuthority() &&
		isWheelLockedCall(type, name)
	) {
		const holder = syncEngine.getAuthority()?.holderName ?? "Another device";
		util.logEvent(
			{
				type: "error",
				text: `${holder} has the wheel right now, so simming and advancing the league is disabled on this device. To control the league here, go to Multiplayer Sync (under Tools) and choose "Take the wheel".`,
				persistent: true,
			},
			conditions,
		);
		return undefined;
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
