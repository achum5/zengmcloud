import "../common/polyfills.ts";
import api from "./api/index.ts";
import * as common from "../common/constants.ts";
import * as core from "./core/index.ts";
import * as db from "./db/index.ts";
import * as util from "./util/index.ts";
import * as random from "../common/random.ts";
import { promiseWorker } from "./util/promiseWorker.ts";
import { defaultGameAttributes } from "../common/defaultGameAttributes.ts";
import { changeTracker, runExclusive } from "./db/changeTracker.ts";
import {
	captureAfterAction,
	deliverAfterAction,
} from "./core/sync/afterAction.ts";
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

	// Multiplayer wheel guard. In a shared league, advancing the timeline is only
	// allowed on the device that holds the wheel AND is actually connected. We
	// block BEFORE the action runs (not just its broadcast), which is what stops
	// a device from simming locally and diverging. Draft picks are exempt
	// (handled by isWheelLockedCall), so everyone can still draft their own team.
	if (isWheelLockedCall(type, name)) {
		const syncEngine = getSyncEngine();
		if (syncEngine) {
			if (!syncEngine.isAuthority()) {
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
		} else if (getSyncRequired()) {
			// Meant to be synced but not connected (reconnecting after a refresh,
			// or offline). Pause simming so this device can't advance while offline
			// and diverge from everyone else.
			util.logEvent(
				{
					type: "error",
					text: `Reconnecting to the shared league — simming is paused until you're back online. To play offline instead, disconnect on the Multiplayer Sync page.`,
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
	// view fetches) are suppressed so they don't capture or accumulate. Held under
	// the sync lock too, so their suppression can't eat a concurrent apply's/
	// action's writes.
	if (type === "leagueFileUpload" || SKIP_CHANGESET_CAPTURE.has(name)) {
		return runExclusive(() =>
			changeTracker.runSuppressed(() => Promise.resolve(call())),
		);
	}

	// Run the action and capture its changeset UNDER the sync lock, so a remote
	// apply (catch-up / auto-resync) can't interleave and suppress the action's
	// writes mid-flight (which would silently drop the change from the room). The
	// actual publish + push happens AFTER the lock releases - it's network I/O and
	// must not block applies or add latency to the action's response.
	return runExclusive(async () => {
		const value = await call();
		const captured = await captureAfterAction(type, name);
		return { value, captured };
	}).then(({ value, captured }) => {
		if (captured) {
			void deliverAfterAction(captured);
		}
		return value;
	});
});
