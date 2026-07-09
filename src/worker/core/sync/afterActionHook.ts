import { shouldTraceSyncLabel, syncDebugLog } from "./debugLog.ts";

// A dependency-free indirection so the game engine can trigger a cloud-sync
// publish WITHOUT statically importing the sync layer (which would create an
// import cycle: game/play -> sync/afterAction -> league/loadGameAttributes ->
// game). worker/index.ts (which already imports afterAction) registers it here at
// startup; game/play.ts calls runAfterActionHook when a multi-day sim finishes,
// so its accumulated changes get drained + published even though the dispatched
// action already returned (fire-and-forget "until playoffs"/etc.).
//
// options.silent still PUBLISHES the changeset (so sync stays sound) but skips
// the phone push - used when simming a single game within a day, which
// shouldn't ping anyone.

export type AfterActionOptions = { silent?: boolean };

type AfterActionFn = (
	type: string,
	name: string,
	options?: AfterActionOptions,
) => Promise<boolean>;

let hook: AfterActionFn | undefined;

export const setAfterActionHook = (fn: AfterActionFn) => {
	hook = fn;
};

export const runAfterActionHook = async (
	type: string,
	name: string,
	options?: AfterActionOptions,
) => {
	const label = `${type}.${name}`;
	const trace = shouldTraceSyncLabel(label);
	if (trace) {
		syncDebugLog("afterActionHook:start", {
			label,
			hasHook: hook !== undefined,
			silentOption: !!options?.silent,
		});
	}

	if (!hook) {
		if (trace) {
			syncDebugLog("afterActionHook:missing", { label });
		}
		return true;
	}

	try {
		const synced = await hook(type, name, options);
		if (trace) {
			syncDebugLog("afterActionHook:complete", { label, synced });
		}
		return synced;
	} catch (error) {
		if (trace) {
			syncDebugLog("afterActionHook:failed", { label, error });
		}
		throw error;
	}
};

// While a SINGLE-game sim (a live sim, or "Sim one game") is in flight, its game
// result must never trigger a phone push - only a full day/week/month sim does.
// The sim's OWN drain is already silent, but a live sim navigates to the live
// game and animates playback, so an interleaved worker call can drain the game
// changeset first under a non-silent label. And buildNotifications detects a sim
// by CONTENT (it sees `games`), so it would fire regardless of the label. This
// flag lets afterAction force silent for the ENTIRE single-game-sim window, no
// matter what ends up draining the changeset. Set/cleared in game/play.ts.
let singleGameSimActive = false;

export const setSingleGameSimActive = (active: boolean) => {
	singleGameSimActive = active;
};

export const isSingleGameSimActive = () => singleGameSimActive;
