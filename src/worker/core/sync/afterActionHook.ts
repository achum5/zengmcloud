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
) => void;

let hook: AfterActionFn | undefined;

export const setAfterActionHook = (fn: AfterActionFn) => {
	hook = fn;
};

export const runAfterActionHook = (
	type: string,
	name: string,
	options?: AfterActionOptions,
) => {
	hook?.(type, name, options);
};
