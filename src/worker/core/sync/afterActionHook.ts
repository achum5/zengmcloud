// A dependency-free indirection so the game engine can trigger a cloud-sync
// publish WITHOUT statically importing the sync layer (which would create an
// import cycle: game/play -> sync/afterAction -> league/loadGameAttributes ->
// game). worker/index.ts (which already imports afterAction) registers it here at
// startup; game/play.ts calls runAfterActionHook when a multi-day sim finishes,
// so its accumulated changes get drained + published even though the dispatched
// action already returned (fire-and-forget "until playoffs"/etc.).

let hook: ((type: string, name: string) => void) | undefined;

export const setAfterActionHook = (fn: (type: string, name: string) => void) => {
	hook = fn;
};

export const runAfterActionHook = (type: string, name: string) => {
	hook?.(type, name);
};
