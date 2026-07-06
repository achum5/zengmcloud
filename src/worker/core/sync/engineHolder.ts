import type { SyncEngine } from "./SyncEngine.ts";

// The currently-connected sync engine, if the user has joined a shared league.
// Undefined means offline/single-player, in which case afterAction just logs
// (in dev) and does nothing else.
let engine: SyncEngine | undefined;

export const setSyncEngine = (next: SyncEngine | undefined) => {
	engine = next;
};

export const getSyncEngine = () => engine;
