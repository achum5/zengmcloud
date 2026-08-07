import type { SyncEngineV2 } from "./v2/engine.ts";

// There is one engine. The alias survives because most consumers only ever
// wanted "whatever engine is connected", and naming that is still useful.
export type AnySyncEngine = SyncEngineV2;

// The currently-connected sync engine, if the user has joined a shared league.
// Undefined means offline/single-player, in which case afterAction just logs
// (in dev) and does nothing else.
let engine: AnySyncEngine | undefined;

export const setSyncEngine = (next: AnySyncEngine | undefined) => {
	engine = next;
};

export const getSyncEngine = () => engine;
