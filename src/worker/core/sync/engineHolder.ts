import type { SyncEngine } from "./SyncEngine.ts";
import type { SyncEngineV2 } from "./v2/engine.ts";

// Either protocol's engine. Both present the same consumer surface (mapped
// exhaustively when v2 was written), so everything downstream is agnostic.
export type AnySyncEngine = SyncEngine | SyncEngineV2;

// The currently-connected sync engine, if the user has joined a shared league.
// Undefined means offline/single-player, in which case afterAction just logs
// (in dev) and does nothing else.
let engine: AnySyncEngine | undefined;

export const setSyncEngine = (next: AnySyncEngine | undefined) => {
	engine = next;
};

export const getSyncEngine = () => engine;
