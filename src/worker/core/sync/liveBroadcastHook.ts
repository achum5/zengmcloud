// A dependency-free indirection so the game engine can start a live-sim
// broadcast WITHOUT statically importing the sync layer (which would create an
// import cycle, exactly like afterActionHook). worker/index.ts wires it to the
// real implementation in connect.ts at startup; game/play.ts calls
// runLiveBroadcastStart the moment a live single-game sim's play-by-play is
// ready, so followers navigate with minimal lag behind the simmer.
//
// The hook is a no-op unless this device is connected AND is in charge of simming, so a
// single-player live sim never touches the cloud.

type LiveBroadcastStartFn = (gid: number, playByPlay: any[]) => void;

let startHook: LiveBroadcastStartFn | undefined;

export const setLiveBroadcastStartHook = (fn: LiveBroadcastStartFn) => {
	startHook = fn;
};

export const runLiveBroadcastStart = (gid: number, playByPlay: any[]) => {
	startHook?.(gid, playByPlay);
};
