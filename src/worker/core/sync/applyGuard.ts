// A last-line-of-defense check evaluated right before a remote changeset writes
// into the local cache. connect.ts registers a guard that verifies the cache
// currently belongs to the league the sync session was opened for - so even if
// a teardown is missed somewhere, another league file's data can never be
// written into whichever league happens to be loaded. Kept in its own tiny
// module (like engineHolder) to avoid a changeset.ts ↔ connect.ts import cycle.

let guard: (() => boolean) | undefined;

export const setApplyGuard = (fn: (() => boolean) | undefined) => {
	guard = fn;
};

// True when applying is safe (or no guard is registered - tests, dev logger).
export const checkApplyGuard = (): boolean => guard?.() ?? true;
