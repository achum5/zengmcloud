// Records exactly which cache records changed, for cloud sync. This is the
// low-level primitive the sync layer drains to build changesets. It is
// deliberately dependency-free (imports nothing) so it is safe to import from
// Cache.ts without any circular-dependency risk.
//
// It is DISABLED by default, so it has zero effect on normal play until the
// sync layer explicitly turns it on (e.g. once a league is loaded and we're in
// a synced session). While disabled, record() is a single boolean check.

type ChangeType = "put" | "delete";

type PendingChange = {
	store: string;
	id: number | string;
	type: ChangeType;
};

// Store names are fixed lowercase identifiers with no colon, so a colon cleanly
// separates the store from the (possibly arbitrary) id with no collisions.
const key = (store: string, id: number | string) => `${store}:${id}`;

let enabled = false;
// How many runSuppressed calls are in flight. A COUNTER, not a boolean:
// suppressed calls overlap constantly (a live-broadcast heartbeat every ~400ms,
// beforeView/runBefore on every navigation), and the old save/restore boolean
// had a fatal interleaving bug - call B captures prev=true while call A is
// running; A finishes and restores false; B finishes and restores TRUE - leaving
// recording off FOREVER. From then on every local change (sims included) was
// silently invisible to sync until a page refresh, while playing felt normal.
// A counter is symmetric under any interleaving: it always returns to 0.
let suppressDepth = 0;
// How many sims are currently capturing. A sim runs fire-and-forget, so its
// writes interleave (across awaits) with any runSuppressed call that happens
// concurrently - a read-only view load, a cloud-only broadcast heartbeat, etc.
// Because `suppressed` is a GLOBAL flag that spans awaits, those interleaved sim
// writes would be silently dropped and never published: the sim advances locally
// but its delta never reaches the cloud, stranding every other device forever.
// While this is > 0, runSuppressed becomes a pass-through so nothing can swallow
// the sim's writes. Bracketed by game/play.ts around the sim.
let simDepth = 0;
const pending = new Map<string, PendingChange>();

export const changeTracker = {
	enable() {
		enabled = true;
	},

	disable() {
		enabled = false;
	},

	isEnabled() {
		return enabled;
	},

	// Forget everything pending without producing a changeset. Used after
	// league load/import, where the whole DB is the baseline and per-record
	// deltas are meaningless.
	reset() {
		pending.clear();
	},

	// Called from Cache on every add/put ("put") and delete ("delete"). Keyed by
	// store+id so only the latest intent per record is kept - a put-then-delete
	// collapses to a delete, matching whole-record last-write-wins semantics.
	record(store: string, id: number | string, type: ChangeType) {
		// While a sim is capturing, recording WINS over suppression: a suppressed
		// call that started just before the sim (a slow view load spanning the sim
		// start) must not swallow the sim's first writes. Suppressed calls are
		// read-only during a sim window, so nothing extra gets recorded.
		if (!enabled || (suppressDepth > 0 && simDepth === 0)) {
			return;
		}
		pending.set(key(store, id), { store, id, type });
	},

	// For sync debug logs, so a capture wedge is diagnosable from the console.
	debugState() {
		return { enabled, suppressDepth, simDepth };
	},

	size() {
		return pending.size;
	},

	// Drop a specific record from the pending set. Used right after APPLYING a
	// remote change: we let the write record normally (so a concurrent local
	// action's writes to OTHER records are never lost), then immediately forget
	// just the record we applied so it isn't re-broadcast. This replaces the old
	// global "suppressed" flag for applies, which could silently swallow a
	// concurrent local sim's writes and leave it unpublished.
	forget(store: string, id: number | string) {
		pending.delete(key(store, id));
	},

	// Return all pending changes and clear the buffer.
	drain(): PendingChange[] {
		const out = [...pending.values()];
		pending.clear();
		return out;
	},

	// Mark the start/end of a sim's capture window. While active, runSuppressed is
	// a pass-through, so a concurrent runSuppressed call (a read-only view load,
	// etc.) can't swallow the sim's interleaved writes. Counted, so nested/repeated
	// sims are safe; game/play.ts brackets each sim with these.
	beginSim() {
		simDepth += 1;
	},

	endSim() {
		simDepth = Math.max(0, simDepth - 1);
	},

	// Put changes back after a failed sync attempt, so they can be retried by a
	// later capture rather than silently disappearing.
	restore(changes: PendingChange[]) {
		for (const change of changes) {
			pending.set(key(change.store, change.id), change);
		}
	},

	// Run fn with recording suppressed. Used for local-only/bulk calls (league
	// import, read-only view fetches) that must not capture at all.
	async runSuppressed<T>(fn: () => Promise<T>): Promise<T> {
		// Never engage the global suppress counter while a sim is capturing - it
		// would swallow the sim's concurrent, interleaved writes (see simDepth). The
		// calls routed here during a sim are read-only view loads and cloud-only
		// calls with no idb writes, so skipping suppression for them changes nothing
		// they'd write.
		if (simDepth > 0) {
			return await fn();
		}
		suppressDepth += 1;
		try {
			return await fn();
		} finally {
			suppressDepth = Math.max(0, suppressDepth - 1);
		}
	},
};
