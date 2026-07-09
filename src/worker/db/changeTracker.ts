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
let suppressed = false;
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
		if (!enabled || suppressed) {
			return;
		}
		pending.set(key(store, id), { store, id, type });
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
		// Never engage the global suppress flag while a sim is capturing - it would
		// swallow the sim's concurrent, interleaved writes (see simDepth). The calls
		// routed here during a sim are read-only view loads and cloud-only calls with
		// no idb writes, so skipping suppression for them changes nothing they'd write.
		if (simDepth > 0) {
			return await fn();
		}
		const prev = suppressed;
		suppressed = true;
		try {
			return await fn();
		} finally {
			suppressed = prev;
		}
	},
};
