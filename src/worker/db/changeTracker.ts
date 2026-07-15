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
	// For deletes only: a snapshot of the row as it existed at delete time. The
	// sync layer needs it to ship an IDENTITY-based delete for logically-keyed
	// stores (teamSeasons/teamStats), whose autoincrement `rid` diverges across
	// devices - deleting by raw `rid` on a receiver removes whatever local row
	// happens to hold that `rid`, wiping the wrong (often much older) season.
	// Undefined for puts (captureChangeset reads the live value) and for deletes
	// of rows that weren't in the cache.
	value?: any;
};

// Store names are fixed lowercase identifiers with no colon, so a colon cleanly
// separates the store from the (possibly arbitrary) id with no collisions.
const key = (store: string, id: number | string) => `${store}:${id}`;

let enabled = false;

// Capture is OPT-IN, not opt-out: writes are recorded only while a
// cloud-tracked action (runCaptured) or a bulk sim window (beginSim/endSim) is
// in flight. The old model recorded by default and SUPPRESSED around
// non-tracked calls (view loads, heartbeats) - but a global suppress flag
// cannot attribute writes to callers: while any suppressed call was in flight
// (view loads run on every navigation, so nearly always), a concurrently
// executing action's writes were silently swallowed and never synced. Sims
// protected themselves with beginSim; trades/signings/phase changes had no
// protection at all. Inverting the model eliminates the whole class: an open
// capture window always records, calls that never open one (view loads,
// league import) simply never capture, and overlap resolves in favor of
// recording - over-capturing a record is harmless (whole-record idempotent
// writes), losing one forks the league.
let captureDepth = 0;
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
	record(store: string, id: number | string, type: ChangeType, value?: any) {
		if (!enabled || (captureDepth === 0 && simDepth === 0)) {
			return;
		}
		pending.set(key(store, id), { store, id, type, value });
	},

	// For sync debug logs, so a capture wedge is diagnosable from the console.
	debugState() {
		return { enabled, captureDepth, simDepth };
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

	// Run fn as a cloud-tracked action: every write it makes (directly or through
	// code it awaits) is recorded while its window is open. Windows may overlap
	// freely (counted), and a concurrently-running non-captured call cannot
	// switch recording off.
	async runCaptured<T>(fn: () => Promise<T>): Promise<T> {
		captureDepth += 1;
		try {
			return await fn();
		} finally {
			captureDepth = Math.max(0, captureDepth - 1);
		}
	},

	// Run fn WITHOUT opening a capture window. Under the opt-in model this is
	// just fn() - a non-tracked call records nothing on its own - but writes are
	// still recorded if some OTHER capture/sim window is open at the time
	// (capture wins over suppression; see the header comment).
	async runSuppressed<T>(fn: () => Promise<T>): Promise<T> {
		return fn();
	},
};
