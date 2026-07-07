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
const pending = new Map<string, PendingChange>();

// Serializes the two things that must never interleave: a local action's write+
// capture window, and a remote changeset being applied (which suppresses
// recording). Without this, an apply running concurrently with a local sim would
// flip `suppressed` on mid-sim and silently drop the sim's writes from capture -
// so the sim would never be published (e.g. right after a device takes the wheel
// while it's still catching up / auto-resyncing). A simple promise-chain mutex.
let lockTail: Promise<void> = Promise.resolve();

export const runExclusive = async <T>(fn: () => Promise<T> | T): Promise<T> => {
	const prev = lockTail;
	let release: () => void = () => {};
	lockTail = new Promise<void>((resolve) => {
		release = resolve;
	});
	await prev;
	try {
		return await fn();
	} finally {
		release();
	}
};

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

	// Return all pending changes and clear the buffer.
	drain(): PendingChange[] {
		const out = [...pending.values()];
		pending.clear();
		return out;
	},

	// Run fn with recording suppressed. Used while APPLYING a remote changeset
	// so we don't re-capture (and then re-broadcast) changes we just received.
	async runSuppressed<T>(fn: () => Promise<T>): Promise<T> {
		const prev = suppressed;
		suppressed = true;
		try {
			return await fn();
		} finally {
			suppressed = prev;
		}
	},
};
