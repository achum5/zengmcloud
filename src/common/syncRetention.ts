// Change-log retention.
//
// Lives in `common` rather than next to the rest of the sync code because the
// admin UI needs RETENTION_DAYS for its trim control, and the build hard-blocks
// UI code from importing worker modules. Keeping it here is what stops the
// admin default from drifting away from the window actually being stamped.
//
// The room's `changes` collection was append-only forever: nothing ever deleted
// a changeset, so a league that had been simmed for months carried every
// multi-MB sim day it had ever published. That is a storage bill that only ever
// goes up, for data every device consumed long ago.
//
// Entries now carry a `ttlAt`, and a Firestore TTL policy on that field deletes
// them once they age out. See RETENTION.md for how to turn the policy on - the
// field alone does nothing until it is enabled, which is deliberate: the field
// is safe to ship ahead of the policy.
//
// THE SAFETY PROBLEM. A device catches up by reading entries newer than its
// watermark. Deleting entries it has ALREADY applied is harmless - that's the
// whole point. But a device that has been away longer than the retention window
// needs entries that no longer exist, and because catch-up is a `ts >`
// range read, it would find nothing missing and quietly declare itself current
// while holding stale records. That is the same silent-divergence class of bug
// as the duplicated-games incident, so it gets an explicit check rather than a
// hope: see `isTooFarBehind`.

// Long enough that no realistic player is caught out (a device away for a month
// and a half is going to need a re-import for other reasons anyway), short
// enough that a room's log stays a bounded window rather than a full history.
export const RETENTION_DAYS = 45;
export const RETENTION_MS = RETENTION_DAYS * 24 * 60 * 60 * 1000;

// Can this device still catch up from the log, or did the entries it needs get
// deleted while it was away?
//
// `oldestSeq` is the seq (server-timestamp millis) of the OLDEST entry still in
// the room's log. If that is newer than our watermark, everything between the
// two is gone and nothing will ever deliver it.
//
// Deliberately based on what is actually IN the log rather than on
// `Date.now() - RETENTION_MS`: a device with a wrong clock would otherwise
// either lock itself out for no reason or, worse, wave through a real gap.
export const isTooFarBehind = (
	watermark: number,
	oldestSeq: number | undefined,
): boolean => {
	// Never synced this room before: there is no gap to have missed, the whole
	// log is ours to read.
	if (watermark <= 0) {
		return false;
	}
	// Empty log - either a brand-new room or one whose entries have all aged
	// out with no activity since. Nothing to be behind on.
	if (oldestSeq === undefined) {
		return false;
	}
	// Equal is fine: an entry we already applied is still the oldest one there.
	return oldestSeq > watermark;
};

// When a change published now should become eligible for deletion. Client clock,
// which is fine: this only decides WHEN data ages out, and `isTooFarBehind`
// reads the real log rather than trusting any clock, so a skewed device can
// make the window a bit wrong but cannot turn that into silent data loss.
export const ttlAtMsFor = (nowMs: number): number => nowMs + RETENTION_MS;
