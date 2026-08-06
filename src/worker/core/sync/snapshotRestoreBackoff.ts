import { syncDebugLog } from "./debugLog.ts";

// How often an AUTOMATIC recovery may re-attempt the same room snapshot.
//
// Restoring one is the single most expensive thing a device can do: download
// the whole league, decompress it, JSON.parse the entire object graph, and
// write every store. On a phone with a deep league that is repeated
// hundred-megabyte allocation, and iOS answers by killing the tab - which then
// reloads, reconnects, and does it again.
//
// v2 learned this the hard way and got a backoff (CHECKPOINT_RESTORE_RETRY_MS).
// v1's automatic paths never did, and they had a worse version of the same
// wound: the behind-the-room healer re-armed its grace window on every exit
// EXCEPT the one a data-missing device actually takes (snapshot restored fine,
// catch-up couldn't reach the head because there IS a gap), so it re-parsed the
// whole league every five-second health tick.
//
// One attempt per snapshot identity per window. A genuinely transient failure
// still retries, just not at tick speed; a deterministic one (a payload this
// build refuses) stops burning the device down.
export const SNAPSHOT_RESTORE_RETRY_MS = 2 * 60 * 1000;

export type SnapshotRestoreAttempt = { key: string; at: number };

// Pure so the timing rule is testable without a clock or a room.
export const shouldAttemptSnapshotRestore = ({
	key,
	last,
	now,
	retryMs = SNAPSHOT_RESTORE_RETRY_MS,
}: {
	// Identifies WHICH snapshot: a fresh one published by the authority is a
	// different proposition and is always allowed through immediately.
	key: string;
	last: SnapshotRestoreAttempt | undefined;
	now: number;
	retryMs?: number;
}): boolean => {
	if (last === undefined || last.key !== key) {
		return true;
	}
	// A stamp from the future (the clock moved backwards) would lock the
	// snapshot out for as long as the skew lasts. Treat it as due.
	if (last.at > now) {
		return true;
	}
	return now - last.at >= retryMs;
};

let lastAttempt: SnapshotRestoreAttempt | undefined;

// Consulted by the automatic recovery paths only. The manual Force Resync
// button never asks: a person who just clicked is not a runaway loop, and
// making them wait out a backoff is how a recovery tool becomes useless.
export const claimSnapshotRestoreAttempt = (key: string): boolean => {
	const now = Date.now();
	if (!shouldAttemptSnapshotRestore({ key, last: lastAttempt, now })) {
		syncDebugLog("snapshot:restore-backoff", {
			key,
			sinceLastMs: now - (lastAttempt?.at ?? 0),
		});
		return false;
	}
	lastAttempt = { key, at: now };
	return true;
};

// Test hook, and used on teardown so a new session starts clean.
export const resetSnapshotRestoreBackoff = () => {
	lastAttempt = undefined;
};
