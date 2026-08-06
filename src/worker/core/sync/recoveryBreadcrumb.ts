import { idb } from "../../db/index.ts";
import { syncDebugLog } from "./debugLog.ts";

// A durable "I am about to do the expensive thing" note.
//
// Restoring a room snapshot reads the entire league into memory, decompresses
// it, parses the whole object graph and writes every store. On a phone with a
// deep league that is the single biggest allocation the app ever makes, and
// when it is too big the OS does not throw an error - it kills the tab. The PWA
// then reloads, reconnects, decides it still needs to recover, and does it
// again. From the outside: "I click sim and the page crashes and reloads",
// forever.
//
// Nothing in memory can break that loop, because the loop is what destroys
// memory. An in-process backoff is reset by the very crash it is meant to
// bound. So the note is written to disk BEFORE the work starts and cleared
// after it finishes: an attempt still on disk at the next launch is proof the
// last one never returned, and a device that has already been killed once by a
// particular payload stops volunteering for it.
//
// The note doubles as the diagnosis. The debug log lives in memory and dies
// with the tab, which is exactly why this class of bug is invisible - so what
// survives here is deliberately enough to name the culprit in a support
// capture: which operation, when, and how many times it failed to return.

export type RecoveryAttempt = {
	// Identifies the work, specifically enough that a DIFFERENT payload counts
	// as a fresh proposition (e.g. "snapshot-restore:500:gen2").
	op: string;
	startedAt: number;
	// How many times this exact op has been started and never finished.
	failures: number;
};

// One unreturned attempt is enough. The operation is deterministic: the same
// payload on the same device will exhaust the same memory. A second "let's see"
// is just another crash, and the user has already had all of those they can
// stand.
export const MAX_UNFINISHED_ATTEMPTS = 1;

export const shouldSkipRecovery = (
	prior: RecoveryAttempt | undefined,
	op: string,
): boolean =>
	prior !== undefined &&
	prior.op === op &&
	prior.failures >= MAX_UNFINISHED_ATTEMPTS;

// What the next attempt's record should say, given whatever the last one left
// behind. A leftover record for the SAME op means that attempt never returned.
export const nextAttempt = (
	prior: RecoveryAttempt | undefined,
	op: string,
	now: number,
): RecoveryAttempt => ({
	op,
	startedAt: now,
	failures: prior !== undefined && prior.op === op ? prior.failures + 1 : 1,
});

// Never throws. This is bookkeeping ABOUT a recovery; it must not be the thing
// that stops one from happening (or, worse, the new way a launch dies).
const readRow = async (lid: number | undefined) => {
	if (typeof lid !== "number") {
		return undefined;
	}
	try {
		return await idb.meta.get("leagues", lid);
	} catch (error) {
		syncDebugLog("recovery:breadcrumb-read-failed", { error: String(error) });
		return undefined;
	}
};

export const readRecoveryAttempt = async (
	lid: number | undefined,
): Promise<RecoveryAttempt | undefined> =>
	(await readRow(lid))?.syncRecoveryAttempt;

// Returns false when this op has already killed the app and must not be
// attempted automatically again. Otherwise records the attempt (durably, before
// any of the expensive work) and returns true.
export const claimRecoveryAttempt = async (
	lid: number | undefined,
	op: string,
	{ gated = true }: { gated?: boolean } = {},
): Promise<boolean> => {
	const league = await readRow(lid);
	if (!league) {
		// No meta row to remember with. Don't block the recovery over it.
		return true;
	}
	const prior = league.syncRecoveryAttempt;
	if (gated && shouldSkipRecovery(prior, op)) {
		syncDebugLog("recovery:skipped-after-crash", {
			op,
			failures: prior?.failures,
		});
		return false;
	}
	league.syncRecoveryAttempt = nextAttempt(prior, op, Date.now());
	try {
		await idb.meta.put("leagues", league);
	} catch (error) {
		// Unrecorded, so a crash here will not be detected next launch - still
		// better than refusing a recovery the device may badly need.
		syncDebugLog("recovery:breadcrumb-write-failed", { error: String(error) });
	}
	return true;
};

// The work returned - whether it succeeded, refused, or threw. Any of those
// means the app survived it, which is the only thing this record tracks.
export const clearRecoveryAttempt = async (
	lid: number | undefined,
	// Only clear the note if it is still OUR note. Two heavy operations can be
	// bracketed in the same league, and clearing someone else's record would
	// hand a crashing operation a fresh life.
	op?: string,
) => {
	const league = await readRow(lid);
	if (op !== undefined && league?.syncRecoveryAttempt?.op !== op) {
		return;
	}
	if (league?.syncRecoveryAttempt !== undefined) {
		delete league.syncRecoveryAttempt;
		try {
			await idb.meta.put("leagues", league);
		} catch (error) {
			// A stale note only costs one skipped AUTOMATIC attempt; Force Resync
			// still works, and the next successful clear fixes it.
			syncDebugLog("recovery:breadcrumb-clear-failed", {
				error: String(error),
			});
		}
	}
};
