import type { Changeset } from "../changeset.ts";

// ---------------------------------------------------------------------------
// SYNC V2: the version chain.
//
// The v1 engine is a replicated database: every device an independent writer,
// deltas identified by server timestamps, applied with last-write-wins, and
// 9,000 lines of machinery for deciding which of two disagreeing databases is
// right. Every league-eating incident came from some corner of that machinery
// choosing wrong.
//
// V2 deletes the question. The room's state is a single integer, `version`,
// and one rule holds everywhere:
//
//   A DEVICE AT VERSION N MAY APPLY EXACTLY VERSION N+1, ATOMICALLY WITH THE
//   MARKER THAT SAYS SO. NOTHING ELSE. EVER.
//
// - Not version N+2 (that is the day-12-over-missing-day-11 fork; the answer
//   is "gap", and the caller recovers via checkpoint, never by skipping).
// - Not version N again (idempotent duplicate, from a retry or an echo).
// - Not a "probably fine" merge of anything. There are no merges.
//
// Only the sim authority publishes versions, and it publishes them with a
// compare-and-set on the room's version pointer - two racing writers cannot
// both win, so the chain cannot fork at the source either. Followers' edits
// travel as requests the authority folds into its next version.
//
// Everything the v1 engine agonizes over - watermarks against timestamps,
// batch abandonment, era-sorted replays, stale position stamps, divergence
// detection - reduces here to integer comparison. This file is the entire
// protocol, and it is pure so every rule is directly testable.
// ---------------------------------------------------------------------------

// One link in the chain: version N's delta, applied on top of exactly N-1.
export type VersionedChangeset = {
	version: number;
	authorId: string;
	action: string;
	changeset: Changeset;
	at: number;
};

// The room's pointer document. `checkpointVersion` names the newest full-state
// checkpoint, so a device too far behind the retained delta window knows where
// to restart the chain from.
export type RoomVersionState = {
	version: number;
	authorId: string;
	byName: string;
	at: number;
	checkpointVersion?: number;
};

export type ApplyDecision =
	| { type: "apply" }
	// Already have it. A retry, an echo, or a re-subscribe overlap. Applying
	// again would be harmless (whole-record puts) but pointless; skipping is
	// cheaper and keeps "applied exactly once" trivially true.
	| { type: "duplicate" }
	// The chain is missing links between us and this delta. Applying would
	// fork the league; the only honest moves are fetching the missing links or
	// restarting from a checkpoint. NEVER apply over a gap.
	| { type: "gap"; missingFrom: number; missingThrough: number };

// The whole admission rule. Everything the caller may do with an incoming
// version funnels through this one decision.
export const decideApply = (
	appliedVersion: number,
	incomingVersion: number,
): ApplyDecision => {
	if (incomingVersion <= appliedVersion) {
		return { type: "duplicate" };
	}
	if (incomingVersion === appliedVersion + 1) {
		return { type: "apply" };
	}
	return {
		type: "gap",
		missingFrom: appliedVersion + 1,
		missingThrough: incomingVersion - 1,
	};
};

// Which versions a device needs to fetch to walk from where it is to the
// room's head, in the order they must apply. Bounded by the caller against
// the retained window; an empty range means "already caught up".
export const versionsToFetch = (
	appliedVersion: number,
	roomVersion: number,
): number[] => {
	const out: number[] = [];
	for (let v = appliedVersion + 1; v <= roomVersion; v++) {
		out.push(v);
	}
	return out;
};

// Can this device catch up on deltas alone, or does it need the checkpoint?
// Deltas older than the checkpoint may be pruned, so the checkpoint is the
// floor: a device at or above it walks deltas; below it, it restores the
// checkpoint first and walks from there. With no checkpoint published yet,
// deltas are never pruned and the chain reaches back to the start.
export const catchUpPlan = (
	appliedVersion: number,
	room: RoomVersionState,
):
	| { type: "caught-up" }
	| { type: "deltas"; versions: number[] }
	| { type: "checkpoint-then-deltas"; checkpointVersion: number; versions: number[] } => {
	if (appliedVersion >= room.version) {
		return { type: "caught-up" };
	}
	const checkpoint = room.checkpointVersion;
	if (checkpoint !== undefined && appliedVersion < checkpoint) {
		return {
			type: "checkpoint-then-deltas",
			checkpointVersion: checkpoint,
			versions: versionsToFetch(checkpoint, room.version),
		};
	}
	return { type: "deltas", versions: versionsToFetch(appliedVersion, room.version) };
};

// The gameAttributes key holding this device's applied version. It lives in
// the LEAGUE database - not meta, not a side store - because the entire
// soundness argument is that the marker and the data it describes commit in
// the same IndexedDB transaction. A marker that can disagree with the data is
// v1's watermark all over again.
export const APPLIED_VERSION_KEY = "syncV2AppliedVersion";
