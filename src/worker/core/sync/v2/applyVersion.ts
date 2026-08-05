import { idb } from "../../../db/index.ts";
import { changeTracker } from "../../../db/changeTracker.ts";
import { checkApplyGuard } from "../applyGuard.ts";
import { syncDebugLog } from "../debugLog.ts";
import {
	applyRoomSnapshotPayload,
	validateRoomSnapshotPayload,
} from "../roomSnapshot.ts";
import {
	APPLIED_VERSION_KEY,
	decideApply,
	type ApplyDecision,
	type VersionedChangeset,
} from "./protocol.ts";
import { isDeviceLocal, preserveLocalWatch } from "../changeset.ts";

// ---------------------------------------------------------------------------
// The v2 apply: where the version chain touches disk.
//
// The soundness core is ONE property, and everything else in v2 leans on it:
//
//   THE DATA AND THE MARKER COMMIT IN THE SAME INDEXEDDB TRANSACTION.
//
// Every wipe and fork in v1's history reduces to some marker (a watermark, a
// position stamp, a `seen` set) disagreeing with the data it described -
// because they were always written separately, so a kill between the two
// writes manufactured a liar. Here that state cannot be constructed: a killed
// apply rolls back BOTH, a completed apply commits BOTH, and on relaunch the
// marker read from the database is, by construction, the truth about the
// database.
// ---------------------------------------------------------------------------

// Read the applied version straight from the league DB - never from a cache
// or a mirror, because the DB row is the one thing that provably matches the
// DB's data.
export const readAppliedVersion = async (): Promise<number> => {
	const row = await (idb.league as any).get(
		"gameAttributes",
		APPLIED_VERSION_KEY,
	);
	return typeof row?.value === "number" ? row.value : 0;
};

// Patch the in-memory cache to match records just committed to the DB.
// Suppressed so the patch is never re-captured as a local change. The cache
// is a mirror here, not a participant: if the process dies before this runs,
// the next launch fills the cache from the DB and sees the applied state.
const patchCache = async (
	changes: VersionedChangeset["changeset"]["changes"],
): Promise<void> => {
	await changeTracker.runSuppressed(async () => {
		for (const change of changes) {
			const store = (idb.cache as any)[change.store];
			if (!store) {
				continue;
			}
			if (change.type === "delete") {
				try {
					await store.delete(change.id);
				} catch {
					// Deleting a row the cache never held is a no-op, not a problem.
				}
			} else {
				await store.put(change.value);
			}
		}
	});
};

// Apply one versioned changeset. The admission rule is decideApply and
// nothing else; "gap" is returned to the caller (who recovers via the
// checkpoint), never worked around here.
export const applyVersionedChangeset = async (
	vcs: VersionedChangeset,
): Promise<ApplyDecision["type"]> => {
	if (!checkApplyGuard()) {
		throw new Error(
			"Refusing to apply a v2 version: the loaded league is not the one this sync session belongs to.",
		);
	}

	// Timed so a log capture can tell a slow FETCH from a slow APPLY: the
	// field showed a 3-record signing arriving on the listener instantly and
	// then not painting for 30 seconds, and without this number that window
	// is invisible.
	const startedAt = Date.now();

	const applied = await readAppliedVersion();
	const decision = decideApply(applied, vcs.version);
	if (decision.type !== "apply") {
		syncDebugLog("v2:apply-declined", {
			decision: decision.type,
			applied,
			incoming: vcs.version,
		});
		return decision.type;
	}

	// Same per-record protections the v1 apply gives (see applyChangeset):
	// never let a peer's device-local state (their controlled team, their
	// in-progress trades) overwrite ours, and never let a peer's player record
	// carry THEIR watch-list flag onto this device. Both resolved BEFORE the
	// transaction opens - an IndexedDB transaction dies if it idles across an
	// await.
	const changes = vcs.changeset.changes.filter(
		(change) => !isDeviceLocal(change.store, change.id),
	);
	for (const change of changes) {
		if (change.store === "players" && change.type === "put" && change.value) {
			await preserveLocalWatch(change.value);
		}
	}

	// Every store this delta touches, plus gameAttributes for the marker.
	const stores = [
		...new Set([
			...changes.map((change) => String(change.store)),
			"gameAttributes",
		]),
	];

	// THE transaction. Operations are issued synchronously so it stays active;
	// it commits everything or - killed partway - nothing, marker included.
	const transaction = (idb.league as any).transaction(stores, "readwrite");
	for (const change of changes) {
		const objectStore = transaction.objectStore(String(change.store));
		if (change.type === "delete") {
			objectStore.delete(change.id);
		} else {
			objectStore.put(change.value);
		}
	}
	transaction.objectStore("gameAttributes").put({
		key: APPLIED_VERSION_KEY,
		value: vcs.version,
	});
	await transaction.done;

	// Disk is truth now; make memory agree. Failure here is cosmetic (a reload
	// refills from the DB), so it must never make the apply look failed.
	try {
		await patchCache(changes);
	} catch (error) {
		syncDebugLog("v2:cache-patch-failed", { version: vcs.version, error });
	}

	const ms = Date.now() - startedAt;
	syncDebugLog("v2:applied", {
		version: vcs.version,
		records: vcs.changeset.changes.length,
		action: vcs.action,
		ms,
	});
	// A small delta has no business taking seconds; when it does (Safari
	// IndexedDB stalling after idle is the known culprit), make the stall
	// visible in the capture instead of leaving a silent gap.
	if (ms > 5000) {
		syncDebugLog("v2:slow-apply", {
			version: vcs.version,
			records: vcs.changeset.changes.length,
			ms,
		});
	}
	return "apply";
};

// Restore a full-state checkpoint that represents version `checkpointVersion`.
//
// The checkpoint replaces stores one atomic store-replacement at a time (the
// hardened v1 path, reused - validation, integrity gates, apply guard and
// all), so the marker CANNOT ride in those transactions. Instead it is
// written LAST, alone: until it lands, the marker still reads the old
// version, so a kill anywhere mid-checkpoint leaves a device that simply
// re-runs the whole checkpoint on retry. Re-running is idempotent - each
// store replacement produces the same bytes - so the retry converges, and the
// marker only ever advances over a database that fully IS the checkpoint.
export const applyCheckpointV2 = async (
	payload: unknown,
	checkpointVersion: number,
): Promise<void> => {
	const problems = validateRoomSnapshotPayload(payload as any);
	if (problems.length > 0) {
		throw new Error(
			`Refusing to restore the v2 checkpoint: ${problems.join("; ")}. Nothing on this device was changed.`,
		);
	}

	await applyRoomSnapshotPayload(payload as any);

	const transaction = (idb.league as any).transaction(
		"gameAttributes",
		"readwrite",
	);
	transaction.objectStore("gameAttributes").put({
		key: APPLIED_VERSION_KEY,
		value: checkpointVersion,
	});
	await transaction.done;

	syncDebugLog("v2:checkpoint-applied", { version: checkpointVersion });
};
