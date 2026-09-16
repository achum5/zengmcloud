import { idb } from "../../db/index.ts";
import { STORES } from "../../db/Cache.ts";
import { normalizeAwardsRow } from "../../db/normalizeAwardsRow.ts";
import { local } from "../../util/index.ts";
import { league } from "../index.ts";
import {
	DEVICE_LOCAL_GAME_ATTRIBUTES,
	DEVICE_LOCAL_STORES,
} from "./changeset.ts";
import { checkApplyGuard } from "./applyGuard.ts";
import { payloadLeagueId, readLocalLeagueId } from "./leagueIdentity.ts";
import { findPayloadIntegrityProblems } from "./leagueIntegrity.ts";
import { syncDebugLog } from "./debugLog.ts";
// ---------------------------------------------------------------------------
// ROOM SNAPSHOTS: the checkpoint layer under the delta log.
//
// The delta log alone cannot be the whole story. A device that falls more than
// a replay window behind - a league-mate away for a month, a fresh install -
// used to be unrecoverable in place: "force resync" replays a bounded window of
// recent entries, and a few seasons in, the entries it would need are millions
// of writes back. The only fix was passing league export files around by hand.
// And because recovery depended on the log reaching arbitrarily far back, no
// entry could ever be deleted, so the log grew without bound.
//
// A snapshot is the full league database, serialized and chunked into the
// room's control docs by the sim authority, stamped with the log watermark it
// contains everything up to. Recovery becomes bounded FOREVER: restore the
// snapshot, jump the watermark to its seq, replay only the tail. And the log
// only needs to reach back one snapshot interval - everything older is covered
// by the snapshot and gets pruned.
//
// Retention protocol: publishing snapshot N prunes entries older than snapshot
// N-1's seq. The log therefore always spans at least one full interval beyond
// the CURRENT snapshot, so a device that is merely "somewhat behind" still
// catches up from deltas alone and never needs the heavier restore.
// ---------------------------------------------------------------------------

// Payload format version, so a future shape change can refuse gracefully
// instead of half-applying.
const SNAPSHOT_VERSION = 1;

type SnapshotPayload = {
	version: number;
	stores: Record<string, unknown[]>;
};

// Everything shared. Per-device stores (staged trade, personal bookmarks)
// never leave the device, exactly as with changesets.
const snapshotStores = () =>
	STORES.filter((store) => !DEVICE_LOCAL_STORES.has(store));

// The full-DB state, read from idb.league after a flush. NOT from the cache:
// the cache is current-season scoped, but a device restoring from far behind is
// missing intervening HISTORY too (past seasons' games, teamSeasons, awards),
// and only the league DB has it.
export const buildRoomSnapshotPayload = async (): Promise<SnapshotPayload> => {
	await idb.cache.flush();
	const stores: Record<string, unknown[]> = {};
	for (const store of snapshotStores()) {
		stores[store] = await (idb.league as any).getAll(store);
	}
	return { version: SNAPSHOT_VERSION, stores };
};

// Stores without which a league is not a league. If a payload is missing one,
// or has one empty, it is not a snapshot worth destroying local data for.
const REQUIRED_NON_EMPTY_STORES = ["players", "teams", "gameAttributes"];

// Is this payload safe to overwrite a working league with? Checked BEFORE
// anything is destroyed, because there is no undo afterwards. A torn download,
// a half-built payload, or a publisher whose own database was broken all look
// like this: structurally fine, catastrophically empty.
export const validateRoomSnapshotPayload = (payload: SnapshotPayload) => {
	const problems: string[] = [];

	// A corrupted download can deserialize into anything; answer with a named
	// problem instead of crashing on property access.
	if (!payload || typeof payload !== "object") {
		problems.push("not a league payload");
		return problems;
	}
	if (payload.version !== SNAPSHOT_VERSION) {
		problems.push(
			`format version ${payload.version}, but this app understands version ${SNAPSHOT_VERSION}`,
		);
		return problems;
	}
	if (!payload.stores || typeof payload.stores !== "object") {
		problems.push("no stores in the payload");
		return problems;
	}
	for (const store of REQUIRED_NON_EMPTY_STORES) {
		const rows = payload.stores[store];
		if (!Array.isArray(rows)) {
			problems.push(`missing the ${store} store`);
		} else if (rows.length === 0) {
			problems.push(`the ${store} store is empty`);
		}
	}
	if (problems.length > 0) {
		return problems;
	}

	// Structure is necessary but not sufficient: the payload also has to
	// describe a league that could actually be played. A publisher whose own
	// rosters were stripped produces a payload that passes every shape check
	// and fails this one - and this is the last moment to stop it, because
	// after apply, this device becomes the next publisher of the same damage.
	problems.push(...findPayloadIntegrityProblems(payload.stores));
	return problems;
};

// Restore a snapshot payload into the league DB: replace each shared store with
// the snapshot's rows, reload game attributes and rebuild the cache. Replacing
// (rather than merging) is what makes a restore complete - rows deleted since
// this device's state, like finished negotiations or played schedule days,
// vanish with the store instead of lingering.
//
// THE THING THAT MATTERS HERE IS THAT IT CANNOT HALF-HAPPEN. This used to clear
// a store and then write its rows back one at a time, each write its own
// auto-committing transaction, tens of thousands of them, with the write-back
// cache still live alongside. Any interruption - and iOS kills a PWA's
// in-flight IndexedDB work the moment the app is backgrounded - left the store
// cleared and only partly refilled, with no way back. That is how a league came
// back with two players on every roster. Now each store is replaced inside ONE
// transaction, so an interrupted restore aborts and leaves the store exactly as
// it was, and the cache is silenced for the duration so it cannot write stale
// rows into a store that has just been emptied.
export const applyRoomSnapshotPayload = async (
	payload: SnapshotPayload,
): Promise<void> => {
	// The same last-line check every remote changeset passes, and this path
	// needs it more than any of them: a changeset that lands in the wrong league
	// writes some rows, while a snapshot restore replaces the entire database.
	// If the loaded league is not the one this sync session belongs to - a
	// missed teardown, a league switch mid-restore - stop before touching disk.
	if (!checkApplyGuard()) {
		throw new Error(
			"Refusing to restore the room's snapshot: the loaded league is not the one this sync session belongs to.",
		);
	}

	const problems = validateRoomSnapshotPayload(payload);
	if (problems.length > 0) {
		throw new Error(
			`Refusing to restore the room's snapshot: ${problems.join("; ")}. Nothing on this device was changed.`,
		);
	}

	// PROVENANCE. Once this league carries an identity, only a payload carrying
	// the SAME identity may replace its database - full stop. This is the check
	// that would have saved a main save twice: a room still holding some other
	// league's state (however it got there - an old build, a zombie engine, a
	// second tab, a reused code) produces a payload whose identity is missing
	// or different, and the restore refuses with the local database untouched.
	// A league with no identity yet (never connected since identities existed)
	// restores as before and inherits the payload's identity with the data.
	// ABSENCE OF EVIDENCE IS NOT EVIDENCE OF MISMATCH. A payload carrying a
	// DIFFERENT identity is positive proof it belongs to another league, and is
	// refused. A payload carrying NO identity merely predates this protection -
	// every checkpoint published before it existed looks like that - and
	// refusing those bricked v2 outright: a joining device restores the room's
	// checkpoint, was refused, retried on the health tick, and parsed the whole
	// league again every few seconds until the phone ran out of memory.
	// Wrong-room protection for identity-less payloads is the room binding at
	// connect, which is the check that has the evidence to make that call.
	const localLeagueId = await readLocalLeagueId();
	const remoteLeagueId = payloadLeagueId(payload.stores);
	if (
		localLeagueId !== undefined &&
		remoteLeagueId !== undefined &&
		remoteLeagueId !== localLeagueId
	) {
		syncDebugLog("snapshot:league-identity-refused", {
			local: localLeagueId,
			remote: remoteLeagueId,
		});
		throw new Error(
			"Refusing to restore the room's snapshot: it belongs to a different league. Nothing on this device was changed.",
		);
	}
	if (localLeagueId !== undefined && remoteLeagueId === undefined) {
		syncDebugLog("snapshot:league-identity-absent", { local: localLeagueId });
	}

	// This device's identity must survive the restore: which team THIS user
	// controls is per-device state that happens to live in gameAttributes.
	const preserved: unknown[] = [];
	const existingGa: any[] = await (idb.league as any).getAll("gameAttributes");
	for (const row of existingGa) {
		if (DEVICE_LOCAL_GAME_ATTRIBUTES.has(String(row.key))) {
			preserved.push(row);
		}
	}

	// Silence the cache. It holds the PRE-restore league and a set of dirty rows
	// it intends to write back; a flush landing mid-restore would repopulate a
	// store we just emptied with rows from the database we are replacing. This
	// is the no-throw way to stop it - flush() returns early when autoSave is
	// off, where an invalid-status guard would surface as an error toast.
	const previousAutoSave = local.autoSave;
	local.autoSave = false;
	try {
		for (const store of snapshotStores()) {
			const rows = payload.stores[store];
			if (!Array.isArray(rows)) {
				// A store this app knows and the snapshot doesn't (or vice versa) is a
				// version skew smell, but an absent store just means "leave mine alone"
				// - which is strictly safer than clearing it on no evidence.
				continue;
			}

			const isGameAttributes = store === "gameAttributes";

			// A snapshot published from a device on an older build carries its
			// awards in the pre-upgrade shape, and nothing between it and the store
			// would convert them. See normalizeAwardsRow.
			const isAwards = store === "awards";

			const transaction = (idb.league as any).transaction(store, "readwrite");
			const objectStore = transaction.objectStore(store);

			// Issued synchronously so the transaction stays active across the whole
			// store, exactly as Cache.flush does. Clear and refill commit together
			// or not at all.
			objectStore.clear();
			for (const row of rows) {
				if (
					isGameAttributes &&
					DEVICE_LOCAL_GAME_ATTRIBUTES.has(String((row as any)?.key))
				) {
					continue;
				}
				objectStore.put(isAwards ? normalizeAwardsRow(row) : row);
			}
			if (isGameAttributes) {
				for (const row of preserved) {
					objectStore.put(row);
				}
			}

			await transaction.done;
		}
	} finally {
		local.autoSave = previousAutoSave;
	}

	// Make the running app see it: g from the restored gameAttributes, the cache
	// discarded and rebuilt from the restored DB. Discarding first matters -
	// otherwise the dirty rows the cache accumulated before the restore would be
	// flushed back over it at the next opportunity.
	await league.loadGameAttributes();
	idb.cache.discardForRestore();
	await idb.cache.fill();
};

// WHETHER ANY DEVICE BUILDS ROOM CHECKPOINTS AT ALL. Off.
//
// The premise of this whole sync layer is that every device already holds the
// same league file and only deltas travel between them - N+1, forever. A
// checkpoint exists for the one case that breaks: bootstrapping a device that
// does NOT have the league, or one so far behind that the log no longer reaches
// its position. Building one costs the entire league read into memory,
// stringified and gzipped, which is by far the most expensive thing this app
// does - and on a phone acting as sim authority it is not expensive, it is
// fatal: the OS kills the worker mid-build, the app reloads, the room still has
// no checkpoint, and it builds again. Crashing with nobody touching the device.
//
// Paying that, repeatedly, on every device, to serve a case that does not
// happen in a league where everyone started from the same file, is the wrong
// trade. So nothing builds them.
//
// WHAT THIS GIVES UP, plainly: a device that falls further behind than the
// log's retention window can no longer be repaired in place, and the log is no
// longer pruned (publishing checkpoint N is what deletes entries below N-1), so
// it grows. Restoring a checkpoint that ALREADY exists still works - Force
// Resync will use one if the room has one.
//
// v2 reads this flag for its OWN checkpoint publishing (see engine.ts); the v1
// publish and restore paths that used to live in this file were deleted with
// the rest of v1, so flipping it to true now only re-enables v2's.
export const AUTO_PUBLISH_CHECKPOINTS = false;
