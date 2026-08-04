import { idb } from "../../db/index.ts";
import { STORES } from "../../db/Cache.ts";
import { g, lock, local, toUI } from "../../util/index.ts";
import { league } from "../index.ts";
import {
	DEVICE_LOCAL_GAME_ATTRIBUTES,
	DEVICE_LOCAL_STORES,
} from "./changeset.ts";
import {
	compressSerialized,
	decompressSerialized,
	deserializeChangeset,
	serializeChangeset,
} from "./serialize.ts";
import { getLeaguePosition } from "./leaguePosition.ts";
import { repairLeagueHistory } from "./historyRepair.ts";
import { checkApplyGuard } from "./applyGuard.ts";
import { syncDebugLog } from "./debugLog.ts";
import type { SyncEngine } from "./SyncEngine.ts";
import type { RoomSnapshotMeta } from "./types.ts";

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

// Publish a fresh snapshot once this many entries have accumulated since the
// last one. At a typical ~10-15 entries per simmed day this is roughly a
// season's worth - matching the intuition that "the log doesn't need to reach
// back seasons".
export const SNAPSHOT_EVERY_ENTRIES = 1200;

// How often the authority even looks (cheap server count, but no reason to ask
// more than once in a while).
const SNAPSHOT_CHECK_MIN_MS = 5 * 60 * 1000;

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
				objectStore.put(row);
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

// Publish the current full state as the room's snapshot, then prune the log
// entries the PREVIOUS snapshot already covers. Authority only - a follower's
// state is by definition secondhand.
export const publishRoomSnapshot = async (
	engine: SyncEngine,
): Promise<RoomSnapshotMeta | undefined> => {
	const transport = engine.transport;
	if (
		!engine.isAuthority() ||
		!transport.publishRoomSnapshot ||
		!transport.fetchRoomSnapshotMeta
	) {
		return undefined;
	}

	// The watermark BEFORE building: everything at or below it is in the DB the
	// payload reads. Entries landing during the build are above it and stay in
	// the tail, so nothing can fall between snapshot and log.
	const seq = engine.getPersistedSeq();
	const previous = await transport.fetchRoomSnapshotMeta();

	const payload = await buildRoomSnapshotPayload();
	// gzip, ~10x. A full league as raw JSON is tens of megabytes, which meant
	// dozens of sequential 700 KB document writes and a publish window minutes
	// long. Shrinking it shrinks that window by the same factor, and the format
	// is self-describing so a room with older clients still reads it.
	const serialized = await compressSerialized(serializeChangeset(payload));

	const meta: Omit<RoomSnapshotMeta, "chunkCount"> = {
		seq,
		at: Date.now(),
		byName: engine.localName,
		position: await getLeaguePosition(),
	};
	const chunkCount = await transport.publishRoomSnapshot(meta, serialized);
	syncDebugLog("snapshot:published", {
		seq,
		chunkCount,
		bytes: serialized.length,
	});

	// Prune what the PREVIOUS snapshot already covers. Never what this one
	// covers: the log must keep spanning a full interval so ordinary catch-up
	// stays delta-only for everyone who is not catastrophically behind.
	if (previous && transport.deleteEntriesBefore) {
		try {
			const deleted = await transport.deleteEntriesBefore(previous.seq);
			syncDebugLog("snapshot:pruned", { before: previous.seq, deleted });
		} catch (error) {
			// Pruning is housekeeping; a failed pass just leaves extra history for
			// the next one.
			syncDebugLog("snapshot:prune-failed", { error });
		}
	}

	return { ...meta, chunkCount };
};

// Restore the room's snapshot onto this device and jump the watermark to its
// seq. The caller runs an ordinary catch-up afterwards for the tail.
export const restoreFromRoomSnapshot = async (
	engine: SyncEngine,
): Promise<RoomSnapshotMeta | undefined> => {
	const transport = engine.transport;
	if (!transport.fetchRoomSnapshotMeta || !transport.fetchRoomSnapshotData) {
		return undefined;
	}
	const meta = await transport.fetchRoomSnapshotMeta();
	if (!meta) {
		return undefined;
	}
	const serialized = await transport.fetchRoomSnapshotData(
		meta.chunkCount,
		meta.generation,
	);
	if (serialized === undefined) {
		syncDebugLog("snapshot:restore-incomplete-payload", { meta });
		return undefined;
	}

	// A payload that will not parse is a torn or truncated download, not a
	// reason to touch the league. Bail with the local database untouched and
	// let the caller fall back.
	let payload: SnapshotPayload;
	try {
		payload = deserializeChangeset(
			await decompressSerialized(serialized),
		) as SnapshotPayload;
	} catch (error) {
		syncDebugLog("snapshot:restore-unreadable-payload", { meta, error });
		return undefined;
	}

	const problems = validateRoomSnapshotPayload(payload);
	if (problems.length > 0) {
		syncDebugLog("snapshot:restore-rejected", { meta, problems });
		console.error(
			`[sync] Ignored the room's snapshot because it is not usable (${problems.join("; ")}). This device was left alone.`,
		);
		return undefined;
	}

	syncDebugLog("snapshot:restore-start", {
		seq: meta.seq,
		chunkCount: meta.chunkCount,
	});
	await applyRoomSnapshotPayload(payload);
	engine.adoptSnapshotWatermark(meta.seq);

	// The snapshot was another device's database, holes and all. Reconcile the
	// derived history fields against the bracket right away, so a smudge in the
	// publisher's past doesn't become this device's ??? champion.
	try {
		await repairLeagueHistory("snapshot-restore");
	} catch (error) {
		syncDebugLog("snapshot:restore-repair-failed", { error });
	}

	// Bank the watermark durably right away, in BOTH directions. Forward: a
	// crash before the next periodic bank would re-restore from scratch.
	// Backward (a repair restore rewound the watermark): a stale higher durable
	// watermark would make a reload resume PAST the tail this restore now needs
	// re-applied, skipping it forever.
	try {
		const lid = g.get("lid");
		if (typeof lid === "number") {
			const leagueMeta = await idb.meta.get("leagues", lid);
			if (leagueMeta && leagueMeta.syncWatermark !== meta.seq) {
				leagueMeta.syncWatermark = meta.seq;
				await idb.meta.put("leagues", leagueMeta);
			}
		}
	} catch {
		// Best effort; the periodic bank will get it.
	}

	// Everything on screen predates the restore. One broad refresh, same shape
	// a season rollover uses.
	try {
		await toUI("realtimeUpdate", [
			["gameAttributes", "gameSim", "newPhase", "playerMovement"],
		]);
	} catch {
		// Cosmetic; the next navigation shows the restored state regardless.
	}

	syncDebugLog("snapshot:restore-done", { seq: meta.seq });
	return meta;
};

let lastSnapshotCheckAt = 0;

// Test-only.
export const resetSnapshotCadenceForTesting = () => {
	lastSnapshotCheckAt = 0;
};

// Called from the health tick on every device; does anything only on the
// authority, at most once per SNAPSHOT_CHECK_MIN_MS, and only while nothing
// else is moving the league.
export const maybePublishRoomSnapshot = async (
	engine: SyncEngine,
): Promise<void> => {
	if (
		!engine.isAuthority() ||
		!engine.transport.publishRoomSnapshot ||
		!engine.transport.countEntriesSince ||
		engine.isBusyApplying() ||
		lock.get("gameSim") ||
		lock.get("newPhase") ||
		local.autoPlayUntil
	) {
		return;
	}
	const now = Date.now();
	if (now - lastSnapshotCheckAt < SNAPSHOT_CHECK_MIN_MS) {
		return;
	}
	lastSnapshotCheckAt = now;

	try {
		const previous = await engine.transport.fetchRoomSnapshotMeta?.();
		const entriesSince = await engine.transport.countEntriesSince(
			previous?.seq ?? 0,
		);
		if (entriesSince < SNAPSHOT_EVERY_ENTRIES) {
			return;
		}

		// A snapshot makes THIS device's database the room's source of truth, so
		// its history has to actually be true. Repair what is derivable first;
		// anything still wrong after that (a torn bracket, a missing champion
		// row) means this device needs healing itself and must not be the one
		// everyone else restores from.
		const { problems } = await repairLeagueHistory("pre-snapshot-publish");
		if (problems.length > 0) {
			syncDebugLog("snapshot:publish-blocked-bad-history", { problems });
			return;
		}

		await publishRoomSnapshot(engine);
	} catch (error) {
		syncDebugLog("snapshot:publish-failed", { error });
	}
};
