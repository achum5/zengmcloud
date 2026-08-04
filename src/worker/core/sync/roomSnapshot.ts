import { idb } from "../../db/index.ts";
import { STORES } from "../../db/Cache.ts";
import { g, lock, local, toUI } from "../../util/index.ts";
import { league } from "../index.ts";
import {
	DEVICE_LOCAL_GAME_ATTRIBUTES,
	DEVICE_LOCAL_STORES,
} from "./changeset.ts";
import { serializeChangeset, deserializeChangeset } from "./serialize.ts";
import { getLeaguePosition } from "./leaguePosition.ts";
import { repairLeagueHistory } from "./historyRepair.ts";
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

// Restore a snapshot payload into the league DB: clear each shared store, put
// the snapshot's rows, reload game attributes and rebuild the cache. The clear
// is what makes restore complete - rows deleted since this device's state
// (negotiations, schedule days) vanish with the store instead of lingering.
export const applyRoomSnapshotPayload = async (
	payload: SnapshotPayload,
): Promise<void> => {
	if (payload.version !== SNAPSHOT_VERSION) {
		throw new Error(
			`Room snapshot has format version ${payload.version}, but this app understands version ${SNAPSHOT_VERSION}. Update the app and try again.`,
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

	for (const store of snapshotStores()) {
		const rows = payload.stores[store];
		if (!Array.isArray(rows)) {
			// A store this app knows and the snapshot doesn't (or vice versa) is a
			// version skew smell, but an absent store just means "leave mine alone"
			// - which is strictly safer than clearing it on no evidence.
			continue;
		}
		await (idb.league as any).clear(store);
		for (const row of rows) {
			if (
				store === "gameAttributes" &&
				DEVICE_LOCAL_GAME_ATTRIBUTES.has(String((row as any)?.key))
			) {
				continue;
			}
			await (idb.league as any).put(store, row);
		}
	}
	for (const row of preserved) {
		await (idb.league as any).put("gameAttributes", row);
	}

	// Make the running app see it: g from the restored gameAttributes, the cache
	// rebuilt from the restored DB.
	await league.loadGameAttributes();
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
	const serialized = serializeChangeset(payload);

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
	const serialized = await transport.fetchRoomSnapshotData(meta.chunkCount);
	if (serialized === undefined) {
		syncDebugLog("snapshot:restore-incomplete-payload", { meta });
		return undefined;
	}
	const payload = deserializeChangeset(serialized) as SnapshotPayload;

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

	// Bank the watermark durably right away: everything was just flushed as part
	// of the restore, so there is no cache/disk gap to worry about, and without
	// this a crash before the next periodic bank would re-restore from scratch.
	try {
		const lid = g.get("lid");
		if (typeof lid === "number") {
			const leagueMeta = await idb.meta.get("leagues", lid);
			if (leagueMeta && (leagueMeta.syncWatermark ?? 0) < meta.seq) {
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

// Is this device so far behind that deltas alone cannot get it there? True
// when the room's snapshot starts AHEAD of our watermark: the entries between
// us and the snapshot may already be pruned, and even when they aren't, the
// snapshot is the cheaper road.
export const shouldRestoreFromSnapshot = async (
	engine: SyncEngine,
): Promise<boolean> => {
	const transport = engine.transport;
	if (!transport.fetchRoomSnapshotMeta) {
		return false;
	}
	const meta = await transport.fetchRoomSnapshotMeta();
	return meta !== undefined && meta.seq > engine.getPersistedSeq();
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
