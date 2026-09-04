import { idb } from "../../db/index.ts";
import { STORES } from "../../db/Cache.ts";
import { normalizeAwardsRow } from "../../db/normalizeAwardsRow.ts";
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
import { payloadLeagueId, readLocalLeagueId } from "./leagueIdentity.ts";
import {
	claimRecoveryAttempt,
	clearRecoveryAttempt,
} from "./recoveryBreadcrumb.ts";
import {
	checkLeagueIntegrity,
	findPayloadIntegrityProblems,
} from "./leagueIntegrity.ts";
import { syncDebugLog } from "./debugLog.ts";
import type { SyncTransport } from "./types.ts";
import type { RoomSnapshotMeta } from "./types.ts";

// The slice of an engine this module needs - structural, so both protocol
// engines qualify (only the v1 path ever calls in here, but the types must
// not force that).
type SnapshotEngine = {
	transport: SyncTransport;
	localName: string;
	isAuthority: () => boolean;
	isBusyApplying: () => boolean;
	getPersistedSeq: () => number;
	adoptSnapshotWatermark: (seq: number) => void;
};

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

// Publish the current full state as the room's snapshot, then prune the log
// entries the PREVIOUS snapshot already covers. Authority only - a follower's
// state is by definition secondhand.
export const publishRoomSnapshot = async (
	engine: SnapshotEngine,
): Promise<RoomSnapshotMeta | undefined> => {
	const transport = engine.transport;
	if (
		!engine.isAuthority() ||
		!transport.publishRoomSnapshot ||
		!transport.fetchRoomSnapshotMeta
	) {
		return undefined;
	}
	// Never snapshot the loaded league into a room it doesn't belong to (a
	// league switch mid-session, an engine that outlived one). This path reads
	// the entire current database - published into the wrong room, it is
	// instant cross-league contamination for everyone there.
	if (!checkApplyGuard()) {
		return undefined;
	}

	// BUILDING A SNAPSHOT IS THE HEAVIEST THING THIS APP DOES. It reads every
	// store of the league into memory, JSON-stringifies the whole object graph
	// and gzips the result - all three alive at once. On a phone with a deep
	// league that is hundreds of megabytes, and iOS does not throw something
	// catchable, it kills the worker.
	//
	// Only the sim authority publishes, so this used to be somebody else's
	// problem. Then a phone became the authority: it connected, found the room
	// had no snapshot yet, started building one, died, reloaded, and did it all
	// again - crashing with nobody touching it. And because the crash was always
	// the same device, the room never got its first snapshot, which is what made
	// the build fire on every single launch.
	//
	// One attempt. The note is on disk before the build and cleared after it
	// returns, so a device that cannot survive this stops volunteering, and the
	// room's snapshot waits for a device that can. Never publishing is a
	// degraded room; an unusable device is a broken one.
	const publishLid = g.get("lid");
	const publishOp = "snapshot-publish";
	if (!(await claimRecoveryAttempt(publishLid, publishOp))) {
		console.error(
			"[sync] Not building a room checkpoint on this device: the last attempt didn't finish (it likely ran out of memory). Another device in the room will publish one.",
		);
		return undefined;
	}
	try {
		return await publishRoomSnapshotInner(engine, transport);
	} finally {
		await clearRecoveryAttempt(publishLid, publishOp);
	}
};

const publishRoomSnapshotInner = async (
	engine: SnapshotEngine,
	transport: SyncTransport,
): Promise<RoomSnapshotMeta | undefined> => {
	if (!transport.publishRoomSnapshot || !transport.fetchRoomSnapshotMeta) {
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
	engine: SnapshotEngine,
	// True for the recovery paths that run on a timer. Those are rate-limited
	// per snapshot: the meta read below is one cheap document, but everything
	// after it is the whole league, and repeating that at tick speed is how a
	// phone runs out of memory. The manual Force Resync button leaves this off
	// and is never throttled.
	{ automatic = false }: { automatic?: boolean } = {},
): Promise<RoomSnapshotMeta | undefined> => {
	// IF WE DO NOT MAINTAIN THEM, WE MUST NOT RESTORE THEM. Nothing publishes
	// snapshots any more, so any that still exist in a room are frozen at
	// whatever date they were written - and restoring one does not "repair" a
	// device, it drags it back to that date. A snapshot from the start of the
	// season sends the whole league back to the start of the season, which is
	// exactly what one league-mate watched happen to his file.
	//
	// Publishing and restoring are one switch, deliberately: leaving restore on
	// while turning publishing off is strictly worse than either extreme.
	if (!AUTO_PUBLISH_CHECKPOINTS) {
		syncDebugLog("snapshot:restore-disabled", { automatic });
		return undefined;
	}

	const transport = engine.transport;
	if (!transport.fetchRoomSnapshotMeta || !transport.fetchRoomSnapshotData) {
		return undefined;
	}
	const meta = await transport.fetchRoomSnapshotMeta();
	if (!meta) {
		return undefined;
	}
	const snapshotKey = `${meta.seq}:${meta.generation ?? ""}`;
	// Everything below reads, decompresses and parses the WHOLE league, which is
	// the biggest allocation this app makes and the one a phone can die inside.
	// A death here takes the in-memory throttle above with it - the tab is gone -
	// so the only thing that can break a crash/reload loop is a note on disk,
	// written before the work and cleared after it. Force Resync brackets the
	// work too (so a crash still leaves a trace) but is never GATED by it: a
	// person choosing to retry is not a loop.
	const lid = g.get("lid");
	const op = `snapshot-restore:${snapshotKey}`;
	if (!(await claimRecoveryAttempt(lid, op, { gated: automatic }))) {
		console.error(
			"[sync] Not restoring the room's snapshot automatically: the last attempt on this device didn't finish (it likely ran out of memory). Use Force Resync on the Multiplayer Sync page to try it deliberately.",
		);
		return undefined;
	}
	try {
		return await restoreFromRoomSnapshotInner(engine, meta);
	} finally {
		// Reached on success, refusal AND throw - all of which mean the device
		// survived. Only a crash leaves the note behind.
		await clearRecoveryAttempt(lid, op);
	}
};

const restoreFromRoomSnapshotInner = async (
	engine: SnapshotEngine,
	meta: RoomSnapshotMeta,
): Promise<RoomSnapshotMeta | undefined> => {
	const transport = engine.transport;
	if (!transport.fetchRoomSnapshotData) {
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

// Whether this session has already judged the room's current checkpoint.
// Downloading and validating it costs a few MB, so it happens once per
// session, re-armed only when a new checkpoint appears.
let vettedSnapshotSeq: number | undefined;

// Test-only.
export const resetSnapshotCadenceForTesting = () => {
	lastSnapshotCheckAt = 0;
	vettedSnapshotSeq = undefined;
};

// Is the room's PUBLISHED checkpoint one that restorers would refuse? A
// checkpoint published by a damaged device before the publish gates existed
// sits in the room as a landmine: every device that falls far enough behind
// restores it and gets the damage. New builds refuse it - but refusing is not
// removing. When the HEALTHY authority finds a poisoned checkpoint, it must
// replace it immediately rather than waiting out the normal cadence, because
// until it does, any device on an older build that falls behind gets wiped,
// and any device on a new build has no usable checkpoint to recover from.
const roomSnapshotIsPoisoned = async (
	engine: SnapshotEngine,
	meta: RoomSnapshotMeta,
): Promise<boolean> => {
	if (vettedSnapshotSeq === meta.seq) {
		return false;
	}
	const transport = engine.transport;
	if (!transport.fetchRoomSnapshotData) {
		return false;
	}
	try {
		const serialized = await transport.fetchRoomSnapshotData(
			meta.chunkCount,
			meta.generation,
		);
		if (serialized === undefined) {
			// Missing chunks: unreadable by every restorer. Treat as poisoned.
			return true;
		}
		const payload = deserializeChangeset(
			await decompressSerialized(serialized),
		) as SnapshotPayload;
		const problems = validateRoomSnapshotPayload(payload);
		if (problems.length > 0) {
			syncDebugLog("snapshot:room-checkpoint-poisoned", {
				seq: meta.seq,
				problems,
			});
			return true;
		}
		// A checkpoint carrying a DIFFERENT league's identity is another
		// league's state sitting in this room - every restorer refuses it, so
		// it blocks fresh joins until replaced. Replace it now, same urgency as
		// structural poison. A checkpoint carrying NO identity is merely older
		// than the protection; restorers accept those, so it is not poison and
		// forcing a full rebuild over it would be an expensive no-op. The next
		// scheduled checkpoint adds the identity on its own.
		const localLeagueId = await readLocalLeagueId();
		const checkpointLeagueId = payloadLeagueId(payload.stores);
		if (
			localLeagueId !== undefined &&
			checkpointLeagueId !== undefined &&
			checkpointLeagueId !== localLeagueId
		) {
			syncDebugLog("snapshot:room-checkpoint-wrong-league", {
				seq: meta.seq,
			});
			return true;
		}
		vettedSnapshotSeq = meta.seq;
		return false;
	} catch (error) {
		// Unreadable (torn old-format write, corrupt payload): also poison.
		syncDebugLog("snapshot:room-checkpoint-unreadable", { error });
		return true;
	}
};

// Called from the health tick on every device; does anything only on the
// authority, at most once per SNAPSHOT_CHECK_MIN_MS, and only while nothing
// else is moving the league.
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
// Resync will use one if the room has one. Flip this to true to get the
// behaviour back.
export const AUTO_PUBLISH_CHECKPOINTS = false;

export const maybePublishRoomSnapshot = async (
	engine: SnapshotEngine,
	// Tests drive the cadence logic directly; production never passes this.
	{ enabled = AUTO_PUBLISH_CHECKPOINTS }: { enabled?: boolean } = {},
): Promise<void> => {
	if (!enabled) {
		return;
	}
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

		// A poisoned checkpoint is replaced NOW, not on cadence: it wipes any
		// old-build device that falls behind, and leaves new-build devices with
		// no usable checkpoint. The publish below still passes the history and
		// integrity gates, so a damaged authority cannot "replace" poison with
		// more poison - it simply cannot publish at all.
		const mustEvictPoison =
			previous !== undefined &&
			(await roomSnapshotIsPoisoned(engine, previous));

		// A room with NO checkpoint yet gets its first one promptly rather than
		// after 1200 entries. The checkpoint is now the ONLY automatic recovery -
		// the replay-over-live-state fallbacks are gone - so a room without one
		// has no self-heal at all until this runs.
		const mustPublishFirst = previous === undefined;

		if (!mustEvictPoison && !mustPublishFirst) {
			const entriesSince = await engine.transport.countEntriesSince(
				previous.seq,
			);
			if (entriesSince < SNAPSHOT_EVERY_ENTRIES) {
				return;
			}
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

		// Same bar for the present as for the past: a device whose own league
		// fails the catastrophe check must never become what everyone else
		// restores from. Restorers check this too, but the publish gate is what
		// keeps a poisoned checkpoint from ever existing - and from pruning the
		// log entries that still hold the good data.
		const integrityProblems = await checkLeagueIntegrity();
		if (integrityProblems.length > 0) {
			syncDebugLog("snapshot:publish-blocked-bad-league", {
				problems: integrityProblems,
			});
			return;
		}

		const published = await publishRoomSnapshot(engine);
		if (mustEvictPoison && published) {
			vettedSnapshotSeq = published.seq;
			console.log(
				"[sync] Replaced the room's damaged checkpoint with a fresh one from this device.",
			);
		}
	} catch (error) {
		syncDebugLog("snapshot:publish-failed", { error });
	}
};
