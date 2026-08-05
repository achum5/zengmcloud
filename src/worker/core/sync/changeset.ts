import { idb } from "../../db/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import type { Store } from "../../db/Cache.ts";
import loadGameAttributes from "../league/loadGameAttributes.ts";
import {
	g,
	helpers,
	local,
	toUI,
	updatePhase,
	updatePlayMenu,
	updateStatus,
} from "../../util/index.ts";
import { getGlobalSettings } from "../../util/getGlobalSettings.ts";
import { checkApplyGuard } from "./applyGuard.ts";
import { syncDebugLog } from "./debugLog.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { PHASE } from "../../../common/constants.ts";
import { initUILocalGames } from "../../util/initUILocalGames.ts";
import type { Phase, UpdateEvents } from "../../../common/types.ts";

// The landing page each phase redirects to, mirroring the redirect returned by
// each core/phase/newPhase* function (the ONLY phases those functions redirect
// for - the others intentionally have no redirect). A receiving device that
// applies a synced phase change navigates here, so followers "flip" to the new
// phase's page exactly like the simmer.
//   REGULAR_SEASON  -> newPhaseRegularSeason  -> season_preview
//   PLAYOFFS        -> newPhasePlayoffs       -> playoffs
//   DRAFT_LOTTERY   -> newPhaseBeforeDraft    -> history (awards)
//   DRAFT           -> newPhaseDraft          -> draft
//   RESIGN_PLAYERS  -> newPhaseResignPlayers  -> negotiation
//   FREE_AGENCY     -> newPhaseFreeAgency     -> free_agents
const PHASE_REDIRECT_URL: Partial<Record<Phase, string[]>> = {
	[PHASE.REGULAR_SEASON]: ["season_preview"],
	[PHASE.PLAYOFFS]: ["playoffs"],
	[PHASE.DRAFT_LOTTERY]: ["history"],
	[PHASE.DRAFT]: ["draft"],
	[PHASE.RESIGN_PLAYERS]: ["negotiation"],
	[PHASE.FREE_AGENCY]: ["free_agents"],
};

// The league-URL components a receiving device should navigate to for a synced
// change INTO `phase`, or undefined to stay put. Mirrors finalize(): redirect
// only when the phase has a landing page AND this device's phaseChangeRedirects
// setting opts into it (so a user who turned redirects off is never yanked
// around, whether they're simming or following).
export const phaseRedirectComponents = (
	phase: Phase,
	phaseChangeRedirects: Phase[],
): string[] | undefined => {
	const components = PHASE_REDIRECT_URL[phase];
	if (!components || !phaseChangeRedirects.includes(phase)) {
		return undefined;
	}
	return components;
};

// A single changed cache record. Records are whole objects (a `put` replaces
// the entire record), which makes applying a changeset idempotent and safe to
// reorder across different records - the basis for last-write-wins sync.
export type SyncChange =
	| { store: Store; id: number | string; type: "put"; value: any }
	// `value` is a snapshot of the deleted row, carried ONLY for logically-keyed
	// stores (teamSeasons/teamStats) so the receiver can delete the row matching
	// the logical identity (tid, season[, playoffs]) instead of blindly deleting
	// by `id` (the autoincrement `rid`, which points at a DIFFERENT row on a
	// device whose rids diverged). Absent for every other store's delete.
	| { store: Store; id: number | string; type: "delete"; value?: any };

// A batch of changes, ready to be JSON-serialized and shipped to the cloud.
export type Changeset = {
	changes: SyncChange[];
};

// Broad set of UI refresh signals - applying a remote changeset can touch
// almost anything, and the views only re-render for events they care about.
// "notes" is included because a synced change can be a game/player/team note
// edit; without it, a note that just synced in never repaints the box score,
// player page, etc. on the receiving device.
const APPLY_UPDATE_EVENTS: UpdateEvents = [
	"playerMovement",
	"gameAttributes",
	"team",
	"teamFinances",
	"playoffs",
	"gameSim",
	"notes",
];

const storeAPI = (store: Store) => (idb.cache as any)[store];

// Some stores' TRUE identity is a logical key, not their autoincrement primary
// key (`rid`). teamSeasons/teamStats rows are "the 2075 row for team 4" - but
// each device assigns its own `rid` to that row. When two devices independently
// create the same logical row (e.g. a season rollover that ran on more than one
// device, or a device that diverged and re-created it), syncing both by `rid`
// leaves the receiver holding TWO rows for one (tid, season). teamSeasons has a
// UNIQUE index on (tid, season) in IndexedDB, so the next flush throws "Index
// key is not unique" and ABORTS - taking every other pending write down with it,
// exactly the failure a Force resync hit.
//
// So before applying a put to one of these stores, we drop any existing row that
// shares the incoming row's logical identity but has a different `rid`. The
// logical row is then updated in place, converging on the author's `rid`, and no
// duplicate ever reaches the unique index. Replaying the whole log is
// deterministic (last write for a given identity wins), so all devices land on
// the same `rid`.
type IdentityRule = {
	// The primary key FIELD NAME, so apply can rewrite or strip it on a copy.
	pkField: string;
	// Resolve the LOCAL row that shares the incoming row's logical identity.
	// teamSeasons/teamStats use a unique IndexedDB index; draftPicks has no such
	// index, so it scans just its season-scoped slice.
	find: (api: any, value: any) => Promise<any | undefined>;
	// Disk-side identity lookup, for stores whose cache is season-scoped
	// (teamSeasons: past 3 seasons; teamStats: current season). A catch-up replay
	// can carry rows OLDER than the cache window; without checking disk, those
	// rows would look brand-new and could land next to (or on top of) their real
	// on-disk selves. Omit for fully-cached stores (draftPicks, releasedPlayers).
	findDisk?: (value: any) => Promise<any | undefined>;
	// The row's primary key, to delete/forget by (rid, or dpid for draftPicks).
	pk: (row: any) => number;
	sameIdentity: (a: any, b: any) => boolean;
};
const RECONCILE_BY_IDENTITY: Partial<Record<Store, IdentityRule>> = {
	teamSeasons: {
		pkField: "rid",
		find: (api, v) => api.indexGet("teamSeasonsByTidSeason", [v.tid, v.season]),
		findDisk: (v) =>
			(idb.league as any).getFromIndex("teamSeasons", "tid, season", [
				v.tid,
				v.season,
			]),
		pk: (row) => row.rid,
		sameIdentity: (a, b) => a.tid === b.tid && a.season === b.season,
	},
	teamStats: {
		pkField: "rid",
		find: (api, v) =>
			api.indexGet("teamStatsByPlayoffsTid", [v.playoffs, v.tid]),
		findDisk: async (v) => {
			const rows = await (idb.league as any).getAllFromIndex(
				"teamStats",
				"season, tid",
				[v.season, v.tid],
			);
			return rows.find((row: any) => row.playoffs === v.playoffs);
		},
		pk: (row) => row.rid,
		sameIdentity: (a, b) =>
			a.tid === b.tid && a.season === b.season && a.playoffs === b.playoffs,
	},
	// Draft picks carry an autoincrement `dpid` that DIVERGES across devices too
	// (a re-import re-numbers them). The lottery writes the current season's draft
	// ORDER onto each pick and syncs it by the author's `dpid`; on a re-imported
	// device that `dpid` addresses a DIFFERENT pick - often a FUTURE season's,
	// since those exist as placeholders - so the order lands on next year's pick,
	// making it look like the future lottery ran (and the current one is wrong).
	// Pick deletes as players are drafted hit the wrong pick the same way. The
	// pick's true identity is (season, round, originalTid); with no unique index
	// for it, scan just that season's slice via the cache's season index.
	draftPicks: {
		pkField: "dpid",
		find: async (api, v) => {
			const all = await api.indexGetAll("draftPicksBySeason", v.season);
			return all.find(
				(row: any) =>
					row.round === v.round && row.originalTid === v.originalTid,
			);
		},
		pk: (row) => row.dpid,
		sameIdentity: (a, b) =>
			a.season === b.season &&
			a.round === b.round &&
			a.originalTid === b.originalTid,
	},
	// Released-contract rows are immutable once created and their autoincrement
	// `rid` is renumbered by a league import (rows get deleted as contracts
	// expire, so the sequence has gaps and the renumbering SHIFTS every key). A
	// synced put/delete addressed by the author's `rid` would hit a different
	// released contract here, silently corrupting the receiver's salary-cap math.
	// The row's true identity is its content: which player, released by which
	// team, under what contract. (Two identical releases of the same player by
	// the same team with the same terms would be indistinguishable - matching
	// either row is harmless precisely because they're identical.)
	releasedPlayers: {
		pkField: "rid",
		find: async (api, v) => {
			const all = await api.getAll();
			return all.find(
				(row: any) =>
					row.pid === v.pid &&
					row.tid === v.tid &&
					row.contract?.amount === v.contract?.amount &&
					row.contract?.exp === v.contract?.exp,
			);
		},
		pk: (row) => row.rid,
		sameIdentity: (a, b) =>
			a.pid === b.pid &&
			a.tid === b.tid &&
			a.contract?.amount === b.contract?.amount &&
			a.contract?.exp === b.contract?.exp,
	},
};

// Stores whose rows have NO stable logical key but are immutable in practice at
// delete time, and whose autoincrement pk diverges across devices (renumbered by
// import). Deletes for these carry a snapshot of the deleted row; the receiver
// deletes the row whose CONTENT matches the snapshot, falling back to the raw id
// only when nothing matches. scheduledEvents: processed events are deleted as
// the sim advances - a delete by the author's `id` on a device whose ids shifted
// would remove a DIFFERENT pending event (and leave a phantom one to fire).
const DELETE_BY_CONTENT = new Set<Store>(["scheduledEvents"]);

// Order-insensitive deep equality for plain JSON values (rows have been through
// JSON serialization, so only objects/arrays/primitives appear).
const deepEqual = (a: any, b: any): boolean => {
	if (a === b) {
		return true;
	}
	if (typeof a !== "object" || typeof b !== "object" || !a || !b) {
		return false;
	}
	if (Array.isArray(a) !== Array.isArray(b)) {
		return false;
	}
	const aKeys = Object.keys(a);
	const bKeys = Object.keys(b);
	if (aKeys.length !== bKeys.length) {
		return false;
	}
	return aKeys.every(
		(key) => Object.hasOwn(b, key) && deepEqual(a[key], b[key]),
	);
};

const rowMatchesSnapshot = (row: any, snapshot: any, pkField: string) => {
	const { [pkField]: _a, ...rowRest } = row;
	const { [pkField]: _b, ...snapshotRest } = snapshot;
	return deepEqual(rowRest, snapshotRest);
};

// Find the local row sharing the incoming row's logical identity, checking the
// cache first and falling back to disk for cache-scoped stores. Returns
// undefined when no local row has that identity.
const findByIdentity = async (rule: IdentityRule, store: Store, value: any) => {
	const existing = await rule.find(storeAPI(store), value);
	if (existing !== undefined && rule.sameIdentity(existing, value)) {
		return existing;
	}
	if (rule.findDisk && idb.league) {
		try {
			const diskRow = await rule.findDisk(value);
			if (diskRow !== undefined && rule.sameIdentity(diskRow, value)) {
				return diskRow;
			}
		} catch {
			// Index missing / read failed - treat as not found; the occupant check
			// below still prevents any clobbering.
		}
	}
	return undefined;
};

// Whatever row currently sits at `pk` locally - cache first, then disk, because
// the cache is season-scoped for some stores and an old-season row that only
// lives on disk would otherwise look like a free slot (writing there is exactly
// the historical-stats wipe this file guards against).
const getOccupant = async (store: Store, pk: number) => {
	const occupant = await storeAPI(store).get(pk);
	if (occupant !== undefined) {
		return occupant;
	}
	if (idb.league) {
		try {
			return await (idb.league as any).get(store, pk);
		} catch {
			// Store not in the league DB / read failed - treat as unoccupied; for
			// fully-cached stores the cache lookup above was already authoritative.
		}
	}
	return undefined;
};

// Apply a put to a logically-keyed store WITHOUT ever overwriting an unrelated
// row. The author addresses the row by ITS autoincrement pk, but on a device
// whose keys diverged (a league import renumbers them) that pk can point at a
// different logical row - blindly putting there overwrote it (the observed
// "teams lost a whole season of history" wipe: the author's current-season rids
// landed on top of this device's prior-season rows). Rules, in order:
//   1. A local row with the SAME identity at the author's pk - plain in-place put.
//   2. Local identity row elsewhere + author's pk slot FREE - converge: move our
//      row to the author's pk (deterministic replay lands all devices on the
//      author's numbering, and a free slot makes the move safe).
//   3. Local identity row elsewhere + author's pk OCCUPIED by an unrelated row -
//      update our row in place under its LOCAL pk; never touch the occupant.
//   4. No local identity row + author's pk free - put under the author's pk.
//   5. No local identity row + author's pk occupied by an unrelated row - insert
//      under a fresh local pk (autoincrement); never touch the occupant.
// Returns the pk the row was actually written under.
const applyIdentityPut = async (
	store: Store,
	rule: IdentityRule,
	value: any,
): Promise<number | string> => {
	const api = storeAPI(store);
	const authorPk = rule.pk(value);
	const existing = await findByIdentity(rule, store, value);

	let writtenPk: number | string;
	if (existing !== undefined && rule.pk(existing) === authorPk) {
		writtenPk = await api.put(value);
	} else {
		const occupant = await getOccupant(store, authorPk);
		const occupiedByOtherRow =
			occupant !== undefined && !rule.sameIdentity(occupant, value);

		if (existing !== undefined) {
			if (occupiedByOtherRow) {
				writtenPk = await api.put({
					...value,
					[rule.pkField]: rule.pk(existing),
				});
			} else {
				await api.delete(rule.pk(existing));
				changeTracker.forget(store, rule.pk(existing));
				writtenPk = await api.put(value);
			}
		} else if (occupiedByOtherRow) {
			const fresh = { ...value };
			delete fresh[rule.pkField];
			writtenPk = await api.add(fresh);
		} else {
			writtenPk = await api.put(value);
		}
	}

	// Sweep: exactly ONE local row may hold this logical identity. The unique
	// cache index can only surface one row per identity, so if a duplicate ever
	// sneaks in (a lookup that transiently missed, a partially-applied history),
	// the identity find above can't see it - but the store scan here can. A
	// lingering duplicate is poison: it violates the on-disk unique index
	// (aborting flushes) or resurfaces under a fresh high pk where season-range
	// consumers pick it up as the "latest" row.
	try {
		const all = await api.getAll();
		for (const row of all) {
			if (rule.pk(row) !== writtenPk && rule.sameIdentity(row, value)) {
				await api.delete(rule.pk(row));
				changeTracker.forget(store, rule.pk(row));
			}
		}
	} catch (error) {
		console.error(`Failed duplicate sweep for ${store}`, error);
	}

	return writtenPk;
};

// Replace an incoming player record's watch flag with this device's own, so
// watch lists stay per-user instead of bleeding across the room (a Cavs
// device's shortlist should only ever show on that device). The local value is
// looked up in the cache first, then on disk (retired players and prospects
// can be watched but may not be cached). If this device never watched the
// player, the incoming flag is dropped entirely.
export const preserveLocalWatch = async (incoming: any) => {
	try {
		let localPlayer = await idb.cache.players.get(incoming.pid);
		if (localPlayer === undefined && idb.league) {
			localPlayer = await (idb.league as any).get("players", incoming.pid);
		}
		if (localPlayer?.watch !== undefined) {
			incoming.watch = localPlayer.watch;
		} else {
			delete incoming.watch;
		}
	} catch {
		// If the lookup fails, err on the side of NOT importing someone else's
		// watch flag.
		delete incoming.watch;
	}
};

// Which team THIS device is currently acting as. In the multiplayer model the
// league is in multi-team mode with all the friends' teams in `userTids` (which
// DOES sync - it's what makes re-signing, the draft, etc. treat every friend's
// team as human-controlled when the host sims). Only `userTid` - the one team
// you're currently viewing/managing - is per-device, so friends don't yank each
// other onto the same team.
export const DEVICE_LOCAL_GAME_ATTRIBUTES = new Set([
	"userTid",
	// The seed behind "show me a fresh set of AI trade proposals" on the Trade
	// Proposals page - per-device UI state (each device browses proposals for its
	// own team). Sharing it would reshuffle every league-mate's proposals.
	"tradeProposalsSeed",
	// The Team Finances checkbox plan: which players you have ticked to ask
	// "what do the books look like without him". A private what-if, not a fact
	// about the league - and worse than merely visible, it is ONE record holding
	// a map of every team, so whole-record last-write-wins meant a league-mate
	// ticking a box on their own team replaced YOUR plan for YOUR team too.
	"teamFinancesPlan",
	// The v2 sync protocol's applied-version marker (see sync/v2/). It states
	// how far THIS device's database has applied the room's version chain, so
	// it must never travel to another device, and a checkpoint restore must
	// not clobber it (the v2 restore path writes it explicitly, last).
	"syncV2AppliedVersion",
	// Which room the marker above belongs to. A version number is meaningful
	// only within ONE room's chain; without this, a copy of a league carried
	// its old room's marker into a NEW room and was believed - which let two
	// rooms sharing a league lineage cross-contaminate. On connect, a code
	// mismatch here resets the marker to 0 so the league joins the room
	// cleanly through its checkpoint.
	"syncV2Room",
]);

// Whole stores that are per-device scratch / personal UI state, never shared
// league data, so they must never be broadcast NOR applied from the log:
//   - trade:             the in-progress Trade page selection (staged pids/tid).
//                        Broadcasting it clobbers whatever trade a league-mate is
//                        assembling on their own device.
//   - savedTrades:       your personal saved/bookmarked trades.
//   - savedTradingBlock: your personal saved trading-block shopping list.
// Handled at the store level (not per-action) because even a legitimate action
// like proposeTrade touches these incidentally (it clears the staged trade and
// removes a saved trade) - so suppressing individual actions wouldn't be enough.
export const DEVICE_LOCAL_STORES = new Set<Store>([
	"trade",
	"savedTrades",
	"savedTradingBlock",
]);

export const isDeviceLocal = (store: Store, id: number | string) =>
	DEVICE_LOCAL_STORES.has(store) ||
	(store === "gameAttributes" && DEVICE_LOCAL_GAME_ATTRIBUTES.has(String(id)));

// Apply gameAttributes LAST, keeping relative order otherwise (records are
// whole-object last-write-wins, so reordering across different records is safe
// by design - see SyncChange). gameAttributes act as the changeset's COMMIT
// POINT: a season rollover writes `season` BEFORE it creates the new season's
// teamSeasons/players rows, so applying in capture order means an interrupted
// apply (a thrown record, a killed tab) can leave a receiver living in the new
// season with the new season's data missing - every mood/standings/roster
// computation then runs against a hole (this looked like "all my players'
// moods reset" in the field). Data first, then the attributes that make the
// app look at that data.
export const orderChangesForApply = (changes: SyncChange[]): SyncChange[] => {
	const data: SyncChange[] = [];
	const gameAttributes: SyncChange[] = [];
	for (const change of changes) {
		if (change.store === "gameAttributes") {
			gameAttributes.push(change);
		} else {
			data.push(change);
		}
	}
	return [...data, ...gameAttributes];
};

// Drain everything the tracker has recorded since the last capture and turn it
// into a self-contained changeset by reading the current value of each record.
// Whole-record values mean the receiver needs no prior state to apply this.
export const captureChangeset = async (): Promise<Changeset> => {
	const pending = changeTracker.drain();
	const changes: SyncChange[] = [];

	for (const { store, id, type, value: deletedRow } of pending) {
		const typedStore = store as Store;

		// Never broadcast per-device/personal state (which team we control, the
		// in-progress/saved trade stores, etc. - see isDeviceLocal).
		if (isDeviceLocal(typedStore, id)) {
			continue;
		}

		if (type === "delete") {
			// An `events` row is keyed by an autoincrement `eid` that diverges across
			// devices (each generates its own), and events carry no stable logical
			// identity to reconcile by. So a delete-by-eid would remove a DIFFERENT,
			// unrelated event on every other device. The only synced-league path that
			// deletes an event is the sign/release UNDO (deleteOldData is blocked on
			// synced leagues), and there the substantive revert is the player put -
			// which syncs correctly by pid. So keep event deletes LOCAL: the author's
			// log is cleaned, and receivers simply keep the now-stale log entry rather
			// than risk losing a real one. Event ADDs still broadcast normally.
			if (typedStore === "events") {
				continue;
			}

			// For a logically-keyed store, carry the deleted row's identity so the
			// receiver deletes by (tid, season[, playoffs]) rather than by our `rid`
			// - which points at an unrelated row on a device whose rids diverged,
			// silently wiping the wrong (often much older) season. Content-matched
			// stores (scheduledEvents) carry the snapshot for the same reason.
			if (
				(RECONCILE_BY_IDENTITY[typedStore] ||
					DELETE_BY_CONTENT.has(typedStore)) &&
				deletedRow !== undefined
			) {
				changes.push({
					store: typedStore,
					id,
					type: "delete",
					value: deletedRow,
				});
			} else {
				changes.push({ store: typedStore, id, type: "delete" });
			}
			continue;
		}

		const value = await storeAPI(typedStore).get(id);
		if (value === undefined) {
			// Not in the in-memory cache. That does NOT mean it was deleted: the
			// cache is season-scoped, so a rollover's re-fill EVICTS rows that are
			// alive and well on disk. Shipping a blind delete here erased real
			// records (e.g. teamSeasons) from every other device in the room. Only
			// treat it as a delete if it's missing from disk too.
			let diskValue: any;
			try {
				diskValue = await (idb.league as any).get(typedStore, id);
			} catch {
				// Store not in the league DB / read failed - fall through to delete,
				// same as the old behavior.
			}
			if (diskValue !== undefined) {
				changes.push({ store: typedStore, id, type: "put", value: diskValue });
			} else {
				// Put then deleted before we captured - genuinely gone.
				changes.push({ store: typedStore, id, type: "delete" });
			}
		} else {
			changes.push({ store: typedStore, id, type: "put", value });
		}
	}

	return { changes };
};

// Apply a changeset received from another device. Writes go THROUGH the cache
// API (not raw IndexedDB) so the in-memory cache - the live source of truth -
// stays coherent; writing IndexedDB directly would be clobbered by the next
// Does this changeset play games of the CURRENT season while an earlier day
// of that season sits locally unplayed? See the call site for why that must
// never apply. Exported for tests.
export const guardDayContiguity = async (
	changeset: Changeset,
): Promise<void> => {
	let season: number;
	try {
		season = g.get("season");
	} catch {
		// No league loaded (bare test harness) - nothing to judge against.
		return;
	}

	// Only judge changesets that PLAY games of the season this device is in.
	// Cross-season applies (a replay walking old history) are judged at each
	// step against the season the device is in at that step.
	let minDay: number | undefined;
	const incomingGids = new Set<number>();
	for (const change of changeset.changes) {
		if (change.store !== "games" || change.type !== "put") {
			continue;
		}
		const game = change.value as
			| { gid?: number; day?: number; season?: number }
			| undefined;
		if (
			game &&
			typeof game.gid === "number" &&
			typeof game.day === "number" &&
			game.season === season
		) {
			incomingGids.add(game.gid);
			if (minDay === undefined || game.day < minDay) {
				minDay = game.day;
			}
		}
	}
	if (minDay === undefined) {
		return;
	}

	const scheduleRows = await idb.cache.schedule.getAll();
	const missingDays = new Set<number>();
	for (const row of scheduleRows) {
		if (typeof row.day !== "number" || row.day >= minDay) {
			continue;
		}
		// Satisfied by this very changeset (a multi-day sim ships several days
		// at once).
		if (incomingGids.has(row.gid)) {
			continue;
		}
		// A leftover row for a game this device HAS is a phantom row - wrong,
		// but not missing data; the played-game sweep cleans those.
		if (await idb.cache.games.get(row.gid)) {
			continue;
		}
		missingDays.add(row.day);
	}
	if (missingDays.size === 0) {
		return;
	}

	const days = [...missingDays].sort((a, b) => a - b);
	getSyncEngine()?.markResyncNeeded();
	syncDebugLog("apply:refused-day-gap", {
		missingDays: days,
		incomingDay: minDay,
	});
	throw new Error(
		`Refusing to apply day ${minDay}: day ${days.join(", ")} of this season has not arrived on this device yet. Applying out of order would fork the league; it will self-repair from the room's checkpoint instead.`,
	);
};

// Everything a RECEIVING device must refresh after remote records landed, so
// it behaves like the device that ran the action: season-scoped cache refill,
// the in-memory `g` mirror, derived worker caches, phase text + Play menu +
// phase redirect, the status line, the score ticker, and the UI's
// realtimeUpdate. Shared by both sync engines - v1 calls it at the end of
// applyChangeset, v2 after each applied version and after a checkpoint
// restore. Every step is individually caught: one failing refresh must never
// skip the others (a throw in updatePhase once left headers frozen on the old
// phase).
export const refreshAfterApply = async ({
	touchedSeason,
	touchedGameAttributes,
	touchedGames,
	touchedPhase,
	touchedStatus,
	touchedStores,
	refreshUI,
	sweepGames,
	redirect,
}: {
	touchedSeason: boolean;
	touchedGameAttributes: boolean;
	touchedGames: boolean;
	touchedPhase: boolean;
	touchedStatus: boolean;
	touchedStores: Set<Store>;
	refreshUI: boolean;
	// v1 only: heal phantom/stranded schedule rows left by per-record applies.
	sweepGames: boolean;
	// Navigate to the new phase's landing page on a phase flip. Off for
	// checkpoint restores, where yanking the user to a phase page would be a
	// surprise rather than a continuation.
	redirect: boolean;
}): Promise<void> => {
	// A synced season rollover: persist what was just applied, then re-fill the
	// cache, exactly like finalize() does on the simmer for PRESEASON. The cache
	// scopes several stores to the current season (games, teamStats,
	// playoffSeries, allStars, headToHeads); without this a follower's cache
	// keeps last season's rows in "current season" queries forever.
	if (touchedSeason) {
		// If this fails, the cache keeps serving the OLD season's scoped rows while
		// g is about to say the new season - every view (moods, standings, rosters)
		// then computes against missing data until something else happens to
		// re-fill. Worth one immediate retry (the usual cause is a transient flush
		// collision); a second failure is loud because there is no silent recovery.
		try {
			await idb.cache.flush();
			await idb.cache.fill();
		} catch (error) {
			console.error("Failed to re-fill cache after synced rollover", error);
			try {
				await idb.cache.flush();
				await idb.cache.fill();
			} catch (error2) {
				console.error(
					"RETRY FAILED: cache is stale after a synced season rollover - the app may show wrong data until this league is closed and reopened",
					error2,
				);
			}
		}
	}

	// The cache store now holds the new gameAttributes rows, but the in-memory
	// `g` object (and the UI's copy) is stale until we reload it from the cache.
	if (touchedGameAttributes) {
		try {
			await loadGameAttributes();
		} catch (error) {
			console.error("Failed to reload gameAttributes after sync", error);
		}
	}

	// Invalidate worker-local derived caches exactly like the simmer does. The
	// simmer invalidates these inside its own sim/phase code, which a receiver
	// never runs - leaving stale means wrong leaders/mood/value math until this
	// device happens to sim.
	if (touchedGames || touchedStores.has("players")) {
		local.seasonLeaders = undefined;
		local.minFractionDiffs = undefined;
	}
	if (touchedSeason) {
		local.playerOvrMeanStdStale = true;
		local.seasonLeaders = undefined;
		local.minFractionDiffs = undefined;
	}

	// A phase change is more than a data change: the phase text, the Play menu
	// options, and phase-routed views all need refreshing - and the device should
	// navigate to the new phase's page. finalize() does all of this for the device
	// that ran the phase change; a receiving device must do the SAME, or it writes
	// the new phase to data but keeps showing the old phase's page (e.g. stuck on
	// Draft Lottery after the simmer advanced to the Draft).
	const updateEvents: UpdateEvents = [...APPLY_UPDATE_EVENTS];
	// Store-keyed refresh events the blanket set doesn't cover, so followers on
	// those views live-update like the author's device.
	if (touchedStores.has("draftLotteryResults")) {
		updateEvents.push("draftLottery");
	}
	if (touchedStores.has("teams")) {
		updateEvents.push("retiredJerseys");
	}
	if (touchedStores.has("allStars")) {
		updateEvents.push("allStarDunk", "allStarThree");
	}
	if (touchedStores.has("scheduledEvents")) {
		updateEvents.push("scheduledEvents");
	}
	let redirectUrl: string | undefined;
	if (touchedPhase) {
		updateEvents.push("newPhase");

		try {
			await updatePhase();
		} catch (error) {
			console.error("Failed to refresh phase text after sync", error);
		}
		try {
			await updatePlayMenu();
		} catch (error) {
			console.error("Failed to refresh play menu after sync", error);
		}

		// Reset the same worker-local flags the simmer's phase functions reset, so
		// a follower's later behavior (live-sim stop points, fantasy draft results
		// panel) doesn't run on state from a previous season's phases.
		try {
			const phase = g.get("phase") as Phase;
			if (phase === PHASE.PLAYOFFS) {
				local.playingUntilEndOfRound = false;
			}
			if (phase === PHASE.FANTASY_DRAFT || phase === PHASE.EXPANSION_DRAFT) {
				local.fantasyDraftResults = [];
			}
		} catch (error) {
			console.error("Failed to reset local phase flags after sync", error);
		}

		// Redirect to the new phase's landing page, honoring the user's
		// phaseChangeRedirects setting (same gate finalize uses). This is what makes
		// a follower actually "flip" to the new phase instead of sitting on the
		// previous phase's page.
		if (redirect) {
			try {
				const phase = g.get("phase") as Phase;
				const globalSettings = await getGlobalSettings();
				const components = phaseRedirectComponents(
					phase,
					globalSettings.phaseChangeRedirects,
				);
				if (components) {
					redirectUrl = helpers.leagueUrl(components);
				}
			} catch (error) {
				console.error("Failed to compute phase redirect after sync", error);
			}
		}
	}

	// Refresh the status line ("X days left in free agency", draft progress) on a
	// receiving device. A phase change also moves the status (e.g. into/out of
	// free agency), so recompute in either case. updateStatus() with no argument
	// derives the right text from the just-updated g.
	if (touchedStatus || touchedPhase) {
		try {
			await updateStatus();
		} catch (error) {
			console.error("Failed to refresh status after sync", error);
		}
	}

	// A game/schedule change just landed: enforce the played-game invariant (no
	// schedule row may outlive its game) so any phantom left by an earlier
	// partial apply heals as soon as the next sync touches games. v1 only - a v2
	// apply is atomic, so it cannot manufacture phantoms or strand days.
	if (touchedGames && sweepGames) {
		try {
			await sweepPhantomScheduleRows();
		} catch (error) {
			console.error("Phantom schedule sweep failed", error);
		}

		// And the invariant the phantom sweep can't see: an unplayed row on a day
		// the league has already played past means a whole day's changeset never
		// reached this device. Only flag it here - recovery re-reads the entire
		// shared log, which is far too heavy to run from inside an apply. The
		// marker is durable, so the next connect does it.
		try {
			const stranded = await findStrandedScheduleRows();
			if (stranded.gids.length > 0) {
				console.error(
					`[sync] Missing day ${stranded.days.join(", ")} of the current season (league is on day ${stranded.maxPlayedDay}). Will re-read the shared log on the next connect.`,
				);
				getSyncEngine()?.markResyncNeeded();
			}
		} catch (error) {
			console.error("Stranded schedule check failed", error);
		}
	}

	// The LeagueTopBar score ticker is fed by a separate UI-state channel, not by
	// the cache/realtimeUpdate path. When a synced sim adds games, rebuild it from
	// the (now-updated) cache so the ticker reflects the new results.
	if (touchedGames) {
		try {
			await initUILocalGames();
		} catch (error) {
			console.error("Failed to refresh LeagueTopBar after sync", error);
		}
	}

	if (refreshUI) {
		try {
			// Passing the URL makes the UI navigate (like finalize's redirect);
			// without it, realtimeUpdate just refreshes the current page in place.
			if (redirectUrl !== undefined) {
				await toUI("realtimeUpdate", [updateEvents, redirectUrl]);
			} else {
				await toUI("realtimeUpdate", [updateEvents]);
			}
		} catch (error) {
			console.error("Failed to push UI refresh after sync", error);
		}
	}
};

// A remote-apply summary computed from a changeset's records: which refreshes
// the receiving device owes. Same classification applyChangeset derives while
// it applies, for callers (v2) that apply records elsewhere.
export const summarizeChangesetForRefresh = (
	changeset: Changeset,
): {
	touchedSeason: boolean;
	touchedGameAttributes: boolean;
	touchedGames: boolean;
	touchedPhase: boolean;
	touchedStatus: boolean;
	touchedStores: Set<Store>;
} => {
	const summary = {
		touchedSeason: false,
		touchedGameAttributes: false,
		touchedGames: false,
		touchedPhase: false,
		touchedStatus: false,
		touchedStores: new Set<Store>(),
	};
	for (const change of changeset.changes) {
		summary.touchedStores.add(change.store);
		if (change.store === "gameAttributes") {
			summary.touchedGameAttributes = true;
			if (change.id === "phase" || change.id === "nextPhase") {
				summary.touchedPhase = true;
			}
			if (change.id === "season") {
				summary.touchedSeason = true;
			}
			if (change.id === "daysLeft") {
				summary.touchedStatus = true;
			}
		} else if (change.store === "games" || change.store === "schedule") {
			summary.touchedGames = true;
		}
	}
	return summary;
};

// cache flush. Recording is suppressed so we don't re-broadcast what we just
// received. If gameAttributes changed, the in-memory `g` mirror is refreshed.
export const applyChangeset = async (
	changeset: Changeset,
	{ refreshUI = true }: { refreshUI?: boolean } = {},
): Promise<void> => {
	if (changeset.changes.length === 0) {
		return;
	}

	// Refuse to write a remote changeset into a cache that doesn't belong to the
	// connected league (e.g. a session that outlived a league switch). Throwing
	// keeps the watermark pinned, so the entry re-applies once the right league
	// is loaded instead of being lost.
	if (!checkApplyGuard()) {
		throw new Error(
			"Refusing to apply a remote change: the loaded league is not the one this sync session belongs to.",
		);
	}

	// THE DAY-CONTIGUITY GUARD. A changeset that plays day D of the current
	// season must not land on a device that still has an EARLIER day unplayed:
	// applying it forks the league - records, stats and standings that include
	// day D but not day 11 - and the fork looks healthy on every position
	// check, which is how a device once spent an evening a day behind the room
	// with no idea. Missing data has exactly one honest response: stop, keep
	// the watermark pinned below this entry (the throw does that), flag the
	// durable repair marker, and let the checkpoint heal walk history forward
	// IN ORDER. Every engine bug that skips an entry - an abandoned batch, a
	// watermark banked too far, a dedup gone wrong - funnels through here,
	// because this is the one check made against the DATA rather than against
	// the bookkeeping that failed.
	await guardDayContiguity(changeset);

	let touchedGameAttributes = false;
	let touchedPhase = false;
	let touchedGames = false;
	// Per-record failures, collected instead of aborting the whole changeset at
	// the first one. Aborting mid-changeset left PARTIAL state applied: everything
	// before the bad record stuck (e.g. a day's game rows), everything after it
	// never ran (that day's schedule deletes) - and when the failure was
	// deterministic, every retry repeated the same split forever. That surfaced in
	// the field as a whole day's games showing BOTH played (box scores, records)
	// AND still unplayed in the schedule, on the one device where a record
	// happened to fail. Now every healthy record lands, and the aggregate throw at
	// the end still pins the watermark so the failed records retry.
	const failures: string[] = [];
	// Writes declined because they would have moved a monotonic field backwards.
	// Not per-record failures: the changeset is not retried for these (a retry
	// would decline them again). They mean this device applied something out of
	// order, so they trigger a full ordered re-read instead.
	const regressions: string[] = [];
	// The season increment (rollover to preseason) needs special handling: the
	// simmer re-fills its whole cache after it (finalize does idb.cache.fill()),
	// because several stores are season-scoped. A receiver must do the same or
	// its cache accumulates stale prior-season rows forever.
	let touchedSeason = false;
	// The free-agency countdown ("28 days left") and the draft-progress text are
	// driven by `local.statusText`, which only updates when someone calls
	// updateStatus(). The simming device does that itself; a device that merely
	// RECEIVES the daysLeft/phase change must refresh it too, or its status line
	// stays frozen on the old day even though g.daysLeft advanced.
	let touchedStatus = false;
	// Which stores this changeset touched, for view-refresh events that only some
	// stores need (draft lottery, retired jerseys, All-Star contests, scheduled
	// events) - so a follower sitting on those pages refreshes like the author.
	const touchedStores = new Set<Store>();

	// Apply each write, then IMMEDIATELY forget just that record from the change
	// tracker. We deliberately do NOT globally suppress recording during the
	// apply: a global flag would also swallow the writes of a local sim running
	// at the same time (e.g. right after this device takes over simming while
	// still catching up), leaving that sim unpublished. Forgetting only our own
	// applied records keeps a concurrent local action's other writes intact.
	// beginApply only tells the invisible-write CANARY these writes are expected
	// to be uncaptured - it does not suppress recording.
	// The season and phase this changeset belongs to, if it says. A phase is
	// only comparable within a season (see regressionReason), and a changeset
	// that crosses a season boundary writes both.
	let changesetSeason: number | undefined;
	let changesetPhase: number | undefined;
	for (const change of changeset.changes) {
		if (change.store === "gameAttributes" && change.type === "put") {
			const value = change.value as { key?: string; value?: unknown };
			if (value?.key === "season" && typeof value.value === "number") {
				changesetSeason = value.value;
			}
			if (value?.key === "phase" && typeof value.value === "number") {
				changesetPhase = value.value;
			}
		}
	}

	// A changeset that DECLARES where it belongs, and declares somewhere the
	// league has already been, is old history arriving late - and it must be
	// declined WHOLE, before anything applies. This is the live-path defence
	// against the log's ugliest failure: a bulk advance stuck in some device's
	// outbox for days gets re-published with fresh timestamps every time that
	// device opens the app, every other device's live listener reassembles it,
	// and the changeset-level guard used to decline only its two little phase/
	// season writes while the THOUSANDS of writes riding along (draft-era
	// rosters, schedule, finances) sprayed across the live league in front of
	// the user. Declining per-change protected the phase and nothing else; a
	// season rollover is one atomic story and it is either current or junk.
	//
	// Two tells, same as regressionReason: declaring a (season, phase) the
	// league has long moved past, or a season-less multi-phase jump forward (a
	// real advance is one changeset, one phase). ONE phase behind is different
	// - that is the straggler shape: a batch abandoned with chunks missing
	// whose author came back while the room advanced once, and its data is
	// real missed history, so it keeps the old treatment (data applies, the
	// phase write alone is declined per-change below). Negative phases
	// (fantasy/expansion drafts) are special interludes and exempt, and a
	// replay orders the whole log itself so it needs no guard here.
	//
	// The decline also flags a repair pass: if anything in the declined
	// changeset WAS real history this device is missing, the ordered replay
	// delivers it in its true position - the one place it can't do harm.
	if (
		!getSyncEngine()?.isResyncing() &&
		changesetPhase !== undefined &&
		changesetPhase >= 0 &&
		g.get("phase") >= 0
	) {
		const localSeason = g.get("season");
		const localPhase = g.get("phase");
		const incoming = phaseOrder(changesetSeason ?? localSeason, changesetPhase);
		const localOrder = phaseOrder(localSeason, localPhase);
		const farBehind =
			incoming !== undefined &&
			localOrder !== undefined &&
			incoming < localOrder - 1;
		const phantomJump =
			changesetSeason === undefined && changesetPhase > localPhase + 2;
		if (farBehind || phantomJump) {
			console.error(
				`[sync] Declined an entire changeset (${changeset.changes.length} writes) as displaced old history: it declares season ${changesetSeason ?? localSeason} phase ${changesetPhase}, but the league is at season ${localSeason} phase ${localPhase}.`,
			);
			try {
				getSyncEngine()?.markResyncNeeded();
			} catch {
				// Best effort.
			}
			return;
		}
	}

	changeTracker.beginApply();
	try {
		for (const change of orderChangesForApply(changeset.changes)) {
			// Never let a peer's per-device/personal state (their controlled team,
			// their in-progress/saved trades) overwrite ours - also protects catch-up
			// replays of older, unfiltered history that predates this exclusion.
			if (isDeviceLocal(change.store, change.id)) {
				continue;
			}

			// gameAttributes are ordered last as the changeset's COMMIT POINT (see
			// orderChangesForApply): a season/phase flip must never land while some
			// of its data records failed, or the app looks at the new season/phase
			// with holes in its data. Withhold them when anything failed - the
			// aggregate throw below pins the watermark, and the retry applies them
			// once the data goes through.
			if (change.store === "gameAttributes" && failures.length > 0) {
				continue;
			}

			// A write that would move a monotonic field backwards is stale, whatever
			// the ordering machinery thinks. Decline it rather than corrupt the
			// record, and ask for a full ordered re-read so the true state lands.
			// See regressionReason.
			//
			// NEVER during a replay. A resync walks the log oldest-first, so it
			// legitimately moves the phase backwards every time it crosses a season
			// boundary - and the guard would then decline the newer entries that
			// put it right, leaving the league stranded wherever the replay
			// happened to reach. That is not hypothetical: it dropped a device into
			// free agency in the middle of a regular season. The replay is ordered
			// and authoritative; whatever it ends on is the truth.
			let regression: string | undefined;
			if (!getSyncEngine()?.isResyncing()) {
				try {
					regression = await regressionReason(change, changesetSeason);
				} catch {
					// Never let the guard itself block an apply.
				}
			}
			if (regression !== undefined) {
				regressions.push(regression);
				continue;
			}

			const api = storeAPI(change.store);

			try {
				if (change.type === "delete") {
					// Delete the row matching the incoming logical identity, not our own
					// `rid`. For teamSeasons/teamStats the author's `rid` addresses a
					// DIFFERENT row here (rids diverge across devices), so a raw
					// delete-by-rid silently erased unrelated, much older seasons. When an
					// identity snapshot is present, resolve the local `rid` via the unique
					// index; if no row matches, it's already gone - a safe no-op.
					const rule = RECONCILE_BY_IDENTITY[change.store];
					if (rule) {
						if (change.value !== undefined) {
							const existing = await findByIdentity(
								rule,
								change.store,
								change.value,
							);
							if (existing !== undefined) {
								await api.delete(rule.pk(existing));
								changeTracker.forget(change.store, rule.pk(existing));
							}
						}
						// No identity snapshot (the deleted row wasn't in the author's cache
						// - rare): deleting by the raw autoincrement id could erase an
						// UNRELATED row on a device whose keys diverged. A lingering stale
						// row is recoverable; a wrong-row delete is not. Skip.
					} else if (
						DELETE_BY_CONTENT.has(change.store) &&
						change.value !== undefined
					) {
						// No stable logical key, but the snapshot's CONTENT identifies the
						// row: delete the local row that matches it, wherever its diverged
						// autoincrement id put it. Check the cache first, then disk (the
						// scheduledEvents cache only holds the current season's rows). Fall
						// back to the raw id only when nothing matches (pre-snapshot history).
						const pkField = (idb.cache as any).storeInfos[change.store].pk;
						let match = (await api.getAll()).find((row: any) =>
							rowMatchesSnapshot(row, change.value, pkField),
						);
						if (match === undefined && idb.league) {
							try {
								match = (await (idb.league as any).getAll(change.store)).find(
									(row: any) => rowMatchesSnapshot(row, change.value, pkField),
								);
							} catch {
								// Read failed - fall through to the raw-id fallback.
							}
						}
						if (match !== undefined) {
							await api.delete(match[pkField]);
							changeTracker.forget(change.store, match[pkField]);
						} else {
							await api.delete(change.id);
							changeTracker.forget(change.store, change.id);
						}
					} else {
						await api.delete(change.id);
						changeTracker.forget(change.store, change.id);
					}
				} else {
					// Watch-list flags are personal: each user shortlists players for
					// their own team, so a synced player record must never carry another
					// device's watch color onto this one. Keep whatever watch THIS device
					// has for the player (including none) before applying the record.
					if (change.store === "players" && change.value) {
						await preserveLocalWatch(change.value);
					}

					const rule = RECONCILE_BY_IDENTITY[change.store];
					let writtenPk: number | string;
					if (rule) {
						// Logically-keyed row: land it on its identity, never on top of an
						// unrelated row that happens to hold the author's diverged pk.
						writtenPk = await applyIdentityPut(
							change.store,
							rule,
							change.value,
						);
					} else {
						writtenPk = await api.put(change.value);
					}
					// Forget the pk the row was ACTUALLY written under - it can differ from
					// the author's `change.id` when keys diverged, and forgetting the raw
					// `change.id` would both leave our own write pending (re-broadcast echo)
					// and potentially swallow an unrelated concurrent local edit that
					// happens to sit at the author's pk.
					changeTracker.forget(change.store, writtenPk);
				}
			} catch (error) {
				// Name the exact record that failed (a deterministic apply failure
				// retries forever; an anonymous error would make the poison record
				// undiagnosable from the field), but keep applying the REST of the
				// changeset - see `failures` above.
				const detail = `${change.store}/${String(change.id)} (${change.type}): ${
					error instanceof Error ? error.message : String(error)
				}`;
				console.error(`Apply failed at ${detail}`, error);
				failures.push(detail);
				continue;
			}
			touchedStores.add(change.store);

			if (change.store === "gameAttributes") {
				touchedGameAttributes = true;
				if (change.id === "phase" || change.id === "nextPhase") {
					touchedPhase = true;
				}
				if (change.id === "season") {
					touchedSeason = true;
				}
				if (change.id === "daysLeft") {
					touchedStatus = true;
				}
			} else if (change.store === "games" || change.store === "schedule") {
				touchedGames = true;
			}
		}
	} finally {
		changeTracker.endApply();
	}

	await refreshAfterApply({
		touchedSeason,
		touchedGameAttributes,
		touchedGames,
		touchedPhase,
		touchedStatus,
		touchedStores,
		refreshUI,
		// v1 applies are per-record and can leave phantom/stranded schedule rows;
		// the sweep is their healer. v2 applies are atomic, so it never runs there.
		sweepGames: true,
		redirect: true,
	});

	// Declined at least one stale write, which means something reached this
	// device out of order. The records that were declined are still wrong
	// somewhere - re-read the whole log in order to settle them, and say so,
	// because silent corruption discovered days later is how this class of bug
	// has always been found.
	// Not during a full replay: it walks the whole log oldest-first, so declining
	// the old copies of records this device already has is the expected outcome,
	// not an anomaly. Reporting it there would also re-arm the marker the resync
	// is trying to clear, and resync forever.
	if (regressions.length > 0 && !getSyncEngine()?.isResyncing()) {
		console.error(
			`[sync] Declined ${regressions.length} stale write(s) that would have moved the league backwards: ${regressions.slice(0, 3).join("; ")}${regressions.length > 3 ? "; …" : ""}`,
		);
		try {
			getSyncEngine()?.markResyncNeeded();
		} catch {
			// Best effort.
		}
	}

	// Applied everything that could apply; NOW surface any per-record failures so
	// the sync engine pins the watermark below this changeset's entries and
	// retries them (idempotent re-apply). The healthy records above are already
	// in - a retry just re-puts them.
	if (failures.length > 0) {
		throw new Error(
			`Apply failed for ${failures.length} of ${changeset.changes.length} records: ${failures
				.slice(0, 3)
				.join("; ")}${failures.length > 3 ? "; …" : ""}`,
		);
	}
};

// ---------------------------------------------------------------------------
// Regression guard
//
// Everything above is about applying changesets in the order they were
// authored. That order is what makes record-level last-write-wins correct, and
// getting it wrong has now produced the same failure three separate ways: a
// bulk batch judged by a chunk its author re-uploaded later, a resync replaying
// by entry instead of by authoring unit, and a live entry landing mid-replay.
// Each was a real bug and each is fixed, but they share a shape - some path
// applies an old changeset over newer state - and the cost when one slips
// through is silent corruption a user finds days later: a phase snapping back
// to AFTER_DRAFT, a team's record going from 44-4 to 41-4.
//
// So stop relying only on getting the order right, and make an out-of-order
// write HARMLESS for the fields where "older" is provable from the data itself.
// A league's phase and a team's games played only ever move forward; a
// changeset that would move one backward is stale, whatever the ordering
// machinery believes. Refuse the write, say so, and ask for a full re-read of
// the log - which replays everything in order and lands on the true state.
//
// This also makes replay order-insensitive for these fields: applying old and
// new in any sequence converges on the newest, because the older one is simply
// declined.
//
// Deliberately narrow. Only fields that are monotonic by construction are
// guarded - guessing at players or game records would refuse legitimate edits.
// And a genuinely intended backwards move (God Mode rewinding a phase) is not
// lost: it is the newest entry in the log, so the resync this triggers replays
// it last and it wins.

// (season, phase) as one comparable number. Phase resets each season, so the
// season has to dominate.
const phaseOrder = (season: unknown, phase: unknown): number | undefined =>
	typeof season === "number" && typeof phase === "number"
		? season * 100 + phase
		: undefined;

// Would this write move a monotonic field backwards? Returns why, or undefined.
export const regressionReason = async (
	change: SyncChange,
	// The season this change's CHANGESET is for, when it names one.
	changesetSeason?: number,
): Promise<string | undefined> => {
	if (change.type !== "put" || !change.value) {
		return undefined;
	}

	if (change.store === "gameAttributes") {
		const value = change.value as { key?: string; value?: unknown };
		if (value.key === "phase") {
			// Scored against the season the change BELONGS to, not ours.
			//
			// This used to read the local season for both sides, which makes a
			// phase from another season a meaningless comparison: an old entry
			// carrying "free agency" scored as 2005-free-agency against our
			// 2005-regular-season and sailed through as a move FORWARD - then the
			// correct current phase was declined as a move backwards. A device
			// mid-regular-season ended up in free agency and stayed there.
			//
			// A changeset that crosses a season boundary writes the season too, so
			// the season is available on the same changeset; without it there is
			// nothing to compare and the safe answer is to allow the write. The
			// season's own guard below still blocks a genuine rewind.
			const season = changesetSeason ?? g.get("season");
			const incoming = phaseOrder(season, value.value);
			const local = phaseOrder(g.get("season"), g.get("phase"));
			if (incoming !== undefined && local !== undefined && incoming < local) {
				return `phase ${String(value.value)} in season ${season} is behind local ${g.get("season")} phase ${g.get("phase")}`;
			}

			// The other direction old history can wear a disguise: an entry from a
			// PREVIOUS season's offseason that never wrote a season (phases within a
			// season don't) reads as this season's free agency and sails through as
			// a big move forward. But a real advance is one changeset, one phase -
			// nothing legitimate jumps several phases in a single write. Allow a
			// margin of two for an entry or two lost in delivery (catch-up replays
			// stepwise anyway), keep the special negative phases (fantasy and
			// expansion drafts re-enter the ordinary sequence from odd angles) out
			// of it, and treat anything bigger as displaced history. Season
			// crossings are exempt: a rollover names its season and is judged above.
			const localPhase = g.get("phase");
			if (
				season === g.get("season") &&
				typeof value.value === "number" &&
				value.value >= 0 &&
				localPhase >= 0 &&
				value.value > localPhase + 2
			) {
				return `phase ${value.value} jumps ${value.value - localPhase} phases ahead of local phase ${localPhase} in one write`;
			}
		}
		if (value.key === "season") {
			const incoming = value.value;
			if (typeof incoming === "number" && incoming < g.get("season")) {
				return `season ${incoming} is behind local season ${g.get("season")}`;
			}
		}
		return undefined;
	}

	if (change.store === "teamSeasons") {
		const value = change.value as {
			tid?: number;
			season?: number;
			won?: number;
			lost?: number;
			tied?: number;
			otl?: number;
		};
		if (typeof value.tid !== "number" || typeof value.season !== "number") {
			return undefined;
		}
		const existing: any = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsByTidSeason",
			[value.tid, value.season],
		);
		if (!existing) {
			return undefined;
		}
		const played = (row: any) =>
			(row.won ?? 0) + (row.lost ?? 0) + (row.tied ?? 0) + (row.otl ?? 0);
		const incoming = played(value);
		const local = played(existing);
		if (incoming < local) {
			return `tid ${value.tid} ${value.season} record ${value.won}-${value.lost} is behind local ${existing.won}-${existing.lost}`;
		}
	}

	return undefined;
};

// The played-game invariant: a schedule row whose gid already has a game record
// is a sync artifact - the game was simmed somewhere, so that schedule row
// should be gone. (gids are league-unique forever and the cache keeps schedule
// ids above game ids, so a gid collision can never be a legitimate future
// game.) Normally the sim's own schedule delete handles this, but a partially
// applied/abandoned changeset could leave the game row without its delete -
// seen in the field as a whole day's games showing both played AND upcoming on
// one device. Runs after game-touching applies and once per connect; deletes
// through the cache so it's captured (a no-op on healthy devices) and flushed.
export const sweepPhantomScheduleRows = async (): Promise<number> => {
	const [scheduleRows, gameRows] = await Promise.all([
		idb.cache.schedule.getAll(),
		idb.cache.games.getAll(),
	]);
	const playedGids = new Set(gameRows.map((game) => game.gid));
	let removed = 0;
	for (const row of scheduleRows) {
		if (playedGids.has(row.gid)) {
			await idb.cache.schedule.delete(row.gid);
			removed += 1;
		}
	}
	if (removed > 0) {
		console.log(
			`Removed ${removed} already-played game(s) from the schedule (sync self-heal)`,
		);
	}
	return removed;
};

// The forward-progress invariant: a league plays its days in order, deleting
// each schedule row as that game is simmed. So an UNPLAYED row can never sit on
// a day EARLIER than a day that has already been played. When one does, this
// device missed that whole day's changeset - it received neither the games nor
// the schedule deletes - while later days arrived normally.
//
// This is the one corruption sweepPhantomScheduleRows cannot see. That sweep
// matches a schedule row against an existing game; here there is no game to
// match, precisely because the games never landed.
//
// Left alone it is much worse than a hole in the game log. getSchedule takes
// "today" from the first row in the schedule, and schedule rows are keyed by
// gid, so the missed day sorts first and the device is pinned on it: it lists
// that day's matchups as upcoming, the Play button offers to sim a day the rest
// of the league finished long ago, and the games it would write carry gids that
// already have results everywhere else.
export const findStrandedScheduleRows = async (): Promise<{
	gids: number[];
	days: number[];
	maxPlayedDay: number | undefined;
}> => {
	const [scheduleRows, gameRows] = await Promise.all([
		idb.cache.schedule.getAll(),
		// Current season only - that's what the cache holds, and it's the only
		// season with a schedule.
		idb.cache.games.getAll(),
	]);

	let maxPlayedDay: number | undefined;
	for (const game of gameRows) {
		// Older games predate the day field.
		if (
			game.day !== undefined &&
			(maxPlayedDay === undefined || game.day > maxPlayedDay)
		) {
			maxPlayedDay = game.day;
		}
	}
	if (maxPlayedDay === undefined) {
		return { gids: [], days: [], maxPlayedDay };
	}

	const gids: number[] = [];
	const days = new Set<number>();
	for (const row of scheduleRows) {
		// Strictly earlier: a row on the max played day is just an unfinished
		// slate, which is normal while a day is being simmed.
		if (row.day !== undefined && row.day < maxPlayedDay) {
			gids.push(row.gid);
			days.add(row.day);
		}
	}

	return { gids, days: [...days].sort((a, b) => a - b), maxPlayedDay };
};

// Last resort, once a full resync has failed to recover the missed day: drop
// the stranded rows. The games themselves are unrecoverable at that point - no
// local computation can invent results the rest of the league already agreed on
// - but leaving the rows is the actively dangerous half. Removing them puts
// this device back on the league's real day and makes it impossible for it to
// re-sim a day everyone else has already played.
export const dropStrandedScheduleRows = async (
	gids: number[],
): Promise<number> => {
	let removed = 0;
	for (const gid of gids) {
		await idb.cache.schedule.delete(gid);
		removed += 1;
	}
	return removed;
};
