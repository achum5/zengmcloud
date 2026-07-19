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
	// Resolve the LOCAL row that shares the incoming row's logical identity.
	// teamSeasons/teamStats use a unique IndexedDB index; draftPicks has no such
	// index, so it scans just its season-scoped slice.
	find: (api: any, value: any) => Promise<any | undefined>;
	// The row's primary key, to delete/forget by (rid, or dpid for draftPicks).
	pk: (row: any) => number;
	sameIdentity: (a: any, b: any) => boolean;
};
const RECONCILE_BY_IDENTITY: Partial<Record<Store, IdentityRule>> = {
	teamSeasons: {
		find: (api, v) => api.indexGet("teamSeasonsByTidSeason", [v.tid, v.season]),
		pk: (row) => row.rid,
		sameIdentity: (a, b) => a.tid === b.tid && a.season === b.season,
	},
	teamStats: {
		find: (api, v) => api.indexGet("teamStatsByPlayoffsTid", [v.playoffs, v.tid]),
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
};

// Drop a stale duplicate of an incoming logically-keyed row (see
// RECONCILE_BY_IDENTITY) so the put updates the row in place instead of creating
// a second row that violates the store's unique index (or, for draftPicks, a
// second pick for one logical slot). Best-effort and local: the removed primary
// key is forgotten from the tracker so we don't broadcast it (every device heals
// itself the same way when it applies the same authoritative row).
const reconcileIdentity = async (store: Store, value: any) => {
	const rule = RECONCILE_BY_IDENTITY[store];
	if (!rule) {
		return;
	}
	try {
		const existing = await rule.find(storeAPI(store), value);
		if (
			existing &&
			rule.pk(existing) !== rule.pk(value) &&
			rule.sameIdentity(existing, value)
		) {
			await storeAPI(store).delete(rule.pk(existing));
			changeTracker.forget(store, rule.pk(existing));
		}
	} catch (error) {
		console.error(
			`Failed to reconcile duplicate ${store} row before apply`,
			error,
		);
	}
};

// Replace an incoming player record's watch flag with this device's own, so
// watch lists stay per-user instead of bleeding across the room (a Cavs
// device's shortlist should only ever show on that device). The local value is
// looked up in the cache first, then on disk (retired players and prospects
// can be watched but may not be cached). If this device never watched the
// player, the incoming flag is dropped entirely.
const preserveLocalWatch = async (incoming: any) => {
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
const DEVICE_LOCAL_GAME_ATTRIBUTES = new Set([
	"userTid",
	// The seed behind "show me a fresh set of AI trade proposals" on the Trade
	// Proposals page - per-device UI state (each device browses proposals for its
	// own team). Sharing it would reshuffle every league-mate's proposals.
	"tradeProposalsSeed",
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
const DEVICE_LOCAL_STORES = new Set<Store>([
	"trade",
	"savedTrades",
	"savedTradingBlock",
]);

const isDeviceLocal = (store: Store, id: number | string) =>
	DEVICE_LOCAL_STORES.has(store) ||
	(store === "gameAttributes" && DEVICE_LOCAL_GAME_ATTRIBUTES.has(String(id)));

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
			// For a logically-keyed store, carry the deleted row's identity so the
			// receiver deletes by (tid, season[, playoffs]) rather than by our `rid`
			// - which points at an unrelated row on a device whose rids diverged,
			// silently wiping the wrong (often much older) season.
			if (RECONCILE_BY_IDENTITY[typedStore] && deletedRow !== undefined) {
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

	let touchedGameAttributes = false;
	let touchedPhase = false;
	let touchedGames = false;
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
	for (const change of changeset.changes) {
		// Never let a peer's per-device/personal state (their controlled team,
		// their in-progress/saved trades) overwrite ours - also protects catch-up
		// replays of older, unfiltered history that predates this exclusion.
		if (isDeviceLocal(change.store, change.id)) {
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
				if (rule && change.value !== undefined) {
					const existing = await rule.find(api, change.value);
					if (existing) {
						await api.delete(rule.pk(existing));
						changeTracker.forget(change.store, rule.pk(existing));
					}
				} else {
					await api.delete(change.id);
				}
			} else {
				// Watch-list flags are personal: each user shortlists players for
				// their own team, so a synced player record must never carry another
				// device's watch color onto this one. Keep whatever watch THIS device
				// has for the player (including none) before applying the record.
				if (change.store === "players" && change.value) {
					await preserveLocalWatch(change.value);
				}

				// Heal any diverged-rid duplicate first, so a logically-keyed row (e.g.
				// teamSeasons) updates in place instead of tripping its unique index.
				await reconcileIdentity(change.store, change.value);
				await api.put(change.value);
			}
		} catch (error) {
			// Name the exact record that failed. A deterministic apply failure pins
			// the watermark and retries forever; without this the retry loop logs an
			// anonymous error and the poison record is undiagnosable from the field.
			throw new Error(
				`Apply failed at ${change.store}/${String(change.id)} (${change.type}): ${
					error instanceof Error ? error.message : String(error)
				}`,
				{ cause: error },
			);
		}
		changeTracker.forget(change.store, change.id);
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

	// A synced season rollover: persist what was just applied, then re-fill the
	// cache, exactly like finalize() does on the simmer for PRESEASON. The cache
	// scopes several stores to the current season (games, teamStats,
	// playoffSeries, allStars, headToHeads); without this a follower's cache
	// keeps last season's rows in "current season" queries forever.
	if (touchedSeason) {
		try {
			await idb.cache.flush();
			await idb.cache.fill();
		} catch (error) {
			console.error("Failed to re-fill cache after synced rollover", error);
		}
	}

	// The cache store now holds the new gameAttributes rows, but the in-memory
	// `g` object (and the UI's copy) is stale until we reload it from the cache.
	if (touchedGameAttributes) {
		await loadGameAttributes();
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
	//
	// Each refresh runs in its own try/catch so one failing (e.g. updatePlayMenu
	// reading half-synced data) can't skip the others - previously they shared a
	// catch, so a throw in updatePhase left the header frozen on the old phase.
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
		// Passing the URL makes the UI navigate (like finalize's redirect); without
		// it, realtimeUpdate just refreshes the current page in place.
		if (redirectUrl !== undefined) {
			await toUI("realtimeUpdate", [updateEvents, redirectUrl]);
		} else {
			await toUI("realtimeUpdate", [updateEvents]);
		}
	}
};
