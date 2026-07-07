import { idb } from "../../db/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import type { Store } from "../../db/Cache.ts";
import loadGameAttributes from "../league/loadGameAttributes.ts";
import {
	g,
	helpers,
	toUI,
	updatePhase,
	updatePlayMenu,
	updateStatus,
} from "../../util/index.ts";
import { getGlobalSettings } from "../../util/getGlobalSettings.ts";
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
	| { store: Store; id: number | string; type: "delete" };

// A batch of changes, ready to be JSON-serialized and shipped to the cloud.
export type Changeset = {
	changes: SyncChange[];
};

// Broad set of UI refresh signals - applying a remote changeset can touch
// almost anything, and the views only re-render for events they care about.
const APPLY_UPDATE_EVENTS: UpdateEvents = [
	"playerMovement",
	"gameAttributes",
	"team",
	"teamFinances",
	"playoffs",
	"gameSim",
];

const storeAPI = (store: Store) => (idb.cache as any)[store];

// Which team THIS device is currently acting as. In the multiplayer model the
// league is in multi-team mode with all the friends' teams in `userTids` (which
// DOES sync - it's what makes re-signing, the draft, etc. treat every friend's
// team as human-controlled when the host sims). Only `userTid` - the one team
// you're currently viewing/managing - is per-device, so friends don't yank each
// other onto the same team.
const DEVICE_LOCAL_GAME_ATTRIBUTES = new Set(["userTid"]);

const isDeviceLocal = (store: Store, id: number | string) =>
	store === "gameAttributes" && DEVICE_LOCAL_GAME_ATTRIBUTES.has(String(id));

// Drain everything the tracker has recorded since the last capture and turn it
// into a self-contained changeset by reading the current value of each record.
// Whole-record values mean the receiver needs no prior state to apply this.
export const captureChangeset = async (): Promise<Changeset> => {
	const pending = changeTracker.drain();
	const changes: SyncChange[] = [];

	for (const { store, id, type } of pending) {
		const typedStore = store as Store;

		// Never broadcast which team we control.
		if (isDeviceLocal(typedStore, id)) {
			continue;
		}

		if (type === "delete") {
			changes.push({ store: typedStore, id, type: "delete" });
			continue;
		}

		const value = await storeAPI(typedStore).get(id);
		if (value === undefined) {
			// Put then deleted before we captured - treat as a delete.
			changes.push({ store: typedStore, id, type: "delete" });
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

	let touchedGameAttributes = false;
	let touchedPhase = false;
	let touchedGames = false;
	// The free-agency countdown ("28 days left") and the draft-progress text are
	// driven by `local.statusText`, which only updates when someone calls
	// updateStatus(). The simming device does that itself; a device that merely
	// RECEIVES the daysLeft/phase change must refresh it too, or its status line
	// stays frozen on the old day even though g.daysLeft advanced.
	let touchedStatus = false;

	// Apply each write, then IMMEDIATELY forget just that record from the change
	// tracker. We deliberately do NOT globally suppress recording during the
	// apply: a global flag would also swallow the writes of a local sim running
	// at the same time (e.g. right after this device takes over simming while
	// still catching up), leaving that sim unpublished. Forgetting only our own
	// applied records keeps a concurrent local action's other writes intact.
	for (const change of changeset.changes) {
		// Never let a peer's "which team I control" overwrite ours (also
		// protects catch-up replays of older, unfiltered history).
		if (isDeviceLocal(change.store, change.id)) {
			continue;
		}

		const api = storeAPI(change.store);

		if (change.type === "delete") {
			await api.delete(change.id);
		} else {
			await api.put(change.value);
		}
		changeTracker.forget(change.store, change.id);

		if (change.store === "gameAttributes") {
			touchedGameAttributes = true;
			if (change.id === "phase" || change.id === "nextPhase") {
				touchedPhase = true;
			}
			if (change.id === "daysLeft") {
				touchedStatus = true;
			}
		} else if (change.store === "games" || change.store === "schedule") {
			touchedGames = true;
		}
	}

	// The cache store now holds the new gameAttributes rows, but the in-memory
	// `g` object (and the UI's copy) is stale until we reload it from the cache.
	if (touchedGameAttributes) {
		await loadGameAttributes();
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
