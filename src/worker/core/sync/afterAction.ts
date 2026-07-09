import { captureChangeset } from "./changeset.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { logChangeset } from "./devChangesetLogger.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { isSingleGameSimActive } from "./afterActionHook.ts";
import { buildNotifications } from "./notifications.ts";
import { shouldTraceSyncLabel, syncDebugLog } from "./debugLog.ts";
import { idb } from "../../db/index.ts";
import { local, lock } from "../../util/index.ts";

// Actions that sim a SINGLE game within a day (Sim one game / live game), as
// opposed to advancing whole days. Their results must still sync, but they must
// never push a phone notification - you deliberately left the rest of the day
// unplayed. The generic worker wrapper also fires afterAction for these labels
// (and liveGame is fire-and-forget, so it can't rely on the play.ts silent
// hook), so we detect them by label here too, belt-and-suspenders.
const SILENT_SYNC_LABELS = new Set(["actions.simGame", "actions.liveGame"]);

let retryTimeoutID: ReturnType<typeof setTimeout> | undefined;

const scheduleRetry = (type: string, name: string) => {
	if (retryTimeoutID !== undefined) {
		return;
	}

	retryTimeoutID = setTimeout(() => {
		retryTimeoutID = undefined;
		if (getSyncEngine()) {
			void afterAction(type, name);
		}
	}, 5000);
	const nodeTimeout = retryTimeoutID as unknown as
		| { unref?: () => void }
		| undefined;
	if (typeof nodeTimeout?.unref === "function") {
		nodeTimeout.unref();
	}
};

// Runs (fire-and-forget) after each user action that could mutate state. It
// drains the change tracker ONCE and fans the resulting changeset out to both
// the dev console logger and the cloud sync engine (if connected). Must never
// throw - it piggybacks on the action's response and must not affect it.
//
// options.silent: still publishes the changeset (sync stays sound) but skips the
// phone-push notifications. Used for a single-game sim within a day - that
// shouldn't ping anyone, even though its results must still reach other devices.
export const afterAction = async (
	type: string,
	name: string,
	options?: { silent?: boolean },
) => {
	const label = `${type}.${name}`;
	let changeset: Awaited<ReturnType<typeof captureChangeset>> | undefined;
	let published = false;

	try {
		// Fast path: most actions change nothing.
		const pendingBeforeCapture = changeTracker.size();
		const trace = shouldTraceSyncLabel(label) || pendingBeforeCapture > 0;
		if (trace) {
			syncDebugLog("afterAction:start", {
				label,
				pendingBeforeCapture,
				silentOption: !!options?.silent,
				singleGameSimActive: isSingleGameSimActive(),
				hasEngine: !!getSyncEngine(),
			});
		}
		if (pendingBeforeCapture === 0) {
			if (trace) {
				// Include the tracker's internal state: a sim label with zero pending
				// changes is the signature of a capture wedge (suppression stuck on),
				// and this makes it diagnosable straight from the console.
				syncDebugLog("afterAction:no-pending-changes", {
					label,
					tracker: changeTracker.debugState(),
				});
			}
			return true;
		}

		changeset = await captureChangeset();
		if (changeset.changes.length === 0) {
			if (trace) {
				syncDebugLog("afterAction:captured-empty", { label });
			}
			return true;
		}

		// Force silent for the whole single-game-sim window (live sim / "Sim one
		// game"), whatever drains the changeset - a live sim's playback navigation
		// spawns interleaved worker calls that can drain (and would otherwise notify
		// on) the game result before the sim's own silent drain runs.
		const silent =
			!!options?.silent ||
			SILENT_SYNC_LABELS.has(label) ||
			isSingleGameSimActive();

		if (process.env.NODE_ENV === "development") {
			logChangeset(label, changeset);
		}
		if (trace) {
			syncDebugLog("afterAction:captured", {
				label,
				records: changeset.changes.length,
				silent,
				hasEngine: !!getSyncEngine(),
			});
		}

		const engine = getSyncEngine();
		let outcome: "confirmed" | "queued" = "confirmed";
		if (engine) {
			// Hand the changeset to the sync layer. onLocalChangeset persists it to
			// the durable outbox BEFORE any network attempt, so once it returns
			// (confirmed OR queued) the delta can no longer be lost - only delayed.
			// It throws ONLY if the delta could not be made durable, in which case
			// the outer catch restores the pending changes and schedules a retry.
			try {
				if (trace) {
					syncDebugLog("afterAction:publish-start", {
						label,
						records: changeset.changes.length,
					});
				}
				outcome = await engine.onLocalChangeset(changeset, label);
				published = true;
				if (trace) {
					syncDebugLog(
						outcome === "confirmed"
							? "afterAction:publish-confirmed"
							: "afterAction:publish-queued-durably",
						{
							label,
							records: changeset.changes.length,
						},
					);
				}
			} catch (error) {
				console.error(
					`[sync] Could not durably queue "${label}" (${changeset.changes.length} records) - restoring it for retry.`,
					error,
				);
				throw error;
			}

			// Make the just-published change durable locally right away. Until the
			// next periodic flush (every few seconds) it lives ONLY in the in-memory
			// cache. If the app is killed before that flush - e.g. iOS backgrounding
			// the PWA moments after a trade - the change is lost locally even though
			// it reached the cloud, because a device skips re-applying its OWN entries
			// on catch-up (its clientId matches, so the cloud copy can't restore it).
			// Flushing now closes that window. Skip while a local sim / phase change /
			// autoplay is running: those batch their own flushes for speed and persist
			// as they go, so an extra flush here would just fight that batching.
			if (published) {
				try {
					if (
						!lock.get("gameSim") &&
						!lock.get("newPhase") &&
						!local.autoPlayUntil
					) {
						await idb.cache.flush();
					}
				} catch {
					// Best-effort durability; the periodic flush still catches up.
				}
			}

			// Fan phone pushes out ONLY once the change actually reached the room
			// ("confirmed", not just queued). Otherwise a push implies a sync that
			// hasn't happened yet - the confusing case where phones ping but nothing
			// is in the shared log. A queued change syncs on its own later; its data
			// arrives via the change log without a push, which is fine. A sim
			// produces one detailed notification per team; everything else produces
			// one. Best-effort - never blocks play. Skipped entirely for a silent
			// publish (e.g. a single-game sim).
			if (published && outcome === "confirmed" && !silent) {
				try {
					const notifications = await buildNotifications(label, changeset, {
						isHost: engine.getIsHost(),
						authorName: engine.localName,
					});
					for (const notification of notifications) {
						await engine.publishNotification(notification);
					}
				} catch (error) {
					console.error("[sync] Failed to publish notifications", error);
				}
			}
		} else {
			// No cloud target, so there is nothing to retry.
			published = true;
			if (trace) {
				syncDebugLog("afterAction:no-engine", {
					label,
					records: changeset.changes.length,
				});
			}
		}
		// false ⇒ the change is safely queued but hasn't reached the cloud yet;
		// the caller may tell the user it will upload automatically.
		return outcome === "confirmed";
	} catch (error) {
		if (changeset && !published) {
			changeTracker.restore(
				changeset.changes.map((change) => ({
					store: change.store,
					id: change.id,
					type: change.type,
				})),
			);
			syncDebugLog("afterAction:publish-failed-restored-for-retry", {
				label,
				records: changeset.changes.length,
				error,
			});
			scheduleRetry(type, name);
			return false;
		}

		try {
			if (changeTracker.size() > 0) {
				return false;
			}
		} catch {}

		// Sync/logging must never affect gameplay.
		return true;
	}
};
