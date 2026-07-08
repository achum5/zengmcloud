import { captureChangeset } from "./changeset.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { logChangeset } from "./devChangesetLogger.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { buildNotifications } from "./notifications.ts";
import { idb } from "../../db/index.ts";
import { local, lock } from "../../util/index.ts";

// Runs (fire-and-forget) after each user action that could mutate state. It
// drains the change tracker ONCE and fans the resulting changeset out to both
// the dev console logger and the cloud sync engine (if connected). Must never
// throw - it piggybacks on the action's response and must not affect it.
export const afterAction = async (type: string, name: string) => {
	try {
		// Fast path: most actions change nothing.
		if (changeTracker.size() === 0) {
			return;
		}

		const changeset = await captureChangeset();
		if (changeset.changes.length === 0) {
			return;
		}

		const label = `${type}.${name}`;

		if (process.env.NODE_ENV === "development") {
			logChangeset(label, changeset);
		}

		const engine = getSyncEngine();
		if (engine) {
			// Publishing IS the sync - if it throws, this change never reaches the
			// other devices, and the tracker was already drained so it won't be
			// recaptured. Keep it in its own try so a failure is logged loudly
			// (diagnosable) instead of being swallowed by the outer catch.
			let published = false;
			try {
				await engine.onLocalChangeset(changeset, label);
				published = true;
			} catch (error) {
				console.error(
					`[sync] Failed to publish "${label}" (${changeset.changes.length} records) - this change did NOT sync to other devices.`,
					error,
				);
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

			// Fan phone pushes out ONLY once the change actually reached the room.
			// Otherwise a push implies a sync that never happened - the confusing
			// case where the sim device advances and pings phones, but nothing lands
			// in the shared log. A sim produces one detailed notification per team;
			// everything else produces one. Best-effort - never blocks play.
			if (published) {
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
		}
	} catch {
		// Sync/logging must never affect gameplay.
	}
};
