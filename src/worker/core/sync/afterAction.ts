import { captureChangeset } from "./changeset.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { logChangeset } from "./devChangesetLogger.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { buildNotifications } from "./notifications.ts";

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
