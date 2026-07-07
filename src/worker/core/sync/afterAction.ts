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
			await engine.onLocalChangeset(changeset, label);

			// Fan phone pushes out to the other devices in the room, if this change
			// is noteworthy (a trade, a roster move, the host finishing a sim, or a
			// phase that needs a human). A sim produces one detailed notification per
			// team; everything else produces one. Best-effort - never blocks play.
			const notifications = await buildNotifications(label, changeset, {
				isHost: engine.getIsHost(),
				authorName: engine.localName,
			});
			for (const notification of notifications) {
				await engine.publishNotification(notification);
			}
		}
	} catch {
		// Sync/logging must never affect gameplay.
	}
};
