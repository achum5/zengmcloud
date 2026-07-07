import { captureChangeset, type Changeset } from "./changeset.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { logChangeset } from "./devChangesetLogger.ts";
import { getSyncEngine } from "./engineHolder.ts";
import { buildNotifications } from "./notifications.ts";

// The local changeset an action produced, ready to publish.
export type CapturedAction = { label: string; changeset: Changeset };

// Drain the change tracker for the action that just ran and turn it into a
// changeset. This MUST run under the sync lock (see runExclusive), immediately
// after the action's writes and BEFORE any remote apply - otherwise an apply's
// suppression window can eat the action's writes and we'd capture nothing (which
// is how a sim right after taking the wheel could silently fail to publish).
// Returns undefined when there's nothing to sync. Never throws.
export const captureAfterAction = async (
	type: string,
	name: string,
): Promise<CapturedAction | undefined> => {
	try {
		// Fast path: most actions change nothing.
		if (changeTracker.size() === 0) {
			return undefined;
		}

		const changeset = await captureChangeset();
		if (changeset.changes.length === 0) {
			return undefined;
		}

		const label = `${type}.${name}`;

		if (process.env.NODE_ENV === "development") {
			logChangeset(label, changeset);
		}

		return { label, changeset };
	} catch {
		// Capture must never affect gameplay.
		return undefined;
	}
};

// Publish a captured changeset to the room and fan out any phone pushes. This is
// network I/O and runs OUTSIDE the sync lock, so it never blocks remote applies.
// Best-effort - never throws.
export const deliverAfterAction = async ({ label, changeset }: CapturedAction) => {
	try {
		const engine = getSyncEngine();
		if (!engine) {
			return;
		}

		await engine.onLocalChangeset(changeset, label);

		// Fan phone pushes out to the other devices in the room, if this change is
		// noteworthy (a trade, a roster move, the host finishing a sim, or a phase
		// that needs a human). Best-effort - never blocks play.
		const notifications = await buildNotifications(label, changeset, {
			isHost: engine.getIsHost(),
			authorName: engine.localName,
		});
		for (const notification of notifications) {
			await engine.publishNotification(notification);
		}
	} catch {
		// Sync/push must never affect gameplay.
	}
};
