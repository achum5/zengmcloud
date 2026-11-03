// ONE COPY OF A SHARED LEAGUE SYNCS ON A DEVICE.
//
// A league-mate's phone once ran two copies of the game at the same time -
// the web app he was using, and an older copy still alive behind it - both
// signed in as the same device, sharing one save and one outbox, each with
// its own picture of the league in memory. Each copy only took in the
// versions it happened to grab first, so each fell out of date in its own
// way. The one he wasn't looking at ended up thinking free agency still had a
// day to go, and five minutes after the season had rolled over it ran that
// day and the rollover again: second progs, and thirty-odd signings undone,
// for everyone.
//
// So the copy that connected LAST owns syncing on this device, and says so in
// the device's own storage. Every other copy checks before it claims a step,
// publishes a change, or runs anything that needs to be in charge of simming
// - and the moment it finds it is not the owner it stands down: disconnects,
// stops writing its memory over the save, and asks to be reloaded. Reloading
// makes it the newest copy, which is all it takes to be the owner again.

import { idb } from "../../db/index.ts";
import { g, logEvent } from "../../util/index.ts";
import { syncDebugLog } from "./debugLog.ts";

const KEY = "syncInstanceOwner";

const INSTANCE_ID = `${Date.now().toString(36)}-${Math.random()
	.toString(36)
	.slice(2)}`;

type Owner = { id: string; lid: number; at: number };

let stoodDown = false;
let onStandDown: (() => Promise<void>) | undefined;

// Pure. No record yet (a device from before this existed), or a record about
// some other league, leaves this copy free to go on.
export const ownsSync = (
	owner: Owner | undefined,
	lid: number,
	id: string = INSTANCE_ID,
): boolean => !owner || owner.lid !== lid || owner.id === id;

const lidNow = (): number | undefined => {
	try {
		const lid = g.get("lid");
		return typeof lid === "number" ? lid : undefined;
	} catch {
		return undefined;
	}
};

// How to stand down (connect.ts passes its teardown; kept as a hook so this
// file needs nothing from connect).
export const setStandDownHandler = (handler: () => Promise<void>) => {
	onStandDown = handler;
};

export const takeSyncOwnership = async (lid: number): Promise<void> => {
	stoodDown = false;
	try {
		await idb.meta.put(
			"attributes",
			{ id: INSTANCE_ID, lid, at: Date.now() } satisfies Owner,
			KEY,
		);
	} catch (error) {
		syncDebugLog("owner:take-failed", { error: String(error) });
	}
};

export const hasStoodDown = () => stoodDown;

// True while this copy may sync. The first time it finds another copy has
// taken over, it stands down for good.
export const stillSyncOwner = async (): Promise<boolean> => {
	if (stoodDown) {
		return false;
	}
	const lid = lidNow();
	if (lid === undefined) {
		return true;
	}
	let owner: Owner | undefined;
	try {
		owner = (await idb.meta.get("attributes", KEY)) as Owner | undefined;
	} catch {
		// Can't tell - never the reason a real copy stops working.
		return true;
	}
	if (ownsSync(owner, lid)) {
		return true;
	}
	stoodDown = true;
	syncDebugLog("owner:stood-down", {
		me: INSTANCE_ID,
		owner: owner?.id,
		ownerSince: owner?.at,
	});
	// Whatever this copy has in memory and not yet saved is from a picture of
	// the league that is out of date; writing it now would overwrite the
	// owner's save.
	try {
		idb.cache.stopAutoFlush();
	} catch {}
	try {
		await onStandDown?.();
	} catch (error) {
		syncDebugLog("owner:stand-down-failed", { error: String(error) });
	}
	try {
		logEvent({
			type: "error",
			text: "This league was opened again on this device. Reload to keep playing here.",
			saveToDb: false,
			persistent: true,
		});
	} catch {}
	return false;
};
