import { idb } from "../db/index.ts";
import logEvent from "./logEvent.ts";
import type { Conditions } from "../../common/types.ts";
import { CLOUD_CHANGES, unseenChanges } from "../../common/cloudChanges.ts";

// No more than this many at once.
const MAX_NUM_TO_SHOW = 4;

// What's changed in this game since he last loaded it (see cloudChanges.ts),
// shown once - the way BBGM's own changes are (see checkChanges).
export const checkCloudChanges = async (conditions: Conditions) => {
	const newest = CLOUD_CHANGES[0]?.n;
	if (newest === undefined) {
		return;
	}
	const last = (await idb.meta.get("attributes", "lastCloudChange")) as
		| number
		| undefined;
	const changes = unseenChanges(last);
	if (changes.length > 0) {
		let text = "";
		for (const change of changes.slice(0, MAX_NUM_TO_SHOW)) {
			text += `<p class="mt-1 mb-0"><strong>${change.date}</strong>: ${change.text}</p>`;
		}
		const more = changes.length - MAX_NUM_TO_SHOW;
		if (more > 0) {
			text += `<p class="mt-1 mb-0">...and ${more} more ${more === 1 ? "change" : "changes"}</p>`;
		}
		logEvent(
			{
				extraClass: "",
				persistent: true,
				type: "changes",
				text,
				saveToDb: false,
			},
			conditions,
		);
	}
	await idb.meta.put("attributes", newest, "lastCloudChange");
};
