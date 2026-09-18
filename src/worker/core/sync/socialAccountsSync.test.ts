// DOES THE FEED TRAVEL? The question comes up because the timeline is not
// stored anywhere: socialFeed derives a day on demand from the league plus a
// seed, so two devices compute the same posts independently and there is
// nothing to sync. What IS stored is the handful of rows somebody typed over
// the top - a renamed writer, a hand-made account, a removed one - and those
// have to reach everyone or one person's league has a cast the others cannot
// see.
//
// Nothing here is new behaviour; it is a lock on behaviour that comes from
// socialAccounts being an ordinary store. It would be quietly lost by adding
// the store to the device-local list, or by adding its editors to the list of
// calls that never open a capture window.

import { assert, describe, test } from "vitest";
import { STORES } from "../../db/Cache.ts";
import { DEVICE_LOCAL_STORES, isDeviceLocal } from "./changeset.ts";
import { SIM_INERT_STORES } from "./publishGuards.ts";

describe("feed accounts across devices", () => {
	test("the store is shared, not per-device", () => {
		assert.include(STORES, "socialAccounts");
		assert.isFalse(DEVICE_LOCAL_STORES.has("socialAccounts" as any));
		assert.isFalse(isDeviceLocal("socialAccounts" as any, "m:abc"));
	});

	test("an edit cannot cost a league-mate their sim day", () => {
		// See the note on SIM_INERT_STORES. The feed is derived, so a day was
		// never computed from these rows.
		assert.isTrue(SIM_INERT_STORES.has("socialAccounts"));
	});

	test("the editors are not among the calls that skip capture", async () => {
		// A capture window is what puts a write into a changeset. The suppressed
		// list is for bulk and read-only paths; an account editor is neither,
		// and if one ever landed on that list the edit would stay on the device
		// that made it with nothing to show for it.
		const source: string = await import(
			("node" + ":fs") as any
		).then((fs: any) =>
			fs.readFileSync(
				new URL("../../index.ts", import.meta.url).pathname,
				"utf8",
			),
		);
		const skipBlock = source.slice(
			source.indexOf("SKIP_CHANGESET_CAPTURE"),
			source.indexOf("const isChangesetSuppressedCall"),
		);
		assert.isAbove(skipBlock.length, 0, "could not find the skip list");
		// Sanity: the block really is the skip list.
		assert.include(skipBlock, '"beforeView"');
		for (const name of [
			"socialAccountUpdate",
			"socialAccountRemove",
			"socialAccountsBatch",
		]) {
			assert.notInclude(skipBlock, `"${name}"`, name);
		}
	});
});
