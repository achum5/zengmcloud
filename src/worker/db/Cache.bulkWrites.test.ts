import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../test/helpers.ts";
import { idb } from "./index.ts";
import { changeTracker } from "./changeTracker.ts";

// Cloud sync learns what changed by watching the cache: every write lands in
// changeTracker, and the sync layer drains it to build a changeset. Upstream
// added bulk writers (putAll/addAll) and converted dozens of per-row loops to
// use them - writePlayerStats alone now saves a whole day of games in one call.
// If a bulk writer ever wrote straight into Cache._data instead of going
// through _storeObj, every row it wrote would be invisible to the rest of the
// room: the device advances locally and silently forks. These tests pin that
// down, so a future pull from upstream that reimplements the bulk path has to
// keep it.

const draftPick = (dpid: number) => ({
	dpid,
	tid: 0,
	originalTid: 0,
	round: 1,
	pick: dpid,
	season: 2017,
});

const captured = async (fn: () => Promise<void>) => {
	changeTracker.reset();
	await changeTracker.runCaptured(fn);
	return changeTracker.drain();
};

describe("bulk cache writes are visible to cloud sync", () => {
	beforeEach(async () => {
		resetG();
		await resetCache();
		changeTracker.disable();
		changeTracker.reset();
		changeTracker.enable();
	});

	test("putAll records every row, exactly like put", async () => {
		const changes = await captured(async () => {
			await idb.cache.draftPicks.putAll([draftPick(1), draftPick(2)]);
		});

		assert.deepStrictEqual(
			changes.map((change) => [change.store, change.id, change.type]),
			[
				["draftPicks", 1, "put"],
				["draftPicks", 2, "put"],
			],
		);
	});

	test("addAll records every row, and takes any iterable", async () => {
		const changes = await captured(async () => {
			await idb.cache.draftPicks.addAll(
				new Set([draftPick(1), draftPick(2), draftPick(3)]),
			);
		});

		assert.deepStrictEqual(
			changes.map((change) => change.id),
			[1, 2, 3],
		);
		assert.strictEqual((await idb.cache.draftPicks.getAll()).length, 3);
	});

	test("a bulk write outside a capture window records nothing, like a single write", async () => {
		await idb.cache.draftPicks.putAll([draftPick(1)]);
		assert.strictEqual(changeTracker.size(), 0);
	});

	test("getAllByKey sees rows written in bulk", async () => {
		await idb.cache.draftPicks.putAll([draftPick(7)]);
		const byKey = await idb.cache.draftPicks.getAllByKey();
		assert.strictEqual(byKey[7]?.dpid, 7);
	});
});
