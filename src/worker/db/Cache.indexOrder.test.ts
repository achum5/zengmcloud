import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../test/helpers.ts";
import { idb } from "./index.ts";

// Real IndexedDB returns range queries sorted by index key. The in-memory
// cache index iterates in insertion order, which happened to match key order
// until a synced identity reconcile re-created an old season's teamSeasons row
// under a fresh HIGH rid - putting its index key AFTER the current season's.
// writeTeamStats takes `.at(-1)` of a [tid, season-2]→[tid, season] range to
// find the current season's row, so a whole game result (win, streak, revenue)
// landed on the PREVIOUS season. Range results must be sorted by key.
describe("Cache indexGetAll range ordering", () => {
	beforeEach(async () => {
		resetG();
		await resetCache({});
	});

	test("range results are sorted by index key even when rows were inserted out of key order", async () => {
		// Insert seasons out of order with rids that do NOT follow season order -
		// the exact shape that misdirected the game write: (tid 8, 2001) re-created
		// at rid 88, above (tid 8, 2002) at rid 67.
		await idb.cache.teamSeasons.add({
			rid: 17,
			tid: 8,
			season: 2000,
			won: 44,
		} as any);
		await idb.cache.teamSeasons.add({
			rid: 67,
			tid: 8,
			season: 2002,
			won: 0,
		} as any);
		await idb.cache.teamSeasons.add({
			rid: 88,
			tid: 8,
			season: 2001,
			won: 58,
		} as any);
		// Another team's rows, to prove the tid bound holds too.
		await idb.cache.teamSeasons.add({
			rid: 19,
			tid: 9,
			season: 2000,
			won: 39,
		} as any);

		const rows = await idb.cache.teamSeasons.indexGetAll(
			"teamSeasonsByTidSeason",
			[
				[8, 2000],
				[8, 2002],
			],
		);

		assert.deepEqual(
			rows.map((row) => row.season),
			[2000, 2001, 2002],
			JSON.stringify(rows),
		);
		// The idiom writeTeamStats uses: `.at(-1)` must be the CURRENT season.
		assert.strictEqual(rows.at(-1)!.season, 2002);
		assert.strictEqual(rows.at(-1)!.rid, 67);
	});
});
