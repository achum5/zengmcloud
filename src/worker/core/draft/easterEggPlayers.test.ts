import { assert, describe, test } from "vitest";
import { mockIDBLeague, resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { draft } from "../index.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";

// The Easter egg players fire at 1 in 100,000 and the fake age at 1 in 100, so
// neither is reachable from a test without forcing the roll - and forcing
// Math.random globally wedges player generation. What IS worth pinning down is
// the plumbing: g.get throws on an attribute it doesn't know about, so a
// missing entry anywhere in the chain would crash draft generation for every
// league rather than fail quietly.

describe("rare event settings", () => {
	test("both default to on, so existing leagues are unchanged", () => {
		resetG();
		assert.strictEqual(g.get("easterEggPlayers"), true);
		assert.strictEqual(g.get("fakeAges"), true);
	});

	test("generating a draft class with Easter eggs off works and is full size", async () => {
		resetG();
		await resetCache();
		idb.league = mockIDBLeague();
		g.setWithoutSavingToDB("easterEggPlayers", false);

		await draft.genPlayers(g.get("season"), DEFAULT_LEVEL);
		const players = await idb.cache.players.indexGetAll(
			"playersByDraftYearRetiredYear",
			[[g.get("season")], [g.get("season"), Infinity]],
		);

		// Turning the setting off must not cost the draft class a player - the
		// Easter eggs are extra, not part of the count.
		assert.strictEqual(players.length, 70);
		assert.strictEqual(
			players.filter((p) => p.firstName === "LaVar" || p.firstName === "Barack")
				.length,
			0,
		);

		// @ts-expect-error
		idb.league = undefined;
	});
});
