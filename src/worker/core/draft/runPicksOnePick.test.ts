import { afterAll, assert, beforeAll, test } from "vitest";
import { mockIDBLeague } from "../../../test/helpers.ts";
import { draft } from "../index.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { getDraftTids, loadTeamSeasons } from "./testHelpers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";

// "Sim one pick" asked for a particular pick - the one a ready-up step was
// claimed for, or the one the Draft page showed on the clock - makes that
// pick or none. A pick the room made a moment earlier must not turn the click
// into the pick after it.

beforeAll(async () => {
	await loadTeamSeasons();
	idb.league = mockIDBLeague();
	await draft.genPlayers(g.get("season"), DEFAULT_LEVEL);
	const draftTids = await getDraftTids();
	// No user pick in the way of the first few.
	g.setWithoutSavingToDB("userTid", draftTids.at(-1)!);
	g.setWithoutSavingToDB("userTids", [draftTids.at(-1)!]);
});
afterAll(() => {
	// @ts-expect-error
	idb.league = undefined;
});

test("one pick, no pick asked for: the next one", async () => {
	const before = await draft.getOrder();
	const pids = await draft.runPicks({ type: "onePick" });
	assert.strictEqual(pids.length, 1);
	assert.strictEqual((await draft.getOrder()).length, before.length - 1);
});

test("one pick asked for by the pick up now: that one, and only it", async () => {
	const before = await draft.getOrder();
	const pids = await draft.runPicks({ type: "onePick", dpid: before[0]!.dpid });
	assert.strictEqual(pids.length, 1);
	const after = await draft.getOrder();
	assert.strictEqual(after.length, before.length - 1);
	assert.strictEqual(after[0]!.dpid, before[1]!.dpid);
});

test("one pick asked for by a pick already made: none", async () => {
	const before = await draft.getOrder();
	const made = before[0]!.dpid;
	await draft.runPicks({ type: "onePick", dpid: made });

	const pids = await draft.runPicks({ type: "onePick", dpid: made });
	assert.strictEqual(pids.length, 0);
	assert.strictEqual((await draft.getOrder()).length, before.length - 1);

	// And the draft is free for the next one (the lock let go).
	const next = await draft.runPicks({ type: "onePick" });
	assert.strictEqual(next.length, 1);
});
