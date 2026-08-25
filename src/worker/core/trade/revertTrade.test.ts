import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { player } from "../index.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import processTrade from "./processTrade.ts";
import revertTrade, { planTradeRevert } from "./revertTrade.ts";
import { PLAYER } from "../../../common/constants.ts";

// The trade this suite keeps undoing: team 0 sends its first player to team 1
// for team 1's first player and a future first-round pick.
const makeTrade = async () => {
	const [p0, p1] = await idb.cache.players.getAll();
	await processTrade([0, 1], [[p0!.pid], [p1!.pid]], [[], [1]]);
	const events = await idb.cache.events.getAll();
	const event = events.at(-1)!;
	return { p0: p0!, p1: p1!, eid: event.eid as number };
};

beforeEach(async () => {
	resetG();
	g.setWithoutSavingToDB("numTeams", 3);
	g.setWithoutSavingToDB("numActiveTeams", 3);
	g.setWithoutSavingToDB("godMode", true);

	await resetCache({
		players: [
			player.generate(0, 25, 2015, true, DEFAULT_LEVEL),
			player.generate(1, 25, 2015, true, DEFAULT_LEVEL),
			player.generate(1, 25, 2015, true, DEFAULT_LEVEL),
		],
		draftPicks: [
			{
				dpid: 1,
				tid: 1,
				originalTid: 1,
				round: 1,
				pick: 0,
				season: g.get("season") + 1,
			} as any,
		],
	});
});

describe("revertTrade", () => {
	test("puts every asset back and records the reversal as a trade", async () => {
		const { p0, p1, eid } = await makeTrade();

		// The trade landed.
		assert.strictEqual((await idb.cache.players.get(p0.pid))!.tid, 1);
		assert.strictEqual((await idb.cache.players.get(p1.pid))!.tid, 0);
		assert.strictEqual((await idb.cache.draftPicks.get(1))!.tid, 0);

		const error = await revertTrade(eid);
		assert.strictEqual(error, undefined);

		// Everything is home again.
		assert.strictEqual((await idb.cache.players.get(p0.pid))!.tid, 0);
		assert.strictEqual((await idb.cache.players.get(p1.pid))!.tid, 1);
		assert.strictEqual((await idb.cache.draftPicks.get(1))!.tid, 1);

		// The original event stays - deleting or editing events is exactly what
		// a synced league cannot reconcile - and the reversal is its own trade
		// event, so every device's log tells the same story.
		const events = await idb.cache.events.getAll();
		const tradeEvents = events.filter((e) => e.type === "trade");
		assert.strictEqual(tradeEvents.length, 2);
	});

	test("the revert does not charge the mood penalty for trading players", async () => {
		await idb.cache.teamSeasons.add({
			tid: 1,
			season: g.get("season"),
			numPlayersTradedAway: 0,
		} as any);

		const { eid } = await makeTrade();
		const after = (await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[g.get("season"), 1],
		))!.numPlayersTradedAway;
		assert.isAbove(after, 0, "the trade itself is charged");

		await revertTrade(eid);
		const afterRevert = (await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[g.get("season"), 1],
		))!.numPlayersTradedAway;
		assert.strictEqual(afterRevert, after, "the revert is not");
	});

	test("a reverted trade cannot be reverted again", async () => {
		const { eid } = await makeTrade();
		assert.strictEqual(await revertTrade(eid), undefined);

		const error = await revertTrade(eid);
		assert.isString(error);
	});

	test("refuses once any traded player has moved on", async () => {
		const { p0, eid } = await makeTrade();

		// The player team 1 traded for gets released.
		const p = (await idb.cache.players.get(p0.pid))!;
		p.tid = PLAYER.FREE_AGENT;
		await idb.cache.players.put(p);

		const error = await revertTrade(eid);
		assert.isString(error);

		// And nothing else moved: all-or-nothing, never a half revert.
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.filter((e) => e.type === "trade").length, 1);
	});

	test("refuses once a traded pick has been used", async () => {
		const { eid } = await makeTrade();
		await idb.cache.draftPicks.delete(1);

		const error = await revertTrade(eid);
		assert.isString(error);
	});

	test("requires God Mode", async () => {
		const { eid } = await makeTrade();
		g.setWithoutSavingToDB("godMode", false);

		const error = await revertTrade(eid);
		assert.isString(error);
	});

	test("planTradeRevert builds the exact inverse", async () => {
		const { p0, p1, eid } = await makeTrade();
		const event = (await idb.cache.events.getAll()).find((e) => e.eid === eid)!;
		const plan = await planTradeRevert(event as any);
		assert.notProperty(plan, "error");
		if ("error" in plan) {
			throw new Error("unreachable");
		}
		// Side 0 received p1 and the pick; side 1 received p0. Each gives back
		// what it received.
		assert.deepEqual(plan.pids, [[p1.pid], [p0.pid]]);
		assert.deepEqual(plan.dpids, [[1], []]);
	});
});
