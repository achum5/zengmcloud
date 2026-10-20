import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { chargeColaOptOuts, updateColaAfterPlayoffs } from "./cola.ts";
import { COLA_ALPHA, COLA_OPT_OUT_PENALTY } from "../../../common/constants.ts";

// COLA chance changes compound (+= alpha, or *= playoff factor), so the
// after-playoffs update must be a no-op if it ever executes twice for the same
// season - a replayed/raced phase change must not double-charge anyone.
describe("updateColaAfterPlayoffs idempotence", () => {
	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("draftType", "cola");
	});

	test("running twice for one season changes chances exactly once", async () => {
		const season = g.get("season");
		await resetCache({
			teams: [
				{ tid: 0, draftLottery: { type: "cola", chances: 1000 } },
				{ tid: 1, draftLottery: { type: "cola", chances: 1000 } },
			] as any,
			teamSeasons: [
				// Missed the playoffs: += COLA_ALPHA
				{ rid: 1, tid: 0, season, playoffRoundsWon: -1 },
				// Champion (4 playoff rounds in the default settings): *= 0
				{
					rid: 2,
					tid: 1,
					season,
					playoffRoundsWon: g.get("numGamesPlayoffSeries", "current").length,
				},
			] as any,
		});

		await updateColaAfterPlayoffs();

		const after1 = [
			(await idb.cache.teams.get(0))!.draftLottery!,
			(await idb.cache.teams.get(1))!.draftLottery!,
		];
		assert.strictEqual((after1[0] as any).chances, 1000 + COLA_ALPHA);
		assert.strictEqual((after1[1] as any).chances, 0);

		// The exact double-run hazard: run it again for the same season.
		await updateColaAfterPlayoffs();

		const after2 = [
			(await idb.cache.teams.get(0))!.draftLottery!,
			(await idb.cache.teams.get(1))!.draftLottery!,
		];
		assert.strictEqual(
			(after2[0] as any).chances,
			1000 + COLA_ALPHA,
			"alpha must not be added twice",
		);
		assert.strictEqual((after2[1] as any).chances, 0);
	});
});

// An opt out is declared before the draw and paid for after it, and the two
// are separate calls because a COLA league can reach draft day without running
// the draw at all - genOrder falls back to "noLottery" when too few teams hold a
// first rounder. A flag left standing after that would sit the team out for
// free, every season, forever.
describe("chargeColaOptOuts", () => {
	test("an opt out is paid for even when no lottery is held", async () => {
		resetG();
		g.setWithoutSavingToDB("draftType", "cola");
		await resetCache({
			teams: [
				{
					tid: 0,
					draftLottery: { type: "cola", chances: 40_000, optOut: true },
				},
				{ tid: 1, draftLottery: { type: "cola", chances: 40_000 } },
			] as any,
		});

		await chargeColaOptOuts();

		const optedOut = (await idb.cache.teams.get(0))!.draftLottery!;
		assert.strictEqual(
			(optedOut as any).chances,
			40_000 - COLA_OPT_OUT_PENALTY,
		);
		assert.isUndefined(
			(optedOut as any).optOut,
			"the flag has to come off, or the team sits out again next year",
		);
		const stayedIn = (await idb.cache.teams.get(1))!.draftLottery!;
		assert.strictEqual((stayedIn as any).chances, 40_000);
	});
});
