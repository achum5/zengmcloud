import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import {
	type ClassEdge,
	setAiColaOptOuts,
	shouldOptOutOfCola,
	updateColaAfterPlayoffs,
} from "./cola.ts";
import {
	COLA_ALPHA,
	COLA_NUM_LOTTERY_PICKS,
	PLAYER,
} from "../../../common/constants.ts";

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

// ---------------------------------------------------------------------------
// SITTING OUT THE DRAW.
//
// Opting out pays a flat penalty to keep a stockpile and forfeits any shot at
// the top four. Whether that is ever worth doing is a question with an actual
// answer, and these tests hold the rule to it rather than to a threshold
// somebody picked.
//
// The arithmetic is unforgiving. Entering the draw costs you, in expectation,
// 62.5% of your stockpile times your odds of winning; opting out costs 2000
// flat. Opting out only preserves chances at all once that expected burn
// exceeds 2000, and the chances it preserves buy a slightly better draw NEXT
// year - against which you have written off this year's entirely. The best
// case for it anywhere in the parameter space is a stockpile worth about 30%
// of the league pool, and even there the extra future odds are worth under a
// quarter of the odds given up now. So the rule fires only when a lottery pick
// in THIS class is worth almost nothing over the pick you would get anyway.
// ---------------------------------------------------------------------------

// A stockpile at the most favourable share there is - big enough that the
// penalty is small change, not so big that the draw is already a formality.
const SWEET_SPOT = { chances: 40_000, total: 131_000 };

const cls = (lottery: number, fallback: number): ClassEdge => ({
	lottery,
	fallback,
});

const decide = (args: {
	chances?: number;
	total?: number;
	thisClass?: ClassEdge | undefined;
	nextClass?: ClassEdge | undefined;
}) =>
	shouldOptOutOfCola({
		...SWEET_SPOT,
		numLotteryPicks: COLA_NUM_LOTTERY_PICKS,
		// Nothing at the top of this year's class; a real prize in next year's.
		thisClass: cls(51, 50),
		nextClass: cls(60, 50),
		...args,
	});

describe("when an AI front office sits out the COLA lottery", () => {
	test("a class with nothing at the top of it is one to skip", () => {
		assert.isTrue(decide({}));
	});

	// The ordinary case, and the one that has to stay false: this year's
	// lottery is worth about as much as next year's. Overwhelmingly the most
	// common situation, so a rule that got this wrong would have AI teams
	// forfeiting picks every season.
	test("an ordinary class is entered", () => {
		assert.isFalse(decide({ thisClass: cls(60, 50) }));
	});

	// It is the EDGE over the pick you get anyway that matters, not how good
	// the class is. A stacked class whose ninth-best prospect is nearly as good
	// as its best is still one where winning the lottery buys nothing.
	test("a strong class is skipped anyway if winning it buys nothing", () => {
		assert.isFalse(decide({ thisClass: cls(80, 70), nextClass: cls(80, 70) }));
		assert.isTrue(decide({ thisClass: cls(80, 79), nextClass: cls(80, 70) }));
	});

	// The penalty would be most of what it has, so opting out would spend the
	// stockpile in order to save it - and leave next year's draw WORSE.
	test("a team with little banked keeps what it has", () => {
		assert.isFalse(decide({ chances: 3000, total: 131_000 }));
	});

	// Nothing to protect and nothing to lose.
	test("a team with no chances has no decision to make", () => {
		assert.isFalse(decide({ chances: 0 }));
		assert.isFalse(decide({ chances: 0, total: 0 }));
	});

	// A class it cannot read is not a class it should gamble on skipping.
	test("an unreadable class means stay in", () => {
		assert.isFalse(decide({ thisClass: undefined }));
		assert.isFalse(decide({ nextClass: undefined }));
	});

	// THE MEASURED REALITY, pinned so it cannot drift unnoticed. Across sixty
	// simulated seasons the largest stockpile any team in a thirty team league
	// ever built was about a tenth of the pool. At that share the extra odds a
	// preserved stockpile buys next year are worth ~3% of the odds thrown away
	// now, so no draft class variation observed in those sixty seasons - the
	// edge ratio ranged 0.23 to 2.82 - comes anywhere near making it worth it.
	// An AI in a normal league should therefore never opt out, and this is the
	// test that says so out loud.
	test("at the stockpiles a full league actually reaches, never", () => {
		const chances = 10_577;
		const total = 103_118;
		for (const edge of [0.23, 0.5, 1, 2.82]) {
			assert.isFalse(
				shouldOptOutOfCola({
					chances,
					total,
					numLotteryPicks: COLA_NUM_LOTTERY_PICKS,
					thisClass: cls(50 + edge, 50),
					nextClass: cls(51, 50),
				}),
				`opted out on an edge ratio of ${edge}`,
			);
		}
	});
});

// The rule is only half of it - the other half is that an AI team can reach the
// lever at all, which before this it could not: toggleColaOptOut reads userTid
// and nothing else.
describe("setAiColaOptOuts", () => {
	const USER_TID = 0;
	const AI_TID = 1;

	const build = async ({
		chances,
		thisTop,
	}: {
		chances: number;
		thisTop: number;
	}) => {
		resetG();
		g.setWithoutSavingToDB("draftType", "cola");
		g.setWithoutSavingToDB("smartAiFrontOffice", true);
		g.setWithoutSavingToDB("userTids", [USER_TID]);

		const season = g.get("season");
		let pid = 0;
		const players: any[] = [];
		// Eight prospects per class, so both the lottery band and the band
		// behind it are readable. This year's top is worth `thisTop`; next
		// year's is a genuine prize.
		const addClass = (year: number, top: number) => {
			for (let i = 0; i < COLA_NUM_LOTTERY_PICKS; i++) {
				for (const value of [top, 50]) {
					players.push({
						pid: pid++,
						tid: PLAYER.UNDRAFTED,
						draft: { year },
						value,
					});
				}
			}
		};
		addClass(season, thisTop);
		addClass(season + 1, 60);

		await resetCache({
			teams: [
				// Both sitting on the same stockpile, so the only difference
				// between them is which one the human is running.
				{ tid: USER_TID, draftLottery: { type: "cola", chances } },
				{ tid: AI_TID, draftLottery: { type: "cola", chances } },
				{
					tid: 2,
					draftLottery: { type: "cola", chances: 131_000 - 2 * chances },
				},
			] as any,
			players,
		});
	};

	const optedOut = async (tid: number) => {
		const draftLottery = (await idb.cache.teams.get(tid))!.draftLottery;
		return draftLottery?.type === "cola" && draftLottery.optOut === true;
	};

	test("an AI team can sit out a draw the user's team is left to decide on", async () => {
		await build({ chances: 40_000, thisTop: 51 });
		await setAiColaOptOuts();
		assert.isTrue(await optedOut(AI_TID));
		assert.isFalse(
			await optedOut(USER_TID),
			"the opt out button belongs to the user",
		);
	});

	test("nobody sits out a draft worth entering", async () => {
		await build({ chances: 40_000, thisTop: 60 });
		await setAiColaOptOuts();
		assert.isFalse(await optedOut(AI_TID));
	});

	// Same league, same class, smart mode off: the AI does not touch the lever.
	test("only smart front offices use it", async () => {
		await build({ chances: 40_000, thisTop: 51 });
		g.setWithoutSavingToDB("smartAiFrontOffice", false);
		await setAiColaOptOuts();
		assert.isFalse(await optedOut(AI_TID));
	});

	// And it stays a COLA-only mechanic.
	test("not under any other draft type", async () => {
		await build({ chances: 40_000, thisTop: 51 });
		g.setWithoutSavingToDB("draftType", "nba2019");
		await setAiColaOptOuts();
		assert.isFalse(await optedOut(AI_TID));
	});
});
