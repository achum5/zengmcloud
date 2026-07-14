import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import {
	analyzePositions,
	capPosture,
	classifyTier,
	getTradePostureReport,
	lookingForFromPosture,
	posBucket,
	selectBuildingBlocks,
	selectShopVeterans,
	tierToStrategy,
	type PosturePlayer,
} from "./tradePosture.ts";

describe("tierToStrategy", () => {
	test("maps our tiers onto the valuation's strategy buckets", () => {
		assert.strictEqual(tierToStrategy("allIn"), "contending");
		assert.strictEqual(tierToStrategy("buyer"), "contending");
		assert.strictEqual(tierToStrategy("seller"), "rebuilding");
		assert.strictEqual(tierToStrategy("teardown"), "rebuilding");
		assert.strictEqual(tierToStrategy("fringe"), "");
	});
});

describe("posBucket", () => {
	test("maps fine positions to G/F/C slots (PF is a big, GF a wing)", () => {
		for (const pos of ["PG", "SG", "G"]) {
			assert.strictEqual(posBucket(pos), "G", pos);
		}
		for (const pos of ["SF", "F", "GF"]) {
			assert.strictEqual(posBucket(pos), "F", pos);
		}
		for (const pos of ["C", "FC", "PF"]) {
			assert.strictEqual(posBucket(pos), "C", pos);
		}
	});

	test("unrecognized positions fall through to forward", () => {
		assert.strictEqual(posBucket("??"), "F");
	});
});

describe("classifyTier", () => {
	const base = {
		winp: 0.5,
		ovrRankPct: 0.5,
		avgAge: 26,
		youngCoreCount: 0,
		hasFoundation: false,
	};

	test("strong + aging core → all-in", () => {
		assert.strictEqual(
			classifyTier({ ...base, winp: 0.8, ovrRankPct: 0, avgAge: 28 }),
			"allIn",
		);
	});

	test("strong but young core → patient buyer (keeps its youth)", () => {
		assert.strictEqual(
			classifyTier({
				...base,
				winp: 0.8,
				ovrRankPct: 0,
				avgAge: 24,
				youngCoreCount: 3,
			}),
			"buyer",
		);
	});

	test("solidly above average → buyer", () => {
		assert.strictEqual(
			classifyTier({ ...base, winp: 0.55, ovrRankPct: 0.4 }),
			"buyer",
		);
	});

	test("middling → fringe", () => {
		assert.strictEqual(
			classifyTier({ ...base, winp: 0.45, ovrRankPct: 0.5 }),
			"fringe",
		);
	});

	test("below average → seller", () => {
		assert.strictEqual(
			classifyTier({ ...base, winp: 0.35, ovrRankPct: 0.7 }),
			"seller",
		);
	});

	test("a hopeless team with no foundation → teardown", () => {
		assert.strictEqual(
			classifyTier({ ...base, winp: 0.15, ovrRankPct: 1 }),
			"teardown",
		);
	});

	test("a truly bad, no-core team (≈26 wins) fully tears down, not a half-sell", () => {
		// winp 0.30, worst-third roster, nothing young to build on → no half-measure.
		assert.strictEqual(
			classifyTier({ ...base, winp: 0.3, ovrRankPct: 0.85 }),
			"teardown",
		);
	});

	test("an equally bad team WITH a young cornerstone retools (seller, not teardown)", () => {
		// Same terrible record, but a foundation to build around → it keeps the
		// kid and sells the rest rather than blowing everything up.
		assert.strictEqual(
			classifyTier({
				...base,
				winp: 0.15,
				ovrRankPct: 1,
				hasFoundation: true,
			}),
			"seller",
		);
	});

	test("classification is self-contained — record + strength + core, no external flag", () => {
		// A good record on a strong, aging roster is all-in regardless of any
		// league 'strategy' label; a hot record on a young roster is a buyer.
		assert.strictEqual(
			classifyTier({
				winp: 0.7,
				ovrRankPct: 0.05,
				avgAge: 28,
				youngCoreCount: 0,
				hasFoundation: false,
			}),
			"allIn",
		);
		assert.strictEqual(
			classifyTier({
				winp: 0.7,
				ovrRankPct: 0.05,
				avgAge: 24,
				youngCoreCount: 3,
				hasFoundation: true,
			}),
			"buyer",
		);
	});
});

describe("analyzePositions", () => {
	test("flags weak/empty slots as needs and stacked slots as surpluses", () => {
		const { needs, surpluses, upgradePos } = analyzePositions(
			[
				{ pos: "PG", ovr: 60 }, // G
				{ pos: "SG", ovr: 55 }, // G — second starter-caliber guard
				{ pos: "SF", ovr: 45 }, // F — below the 50 starter bar
				// no centers at all
			],
			50,
		);

		// Center (no one) is the most severe need, then the weak forward.
		assert.deepEqual(needs, [
			{ pos: "C", severity: 50 },
			{ pos: "F", severity: 5 },
		]);
		// Two starter-caliber guards → one surplus guard.
		assert.deepEqual(surpluses, [{ pos: "G", depth: 1 }]);
		// The upgrade slot is a soft, non-surplus spot — the empty center.
		assert.strictEqual(upgradePos, "C");
	});

	test("a solid, balanced roster names no upgrade slot (best available)", () => {
		const { needs, surpluses, upgradePos } = analyzePositions(
			[
				{ pos: "PG", ovr: 65 },
				{ pos: "SF", ovr: 66 },
				{ pos: "C", ovr: 67 },
			],
			50,
		);
		assert.deepEqual(needs, []);
		assert.deepEqual(surpluses, []);
		// Every slot's best is well above the 50+6 bar → no soft spot to chase.
		assert.strictEqual(upgradePos, undefined);
	});

	test("a soft (replacement-level) non-surplus slot IS an upgrade target", () => {
		const { upgradePos } = analyzePositions(
			[
				{ pos: "PG", ovr: 66 },
				{ pos: "SG", ovr: 64 }, // guards are deep/solid (surplus)
				{ pos: "SF", ovr: 53 }, // lone forward, only replacement-level
				{ pos: "C", ovr: 66 },
			],
			50,
		);
		// G is a surplus, C is solid; the replacement-level forward is the target.
		assert.strictEqual(upgradePos, "F");
	});
});

describe("capPosture", () => {
	const caps = {
		salaryCap: 150000,
		luxuryPayroll: 168000,
		minPayroll: 95000,
		salaryCapType: "soft",
	};

	test("a taxpaying seller wants relief and cannot absorb money", () => {
		const c = capPosture({ ...caps, payroll: 180000, tier: "seller" });
		assert.strictEqual(c.overCap, true);
		assert.strictEqual(c.overLuxury, true);
		assert.strictEqual(c.wantsRelief, true);
		assert.strictEqual(c.canAbsorb, false);
	});

	test("a taxpaying CONTENDER does not seek relief", () => {
		const c = capPosture({ ...caps, payroll: 180000, tier: "allIn" });
		assert.strictEqual(c.overLuxury, true);
		assert.strictEqual(c.wantsRelief, false);
	});

	test("room under the cap → can absorb salary", () => {
		const c = capPosture({ ...caps, payroll: 120000, tier: "buyer" });
		assert.strictEqual(c.overCap, false);
		assert.strictEqual(c.canAbsorb, true);
	});

	test("under the spending floor → can absorb salary", () => {
		const c = capPosture({ ...caps, payroll: 80000, tier: "fringe" });
		assert.strictEqual(c.underFloor, true);
		assert.strictEqual(c.canAbsorb, true);
	});

	test("no salary cap → never over the cap, always able to absorb", () => {
		const c = capPosture({
			...caps,
			salaryCapType: "none",
			payroll: 999999,
			tier: "seller",
		});
		assert.strictEqual(c.overCap, false);
		assert.strictEqual(c.canAbsorb, true);
	});
});

describe("selectBuildingBlocks", () => {
	const players: PosturePlayer[] = [
		mkP(1, { age: 22, value: 65 }), // young core
		mkP(2, { age: 30, value: 72 }), // quality veteran
		mkP(3, { age: 30, value: 55 }), // aging role player
		mkP(4, { age: 20, value: 50 }), // young but not good enough
		mkP(5, { age: 26, value: 63 }), // quality prime player (not a graybeard)
	];
	const opts = { coreAge: 27, coreValue: 60, starValue: 70 };

	test("a buyer (young riser) protects every quality player, keeping its core", () => {
		const blocks = selectBuildingBlocks(players, { ...opts, tier: "buyer" });
		assert.deepEqual(blocks.sort(), [1, 2, 5]);
	});

	test("an all-in contender protects only stars — young talent is a trade chip", () => {
		// Only 2 (value 72) clears the star bar; the good young players (65, 63)
		// are available to package for a present-day upgrade.
		const blocks = selectBuildingBlocks(players, { ...opts, tier: "allIn" });
		assert.deepEqual(blocks, [2]);
	});

	test("a mild sell keeps its young-and-prime core, not its graybeards", () => {
		// 1 (22) and 5 (26) are within the prime window; 2 (30) is a graybeard.
		const blocks = selectBuildingBlocks(players, { ...opts, tier: "seller" });
		assert.deepEqual(blocks.sort(), [1, 5]);
	});

	test("even a full teardown keeps its young/prime cornerstones", () => {
		// 1 (22) and 5 (26) are the future to build around; only the 30-year-old
		// is available. A teardown never trades its 26-year-old franchise piece.
		const blocks = selectBuildingBlocks(players, { ...opts, tier: "teardown" });
		assert.deepEqual(blocks.sort(), [1, 5]);
	});
});

describe("selectShopVeterans", () => {
	const players: PosturePlayer[] = [
		mkP(1, { age: 31, value: 60 }), // good vet, should be shopped
		mkP(2, { age: 33, value: 66 }), // better vet, shopped first
		mkP(3, { age: 31, value: 40 }), // too little value for anyone to want
		mkP(4, { age: 23, value: 60 }), // young, not a "wasting away" vet
		mkP(5, { age: 32, value: 70 }), // a protected building block
		mkP(6, { age: 26, value: 61 }), // prime, mid-career
	];
	const opts = { vetAge: 29, teardownAge: 25, minTradeValue: 45 };

	test("only selling teams shop veterans", () => {
		assert.deepEqual(
			selectShopVeterans(players, new Set([5]), { ...opts, tier: "buyer" }),
			[],
		);
	});

	test("a mild sell only cashes in clear veterans (29+)", () => {
		const shop = selectShopVeterans(players, new Set([5]), {
			...opts,
			tier: "seller",
		});
		// 2 (66) before 1 (60); 3 too cheap, 4 too young, 5 protected, 6 not a vet.
		assert.deepEqual(shop, [2, 1]);
	});

	test("a full teardown also moves prime mid-career players (25+)", () => {
		const shop = selectShopVeterans(players, new Set([5]), {
			...opts,
			tier: "teardown",
		});
		// Now 6 (26, value 61) is fair game too; still value-sorted; 4 (23) stays.
		assert.deepEqual(shop, [2, 6, 1]);
	});
});

describe("lookingForFromPosture", () => {
	test("a seller chases youth + picks, position-agnostic", () => {
		const lf = lookingForFromPosture("seller", [{ pos: "G", severity: 9 }], false);
		assert.strictEqual(lf.draftPicks, true);
		assert.strictEqual(lf.prospects, true);
		assert.strictEqual(lf.bestCurrentPlayers, false);
		assert.strictEqual(lf.positions.size, 0);
	});

	test("an all-in team lacking a star hunts the best player anywhere", () => {
		const lf = lookingForFromPosture("allIn", [{ pos: "C", severity: 8 }], true);
		assert.strictEqual(lf.bestCurrentPlayers, true);
		assert.strictEqual(lf.draftPicks, false);
		assert.strictEqual(lf.positions.size, 0);
	});

	test("a buyer targets proven talent at its two biggest needs", () => {
		const lf = lookingForFromPosture(
			"buyer",
			[
				{ pos: "C", severity: 10 },
				{ pos: "G", severity: 5 },
				{ pos: "F", severity: 1 },
			],
			false,
		);
		assert.strictEqual(lf.bestCurrentPlayers, true);
		assert.deepEqual([...lf.positions].sort(), ["C", "G"]);
	});

	test("with no outright hole, a buyer still targets its weakest slot", () => {
		const lf = lookingForFromPosture("buyer", [], false, "G");
		assert.strictEqual(lf.bestCurrentPlayers, true);
		assert.deepEqual([...lf.positions], ["G"]);
	});
});

// --- End-to-end wiring: a tiny 3-team league produces sane franchise outlooks -
describe("getTradePostureReport (integration)", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("season", 2016);
		g.setWithoutSavingToDB("numActiveTeams", 3);
		g.setWithoutSavingToDB("numTeams", 3);
	});

	// A rostered player with the fields the orchestrator reads.
	const player = (
		pid: number,
		tid: number,
		ovr: number,
		age: number,
		value: number,
		pos = "SF",
	) => ({
		pid,
		tid,
		born: { year: 2016 - age, loc: "" },
		ratings: [{ ovr, pot: ovr, pos, fuzz: 0 }],
		value,
		contract: { amount: 10000, exp: 2019 },
		injury: { type: "Healthy", gamesRemaining: 0 },
		// Needed by the players cache index (draft.year + retiredYear).
		draft: { year: 2016 - age - 19, round: 1, pick: 1, tid, originalTid: tid },
		retiredYear: Infinity,
	});

	test("good+old → all-in, good+young → buyer, bad → sells its vets", async () => {
		const teams = [
			{ tid: 0, region: "A", name: "Aces", abbrev: "AAA", strategy: "contending" },
			{ tid: 1, region: "B", name: "Blues", abbrev: "BBB", strategy: "" },
			{ tid: 2, region: "C", name: "Cubs", abbrev: "CCC", strategy: "rebuilding" },
		];
		const players = [
			// Team 0: strong, old veterans.
			...[0, 1, 2, 3, 4].map((i) => player(i, 0, 70, 30, 70, "SF")),
			// Team 1: strong, young studs.
			...[5, 6, 7, 8, 9].map((i) => player(i, 1, 60, 22, 68, "PG")),
			// Team 2: weak, old — a rebuild that must cash in its vets.
			...[10, 11, 12, 13, 14].map((i) => player(i, 2, 42, 31, 55, "C")),
		];
		const teamSeasons = [
			{ rid: 0, season: 2016, tid: 0, won: 60, lost: 10 },
			{ rid: 1, season: 2016, tid: 1, won: 45, lost: 25 },
			{ rid: 2, season: 2016, tid: 2, won: 10, lost: 60 },
		];
		await resetCache({ teams, teamSeasons, players });

		const report = await getTradePostureReport();
		const byTid = new Map(report.map((p) => [p.tid, p]));

		// The stacked veteran juggernaut goes all-in.
		assert.strictEqual(byTid.get(0)!.tier, "allIn");

		// The strong-but-young team stays a buyer and protects its young core.
		assert.strictEqual(byTid.get(1)!.tier, "buyer");
		assert.ok(byTid.get(1)!.buildingBlockPids.length >= 3);

		// The weak team is selling and actively shopping its veterans, not
		// letting them waste away.
		assert.ok(["seller", "teardown"].includes(byTid.get(2)!.tier));
		assert.ok(byTid.get(2)!.shopVeteranPids.length > 0);

		// Everyone got a coherent shopping list.
		for (const p of report) {
			assert.ok(typeof p.aggression === "number");
			assert.ok(p.cap.payroll > 0);
		}
	});
});

// Helper: a slim PosturePlayer with defaults.
function mkP(
	pid: number,
	over: Partial<PosturePlayer>,
): PosturePlayer {
	return {
		pid,
		ovr: 50,
		pot: 50,
		value: 50,
		age: 25,
		pos: "SF",
		contractAmount: 5000,
		contractExp: 2019,
		...over,
	};
}
