import { assert, describe, test } from "vitest";
import {
	ageMultiplier,
	CONTRACT_FACTOR,
	PICK_MULTIPLIER,
	tierForLegacyStrategy,
} from "./tierValuation.ts";
import type { TradeTier } from "../trade/tradePosture.ts";

// Selling to buying.
const TIERS: TradeTier[] = ["teardown", "seller", "fringe", "buyer", "allIn"];

describe("turning the smart front office off changes nothing", () => {
	// The middle of every table is the old rebuilding/contending numbers to the
	// decimal, so a league that never wanted this gets back exactly what BBGM
	// always did.
	test("the legacy flag maps onto the rows that reproduce it", () => {
		assert.strictEqual(tierForLegacyStrategy("rebuilding"), "seller");
		assert.strictEqual(tierForLegacyStrategy("contending"), "buyer");
		// Anything else was already treated as contending by the old code.
		assert.strictEqual(tierForLegacyStrategy(""), "buyer");
	});

	test("the old rebuilding numbers, exactly", () => {
		assert.strictEqual(PICK_MULTIPLIER.seller, 1.1);
		assert.strictEqual(CONTRACT_FACTOR.seller, 2);
		for (const [age, expected] of [
			[19, 1.075],
			[20, 1.05],
			[21, 1.0375],
			[22, 1.025],
			[23, 1.0125],
			[27, 0.975],
			[28, 0.95],
			[29, 0.9],
		] as const) {
			assert.strictEqual(ageMultiplier("seller", age), expected, `${age}`);
		}
		// The old code left the prime alone.
		for (const age of [24, 25, 26]) {
			assert.strictEqual(ageMultiplier("seller", age), 1, `${age}`);
		}
	});

	test("the old contending numbers, exactly", () => {
		assert.strictEqual(PICK_MULTIPLIER.buyer, 0.825);
		assert.strictEqual(CONTRACT_FACTOR.buyer, 0.5);
		for (const [age, expected] of [
			[19, 0.8],
			[20, 0.825],
			[21, 0.85],
			[22, 0.875],
			[23, 0.925],
			[24, 0.95],
		] as const) {
			assert.strictEqual(ageMultiplier("buyer", age), expected, `${age}`);
		}
		for (const age of [25, 30, 35]) {
			assert.strictEqual(ageMultiplier("buyer", age), 1, `${age}`);
		}
	});

	// The old code clamped both ends: `age <= 19` and `age >= 29` for rebuilding.
	test("the old clamps at both ends are preserved", () => {
		assert.strictEqual(ageMultiplier("seller", 17), 1.075);
		assert.strictEqual(ageMultiplier("seller", 35), 0.9);
		assert.strictEqual(ageMultiplier("buyer", 17), 0.8);
	});
});

describe("the two ends the old flag could not express", () => {
	// A teardown wants youth and picks MORE than a measured seller; a title
	// favourite wants them less than an ordinary buyer. Two values cannot say
	// that, so both used to be rounded to the middle.
	test("wanting picks runs monotonically from teardown to all-in", () => {
		for (let i = 1; i < TIERS.length; i++) {
			assert.isBelow(
				PICK_MULTIPLIER[TIERS[i]!],
				PICK_MULTIPLIER[TIERS[i - 1]!],
				TIERS[i],
			);
		}
	});

	test("caring what you are committed to runs the same way", () => {
		for (let i = 1; i < TIERS.length; i++) {
			assert.isBelow(
				CONTRACT_FACTOR[TIERS[i]!],
				CONTRACT_FACTOR[TIERS[i - 1]!],
				TIERS[i],
			);
		}
	});

	test("wanting a 20-year-old runs the same way", () => {
		for (let i = 1; i < TIERS.length; i++) {
			assert.isBelow(
				ageMultiplier(TIERS[i]!, 20),
				ageMultiplier(TIERS[i - 1]!, 20),
				TIERS[i],
			);
		}
	});

	// A .500 team is not choosing yet, which is the honest answer the old flag
	// had to round away.
	test("a fringe team has no lean at all", () => {
		assert.strictEqual(PICK_MULTIPLIER.fringe, 1);
		for (const age of [18, 19, 24, 29, 35]) {
			assert.strictEqual(ageMultiplier("fringe", age), 1, `${age}`);
		}
	});

	test("a teardown pays a premium for youth and a discount for a prime veteran", () => {
		assert.isAbove(ageMultiplier("teardown", 20), 1);
		assert.isBelow(ageMultiplier("teardown", 29), 1);
	});

	test("an all-in team discounts anyone who cannot help this season", () => {
		for (const age of [19, 20, 21, 22, 23, 24]) {
			assert.isBelow(ageMultiplier("allIn", age), 1, `${age}`);
		}
	});
});

describe("the table can never produce a nonsense price", () => {
	test("every multiplier is positive and sane", () => {
		for (const tier of TIERS) {
			assert.isAbove(PICK_MULTIPLIER[tier], 0);
			assert.isBelow(PICK_MULTIPLIER[tier], 2);
			assert.isAbove(CONTRACT_FACTOR[tier], 0);
			for (let age = 15; age <= 45; age++) {
				const m = ageMultiplier(tier, age);
				assert.isAbove(m, 0, `${tier}/${age}`);
				assert.isBelow(m, 2, `${tier}/${age}`);
			}
		}
	});

	test("a missing or absurd age is worth face value, not zero", () => {
		for (const tier of TIERS) {
			for (const age of [Number.NaN, Number.POSITIVE_INFINITY]) {
				assert.strictEqual(ageMultiplier(tier, age), 1, tier);
			}
		}
	});
});
