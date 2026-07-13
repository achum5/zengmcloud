import { assert, describe, test } from "vitest";
import {
	DEADLINE_WINDOW_DAYS,
	deadlineRampMultiplier,
	isBadRental,
	isPureDowngrade,
	isSelling,
	partnerWeight,
	shouldDumpExpiring,
} from "./tradeMotivation.ts";

describe("deadlineRampMultiplier", () => {
	test("no ramp well before the deadline (or with no deadline known)", () => {
		assert.strictEqual(deadlineRampMultiplier(undefined), 1);
		assert.strictEqual(deadlineRampMultiplier(DEADLINE_WINDOW_DAYS + 5), 1);
	});

	test("ramps up as the deadline nears, peaking at the deadline", () => {
		const atWindow = deadlineRampMultiplier(DEADLINE_WINDOW_DAYS);
		const halfway = deadlineRampMultiplier(DEADLINE_WINDOW_DAYS / 2);
		const atDeadline = deadlineRampMultiplier(0);
		assert.ok(Math.abs(atWindow - 1) < 1e-9, `${atWindow}`);
		assert.ok(halfway > atWindow && halfway < atDeadline);
		assert.ok(atDeadline > 3);
	});
});

describe("shouldDumpExpiring", () => {
	test("a non-contender dumps a walk-year player who won't re-sign", () => {
		assert.strictEqual(
			shouldDumpExpiring({
				isExpiring: true,
				probWillingCurrent: 0.2,
				tier: "seller",
			}),
			true,
		);
	});

	test("a contender keeps him for the run", () => {
		assert.strictEqual(
			shouldDumpExpiring({
				isExpiring: true,
				probWillingCurrent: 0.2,
				tier: "allIn",
			}),
			false,
		);
	});

	test("if he'll happily re-sign, no need to dump", () => {
		assert.strictEqual(
			shouldDumpExpiring({
				isExpiring: true,
				probWillingCurrent: 0.9,
				tier: "seller",
			}),
			false,
		);
	});

	test("a non-expiring player is not a dump candidate", () => {
		assert.strictEqual(
			shouldDumpExpiring({
				isExpiring: false,
				probWillingCurrent: 0.1,
				tier: "teardown",
			}),
			false,
		);
	});
});

describe("isBadRental", () => {
	test("only an all-in contender takes a low-mood expiring rental", () => {
		const base = { isExpiring: true, probWillingAcquirer: 0.2 };
		assert.strictEqual(isBadRental({ ...base, acquirerTier: "allIn" }), false);
		assert.strictEqual(isBadRental({ ...base, acquirerTier: "buyer" }), true);
		assert.strictEqual(isBadRental({ ...base, acquirerTier: "seller" }), true);
	});

	test("a player who WILL re-sign is not a rental — anyone can trade for him", () => {
		assert.strictEqual(
			isBadRental({
				isExpiring: true,
				probWillingAcquirer: 0.85,
				acquirerTier: "buyer",
			}),
			false,
		);
	});

	test("a non-expiring player is never a rental", () => {
		assert.strictEqual(
			isBadRental({
				isExpiring: false,
				probWillingAcquirer: 0.1,
				acquirerTier: "fringe",
			}),
			false,
		);
	});
});

describe("partnerWeight", () => {
	test("opposite ends of the spectrum are the strongest match", () => {
		assert.ok(partnerWeight("allIn", "teardown") > partnerWeight("allIn", "buyer"));
		assert.ok(partnerWeight("seller", "buyer") > partnerWeight("seller", "teardown"));
	});
});

describe("isSelling", () => {
	test("fringe/seller/teardown sell; contenders don't", () => {
		assert.strictEqual(isSelling("teardown"), true);
		assert.strictEqual(isSelling("seller"), true);
		assert.strictEqual(isSelling("fringe"), true);
		assert.strictEqual(isSelling("buyer"), false);
		assert.strictEqual(isSelling("allIn"), false);
	});
});

describe("isPureDowngrade", () => {
	test("the Denver deal: gave a 64-value star, got worse AND older, no picks", () => {
		// Murray (v64, 30) + Holmes (v36, 25) out; Embiid (v56, 33) + Westbrook
		// (v~0, 39) + Beauchamp (v41, 27) in — less talent, older, no picks.
		assert.strictEqual(
			isPureDowngrade({
				givenValue: 100,
				receivedValue: 60,
				givenAge: 28,
				receivedAge: 31,
				receivedPicks: false,
			}),
			true,
		);
	});

	test("less talent but YOUNGER is a fine rebuild-ish move", () => {
		assert.strictEqual(
			isPureDowngrade({
				givenValue: 100,
				receivedValue: 60,
				givenAge: 31,
				receivedAge: 24,
				receivedPicks: false,
			}),
			false,
		);
	});

	test("less talent but you got PICKS back is allowed", () => {
		assert.strictEqual(
			isPureDowngrade({
				givenValue: 100,
				receivedValue: 40,
				givenAge: 28,
				receivedAge: 33,
				receivedPicks: true,
			}),
			false,
		);
	});

	test("gaining talent (a contender loading up) is never a downgrade", () => {
		assert.strictEqual(
			isPureDowngrade({
				givenValue: 60,
				receivedValue: 100,
				givenAge: 24,
				receivedAge: 29,
				receivedPicks: false,
			}),
			false,
		);
	});
});
