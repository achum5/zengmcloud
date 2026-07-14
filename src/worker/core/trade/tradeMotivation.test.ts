import { assert, describe, test } from "vitest";
import {
	contenderDowngradesBest,
	DEADLINE_WINDOW_DAYS,
	deadlineRampMultiplier,
	isBadRental,
	isPureDowngrade,
	isSelling,
	isStarAcquisition,
	partnerWeight,
	sellerAcquiresVet,
	shouldDumpExpiring,
	wasTradedThisSeason,
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

describe("sellerAcquiresVet", () => {
	test("the Booker case: a teardown acquiring a 33yo star with no picks back", () => {
		assert.strictEqual(
			sellerAcquiresVet({
				acquirerTier: "teardown",
				age: 33,
				value: 55,
				receivesPicks: false,
			}),
			true,
		);
		assert.strictEqual(
			sellerAcquiresVet({
				acquirerTier: "seller",
				age: 37,
				value: 45,
				receivesPicks: false,
			}),
			true,
		);
	});

	test("getting PAID in picks to absorb a vet is a legitimate rebuild move", () => {
		assert.strictEqual(
			sellerAcquiresVet({
				acquirerTier: "teardown",
				age: 33,
				value: 55,
				receivesPicks: true,
			}),
			false,
		);
	});

	test("cheap veteran filler is fine, and contenders may buy vets freely", () => {
		assert.strictEqual(
			sellerAcquiresVet({
				acquirerTier: "seller",
				age: 34,
				value: 25,
				receivesPicks: false,
			}),
			false,
		);
		assert.strictEqual(
			sellerAcquiresVet({
				acquirerTier: "allIn",
				age: 33,
				value: 60,
				receivesPicks: false,
			}),
			false,
		);
	});

	test("a young player is never a timeline violation for a rebuilder", () => {
		assert.strictEqual(
			sellerAcquiresVet({
				acquirerTier: "teardown",
				age: 23,
				value: 60,
				receivesPicks: false,
			}),
			false,
		);
	});
});

describe("contenderDowngradesBest", () => {
	test("a contender swapping its best player for a clearly worse one is blocked", () => {
		// The OKC case: 60-22 ships a 56ovr rotation piece for prospects.
		assert.strictEqual(
			contenderDowngradesBest({
				acquirerTier: "allIn",
				bestGivenValue: 58,
				bestReceivedValue: 42,
			}),
			true,
		);
	});

	test("consolidation and star hunts pass (best player improves)", () => {
		assert.strictEqual(
			contenderDowngradesBest({
				acquirerTier: "allIn",
				bestGivenValue: 52,
				bestReceivedValue: 64,
			}),
			false,
		);
	});

	test("spending picks/depth (nothing good leaves) passes", () => {
		assert.strictEqual(
			contenderDowngradesBest({
				acquirerTier: "buyer",
				bestGivenValue: 40,
				bestReceivedValue: 0,
			}),
			false,
		);
	});

	test("sellers are free to downgrade the present (that's the point)", () => {
		assert.strictEqual(
			contenderDowngradesBest({
				acquirerTier: "seller",
				bestGivenValue: 60,
				bestReceivedValue: 30,
			}),
			false,
		);
	});
});

describe("wasTradedThisSeason", () => {
	test("last transaction is a same-season trade → cooldown", () => {
		assert.strictEqual(
			wasTradedThisSeason(
				[
					{ type: "draft", season: 2027 },
					{ type: "trade", season: 2029 },
				],
				2029,
			),
			true,
		);
	});

	test("a trade in a PRIOR season doesn't block", () => {
		assert.strictEqual(
			wasTradedThisSeason([{ type: "trade", season: 2028 }], 2029),
			false,
		);
	});

	test("signed after the trade → free to move again; no history → free", () => {
		assert.strictEqual(
			wasTradedThisSeason(
				[
					{ type: "trade", season: 2029 },
					{ type: "freeAgent", season: 2029 },
				],
				2029,
			),
			false,
		);
		assert.strictEqual(wasTradedThisSeason(undefined, 2029), false);
		assert.strictEqual(wasTradedThisSeason([], 2029), false);
	});
});

describe("isStarAcquisition", () => {
	const starOvr = 60;

	test("a genuine star landing on a win-now contender is a blockbuster", () => {
		assert.strictEqual(
			isStarAcquisition({ bestReceivedOvr: 63, acquirerTier: "allIn", starOvr }),
			true,
		);
		assert.strictEqual(
			isStarAcquisition({ bestReceivedOvr: 61, acquirerTier: "buyer", starOvr }),
			true,
		);
	});

	test("a non-contender loading up on a star is NOT a blockbuster overpay", () => {
		// A rebuilder/teardown should never empty its future for a win-now star.
		assert.strictEqual(
			isStarAcquisition({ bestReceivedOvr: 65, acquirerTier: "teardown", starOvr }),
			false,
		);
		assert.strictEqual(
			isStarAcquisition({ bestReceivedOvr: 65, acquirerTier: "seller", starOvr }),
			false,
		);
		assert.strictEqual(
			isStarAcquisition({ bestReceivedOvr: 65, acquirerTier: "fringe", starOvr }),
			false,
		);
	});

	test("a merely-good player (below the star bar) is not a blockbuster", () => {
		assert.strictEqual(
			isStarAcquisition({ bestReceivedOvr: 58, acquirerTier: "allIn", starOvr }),
			false,
		);
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
