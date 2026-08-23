import { assert, describe, test } from "vitest";
import {
	AGE_DRIFT_PER_YEAR,
	colaAdjustedSlot,
	colaLotteryOdds,
	NEUTRAL_ROSTER_AGE,
	projectedSlot,
	projectedSlotShare,
	TIER_SLOT_SHARE,
} from "./futurePickOutlook.ts";
import type { TradeTier } from "./tradePosture.ts";

const TIERS: TradeTier[] = ["teardown", "seller", "fringe", "buyer", "allIn"];
const share = (tier: TradeTier, seasons: number, avgAge?: number) =>
	projectedSlotShare({ tier, avgAge, seasons });

describe("where a franchise's pick is heading", () => {
	// THE READ THIS EXISTS FOR. Both of these used to regress to the same
	// constant, so the AI could not tell a genuinely valuable future first from
	// a worthless one - the one judgement that matters when picks change hands.
	test("a teardown's future first is a much better pick than a contender's", () => {
		for (const seasons of [1, 2, 3]) {
			assert.isBelow(
				share("teardown", seasons),
				share("allIn", seasons),
				`${seasons} seasons out`,
			);
		}
	});

	test("the projection runs in order across the tiers", () => {
		for (const seasons of [0, 1, 3]) {
			for (let i = 1; i < TIERS.length; i++) {
				assert.isAbove(
					share(TIERS[i]!, seasons),
					share(TIERS[i - 1]!, seasons),
					`${TIERS[i]} at ${seasons}`,
				);
			}
		}
	});

	// The bad end of the league is stickier than the good end: tearing down is a
	// choice a team commits to, while staying at 55 wins takes an expensive
	// roster nobody can hold together.
	test("the bad end is further from the middle than the good end", () => {
		assert.isAbove(0.5 - TIER_SLOT_SHARE.teardown, TIER_SLOT_SHARE.allIn - 0.5);
	});
});

describe("roster age is the other half of the read", () => {
	// The classic GM line about a contender's distant pick: that team is old,
	// it will fall apart, that pick will be good.
	test("an old contender's distant pick is better than a young one's", () => {
		const old = share("allIn", 4, NEUTRAL_ROSTER_AGE + 3);
		const young = share("allIn", 4, NEUTRAL_ROSTER_AGE - 3);
		assert.isBelow(old, young, "older should mean a higher pick");
	});

	test("a young rebuild's distant pick is worse than an old one's", () => {
		const young = share("teardown", 4, NEUTRAL_ROSTER_AGE - 4);
		const old = share("teardown", 4, NEUTRAL_ROSTER_AGE + 4);
		assert.isAbove(young, old);
	});

	// Age is about where a roster is GOING, so it barely matters next year and
	// matters a lot four years out.
	test("age hardly moves a pick one year out", () => {
		const nextYear = Math.abs(
			share("buyer", 1, NEUTRAL_ROSTER_AGE + 4) - share("buyer", 1),
		);
		const farOut = Math.abs(
			share("buyer", 4, NEUTRAL_ROSTER_AGE + 4) - share("buyer", 4),
		);
		assert.isBelow(nextYear, farOut);
	});

	test("a neutral-aged roster drifts nowhere", () => {
		for (const tier of TIERS) {
			assert.approximately(
				share(tier, 3, NEUTRAL_ROSTER_AGE),
				share(tier, 3),
				1e-9,
				tier,
			);
		}
	});

	test("an absurd age cannot take over the projection", () => {
		for (const age of [0, 99, Number.NaN, undefined]) {
			for (const tier of TIERS) {
				const s = share(tier, 5, age);
				assert.isAtLeast(s, 0, `${tier}/${age}`);
				assert.isAtMost(s, 1, `${tier}/${age}`);
			}
		}
		// The drift itself is bounded well short of flipping a tier's identity.
		assert.isBelow(6 * AGE_DRIFT_PER_YEAR, 0.25);
	});
});

describe("nobody knows anything four years out", () => {
	test("every tier converges toward the middle as the horizon grows", () => {
		for (const tier of ["teardown", "allIn"] as const) {
			let previous = Math.abs(share(tier, 0) - 0.5);
			for (const seasons of [1, 2, 3, 4, 5]) {
				const distance = Math.abs(share(tier, seasons) - 0.5);
				assert.isAtMost(distance, previous + 1e-9, `${tier} at ${seasons}`);
				previous = distance;
			}
		}
	});

	// But converging is not surrendering: a teardown's pick five years out is
	// still worth more than a contender's five years out.
	test("it never converges all the way", () => {
		assert.isBelow(share("teardown", 5), share("allIn", 5) - 0.02);
	});
});

describe("turning it into a pick number", () => {
	test("the round's own size decides the spacing", () => {
		for (const numPicksPerRound of [4, 12, 30, 40]) {
			for (const tier of TIERS) {
				const slot = projectedSlot({
					tier,
					avgAge: undefined,
					seasons: 2,
					numPicksPerRound,
				});
				assert.isAtLeast(slot, 1, `${tier}/${numPicksPerRound}`);
				assert.isAtMost(slot, numPicksPerRound, `${tier}/${numPicksPerRound}`);
				assert.strictEqual(slot, Math.round(slot));
			}
		}
	});

	test("a league with no picks in a round does not produce pick zero", () => {
		for (const numPicksPerRound of [0, -1, Number.NaN]) {
			assert.strictEqual(
				projectedSlot({
					tier: "fringe",
					avgAge: undefined,
					seasons: 1,
					numPicksPerRound,
				}),
				1,
			);
		}
	});

	test("a teardown really does land near the top of a 30-team round", () => {
		const slot = projectedSlot({
			tier: "teardown",
			avgAge: NEUTRAL_ROSTER_AGE,
			seasons: 1,
			numPicksPerRound: 30,
		});
		assert.isAtMost(slot, 8, `got ${slot}`);
	});

	test("and a title favourite lands near the bottom", () => {
		const slot = projectedSlot({
			tier: "allIn",
			avgAge: NEUTRAL_ROSTER_AGE,
			seasons: 1,
			numPicksPerRound: 30,
		});
		assert.isAtLeast(slot, 18, `got ${slot}`);
	});

	test("a pick in the past is treated as one this season", () => {
		assert.strictEqual(
			projectedSlot({
				tier: "seller",
				avgAge: 27,
				seasons: -3,
				numPicksPerRound: 30,
			}),
			projectedSlot({
				tier: "seller",
				avgAge: 27,
				seasons: 0,
				numPicksPerRound: 30,
			}),
		);
	});
});

describe("a pick under Carry-Over Lottery Allocation", () => {
	const NUM_PICKS = 30;
	const LOTTERY = 4;
	const base = {
		numLotteryPicks: LOTTERY,
		numPicksPerRound: NUM_PICKS,
		lotteryEligible: true,
	};

	test("no stockpile is no lottery, a dominant one is near certain", () => {
		assert.strictEqual(
			colaLotteryOdds({ chancesShare: 0, numLotteryPicks: LOTTERY }),
			0,
		);
		assert.isAbove(
			colaLotteryOdds({ chancesShare: 0.9, numLotteryPicks: LOTTERY }),
			0.99,
		);
		assert.isAbove(
			colaLotteryOdds({ chancesShare: 0.3, numLotteryPicks: LOTTERY }),
			colaLotteryOdds({ chancesShare: 0.1, numLotteryPicks: LOTTERY }),
		);
	});

	// The whole point of the system: two teams with the same record can be in
	// completely different positions depending on what they have banked.
	test("the same record is worth more with chances banked", () => {
		const stocked = colaAdjustedSlot({
			...base,
			recordSlot: 12,
			chancesShare: 0.35,
		});
		const spent = colaAdjustedSlot({
			...base,
			recordSlot: 12,
			chancesShare: 0,
		});
		assert.isBelow(stocked, spent);
	});

	// A team that just cashed in has nothing left, so its pick is worth only
	// what its record says - and not even that, since the lottery winners all
	// pick ahead of it.
	test("missing the lottery means everyone who won it picks first", () => {
		assert.strictEqual(
			colaAdjustedSlot({ ...base, recordSlot: 1, chancesShare: 0 }),
			LOTTERY + 1,
		);
		assert.strictEqual(
			colaAdjustedSlot({ ...base, recordSlot: 20, chancesShare: 0 }),
			20,
		);
	});

	// THE RULE THAT CATCHES PEOPLE OUT. A first that has changed hands is
	// excluded from the lottery outright, so the worst team's pick stops being
	// a lottery ticket the moment you trade for it.
	test("a pick that has changed hands cannot win the lottery", () => {
		const held = colaAdjustedSlot({
			...base,
			recordSlot: 2,
			chancesShare: 0.4,
		});
		const acquired = colaAdjustedSlot({
			...base,
			recordSlot: 2,
			chancesShare: 0.4,
			lotteryEligible: false,
		});
		assert.isBelow(held, acquired);
		assert.strictEqual(acquired, LOTTERY + 1);
	});

	test("a late pick is unaffected either way - it was never winning", () => {
		for (const lotteryEligible of [true, false]) {
			assert.strictEqual(
				colaAdjustedSlot({
					...base,
					recordSlot: 28,
					chancesShare: 0,
					lotteryEligible,
				}),
				28,
			);
		}
	});

	test("nothing ever projects outside the round", () => {
		for (const recordSlot of [1, 15, 30]) {
			for (const chancesShare of [0, 0.5, 1]) {
				const slot = colaAdjustedSlot({ ...base, recordSlot, chancesShare });
				assert.isAtLeast(slot, 1);
				assert.isAtMost(slot, NUM_PICKS);
			}
		}
	});

	// A tiny round cannot have four lottery picks ahead of everyone.
	test("a round smaller than the lottery does not invert", () => {
		const slot = colaAdjustedSlot({
			recordSlot: 2,
			chancesShare: 0,
			lotteryEligible: false,
			numLotteryPicks: LOTTERY,
			numPicksPerRound: 3,
		});
		assert.isAtMost(slot, 3);
	});
});
