import { assert, test } from "vitest";
import {
	fairToSeller,
	marketSettings,
	withAlternatives,
	type SmartDeal,
} from "./market.ts";
import type { SmartView } from "./worth.ts";

// THE BUYER'S NEXT BEST (marketSettings.buyerAlternatives): a seller keeps its
// share of what a deal makes past the buyer's best deal elsewhere, not of all
// the buyer gains.

test("the seller's share is of what the deal makes past the buyer's next best", () => {
	// The buyer gains 0.08 and the seller 0.02: a quarter, short of a third.
	assert.isFalse(fairToSeller(0.02, 0.08));
	// With another seller's deal worth 0.05 to it, the deal makes 0.05 past
	// that for both, and the seller has two fifths of it.
	assert.isTrue(fairToSeller(0.02, 0.08, 0.05));
	// Only another deal as good leaves nothing to share: what beats keeping
	// the player is enough.
	assert.isTrue(fairToSeller(0.002, 0.08, 0.08));
	// An upgrade nobody else can sell it: the share holds.
	assert.isFalse(fairToSeller(0.02, 0.2, 0.05));
});

const deal = (
	seller: number,
	buyer: number,
	sellerGain: number,
	bar = 0.001,
): SmartDeal => ({
	seller,
	buyer,
	out: { players: [], picks: [] },
	offer: { players: [], picks: [] },
	sellerGain,
	buyerGain: Number.NaN,
	bar,
});

test("a buyer's next best is its best deal with another seller, that seller taking what it must", async () => {
	const deals = [
		deal(1, 9, 0.05),
		deal(2, 9, 0.04, 0.01),
		deal(3, 9, 0.03),
		deal(3, 8, 0.02),
	];
	// What each deal's offer is worth to its buyer.
	const buyerGains = [0.1, 0.06, 0.02, 0.07];
	const view = {
		dealWorth: async (buyer: number, seller: number) =>
			buyerGains[
				deals.findIndex((d) => d.buyer === buyer && d.seller === seller)
			]!,
	} as unknown as SmartView;
	const before = marketSettings.buyerAlternatives;
	marketSettings.buyerAlternatives = true;
	try {
		const open = await withAlternatives(view, deals);
		// Each deal for both, less what its seller must have.
		const free = [0.15 - 0.001, 0.1 - 0.01, 0.05 - 0.001];
		assert.closeTo(open[0]!.buyerAlt!, Math.max(free[1]!, free[2]!), 1e-9);
		assert.closeTo(open[1]!.buyerAlt!, Math.max(free[0]!, free[2]!), 1e-9);
		assert.closeTo(open[2]!.buyerAlt!, Math.max(free[0]!, free[1]!), 1e-9);
		// A buyer with one deal has nothing else.
		assert.strictEqual(open[3]!.buyerAlt, 0);
	} finally {
		marketSettings.buyerAlternatives = before;
	}
});
