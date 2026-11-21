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
		// Each deal for both, less what its seller must have: its bar, or the
		// smallest gain worth a deal.
		const must = (bar: number) => Math.max(bar, marketSettings.minGain);
		const free = [0.15 - must(0.001), 0.1 - must(0.01), 0.05 - must(0.001)];
		assert.closeTo(open[0]!.buyerAlt!, Math.max(free[1]!, free[2]!), 1e-9);
		assert.closeTo(open[1]!.buyerAlt!, Math.max(free[0]!, free[2]!), 1e-9);
		assert.closeTo(open[2]!.buyerAlt!, Math.max(free[0]!, free[1]!), 1e-9);
		// A buyer with one deal has nothing else.
		assert.strictEqual(open[3]!.buyerAlt, 0);
	} finally {
		marketSettings.buyerAlternatives = before;
	}
});

// A DEAL'S SURE GAIN (smartSettings.premiumRealized): what it gains in the
// market's prices counts toward its margin only at the share it can be sure
// of getting; what it gives up in price counts in full.
test("a gain in price counts at the share a team can be sure of, a cost in full", async () => {
	const { SmartView, smartSettings } = await import("./worth.ts");
	const sure = (parts: {
		goal: number;
		got: number;
		given: number;
		flip: number;
	}) =>
		SmartView.prototype.sureWorthNow.call(
			{ dealPartsNow: () => parts } as unknown as SmartView,
			0,
			1,
			{ players: [], picks: [] },
			{ players: [], picks: [] },
			{ players: [], picks: [] },
		);
	const k = smartSettings.premiumRealized;
	// Golden State's Osman for a 2022 first: -0.0137 by its goal, the pick
	// 0.175 past its use, Osman 0.118.
	assert.closeTo(
		sure({ goal: -0.0137, got: 0.175, given: 0.1176, flip: 0 }),
		-0.0137 + k * (0.175 - 0.1176),
		1e-12,
	);
	assert.isBelow(
		sure({ goal: -0.0137, got: 0.175, given: 0.1176, flip: 0 }),
		0.001,
	);
	// A buyer paying in picks past their use: the cost in full.
	assert.closeTo(
		sure({ goal: 0.2, got: 0, given: 0.12, flip: 0.01 }),
		0.2 - 0.12 - 0.01,
		1e-12,
	);
});

// A PACKAGE NOBODY COULD TAKE WHOLE (SmartView.packagePriceNow): priced piece
// by piece, so a first sent with salary filler still costs its team its price.
test("a package is priced piece by piece, a piece nobody would take at its use", async () => {
	const { SmartView } = await import("./worth.ts");
	const filler = { pid: 1 } as any;
	const scrub = { pid: 2 } as any;
	const first = { dpid: 9 } as any;
	const prices = new Map<string, number>([
		["1|", 0.0003],
		["2|", -Infinity],
		["|9", 0.31],
	]);
	const uses = new Map<string, number>([["2|", 0.001]]);
	const key = (x: any) =>
		`${x.players.map((p: any) => p.pid).join(",")}|${x.picks.map((dp: any) => dp.dpid).join(",")}`;
	const stub = {
		piecePriceNow: (x: any) => prices.get(key(x)) ?? -Infinity,
		useOfNow: (_tid: number, x: any) => uses.get(key(x)) ?? 0,
	} as unknown as InstanceType<typeof SmartView>;
	const price = SmartView.prototype.packagePriceNow.call(
		stub,
		0,
		{ players: [filler, scrub], picks: [first] },
		[],
		{ players: [], picks: [] },
	);
	assert.closeTo(price, 0.0003 + 0.001 + 0.31, 1e-12);
	// One piece nobody would take: its use, nothing past it to hold out for.
	assert.strictEqual(
		SmartView.prototype.packagePriceNow.call(
			stub,
			0,
			{ players: [scrub], picks: [] },
			[],
			{ players: [], picks: [] },
		),
		0.001,
	);
});
