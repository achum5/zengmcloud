import { assert, test } from "vitest";
import { clearerGains, takerGains } from "./clearRoom.ts";
import { marketSettings } from "./market.ts";

// CLEARING ROOM, judged as a trade is (marketSettings.margins): each side
// must gain more than its margin of error on what moves.

test("the team clearing room must gain past its best signing by more than its margin", () => {
	// 0.03 past nothing, with a margin of 0.025: enough.
	assert.isTrue(clearerGains(0.03, 0, 0.025));
	// The same gain against a best signing worth 0.01 without trading leaves
	// 0.02, inside the margin: a like-for-like swap, not worth it.
	assert.isFalse(clearerGains(0.03, 0.01, 0.025));
	// With a margin under the smallest gain worth a deal, that still holds.
	assert.isFalse(clearerGains(marketSettings.minGain / 2, 0, 0));
	assert.isTrue(clearerGains(marketSettings.minGain * 2, 0, 0));
});

test("the team taking the salary must gain more than its margin and its share", () => {
	// Its share of 0.06 made: 0.02 is a third.
	assert.isTrue(takerGains(0.02, 0.02, 0.01, 0.04));
	// Inside its margin, however fair the share.
	assert.isFalse(takerGains(0.02, 0.02, 0.025, 0.04));
	// Past its margin, but short of its share.
	assert.isFalse(takerGains(0.012, 0.012, 0.01, 0.1));
	// A gain that is all the price of a pick thrown in, which it can be sure
	// of getting only a part of: a salary taken for a second.
	assert.isFalse(takerGains(0.02, 0.005, 0.01, 0.04));
});
