import { assert, describe, test } from "vitest";
import { getGameSpread } from "./getGameSpread.ts";
import { defaultGameAttributes } from "./defaultGameAttributes.ts";

// Keep the game-length multiplier at 1 by using the default period settings, so
// these assertions test the OVR/home-court part of the formula.
const base = {
	homeCourtAdvantage: 1,
	numPeriods: defaultGameAttributes.numPeriods,
	quarterLength: defaultGameAttributes.quarterLength,
};

describe("getGameSpread", () => {
	test("the better home team is favored (positive spread)", () => {
		const spread = getGameSpread({
			...base,
			ovr0: 60,
			ovr1: 50,
			neutralSite: false,
		})!;
		assert.ok(spread > 0, `${spread}`);
	});

	test("home-court advantage makes an evenly-matched home team a favorite", () => {
		const spread = getGameSpread({
			...base,
			ovr0: 50,
			ovr1: 50,
			neutralSite: false,
		})!;
		assert.ok(spread > 0, `${spread}`);
	});

	test("at a neutral site, evenly-matched teams are a pick'em", () => {
		const spread = getGameSpread({
			...base,
			ovr0: 50,
			ovr1: 50,
			neutralSite: true,
		})!;
		assert.strictEqual(spread, 0);
	});

	test("a much better away team is favored (negative spread) even at a neutral site", () => {
		const spread = getGameSpread({
			...base,
			ovr0: 40,
			ovr1: 70,
			neutralSite: true,
		})!;
		assert.ok(spread < 0, `${spread}`);
	});

	test("dropping home court lowers the home team's spread", () => {
		const home = { ovr0: 60, ovr1: 50 };
		const withHca = getGameSpread({ ...base, ...home, neutralSite: false })!;
		const neutral = getGameSpread({ ...base, ...home, neutralSite: true })!;
		assert.ok(withHca > neutral, `${withHca} vs ${neutral}`);
	});

	test("undefined when a team OVR is missing (legacy games)", () => {
		assert.strictEqual(
			getGameSpread({ ...base, ovr0: undefined, ovr1: 50, neutralSite: false }),
			undefined,
		);
	});
});
