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

// The synergy-aware model. Coefficients were fitted against the engine itself
// (every pairing of two real leagues' rosters, 60 sims a pairing) and each
// league's fit priced the other league within 0.02 points of its own best fit -
// see the note in getGameSpread.ts.
describe("getGameSpread with a synergy reading", () => {
	const home = { ...base, neutralSite: false };

	test("without synergy, the historical model is untouched", () => {
		// 0.3 * 10 + 3.3504 = 6.3504 -> 6.5
		assert.strictEqual(getGameSpread({ ...home, ovr0: 60, ovr1: 50 }), 6.5);
	});

	test("with synergy, both terms price in", () => {
		// 0.17 * 10 + 8.6 * 0.5 + 3.3504 = 9.4004 -> 9.5
		assert.strictEqual(
			getGameSpread({ ...home, ovr0: 60, ovr1: 50, synergyDiff: 0.5 }),
			9.5,
		);
		// A synergy deficit can flip a small talent edge: 0.17 * 4 + 8.6 * (-0.6)
		// + 3.3504 = -1.13 -> -1.
		assert.strictEqual(
			getGameSpread({ ...home, ovr0: 54, ovr1: 50, synergyDiff: -0.6 }),
			-1,
		);
	});

	test("a zero synergy difference is still the synergy model", () => {
		// Same shape, different talent: the slope must be 0.17, not 0.3.
		// 0.17 * 10 + 3.3504 = 5.0504 -> 5.
		assert.strictEqual(
			getGameSpread({ ...home, ovr0: 60, ovr1: 50, synergyDiff: 0 }),
			5,
		);
	});

	test("a non-finite synergy reading falls back rather than poisoning the line", () => {
		assert.strictEqual(
			getGameSpread({ ...home, ovr0: 60, ovr1: 50, synergyDiff: Number.NaN }),
			getGameSpread({ ...home, ovr0: 60, ovr1: 50 }),
		);
	});

	test("game length scales the whole line, synergy included", () => {
		// 9.4004 / 2 = 4.7002 -> 4.5.
		assert.strictEqual(
			getGameSpread({
				...home,
				quarterLength: defaultGameAttributes.quarterLength / 2,
				ovr0: 60,
				ovr1: 50,
				synergyDiff: 0.5,
			}),
			4.5,
		);
	});

	test("home court still scales with the setting under the synergy model", () => {
		// 3.3504 * 2 = 6.7008 -> 6.5.
		assert.strictEqual(
			getGameSpread({
				...home,
				homeCourtAdvantage: 2,
				ovr0: 50,
				ovr1: 50,
				synergyDiff: 0,
			}),
			6.5,
		);
	});

	test("neutral site drops home court under the synergy model too", () => {
		// 0.17 * 10 + 8.6 * 0.2 = 3.42 -> 3.5.
		assert.strictEqual(
			getGameSpread({
				...base,
				neutralSite: true,
				ovr0: 60,
				ovr1: 50,
				synergyDiff: 0.2,
			}),
			3.5,
		);
	});
});

// The playoff branch: the engine plays playoff games with synergy counting
// roughly double and a bigger home edge (measured - see the playoff constants
// in getGameSpread.ts).
describe("getGameSpread in the playoffs", () => {
	const base = {
		homeCourtAdvantage: 1,
		neutralSite: false,
		numPeriods: 4,
		quarterLength: 12,
		playoffs: true,
	};

	test("uses the measured playoff coefficients exactly", () => {
		// 0.108 * 10 + 17.7 * 0.5 + 3.3504 * 1.465 = 14.8368... -> 15.
		assert.strictEqual(
			getGameSpread({ ...base, ovr0: 60, ovr1: 50, synergyDiff: 0.5 }),
			15,
		);
	});

	test("playoff home court is worth about a point and a half more", () => {
		const regular = getGameSpread({
			...base,
			playoffs: false,
			ovr0: 50,
			ovr1: 50,
			synergyDiff: 0,
		})!;
		const playoffs = getGameSpread({
			...base,
			ovr0: 50,
			ovr1: 50,
			synergyDiff: 0,
		})!;
		assert.strictEqual(regular, 3.5); // 3.3504
		assert.strictEqual(playoffs, 5); // 4.9083
	});

	test("a fit-built team gains on a talent-built team come playoff time", () => {
		// Same matchup, two models: dOvr -5 but dSyn +0.3.
		const args = { ...base, ovr0: 50, ovr1: 55, synergyDiff: 0.3 };
		const regular = getGameSpread({ ...args, playoffs: false })!;
		const playoffs = getGameSpread(args)!;
		// Regular: -0.85 + 2.58 + 3.35 = 5.08; playoffs: -0.54 + 5.31 + 4.91 = 9.68.
		assert.ok(playoffs - regular > 4, `${regular} -> ${playoffs}`);
	});

	test("the ovr-only fallback also has a playoff slope", () => {
		// 0.37 * 10 + 4.9083 = 8.608... -> 8.5.
		assert.strictEqual(getGameSpread({ ...base, ovr0: 60, ovr1: 50 }), 8.5);
	});

	test("neutral playoff games drop the whole home edge", () => {
		// 0.108 * 10 = 1.08 -> 1.
		assert.strictEqual(
			getGameSpread({
				...base,
				neutralSite: true,
				ovr0: 60,
				ovr1: 50,
				synergyDiff: 0,
			}),
			1,
		);
	});
});
