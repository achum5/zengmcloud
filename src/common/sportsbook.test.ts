import { assert, describe, test } from "vitest";
import {
	americanToDecimal,
	americanToImpliedProb,
	formatAmerican,
	formatSportsbookMoney,
	probToAmerican,
	SPORTSBOOK_VIG,
} from "./sportsbook.ts";

describe("probToAmerican", () => {
	test("a coin-flip prices as a slight favorite once the vig is applied", () => {
		const odds = probToAmerican(0.5);
		assert.ok(odds < 0, `expected a negative (favored) price, got ${odds}`);
		assert.ok(odds <= -100);
	});

	test("a strong favorite is a big negative number", () => {
		const odds = probToAmerican(0.8);
		assert.ok(odds < -200, `expected < -200, got ${odds}`);
	});

	test("a long shot is a big positive number", () => {
		const odds = probToAmerican(0.1);
		assert.ok(odds > 200, `expected > +200, got ${odds}`);
	});

	test("higher true probability never yields a worse (more positive) price", () => {
		let prev = Number.POSITIVE_INFINITY;
		for (let p = 0.05; p <= 0.95; p += 0.05) {
			const odds = probToAmerican(p);
			assert.ok(
				odds <= prev + 1,
				`monotonic: p=${p} odds=${odds} prev=${prev}`,
			);
			prev = odds;
		}
	});

	test("the book's implied probabilities carry the vig (sum > 100% on a 2-way)", () => {
		// A perfectly even 2-way market: both sides at true 50%.
		const a = probToAmerican(0.5);
		const overround = 2 * americanToImpliedProb(a);
		assert.ok(overround > 1, `expected overround > 1, got ${overround}`);
		assert.ok(overround < 1 + 2 * SPORTSBOOK_VIG + 0.05);
	});
});

describe("americanToDecimal", () => {
	test("negative (favorite) odds", () => {
		assert.ok(Math.abs(americanToDecimal(-200) - 1.5) < 1e-9);
	});
	test("positive (underdog) odds", () => {
		assert.ok(Math.abs(americanToDecimal(150) - 2.5) < 1e-9);
	});
	test("a winning bet returns stake * decimal", () => {
		const stake = 1000;
		const payout = stake * americanToDecimal(-110);
		// -110 → risk 110 to win 100 → $1000 returns ~$1909.
		assert.ok(payout > 1900 && payout < 1920, `payout ${payout}`);
	});
});

describe("formatting", () => {
	test("formatAmerican", () => {
		assert.strictEqual(formatAmerican(150), "+150");
		assert.strictEqual(formatAmerican(-150), "-150");
		assert.strictEqual(formatAmerican(0), "EVEN");
	});

	test("formatSportsbookMoney", () => {
		assert.strictEqual(formatSportsbookMoney(1_000_000), "$1M");
		assert.strictEqual(formatSportsbookMoney(1_500_000), "$1.5M");
		assert.strictEqual(formatSportsbookMoney(12_500), "$12.5K");
		assert.strictEqual(formatSportsbookMoney(500), "$500");
		assert.strictEqual(formatSportsbookMoney(-2_000_000), "-$2M");
	});
});
