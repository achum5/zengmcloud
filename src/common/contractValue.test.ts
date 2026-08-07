import { assert, describe, test } from "vitest";
import {
	CAP_SHARE_PER_WIN,
	WINS_PER_VORP,
	getContractValue,
	getContractValues,
	getDollarsPerWin,
	warFromVorp,
} from "./contractValue.ts";

// Default-ish league economics, in thousands.
const SETTINGS = { minContract: 1200, salaryCap: 150000 };

describe("contract value", () => {
	test("converts VORP to wins the way Basketball-Reference does", () => {
		assert.strictEqual(warFromVorp(2), 2 * WINS_PER_VORP);
		assert.strictEqual(warFromVorp(undefined), 0, "never played is not bad");
		assert.strictEqual(warFromVorp(Number.NaN), 0);
	});

	// THE reason this isn't rating/salary. Dividing by a small denominator puts
	// every minimum-salary body at the top of the list, which is exactly the
	// list you were trying to read.
	test("a minimum-salary player who does nothing is neutral, not elite", () => {
		// A star on a bargain deal, a veteran being wildly overpaid, and a body
		// on the minimum who does nothing at all.
		const star = { ovr: 75, vorp: 6, salary: 5000 };
		const overpaid = { ovr: 62, vorp: 3, salary: 20000 };
		const scrub = { ovr: 40, vorp: 0, salary: SETTINGS.minContract };
		const values = getContractValues([star, overpaid, scrub], SETTINGS);

		assert.strictEqual(
			values.get(scrub)!.surplus,
			0,
			"paid the minimum, produced nothing, so he cost exactly what he was worth",
		);
		assert.ok(
			values.get(star)!.surplus > values.get(scrub)!.surplus,
			"and the bargain star has to outrank him",
		);
		assert.ok(values.get(overpaid)!.surplus < 0);

		// The rating/salary version, spelled out so the failure stays on record:
		// it ranks the do-nothing scrub top of the league, ahead of the bargain
		// star, purely because dividing by 1.2 beats dividing by 5.
		const ratio = (p: typeof star) => p.ovr / (p.salary / 1000);
		assert.ok(
			ratio(scrub) > ratio(star),
			"a ratio really does invert them, which is why it isn't what ships",
		);
	});

	test("bargains are positive and overpays are negative", () => {
		const league = [
			// Star on a rookie-scale deal.
			{ vorp: 5, salary: 5000 },
			// Paid like a star, produces like a backup.
			{ vorp: 0.5, salary: 30000 },
			{ vorp: 2, salary: 15000 },
		];
		const values = getContractValues(league, SETTINGS);

		assert.ok(values.get(league[0]!)!.surplus > 0);
		assert.ok(values.get(league[1]!)!.surplus < 0);
	});

	// The property that lets one formula serve a 41-game league, a season that
	// is only a quarter played, and a $10M cap: everything that scales payroll
	// and production together cancels, because the price of a win is measured
	// from the same league it is applied to.
	test("pricing is invariant to how much of the season has been played", () => {
		const full = [
			{ vorp: 6, salary: 40000 },
			{ vorp: 3, salary: 20000 },
			{ vorp: 1, salary: 8000 },
			{ vorp: 0, salary: 1200 },
		];
		// A quarter of the way in, everyone's VORP is a quarter of the size while
		// the contracts are unchanged.
		const quarter = full.map((p) => ({ ...p, vorp: p.vorp / 4 }));

		const fullValues = getContractValues(full, SETTINGS);
		const quarterValues = getContractValues(quarter, SETTINGS);

		for (const [i, p] of full.entries()) {
			assert.ok(
				Math.abs(
					fullValues.get(p)!.surplus - quarterValues.get(quarter[i]!)!.surplus,
				) < 1e-6,
				`player ${i} must be priced the same either way`,
			);
		}
	});

	// Falls out of the calibration: the league's whole payroll is redistributed
	// according to who earned it, so it can't invent or destroy money.
	test("surplus sums to zero across a league with no sub-replacement players", () => {
		const league = [
			{ vorp: 7, salary: 45000 },
			{ vorp: 4, salary: 33000 },
			{ vorp: 2, salary: 9000 },
			{ vorp: 0.2, salary: 2500 },
			{ vorp: 0, salary: 1200 },
		];
		const values = getContractValues(league, SETTINGS);
		const total = league.reduce((sum, p) => sum + values.get(p)!.surplus, 0);
		assert.ok(Math.abs(total) < 1e-6, `expected ~0, got ${total}`);
	});

	describe("nothing to measure yet", () => {
		test("preseason falls back to the real-NBA share of the cap", () => {
			const preseason = [
				{ vorp: undefined, salary: 40000 },
				{ vorp: undefined, salary: 20000 },
			];
			assert.strictEqual(
				getDollarsPerWin(preseason, SETTINGS),
				CAP_SHARE_PER_WIN * SETTINGS.salaryCap,
			);
		});

		test("a league entirely on minimum deals falls back too", () => {
			// Dividing by an above-floor budget of zero would price every win at
			// zero and report the whole league as worthless.
			const broke = [
				{ vorp: 5, salary: SETTINGS.minContract },
				{ vorp: 2, salary: SETTINGS.minContract },
			];
			assert.strictEqual(
				getDollarsPerWin(broke, SETTINGS),
				CAP_SHARE_PER_WIN * SETTINGS.salaryCap,
			);
		});

		test("an empty league does not divide by zero", () => {
			assert.strictEqual(
				getDollarsPerWin([], SETTINGS),
				CAP_SHARE_PER_WIN * SETTINGS.salaryCap,
			);
		});
	});

	test("market value floors at zero rather than going negative", () => {
		// Deeply sub-replacement. He is worth nothing, but nobody is worth a
		// negative salary, so the loss is capped at what he is actually paid.
		const value = getContractValue({ vorp: -20, salary: 9000 }, 1200, 4800);
		assert.strictEqual(value.marketValue, 0);
		assert.strictEqual(value.surplus, -9000);
	});

	// The fallback is only defensible if it lands somewhere sane, so pin it:
	// a genuine superstar season should price out near a max contract.
	test("the fallback prices a 10-win season near the max contract", () => {
		const dollarsPerWin = CAP_SHARE_PER_WIN * SETTINGS.salaryCap;
		const { marketValue } = getContractValue(
			{ vorp: 10 / WINS_PER_VORP, salary: 0 },
			SETTINGS.minContract,
			dollarsPerWin,
		);
		// Default max contract is 50000.
		assert.ok(
			marketValue > 45000 && marketValue < 55000,
			`expected roughly a max contract, got ${marketValue}`,
		);
	});
});
