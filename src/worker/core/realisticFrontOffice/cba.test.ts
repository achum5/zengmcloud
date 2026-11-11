import { assert, beforeEach, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import {
	amountsWithRaise,
	birdTid,
	checkSigning,
	checkTradeSalary,
	hardCap,
	maxFor,
	midLevelAmount,
	raiseLimit,
	raisesOk,
	serviceYears,
} from "./cba.ts";

beforeEach(() => {
	resetG();
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("season", 2030);
	g.setWithoutSavingToDB("salaryCap", 100_000);
	g.setWithoutSavingToDB("maxContract", 35_000);
	g.setWithoutSavingToDB("minContract", 1_000);
	g.setWithoutSavingToDB("luxuryPayroll", 120_000);
	g.setWithoutSavingToDB("minRosterSize", 13);
	g.setWithoutSavingToDB("hardCapAmount", 0);
	g.setWithoutSavingToDB("hardCapTids", []);
	g.setWithoutSavingToDB("hardCapUseLuxuryTax", false);
});

const base = {
	tid: 1,
	service: 8,
	own: false,
	midLevelUsed: false,
	rosterSize: 12,
};

test("one max contract for everyone", () => {
	assert.closeTo(maxFor(0), 35_000, 1e-6);
	assert.closeTo(maxFor(12), 35_000, 1e-6);
});

test("seasons played count regular seasons with games, once each", () => {
	const p = {
		stats: [
			{ season: 2027, playoffs: false, gp: 50, tid: 1 },
			{ season: 2028, playoffs: false, gp: 20, tid: 1 },
			{ season: 2028, playoffs: false, gp: 30, tid: 2 },
			{ season: 2028, playoffs: true, gp: 4, tid: 2 },
			{ season: 2029, playoffs: false, gp: 0, tid: 2 },
		],
	} as any;
	assert.strictEqual(serviceYears(p), 2);
	assert.strictEqual(birdTid(p, 2030), 2);
	assert.strictEqual(birdTid(p, 2031), undefined);
});

test("under the cap, room; over it, only Bird rights, the mid-level or the minimum", () => {
	assert.deepEqual(checkSigning({ ...base, payroll: 70_000, amount: 25_000 }), {
		ok: true,
		path: "room",
	});
	assert.deepEqual(checkSigning({ ...base, payroll: 95_000, amount: 25_000 }), {
		ok: false,
		reason: "Over the cap; more than the mid-level",
	});
	assert.deepEqual(
		checkSigning({ ...base, payroll: 95_000, amount: 25_000, own: true }),
		{ ok: true, path: "bird" },
	);
	assert.deepEqual(
		checkSigning({ ...base, payroll: 95_000, amount: midLevelAmount() }),
		{ ok: true, path: "midLevel" },
	);
	assert.isFalse(
		checkSigning({
			...base,
			payroll: 95_000,
			amount: midLevelAmount(),
			midLevelUsed: true,
		}).ok,
	);
	assert.deepEqual(checkSigning({ ...base, payroll: 95_000, amount: 1_000 }), {
		ok: true,
		path: "minimum",
	});
});

test("the mid-level up to the hard cap, and nothing past it but minimums to fill a roster", () => {
	assert.strictEqual(hardCap(1), 130_000);
	assert.deepEqual(
		checkSigning({ ...base, payroll: 115_000, amount: midLevelAmount() }),
		{ ok: true, path: "midLevel" },
	);
	assert.isFalse(
		checkSigning({ ...base, payroll: 125_000, amount: 10_000, own: true }).ok,
	);
	assert.isFalse(
		checkSigning({ ...base, payroll: 130_000, amount: 1_000, rosterSize: 14 })
			.ok,
	);
	assert.isTrue(
		checkSigning({ ...base, payroll: 130_000, amount: 1_000, rosterSize: 12 })
			.ok,
	);
	assert.isFalse(
		checkSigning({ ...base, payroll: 0, amount: 40_000, service: 3 }).ok,
	);
});

test("the league's own hard cap counts where it is lower", () => {
	g.setWithoutSavingToDB("hardCapUseLuxuryTax", true);
	g.setWithoutSavingToDB("hardCapTids", [1]);
	assert.strictEqual(hardCap(1), 120_000);
	assert.strictEqual(hardCap(2), 130_000);
});

test("raises: more re-signing his own player", () => {
	assert.strictEqual(raiseLimit(true), 0.08);
	assert.strictEqual(raiseLimit(false), 0.05);
	const amounts = amountsWithRaise(10_000, 4, 0.05);
	assert.deepEqual(amounts, [10_000, 10_500, 11_000, 11_500]);
	assert.isTrue(raisesOk(amounts, 0.05));
	assert.isFalse(raisesOk(amountsWithRaise(10_000, 4, 0.08), 0.05));
	assert.isTrue(raisesOk(amountsWithRaise(10_000, 4, -0.05), 0.05));
});

test("trades past the hard cap may not add salary", () => {
	assert.isTrue(
		checkTradeSalary({
			tid: 1,
			payrollAfter: 125_000,
			incoming: 20_000,
			outgoing: 10_000,
		}).ok,
	);
	assert.isFalse(
		checkTradeSalary({
			tid: 1,
			payrollAfter: 135_000,
			incoming: 20_000,
			outgoing: 10_000,
		}).ok,
	);
	assert.isTrue(
		checkTradeSalary({
			tid: 1,
			payrollAfter: 140_000,
			incoming: 10_000,
			outgoing: 10_000,
		}).ok,
	);
});
