import { assert, beforeEach, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { contractOptions, mayPay } from "./contracts.ts";

beforeEach(() => {
	resetG();
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("season", 2030);
});

const wantsThreeYears = {
	contract: { amount: 10000, exp: 2033 },
	ratings: [{ ovr: 60 }],
} as any;

test("the length he wants costs his ask, every other length more", () => {
	const options = contractOptions(wantsThreeYears, 10000);
	const wanted = options.find((c) => c.exp === 2033)!;
	assert.strictEqual(wanted.amount, 10000);
	for (const c of options) {
		if (c !== wanted) {
			assert.isAbove(c.amount, wanted.amount);
		}
	}
	// One year either side costs the same; further costs more.
	const by = (exp: number) => options.find((c) => c.exp === exp)!.amount;
	assert.strictEqual(by(2032), by(2034));
	assert.isAbove(by(2031), by(2032));
});

test("no length is offered above the maximum contract, except the one he wants", () => {
	const options = contractOptions(wantsThreeYears, g.get("maxContract"));
	assert.deepEqual(
		options.map((c) => c.exp),
		[2033],
	);
});

test("over the cap, only the minimum - unless re-signing in a soft cap league", () => {
	const cap = g.get("salaryCap");
	const base = { tid: 0, payroll: cap, rosterSize: 13 };
	assert.isFalse(mayPay({ ...base, amount: 5000, resigning: false }));
	assert.isTrue(
		mayPay({ ...base, amount: g.get("minContract"), resigning: false }),
	);
	assert.isTrue(mayPay({ ...base, amount: 5000, resigning: true }));
	assert.isTrue(
		mayPay({ ...base, payroll: cap - 6000, amount: 5000, resigning: false }),
	);
});
