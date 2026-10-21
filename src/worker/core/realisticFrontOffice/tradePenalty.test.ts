import { assert, test } from "vitest";
import { moodShiftFromTrading, shiftRetention } from "./tradePenalty.ts";

test("trading costs nothing until the count passes 5", () => {
	// 4 traded away this season counts as 3: one more stays under 5.
	assert.strictEqual(moodShiftFromTrading([4, 0, 0], 1, 0), 0);
});

test("past 5, each player traded away lowers mood", () => {
	const one = moodShiftFromTrading([8, 0, 0], 1, 0);
	const two = moodShiftFromTrading([8, 0, 0], 2, 0);
	assert.closeTo(one, -0.75 / 4, 1e-9);
	assert.closeTo(two, 2 * one, 1e-9);
});

test("the count fades over three seasons, then is forgotten", () => {
	const now = moodShiftFromTrading([10, 0, 0], 1, 0);
	const next = moodShiftFromTrading([10, 0, 0], 1, 1);
	const after = moodShiftFromTrading([10, 0, 0], 1, 2);
	assert.isBelow(now, next);
	assert.isBelow(next, after);
	assert.strictEqual(moodShiftFromTrading([10, 0, 0], 1, 3), 0);
});

test("a worse mood means less chance of re-signing, on the mood model's curve", () => {
	assert.isBelow(shiftRetention(0.5, -1), 0.5);
	assert.closeTo(shiftRetention(0.5, -1), 1 / (1 + Math.exp(0.7)), 1e-9);
	assert.strictEqual(shiftRetention(1, -1), 1);
	assert.strictEqual(shiftRetention(0.3, 0), 0.3);
});
