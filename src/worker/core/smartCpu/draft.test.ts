import { assert, test } from "vitest";
import { boardChoice } from "./draft.ts";

// THE BOARD BETWEEN PROSPECTS A TEAM CAN'T TELL APART.

const prospect = (pid: number, value: number, worth: number) => ({
	p: { pid, value },
	worth,
});

test("a team takes the prospect worth most to it when the rest are past its margin", () => {
	const worths = [prospect(1, 60, 0.1), prospect(2, 70, 0.05)];
	assert.strictEqual(boardChoice(worths, 0.02)!.p.pid, 1);
});

test("among prospects within its margin of its best, the board decides", () => {
	// Bol Bol against Jaxson Hayes, by Detroit's read in a League 12 run.
	const worths = [
		prospect(1, 58, 0.0653),
		prospect(2, 66, 0.0572),
		prospect(3, 57, 0.0535),
	];
	assert.strictEqual(boardChoice(worths, 0.01)!.p.pid, 2);
	// With no margin, its own read.
	assert.strictEqual(boardChoice(worths, 0)!.p.pid, 1);
});

test("ties on the board go to the lower pid, the same on every device", () => {
	const worths = [prospect(9, 60, 0.05), prospect(4, 60, 0.049)];
	assert.strictEqual(boardChoice(worths, 0.01)!.p.pid, 4);
	assert.isUndefined(boardChoice([], 0.01));
});
