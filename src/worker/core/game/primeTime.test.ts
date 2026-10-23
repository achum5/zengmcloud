import { assert, test } from "vitest";
import { isPrimeTime } from "./primeTime.ts";

// Ten teams, tid 0 the best and tid 9 the worst.
const ovrs = new Map(Array.from({ length: 10 }, (_, tid) => [tid, 70 - tid]));

test("two of the league's better teams meet in prime time", () => {
	assert.isTrue(isPrimeTime(ovrs, [0, 3]));
	assert.isTrue(isPrimeTime(ovrs, [2, 1]));
});

test("not when either of them is not one", () => {
	assert.isFalse(isPrimeTime(ovrs, [0, 4]));
	assert.isFalse(isPrimeTime(ovrs, [8, 9]));
	assert.isFalse(isPrimeTime(ovrs, [7, 1]));
});

test("not when it cannot be told", () => {
	assert.isFalse(isPrimeTime(new Map(), [0, 1]));
	assert.isFalse(isPrimeTime(ovrs, [0, 12]));
});
