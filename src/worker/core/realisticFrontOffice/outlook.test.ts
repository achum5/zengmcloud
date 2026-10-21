import { assert, beforeAll, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { retireChance } from "./outlook.ts";

beforeAll(() => {
	resetG();
});

test("nobody on a roster retires before 34", () => {
	for (let age = 20; age <= 33; age++) {
		assert.strictEqual(retireChance(age, 40, 40, 60), 0);
	}
	assert.isAbove(retireChance(34, 40, 40, 45), 0);
});

test("the better the old player, the likelier he keeps playing", () => {
	const scrub = retireChance(36, 44, 44, 50);
	const starter = retireChance(36, 58, 58, 64);
	assert.isAbove(scrub, 0.1);
	assert.isBelow(starter, 0.02);
	assert.isAbove(retireChance(38, 44, 44, 50), scrub);
});

test("a star far past his best may hang it up", () => {
	assert.isAbove(retireChance(35, 58, 48, 72), 0.2);
	assert.isBelow(retireChance(35, 58, 58, 72), 0.02);
});
