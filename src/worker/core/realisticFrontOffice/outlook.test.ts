import { assert, beforeAll, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { losingShifts, retireChance } from "./outlook.ts";

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

test("tearing a team down costs it its players' mood, and its room", () => {
	const flat = Array.from({ length: 11 }, () => 0);
	const outlook = {
		basePoints: new Map([[0, flat]]),
		meanPoints: flat,
		hype: new Map([[0, 0.5]]),
		records: new Map([[0, { won: 0, lost: 0 }]]),
		remaining: new Map([[0, 0]]),
		progress: 0,
	};
	// Nothing changed: nothing to price again.
	assert.isUndefined(losingShifts(outlook, 0, flat));
	// Five points worse every season: worse moods, worse free agents - most
	// this season and next, less seasons out as the finish drifts back.
	const worse = losingShifts(
		outlook,
		0,
		flat.map(() => -5),
	)!;
	assert.isBelow(worse.mood[0]!, -2);
	assert.isBelow(worse.winp[1]!, -0.1);
	assert.isAbove(worse.mood[6]!, worse.mood[0]!);
	// Better is worth less than worse costs: losing counts twice.
	const better = losingShifts(
		outlook,
		0,
		flat.map(() => 5),
	)!;
	assert.isBelow(better.mood[0]!, -worse.mood[0]!);
});
