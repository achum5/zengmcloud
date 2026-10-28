import { assert, describe, test } from "vitest";
import { groupsOf } from "./lineups.ts";

const line = (pids: number[], sec: number, pts: number, ptsOpp: number) => ({
	pids,
	sec,
	poss: 10,
	possOpp: 10,
	pts,
	ptsOpp,
});

describe("groupsOf", () => {
	const fives = [
		line([1, 2, 3, 4, 5], 600, 25, 20),
		line([1, 2, 3, 4, 6], 300, 10, 14),
	];

	test("fives are fives", () => {
		assert.deepStrictEqual(groupsOf(fives, 5), fives);
	});

	test("a group adds up every five it was in", () => {
		const fours = groupsOf(fives, 4);
		// Five from the first, five from the second, one shared.
		assert.strictEqual(fours.length, 9);
		const both = fours[0]!;
		assert.deepStrictEqual(both.pids, [1, 2, 3, 4]);
		assert.strictEqual(both.sec, 900);
		assert.strictEqual(both.pts, 35);
		assert.strictEqual(both.ptsOpp, 34);
		assert.strictEqual(both.poss, 20);
	});

	test("pairs", () => {
		const pairs = groupsOf(fives, 2);
		// 1-2-3-4 pair up six ways in both; 5 and 6 with each of them in one.
		assert.strictEqual(pairs.length, 6 + 4 + 4);
		assert.ok(pairs.slice(0, 6).every((pair) => pair.sec === 900));
		assert.ok(
			pairs.every((pair, i) => i === 0 || pairs[i - 1]!.sec >= pair.sec),
		);
	});
});
