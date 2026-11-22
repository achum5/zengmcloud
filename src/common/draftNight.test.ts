import { assert, describe, test } from "vitest";
import {
	announcement,
	bestAvailable,
	bigBoard,
	draftFacts,
	nextPicks,
	overallPick,
	pickOrder,
	type BoardMember,
} from "./draftNight.ts";

const member = (
	pid: number,
	value: number,
	round: number,
	pick: number,
): BoardMember => ({
	pid,
	name: `P${pid}`,
	pos: "G",
	college: "State",
	round,
	pick,
	value,
});

describe("draft night", () => {
	test("an undrafted player falls after every pick", () => {
		assert.isBelow(
			pickOrder({ round: 2, pick: 30 }),
			pickOrder({ round: 0, pick: 0 }),
		);
		assert.isBelow(
			pickOrder({ round: 1, pick: 30 }),
			pickOrder({ round: 2, pick: 1 }),
		);
	});

	test("the big board ranks on value, not on where they went", () => {
		const board = bigBoard([
			member(1, 60, 1, 1),
			member(2, 70, 1, 2),
			member(3, 50, 0, 0),
		]);
		assert.strictEqual(board.get(2), 1);
		assert.strictEqual(board.get(1), 2);
		assert.strictEqual(board.get(3), 3);
	});

	test("best available is everyone taken later or not at all, by board rank", () => {
		const members = [
			member(1, 60, 1, 1),
			member(2, 70, 1, 2),
			member(3, 65, 0, 0),
			member(4, 40, 1, 3),
		];
		const best = bestAvailable(members, { round: 1, pick: 1 }, 5);
		assert.deepEqual(
			best.map((b) => [b.name, b.rank]),
			[
				["P2", 1],
				["P3", 2],
				["P4", 4],
			],
		);
		// The man taken at this pick is not on it, nor anyone before him.
		assert.deepEqual(
			bestAvailable(members, { round: 1, pick: 2 }, 1).map((b) => b.name),
			["P3"],
		);
	});

	test("next picks come once each, in order, after this one", () => {
		const slots = [
			{ round: 1, pick: 3, tid: 7 },
			{ round: 1, pick: 2, tid: 5 },
			{ round: 1, pick: 2, tid: 5 },
			{ round: 1, pick: 1, tid: 9 },
			{ round: 2, pick: 1, tid: 9 },
		];
		assert.deepEqual(
			nextPicks(slots, { round: 1, pick: 1 }, 3).map((s) => s.tid),
			[5, 7, 9],
		);
	});

	test("the pick is announced counted through the whole draft", () => {
		const slots = [
			{ round: 1, pick: 30 },
			{ round: 1, pick: 12 },
			{ round: 2, pick: 1 },
		];
		assert.strictEqual(overallPick(slots, { round: 1, pick: 12 }), 12);
		assert.strictEqual(overallPick(slots, { round: 2, pick: 1 }), 31);
		assert.strictEqual(
			announcement({ overall: 31, season: 2026, team: "Utah Jazz" }),
			"With the 31st pick in the 2026 draft, the Utah Jazz select",
		);
	});

	test("facts are true things, the most telling first", () => {
		assert.deepEqual(
			draftFacts({
				relatives: [
					{ type: "son", name: "Kid" },
					{ type: "father", name: "Dad Smith" },
				],
				rank: 3,
				born: "Chicago, IL",
				via: "Boston Celtics",
			}),
			[
				"Son of Dad Smith",
				"No. 3 on the big board",
				"From Chicago, IL",
				"Pick acquired from Boston Celtics",
			],
		);
		assert.deepEqual(
			draftFacts({
				relatives: [],
				rank: undefined,
				born: undefined,
				via: undefined,
			}),
			[],
		);
	});
});
