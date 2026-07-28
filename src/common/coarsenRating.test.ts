import { assert, describe, test } from "vitest";
import {
	coarsenPlayerForDisplay,
	coarsenRating,
	coarsenRatingChange,
	coarsenRatingsRow,
} from "./coarsenRating.ts";

describe("coarsenRating", () => {
	test("shows the tens digit", () => {
		assert.strictEqual(coarsenRating(56), 5);
		assert.strictEqual(coarsenRating(59), 5);
		assert.strictEqual(coarsenRating(60), 6);
		assert.strictEqual(coarsenRating(0), 0);
		assert.strictEqual(coarsenRating(100), 10);
	});
});

describe("coarsenRatingChange", () => {
	// The whole point of the mode: a rating that moved inside its own ten hasn't
	// visibly moved, so showing "(+2)" next to an unchanged 5 gives away the
	// digit the mode exists to hide.
	test("a move inside the same ten is no change at all", () => {
		assert.strictEqual(coarsenRatingChange(58, 2), 0);
		assert.strictEqual(coarsenRatingChange(51, -1), 0);
	});

	test("crossing a boundary is the change in the displayed digit", () => {
		assert.strictEqual(coarsenRatingChange(60, 2), 1); // 58 -> 60
		assert.strictEqual(coarsenRatingChange(49, -2), -1); // 51 -> 49
		assert.strictEqual(coarsenRatingChange(70, 25), 3); // 45 -> 70
	});

	test("no change stays no change", () => {
		assert.strictEqual(coarsenRatingChange(56, 0), 0);
	});
});

describe("coarsenRatingsRow", () => {
	const row = {
		season: 2012,
		age: 25,
		ovr: 56,
		pot: 68,
		dovr: 2,
		dpot: -9,
		stre: 41,
		pos: "PG",
		skills: ["3"],
	};

	test("rounds the ratings and leaves everything else alone", () => {
		const out = coarsenRatingsRow(row, [
			"season",
			"age",
			"ovr",
			"pot",
			"stre",
			"pos",
			"skills",
		]);
		assert.strictEqual(out.ovr, 5);
		assert.strictEqual(out.pot, 6);
		assert.strictEqual(out.stre, 4);
		assert.strictEqual(out.season, 2012);
		assert.strictEqual(out.age, 25);
		assert.strictEqual(out.pos, "PG");
		assert.deepStrictEqual(out.skills, ["3"]);
	});

	test("changes become changes in the displayed digit", () => {
		const out = coarsenRatingsRow(row, ["ovr", "pot", "dovr", "dpot"]);
		assert.strictEqual(out.dovr, 0); // 54 -> 56, still a 5
		assert.strictEqual(out.dpot, -1); // 77 -> 68
	});

	test("does not mutate the row it was given", () => {
		coarsenRatingsRow(row, ["ovr", "dovr"]);
		assert.strictEqual(row.ovr, 56);
		assert.strictEqual(row.dovr, 2);
	});
});

describe("coarsenPlayerForDisplay", () => {
	const ratings = ["ovr", "pot", "dovr", "pos"];

	test("handles one season's ratings", () => {
		const out = coarsenPlayerForDisplay(
			{ pid: 1, ratings: { ovr: 74, pot: 81, dovr: 4, pos: "SF" } },
			ratings,
		);
		assert.strictEqual(out.ratings.ovr, 7);
		assert.strictEqual(out.ratings.pot, 8);
		assert.strictEqual(out.ratings.dovr, 0);
		assert.strictEqual(out.pid, 1);
	});

	test("handles every season's ratings", () => {
		const out = coarsenPlayerForDisplay(
			{ ratings: [{ ovr: 44 }, { ovr: 63 }] },
			["ovr"],
		);
		assert.deepStrictEqual(out.ratings, [{ ovr: 4 }, { ovr: 6 }]);
	});

	test("rounds the draft-day ratings too, keeping the rest of the pick", () => {
		const out = coarsenPlayerForDisplay(
			{
				ratings: { ovr: 50 },
				draft: { year: 2011, round: 1, pick: 3, ovr: 47, pot: 72 },
			},
			["ovr"],
		);
		assert.strictEqual(out.draft.ovr, 4);
		assert.strictEqual(out.draft.pot, 7);
		assert.strictEqual(out.draft.round, 1);
		assert.strictEqual(out.draft.pick, 3);
	});

	test("does not mutate the player it was given", () => {
		const p = { ratings: { ovr: 74 }, draft: { ovr: 47 } };
		coarsenPlayerForDisplay(p, ["ovr"]);
		assert.strictEqual(p.ratings.ovr, 74);
		assert.strictEqual(p.draft.ovr, 47);
	});
});
