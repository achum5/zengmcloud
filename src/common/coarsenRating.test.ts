import { assert, describe, test } from "vitest";
import {
	coarsenPlayerForDisplay,
	coarsenRating,
	coarsenRatingChange,
	coarsenRatingsRow,
	coarsenRatingValue,
	exemptFromCoarseRatings,
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

describe("exemptFromCoarseRatings", () => {
	// Scouting a draft class is the one place the tens digit really matters, so
	// prospects can be spared. That ends the moment they're drafted.
	test("an undrafted prospect is exempt", () => {
		assert.strictEqual(exemptFromCoarseRatings(-2, true), true);
	});

	test("so are the legacy future draft classes", () => {
		assert.strictEqual(exemptFromCoarseRatings(-4, true), true);
		assert.strictEqual(exemptFromCoarseRatings(-5, true), true);
	});

	test("a player on a team is not, however good he was as a prospect", () => {
		for (const tid of [0, 1, 29]) {
			assert.strictEqual(exemptFromCoarseRatings(tid, true), false);
		}
	});

	test("free agents and retirees are not prospects", () => {
		assert.strictEqual(exemptFromCoarseRatings(-1, true), false);
		assert.strictEqual(exemptFromCoarseRatings(-3, true), false);
	});

	test("nothing is exempt with the option off", () => {
		assert.strictEqual(exemptFromCoarseRatings(-2, false), false);
	});

	test("an unknown team is not exempt", () => {
		assert.strictEqual(exemptFromCoarseRatings(undefined, true), false);
	});
});

describe("coarsenPlayerForDisplay honours the exemption", () => {
	const prospect = { tid: -2, ratings: { ovr: 74, pot: 81 } };
	const drafted = { tid: 3, ratings: { ovr: 74, pot: 81 } };

	test("a prospect comes back untouched", () => {
		const out = coarsenPlayerForDisplay(prospect, ["ovr", "pot"], true);
		assert.strictEqual(out.ratings.ovr, 74);
		assert.strictEqual(out.ratings.pot, 81);
	});

	test("a drafted player is still rounded", () => {
		const out = coarsenPlayerForDisplay(drafted, ["ovr", "pot"], true);
		assert.strictEqual(out.ratings.ovr, 7);
	});

	test("without the option, a prospect is rounded like anyone else", () => {
		const out = coarsenPlayerForDisplay(prospect, ["ovr", "pot"]);
		assert.strictEqual(out.ratings.ovr, 7);
	});
});

// Found by auditing the mode end to end: ovrs/pots are per-position MAPS, not
// numbers, so a plain typeof check walked straight past them and they reached
// the Depth chart and the ratings CSV at full resolution.
describe("coarsenRatingValue", () => {
	test("rounds a plain rating", () => {
		assert.strictEqual(coarsenRatingValue(56), 5);
	});

	test("rounds every rating inside a per-position map", () => {
		assert.deepStrictEqual(coarsenRatingValue({ C: 55, PG: 41, SF: 68 }), {
			C: 5,
			PG: 4,
			SF: 6,
		});
	});

	test("leaves non-numbers in the map alone", () => {
		assert.deepStrictEqual(coarsenRatingValue({ pos: "PG", ovr: 74 }), {
			pos: "PG",
			ovr: 7,
		});
	});

	test("leaves strings, arrays and nullish values untouched", () => {
		assert.strictEqual(coarsenRatingValue("PG"), "PG");
		assert.deepStrictEqual(coarsenRatingValue(["3", "Dp"]), ["3", "Dp"]);
		assert.strictEqual(coarsenRatingValue(undefined), undefined);
		assert.strictEqual(coarsenRatingValue(null), null);
	});

	test("does not mutate the map it was given", () => {
		const ovrs = { C: 55 };
		coarsenRatingValue(ovrs);
		assert.strictEqual(ovrs.C, 55);
	});
});
