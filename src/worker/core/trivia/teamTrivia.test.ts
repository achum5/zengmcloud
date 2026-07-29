import { assert, describe, test } from "vitest";
import { narrowCandidates } from "./teamTrivia.ts";

const candidates = [
	{ season: 2044, tid: 1 },
	{ season: 2044, tid: 2 },
	{ season: 2050, tid: 1 },
	{ season: 2050, tid: 3 },
	{ season: 2060, tid: 2 },
];

describe("narrowCandidates", () => {
	test("no options means anything goes", () => {
		assert.strictEqual(narrowCandidates(candidates, {}).length, 5);
	});

	test("an exact pick narrows to one team-season", () => {
		assert.deepStrictEqual(
			narrowCandidates(candidates, { season: 2050, tid: 3 }),
			[{ season: 2050, tid: 3 }],
		);
	});

	test("a team on its own leaves that team's seasons", () => {
		assert.deepStrictEqual(
			narrowCandidates(candidates, { tid: 1 }).map((c) => c.season),
			[2044, 2050],
		);
	});

	test("a year range is inclusive at both ends", () => {
		assert.deepStrictEqual(
			narrowCandidates(candidates, {
				minSeason: 2044,
				maxSeason: 2050,
			}).map((c) => c.season),
			[2044, 2044, 2050, 2050],
		);
	});

	test("range and team compose", () => {
		assert.deepStrictEqual(
			narrowCandidates(candidates, { tid: 2, minSeason: 2050 }),
			[{ season: 2060, tid: 2 }],
		);
	});

	// A filter that rules everything out has to fall back rather than leave the
	// page with no game at all: a dropdown that silently does nothing reads as
	// broken, and the alternative is showing an error for a combination the
	// player can't be expected to know is impossible.
	test("an impossible combination falls back to the full list", () => {
		assert.strictEqual(
			narrowCandidates(candidates, { season: 2044, tid: 3 }).length,
			5,
		);
		assert.strictEqual(
			narrowCandidates(candidates, { minSeason: 2999 }).length,
			5,
		);
	});

	test("an empty list stays empty", () => {
		assert.deepStrictEqual(narrowCandidates([], { tid: 1 }), []);
	});

	test("does not mutate the list it was given", () => {
		const original = [...candidates];
		narrowCandidates(candidates, { tid: 1 });
		assert.deepStrictEqual(candidates, original);
	});
});
