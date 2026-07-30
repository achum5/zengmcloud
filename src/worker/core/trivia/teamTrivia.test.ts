import { assert, describe, test } from "vitest";
import { byRosterDisplayOrder, narrowCandidates } from "./teamTrivia.ts";

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

// The card grid is on screen the whole time the stat-leader round is asking
// who led the team in points, so the order the cards sit in must not answer it.
describe("byRosterDisplayOrder", () => {
	const p = (name: string, pos: string, jerseyNumber: string | undefined) => ({
		name,
		pos,
		jerseyNumber,
	});

	test("guards first, centers last", () => {
		const sorted = [
			p("Center", "C", "1"),
			p("Point", "PG", "1"),
			p("Wing", "SF", "1"),
		].sort(byRosterDisplayOrder);
		assert.deepStrictEqual(
			sorted.map((x) => x.pos),
			["PG", "SF", "C"],
		);
	});

	test("jersey number orders within a position, numerically", () => {
		const sorted = [
			p("Nine", "SG", "9"),
			p("Twelve", "SG", "12"),
			p("Two", "SG", "2"),
		].sort(byRosterDisplayOrder);
		assert.deepStrictEqual(
			sorted.map((x) => x.name),
			["Two", "Nine", "Twelve"],
		);
	});

	test("a player with no number sorts last, not first", () => {
		const sorted = [
			p("Nameless", "PF", undefined),
			p("Fifty", "PF", "50"),
		].sort(byRosterDisplayOrder);
		assert.deepStrictEqual(
			sorted.map((x) => x.name),
			["Fifty", "Nameless"],
		);
	});

	test("an unrecognised position lands at the end rather than the front", () => {
		const sorted = [p("Odd", "??", "1"), p("Guard", "PG", "99")].sort(
			byRosterDisplayOrder,
		);
		assert.deepStrictEqual(
			sorted.map((x) => x.name),
			["Guard", "Odd"],
		);
	});
});
