import { assert, describe, test } from "vitest";
import { relabelAwardsFromSettings } from "./awards.ts";

const settings = [
	{ shortName: "MVP", name: "Most Valuable Player" },
	{ shortName: "ALL", name: "All-NBA", numTeams: 3 },
	{ shortName: "DEF", name: "All-Defensive", numTeams: 2 },
];

const award = (over: Record<string, unknown> = {}) => ({
	season: 2005,
	shortName: "ALL",
	name: "All-League",
	numTeams: 3,
	rank: 1,
	...over,
});

describe("relabelAwardsFromSettings", () => {
	// The whole point: a copy written down under the old name reads as the new
	// one, without anything having to rewrite it first.
	test("a stale label takes the name the settings use now", () => {
		const out = relabelAwardsFromSettings([award()], settings)!;
		assert.strictEqual(out[0]!.name, "All-NBA");
		// The abbrev identifies the award and never moves.
		assert.strictEqual(out[0]!.shortName, "ALL");
		// Everything else about it is untouched.
		assert.strictEqual(out[0]!.rank, 1);
		assert.strictEqual(out[0]!.season, 2005);
	});

	test("an award that already agrees is returned as it was", () => {
		const awards = [award({ name: "All-NBA" })];
		assert.strictEqual(relabelAwardsFromSettings(awards, settings), awards);
	});

	test("an individual award too", () => {
		const out = relabelAwardsFromSettings(
			[award({ shortName: "MVP", name: "MVP", numTeams: undefined })],
			settings,
		)!;
		assert.strictEqual(out[0]!.name, "Most Valuable Player");
	});

	// An abbrev the settings no longer use belongs to an award that was deleted,
	// or renamed abbrev and all. Its history is not ours to rewrite.
	test("an abbrev the settings do not use is left alone", () => {
		const out = relabelAwardsFromSettings(
			[award({ shortName: "SMOY", name: "Sixth Man" })],
			settings,
		)!;
		assert.strictEqual(out[0]!.name, "Sixth Man");
	});

	// The same abbrev on a team award and an individual one is not the same
	// award wearing an old label.
	test("a team award never takes an individual award's name", () => {
		const out = relabelAwardsFromSettings(
			[award({ shortName: "MVP", name: "Playoffs MVP", numTeams: 3 })],
			settings,
		)!;
		assert.strictEqual(out[0]!.name, "Playoffs MVP");
	});

	// Legacy awards are a bare string with no abbrev to match on.
	test("a legacy award is left alone", () => {
		const out = relabelAwardsFromSettings(
			[{ season: 2005, type: "First Team All-League" } as any],
			settings,
		)!;
		assert.strictEqual((out[0] as any).type, "First Team All-League");
	});

	test("nothing to relabel", () => {
		assert.isUndefined(relabelAwardsFromSettings(undefined, settings));
		assert.deepStrictEqual(relabelAwardsFromSettings([], settings), []);
		const awards = [award()];
		assert.strictEqual(relabelAwardsFromSettings(awards, undefined), awards);
		assert.strictEqual(relabelAwardsFromSettings(awards, []), awards);
	});

	// The input is somebody else's data - a cached player, an awards row - so
	// it is never written to.
	test("the awards handed in are not mutated", () => {
		const awards = [award()];
		relabelAwardsFromSettings(awards, settings);
		assert.strictEqual(awards[0]!.name, "All-League");
	});

	test("a mix relabels only what needs it", () => {
		const out = relabelAwardsFromSettings(
			[
				award(),
				award({ shortName: "DEF", name: "All-Defensive", numTeams: 2 }),
				award({ shortName: "XYZ", name: "Made Up", numTeams: undefined }),
			],
			settings,
		)!;
		assert.deepStrictEqual(
			out.map((a) => a.name),
			["All-NBA", "All-Defensive", "Made Up"],
		);
	});
});
