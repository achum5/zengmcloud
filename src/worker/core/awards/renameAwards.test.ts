import { assert, describe, test } from "vitest";
import {
	awardLabelsOutOfDate,
	awardRenames,
	awardRenamesFromSettings,
	renameMatches,
} from "./renameAwards.ts";
import type { AwardSettings } from "../../../common/types.ts";

const mvp: AwardSettings[number] = {
	shortName: "MVP",
	name: "Most Valuable Player",
	formula: "ewa + vorp",
	showStats: "offense",
	actAs: "mvp",
};

const allLeague: AwardSettings[number] = {
	shortName: "ALL",
	name: "All-League",
	formula: "ewa / 22",
	showStats: "offense",
	numTeams: 3,
};

const roy: AwardSettings[number] = {
	shortName: "ROY",
	name: "Rookie of the Year",
	formula: "ewa",
	showStats: "offense",
	rookie: true,
};

describe("awardRenames", () => {
	test("no change, no renames", () => {
		assert.deepStrictEqual(
			awardRenames([mvp, allLeague], [mvp, allLeague]),
			[],
		);
	});

	// The abbrev is unique, so an award that keeps it is the same award and a
	// changed name is a rename.
	test("a renamed award keeping its abbrev", () => {
		assert.deepStrictEqual(
			awardRenames([mvp, allLeague], [mvp, { ...allLeague, name: "All-NBA" }]),
			[
				{
					fromShortName: "ALL",
					toName: "All-NBA",
					toShortName: "ALL",
					isTeam: true,
				},
			],
		);
	});

	// Name and abbrev together: recognized by its slot, since the abbrev it used
	// to carry is gone from the list and nothing else about it changed.
	test("a renamed award with a new abbrev", () => {
		assert.deepStrictEqual(
			awardRenames(
				[mvp, allLeague],
				[mvp, { ...allLeague, name: "All-NBA", shortName: "ANBA" }],
			),
			[
				{
					fromShortName: "ALL",
					toName: "All-NBA",
					toShortName: "ANBA",
					isTeam: true,
				},
			],
		);
	});

	// The name follows the abbrev wherever it sits, so reordering the list is
	// not a rename and does not confuse one.
	test("reordering is not a rename", () => {
		assert.deepStrictEqual(
			awardRenames([mvp, allLeague], [allLeague, mvp]),
			[],
		);

		assert.deepStrictEqual(
			awardRenames([mvp, allLeague], [{ ...allLeague, name: "All-NBA" }, mvp]),
			[
				{
					fromShortName: "ALL",
					toName: "All-NBA",
					toShortName: "ALL",
					isTeam: true,
				},
			],
		);
	});

	test("adding an award renames nothing", () => {
		assert.deepStrictEqual(awardRenames([mvp], [mvp, roy]), []);
	});

	test("deleting an award renames nothing", () => {
		assert.deepStrictEqual(awardRenames([mvp, roy], [mvp]), []);
	});

	// A slot edited into a different award entirely - new abbrev, new name, new
	// formula - is a new award, not a renamed one, and its history stays put.
	test("a slot rewritten into something else is not a rename", () => {
		assert.deepStrictEqual(
			awardRenames(
				[mvp, allLeague],
				[
					mvp,
					{
						...allLeague,
						name: "Defensive Player of the Year",
						shortName: "DPOY",
						formula: "dws",
					},
				],
			),
			[],
		);
	});

	// The formula changed too, but the abbrev says it is the same award, so the
	// name still follows.
	test("an edited award that keeps its abbrev still renames", () => {
		assert.deepStrictEqual(
			awardRenames(
				[allLeague],
				[{ ...allLeague, name: "All-NBA", formula: "ws" }],
			),
			[
				{
					fromShortName: "ALL",
					toName: "All-NBA",
					toShortName: "ALL",
					isTeam: true,
				},
			],
		);
	});

	// An abbrev handed from a team award to an individual one. The abbrev says
	// it is the same award, so a rename is recorded - but it carries the shape
	// the abbrev means NOW, and the shape is what stops it relabeling three
	// All-League teams as Rookie of the Year.
	test("a rename never crosses between team and individual awards", () => {
		const renames = awardRenames(
			[allLeague],
			[{ ...roy, shortName: "ALL", name: "Rookie of the Year" }],
		);
		assert.deepStrictEqual(renames, [
			{
				fromShortName: "ALL",
				toName: "Rookie of the Year",
				toShortName: "ALL",
				isTeam: false,
			},
		]);

		// The team award it used to be is left alone.
		assert.strictEqual(
			renameMatches({ shortName: "ALL", numTeams: 3 }, renames[0]!),
			false,
		);
		assert.strictEqual(renameMatches({ shortName: "ALL" }, renames[0]!), true);
	});

	test("renaming back undoes it", () => {
		const renamed = { ...allLeague, name: "All-NBA", shortName: "ANBA" };
		assert.deepStrictEqual(awardRenames([renamed], [allLeague]), [
			{
				fromShortName: "ANBA",
				toName: "All-League",
				toShortName: "ALL",
				isTeam: true,
			},
		]);
	});

	test("several at once", () => {
		const renames = awardRenames(
			[mvp, allLeague, roy],
			[
				{ ...mvp, name: "League MVP" },
				{ ...allLeague, name: "All-NBA", shortName: "ANBA" },
				roy,
			],
		);
		assert.deepStrictEqual(
			renames.map((rename) => [rename.fromShortName, rename.toShortName]),
			[
				["MVP", "MVP"],
				["ALL", "ANBA"],
			],
		);
	});
});

// The case a diff can never reach: the settings were changed before renaming
// carried through history, so both sides of any comparison now say All-NBA and
// only the seasons still say All-League.
describe("awardRenamesFromSettings", () => {
	test("every award becomes a rename onto its own current label", () => {
		assert.deepStrictEqual(
			awardRenamesFromSettings([mvp, { ...allLeague, name: "All-NBA" }]),
			[
				{
					fromShortName: "MVP",
					toName: "Most Valuable Player",
					toShortName: "MVP",
					isTeam: false,
				},
				{
					fromShortName: "ALL",
					toName: "All-NBA",
					toShortName: "ALL",
					isTeam: true,
				},
			],
		);
	});
});

describe("awardLabelsOutOfDate", () => {
	const season = (name: string, shortName = "ALL") => ({
		awards: [{ shortName, name, numTeams: 3 }],
	});

	test("a season still carrying the old label", () => {
		assert.isTrue(
			awardLabelsOutOfDate(
				[season("All-League")],
				[{ ...allLeague, name: "All-NBA" }],
			),
		);
	});

	test("a league that was never renamed has nothing to do", () => {
		assert.isFalse(awardLabelsOutOfDate([season("All-League")], [allLeague]));
		assert.isFalse(awardLabelsOutOfDate([], [allLeague]));
	});

	// An abbrev no longer in the settings is an award that was deleted or given
	// a new abbrev, and its history is not this check's to touch.
	test("an abbrev the settings no longer use is left alone", () => {
		assert.isFalse(
			awardLabelsOutOfDate(
				[season("All-League", "ALL")],
				[{ ...allLeague, name: "All-NBA", shortName: "ANBA" }],
			),
		);
	});

	// The same abbrev on a team award and an individual one is not the same
	// award wearing an old label.
	test("a rename never crosses between team and individual awards", () => {
		assert.isFalse(
			awardLabelsOutOfDate(
				[{ awards: [{ shortName: "ALL", name: "All-League" }] }],
				[{ ...allLeague, name: "All-NBA" }],
			),
		);
	});

	// Nothing here reads an old-format row's fields, so a season synced from an
	// older build is not mistaken for a stale label.
	test("a row with no award list says nothing", () => {
		assert.isFalse(awardLabelsOutOfDate([{}], [allLeague]));
	});
});
