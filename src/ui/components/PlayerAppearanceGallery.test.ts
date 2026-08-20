import { assert, describe, test } from "vitest";
import {
	formatSeasonRange,
	groupSeasonsByUniform,
	stintLabel,
	type AppearanceTeam,
} from "./PlayerAppearanceGallery.tsx";

const team = (
	overrides: Partial<AppearanceTeam> & Pick<AppearanceTeam, "abbrev">,
): AppearanceTeam => ({
	colors: ["#000000", "#ffffff", "#cccccc"],
	name: "Team",
	region: "City",
	...overrides,
});

const BOS = team({ abbrev: "BOS", region: "Boston", name: "Celtics" });
const LAL = team({ abbrev: "LAL", region: "Los Angeles", name: "Lakers" });

describe("groupSeasonsByUniform", () => {
	test("a one-team career is one stint", () => {
		const stints = groupSeasonsByUniform([2009, 2010, 2011], {
			2009: BOS,
			2010: BOS,
			2011: BOS,
		});
		assert.strictEqual(stints.length, 1);
		assert.deepStrictEqual(stints[0]!.seasons, [2009, 2010, 2011]);
		assert.strictEqual(stints[0]!.team, BOS);
	});

	test("a trade splits the career where the uniform changed", () => {
		const stints = groupSeasonsByUniform([2009, 2010, 2011, 2012], {
			2009: BOS,
			2010: BOS,
			2011: LAL,
			2012: LAL,
		});
		assert.deepStrictEqual(
			stints.map((stint) => [stint.team?.abbrev, stint.seasons]),
			[
				["BOS", [2009, 2010]],
				["LAL", [2011, 2012]],
			],
		);
	});

	// Same team, new number: the player page already groups jersey numbers this
	// way, and the two displays should not disagree about what a stint is.
	test("a number change splits a stint on one team", () => {
		const stints = groupSeasonsByUniform([2009, 2010, 2011], {
			2009: { ...BOS, jerseyNumber: "9" },
			2010: { ...BOS, jerseyNumber: "9" },
			2011: { ...BOS, jerseyNumber: "34" },
		});
		assert.deepStrictEqual(
			stints.map((stint) => [stint.team?.jerseyNumber, stint.seasons]),
			[
				["9", [2009, 2010]],
				["34", [2011]],
			],
		);
	});

	// A rebrand is a real uniform change even though the franchise did not move.
	test("a rebrand splits the stint", () => {
		const stints = groupSeasonsByUniform([2009, 2010], {
			2009: BOS,
			2010: { ...BOS, colors: ["#ff0000", "#ffffff", "#000000"] },
		});
		assert.strictEqual(stints.length, 2);
	});

	// Consecutive seasons only. Leaving and coming back is two stints, because
	// merging them would put seasons in a group whose range lies about them.
	test("a return to an old team is a separate stint", () => {
		const stints = groupSeasonsByUniform([2009, 2010, 2011], {
			2009: BOS,
			2010: LAL,
			2011: BOS,
		});
		assert.deepStrictEqual(
			stints.map((stint) => stint.team?.abbrev),
			["BOS", "LAL", "BOS"],
		);
	});

	test("seasons on nobody's roster group together, with no team", () => {
		const stints = groupSeasonsByUniform([2007, 2008, 2009], { 2009: BOS });
		assert.deepStrictEqual(
			stints.map((stint) => [stint.team?.abbrev, stint.seasons]),
			[
				[undefined, [2007, 2008]],
				["BOS", [2009]],
			],
		);
	});

	test("no team data at all is a single unlabeled stint", () => {
		const stints = groupSeasonsByUniform([2009, 2010], undefined);
		assert.strictEqual(stints.length, 1);
		assert.strictEqual(stints[0]!.team, undefined);
	});

	test("no seasons is no stints", () => {
		assert.deepStrictEqual(groupSeasonsByUniform([], {}), []);
	});
});

describe("stintLabel", () => {
	test("a team stint reads as the team", () => {
		assert.strictEqual(
			stintLabel({ team: BOS, seasons: [2009] }, 0),
			"Boston Celtics",
		);
	});

	// The seasons before a player is drafted are the only ones he could not have
	// been on a roster for. "No team" reads like he went unsigned; he hadn't
	// entered the league yet.
	test("teamless seasons at the start of a career are the scouting pool", () => {
		assert.strictEqual(
			stintLabel({ seasons: [2007, 2008] }, 0),
			"Draft prospect",
		);
	});

	// Mid-career, teamless means exactly what it says.
	test("a teamless stretch later on is a year out of the league", () => {
		assert.strictEqual(stintLabel({ seasons: [2013] }, 2), "No team");
	});
});

describe("formatSeasonRange", () => {
	test("a span reads as a span", () => {
		assert.strictEqual(formatSeasonRange([2009, 2010, 2011]), "2009-2011");
	});

	test("one season is just the season", () => {
		assert.strictEqual(formatSeasonRange([2009]), "2009");
	});

	test("nothing to describe", () => {
		assert.strictEqual(formatSeasonRange([]), "");
	});
});
