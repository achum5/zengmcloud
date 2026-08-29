import { assert, describe, test } from "vitest";
import { liveSimCourt } from "./liveGame.ts";
import type { CourtStyle } from "../../common/types.ts";

// ---------------------------------------------------------------------------
// A SCRIMMAGE PLAYED ON HALF OF SOMEBODY ELSE'S COURT.
//
// The live-game floor is the home team's custom court, found by tid. That is
// right for a league game and wrong for every synthetic one, because a
// synthetic game's tid is an ARRAY INDEX: an intrasquad scrimmage numbers its
// two squads 0 and 1 so the sim can tell them apart, and an exhibition numbers
// its two sides the same way.
//
// So the field report: a Cleveland intrasquad scrimmage drawn with Cleveland's
// logo and Cleveland's colors - which come from the override, and were right -
// on Boston's parquet, green key and TD Garden rails, which came from looking
// up tid 1. Half one court, half another, and nothing in the game had anything
// to do with Boston.
// ---------------------------------------------------------------------------

const CLEVELAND: CourtStyle = { paint: "#860038", floorPattern: "hardwood" };
const BOSTON: CourtStyle = { paint: "#007a33", floorPattern: "parquet" };

describe("liveSimCourt", () => {
	// The ordinary league game, unchanged: no override, so the team the tid
	// names owns the floor.
	test("a league game is played on the home team's own court", () => {
		assert.deepStrictEqual(
			liveSimCourt({ override: undefined, teamCourt: BOSTON }),
			BOSTON,
		);
		assert.strictEqual(
			liveSimCourt({ override: undefined, teamCourt: undefined }),
			undefined,
		);
	});

	test("REGRESSION: a scrimmage is played on the scrimmaging team's court", () => {
		// Squad 1 of a Cleveland scrimmage: numbered tid 1, which is Boston.
		assert.deepStrictEqual(
			liveSimCourt({
				override: { region: "Cleveland", court: CLEVELAND },
				teamCourt: BOSTON,
			}),
			CLEVELAND,
		);
	});

	// An exhibition between two historical teams has no court to speak of, and
	// the honest answer is a neutral floor - NOT whichever court the league's
	// first two teams happen to own today.
	test("an override with no court means no court, not the tid's", () => {
		assert.strictEqual(
			liveSimCourt({ override: { region: "Chicago" }, teamCourt: BOSTON }),
			undefined,
		);
	});
});
