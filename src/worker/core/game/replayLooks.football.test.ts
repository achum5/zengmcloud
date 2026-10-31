import { assert, beforeEach, test } from "vitest";
import { mockIDBLeague, resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { team } from "../index.ts";
import { takeReplayLooks } from "./replayLooks.ts";
import { changeTracker } from "../../db/changeTracker.ts";

// A football game's replay keeps each team's helmet and uniform, and the
// home team's stadium, as they were that day (see the 3D field).

beforeEach(() => {
	changeTracker.disable();
	changeTracker.reset();
});

test("each team's helmet, uniform and stadium kept as they were", async () => {
	resetG();
	g.setWithoutSavingToDB("numTeams", 2);
	g.setWithoutSavingToDB("numActiveTeams", 2);
	g.setWithoutSavingToDB("season", 2030);
	const teams = [0, 1].map((tid) =>
		team.generate({
			tid,
			cid: 0,
			did: 0,
			region: `R${tid}`,
			name: `T${tid}`,
			abbrev: `T${tid}`,
			pop: 3,
			popRank: tid + 1,
		}),
	);
	teams[0]!.helmet = { shell: "#b3995d", homeJersey: "#101820" };
	teams[0]!.stadium = { roof: "dome", sky: "night" };
	await resetCache({ teams });
	idb.league = mockIDBLeague();
	const looks = await takeReplayLooks([0, 1], []);
	assert.deepStrictEqual(looks.teams[0]?.helmet, {
		shell: "#b3995d",
		homeJersey: "#101820",
	});
	assert.deepStrictEqual(looks.teams[0]?.stadium, {
		roof: "dome",
		sky: "night",
	});
	// (A team that has set neither: its colors it is, then and now.)
	assert.isUndefined(looks.teams[1]?.helmet);
	assert.isUndefined(looks.teams[1]?.stadium);
});
