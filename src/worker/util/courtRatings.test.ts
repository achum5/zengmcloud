import { assert, test } from "vitest";
import { DEFAULT_LEVEL } from "../../common/budgetLevels.ts";
import { resetCache, resetG } from "../../test/helpers.ts";
import { player } from "../core/index.ts";
import { courtRatingsFor } from "./courtRatings.ts";

// The 3D game plays a man by his ratings as of the game's season.
test("a man's ratings for the 3D game are his for that season", async () => {
	resetG();
	const p = player.generate(0, 25, 2014, true, DEFAULT_LEVEL);
	p.pid = 7;
	p.ratings = [
		{ ...p.ratings[0]!, season: 2014, spd: 40, tp: 30 },
		{ ...p.ratings[0]!, season: 2015, spd: 60, tp: 50 },
		{ ...p.ratings[0]!, season: 2016, spd: 80, tp: 70 },
	] as unknown as typeof p.ratings;
	await resetCache({ players: [p] });
	const got = await courtRatingsFor([7], 2015);
	assert.deepEqual(Object.keys(got), ["7"]);
	assert.strictEqual(got[7]!.spd, 60);
	assert.strictEqual(got[7]!.tp, 50);
	assert.isNumber(got[7]!.drb);
	// (A season before any of his: his first.)
	assert.strictEqual((await courtRatingsFor([7], 2010))[7]!.spd, 40);
});
