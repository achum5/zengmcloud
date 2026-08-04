import { assert, beforeEach, describe, test } from "vitest";
import { PLAYER } from "../../../common/constants.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { player } from "../../core/index.ts";
import { idb } from "../index.ts";
import { g } from "../../util/index.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { coarsenRating } from "../../../common/coarsenRating.ts";

// The draft board shows two tables side by side: who is left, and who has gone.
// With "hide ratings ones digit, except prospects" on, a player used to cross
// between them and lose a digit on the way - 49/57 in the left table, 5/6 in
// the right one, the same scouting report a row apart. The exemption is about
// the report, and the report doesn't stop having been true the moment he's
// picked.

const SEASON = 2005;

const draftedThisYear = async () => {
	resetG();
	g.setWithoutSavingToDB("season", SEASON);
	g.setWithoutSavingToDB("hideRatingsOnesDigit", true);
	g.setWithoutSavingToDB("hideRatingsOnesDigitExceptProspects", true);

	const p = {
		pid: 0,
		...player.generate(PLAYER.UNDRAFTED, 19, SEASON, false, DEFAULT_LEVEL),
	};
	// Taken by a team, which is exactly the moment the old behavior changed.
	p.tid = 4;
	p.draft.year = SEASON;
	p.ratings[0]!.season = SEASON;
	await resetCache({ players: [p as any] });
	return p;
};

describe("a drafted player's draft-year ratings", () => {
	let raw: { ovr: number; pot: number };

	beforeEach(async () => {
		const p = await draftedThisYear();
		raw = { ovr: p.ratings[0]!.ovr, pot: p.ratings[0]!.pot };
	});

	test("stay exact with prospectSeasonsExact, as on the draft board", async () => {
		const [out] = await idb.getCopies.playersPlus(
			await idb.cache.players.getAll(),
			{
				attrs: ["pid"],
				ratings: ["ovr", "pot"],
				stats: [],
				season: SEASON,
				showRookies: true,
				showNoStats: true,
				prospectSeasonsExact: true,
			},
		);
		assert.strictEqual(out.ratings.ovr, raw.ovr);
		assert.strictEqual(out.ratings.pot, raw.pot);
	});

	test("are coarsened without it, which is what the pick used to cost him", async () => {
		const [out] = await idb.getCopies.playersPlus(
			await idb.cache.players.getAll(),
			{
				attrs: ["pid"],
				ratings: ["ovr", "pot"],
				stats: [],
				season: SEASON,
				showRookies: true,
				showNoStats: true,
			},
		);
		assert.strictEqual(out.ratings.ovr, coarsenRating(raw.ovr));
		assert.strictEqual(out.ratings.pot, coarsenRating(raw.pot));
	});
});
