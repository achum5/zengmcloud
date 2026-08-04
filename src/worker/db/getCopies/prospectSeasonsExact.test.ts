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

// The other half of the same feature: a rookie's PROGS. His draft-year row is
// exact and his first pro row is floored, and the change used to be zeroed
// rather than risk subtracting across the two scales - so a rookie showed no
// movement anywhere, while the roster (which fetches true ratings and coarsens
// on the way out) showed his +1 the whole time.
describe("a rookie's first pro season still reports progs", () => {
	const SEASON = 2006;

	const drafted2005 = async ({
		ovr2005,
		ovr2006,
	}: {
		ovr2005: number;
		ovr2006: number;
	}) => {
		resetG();
		g.setWithoutSavingToDB("season", SEASON);
		g.setWithoutSavingToDB("hideRatingsOnesDigit", true);
		g.setWithoutSavingToDB("hideRatingsOnesDigitExceptProspects", true);

		const p: any = {
			pid: 0,
			...player.generate(PLAYER.UNDRAFTED, 19, 2005, false, DEFAULT_LEVEL),
		};
		p.tid = 4;
		p.draft.year = 2005;

		const base = p.ratings[0];
		p.ratings = [
			{ ...base, season: 2005, ovr: ovr2005, pot: ovr2005 + 10, fuzz: 0 },
			{ ...base, season: SEASON, ovr: ovr2006, pot: ovr2006 + 10, fuzz: 0 },
		];
		await resetCache({ players: [p] });
		return p;
	};

	const fetch = async () =>
		(
			await idb.getCopies.playersPlus(await idb.cache.players.getAll(), {
				attrs: ["pid"],
				ratings: ["ovr", "pot", "dovr", "dpot"],
				stats: [],
				season: SEASON,
				showRookies: true,
				showNoStats: true,
				prospectSeasonsExact: true,
			})
		)[0];

	test("the tens digit's move across the year flip is the prog", async () => {
		// 68 -> 71: the displayed rating goes 6 -> 7, so +1.
		await drafted2005({ ovr2005: 68, ovr2006: 71 });
		const out = await fetch();
		assert.strictEqual(out.ratings.ovr, 7);
		assert.strictEqual(out.ratings.dovr, 1);
		assert.strictEqual(out.ratings.dpot, 1);
	});

	test("a prog inside one decade reports nothing, same as any other player", async () => {
		// 61 -> 68 is real movement, but the tens digit never moves, so the
		// indicator stays quiet - exactly how it behaves for a veteran.
		await drafted2005({ ovr2005: 61, ovr2006: 68 });
		const out = await fetch();
		assert.strictEqual(out.ratings.ovr, 6);
		assert.strictEqual(out.ratings.dovr, 0);
	});

	test("a drop across the boundary is reported too, not swallowed", async () => {
		await drafted2005({ ovr2005: 71, ovr2006: 68 });
		const out = await fetch();
		assert.strictEqual(out.ratings.ovr, 6);
		assert.strictEqual(out.ratings.dovr, -1);
	});
});
