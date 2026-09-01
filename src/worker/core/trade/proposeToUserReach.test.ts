import { assert, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { team } from "../index.ts";
import createRandomPlayers from "../league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import proposeToUser from "./proposeToUser.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";

// ---------------------------------------------------------------------------
// DOES THE SMART FRONT OFFICE ACTUALLY REACH THE PAGE?
//
// The proposals page asks proposeToUser for five offers and TOPS THE LIST UP
// from the old random generator when it gets fewer (see views/tradeProposals).
// That fallback is the right call - a short page is worse than a dull one -
// but it also means the whole feature can quietly stop reaching the player
// without anything failing: the page still shows five offers, they are just
// the pre-feature ones again. Nothing else in the tree would notice.
//
// The other tests in this directory pin the RULES the offers obey (who a team
// covets, what it will not trade, that a seed reproduces). This one pins that
// the offers arrive at all, and that they are worth looking at.
//
// Measured on a full thirty-team league, every team taking a turn as the user:
// the slate came out full for all thirty, and every one of the 150 offers was a
// package the user's own valuation would accept - median +0.02, which is
// makeItWork doing exactly what it should, stopping the instant the other side
// says yes rather than dressing the deal up.
// ---------------------------------------------------------------------------

const NUM_TEAMS = 30;
const NUM_OFFERS = 5;

const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

// proposeToUser reaches past the cache through isUntradable; the league store
// is not needed for anything it asks.
const stubLeagueDb = () => {
	const store = {
		index: () => store,
		getAll: async () => [],
		get: async () => undefined,
		async *iterate() {},
	};
	(idb as any).league = {
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
		getAll: async () => [],
		get: async () => undefined,
	};
};

const build = async () => {
	resetG();
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("smartAiFrontOffice", true);

	const teams: any[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push(
			team.generate({
				tid,
				cid: tid % 2,
				did: tid % 2,
				region: `R${tid}`,
				name: `N${tid}`,
				abbrev: `T${tid}`,
				pop: 2,
				imgURL: "",
			} as any),
		);
	}
	const players = await createRandomPlayers({
		activeTids: teams.map((t) => t.tid),
		onlyFreeAgents: false,
		scoutingLevel: DEFAULT_LEVEL,
		teams,
	});
	await resetCache({ players, teams, draftPicks: [] });
	stubLeagueDb();

	// Staggered records, so the thirty postures spread across every tier - a
	// league of identical teams would not exercise the motive ordering at all.
	const season = g.get("season");
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const row: any = team.genSeasonRow((await idb.cache.teams.get(tid))!);
		row.season = season;
		row.won = Math.round(12 + (58 * tid) / (NUM_TEAMS - 1));
		row.lost = 82 - row.won;
		row.gp = 82;
		await idb.cache.teamSeasons.add(row);
	}

	let dpid = 0;
	for (const round of [1, 2]) {
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			await idb.cache.draftPicks.add({
				dpid: dpid++,
				tid,
				originalTid: tid,
				round,
				pick: 0,
				season: season + 1,
			} as any);
		}
	}
};

describe("the offers a user actually gets", () => {
	test("the smart path fills the page on its own, for every team in the league", async () => {
		const realRandom = Math.random;
		Math.random = makeRng(4242);
		try {
			await build();

			const counts: number[] = [];
			const userDvs: number[] = [];
			for (let tid = 0; tid < NUM_TEAMS; tid++) {
				g.setWithoutSavingToDB("userTids", [tid]);
				g.setWithoutSavingToDB("userTid", tid);
				const offers = await proposeToUser({
					numOffers: NUM_OFFERS,
					seed: 1000 + tid,
				});
				counts.push(offers?.length ?? 0);

				for (const [userSide, aiSide] of offers ?? []) {
					// What an ordinary team would think of the offer - which is the
					// honest read of whether anything on the page is worth a click.
					const calc = new ValueChangeCalculator();
					userDvs.push(
						await calc.evaluate({
							tid: userSide.tid,
							pidsAdd: aiSide.pids,
							pidsRemove: userSide.pids,
							dpidsAdd: aiSide.dpids,
							dpidsRemove: userSide.dpids,
							tradingPartnerTid: undefined,
						}),
					);
				}
			}

			// The measurement was 30/30 full; the bar is set below that so an
			// unlucky league draw is not a failure, and well above the level
			// where the random fallback would be supplying most of the page.
			const mean = counts.reduce((a, c) => a + c, 0) / counts.length;
			assert.isAtLeast(
				mean,
				4.5,
				`the page is being topped up from the old random generator: ${counts.join(",")}`,
			);
			assert.isAtLeast(
				Math.min(...counts),
				3,
				`some team got almost nothing: ${counts.join(",")}`,
			);

			// makeItWork stops the moment the other side says yes, so an offer the
			// user's own valuation rejects means it was never assembled against
			// him at all.
			const rejected = userDvs.filter((dv) => dv < 0).length;
			assert.isAtMost(
				rejected / Math.max(1, userDvs.length),
				0.1,
				`${rejected} of ${userDvs.length} offers are ones the user's own valuation turns down`,
			);
		} finally {
			Math.random = realRandom;
		}
	}, 300000);
});
