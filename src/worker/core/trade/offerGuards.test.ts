import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { player, team, trade } from "../index.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { offerPassesGuards } from "./betweenAiTeams.ts";
import { getLeagueTradeContext, getTradePosture } from "./tradePosture.ts";

import type { TradeTeams } from "../../../common/types.ts";

// The guards are one shared function with three callers (the AI-AI market,
// the proposals page, the trading block), so what is being pinned here is the
// shared behavior: a deal that betrays a team's timeline is refused wherever
// it comes from.

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

const makePlayer = ({ pid, tid, ovr, age, amount, exp }: any) => {
	const p: any = player.generate(
		tid,
		age,
		g.get("season") - age,
		true,
		DEFAULT_LEVEL,
	);
	p.pid = pid;
	p.born.year = g.get("season") - age;
	const r = p.ratings.at(-1);
	r.ovr = ovr;
	r.pot = ovr;
	p.value = ovr;
	p.valueNoPot = ovr;
	p.valueFuzz = ovr;
	p.valueNoPotFuzz = ovr;
	p.contract = { amount, exp };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	return p;
};

const posture = (tid: number, tier: string): any => ({ tid, tier });

const deal = (
	pidsA: number[],
	pidsB: number[],
	dpidsA: number[] = [],
	dpidsB: number[] = [],
): TradeTeams => [
	{ tid: 0, pids: pidsA, pidsExcluded: [], dpids: dpidsA, dpidsExcluded: [] },
	{ tid: 1, pids: pidsB, pidsExcluded: [], dpids: dpidsB, dpidsExcluded: [] },
];

describe("the shared deal guards", () => {
	beforeEach(async () => {
		changeTracker.disable();
		changeTracker.reset();
		resetG();
		g.setWithoutSavingToDB("numActiveTeams", 2);
		g.setWithoutSavingToDB("numTeams", 2);
		g.setWithoutSavingToDB("userTids", []);
		g.setWithoutSavingToDB("userTid", 0);

		const teams: any[] = [];
		for (let tid = 0; tid < 2; tid++) {
			teams.push(
				team.generate({
					tid,
					cid: 0,
					did: 0,
					region: `R${tid}`,
					name: `N${tid}`,
					abbrev: `T${tid}`,
					pop: 2,
					imgURL: "",
				} as any),
			);
		}
		const season = g.get("season");
		// Team 0: a rebuilder with a young player. Team 1: has an expensive
		// 33-year-old veteran. Multi-year contracts so no rental logic fires.
		const players = [
			makePlayer({
				pid: 1,
				tid: 0,
				ovr: 48,
				age: 22,
				amount: 2000,
				exp: season + 2,
			}),
			makePlayer({
				pid: 2,
				tid: 1,
				ovr: 60,
				age: 33,
				amount: 20000,
				exp: season + 2,
			}),
			// Filler so rosters are not empty after the hypothetical.
			makePlayer({
				pid: 3,
				tid: 0,
				ovr: 40,
				age: 25,
				amount: 2000,
				exp: season + 2,
			}),
			makePlayer({
				pid: 4,
				tid: 1,
				ovr: 40,
				age: 25,
				amount: 2000,
				exp: season + 2,
			}),
		];
		await resetCache({ players, teams, draftPicks: [] });
		stubLeagueDb();
		for (let tid = 0; tid < 2; tid++) {
			await idb.cache.teamSeasons.add(
				team.genSeasonRow((await idb.cache.teams.get(tid))!) as any,
			);
		}
		await idb.cache.draftPicks.add({
			dpid: 10,
			tid: 1,
			originalTid: 1,
			round: 1,
			pick: 0,
			season: season + 1,
		} as any);
	});

	test("a rebuilder does not take on a veteran for his own sake", async () => {
		const postures = new Map([
			[0, posture(0, "teardown")],
			[1, posture(1, "buyer")],
		]);
		// Team 0 (teardown) receives the 33-year-old, gives its young player,
		// gets no picks: refused, whoever suggested it.
		assert.strictEqual(
			await offerPassesGuards(deal([1], [2]), postures, g.get("season")),
			false,
		);
	});

	test("the same veteran moves when the rebuilder is paid in draft capital", async () => {
		// The vet's own team is a seller here - a contender would refuse to give
		// up its best player for a kid, and that refusal is the other test.
		const postures = new Map([
			[0, posture(0, "teardown")],
			[1, posture(1, "seller")],
		]);
		// Now the rebuilder also receives a first-round pick for absorbing him.
		assert.strictEqual(
			await offerPassesGuards(
				deal([1], [2], [], [10]),
				postures,
				g.get("season"),
			),
			true,
		);
	});
});

describe("proposing a trade to a smart front office", () => {
	const NUM = 6;

	const build = async () => {
		resetG();
		g.setWithoutSavingToDB("numActiveTeams", NUM);
		g.setWithoutSavingToDB("numTeams", NUM);
		g.setWithoutSavingToDB("userTids", [0]);
		g.setWithoutSavingToDB("userTid", 0);
		g.setWithoutSavingToDB("smartAiFrontOffice", true);

		const teams: any[] = [];
		for (let tid = 0; tid < NUM; tid++) {
			teams.push(
				team.generate({
					tid,
					cid: 0,
					did: 0,
					region: `R${tid}`,
					name: `N${tid}`,
					abbrev: `T${tid}`,
					pop: 2,
					imgURL: "",
				} as any),
			);
		}
		const season = g.get("season");
		const players: any[] = [];
		let pid = 1;
		for (let tid = 0; tid < NUM; tid++) {
			for (let i = 0; i < 10; i++) {
				players.push(
					makePlayer({
						pid: pid++,
						tid,
						ovr: 45 + (i % 5),
						age: 26 + (i % 6),
						amount: 4000,
						exp: season + 1 + (i % 3),
					}),
				);
			}
		}
		// Team 1's young cornerstone - the player its posture protects.
		const kid = makePlayer({
			pid: 999,
			tid: 1,
			ovr: 64,
			age: 21,
			amount: 4000,
			exp: season + 3,
		});
		players.push(kid);
		await resetCache({ players, teams, draftPicks: [] });
		stubLeagueDb();
		for (let tid = 0; tid < NUM; tid++) {
			const row: any = team.genSeasonRow((await idb.cache.teams.get(tid))!);
			row.season = season;
			row.won = 10 + tid * 10;
			row.lost = 82 - row.won;
			row.gp = 82;
			await idb.cache.teamSeasons.add(row);
		}
		return kid;
	};

	test("with the smart front office off, AI-AI trading still runs (stock path)", async () => {
		// Seeded, like every other simulation test here - the stock path rolls
		// dice constantly and an unseeded run is an unreproducible one.
		let rs = 424_242;
		const rng = () => {
			rs = (rs * 1_664_525 + 1_013_904_223) >>> 0;
			return rs / 4_294_967_296;
		};
		const realRandom = Math.random;
		Math.random = rng;
		try {
			await build();
			g.setWithoutSavingToDB("smartAiFrontOffice", false);
			g.setWithoutSavingToDB("aiTradesFactor", 5);

			// The stock path must not depend on any posture machinery - it should
			// simply run. Trades themselves are probabilistic, so the assertion is
			// that a bunch of attempts complete cleanly and rosters stay coherent.
			for (let i = 0; i < 10; i++) {
				await trade.betweenAiTeams();
			}
			for (let tid = 0; tid < NUM; tid++) {
				const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
				assert.isAbove(roster.length, 0);
			}
		} finally {
			Math.random = realRandom;
		}
	});

	test("its young cornerstone is not for sale at any calculated price", async () => {
		const kid = await build();

		// Make sure the posture really does protect him, so the test means
		// what it says.
		const context = await getLeagueTradeContext();
		const posture = await getTradePosture(1, context);
		assert.ok(
			posture.buildingBlockPids.includes(kid.pid),
			"fixture: the kid should be a building block",
		);

		// The user offers a mountain of value for him.
		const userPids = (
			await idb.cache.players.indexGetAll("playersByTid", 0)
		).map((p) => p.pid);
		await idb.cache.trade.add({
			rid: 0,
			teams: [
				{
					tid: 0,
					pids: userPids.slice(0, 4),
					pidsExcluded: [],
					dpids: [],
					dpidsExcluded: [],
				},
				{
					tid: 1,
					pids: [kid.pid],
					pidsExcluded: [],
					dpids: [],
					dpidsExcluded: [],
				},
			],
		} as any);

		const [accepted, message] = await trade.propose(false);
		assert.strictEqual(accepted, false);
		assert.ok(
			message?.includes("isn't going anywhere"),
			`unexpected message: ${message}`,
		);
	});
});
