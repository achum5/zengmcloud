import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { team } from "../index.ts";
import createRandomPlayers from "../league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { getLeagueTradeContext, getTradePosture } from "./tradePosture.ts";
import proposeToUser, { covetWeight, proposerWeight } from "./proposeToUser.ts";

const NUM_TEAMS = 8;
const USER_TID = 0;

const makeRng = (seed: number) => {
	let s = seed >>> 0;
	return () => {
		s = (s * 1_664_525 + 1_013_904_223) >>> 0;
		return s / 4_294_967_296;
	};
};

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
	g.setWithoutSavingToDB("userTids", [USER_TID]);
	g.setWithoutSavingToDB("userTid", USER_TID);

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

	// Staggered records so the postures spread across tiers.
	const season = g.get("season");
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const row: any = team.genSeasonRow((await idb.cache.teams.get(tid))!);
		row.season = season;
		row.won = Math.round(12 + (58 * tid) / (NUM_TEAMS - 1));
		row.lost = 82 - row.won;
		row.gp = 82;
		await idb.cache.teamSeasons.add(row);
	}

	// A pick per team per round so pick-based offers are possible.
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

describe("what a team covets on the user's roster", () => {
	const base = { value: 50, age: 27, pos: "SF", ovr: 55 };
	const posture = (overrides: any = {}): any => ({
		tier: "buyer",
		aggression: 0.5,
		elite: false,
		starGap: false,
		needs: [],
		surpluses: [],
		targetPos: undefined,
		shopVeteranPids: [],
		...overrides,
	});

	test("a star hunter covets the user's star most", () => {
		const hunter = posture({ tier: "allIn", starGap: true });
		const star = { ...base, value: 70, ovr: 70 };
		const role = { ...base, value: 55, ovr: 58 };
		assert.isAbove(
			covetWeight({ posture: hunter, p: star, starOvr: 65 }),
			2 * covetWeight({ posture: hunter, p: role, starOvr: 65 }),
		);
	});

	test("a rebuilder has no appetite for the user's 32-year-old", () => {
		const seller = posture({ tier: "teardown" });
		const vet = { ...base, age: 32, value: 60, ovr: 62 };
		const kid = { ...base, age: 22, value: 45, ovr: 48 };
		assert.isAbove(
			covetWeight({ posture: seller, p: kid, starOvr: 65 }),
			covetWeight({ posture: seller, p: vet, starOvr: 65 }),
		);
	});

	test("a hole at a position raises the call about the user's player there", () => {
		const needsBigs = posture({ needs: [{ pos: "C", severity: 12 }] });
		const centre = { ...base, pos: "C" };
		const guard = { ...base, pos: "PG" };
		assert.isAbove(
			covetWeight({ posture: needsBigs, p: centre, starOvr: 65 }),
			covetWeight({ posture: needsBigs, p: guard, starOvr: 65 }),
		);
	});

	test("motivated teams call more", () => {
		assert.isAbove(
			proposerWeight({
				posture: posture({ aggression: 0.8, shopVeteranPids: [1] }),
				bestCovet: 1,
			}),
			proposerWeight({ posture: posture({ aggression: 0.2 }), bestCovet: 0.5 }),
		);
	});
});

describe("proposals from the same brain as AI-AI trades", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("offers are credible, guarded, and deterministic", async () => {
		const rng = makeRng(777);
		const realRandom = Math.random;
		Math.random = rng;
		try {
			await build();
			g.setWithoutSavingToDB("smartAiFrontOffice", true);

			const offers = await proposeToUser({ numOffers: 5, seed: 12345 });
			assert.ok(offers, "the smart path should produce offers");
			assert.isAbove(offers!.length, 0);

			// The user side comes first - that is the page's convention.
			for (const offer of offers!) {
				assert.strictEqual(offer[0].tid, USER_TID);
				assert.notStrictEqual(offer[1].tid, USER_TID);
			}

			// No AI team offers a player its own posture calls untouchable.
			const context = await getLeagueTradeContext();
			for (const offer of offers!) {
				const posture = await getTradePosture(offer[1].tid, context);
				const blocks = new Set(posture.buildingBlockPids);
				for (const pid of offer[1].pids) {
					assert.notOk(
						blocks.has(pid),
						`team ${offer[1].tid} offered its building block ${pid}`,
					);
				}
			}

			// The same seed reproduces the same offers (the page must not
			// reshuffle on every render).
			const again = await proposeToUser({ numOffers: 5, seed: 12345 });
			assert.deepStrictEqual(again, offers);
		} finally {
			Math.random = realRandom;
		}
	}, 60000);

	test("the off switch really is off", async () => {
		const rng = makeRng(777);
		const realRandom = Math.random;
		Math.random = rng;
		try {
			await build();
			g.setWithoutSavingToDB("smartAiFrontOffice", false);
			assert.strictEqual(
				await proposeToUser({ numOffers: 5, seed: 12345 }),
				undefined,
			);
		} finally {
			Math.random = realRandom;
		}
	});
});
