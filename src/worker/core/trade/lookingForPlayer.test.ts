import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import {
	getContractExp,
	getCurrentContract,
	getNearestContractAmount,
} from "../../../common/contracts.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { tradingBlockTeams } from "../realisticFrontOffice/proposals.ts";
import { vanillaPlusSettings } from "../vanillaPlus/settings.ts";
import { tradeValueCalculator } from "../vanillaPlus/index.ts";
import { isWanted, paidTooMuch, perOf } from "./lookingForPlayer.ts";
import makeItWork from "./makeItWork.ts";

// The trading block, asked for a player of some sort: a PER of at least so
// much, under contract through a season, paid at most so much.

const importNode = (name: string) => import(/* @vite-ignore */ name);
const userTid = 0;
const realRandom = Math.random;
const vanillaPlusWas = vanillaPlusSettings.on;

beforeAll(async () => {
	vanillaPlusSettings.on = false;
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(21);
	(idb as any).league = undefined;
	await createArenaLeague(9011);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", userTid);
	g.setWithoutSavingToDB("userTids", [userTid]);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	vanillaPlusSettings.on = vanillaPlusWas;
	Math.random = realRandom;
});

const paid = (p: any) =>
	getNearestContractAmount(getCurrentContract(p)!, g.get("season"));
const through = (p: any) => getContractExp(getCurrentContract(p)!);

test("a player is what was asked for only if he is all of it", async () => {
	const [p0] = await idb.cache.players.indexGetAll("playersByTid", 1);
	const p = helpers.deepCopy(p0!);
	// His PER: this season's, or the last he had - never a playoff one, nor a
	// season he has not played in.
	p.stats = [
		{ season: 1, playoffs: false, gp: 60, per: 17 },
		{ season: 1, playoffs: true, gp: 8, per: 30 },
		{ season: 2, playoffs: false, gp: 0, per: 0 },
	] as any;
	assert.strictEqual(perOf(p), 17);
	assert.isTrue(isWanted(p, { per: 15 }));
	assert.isFalse(isWanted(p, { per: 18 }));
	p.stats = [];
	assert.isFalse(isWanted(p, { per: 0 }));
	assert.isTrue(isWanted(p, {}));
	// Through a season, and paid at most so much.
	assert.isTrue(isWanted(p, { through: through(p) }));
	assert.isFalse(isWanted(p, { through: through(p) + 1 }));
	assert.isTrue(isWanted(p, { salaryMost: paid(p) }));
	assert.isFalse(isWanted(p, { salaryMost: paid(p) - 1 }));
	assert.isTrue(paidTooMuch(p, { through: 1, salaryMost: paid(p) - 1 }));
	assert.isFalse(paidTooMuch(p, { through: 9999 }));
});

test(
	"asked for a player, every bid holds one - and nobody paid more than asked",
	{ timeout: 300_000 },
	async () => {
		const mine = (
			await idb.cache.players.indexGetAll("playersByTid", userTid)
		).sort((a, b) => b.valueNoPot - a.valueNoPot);
		const pid = mine[0]!.pid;
		const season = g.get("season");
		const want = { through: season + 1, salaryMost: 15000 };
		const lookingFor = {
			positions: new Set<string>(),
			skills: new Set<string>(),
			draftPicks: false,
			prospects: false,
			bestCurrentPlayers: false,
			player: want,
		};
		const check = async (cpuPids: number[]) => {
			const theirs = await Promise.all(
				cpuPids.map((x) => idb.cache.players.get(x)),
			);
			for (const p of theirs) {
				assert.isAtMost(paid(p), want.salaryMost, `${p!.pid} paid`);
			}
			assert.isTrue(
				theirs.some((p) => through(p) >= want.through),
				"one through the season asked",
			);
		};

		// The title-odds engine's bids (realistic front offices).
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const bids = await tradingBlockTeams(userTid, [pid], [], lookingFor);
		assert.isAbove(bids.length, 0);
		for (const [, cpu] of bids) {
			await check(cpu.pids);
		}

		// The stock ones.
		g.setWithoutSavingToDB("realisticFrontOffices", false);
		const valueChangeCalculator = await tradeValueCalculator();
		let stock = 0;
		for (const tid of [1, 2, 3, 4, 5, 6]) {
			const teams = await makeItWork(
				[
					{
						tid: userTid,
						pids: [pid],
						pidsExcluded: [],
						dpids: [],
						dpidsExcluded: [],
					},
					{ tid, pids: [], pidsExcluded: [], dpids: [], dpidsExcluded: [] },
				],
				{
					holdUserConstant: true,
					maxAssetsToAdd: 5,
					lookingFor,
					valueChangeCalculator,
				},
			);
			if (teams && teams[1].pids.length > 0) {
				stock += 1;
				await check(teams[1].pids);
			}
		}
		assert.isAbove(stock, 0);
	},
);
