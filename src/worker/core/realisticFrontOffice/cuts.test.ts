import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { player } from "../index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { frontOfficeCuts } from "./cuts.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;
const tid = 4;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(8);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9012);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

test(
	"a team over the limit cuts the players it can best do without",
	{ timeout: 120_000 },
	async () => {
		const freeAgents = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		).sort((a, b) => a.value - b.value);
		// The worst free agents join, two over the limit.
		const size = (await idb.cache.players.indexGetAll("playersByTid", tid))
			.length;
		for (const p of freeAgents.slice(0, g.get("maxRosterSize") - size + 2)) {
			await player.sign(p, tid, p.contract, g.get("phase"));
			await idb.cache.players.put(p);
		}
		const players = await idb.cache.players.indexGetAll("playersByTid", tid);
		const best = [...players]
			.sort((a, b) => b.valueNoPot - a.valueNoPot)
			.slice(0, 5)
			.map((p) => p.pid);
		const over = players.length - g.get("maxRosterSize");
		assert.isAbove(over, 0);

		const cut = await frontOfficeCuts(players, over);
		assert.strictEqual(cut.length, over);
		const left = await idb.cache.players.indexGetAll("playersByTid", tid);
		assert.strictEqual(left.length, g.get("maxRosterSize"));
		for (const pid of best) {
			assert.notInclude(cut, pid);
		}
	},
);
