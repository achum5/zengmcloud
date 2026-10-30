import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { contractOptionsFor } from "../contractNegotiation/generateContractOptions.ts";
import { player } from "../index.ts";
import moodInfo from "../player/moodInfo.ts";
import { realisticFreeAgencyOn } from "./isOn.ts";
import { instantSigningRefusal } from "./offerScreen.ts";
import { stayChance } from "./offers.ts";
import { marketAsks, reSignAsk, signingDay } from "./stockRules.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const userTid = 0;
const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(41);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9031);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", userTid);
	g.setWithoutSavingToDB("userTids", [userTid]);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("realisticFreeAgency", false);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

test("with Realistic Free Agency off, humans sign and re-sign as in stock", async () => {
	assert.isFalse(realisticFreeAgencyOn());
	const [p] = await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	);
	assert.isUndefined(await instantSigningRefusal(p!.pid));
});

test("players judge every team on the same terms", async () => {
	const p = (await idb.cache.players.indexGetAll("playersByTid", 3)).find(
		(x) => x.value > 55,
	)!;
	const willing = async () => (await moodInfo(p, 3)).probWilling;
	const sameTerms = await willing();
	g.setWithoutSavingToDB("realisticFrontOffices", false);
	try {
		// Stock: a CPU team's players test free agency more than a human's.
		assert.isBelow(await willing(), sameTerms);
	} finally {
		g.setWithoutSavingToDB("realisticFrontOffices", true);
	}
	// A CPU team forecasts his staying by his willingness.
	assert.strictEqual(await stayChance(p, 3), sameTerms);
});

test(
	"CPU teams sign free agents willing to sign with them, at one of their asks",
	{ timeout: 300_000 },
	async () => {
		// A good player let go (the league's own pool is all minimum players,
		// nobody's worth signing).
		const star = (await idb.cache.players.indexGetAll("playersByTid", 9)).sort(
			(a, b) => b.value - a.value,
		)[0]!;
		player.addToFreeAgents(star, {});
		await idb.cache.players.put(star);

		const signed = [];
		for (let day = 0; day < 10 && signed.length === 0; day++) {
			signed.push(...(await signingDay()));
		}
		assert.include(
			signed.map((x) => x.pid),
			star.pid,
		);
		for (const { pid, tid, contract, ask } of signed) {
			assert.notStrictEqual(tid, userTid);
			const p = (await idb.cache.players.get(pid))!;
			assert.strictEqual(p.tid, tid);
			// One of his asks of that team, and he was willing to sign there.
			assert.include(
				contractOptionsFor(
					{ amount: ask.amount / 1000, exp: ask.exp },
					p.ratings.at(-1)!.ovr,
				).map((x) => Math.round(x.amount * 1000)),
				contract.amount,
			);
			assert.isTrue(
				(await moodInfo({ ...p, tid: PLAYER.FREE_AGENT }, tid)).willing,
			);
		}
	},
);

test(
	"a CPU team re-signs a good player willing to stay, not a bad one asking the max",
	{ timeout: 300_000 },
	async () => {
		const tid = 7;
		const roster = (
			await idb.cache.players.indexGetAll("playersByTid", tid)
		).sort((a, b) => b.value - a.value);
		const payrollWithout = (pid: number) =>
			roster
				.filter((p) => p.pid !== pid)
				.reduce((a, p) => a + p.contract.amount, 0);
		const best = roster[0]!;
		const worst = roster.at(-1)!;
		const kept = await reSignAsk(
			best,
			payrollWithout(best.pid),
			best.contract.amount,
		);
		assert.isDefined(kept);
		assert.isUndefined(
			await reSignAsk(worst, payrollWithout(worst.pid), g.get("maxContract")),
		);
	},
);

test(
	"as re-signing opens, players whose contracts are up ask what the market would pay",
	{ timeout: 300_000 },
	async () => {
		g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
		try {
			const expiring = (
				await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
			)
				.filter((p) => p.contract.exp <= g.get("season") && p.value > 55)
				.sort((a, b) => b.value - a.value)
				.slice(0, 8);
			assert.isAbove(expiring.length, 0);
			const before = expiring.map((p) => p.contract.amount);
			await marketAsks(expiring.map((p) => p.pid));
			const after = await Promise.all(
				expiring.map(
					async (p) => (await idb.cache.players.get(p.pid))!.contract.amount,
				),
			);
			assert.notDeepEqual(after, before);
			for (const amount of after) {
				assert.isAtLeast(amount, g.get("minContract"));
				assert.isAtMost(amount, g.get("maxContract"));
			}
		} finally {
			g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		}
	},
);
