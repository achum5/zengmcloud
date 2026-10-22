import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { tradeDifficulty } from "../../util/difficulty.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { judgeHumanOffer } from "./humanTrades.ts";
import { currentOutlook } from "./market.ts";
import { retentionKey } from "./outlook.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const userTid = 0;
const cpuTid = 1;
const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(11);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9002);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", userTid);
	g.setWithoutSavingToDB("userTids", [userTid]);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

const roster = async (tid: number) =>
	(await idb.cache.players.indexGetAll("playersByTid", tid)).sort(
		(a, b) => b.valueNoPot - a.valueNoPot || a.pid - b.pid,
	);

test("a scrub for a star is refused", { timeout: 120_000 }, async () => {
	const mine = await roster(userTid);
	const theirs = await roster(cpuTid);
	const verdict = await judgeHumanOffer(
		{ tid: cpuTid, pids: [theirs[0]!.pid], dpids: [] },
		{ tid: userTid, pids: [mine.at(-1)!.pid], dpids: [] },
	);
	assert.strictEqual(verdict.kind, "no");
});

test("asking again gets the same answer", { timeout: 120_000 }, async () => {
	const mine = await roster(userTid);
	const theirs = await roster(cpuTid);
	const offer = [
		{ tid: cpuTid, pids: [theirs[3]!.pid], dpids: [] },
		{ tid: userTid, pids: [mine[2]!.pid], dpids: [] },
	] as const;
	const first = await judgeHumanOffer(offer[0], offer[1]);
	const second = await judgeHumanOffer(offer[0], offer[1]);
	assert.deepEqual(first, second);
});

test(
	"a star for a scrub is not turned down as a bad deal",
	{
		timeout: 120_000,
	},
	async () => {
		const mine = await roster(userTid);
		const theirs = await roster(cpuTid);
		const verdict = await judgeHumanOffer(
			{ tid: cpuTid, pids: [theirs.at(-1)!.pid], dpids: [] },
			{ tid: userTid, pids: [mine[0]!.pid], dpids: [] },
		);
		// Taken, unless another team would give more for the scrub - or it would
		// rather wait for the deadline.
		assert.notStrictEqual(verdict.kind, "no");
	},
);

test(
	"a player coming over is priced on how he would feel about his new team",
	{ timeout: 120_000 },
	async () => {
		const mine = await roster(userTid);
		const theirs = await roster(cpuTid);
		const outlook = await currentOutlook();
		const pid = mine[1]!.pid;
		await judgeHumanOffer(
			{ tid: cpuTid, pids: [theirs[1]!.pid], dpids: [] },
			{ tid: userTid, pids: [pid], dpids: [] },
		);
		assert.isTrue(outlook.retention.has(retentionKey(pid, cpuTid)));
		assert.isTrue(outlook.retention.has(retentionKey(theirs[1]!.pid, userTid)));
	},
);

test("difficulty does nothing to trades", () => {
	g.setWithoutSavingToDB("difficulty", 1);
	assert.strictEqual(tradeDifficulty(), 0);
});
