import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { judgeHumanOffer } from "./humanTrades.ts";
import tradeProposals from "../../views/tradeProposals.ts";
import { currentOutlook, loadTeam, tableAssets } from "./market.ts";
import {
	closeWindows,
	isNotable,
	isReserved,
	loadReserved,
	placeBid,
} from "./windows.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const userTid = 0;
const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(13);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9013);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", userTid);
	g.setWithoutSavingToDB("userTids", [userTid]);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

const picks = async (tid: number, round: number) =>
	(await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid))
		.filter((dp) => dp.round === round && typeof dp.season === "number")
		.sort((a, b) => Number(a.season) - Number(b.season) || a.dpid - b.dpid);

// Holds a deal: `tids[0]` gives `give0`, `tids[1]` gives `give1`.
const hold = async (
	tids: [number, number],
	give0: { pids: number[]; dpids: number[] },
	give1: { pids: number[]; dpids: number[] },
) => {
	const t = (await idb.cache.teams.get(tids[0]))!;
	t.pendingTrade = { tids, give: [give0, give1], day: "earlier", bids: [] };
	await idb.cache.teams.put(t);
	await loadReserved();
};

const nothing = { pids: [], dpids: [] };

test(
	"a first-round pick or a top-five player makes a deal notable",
	{ timeout: 120_000 },
	async () => {
		const outlook = await currentOutlook();
		const sides = [
			await loadTeam(outlook, 1),
			await loadTeam(outlook, 2),
		] as const;
		const best = [...sides[0].players].sort(
			(a, b) => b.valueNoPot - a.valueNoPot,
		);
		const first = sides[1].picks.find((dp) => dp.round === 1)!;
		const second = sides[1].picks.find((dp) => dp.round === 2)!;
		const notable = (assets: Parameters<typeof isNotable>[0]["assets"]) =>
			isNotable({ assets }, sides, 1);
		assert.isTrue(notable([[{ kind: "player", p: best[0]! }], []]));
		assert.isTrue(notable([[], [{ kind: "pick", dp: first }]]));
		assert.isFalse(
			notable([
				[{ kind: "player", p: best.at(-1)! }],
				[{ kind: "pick", dp: second }],
			]),
		);
	},
);

test(
	"a held deal's assets are off the table, and an offer for some of them is refused",
	{ timeout: 120_000 },
	async () => {
		const outlook = await currentOutlook();
		const a = await loadTeam(outlook, 3);
		const [p1, p2] = [...a.tradablePlayers].sort(
			(x, y) => y.valueNoPot - x.valueNoPot,
		);
		await hold([3, 4], { pids: [p1!.pid, p2!.pid], dpids: [] }, nothing);
		const keys = tableAssets(await loadTeam(outlook, 3)).map((x) =>
			x.kind === "player" ? x.p.pid : -1,
		);
		assert.notInclude(keys, p1!.pid);
		assert.notInclude(keys, p2!.pid);

		const mine = await picks(userTid, 1);
		const verdict = await judgeHumanOffer(
			{ tid: 3, pids: [p1!.pid], dpids: [] },
			{ tid: userTid, pids: [], dpids: mine.map((dp) => dp.dpid) },
		);
		assert.strictEqual(verdict.kind, "held");

		// Its window closes on the next trading day.
		await closeWindows();
		assert.isUndefined((await idb.cache.teams.get(3))!.pendingTrade);
		assert.isFalse(isReserved({ kind: "player", p: p1! }));
	},
);

test(
	"a human's bid that beats the deal takes what that side gives",
	{ timeout: 120_000 },
	async () => {
		// A team gives a player for nothing; the human bids its best player, on
		// about the same salary, to a team that would take that trade on its
		// own. Which team that is depends on the draw - a rebuilding side wants
		// none of a 29-year-old's contract - and the draw moves whenever
		// anything in player generation does, so it is searched for rather than
		// named. Teams 3 and 4 held a deal above, so the search starts past them.
		const best = (
			await idb.cache.players.indexGetAll("playersByTid", userTid)
		).sort((a, b) => b.valueNoPot - a.valueNoPot)[0]!;
		const human = { tid: userTid, pids: [best.pid], dpids: [] };
		let found: { tid: number; pid: number } | undefined;
		for (let tid = 5; tid < g.get("numTeams") - 1 && !found; tid++) {
			const candidates = (
				await idb.cache.players.indexGetAll("playersByTid", tid)
			)
				.filter(
					(p) =>
						p.valueNoPot < best.valueNoPot - 10 &&
						Math.abs(p.contract.amount - best.contract.amount) <
							0.15 * best.contract.amount,
				)
				.sort((a, b) => a.valueNoPot - b.valueNoPot);
			for (const p of candidates) {
				const alone = await judgeHumanOffer(
					{ tid, pids: [p.pid], dpids: [] },
					human,
				);
				if (alone.kind === "yes" || alone.kind === "wait") {
					found = { tid, pid: p.pid };
					break;
				}
			}
		}
		assert.isDefined(found, "no team would take the human's best player");
		const { tid, pid } = found!;
		const theirs = (await idb.cache.players.get(pid))!;
		await hold([tid, tid + 1], { pids: [theirs.pid], dpids: [] }, nothing);
		const cpu = { tid, pids: [theirs.pid], dpids: [] };
		const verdict = await judgeHumanOffer(cpu, human);
		assert.strictEqual(verdict.kind, "bid");
		assert.isTrue(await placeBid(cpu, human));

		await closeWindows();
		assert.strictEqual((await idb.cache.players.get(theirs.pid))!.tid, userTid);
		assert.strictEqual((await idb.cache.players.get(best.pid))!.tid, tid);
	},
);

test(
	"a pending deal shows each player's ratings, and why each team makes it",
	{ timeout: 120_000 },
	async () => {
		const outlook = await currentOutlook();
		const a = await loadTeam(outlook, 5);
		const [p1] = [...a.tradablePlayers].sort(
			(x, y) => y.valueNoPot - x.valueNoPot,
		);
		const dp = (await picks(6, 1))[0]!;
		const t = (await idb.cache.teams.get(5))!;
		t.pendingTrade = {
			tids: [5, 6],
			give: [
				{ pids: [p1!.pid], dpids: [] },
				{ pids: [], dpids: [dp.dpid] },
			],
			day: "earlier",
			bids: [],
			notes: [
				{ why: "Rebuilding, for later", wants: "Young players and picks" },
				{
					why: "A contender adding for a title run",
					wants: "Players who help now",
				},
			],
		};
		await idb.cache.teams.put(t);
		await loadReserved();
		const data = (await tradeProposals.load({
			inputs: {},
			updateEvents: new Set(["firstRun"]),
			prevInputs: undefined,
			prevOutput: {},
			conditions: {},
		} as any)) as any;
		const deal = data.pending.find((x: any) => x.tids[0] === 5);
		assert.isDefined(deal);
		assert.strictEqual(deal.notes[1].wants, "Players who help now");
		const shown = deal.summary.teams[0].trade[0];
		assert.strictEqual(shown.pid, p1!.pid);
		assert.isNumber(shown.age);
		assert.isNumber(shown.ratings.ovr);
		assert.isNumber(shown.ratings.pot);
		await closeWindows();
	},
);
