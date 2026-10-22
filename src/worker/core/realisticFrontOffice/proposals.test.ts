import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { judgeHumanOffer } from "./humanTrades.ts";
import { currentOutlook, loadTeam } from "./market.ts";
import {
	bestProposal,
	counterOffer,
	publicGain,
	publicPlayer,
	publicTeam,
	refreshProposals,
	tradingBlockTeams,
} from "./proposals.ts";
import {
	declineProposal,
	dropMovedProposals,
	proposalKey,
	proposalsTo,
} from "./proposalStore.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const userTid = 0;
const cpuTid = 1;
const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(12);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9003);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", userTid);
	g.setWithoutSavingToDB("userTids", [userTid]);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

const side = (tid: number, assets: { kind: string; p?: any; dp?: any }[]) => ({
	tid,
	pids: assets.flatMap((x) => (x.kind === "player" ? [x.p.pid] : [])),
	dpids: assets.flatMap((x) => (x.kind === "pick" ? [x.dp.dpid] : [])),
});

test("a human sees his players' ratings with the scouting fuzz on them", async () => {
	const [p] = await idb.cache.players.indexGetAll("playersByTid", cpuTid);
	const seen = publicPlayer(p!);
	const r = p!.ratings.at(-1)!;
	const shown = seen.ratings.at(-1)!;
	assert.strictEqual(shown.fuzz, r.fuzz === 0 ? r.fuzz : 0);
	assert.strictEqual(
		shown.hgt,
		Math.round(Math.min(100, Math.max(0, r.hgt + r.fuzz))),
	);
	assert.strictEqual(seen.valueNoPot, p!.valueNoPotFuzz);
});

// The first CPU team with a standing proposal for the user.
let proposer: number | undefined;

test(
	"a CPU team's proposal is one it would take, and fair on what the human sees",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
		const human = await loadTeam(outlook, userTid);
		const pub = publicTeam(outlook, human);
		let found = 0;
		for (let tid = 1; tid < g.get("numTeams") && found < 3; tid++) {
			const cpu = await loadTeam(outlook, tid);
			const proposal = await bestProposal(outlook, cpu, human);
			if (!proposal) {
				continue;
			}
			found += 1;
			assert.isAbove(proposal.give.length, 0);
			assert.isAbove(proposal.get.length, 0);
			// Its gain clears the bar for a human's deal.
			assert.isAbove(proposal.net, 0);
			// The human gains by what he can see.
			assert.isAtLeast(
				publicGain(outlook, pub, proposal.get, proposal.give, cpu),
				-1e-9,
			);
		}
		assert.isAbove(found, 0);
	},
);

test(
	"a standing proposal is kept, shown, and gone once its players move",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
		for (let tid = 1; tid < g.get("numTeams") && !proposer; tid++) {
			const fresh = await refreshProposals(outlook, tid, [userTid]);
			if (fresh.includes(userTid)) {
				proposer = tid;
			}
		}
		assert.isDefined(proposer);
		const standing = (await proposalsTo(userTid)).find(
			(x) => x.tid === proposer,
		);
		assert.isDefined(standing);
		// Offered back, the CPU team takes it.
		const verdict = await judgeHumanOffer(
			{ tid: proposer!, ...standing!.give },
			{ tid: userTid, ...standing!.get },
		);
		assert.strictEqual(verdict.kind, "yes");
		// Asked again with nothing changed, it stands as it was.
		assert.deepEqual(await refreshProposals(outlook, proposer!, [userTid]), []);

		// One of the players in it leaves.
		const pid = standing!.get.pids[0] ?? standing!.give.pids[0]!;
		const p = (await idb.cache.players.get(pid))!;
		const tid = p.tid;
		p.tid = proposer === 5 ? 6 : 5;
		await idb.cache.players.put(p);
		await dropMovedProposals();
		assert.isUndefined(
			(await proposalsTo(userTid)).find((x) => x.tid === proposer),
		);
		p.tid = tid;
		await idb.cache.players.put(p);
		await refreshProposals(outlook, proposer!, [userTid]);
	},
);

test(
	"what would make it work keeps the human's offer",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
		const standing = (await proposalsTo(userTid)).find(
			(x) => x.tid === proposer,
		);
		assert.isDefined(standing);
		const asked = { tid: proposer!, ...standing!.give };
		// A deal it would take already is left as it is.
		const same = await counterOffer(outlook, asked, {
			tid: userTid,
			...standing!.get,
		});
		assert.deepEqual(side(proposer!, same!.give), asked);
		assert.deepEqual(side(userTid, same!.get), {
			tid: userTid,
			...standing!.get,
		});

		// The same ask for nothing: whatever it comes back with keeps the ask,
		// and it would take.
		const counter = await counterOffer(outlook, asked, {
			tid: userTid,
			pids: [],
			dpids: [],
		});
		if (counter) {
			for (const pid of asked.pids) {
				assert.include(side(proposer!, counter.give).pids, pid);
			}
			const verdict = await judgeHumanOffer(
				side(proposer!, counter.give),
				side(userTid, counter.get),
			);
			assert.strictEqual(verdict.kind, "yes");
		}
	},
);

test(
	"a proposal turned down is off and not made again",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
		const standing = (await proposalsTo(userTid)).find(
			(x) => x.tid === proposer,
		);
		assert.isDefined(standing);
		await declineProposal(proposer!, userTid);
		assert.isUndefined(
			(await proposalsTo(userTid)).find((x) => x.tid === proposer),
		);
		// Asked again, it does not make the same one.
		await refreshProposals(outlook, proposer!, [userTid]);
		const again = (await proposalsTo(userTid)).find((x) => x.tid === proposer);
		if (again) {
			assert.notStrictEqual(proposalKey(again), proposalKey(standing!));
		}
	},
);

test(
	"the trading block gets offers CPU teams would make",
	{ timeout: 300_000 },
	async () => {
		const players = (
			await idb.cache.players.indexGetAll("playersByTid", userTid)
		).sort((a, b) => b.valueNoPot - a.valueNoPot);
		const started = Date.now();
		const offers = await tradingBlockTeams(userTid, [players[4]!.pid], [], {
			positions: new Set(),
			skills: new Set(),
			draftPicks: false,
		});
		const seconds = (Date.now() - started) / 1000;
		assert.isBelow(seconds, 60);
		for (const [user, cpu] of offers) {
			assert.deepEqual(user.pids, [players[4]!.pid]);
			const verdict = await judgeHumanOffer(
				{ tid: cpu.tid, pids: cpu.pids, dpids: cpu.dpids },
				{ tid: userTid, pids: user.pids, dpids: user.dpids },
			);
			assert.strictEqual(verdict.kind, "yes");
		}
		const fs = await importNode("node:fs/promises");
		if ((globalThis as any).process?.env?.PROPOSALS_TIMING) {
			await fs.writeFile(
				(globalThis as any).process.env.PROPOSALS_TIMING,
				`trading block: ${offers.length} offers in ${seconds.toFixed(1)} s\n`,
			);
		}
	},
);
