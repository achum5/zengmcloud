import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { judgeHumanOffer } from "./humanTrades.ts";
import { currentOutlook } from "./market.ts";
import {
	counterOffer,
	publicPlayer,
	proposalTeams,
	tradingBlockTeams,
} from "./proposals.ts";
import tradeProposals from "../../views/tradeProposals.ts";

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

// The first CPU team with a proposal for the user, and its proposal.
let proposer: number | undefined;
let standing:
	| {
			give: { pids: number[]; dpids: number[] };
			get: { pids: number[]; dpids: number[] };
	  }
	| undefined;

test(
	"Trade Proposals: what a team would really give for one of the human's assets",
	{ timeout: 300_000 },
	async () => {
		const mine = (
			await idb.cache.players.indexGetAll("playersByTid", userTid)
		).sort((a, b) => b.value - a.value);
		let found = 0;
		for (let tid = 1; tid < g.get("numTeams") && found < 3; tid++) {
			for (const p of mine.slice(0, 3)) {
				const teams = await proposalTeams([
					{
						tid: userTid,
						pids: [p.pid],
						dpids: [],
						pidsExcluded: [],
						dpidsExcluded: [],
					},
					{ tid, pids: [], dpids: [], pidsExcluded: [], dpidsExcluded: [] },
				]);
				if (!teams) {
					continue;
				}
				found += 1;
				if (proposer === undefined) {
					proposer = tid;
					standing = {
						give: { pids: teams[1].pids, dpids: teams[1].dpids },
						get: { pids: teams[0].pids, dpids: teams[0].dpids },
					};
				}
				// It keeps the asset asked for, gives something for it, and would
				// take it offered back.
				assert.include(teams[0].pids, p.pid);
				assert.isAbove(teams[1].pids.length + teams[1].dpids.length, 0);
				const verdict = await judgeHumanOffer(
					{ tid, pids: teams[1].pids, dpids: teams[1].dpids },
					{ tid: userTid, pids: teams[0].pids, dpids: teams[0].dpids },
				);
				assert.strictEqual(verdict.kind, "yes");
				break;
			}
		}
		assert.isAbove(found, 0);
	},
);

test(
	"what would make it work keeps the human's offer",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
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
	"the Trade Proposals page offers deals the vanilla way, priced by the mode",
	{ timeout: 300_000 },
	async () => {
		const load = async () =>
			(await tradeProposals.load({
				inputs: {},
				updateEvents: new Set(["firstRun"]),
				prevInputs: undefined,
				prevOutput: {},
				conditions: {},
			} as any)) as any;
		const started = Date.now();
		const data = await load();
		assert.isArray(data.offers);
		for (const offer of data.offers) {
			const verdict = await judgeHumanOffer(
				{ tid: offer.tid, pids: offer.pids, dpids: offer.dpids },
				{ tid: userTid, pids: offer.pidsUser, dpids: offer.dpidsUser },
			);
			assert.strictEqual(verdict.kind, "yes");
		}
		console.log(
			`${data.offers.length} offers in ${((Date.now() - started) / 1000).toFixed(1)} s`,
		);

		// Loaded again for the same seed, the same offers, kept.
		const again = await load();
		assert.deepEqual(
			again.offers.map((x: any) => [x.tid, x.pids, x.dpids]),
			data.offers.map((x: any) => [x.tid, x.pids, x.dpids]),
		);
	},
);

test(
	"the trading block gets bids CPU teams would make",
	{ timeout: 300_000 },
	async () => {
		const players = (
			await idb.cache.players.indexGetAll("playersByTid", userTid)
		).sort((a, b) => b.valueNoPot - a.valueNoPot);
		const pid = players[0]!.pid;
		const started = Date.now();
		const offers = await tradingBlockTeams(userTid, [pid], [], {
			positions: new Set(),
			skills: new Set(),
			draftPicks: true,
		});
		const seconds = (Date.now() - started) / 1000;
		assert.isBelow(seconds, 60);
		// A good player draws bids, in picks only when only those are asked for.
		assert.isAbove(offers.length, 0);
		for (const [user, cpu] of offers) {
			assert.deepEqual(user.pids, [pid]);
			assert.deepEqual(cpu.pids, []);
			assert.isAbove(cpu.dpids.length, 0);
			// One the team would take as offered, before it shops what it gives.
			const verdict = await judgeHumanOffer(
				{ tid: cpu.tid, pids: cpu.pids, dpids: cpu.dpids },
				{ tid: userTid, pids: user.pids, dpids: user.dpids },
				{ shop: false },
			);
			assert.strictEqual(verdict.kind, "yes");
		}

		// Asked for a position, every bid holds a player there.
		const pos = "G";
		const guards = await tradingBlockTeams(userTid, [pid], [], {
			positions: new Set([pos]),
			skills: new Set(),
			draftPicks: false,
		});
		for (const [, cpu] of guards) {
			const theirs = await Promise.all(
				cpu.pids.map((x) => idb.cache.players.get(x)),
			);
			assert.isTrue(theirs.some((p) => p!.ratings.at(-1)!.pos.includes(pos)));
		}

		const fs = await importNode("node:fs/promises");
		if ((globalThis as any).process?.env?.PROPOSALS_TIMING) {
			await fs.writeFile(
				(globalThis as any).process.env.PROPOSALS_TIMING,
				`trading block: ${offers.length} bids in ${seconds.toFixed(1)} s\n`,
			);
		}
	},
);
