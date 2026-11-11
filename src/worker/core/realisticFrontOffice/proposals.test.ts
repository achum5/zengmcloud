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
	"what would make it work keeps the human's offer and evens it up",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
		assert.isDefined(standing);
		const asked = { tid: proposer!, ...standing!.give };
		// A deal it would take already, as vanilla's: everything in it kept,
		// anything added only to even it up, and it would take that.
		const same = await counterOffer(outlook, asked, {
			tid: userTid,
			...standing!.get,
		});
		assert.isDefined(same);
		const gives = side(proposer!, same!.give);
		const gets = side(userTid, same!.get);
		for (const pid of asked.pids) {
			assert.include(gives.pids, pid);
		}
		for (const dpid of asked.dpids) {
			assert.include(gives.dpids, dpid);
		}
		for (const pid of standing!.get.pids) {
			assert.include(gets.pids, pid);
		}
		for (const dpid of standing!.get.dpids) {
			assert.include(gets.dpids, dpid);
		}
		assert.strictEqual((await judgeHumanOffer(gives, gets)).kind, "yes");

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
	"difficulty: harder, a CPU team wants to be surer of gaining from a human",
	{ timeout: 300_000 },
	async () => {
		assert.isDefined(standing);
		const cpu = { tid: proposer!, ...standing!.give };
		const human = { tid: userTid, ...standing!.get };
		const verdictAt = async (
			level: "easy" | "realistic" | "hard" | "insane",
		) => {
			g.setWithoutSavingToDB("realisticFrontOfficesDifficulty", level);
			try {
				return (await judgeHumanOffer(cpu, human)).kind;
			} finally {
				g.setWithoutSavingToDB("realisticFrontOfficesDifficulty", "realistic");
			}
		};
		// Its price at the realistic level: still a deal easier, too little
		// at insane.
		assert.strictEqual(await verdictAt("realistic"), "yes");
		assert.strictEqual(await verdictAt("easy"), "yes");
		assert.strictEqual(await verdictAt("insane"), "no");
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
			prospects: false,
			bestCurrentPlayers: false,
		});
		const seconds = (Date.now() - started) / 1000;
		assert.isBelow(seconds, 60);
		// A good player draws bids, picks first when picks are asked for (as
		// vanilla's: players after, if it would give no more picks).
		assert.isAbove(offers.length, 0);
		for (const [user, cpu] of offers) {
			assert.deepEqual(user.pids, [pid]);
			assert.isAbove(cpu.dpids.length, 0);
			assert.isAtMost(cpu.pids.length + cpu.dpids.length, 5);
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
			prospects: false,
			bestCurrentPlayers: false,
		});
		for (const [, cpu] of guards) {
			const theirs = await Promise.all(
				cpu.pids.map((x) => idb.cache.players.get(x)),
			);
			assert.isTrue(theirs.some((p) => p!.ratings.at(-1)!.pos.includes(pos)));
		}

		// The league's best player draws bids in picks from all but a few
		// teams: the stock values price a star past any three of a team's
		// assets, and with every bid held to a third of his price, 16 of 29
		// teams bid for this league's (rated 80), none for the owner's 84.
		const best = (await idb.cache.players.getAll())
			.filter((p) => p.tid >= 0)
			.sort((a, b) => b.valueNoPot - a.valueNoPot)[0]!;
		g.setWithoutSavingToDB("userTid", best.tid);
		g.setWithoutSavingToDB("userTids", [best.tid]);
		try {
			const star = await tradingBlockTeams(best.tid, [best.pid], [], {
				positions: new Set(),
				skills: new Set(),
				draftPicks: true,
				prospects: false,
				bestCurrentPlayers: false,
			});
			assert.isAbove(star.length, 0.75 * (g.get("numTeams") - 1));
		} finally {
			g.setWithoutSavingToDB("userTid", userTid);
			g.setWithoutSavingToDB("userTids", [userTid]);
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
