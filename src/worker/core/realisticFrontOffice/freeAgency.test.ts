import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { mostWorthPaying, priceSigning } from "./freeAgency.ts";
import { currentOutlook, loadTeam } from "./market.ts";
import { moneyCost, teamStrengths } from "./outlook.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(37);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9019);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

test(
	"over the cap, more money for the same player is worth less to a team",
	{ timeout: 300_000 },
	async () => {
		const season = g.get("season");
		const cap = g.get("salaryCap");
		const tid = 5;
		const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		const amounts = roster.map((p) => p.contract.amount);
		const total = amounts.reduce((a, b) => a + b, 0);
		try {
			for (const [i, p] of roster.entries()) {
				p.contract.amount = (amounts[i]! * 1.2 * cap) / total;
				p.contract.exp = Math.max(p.contract.exp, season + 3);
				await idb.cache.players.put(p);
			}
			const outlook = await currentOutlook();
			const t = await loadTeam(outlook, tid);
			const p = (
				await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
			).sort((a, b) => b.value - a.value)[0]!;
			const at = (amount: number) =>
				priceSigning(outlook, t, p, { amount, exp: season + 3 }).gain;
			assert.isBelow(
				at(g.get("maxContract")),
				at(g.get("minContract")) - 0.001,
			);
		} finally {
			for (const [i, p] of roster.entries()) {
				p.contract.amount = amounts[i]!;
				await idb.cache.players.put(p);
			}
		}
	},
);

test(
	"in the season, a team pays for this season's payroll past the minimum",
	{ timeout: 300_000 },
	async () => {
		const cap = g.get("salaryCap");
		const tid = 11;
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		const was = roster.map((p) => p.contract.amount);
		const total = was.reduce((a, b) => a + b, 0);
		try {
			const outlook = await currentOutlook();
			const costAt = async (payroll: number) => {
				for (const [i, p] of roster.entries()) {
					p.contract.amount = (was[i]! * payroll) / total;
					await idb.cache.players.put(p);
				}
				const t = await loadTeam(outlook, tid);
				return (
					moneyCost(teamStrengths(outlook, tid, t.players, t.picks))?.[0] ?? 0
				);
			};
			const under = await costAt(0.9 * g.get("minPayroll"));
			const over = await costAt(g.get("luxuryPayroll") + 0.1 * cap);
			assert.strictEqual(under, 0);
			assert.isAbove(over, 0);
		} finally {
			for (const [i, p] of roster.entries()) {
				p.contract.amount = was[i]!;
				await idb.cache.players.put(p);
			}
			g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		}
	},
);

test(
	"no team would pay a free agent more than he is worth to it at that price",
	{ timeout: 300_000 },
	async () => {
		// Teams left with their best player and room to spare: such a team loses
		// nothing to a role player's market salary, and room it needs past that.
		const season = g.get("season");
		const cap = g.get("salaryCap");
		const tids = [2, 8, 14, 20, 26];
		for (const tid of tids) {
			const roster = (
				await idb.cache.players.indexGetAll("playersByTid", tid)
			).sort((a, b) => b.valueNoPot - a.valueNoPot);
			for (const p of roster.slice(1, 6)) {
				p.tid = tid + 1;
				await idb.cache.players.put(p);
			}
			const kept = roster.slice(0, 1).concat(roster.slice(6));
			const total = kept.reduce((a, p) => a + p.contract.amount, 0);
			for (const p of kept) {
				p.contract.amount = Math.max(
					g.get("minContract"),
					(p.contract.amount * 0.4 * cap) / total,
				);
				await idb.cache.players.put(p);
			}
		}
		const players = await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		]);
		const freeAgents = [52, 56, 60].map((v) =>
			players.find((p) => p.valueNoPot >= v && p.valueNoPot < v + 2)!,
		);
		for (const p of freeAgents) {
			p.tid = PLAYER.FREE_AGENT;
			p.born.year = season - 31;
			await idb.cache.players.put(p);
		}
		const outlook = await currentOutlook();
		let checked = 0;
		for (const tid of tids) {
			const t = await loadTeam(outlook, tid);
			for (const p of freeAgents) {
				const most = mostWorthPaying(outlook, t, p, 3);
				if (most === undefined) {
					continue;
				}
				checked += 1;
				const amount = Math.min(most, g.get("maxContract"));
				const { gain } = priceSigning(outlook, t, p, {
					amount,
					exp: season + 3,
				});
				assert.isAtLeast(gain, 0, `team ${tid}, player ${p.pid} at ${amount}`);
			}
		}
		assert.isAbove(checked, 0);
	},
);
