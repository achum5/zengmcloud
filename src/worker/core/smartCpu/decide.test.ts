import { afterAll, assert, beforeAll, onTestFinished, test } from "vitest";
import { PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import type { Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import {
	advanceSeasons,
	createArenaLeague,
} from "../../../test/arena/runLeague.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { frontOfficeCuts } from "../realisticFrontOffice/cuts.ts";
import {
	decideOptions,
	mostWorthPaying,
} from "../realisticFrontOffice/freeAgency.ts";
import { currentOutlook, loadTeam } from "../realisticFrontOffice/market.ts";
import { marketSalaries } from "../realisticFrontOffice/payroll.ts";
import isUntradable from "../trade/isUntradable.ts";
import { prepareSignings } from "./signing.ts";

// LAYER 2 (docs/smart-cpu.md): the GM's decisions, each a rule it keeps, on
// the test league (an arena league one season in, every team the CPU's, the
// smart CPU on).

const importNode = (name: string) => import(/* @vite-ignore */ name);
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
// Every case's numbers, for reading (SMART_L2_OUT=<dir>).
const cases: Record<string, { what: string; value: number }[]> = {};
const note = (rule: string, what: string, value: number) => {
	(cases[rule] ??= []).push({ what, value });
};
const realRandom = Math.random;
let tids: number[];
const age = (p: Player) => g.get("season") - p.born.year;
const label = (p: Player) =>
	`${p.firstName} ${p.lastName} (tid ${p.tid}, ${age(p)}y ${p.ratings.at(-1)!.ovr}/${p.ratings.at(-1)!.pot})`;
const rosterOf = (tid: number) =>
	idb.cache.players.indexGetAll("playersByTid", tid);

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(77);
	(idb as any).league = undefined;
	await createArenaLeague(9177);
	await advanceSeasons(1);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("smartCpu", true);
	tids = (await idb.cache.teams.getAll())
		.filter((t) => !t.disabled)
		.map((t) => t.tid);
}, 900_000);

afterAll(async () => {
	Math.random = realRandom;
	if (env.SMART_L2_OUT) {
		const fs = await importNode("node:fs/promises");
		await fs.writeFile(
			`${env.SMART_L2_OUT}/layer2-cases.json`,
			JSON.stringify(cases, null, 1),
		);
	}
});

test(
	"free agency: a team down to three players wants enough free agents at their asks to fill its roster",
	{ timeout: 900_000 },
	async () => {
		// The 2035 Lakers of the owner's all-CPU league.
		const tid = tids.at(-1)!;
		const roster = (await rosterOf(tid)).sort((a, b) => a.value - b.value);
		const released = roster.slice(0, roster.length - 3);
		for (const p of released) {
			p.tid = PLAYER.FREE_AGENT;
			await idb.cache.players.put(p);
		}
		onTestFinished(async () => {
			for (const p of released) {
				p.tid = tid;
				await idb.cache.players.put(p);
			}
		});
		const outlook = await currentOutlook();
		const pool = (await rosterOf(PLAYER.FREE_AGENT))
			.filter((p) => p.ratings.at(-1)!.ovr >= 45)
			.sort((a, b) => b.value - a.value)
			.slice(0, 15);
		await prepareSignings(outlook, pool);
		const t = await loadTeam(outlook, tid);
		let wanted = 0;
		for (const p of pool) {
			const ask = contractOf(p);
			const years = Math.max(1, ask.exp - g.get("season") + 1);
			const most = mostWorthPaying(outlook, t, p, years);
			note(
				"emptyRoster",
				`${label(p)} asks ${ask.amount} x${years}`,
				(most ?? 0) - ask.amount,
			);
			if (most !== undefined && most >= ask.amount) {
				wanted += 1;
			}
		}
		assert.isAtLeast(wanted, 10);
	},
);

test(
	"re-signing: a young star is worth his market salary to his team; an old player is worth less a year on a long deal than a short one",
	{ timeout: 900_000 },
	async () => {
		const outlook = await currentOutlook();
		const marketSalary = marketSalaries();
		const young: string[] = [];
		const old: string[] = [];
		for (const tid of tids) {
			const roster = await rosterOf(tid);
			await prepareSignings(outlook, roster);
			const t = await loadTeam(outlook, tid);
			const without = (p: Player) => ({
				...t,
				players: t.players.filter((x) => x.pid !== p.pid),
			});
			// A team never lets a young star walk: one of its three best, 25 or
			// younger, rated 60 or more, is worth to it at least (within a tenth)
			// what players of his value are paid. Not every young starter is: a
			// bad team can rightly decline four years of a 24-year-old 57/61 at
			// $15M (2026-11-13: the six below the table were all on bad teams,
			// rated 54-58).
			const best = [...roster].sort((a, b) => b.value - a.value).slice(0, 3);
			for (const p of best.filter(
				(x) => age(x) <= 25 && x.ratings.at(-1)!.ovr >= 60,
			)) {
				const market = marketSalary(p.value);
				const most = mostWorthPaying(outlook, without(p), p, 4);
				note(
					"youngReSign",
					`${label(p)} market ${Math.round(market)}`,
					(most ?? 0) / market,
				);
				if (!(most !== undefined && most >= 0.9 * market)) {
					young.push(`${label(p)}: most ${most}, market ${market}`);
				}
			}
			for (const p of roster.filter(
				(x) => age(x) >= 33 && x.ratings.at(-1)!.ovr >= 45,
			)) {
				const short = mostWorthPaying(outlook, without(p), p, 1) ?? 0;
				const long = mostWorthPaying(outlook, without(p), p, 4) ?? 0;
				note(
					"oldReSign",
					`${label(p)}: 1y ${Math.round(short)}, 4y ${Math.round(long)}`,
					short - long,
				);
				// Never clearly more: a player who retires takes the rest of his
				// contract with him, so for the oldest the two come out alike.
				// Deals at about the minimum aren't what this guards against (a
				// one-year deal is priced with his re-signing at market after
				// it, four at the minimum without: 2026-11-13, a 35-year-old 54
				// at $0 for one year and $1.1M for four).
				if (!(long <= 1.02 * short) && long > 2 * g.get("minContract")) {
					old.push(`${label(p)}: 1y ${short}, 4y ${long}`);
				}
			}
		}
		assert.isAtLeast(cases.youngReSign?.length ?? 0, 8);
		assert.isAtLeast(cases.oldReSign?.length ?? 0, 5);
		assert.deepStrictEqual(young, []);
		assert.deepStrictEqual(old, []);
	},
);

test(
	"cuts: over the limit, a team releases a scrub, never one of its eight best",
	{ timeout: 900_000 },
	async () => {
		const bad: string[] = [];
		for (const tid of tids.filter((_, i) => i % 5 === 0)) {
			const roster = await rosterOf(tid);
			const top = new Set(
				[...roster]
					.sort((a, b) => b.value - a.value)
					.slice(0, 8)
					.map((p) => p.pid),
			);
			// Two free agents added, one decent and one a scrub, until it is two
			// over the limit.
			const pool = (await rosterOf(PLAYER.FREE_AGENT)).sort(
				(a, b) => b.value - a.value,
			);
			const added = [pool[0]!, pool.at(-1)!];
			const over = roster.length + added.length - g.get("maxRosterSize");
			for (const p of added) {
				p.tid = tid;
				await idb.cache.players.put(p);
			}
			const released = await frontOfficeCuts(await rosterOf(tid), over);
			for (const pid of released) {
				const p = (await idb.cache.players.get(pid))!;
				note("cuts", `team ${tid} releases ${label(p)}`, p.value);
				if (top.has(pid)) {
					bad.push(`team ${tid} released ${label(p)}, one of its eight best`);
				}
				// Back as he was, for the next team.
				p.tid = added.includes(p) ? PLAYER.FREE_AGENT : tid;
				await idb.cache.players.put(p);
			}
			for (const p of added) {
				p.tid = PLAYER.FREE_AGENT;
				await idb.cache.players.put(p);
			}
			assert.strictEqual(released.length, Math.max(0, over));
		}
		assert.deepStrictEqual(bad, []);
	},
);

test(
	"team options: a team keeps a bargain and declines a dear one",
	{ timeout: 900_000 },
	async () => {
		const season = g.get("season");
		const cases_: { p: Player; keep: boolean; was: unknown }[] = [];
		for (const tid of tids.filter((_, i) => i % 6 === 0)) {
			const roster = [...(await rosterOf(tid))].sort(
				(a, b) => b.value - a.value,
			);
			// Its second-best player with a cheap option year; its oldest
			// scrub with one at the max.
			const bargain = roster[1]!;
			const dear = [...roster]
				.reverse()
				.find((p) => age(p) >= 30 && p !== bargain);
			for (const [p, amount, keep] of [
				[bargain, g.get("minContract"), true],
				...(dear ? [[dear, g.get("maxContract"), false]] : []),
			] as [Player, number, boolean][]) {
				const c = p.contracts.at(-1)!;
				cases_.push({ p, keep, was: structuredClone(c) });
				c.start = season;
				c.amounts = [contractOf(p).amount, amount];
				c.option = "team";
				await idb.cache.players.put(p);
			}
		}
		onTestFinished(async () => {
			for (const { p, was } of cases_) {
				p.contracts[p.contracts.length - 1] =
					was as Player["contracts"][number];
				await idb.cache.players.put(p);
			}
		});
		await decideOptions();
		const wrong: string[] = [];
		for (const { p, keep } of cases_) {
			const kept = p.contracts.at(-1)!.amounts.length === 2;
			note("options", `${label(p)} ${keep ? "bargain" : "dear"}`, kept ? 1 : 0);
			if (kept !== keep) {
				wrong.push(
					`${label(p)}: ${keep ? "bargain declined" : "dear one kept"}`,
				);
			}
		}
		assert.isAtLeast(cases_.length, 8);
		assert.deepStrictEqual(wrong, []);
	},
);

test(
	"draft: with the first pick, every team takes one of the class's best, and the clearly best one when there is one",
	{ timeout: 1_800_000 },
	async () => {
		const { smartDraftChoice } = await import("./draft.ts");
		const prospects = (await rosterOf(PLAYER.UNDRAFTED)).sort(
			(a, b) => b.value - a.value,
		);
		const top3 = new Set(prospects.slice(0, 3).map((p) => p.pid));
		const clear = prospects[0]!.value - prospects[1]!.value >= 3;
		const wrong: string[] = [];
		const t0 = performance.now();
		for (const tid of tids) {
			const { choice } = await smartDraftChoice(
				{ dpid: -1, tid, round: 1, pick: 1 },
				prospects,
			);
			const rank = prospects.indexOf(choice!);
			note(
				"draft",
				`team ${tid} takes ${label(choice!)}, ${rank + 1} by value`,
				rank + 1,
			);
			if (!top3.has(choice!.pid) || (clear && rank !== 0)) {
				wrong.push(`team ${tid} took ${label(choice!)} (${rank + 1} by value)`);
			}
		}
		note(
			"draftSeconds",
			"a pick",
			(performance.now() - t0) / 1000 / tids.length,
		);
		assert.isAtLeast(prospects.length, 30);
		assert.deepStrictEqual(wrong, []);
	},
);

test(
	"human offers: a fleece is refused; less than the best real CPU offer is refused; more is taken",
	{ timeout: 1_800_000 },
	async () => {
		const { smartJudgeOffer } = await import("./humanTrades.ts");
		const { currentOutlook } =
			await import("../realisticFrontOffice/market.ts");
		const { viewFor } = await import("./signing.ts");
		const human = tids[0]!;
		const was = {
			userTids: g.get("userTids"),
			userTid: g.get("userTid"),
			spectator: g.get("spectator"),
		};
		g.setWithoutSavingToDB("userTids", [human]);
		g.setWithoutSavingToDB("userTid", human);
		g.setWithoutSavingToDB("spectator", false);
		onTestFinished(() => {
			g.setWithoutSavingToDB("userTids", was.userTids);
			g.setWithoutSavingToDB("userTid", was.userTid);
			g.setWithoutSavingToDB("spectator", was.spectator);
		});
		const view = viewFor(await currentOutlook());
		await view.prime();
		const ids = (players: Player[]) => ({
			pids: players.map((p) => p.pid),
			dpids: [],
		});
		const humanRoster = [...(await rosterOf(human))]
			.filter((p) => !isUntradable(p).untradable)
			.sort((a, b) => a.value - b.value);
		const wrong: string[] = [];
		let lowballs = 0;
		let fleeces = 0;
		let taken = 0;
		for (const seller of tids.filter((t) => t !== human)) {
			const mine = [...(await rosterOf(seller))]
				.filter((p) => !isUntradable(p).untradable)
				.sort((a, b) => b.value - a.value)
				.slice(0, 4);
			for (const x of mine) {
				const real = await view.bestRealOffer(
					seller,
					{ players: [x], picks: [] },
					human,
				);
				// The human's worst player for him.
				const fleece = await smartJudgeOffer(
					seller,
					human,
					ids([x]),
					ids([humanRoster[0]!]),
				);
				fleeces += 1;
				note("humanFleece", label(x), fleece.gain);
				if (fleece.gain > 0) {
					wrong.push(`${label(x)} taken for ${label(humanRoster[0]!)}`);
				}
				if (!real || !(real.worth > 0)) {
					continue;
				}
				// One of the human's players the seller would take alone for
				// him, by its worth, against the real offer elsewhere.
				for (const p of humanRoster) {
					const v = await smartJudgeOffer(seller, human, ids([x]), ids([p]));
					if (v.worth > 0 && v.worth < real.worth) {
						lowballs += 1;
						note("humanLowball", `${label(x)} for ${label(p)}`, v.gain);
						if (v.gain > 0 || !v.betterElsewhere) {
							wrong.push(`${label(x)} for ${label(p)}: ${v.gain}`);
						}
						break;
					}
					if (v.worth > real.worth) {
						taken += 1;
						note("humanTaken", `${label(x)} for ${label(p)}`, v.gain);
						if (!(v.gain > 0)) {
							wrong.push(`${label(x)} for ${label(p)} refused: ${v.gain}`);
						}
						break;
					}
				}
			}
		}
		assert.deepStrictEqual(wrong, []);
		assert.isAtLeast(fleeces, 50);
		assert.isAtLeast(lowballs + taken, 3);
	},
);

// Last: it trades.
test(
	"market: every deal a moment makes helps both sides, the market rule on; nobody deals twice; within the budget",
	{ timeout: 1_800_000 },
	async () => {
		const { marketMoment } = await import("./market.ts");
		const t0 = performance.now();
		const made = await marketMoment();
		const seconds = (performance.now() - t0) / 1000;
		note("marketSeconds", `a moment, ${made.length} deals`, seconds);
		const { marketStats } = await import("./market.ts");
		for (const [k, v] of Object.entries(marketStats)) {
			note("marketStats", k, v);
		}
		const seen = new Set<number>();
		const bad: string[] = [];
		for (const d of made) {
			const names = (h: {
				players: Player[];
				picks: { season: unknown; round: number; originalTid: number }[];
			}) =>
				[
					...h.players.map(label),
					...h.picks.map(
						(dp) =>
							`${dp.season} round ${dp.round} pick (from ${dp.originalTid})`,
					),
				].join(" + ");
			note(
				"deals",
				`${d.seller} sends ${names(d.out)} to ${d.buyer} for ${names(d.offer)}: seller ${d.sellerGain.toFixed(4)}, buyer ${d.buyerGain.toFixed(4)}`,
				Math.min(d.sellerGain, d.buyerGain),
			);
			if (!(d.sellerGain > 0 && d.buyerGain > 0)) {
				bad.push(`${d.seller}-${d.buyer}: ${d.sellerGain}, ${d.buyerGain}`);
			}
			for (const tid of [d.seller, d.buyer]) {
				if (seen.has(tid)) {
					bad.push(`team ${tid} dealt twice`);
				}
				seen.add(tid);
			}
			for (const p of d.out.players) {
				if ((await idb.cache.players.get(p.pid))!.tid !== d.buyer) {
					bad.push(`${label(p)} not with ${d.buyer}`);
				}
			}
		}
		assert.deepStrictEqual(bad, []);
		assert.isAtLeast(made.length, 1);
		assert.isBelow(seconds, 120);
	},
);
