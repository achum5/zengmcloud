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
import { buildOutlook } from "../realisticFrontOffice/outlook.ts";
import isUntradable from "../trade/isUntradable.ts";
import { SmartView } from "./worth.ts";

// LAYER 1 (docs/smart-cpu.md): what anything is worth to a team. Each rule a
// GM never breaks, as a test - on a settled league (one season played), every
// team run by the CPU.

const importNode = (name: string) => import(/* @vite-ignore */ name);
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
// Every case's margin, for reading (SMART_L1_OUT=<dir>).
const cases: Record<string, { what: string; gain: number }[]> = {};
const note = (rule: string, what: string, gain: number) => {
	(cases[rule] ??= []).push({ what, gain });
};
const realRandom = Math.random;
let view: SmartView;
let tids: number[];
const label = (p: Player) =>
	`${p.firstName} ${p.lastName} (tid ${p.tid}, ${g.get("season") - p.born.year}y ${p.ratings.at(-1)!.ovr}/${p.ratings.at(-1)!.pot})`;

const fresh = async () => {
	view = new SmartView(await buildOutlook({ iterations: 1000 }), []);
};

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(77);
	(idb as any).league = undefined;
	await createArenaLeague(9177);
	await advanceSeasons(1);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	tids = (await idb.cache.teams.getAll())
		.filter((t) => !t.disabled)
		.map((t) => t.tid);
	await fresh();
}, 900_000);

afterAll(async () => {
	Math.random = realRandom;
	if (env.SMART_L1_OUT) {
		const fs = await importNode("node:fs/promises");
		await fs.writeFile(
			`${env.SMART_L1_OUT}/layer1-cases.json`,
			JSON.stringify(cases, null, 1),
		);
	}
});

const best = async (tid: number, n = 1) =>
	(await idb.cache.players.indexGetAll("playersByTid", tid))
		.sort((a, b) => b.value - a.value)
		.slice(0, n);
const other = (tid: number) => tids.find((x) => x !== tid)!;

test(
	"no team gives its best player away for nothing",
	{ timeout: 600_000 },
	async () => {
		const yes: string[] = [];
		for (const tid of tids) {
			const [p] = await best(tid);
			const v = await view.verdict(
				tid,
				other(tid),
				{ pids: [p!.pid], dpids: [] },
				{ pids: [], dpids: [] },
			);
			note("giveaway", label(p!), v.gain);
			if (v.gain > 0) {
				yes.push(`${label(p!)}: ${v.gain}`);
			}
		}
		assert.deepStrictEqual(yes, []);
		assert.isAtLeast(cases.giveaway!.length, 25);
	},
);

test(
	"a first-round pick is never worthless: it has a price, and its team won't give it for nothing",
	{ timeout: 600_000 },
	async () => {
		const bad: string[] = [];
		for (const tid of tids) {
			const firsts = (
				await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid)
			).filter((dp) => dp.round === 1);
			for (const dp of firsts) {
				const price = await view.price({ players: [], picks: [dp] }, [tid]);
				const v = await view.verdict(
					tid,
					other(tid),
					{ pids: [], dpids: [dp.dpid] },
					{ pids: [], dpids: [] },
				);
				note(
					"pick",
					`${dp.season} R1 of ${dp.originalTid} held by ${tid}: price ${price}`,
					v.gain,
				);
				if (!(price > 0) || v.gain > 0) {
					bad.push(
						`${dp.season} first of ${dp.originalTid} (team ${tid}): price ${price}, gives it away ${v.gain > 0}`,
					);
				}
			}
		}
		assert.deepStrictEqual(bad, []);
		assert.isAtLeast(cases.pick!.length, 100);
	},
);

test(
	"no salary dump for free: a bad long contract costs a team that takes it",
	{ timeout: 600_000 },
	async () => {
		// A role player owed a quarter of the cap for six seasons.
		const season = g.get("season");
		const dumper = tids[0]!;
		const roster = await idb.cache.players.indexGetAll("playersByTid", dumper);
		const p = roster
			.filter((x) => x.ratings.at(-1)!.ovr >= 47 && x.ratings.at(-1)!.ovr <= 53)
			.sort((a, b) => a.value - b.value)[0]!;
		const c = p.contracts.at(-1)!;
		const was = structuredClone(c);
		c.start = season;
		c.amounts = Array(6).fill(Math.round(0.25 * g.get("salaryCap")));
		c.payments = [];
		await idb.cache.players.put(p);
		onTestFinished(async () => {
			Object.assign(c, was);
			await idb.cache.players.put(p);
		});
		await fresh();
		const second = (
			await idb.cache.draftPicks.indexGetAll("draftPicksByTid", dumper)
		).find((dp) => dp.round === 2);
		const takes: string[] = [];
		for (const tid of tids.filter((x) => x !== dumper)) {
			for (const dpids of [[], second ? [second.dpid] : []]) {
				const v = await view.verdict(
					tid,
					dumper,
					{ pids: [], dpids: [] },
					{ pids: [p.pid], dpids },
				);
				note(
					"dump",
					`team ${tid} ${dpids.length > 0 ? "with a second" : "for nothing"}`,
					v.gain,
				);
				if (v.gain > 0) {
					takes.push(
						`team ${tid} takes ${label(p)} on $${(c.amounts[0]! / 1000).toFixed(1)}M x6 ${dpids.length > 0 ? "with a second" : "for nothing"}: ${v.gain}`,
					);
				}
			}
		}
		assert.deepStrictEqual(takes, []);
		assert.isAtLeast(cases.dump!.length, 50);
	},
);

test(
	"two lesser players don't buy a star because their prices add up",
	{ timeout: 900_000 },
	async () => {
		const yes: string[] = [];
		for (const tid of tids) {
			const [star] = await best(tid);
			const ovr = star!.ratings.at(-1)!.ovr;
			if (ovr < 62) {
				continue;
			}
			const buyer = other(tid);
			const theirs = (
				await idb.cache.players.indexGetAll("playersByTid", buyer)
			)
				.filter((x) => x.ratings.at(-1)!.ovr <= ovr - 8)
				.sort((a, b) => b.value - a.value)
				.slice(0, 2);
			if (theirs.length < 2) {
				continue;
			}
			const v = await view.verdict(
				tid,
				buyer,
				{ pids: [star!.pid], dpids: [] },
				{ pids: theirs.map((x) => x.pid), dpids: [] },
			);
			note(
				"twoForOne",
				`${label(star!)} for ${theirs.map(label).join(" + ")}`,
				v.gain,
			);
			if (v.gain > 0) {
				yes.push(
					`${label(star!)} for ${theirs.map(label).join(" + ")}: ${v.gain}`,
				);
			}
		}
		assert.deepStrictEqual(yes, []);
		assert.isAtLeast(cases.twoForOne!.length, 15);
	},
);

test(
	"a contender doesn't sell its core to a rival contender for prospects",
	{ timeout: 900_000 },
	async () => {
		const byChance = [...view.outlook.teams.values()]
			.sort((a, b) => b.title[0]! - a.title[0]!)
			.map((o) => o.tid);
		const [a, b] = byChance as [number, number];
		const prospects = (await idb.cache.players.indexGetAll("playersByTid", b))
			.filter((p) => g.get("season") - p.born.year <= 22)
			.sort((x, y) => y.value - x.value)
			.slice(0, 2);
		const yes: string[] = [];
		for (const core of await best(a, 3)) {
			const v = await view.verdict(
				a,
				b,
				{ pids: [core.pid], dpids: [] },
				{ pids: prospects.map((p) => p.pid), dpids: [] },
			);
			note(
				"rival",
				`${label(core)} for ${prospects.map(label).join(" + ")}`,
				v.gain,
			);
			if (v.gain > 0) {
				yes.push(
					`${label(core)} for ${prospects.map(label).join(" + ")}: ${v.gain}`,
				);
			}
		}
		assert.deepStrictEqual(yes, []);
		assert.strictEqual(cases.rival!.length, 3);
	},
);

test(
	"a team with an all but empty roster wants decent free agents at their asking price",
	{ timeout: 900_000 },
	async () => {
		// The 2035 Lakers of the owner's all-CPU league: three players left, room
		// to burn, free agents unsigned.
		const tid = tids.at(-1)!;
		const roster = (
			await idb.cache.players.indexGetAll("playersByTid", tid)
		).sort((a, b) => a.value - b.value);
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
		await fresh();
		const freeAgents = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		)
			.filter((p) => p.ratings.at(-1)!.ovr >= 45)
			.sort((a, b) => b.value - a.value)
			.slice(0, 8);
		const no: string[] = [];
		for (const p of freeAgents) {
			const v = await view.verdict(
				tid,
				PLAYER.FREE_AGENT,
				{ pids: [], dpids: [] },
				{ pids: [p.pid], dpids: [] },
			);
			note("emptyRoster", label(p), v.gain);
			if (!(v.gain > 0)) {
				no.push(`${label(p)}: ${v.gain}`);
			}
		}
		assert.deepStrictEqual(no, []);
		assert.strictEqual(cases.emptyRoster!.length, 8);
	},
);

// Sales the market makes: each team's best tradable players, and the best
// real offer for each from the keenest CPU teams when the seller takes it.
type Sale = {
	tid: number;
	x: Player;
	buyer: number;
	worth: number;
	offer: Player[];
};
let sales: Sale[] | undefined;
const scanSales = async () => {
	if (!sales) {
		await fresh();
		sales = [];
		for (const tid of tids) {
			const mine = (await idb.cache.players.indexGetAll("playersByTid", tid))
				.filter((p) => !isUntradable(p).untradable)
				.sort((a, b) => b.value - a.value)
				.slice(0, 6);
			for (const x of mine) {
				const real = await view.bestRealOffer(
					tid,
					{ players: [x], picks: [] },
					-1,
				);
				note(
					"realOffers",
					`${label(x)}: ${real ? `from ${real.buyer}, ${real.offer.players.map(label).join(" + ")}` : "none"}`,
					real?.worth ?? Number.NaN,
				);
				if (real && real.worth > 0) {
					sales.push({
						tid,
						x,
						buyer: real.buyer,
						worth: real.worth,
						offer: real.offer.players,
					});
				}
			}
		}
	}
	return sales;
};

// The other side of the rules above: a GM that turned everything down would
// pass them all and never trade.
test(
	"trades still happen: teams sell players for offers both sides take",
	{ timeout: 1_800_000 },
	async () => {
		const found = await scanSales();
		const blocked: string[] = [];
		for (const s of found) {
			// The buyer gains by its own worth (bestRealOffer only makes such
			// offers), and the seller takes it with the market rule on: no other
			// keen team would give more.
			const v = await view.verdict(
				s.tid,
				s.buyer,
				{ pids: [s.x.pid], dpids: [] },
				{ pids: s.offer.map((p) => p.pid), dpids: [] },
			);
			note(
				"sales",
				`${label(s.x)} to ${s.buyer} for ${s.offer.map(label).join(" + ")}`,
				v.gain,
			);
			if (!(v.gain > 0)) {
				blocked.push(
					`${label(s.x)}: ${v.gain} (best elsewhere ${v.best?.worth} from ${v.best?.buyer})`,
				);
			}
		}
		assert.deepStrictEqual(blocked, []);
		assert.isAtLeast(found.length, 1);
	},
);

test(
	"the market: a team turns down an offer it would take, when another team would really give more",
	{ timeout: 1_800_000 },
	async () => {
		const found = await scanSales();
		const yes: string[] = [];
		let cases = 0;
		for (const s of found) {
			// Lowballs: one of another team's players, enough for the seller by
			// its own worth, but less than the sale's real offer.
			let here = 0;
			for (const from of tids) {
				if (from === s.tid || from === s.buyer || here >= 2) {
					continue;
				}
				const theirs = (
					await idb.cache.players.indexGetAll("playersByTid", from)
				)
					.filter((p) => !isUntradable(p).untradable)
					.sort((a, b) => b.value - a.value)
					.slice(0, 4);
				for (const p of theirs) {
					const out = { pids: [s.x.pid], dpids: [] };
					const inn = { pids: [p.pid], dpids: [] };
					const alone = await view.verdict(s.tid, from, out, inn, {
						market: false,
					});
					if (!(alone.worth > 0 && alone.worth < s.worth)) {
						continue;
					}
					const v = await view.verdict(s.tid, from, out, inn);
					cases += 1;
					here += 1;
					note(
						"market",
						`${label(s.x)} for ${label(p)}: ${alone.worth} alone, ${s.worth} from ${s.buyer}`,
						v.gain,
					);
					if (v.gain > 0) {
						yes.push(`${label(s.x)} for ${label(p)}: ${v.gain}`);
					}
					break;
				}
			}
		}
		assert.deepStrictEqual(yes, []);
		assert.isAtLeast(cases, 1);
	},
);

test(
	"contracts count: the same player for the same years is worth less on more money, to every team",
	{ timeout: 900_000 },
	async () => {
		const season = g.get("season");
		const byChance = [...view.outlook.teams.values()]
			.sort((a, b) => b.title[0]! - a.title[0]!)
			.map((o) => o.tid);
		const teams = [byChance[0]!, byChance.at(-1)!];
		// A solid veteran from a third team, made a free agent for the test.
		const vets = (
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
		)
			.filter(
				(x) =>
					!teams.includes(x.tid) &&
					x.ratings.at(-1)!.ovr >= 52 &&
					x.ratings.at(-1)!.ovr <= 58 &&
					season - x.born.year >= 27 &&
					season - x.born.year <= 31,
			)
			.sort((a, b) => a.value - b.value);
		const p = vets[Math.floor(vets.length / 2)]!;
		const c = p.contracts.at(-1)!;
		const was = { tid: p.tid, contract: structuredClone(c) };
		p.tid = PLAYER.FREE_AGENT;
		onTestFinished(async () => {
			p.tid = was.tid;
			Object.assign(c, was.contract);
			await idb.cache.players.put(p);
		});
		const fair = c.amounts[0]!;
		const worthOn = async (tid: number, amounts: number[]) => {
			c.start = season;
			c.amounts = amounts;
			c.payments = [];
			await idb.cache.players.put(p);
			await fresh();
			return (
				await view.verdict(
					tid,
					PLAYER.FREE_AGENT,
					{ pids: [], dpids: [] },
					{ pids: [p.pid], dpids: [] },
					{ market: false },
				)
			).gain;
		};
		// Whether more years are worth more depends on the player (a 28-year-old
		// on twice a cheap salary can be a bargain for five years); the same
		// years for more money never are.
		const bad: string[] = [];
		for (const tid of teams) {
			for (const years of [1, 3]) {
				const cheap = await worthOn(tid, Array(years).fill(fair));
				const dear = await worthOn(
					tid,
					Array(years).fill(Math.round(fair * 2)),
				);
				note(
					"contract",
					`team ${tid}: ${label(p)} ${years}y at ${fair} vs twice that`,
					cheap - dear,
				);
				if (!(cheap > dear)) {
					bad.push(
						`team ${tid}, ${years}y: ${cheap} at ${fair}, ${dear} at twice that`,
					);
				}
			}
		}
		assert.deepStrictEqual(bad, []);
	},
);
