import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import type { DraftPick, Player } from "../../common/types.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// SMART_LEAGUE=<league.json> SMART_PRICES=<prices-probe.json>: each team's
// window and exchange rate worked out by spending, not by its best bargains
// (docs/smart-cpu.md, "The goal, worked out"). A team's spare assets are the
// ones the market prices above their use to it; it sells them at the market
// price and buys, one at a time, what does most for its title chances per
// unit of price, its title chances re-played after every buy. What that
// bought, per unit spent, is its rate. Then some trades, by the team's worth:
// title chances from what it keeps, plus its spare assets at its rate.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

type Prices = {
	assets: { key: string; owner: number; label: string }[];
	use: Record<string, Record<string, number>>;
	price: Record<string, number>;
};

test.skipIf(!env.SMART_LEAGUE || !env.SMART_PRICES)(
	"windows and exchange rates by spending",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { buildOutlook, franchiseValue, teamStrengths } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const prices: Prices = JSON.parse(
			await fs.readFile(env.SMART_PRICES!, "utf8"),
		);
		await loadLeagueFile(env.SMART_LEAGUE!);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const out: string[] = [];
		const say = (s: string) => out.push(s);
		const m = (x: number) => (x * 1000).toFixed(1);
		const outlook = await buildOutlook({ iterations: 1000 });
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const players = new Map(
			(await idb.cache.players.getAll()).map((p) => [p.pid, p]),
		);
		const picks = new Map(
			(await idb.cache.draftPicks.getAll()).map((dp) => [dp.dpid, dp]),
		);
		const label = new Map(prices.assets.map((a) => [a.key, a.label]));
		const owner = new Map(prices.assets.map((a) => [a.key, a.owner]));
		const price = (key: string) => prices.price[key] ?? 0;
		const use = (key: string, tid: number) => prices.use[key]?.[tid] ?? 0;

		type Holdings = { players: Player[]; picks: DraftPick[] };
		const holdingsOf = async (tid: number): Promise<Holdings> => ({
			players: await idb.cache.players.indexGetAll("playersByTid", tid),
			picks: await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid),
		});
		const keyOf = (x: Player | DraftPick) =>
			"pid" in x ? `p${x.pid}` : `d${x.dpid}`;
		const G = (tid: number, h: Holdings) =>
			franchiseValue(outlook, teamStrengths(outlook, tid, h.players, h.picks), tid);
		const without = (h: Holdings, keys: Set<string>): Holdings => ({
			players: h.players.filter((p) => !keys.has(keyOf(p))),
			picks: h.picks.filter((dp) => !keys.has(keyOf(dp))),
		});
		const plus = (h: Holdings, key: string): Holdings =>
			key.startsWith("p")
				? { ...h, players: [...h.players, players.get(Number(key.slice(1)))!] }
				: { ...h, picks: [...h.picks, picks.get(Number(key.slice(1)))!] };

		// Each team: spare assets, budget, the spending, the rate.
		const rate = new Map<number, number>();
		const spare = new Map<number, Set<string>>();
		const t0 = performance.now();
		for (const t of teams) {
			const h = await holdingsOf(t.tid);
			const owned = prices.assets.filter((a) => a.owner === t.tid);
			const spareKeys = new Set(
				owned.filter((a) => use(a.key, t.tid) < price(a.key)).map((a) => a.key),
			);
			const budget = [...spareKeys].reduce((s, k) => s + Math.max(0, price(k)), 0);
			const kept = without(h, spareKeys);
			const before = G(t.tid, kept);
			const buys = prices.assets
				.filter((a) => a.owner !== t.tid && price(a.key) > 0.0005)
				.map((a) => ({ key: a.key, ratio: use(a.key, t.tid) / price(a.key) }))
				.filter((x) => x.ratio > 0)
				.sort((a, b) => b.ratio - a.ratio);
			let left = budget;
			let now = kept;
			let value = before;
			const bought: string[] = [];
			for (const b of buys) {
				if (price(b.key) > left || bought.length >= 12) {
					continue;
				}
				const next = plus(now, b.key);
				const v = G(t.tid, next);
				if (v > value) {
					now = next;
					value = v;
					left -= price(b.key);
					bought.push(b.key);
				}
			}
			const spent = budget - left;
			rate.set(t.tid, spent > 0 ? (value - before) / spent : 0);
			spare.set(t.tid, spareKeys);
			say(
				`${abbrev.get(t.tid)!.padEnd(4)} value ${m(G(t.tid, h))} | keeps ${m(before)}, spare ${spareKeys.size} worth ${m(budget)} at market | spends ${m(spent)} on ${bought.length}: value ${m(value)} | rate ${(rate.get(t.tid) ?? 0).toFixed(2)} | buys: ${bought
					.slice(0, 4)
					.map((k) => (label.get(k) ?? k).split(" (")[0])
					.join(", ")}`,
			);
		}
		say(`(${((performance.now() - t0) / 1000).toFixed(0)} s)`);

		// A team's worth of its holdings: title chances from what it keeps, and
		// its spare assets at its rate.
		const worthOf = (tid: number, h: Holdings) => {
			const keys = [...h.players.map(keyOf), ...h.picks.map(keyOf)];
			const spareHere = new Set(
				keys.filter(
					(k) =>
						owner.has(k) && use(k, tid) < (rate.get(tid) ?? 0) * price(k),
				),
			);
			return (
				G(tid, without(h, spareHere)) +
				(rate.get(tid) ?? 0) *
					[...spareHere].reduce((s, k) => s + Math.max(0, price(k)), 0)
			);
		};
		const byName = (name: string) =>
			[...players.values()].find((p) => `${p.firstName} ${p.lastName}` === name)!;
		const trade = async (
			title: string,
			a: string,
			aGives: string[],
			b: string,
			bGives: string[],
		) => {
			const ta = teams.find((t) => t.abbrev === a)!.tid;
			const tb = teams.find((t) => t.abbrev === b)!.tid;
			const ga = aGives.map(byName);
			const gb = bGives.map(byName);
			// The state before the trade: each side holds what it gives.
			const ha = await holdingsOf(ta);
			const hb = await holdingsOf(tb);
			const beforeA = {
				...ha,
				players: [...ha.players.filter((p) => !gb.includes(p)), ...ga.filter((p) => !ha.players.includes(p))],
			};
			const beforeB = {
				...hb,
				players: [...hb.players.filter((p) => !ga.includes(p)), ...gb.filter((p) => !hb.players.includes(p))],
			};
			const afterA = { ...beforeA, players: [...beforeA.players.filter((p) => !ga.includes(p)), ...gb] };
			const afterB = { ...beforeB, players: [...beforeB.players.filter((p) => !gb.includes(p)), ...ga] };
			// Variant B: every asset worth at least its market price - title
			// chances, plus each asset's price above its use to the team.
			const bonus = (tid: number, h: Holdings) =>
				[...h.players.map(keyOf), ...h.picks.map(keyOf)]
					.filter((k) => owner.has(k))
					.reduce((s, k) => s + Math.max(0, price(k) - use(k, tid)), 0);
			const wB = (tid: number, h: Holdings) => G(tid, h) + bonus(tid, h);
			const bA = wB(ta, afterA) - wB(ta, beforeA);
			const bB = wB(tb, afterB) - wB(tb, beforeB);
			const dA = worthOf(ta, afterA) - worthOf(ta, beforeA);
			const dB = worthOf(tb, afterB) - worthOf(tb, beforeB);
			const gA = G(ta, afterA) - G(ta, beforeA);
			const gB = G(tb, afterB) - G(tb, beforeB);
			say(
				`${title}: ${a} ${dA > 0 ? "YES" : "no"} (worth ${m(dA)}, titles alone ${m(gA)}, B ${m(bA)}), ${b} ${dB > 0 ? "YES" : "no"} (worth ${m(dB)}, titles alone ${m(gB)}, B ${m(bB)})`,
			);
		};
		say("\nTrades (each side's change in worth, milli-titles; > 0 = takes it):");
		await trade("GSW sends Harris to HOU for Barlow + Roddy", "GSW", ["Tobias Harris"], "HOU", ["Dominick Barlow", "David Roddy"]);
		await trade("LAL sends Beasley to BOS for Kessler + Branham", "LAL", ["Malik Beasley"], "BOS", ["Walker Kessler", "Malaki Branham"]);
		await trade("DET sends Doncic to BOS for Kessler + Branham + Williams", "DET", ["Luka Dončić"], "BOS", ["Walker Kessler", "Malaki Branham", "Mark Williams"]);
		await trade("POR sends Bagley to CLE for Herbert Jones", "POR", ["Marvin Bagley III"], "CLE", ["Herbert Jones"]);
		await fs.writeFile(`${env.SMART_OUT ?? "."}/window-probe.txt`, out.join("\n"));
	},
);
