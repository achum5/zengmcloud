import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import type { DraftPick, Player } from "../../common/types.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// SMART_LEAGUE=<league.json> SMART_PRICES=<prices-probe.json>: every offer
// from the human contenders that makes them better this season (1-for-1,
// 2-for-1, 1-for-2 among each side's best seven), put to the GM's worth
// (docs/smart-cpu.md: title chances, every asset worth at least its market
// price) and to vanilla at the league's trade difficulty.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

type Prices = {
	assets: { key: string; owner: number; label: string }[];
	use: Record<string, Record<string, number>>;
	price: Record<string, number>;
};

test.skipIf(!env.SMART_LEAGUE || !env.SMART_PRICES)(
	"human offers: the GM against vanilla",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { buildOutlook, franchiseValue, teamStrengths } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { trade } = await import("../../worker/core/index.ts");
		const { ValueChangeCalculator } =
			await import("../../worker/core/team/ValueChangeCalculator.ts");
		const isUntradable = (
			await import("../../worker/core/trade/isUntradable.ts")
		).default;
		const prices: Prices = JSON.parse(
			await fs.readFile(env.SMART_PRICES!, "utf8"),
		);
		await loadLeagueFile(env.SMART_LEAGUE!);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const outlook = await buildOutlook({ iterations: 1000 });
		// Every team's chance of keeping each player it could get, as on that
		// team.
		if (env.SMART_RETENTION !== "0") {
			const { ensureRetention } =
				await import("../../worker/core/realisticFrontOffice/outlook.ts");
			const everyone = (await idb.cache.players.getAll()).filter(
				(p) => p.tid >= 0,
			);
			for (const t of await idb.cache.teams.getAll()) {
				if (!t.disabled) {
					await ensureRetention(outlook, everyone, t.tid);
				}
			}
		}
		g.setWithoutSavingToDB("realisticFrontOffices", false);
		const out: string[] = [];
		const say = (s: string) => out.push(s);
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const humans: number[] = g.get("userTids");
		const owner = new Map(prices.assets.map((a) => [a.key, a.owner]));
		const price = (key: string) => prices.price[key] ?? 0;
		const use = (key: string, tid: number) => prices.use[key]?.[tid] ?? 0;
		const titleNow = new Map(
			[...outlook.teams.values()].map((o) => [o.tid, o.title[0] ?? 0]),
		);

		type Holdings = { players: Player[]; picks: DraftPick[] };
		const keyOf = (x: Player | DraftPick) =>
			"pid" in x ? `p${x.pid}` : `d${x.dpid}`;
		const G = (tid: number, h: Holdings) =>
			franchiseValue(
				outlook,
				teamStrengths(outlook, tid, h.players, h.picks),
				tid,
			);
		// The GM's worth: title chances, and each asset at least its price.
		const worth = (tid: number, h: Holdings) =>
			G(tid, h) +
			[...h.players.map(keyOf), ...h.picks.map(keyOf)]
				.filter((k) => owner.has(k))
				.reduce((s, k) => s + Math.max(0, price(k) - use(k, tid)), 0);
		const holdings = new Map<number, Holdings>();
		const baseWorth = new Map<number, number>();
		for (const t of teams) {
			const h = {
				players: await idb.cache.players.indexGetAll("playersByTid", t.tid),
				picks: await idb.cache.draftPicks.indexGetAll("draftPicksByTid", t.tid),
			};
			holdings.set(t.tid, h);
			baseWorth.set(t.tid, worth(t.tid, h));
		}

		const MAX = Number(env.SMART_SCAN_TOP ?? 7);
		const tradable = (tid: number) =>
			holdings
				.get(tid)!
				.players.filter((p) => !isUntradable(p).untradable)
				.sort((a, b) => b.value - a.value)
				.slice(0, MAX);
		type Row = {
			human: string;
			cpu: string;
			contender: boolean;
			gain: number;
			cpuLoss: number;
			gm: boolean;
			market: boolean;
			rival: string;
			vanilla: boolean;
			gives: string;
			gets: string;
		};
		const rows: Row[] = [];
		const name = (p: Player) =>
			`${p.firstName} ${p.lastName} ${p.ratings.at(-1)!.ovr}/${p.ratings.at(-1)!.pot} ${g.get("season") - p.born.year}y`;
		for (const H of (env.SMART_HUMANS ?? "CLE,BOS")
			.split(",")
			.map((a) => teams.find((t) => t.abbrev === a)!.tid)) {
			g.setWithoutSavingToDB("userTid", H);
			const mine = tradable(H);
			for (const t of teams) {
				const C = t.tid;
				if (humans.includes(C)) {
					continue;
				}
				const theirs = tradable(C);
				const shapes: [Player[], Player[]][] = [];
				for (const a of mine) {
					for (const b of theirs) {
						shapes.push([[a], [b]]);
					}
				}
				for (let i = 0; i < mine.length; i++) {
					for (let j = i + 1; j < mine.length; j++) {
						for (const b of theirs) {
							shapes.push([[mine[i]!, mine[j]!], [b]]);
						}
					}
				}
				for (const a of mine) {
					for (let i = 0; i < theirs.length; i++) {
						for (let j = i + 1; j < theirs.length; j++) {
							shapes.push([[a], [theirs[i]!, theirs[j]!]]);
						}
					}
				}
				const vcc = new ValueChangeCalculator();
				// A package's market price: the second-highest use for all of it
				// together among the other teams most keen on its parts (single
				// prices add up wrong: two good players are worth far less
				// together than apart).
				const keenest = (keys: string[], not: number) => {
					const tids = new Set<number>();
					for (const k of keys) {
						const u = prices.use[k];
						if (!u) {
							continue;
						}
						for (const [tid] of Object.entries(u)
							.map(([t, v]) => [Number(t), v] as const)
							.filter(([t]) => t !== not)
							.sort((a, b) => b[1] - a[1])
							.slice(0, 6)) {
							tids.add(tid);
						}
					}
					return [...tids];
				};
				const packagePrice = (ps: Player[], not: number) => {
					const bids = keenest(ps.map(keyOf), not)
						.map((U) => {
							const hu = holdings.get(U)!;
							const owned = ps.filter((p) => p.tid === U);
							const base =
								owned.length > 0
									? G(U, {
											...hu,
											players: hu.players.filter((p) => !owned.includes(p)),
										})
									: G(U, hu);
							return (
								G(U, {
									...hu,
									players: [
										...hu.players.filter((p) => !owned.includes(p)),
										...ps,
									],
								}) - base
							);
						})
						.sort((a, b) => b - a);
					return bids[1] ?? bids[0] ?? 0;
				};
				// A team's change in worth from giving `out` and getting `in`:
				// title chances, and each side at least its market price as a
				// package.
				const worthDelta = (S: number, out: Player[], inn: Player[]) => {
					const h = holdings.get(S)!;
					const gS = G(S, h);
					const useIn = G(S, { ...h, players: [...h.players, ...inn] }) - gS;
					const useOut =
						gS -
						G(S, { ...h, players: h.players.filter((p) => !out.includes(p)) });
					const dG =
						G(S, {
							...h,
							players: [...h.players.filter((p) => !out.includes(p)), ...inn],
						}) - gS;
					return (
						dG +
						Math.max(0, packagePrice(inn, S) - useIn) -
						Math.max(0, packagePrice(out, S) - useOut)
					);
				};
				// The same, quicker, for searching: single prices.
				const quickDelta = (S: number, out: Player[], inn: Player[]) => {
					const h = holdings.get(S)!;
					const gS = G(S, h);
					const dG =
						G(S, {
							...h,
							players: [...h.players.filter((p) => !out.includes(p)), ...inn],
						}) - gS;
					const prem = (ps: Player[]) =>
						ps.reduce(
							(sum, p) => sum + Math.max(0, price(keyOf(p)) - use(keyOf(p), S)),
							0,
						);
					return dG + prem(inn) - prem(out);
				};
				// THE MARKET (owner, 2026-11-13: "the cpu would not be willing to
				// send me good player if another good team would give up more
				// than i'm currently offering"): the CPU teams keenest on what a
				// team gives up each make their best real offer - their own
				// players, as much as still leaves them better off by their own
				// worth, legal under the trade rules - and the team takes an
				// offer only if it's worth more to it than every one of those.
				const realOffers = new Map<
					string,
					{ buyer: number; worth: number; offer: Player[] }
				>();
				const bestRealOffer = async (S: number, out: Player[], not: number) => {
					const key = `${S}|${out
						.map((p) => p.pid)
						.sort()
						.join(",")}|${not}`;
					const known = realOffers.get(key);
					if (known) {
						return known;
					}
					const buyers = keenest(out.map(keyOf), S)
						.filter((B) => B !== not && !humans.includes(B))
						.map((B) => ({
							B,
							want: out.reduce((sum, p) => sum + use(keyOf(p), B), 0),
						}))
						.sort((a, b) => b.want - a.want)
						.slice(0, Number(env.SMART_BUYERS ?? 4));
					let best = { buyer: -1, worth: -Infinity, offer: [] as Player[] };
					for (const { B } of buyers) {
						const theirs = holdings
							.get(B)!
							.players.filter(
								(p) => owner.has(keyOf(p)) && !isUntradable(p).untradable,
							)
							.sort(
								(a, b) =>
									Math.max(use(keyOf(b), S), price(keyOf(b))) -
									Math.max(use(keyOf(a), S), price(keyOf(a))),
							)
							.slice(0, 8);
						let offer: Player[] = [];
						for (const p of theirs) {
							if (offer.length >= 3) {
								break;
							}
							const next = [...offer, p];
							if (quickDelta(B, next, out) > 0) {
								offer = next;
							}
						}
						if (offer.length === 0) {
							continue;
						}
						const legal = await trade.summary([
							{
								tid: B,
								pids: offer.map((p) => p.pid),
								dpids: [],
								pidsExcluded: [],
								dpidsExcluded: [],
							},
							{
								tid: S,
								pids: out.map((p) => p.pid),
								dpids: [],
								pidsExcluded: [],
								dpidsExcluded: [],
							},
						] as any);
						if (legal.warning || worthDelta(B, offer, out) <= 0) {
							continue;
						}
						const w = worthDelta(S, out, offer);
						if (w > best.worth) {
							best = { buyer: B, worth: w, offer };
						}
					}
					realOffers.set(key, best);
					return best;
				};
				for (const [gives, gets] of shapes) {
					const s = await trade.summary([
						{
							tid: H,
							pids: gives.map((p) => p.pid),
							dpids: [],
							pidsExcluded: [],
							dpidsExcluded: [],
						},
						{
							tid: C,
							pids: gets.map((p) => p.pid),
							dpids: [],
							pidsExcluded: [],
							dpidsExcluded: [],
						},
					] as any);
					if (s.warning) {
						continue;
					}
					// (The summary's teams are the other way round.)
					const gain = s.teams[1].ovrAfter - s.teams[1].ovrBefore;
					if (gain < 1) {
						continue;
					}
					const h = holdings.get(C)!;
					const after = {
						...h,
						players: [...h.players.filter((p) => !gets.includes(p)), ...gives],
					};
					// The GM: title chances, and what it gets and gives each at
					// least its market price as a package.
					const gC = G(C, h);
					const useGets =
						gC -
						G(C, { ...h, players: h.players.filter((p) => !gets.includes(p)) });
					const useGives =
						G(C, { ...h, players: [...h.players, ...gives] }) - gC;
					const dG = G(C, after) - gC;
					const gm =
						dG +
							Math.max(0, packagePrice(gives, C) - useGives) -
							Math.max(0, packagePrice(gets, C) - useGets) >
						0;
					// With the market: only if no real offer elsewhere beats it.
					let market = gm;
					let rival = "";
					if (gm) {
						const worthHere = worthDelta(C, gets, gives);
						const best = await bestRealOffer(C, gets, H);
						if (best.worth >= worthHere) {
							market = false;
							rival = `${abbrev.get(best.buyer)} offers ${best.offer.map(name).join(" + ")}`;
						}
					}
					const vdv = await vcc.evaluate({
						tid: C,
						pidsAdd: gives.map((p) => p.pid),
						pidsRemove: gets.map((p) => p.pid),
						dpidsAdd: [],
						dpidsRemove: [],
						tradingPartnerTid: H,
					});
					rows.push({
						human: abbrev.get(H)!,
						cpu: abbrev.get(C)!,
						contender: (titleNow.get(C) ?? 0) >= 0.01,
						gain,
						cpuLoss: s.teams[0].ovrBefore - s.teams[0].ovrAfter,
						gm,
						market,
						rival,
						vanilla: vdv > 0,
						gives: gives.map(name).join(" + "),
						gets: gets.map(name).join(" + "),
					});
				}
			}
		}
		for (const human of new Set(rows.map((r) => r.human))) {
			const mine = rows.filter((r) => r.human === human);
			for (const cut of [1, 2, 3, 4, 6]) {
				const w = mine.filter((r) => r.gain >= cut);
				say(
					`${human}: offers that raise its rating ${cut}+ : ${w.length}; CPU says yes - vanilla 3.5 ${w.filter((r) => r.vanilla).length}, GM ${w.filter((r) => r.gm).length}, GM with the market ${w.filter((r) => r.market).length}`,
				);
			}
			const c = mine.filter((r) => r.gain >= 2 && r.contender);
			say(
				`   to CPU teams with 1%+ title chance now: ${c.length}; yes - vanilla ${c.filter((r) => r.vanilla).length}, GM ${c.filter((r) => r.gm).length}, with the market ${c.filter((r) => r.market).length}`,
			);
			say("   the GM's yeses with the market, biggest gain first:");
			for (const r of mine
				.filter((x) => x.market)
				.sort((a, b) => b.gain - a.gain)
				.slice(0, 8)) {
				say(
					`     +${r.gain} (${r.cpu} ${-r.cpuLoss}): gives ${r.gives}; gets ${r.gets}${r.vanilla ? " [vanilla yes too]" : ""}`,
				);
			}
			say(
				"   turned down by the market (a real better offer elsewhere), biggest gain first:",
			);
			for (const r of mine
				.filter((x) => x.gm && !x.market)
				.sort((a, b) => b.gain - a.gain)
				.slice(0, 6)) {
				say(
					`     +${r.gain} (${r.cpu}): gives ${r.gives}; gets ${r.gets} | ${r.rival}`,
				);
			}
		}
		await fs.writeFile(`${env.SMART_OUT ?? "."}/gm-scan.txt`, out.join("\n"));
		await fs.writeFile(
			`${env.SMART_OUT ?? "."}/gm-scan.json`,
			JSON.stringify(rows),
		);
	},
);
