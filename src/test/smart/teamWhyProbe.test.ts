import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// WHY A TEAM ISN'T DEALING: an owner's league at a market moment, run once
// as it stands (nobody rested), told from one team's side - the league's
// standing by the outlook, its roster and picks, every player it bid for
// and what came of it, the deals it was in, and what it could gain past
// the cool-off.
//   WT_LEAGUE=<league.json> WT_OUT=<file> WT_TID=<tid> npx vitest --run \
//     --project basketball src/test/smart/teamWhyProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.WT_LEAGUE)(
	"why a team isn't dealing",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.WT_OUT!, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.WT_LEAGUE!);
		const tid = Number(env.WT_TID);
		const { g } = await import("../../worker/util/index.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const { league } = await import("../../worker/core/index.ts");
		const { smartHumans, viewFor } =
			await import("../../worker/core/smartCpu/signing.ts");
		const market = await import("../../worker/core/smartCpu/market.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const getPayroll = (await import("../../worker/core/team/getPayroll.ts"))
			.default;
		smartHumans.tids = [...g.get("userTids")];
		await league.setGameAttributes({ smartMarketRested: [] });
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const season = g.get("season");
		const name = (p: any) =>
			`${p.firstName} ${p.lastName} (${season - p.born.year}, ${p.ratings.at(-1).ovr}/${p.ratings.at(-1).pot})`;
		const pickName = (dp: any) =>
			`${dp.season} R${dp.round} ${abbrev.get(dp.originalTid)}`;
		const holdings = (h: any) =>
			[...h.players.map(name), ...h.picks.map(pickName)].join("; ");

		const outlook = await currentOutlook();
		const view = viewFor(outlook);
		await view.prime();
		log(
			`${season}, ${g.get("phase")}; humans ${g
				.get("userTids")
				.map((t: number) => abbrev.get(t))
				.join(" ")}`,
		);
		const ranked = [...outlook.teams.keys()].sort(
			(a, b) =>
				outlook.teams.get(b)!.title[0]! - outlook.teams.get(a)!.title[0]!,
		);
		for (const t of ranked.slice(0, 12)) {
			const ts = await idb.cache.teamSeasons.indexGet(
				"teamSeasonsBySeasonTid",
				[season, t],
			);
			log(
				`  ${abbrev.get(t)} ${ts?.won}-${ts?.lost}: ${market.standing(view, t)}`,
			);
		}
		const roster = (await idb.cache.players.indexGetAll("playersByTid", tid))
			.slice()
			.sort((a, b) => b.value - a.value);
		log(
			`\n${abbrev.get(tid)} payroll $${((await getPayroll(tid)) / 1000).toFixed(1)}M (cap $${(g.get("salaryCap") / 1000).toFixed(1)}M, tax $${(g.get("luxuryPayroll") / 1000).toFixed(1)}M, ${g.get("salaryCapType")} cap)`,
		);
		for (const p of roster) {
			const c = contractOf(p);
			log(
				`  ${name(p)} value ${p.value.toFixed(1)}, $${(c.amount / 1000).toFixed(1)}M to ${c.exp}${p.gamesUntilTradable > 0 ? `, untradable ${p.gamesUntilTradable} games (${p.tradableOn ?? ""})` : ""}`,
			);
		}
		const picks = (await idb.cache.draftPicks.getAll()).filter(
			(dp) => dp.tid === tid,
		);
		log(`  picks: ${picks.map(pickName).join(", ") || "none"}`);

		const bids: string[] = [];
		const sales: string[] = [];
		market.smartTraceWatchers.push((t) => {
			const line = `${t.stage}: ${name(t.p)} of ${abbrev.get(t.seller)}${t.buyer !== undefined ? `, to ${abbrev.get(t.buyer)}` : ""}${t.buyers ? `, bidders ${t.buyers.map((b) => abbrev.get(b)).join(" ")}` : ""}${t.sellerGain !== undefined ? `, seller ${t.sellerGain.toFixed(4)}` : ""}${t.buyerGain !== undefined ? `, buyer ${t.buyerGain.toFixed(4)}` : ""}${t.why ? ` (${t.why})` : ""}`;
			if (t.seller === tid) {
				sales.push(line);
			} else if (t.buyer === tid || t.buyers?.includes(tid)) {
				bids.push(line);
			}
		});
		let found = 0;
		const involved: string[] = [];
		market.smartMomentWatchers.push(async (deals, v) => {
			found = deals.length;
			for (const d of deals) {
				if (d.buyer === tid || d.seller === tid) {
					const buyerGain = await v.dealWorth(
						d.buyer,
						d.seller,
						d.offer,
						d.out,
					);
					involved.push(
						`${abbrev.get(d.buyer)} gets ${holdings(d.out)} from ${abbrev.get(d.seller)} for [${holdings(d.offer)}]: seller ${d.sellerGain.toFixed(4)}, buyer ${buyerGain.toFixed(4)}`,
					);
				}
			}
		});
		const made: string[] = [];
		market.smartDealWatchers.push((d) => {
			made.push(
				`${abbrev.get(d.buyer)} gets ${holdings(d.out)} from ${abbrev.get(d.seller)} for [${holdings(d.offer)}]: seller ${d.sellerGain.toFixed(4)}, buyer ${d.buyerGain.toFixed(4)}`,
			);
		});
		let laterGain: number | undefined;
		market.smartLaterWatchers.push((_ahead, later) => {
			laterGain = later.get(tid);
		});
		const t0 = performance.now();
		await market.marketMoment();
		log(
			`\na moment now (${((performance.now() - t0) / 1000).toFixed(0)} s): ${found} deals found, ${made.length} made`,
		);
		for (const m of made) {
			log(`  MADE ${m}`);
		}
		log(`\n${abbrev.get(tid)} in the deals found: ${involved.length}`);
		for (const x of involved) {
			log(`  ${x}`);
		}
		log(`\n${abbrev.get(tid)} as a bidder: ${bids.length} players`);
		for (const x of bids) {
			log(`  ${x}`);
		}
		log(`\n${abbrev.get(tid)}'s players shopped: ${sales.length}`);
		for (const x of sales) {
			log(`  ${x}`);
		}
		log(
			`\nits best gain past the cool-off: ${laterGain === undefined ? "none" : laterGain.toFixed(4)}`,
		);
	},
);
