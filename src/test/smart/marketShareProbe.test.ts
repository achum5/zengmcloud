import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// One market moment on a real league, every deal it makes written out with
// what each side gains and the seller's share of the two - to see what the
// seller's share (marketSettings.sellerShare) changes.
//   MS_LEAGUE=<league.json> MS_OUT=<file> [MS_SHARE=0.5] [MS_MOMENTS=1 | MS_DAYS=30] \
//     npx vitest --run --project basketball src/test/smart/marketShareProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.MS_LEAGUE)(
	"market share probe",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.MS_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.MS_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		const { marketMoment, marketSettings, smartDealWatchers, standing } =
			await import("../../worker/core/smartCpu/market.ts");
		const { idb } = await import("../../worker/db/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		if (env.MS_SHARE !== undefined) {
			marketSettings.sellerShare = Number(env.MS_SHARE);
		}
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const name = (x: any) =>
			"pid" in x
				? `${x.firstName} ${x.lastName} ${g.get("season") - x.born.year}y ${x.ratings.at(-1).ovr}/${x.ratings.at(-1).pot}`
				: `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
		smartDealWatchers.push((deal: any, view: any) => {
			const s = deal.sellerGain;
			const b = deal.buyerGain;
			const sb = standing(view, deal.seller) as any;
			const bb = standing(view, deal.buyer) as any;
			log(
				`${abbrev.get(deal.seller)} (${(100 * (sb?.title ?? 0)).toFixed(1)}%) sells [${[...deal.out.players, ...deal.out.picks].map(name).join("; ")}] to ${abbrev.get(deal.buyer)} (${(100 * (bb?.title ?? 0)).toFixed(1)}%) for [${[...deal.offer.players, ...deal.offer.picks].map(name).join("; ")}] | seller +${s.toFixed(4)}, buyer +${b.toFixed(4)}, seller's share ${(s / (s + Math.max(0, b))).toFixed(2)}`,
			);
		});
		log(
			`${g.get("season")} phase ${g.get("phase")}, sellerShare ${marketSettings.sellerShare}`,
		);
		// MS_DEBUG: into the season, up to the first market moment, then every
		// deal it finds shown untrimmed and trimmed, with each side's gain.
		if (env.MS_DEBUG) {
			const { game, phase } = await import("../../worker/core/index.ts");
			const { PHASE } = await import("../../common/constants.ts");
			const { currentOutlook, gamesPlayed } =
				await import("../../worker/core/realisticFrontOffice/market.ts");
			const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
			const { dealsNow, trimDeal } =
				await import("../../worker/core/smartCpu/market.ts");
			if (g.get("phase") < PHASE.REGULAR_SEASON) {
				await phase.newPhase(PHASE.REGULAR_SEASON, {});
			}
			const every =
				g.get("numActiveTeams") * marketSettings.vanillaGamesBetween;
			const cpu = new Set(teams.filter((t) => !t.disabled).map((t) => t.tid));
			for (let k = 1; k <= Number(env.MS_DEBUG); k++) {
				while ((await gamesPlayed()) + 30 < k * every) {
					await game.play(1, {});
				}
				const view = viewFor(await currentOutlook());
				const found = await dealsNow(view, cpu);
				log(
					`\n=== ${found.length} deals found at ${await gamesPlayed()} games`,
				);
				const both = (d: any, offer: any) => {
					const s = view.dealWorthNow(d.seller, d.buyer, d.out, offer);
					const b = view.dealWorthNow(d.buyer, d.seller, offer, d.out);
					return `seller ${s.toFixed(4)}, buyer ${b.toFixed(4)}, share ${(s / (s + Math.max(0, b))).toFixed(2)}`;
				};
				const list = (h: any) =>
					[...h.players, ...h.picks].map(name).join("; ");
				for (const d of found.slice(0, 25)) {
					log(
						`\n${abbrev.get(d.seller)} sells [${list(d.out)}] to ${abbrev.get(d.buyer)}; bar ${d.bar.toFixed(4)}`,
					);
					log(`   untrimmed [${list(d.offer)}]: ${both(d, d.offer)}`);
					for (const share of [0, 0.5]) {
						marketSettings.sellerShare = share;
						const t = await trimDeal(view, d);
						log(`   trimmed at ${share} [${list(t)}]: ${both(d, t)}`);
					}
				}
			}
			return;
		}
		// MS_DAYS: into the regular season and play that many days instead,
		// the market meeting as it does in a season.
		if (env.MS_DAYS) {
			const { game, phase } = await import("../../worker/core/index.ts");
			const { PHASE } = await import("../../common/constants.ts");
			if (g.get("phase") < PHASE.REGULAR_SEASON) {
				await phase.newPhase(PHASE.REGULAR_SEASON, {});
			}
			const t = performance.now();
			await game.play(Number(env.MS_DAYS), {});
			log(
				`${env.MS_DAYS} days in ${((performance.now() - t) / 1000).toFixed(0)} s`,
			);
			const { marketStats } =
				await import("../../worker/core/smartCpu/market.ts");
			log(`market stats: ${JSON.stringify(marketStats)}`);
			const rows = (await idb.cache.teamSeasons.getAll())
				.filter((ts) => ts.season === g.get("season"))
				.map((ts) => ({ a: abbrev.get(ts.tid), w: ts.won, l: ts.lost }))
				.sort(
					(x, y) => y.w / Math.max(1, y.w + y.l) - x.w / Math.max(1, x.w + x.l),
				);
			log(`standings: ${rows.map((r) => `${r.a} ${r.w}-${r.l}`).join(", ")}`);
			return;
		}
		const moments = Number(env.MS_MOMENTS ?? 1);
		for (let i = 0; i < moments; i++) {
			const t = performance.now();
			const made = await marketMoment();
			log(
				`moment ${i + 1}: ${made.length} deals in ${((performance.now() - t) / 1000).toFixed(0)} s`,
			);
		}
	},
);
