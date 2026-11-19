import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Teams stuck in the middle: a league played on to a later preseason, then
// every trade written out by what each side gave and got (ages, ratings,
// picks) beside where each team stood when the run began - its title odds,
// its chance of the playoffs and the series it was expected to win - to see
// whether teams out of the race keep their veterans or build, with a run
// counted by series won or by making the playoffs (outlookSettings.
// seriesOnly), and players got in a trade counted at use or at their price
// (smartSettings.boughtAtUse). Each market deal is also written as made,
// each side's gain split into its goal and the price premiums; and at the
// end, each season's records and where the league's 20 best players are.
//   PG_LEAGUE=<league.json> PG_OUT=<file> [PG_SERIES=0|1] [PG_ATUSE=0|1] \
//     [PG_PICKS=0|1] [PG_SEASONS=1] npx vitest --run --project basketball \
//     src/test/smart/purgatoryProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PG_LEAGUE)(
	"purgatory probe",
	{ timeout: 14_400_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.PG_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.PG_LEAGUE!);
		const { league, game, phase, draft, freeAgents } =
			await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const { outlookSettings } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		if (env.PG_SERIES !== undefined) {
			outlookSettings.seriesOnly = env.PG_SERIES === "1";
		}
		const { smartSettings } =
			await import("../../worker/core/smartCpu/worth.ts");
		if (env.PG_ATUSE !== undefined) {
			smartSettings.boughtAtUse = env.PG_ATUSE === "1";
		}
		if (env.PG_PICKS !== undefined) {
			smartSettings.laterPicksByNextClass = env.PG_PICKS === "1";
		}
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const { smartDealWatchers } =
			await import("../../worker/core/smartCpu/market.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const named = (x: any) =>
			"pid" in x
				? `${x.firstName} ${x.lastName} ${g.get("season") - x.born.year}y ${x.ratings.at(-1).ovr}/${x.ratings.at(-1).pot}${contractOf(x).exp <= g.get("season") ? " (expiring)" : ""}`
				: `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
		// A side's gain: its goal, and the premiums on what it gets and gives.
		const split = (
			view: any,
			tid: number,
			other: number,
			inn: any,
			out: any,
			gain: number,
		) => {
			const h = view.heldNow(tid);
			const pin = view.premiumNow(
				tid,
				smartSettings.boughtAtUse ? { players: [], picks: inn.picks } : inn,
				[tid, other],
				h,
			);
			const pout = view.premiumNow(tid, out, [tid, other], h);
			return `goal ${(gain - pin + pout).toFixed(4)}, premium got ${pin.toFixed(4)} given ${pout.toFixed(4)}`;
		};
		const deals: string[] = [];
		smartDealWatchers.push((deal: any, view: any) => {
			const title = (tid: number) =>
				(100 * (view.outlook.teams.get(tid)?.title?.[0] ?? 0)).toFixed(1);
			deals.push(
				`${g.get("season")} ph${g.get("phase")} ${abbrev.get(deal.seller)} (${title(deal.seller)}%) sells [${[...deal.out.players, ...deal.out.picks].map(named).join("; ")}] to ${abbrev.get(deal.buyer)} (${title(deal.buyer)}%) for [${[...deal.offer.players, ...deal.offer.picks].map(named).join("; ")}] | seller +${deal.sellerGain.toFixed(4)} (${split(view, deal.seller, deal.buyer, deal.offer, deal.out, deal.sellerGain)}), buyer +${deal.buyerGain.toFixed(4)} (${split(view, deal.buyer, deal.seller, deal.out, deal.offer, deal.buyerGain)})`,
			);
		});
		const startSeason = g.get("season");
		const outlook: any = await currentOutlook();
		const standing = new Map<number, string>();
		for (const t of teams) {
			const o = outlook.teams.get(t.tid);
			if (o) {
				standing.set(
					t.tid,
					`${(100 * o.title[0]).toFixed(1)}% title, ${(100 * o.playoffs[0]).toFixed(0)}% playoffs`,
				);
			}
		}
		log(
			`${startSeason} phase ${g.get("phase")}, seriesOnly ${outlookSettings.seriesOnly}, boughtAtUse ${smartSettings.boughtAtUse}, laterPicksByNextClass ${smartSettings.laterPicksByNextClass}`,
		);
		for (const t of teams) {
			log(`  ${t.abbrev}: ${standing.get(t.tid)}`);
		}
		const seasons = Number(env.PG_SEASONS ?? 1);
		const events = async () => {
			const all = [];
			for (let s = startSeason; s <= startSeason + seasons; s++) {
				all.push(...(await idb.merged.events.getAll({ season: s })));
			}
			return all;
		};
		const firstEid =
			Math.max(-1, ...(await events()).map((e: any) => e.eid)) + 1;
		const t0 = performance.now();
		let guard = 0;
		while (
			!(
				g.get("season") >= startSeason + seasons &&
				g.get("phase") === PHASE.PRESEASON
			) &&
			guard++ < 5000
		) {
			const ph = g.get("phase");
			if (
				ph === PHASE.PLAYOFFS ||
				ph === PHASE.REGULAR_SEASON ||
				ph === PHASE.AFTER_TRADE_DEADLINE
			) {
				await game.play(ph === PHASE.PLAYOFFS ? 100 : 7, {});
			} else if (ph === PHASE.DRAFT) {
				await draft.runPicks({ type: "untilEnd" }, {});
				if (g.get("phase") === PHASE.DRAFT) {
					await phase.newPhase(PHASE.AFTER_DRAFT, {});
				}
			} else if (ph === PHASE.FREE_AGENCY) {
				await freeAgents.play(g.get("daysLeft"), {});
				if (g.get("phase") === PHASE.FREE_AGENCY) {
					await phase.newPhase(PHASE.PRESEASON, {});
				}
			} else {
				await phase.newPhase((ph + 1) as any, {});
			}
		}
		log(
			`to ${g.get("season")} preseason in ${((performance.now() - t0) / 60000).toFixed(1)} min`,
		);
		const players = new Map<number, any>();
		for (const p of await idb.merged.players.getAll({
			activeAndRetired: true,
		})) {
			players.set(p.pid, p);
		}
		const describe = (a: any, season: number) => {
			if ("dpid" in a) {
				return `${a.season} R${a.round} ${abbrev.get(a.originalTid)}`;
			}
			const p = players.get(a.pid);
			const r = p?.ratings[a.ratingsIndex] ?? p?.ratings.at(-1);
			return `${a.name} ${p ? season - p.born.year : "?"}y ${r ? `${r.ovr}/${r.pot}` : "?"}`;
		};
		const trades = (await events()).filter(
			(e: any) => e.eid >= firstEid && e.type === "trade",
		);
		log(`\n${trades.length} trades`);
		for (const e of trades as any[]) {
			const [a, b] = e.tids;
			log(
				`${e.season} ph${e.phase} ${e.aiTrade?.motivation ?? ""}: ${abbrev.get(a)} (${standing.get(a)}) gets [${e.teams[0].assets.map((x: any) => describe(x, e.season)).join("; ")}] | ${abbrev.get(b)} (${standing.get(b)}) gets [${e.teams[1].assets.map((x: any) => describe(x, e.season)).join("; ")}]`,
			);
		}
		log(`\n${deals.length} market deals as made`);
		for (const d of deals) {
			log(d);
		}
		const best = [...players.values()]
			.filter((p: any) => p.tid >= 0)
			.sort((a: any, b: any) => b.ratings.at(-1).ovr - a.ratings.at(-1).ovr)
			.slice(0, 20);
		const holders = new Map<string, number>();
		for (const p of best) {
			const ab = abbrev.get(p.tid)!;
			holders.set(ab, (holders.get(ab) ?? 0) + 1);
		}
		log(
			`\nthe 20 best players now: ${[...holders.entries()]
				.sort((a, b) => b[1] - a[1])
				.map(([ab, n]) => `${ab} ${n}`)
				.join(", ")}`,
		);
		log("\nrecords");
		for (let s = startSeason; s < g.get("season"); s++) {
			const rows = (await idb.merged.teamSeasons.getAll({
				season: s,
			})) as any[];
			log(
				`${s}: ${rows
					.sort((x, y) => y.won - x.won)
					.map(
						(r) =>
							`${abbrev.get(r.tid)} ${r.won}-${r.lost} r${r.playoffRoundsWon}`,
					)
					.join(", ")}`,
			);
		}
	},
);
