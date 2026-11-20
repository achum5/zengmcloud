import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE SECOND TIER: one market moment from a league file, each deal open and
// each made with its buyer's place among the franchises (by franchise value)
// and its gain - to see whether teams just below the best are ever offered
// the players they would gain from, and where the offers fall away.
// ST_BUYERS sets how many teams each player is put to.
//   ST_LEAGUE=<league.json> ST_OUT=<file> [ST_BUYERS=2] \
//     npx vitest --run --project basketball src/test/smart/secondTierProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.ST_LEAGUE)(
	"second tier probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.ST_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.ST_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const market = await import("../../worker/core/smartCpu/market.ts");
		market.marketSettings.buyers = Number(env.ST_BUYERS ?? 2);
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		let rank = new Map<number, number>();
		const title = new Map<number, number>();
		const who = (tid: number) =>
			`${abbrev.get(tid)} (#${rank.get(tid)}, ${(100 * (title.get(tid) ?? 0)).toFixed(1)}%)`;
		const name = (x: any) =>
			"pid" in x
				? `${x.firstName} ${x.lastName} ${x.ratings.at(-1).ovr}`
				: `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
		market.smartMomentWatchers.push(async (open, view: any) => {
			const rows = [];
			for (const tid of view.outlook.teams.keys()) {
				rows.push({ tid, v: view.value(tid, view.heldNow(tid)) });
				title.set(tid, view.outlook.teams.get(tid)?.title?.[0] ?? 0);
			}
			rows.sort((a, b) => b.v - a.v);
			rank = new Map(rows.map((r, i) => [r.tid, i + 1]));
			log(`OPEN ${open.length} deals`);
			for (const d of open) {
				log(
					`  ${who(d.buyer)} buys ${d.out.players.map(name).join("; ")} from ${who(d.seller)}; seller +${d.sellerGain.toFixed(4)}`,
				);
			}
		});
		market.smartDealWatchers.push((d) => {
			log(
				`MADE ${who(d.buyer)} gets [${d.out.players.map(name).join("; ")}] from ${who(d.seller)} for [${[...d.offer.players, ...d.offer.picks].map(name).join("; ")}] | buyer +${d.buyerGain.toFixed(4)} seller +${d.sellerGain.toFixed(4)}`,
			);
		});
		const t0 = Date.now();
		await market.marketMoment();
		log(
			`season ${g.get("season")} phase ${g.get("phase")}: ${((Date.now() - t0) / 1000).toFixed(0)} s; ${JSON.stringify(market.marketStats)}; offers ${JSON.stringify((await import("../../worker/core/smartCpu/worth.ts")).offerStats)}`,
		);
	},
);
