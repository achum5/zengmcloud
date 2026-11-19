import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// What draft picks are priced at on a real league, and why: for some of the
// favorite's, a middle team's and a bad team's picks in each round and
// draft, the market's price, the bids it is made of, its floor (pickFloorNow)
// and what it is worth to a team out of the race; and the class prices the
// floors come from (the next draft's prospects, best first).
//   PP_LEAGUE=<league.json> PP_OUT=<file> npx vitest --run \
//     --project basketball src/test/smart/pickPriceProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PP_LEAGUE)(
	"pick price probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.PP_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.PP_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		await view.classPrices();
		const keen: number[] = await view.keenest();
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const title = (tid: number) => outlook.teams.get(tid)?.title?.[0] ?? 0;
		log(
			`${g.get("season")} phase ${g.get("phase")}; keenest ${keen.map((t) => `${abbrev.get(t)} ${(100 * title(t)).toFixed(1)}%`).join(", ")}`,
		);
		const classes = view.keenestCache?.classes as Map<number, number[]>;
		for (const [season, prices] of classes ?? []) {
			log(
				`class ${season} prices (best first, titles): ${prices
					.slice(0, 60)
					.map((x: number) => x.toFixed(3))
					.join(" ")}`,
			);
		}
		const ranked = [...outlook.teams.keys()].sort(
			(a: number, b: number) => title(b) - title(a),
		);
		const favorite = ranked[0]!;
		const middle = ranked[Math.floor(ranked.length / 2)]!;
		const worst = ranked.at(-1)!;
		// A team out of the race to hold the picks: the worst one that owns
		// none of them.
		const holder = ranked.at(-2)!;
		const all = await idb.cache.draftPicks.getAll();
		for (const tid of [favorite, middle, worst]) {
			const picks = all
				.filter((dp: any) => dp.originalTid === tid)
				.sort(
					(a: any, b: any) =>
						(a.season as number) - (b.season as number) || a.round - b.round,
				);
			for (const dp of picks as any[]) {
				const a = { players: [], picks: [dp] };
				const not = [dp.tid];
				const price = view.priceNow(a, not);
				const bids = keen
					.filter((t: number) => !not.includes(t))
					.map(
						(t: number) => `${abbrev.get(t)} ${view.bidNow(t, a).toFixed(3)}`,
					)
					.join(", ");
				const floor = view.pickFloorNow(dp);
				const use = dp.tid === holder ? Number.NaN : view.useOfNow(holder, a);
				log(
					`${dp.season} R${dp.round} ${abbrev.get(dp.originalTid)} (held by ${abbrev.get(dp.tid)}): price ${price.toFixed(3)}, floor ${floor.toFixed(3)}, use to ${abbrev.get(holder)} ${use.toFixed(3)} | bids ${bids}`,
				);
			}
		}
	},
);
