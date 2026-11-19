import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Every deal each market moment finds, before any is made, as a league plays
// on (MD_DAYS): each trimmed as the market would make it, with what seller
// and buyer would gain - to see which a buyer had to choose from.
//   MD_LEAGUE=<league.json> MD_OUT=<file> [MD_DAYS=10] npx vitest --run \
//     --project basketball src/test/smart/momentDealsProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.MD_LEAGUE)(
	"moment deals probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		await loadLeagueFile(env.MD_LEAGUE!);
		const { league, game } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { smartDealWatchers, smartMomentWatchers, trimDeal } =
			await import("../../worker/core/smartCpu/market.ts");
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const lines: string[] = [];
		const name = (x: any) =>
			"pid" in x
				? `${x.firstName} ${x.lastName} ${g.get("season") - x.born.year}y ${x.ratings.at(-1).ovr}/${x.ratings.at(-1).pot}`
				: `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
		let moment = 0;
		smartMomentWatchers.push(async (deals: any, view: any) => {
			moment += 1;
			lines.push(
				`moment ${moment} (${g.get("season")} day ${g.get("daysLeft") ?? ""}): ${deals.length} deals`,
			);
			for (const d of deals) {
				const offer = await trimDeal(view, d);
				const sellerGain = await view.dealWorth(
					d.seller,
					d.buyer,
					d.out,
					offer,
				);
				const buyerGain = await view.dealWorth(d.buyer, d.seller, offer, d.out);
				lines.push(
					`  ${abbrev.get(d.buyer)} gets ${d.out.players.map(name).join("; ")} from ${abbrev.get(d.seller)} for [${[...offer.players, ...offer.picks].map(name).join("; ")}]: seller ${sellerGain.toFixed(4)}, buyer ${buyerGain.toFixed(4)}`,
				);
			}
			fs.writeFileSync(env.MD_OUT!, lines.join("\n") + "\n");
		});
		// And the deals made, as they are.
		smartDealWatchers.push((d: any) => {
			lines.push(
				`  MADE: ${abbrev.get(d.buyer)} gets ${d.out.players.map(name).join("; ")} from ${abbrev.get(d.seller)} for [${[...d.offer.players, ...d.offer.picks].map(name).join("; ")}]: seller ${d.sellerGain.toFixed(4)}, buyer ${d.buyerGain.toFixed(4)}, bar ${d.bar.toFixed(4)}`,
			);
			fs.writeFileSync(env.MD_OUT!, lines.join("\n") + "\n");
		});
		await game.play(Number(env.MD_DAYS ?? 10), {});
		fs.writeFileSync(env.MD_OUT!, lines.join("\n") + "\n");
	},
);
