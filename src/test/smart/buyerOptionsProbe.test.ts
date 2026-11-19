import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// What a buyer's package would buy: the same players and picks it paid for
// one player, offered to every CPU team for each of its players - the
// legal ones the seller would take, ranked by what they are worth to the
// buyer. Was the player it got the best that package could buy?
//   BO_LEAGUE=<league.json> BO_OUT=<file> BO_BUYER=<tid> BO_PIDS=<pids> \
//     BO_DPIDS=<dpids> npx vitest --run --project basketball \
//     src/test/smart/buyerOptionsProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);
const ids = (x: string | undefined) =>
	(x ?? "")
		.split(",")
		.filter((s) => s !== "")
		.map(Number);

test.skipIf(!env.BO_LEAGUE)(
	"buyer options probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		await loadLeagueFile(env.BO_LEAGUE!);
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
		const { default: summary } =
			await import("../../worker/core/trade/summary.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		await view.classPrices();
		const buyer = Number(env.BO_BUYER);
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const all = await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		]);
		const picks = await idb.cache.draftPicks.getAll();
		const pay = {
			players: all.filter((p) => ids(env.BO_PIDS).includes(p.pid)),
			picks: picks.filter((dp) => ids(env.BO_DPIDS).includes(dp.dpid)),
		};
		const season = g.get("season");
		const rows: string[] = [];
		const found: { worth: number; line: string }[] = [];
		for (const p of all) {
			if (p.tid === buyer || p.tid < 0) {
				continue;
			}
			const seller = p.tid;
			const get = { players: [p], picks: [] };
			const legal = await summary([
				{
					tid: buyer,
					pids: pay.players.map((x) => x.pid),
					dpids: pay.picks.map((x) => x.dpid),
					pidsExcluded: [],
					dpidsExcluded: [],
				},
				{
					tid: seller,
					pids: [p.pid],
					dpids: [],
					pidsExcluded: [],
					dpidsExcluded: [],
				},
			]);
			if (legal.warning) {
				continue;
			}
			const sellerWorth = await view.dealWorth(seller, buyer, get, pay);
			if (!(sellerWorth > 0.001)) {
				continue;
			}
			const buyerWorth = await view.dealWorth(buyer, seller, pay, get);
			const r = p.ratings.at(-1)!;
			found.push({
				worth: buyerWorth,
				line: `${p.firstName} ${p.lastName} (${abbrev.get(seller)}) ${season - p.born.year}y ${r.pos} ${r.ovr}/${r.pot}: to the buyer ${buyerWorth.toFixed(4)}, to the seller ${sellerWorth.toFixed(4)}`,
			});
		}
		found.sort((a, b) => b.worth - a.worth);
		rows.push(
			`${abbrev.get(buyer)} pays [${[...pay.players.map((p) => `${p.firstName} ${p.lastName}`), ...pay.picks.map((dp) => `${dp.season} R${dp.round} ${abbrev.get(dp.originalTid)}`)].join("; ")}]: ${found.length} players it could buy with it`,
		);
		for (const f of found.slice(0, 25)) {
			rows.push(`  ${f.line}`);
		}
		fs.writeFileSync(env.BO_OUT!, rows.join("\n") + "\n");
	},
);
