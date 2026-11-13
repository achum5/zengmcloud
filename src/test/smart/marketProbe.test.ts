import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import isUntradable from "../../worker/core/trade/isUntradable.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";

// LAYER 1 probe: which deals does the market leave? For each team's best
// tradable players, the best real offer from the keenest CPU teams, and
// whether the seller takes it (a deal both sides want). And how far the
// price from the six keenest teams lands from the price from all teams.
//
//   MARKET_PROBE=1 MARKET_OUT=<dir> npx vitest --run --project basketball \
//     src/test/smart/marketProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.MARKET_PROBE === "1")(
	"market probe",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { buildOutlook } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { SmartView } = await import("../../worker/core/smartCpu/worth.ts");
		Math.random = makeSeededRandom(Number(env.MARKET_SEED ?? 77));
		(idb as any).league = undefined;
		await createArenaLeague(Number(env.MARKET_LEAGUE ?? 9177));
		await advanceSeasons(1);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const view = new SmartView(await buildOutlook({ iterations: 1000 }), []);
		const tids = (await idb.cache.teams.getAll())
			.filter((t) => !t.disabled)
			.map((t) => t.tid);
		const per = Number(env.MARKET_PER ?? 8);
		const label = (p: Player) =>
			`${p.firstName} ${p.lastName} (${g.get("season") - p.born.year}y ${p.ratings.at(-1)!.ovr}/${p.ratings.at(-1)!.pot}, $${Math.round(p.contracts.at(-1)!.amounts[0]! / 100) / 10}M x${p.contracts.at(-1)!.amounts.length})`;

		const values: Record<number, number> = {};
		for (const tid of tids) {
			values[tid] = view.value(tid, await view.holdingsOf(tid));
		}
		const keenest = await view.keenest();
		const rows = [];
		const t0 = performance.now();
		for (const tid of tids) {
			const mine = (await idb.cache.players.indexGetAll("playersByTid", tid))
				.filter((p) => !isUntradable(p).untradable)
				.sort((a, b) => b.value - a.value)
				.slice(0, per);
			for (const x of mine) {
				const out = { players: [x], picks: [] };
				const t1 = performance.now();
				const own = await view.useOf(tid, out);
				const price = await view.price(out, [tid]);
				const uses = [];
				for (const t of tids) {
					if (t !== tid) {
						uses.push({ tid: t, use: await view.useOf(t, out) });
					}
				}
				uses.sort((a, b) => b.use - a.use);
				const real = await view.bestRealOffer(tid, out, -1);
				rows.push({
					tid,
					player: label(x),
					own,
					price,
					allPrice: uses[1]?.use ?? 0,
					topBidders: uses.slice(0, 3),
					real: real
						? {
								buyer: real.buyer,
								worth: real.worth,
								offer: real.offer.players.map(label),
							}
						: undefined,
					seconds: (performance.now() - t1) / 1000,
				});
			}
		}
		await fs.writeFile(
			`${env.MARKET_OUT ?? "."}/market-probe.json`,
			JSON.stringify(
				{
					values,
					keenest,
					rows,
					seconds: (performance.now() - t0) / 1000,
				},
				null,
				1,
			),
		);
	},
);
