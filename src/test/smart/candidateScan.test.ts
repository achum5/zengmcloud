import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";

// Every candidate sale of a market moment put to the best real offer: which
// ones make deals, to rank candidates by what predicts one.
//   CANDIDATE_SCAN=1 SCAN_OUT=<dir> npx vitest --run --project basketball \
//     src/test/smart/candidateScan.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.CANDIDATE_SCAN === "1")(
	"candidate scan",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const { ensureRetention } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const { candidates } = await import("../../worker/core/smartCpu/market.ts");
		Math.random = makeSeededRandom(77);
		(idb as any).league = undefined;
		await createArenaLeague(9177);
		await advanceSeasons(1);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		g.setWithoutSavingToDB("smartCpu", true);
		const view = viewFor(await currentOutlook());
		await view.prime();
		const cpu = new Set(
			(await idb.cache.teams.getAll())
				.filter((t) => !t.disabled)
				.map((t) => t.tid),
		);
		const keen = await view.keenest();
		for (const tid of cpu) {
			for (const k of [tid, ...keen]) {
				await ensureRetention(view.outlook, view.heldNow(tid).players, k);
			}
		}
		const tc = performance.now();
		const cands = await candidates(view, cpu, new Set());
		const candidateSeconds = (performance.now() - tc) / 1000;
		const rows = [];
		// SCAN_PROFILE=1: a CPU profile of the first 60 real offers.
		let session: any;
		if (env.SCAN_PROFILE === "1") {
			const inspector = await importNode("node:inspector/promises");
			session = new inspector.Session();
			session.connect();
			await session.post("Profiler.enable");
			await session.post("Profiler.setSamplingInterval", { interval: 200 });
			await session.post("Profiler.start");
		}
		const t0 = performance.now();
		for (const [rank, c] of cands.entries()) {
			if (session && rank === 60) {
				const { profile } = await session.post("Profiler.stop");
				await fs.writeFile(
					`${env.SCAN_OUT ?? "."}/market.cpuprofile`,
					JSON.stringify(profile),
				);
				session.disconnect();
				session = undefined;
				break;
			}
			const out = { players: [c.p] as Player[], picks: [] };
			const real = await view.bestRealOffer(c.seller, out, -1);
			rows.push({
				rank,
				seller: c.seller,
				buyer: c.buyer,
				pid: c.p.pid,
				age: g.get("season") - c.p.born.year,
				ovr: c.p.ratings.at(-1)!.ovr,
				pot: c.p.ratings.at(-1)!.pot,
				salary: contractOf(c.p).amount,
				surplus: c.surplus,
				own: view.useOfNow(c.seller, out),
				price: view.priceNow(out, [c.seller]),
				real: real
					? {
							buyer: real.buyer,
							worth: real.worth,
							offer: real.offer.players.map((p) => ({
								pid: p.pid,
								ovr: p.ratings.at(-1)!.ovr,
								age: g.get("season") - p.born.year,
								salary: contractOf(p).amount,
							})),
						}
					: null,
			});
		}
		await fs.writeFile(
			`${env.SCAN_OUT ?? "."}/candidate-scan.json`,
			JSON.stringify({
				candidateSeconds,
				seconds: (performance.now() - t0) / 1000,
				rows,
			}),
		);
	},
);
