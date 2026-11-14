import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";

// One market moment, timed step by step, and the deals it finds - written out
// whole, so a change meant only for speed can be checked to find the very
// same deals (layer 3).
//   MOMENT_PROBE=1 MOMENT_OUT=<dir> npx vitest --run --project basketball \
//     src/test/smart/momentProbe.test.ts
// MOMENT_PROFILE=1 also writes a CPU profile of the moment.
// MOMENT_SETTINGS='{...}' changes the GM's settings (smartSettings),
// MOMENT_MARKET='{...}' the market's (marketSettings).
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.MOMENT_PROBE === "1")(
	"moment probe",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const out = env.MOMENT_OUT ?? ".";
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const { candidates, dealsNow, marketSettings, marketStats } =
			await import("../../worker/core/smartCpu/market.ts");
		const { smartClock, valueCount } =
			await import("../../worker/core/smartCpu/clock.ts");
		const { offerStats, smartSettings } =
			await import("../../worker/core/smartCpu/worth.ts");
		// MOMENT_SETTINGS='{"decideOnce":false}': the GM's settings changed.
		Object.assign(smartSettings, JSON.parse(env.MOMENT_SETTINGS ?? "{}"));
		// MOMENT_MARKET='{"shopped":5}': the market's.
		Object.assign(marketSettings, JSON.parse(env.MOMENT_MARKET ?? "{}"));
		Math.random = makeSeededRandom(Number(env.MOMENT_SEED ?? 77));
		(idb as any).league = undefined;
		await createArenaLeague(Number(env.MOMENT_LEAGUE ?? 9177));
		await advanceSeasons(1);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		g.setWithoutSavingToDB("smartCpu", true);
		const view = viewFor(await currentOutlook());
		const cpu = new Set(
			(await idb.cache.teams.getAll())
				.filter((t) => !t.disabled)
				.map((t) => t.tid),
		);
		let session: any;
		if (env.MOMENT_PROFILE === "1") {
			const inspector = await importNode("node:inspector/promises");
			session = new inspector.Session();
			session.connect();
			await session.post("Profiler.enable");
			await session.post("Profiler.start");
		}
		const t0 = performance.now();
		const values0 = valueCount.n;
		const deals = await dealsNow(view, cpu);
		// The candidates, as the market ranked them, for which made deals.
		const cands = await candidates(view, cpu, new Set());
		const seconds = (performance.now() - t0) / 1000;
		if (session) {
			const { profile } = await session.post("Profiler.stop");
			await fs.writeFile(`${out}/moment.cpuprofile`, JSON.stringify(profile));
			session.disconnect();
		}
		await fs.writeFile(
			`${out}/moment.json`,
			JSON.stringify(
				{
					seconds,
					values: valueCount.n - values0,
					clock: smartClock,
					stats: marketStats,
					offers: offerStats,
					candidates: cands.map((c) => ({
						seller: c.seller,
						buyer: c.buyer,
						pid: c.p.pid,
						surplus: c.surplus,
					})),
					deals: deals.map((d) => ({
						seller: d.seller,
						buyer: d.buyer,
						out: d.out.players.map((p) => p.pid),
						offer: [
							...d.offer.players.map((p) => p.pid),
							...d.offer.picks.map((dp) => `dp${dp.dpid}`),
						],
						sellerGain: d.sellerGain,
					})),
				},
				null,
				1,
			),
		);
	},
);
