import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// ONE MARKET NOW, AS THE DANGER ZONE RUNS IT: an owner's league as it stands
// (those shopped last moment without a taker still behind the rest, unless
// DN_FRESH=1), one moment through smartTradeOnDemand, with where every
// player shopped got to, the trades made, the near misses and the sellers
// who came closest to yes.
//   DN_LEAGUE=<league.json> DN_OUT=<file> [DN_FRESH=1] npx vitest --run \
//     --project basketball src/test/smart/deadlineNowProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.DN_LEAGUE)(
	"one market now, as the danger zone runs it",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.DN_OUT!, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.DN_LEAGUE!);
		const { g } = await import("../../worker/util/index.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const { league } = await import("../../worker/core/index.ts");
		const { smartHumans } =
			await import("../../worker/core/smartCpu/signing.ts");
		const market = await import("../../worker/core/smartCpu/market.ts");
		smartHumans.tids = [...g.get("userTids")];
		if (env.DN_FRESH) {
			await league.setGameAttributes({ smartMarketRested: [] });
		}
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const season = g.get("season");
		const name = (p: any) =>
			`${p.firstName} ${p.lastName} (${season - p.born.year}, ${p.ratings.at(-1).ovr})`;
		log(
			`${season} phase ${g.get("phase")}, minGain ${market.marketSettings.minGain}, on demand ${market.smartOnDemandPhase()}, rested ${(g.get("smartMarketRested") ?? []).length}`,
		);
		const stages = new Map<string, number>();
		const near: string[] = [];
		const sayNo: { gain: number; line: string }[] = [];
		market.smartTraceWatchers.push((t) => {
			stages.set(t.stage, (stages.get(t.stage) ?? 0) + 1);
			const line = `${abbrev.get(t.buyer!)} for ${name(t.p)} of ${abbrev.get(t.seller)}, seller ${t.sellerGain?.toFixed(4)}, buyer ${t.buyerGain?.toFixed(4)}${t.why ? ` (${t.why})` : ""}`;
			if (t.stage === "seller says no" && t.sellerGain !== undefined) {
				sayNo.push({ gain: t.sellerGain, line });
			} else if (
				t.stage === "within margin" ||
				t.stage === "unfair" ||
				t.stage === "waits" ||
				t.stage === "changed" ||
				t.stage === "illegal" ||
				t.stage === "dealt already"
			) {
				near.push(`${t.stage}: ${line}`);
			}
		});
		const t0 = performance.now();
		const made = await market.smartTradeOnDemand();
		log(
			`made ${made.length} in ${((performance.now() - t0) / 1000).toFixed(0)} s; by where each got to: ${[
				...stages,
			]
				.sort((a, b) => b[1] - a[1])
				.map(([k, v]) => `${k} ${v}`)
				.join(", ")}`,
		);
		log(`\nnear misses (${near.length}):`);
		for (const x of near) {
			log(`  ${x}`);
		}
		log(`\nsellers closest to yes:`);
		for (const x of sayNo.sort((a, b) => b.gain - a.gain).slice(0, 15)) {
			log(`  ${x.line}`);
		}
	},
);
