import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// What marginal players are worth to the league's best teams: a sample of
// players of every rating and age on other teams, each one's use to a
// contender (and where it comes from, season by season, for the dearest)
// beside his market price (docs/smart-cpu.md).
//   MARG_LEAGUE=<league.json> MARG_OUT=<file> npx vitest --run \
//     --project basketball src/test/smart/marginalProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.MARG_LEAGUE)(
	"marginal probe",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => lines.push(x);
		await loadLeagueFile(env.MARG_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			realisticFrontOffices: true,
			smartCpu: true,
			spectator: true,
		});
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const outlookMod =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		const keen: number[] = await view.keenest();
		const season = g.get("season");
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const all = (await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		])) as any[];
		// A spread of marginal players: by overall band and age.
		const bands = [
			[30, 40],
			[40, 45],
			[45, 50],
			[50, 55],
			[55, 60],
		];
		const sample: any[] = [];
		for (const [lo, hi] of bands) {
			for (const young of [true, false]) {
				const pool = all
					.filter((p) => {
						const r = p.ratings.at(-1);
						const age = season - p.born.year;
						return (
							r.ovr >= lo! && r.ovr < hi! && (young ? age <= 21 : age >= 26)
						);
					})
					.sort((a, b) => a.pid - b.pid);
				sample.push(...pool.slice(0, 4));
			}
		}
		for (const tid of keen.slice(0, Number(env.MARG_TEAMS ?? 3))) {
			const h = await view.holdingsOf(tid);
			log(
				`\n${abbrev.get(tid)} (title odds ${outlook.teams
					.get(tid)
					.title.slice(0, 4)
					.map((x: number) => (100 * x).toFixed(1))
					.join(" / ")}; ${h.players.length} players)`,
			);
			const rows: any[] = [];
			for (const p of sample) {
				if (p.tid === tid) {
					continue;
				}
				const a = { players: [p], picks: [] };
				await view.useOf(tid, a);
				await view.price(a, [p.tid]);
				const use = view.useOfNow(tid, a);
				const price = view.priceNow(a, [p.tid]);
				const r = p.ratings.at(-1);
				const proj = outlookMod.projectPlayer(p, season);
				rows.push({ p, use, price, r, proj });
			}
			rows.sort((a, b) => b.use - a.use);
			for (const { p, use, price, r, proj } of rows) {
				const c = contractOf(p);
				log(
					`  ${`${p.firstName} ${p.lastName}`.padEnd(24)} ${season - p.born.year}y ${r.ovr}/${r.pot} ${abbrev.get(p.tid)} $${(c.amount / 1000).toFixed(1)}M->${c.exp} proj ${proj.ovr
						.slice(0, 6)
						.map((x: number) => x.toFixed(0))
						.join(
							" ",
						)} | use ${(100 * use).toFixed(2)} price ${(100 * price).toFixed(2)}`,
				);
			}
			// Where the dearest uses come from.
			for (const { p } of rows.slice(0, 3)) {
				const withHim = { players: [...h.players, p], picks: h.picks };
				const sW = outlookMod.teamStrengths(
					outlook,
					tid,
					withHim.players,
					withHim.picks,
				);
				const sO = outlookMod.teamStrengths(outlook, tid, h.players, h.picks);
				log(
					`  ${p.firstName} ${p.lastName}: y | strength with / without | season value with / without`,
				);
				for (let y = 0; y < sW.length; y++) {
					log(
						`    ${y} | ${sW[y]!.toFixed(2)} / ${sO[y]!.toFixed(2)} | ${(100 * outlookMod.seasonValue(outlook, y, sW[y]!, tid)).toFixed(2)} / ${(100 * outlookMod.seasonValue(outlook, y, sO[y]!, tid)).toFixed(2)}`,
					);
				}
			}
		}
		fs.writeFileSync(
			env.MARG_OUT ?? "marginalProbe.txt",
			`${lines.join("\n")}\n`,
		);
	},
);
