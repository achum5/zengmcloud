import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// WHAT PLAYING THE PLAYOFFS AS THE SIM DOES CHANGES IN THE GMs' ODDS: the
// outlook built with outlookSettings.playoffScale at 1 and at each of
// PS_SCALES (a point of strength worth that much more in a playoff game,
// src/test/impact/playoffLab.test.ts), and for the league's best teams this
// season's title and playoff-run chances and a season's value - and what one
// point of strength more is worth to each, this season and next.
//   PS_LEAGUE=<league.json | arena:<lid>> PS_OUT=<file> [PS_SCALES=1.58]
//   [PS_TOP=10] npx vitest --run --project basketball
//   src/test/smart/playoffScaleProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PS_LEAGUE)(
	"playoff scale probe",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.PS_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.PS_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { g } = await import("../../worker/util/index.ts");
		const o = await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev] as const));
		const scales = [1, ...(env.PS_SCALES ?? "1.58").split(",").map(Number)];
		log(`${g.get("season")} phase ${g.get("phase")}`);
		let order: number[] | undefined;
		for (const scale of scales) {
			o.outlookSettings.playoffScale = scale;
			const outlook = await o.buildOutlook({ iterations: 1000 });
			order ??= [...outlook.teams.values()]
				.sort((a, b) => b.title[0]! - a.title[0]!)
				.slice(0, Number(env.PS_TOP ?? 10))
				.map((t) => t.tid);
			log(`\nplayoff scale ${scale}`);
			log(
				"team | strength | title | season value | +1 point now | +1 point next season",
			);
			let total = 0;
			for (const t of outlook.teams.values()) {
				total += t.title[0]!;
			}
			for (const tid of order) {
				const t = outlook.teams.get(tid)!;
				const at = (y: number, s: number) => o.seasonValue(outlook, y, s, tid);
				log(
					`${abbrev.get(tid)!.padEnd(4)} | ${t.strength[0]!.toFixed(2).padStart(6)} | ${(100 * t.title[0]!).toFixed(1).padStart(5)}% | ${at(0, t.strength[0]!).toFixed(4)} | ${(at(0, t.strength[0]! + 1) - at(0, t.strength[0]!)).toFixed(4)} | ${(at(1, t.strength[1]! + 1) - at(1, t.strength[1]!)).toFixed(4)}`,
				);
			}
			log(`  titles add up to ${(100 * total).toFixed(1)}%`);
		}
	},
);
