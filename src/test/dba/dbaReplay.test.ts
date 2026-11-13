import { test } from "vitest";
import { runDbaReplay } from "./replay.ts";
import { valuationNamed } from "./valuations.ts";

// DBA_LEAGUE=<league.json>: the owner's league export, replayed trade by trade
// (docs/smart-cpu.md, "The DBA replay"). Optional:
//   DBA_DB=<league-slim.json>   what to load into the test league (faster)
//   DBA_FROM / DBA_TO           seasons (default 2013 to the current one)
//   DBA_VALUATIONS=vanilla,realistic (also vanilla@<difficultyTrade>)
//   DBA_CLEAR=0.5               points a game for "clearly" better or worse
//   DBA_CLASSES=3               draft classes in the league at once
//   DBA_OUT=<dir>               writes dba-replay.txt and dba-replay.json
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.DBA_LEAGUE)(
	"DBA replay: the owner's trades put to each valuation",
	{ timeout: 14_400_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		if (env.DBA_OUT) {
			fs.mkdirSync(env.DBA_OUT, { recursive: true });
			fs.writeFileSync(`${env.DBA_OUT}/progress.txt`, "");
		}
		const names = (env.DBA_VALUATIONS ?? "vanilla,realistic").split(",");
		await runDbaReplay({
			league: env.DBA_LEAGUE!,
			db: env.DBA_DB,
			from: Number(env.DBA_FROM ?? 2013),
			to: Number(env.DBA_TO ?? 9999),
			valuations: names.map(valuationNamed),
			clear: env.DBA_CLEAR ? Number(env.DBA_CLEAR) : undefined,
			prospectClasses: env.DBA_CLASSES ? Number(env.DBA_CLASSES) : undefined,
			out: env.DBA_OUT,
			log: (line) => {
				console.log(line);
				if (env.DBA_OUT) {
					fs.appendFileSync(`${env.DBA_OUT}/progress.txt`, `${line}\n`);
				}
			},
		});
	},
);
