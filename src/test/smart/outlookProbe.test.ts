import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// SMART_LEAGUE=<league.json>: the outlook (try 1's eyes) on a real league:
// every team's strength and title chances, season by season, and how long it
// takes to build.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.SMART_LEAGUE)(
	"outlook on a real league",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { buildOutlook, franchiseValue, OUTLOOK_YEARS } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		await loadLeagueFile(env.SMART_LEAGUE!);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const lines: string[] = [];
		const t0 = performance.now();
		const outlook = await buildOutlook({
			iterations: Number(env.SMART_ITER ?? 1000),
		});
		lines.push(
			`season ${g.get("season")} phase ${g.get("phase")}: outlook built in ${((performance.now() - t0) / 1000).toFixed(1)} s, ${OUTLOOK_YEARS + 1} seasons`,
		);
		const abbrev = new Map(
			(await idb.cache.teams.getAll()).map((t) => [t.tid, t.abbrev]),
		);
		const rows = [...outlook.teams.values()].map((o) => ({
			abbrev: abbrev.get(o.tid)!,
			value: franchiseValue(outlook, o.strength, o.tid),
			o,
		}));
		rows.sort((a, b) => b.value - a.value);
		lines.push(
			"team  value  | title chance by season (now, +1, ... ) | strength by season",
		);
		for (const r of rows) {
			lines.push(
				`${r.abbrev.padEnd(4)} ${r.value.toFixed(3)} | ${r.o.title
					.slice(0, 6)
					.map((x) => (x * 100).toFixed(1).padStart(5))
					.join(" ")} | ${r.o.strength
					.slice(0, 6)
					.map((x) => x.toFixed(1).padStart(5))
					.join(" ")}`,
			);
		}
		await fs.writeFile(
			`${env.SMART_OUT ?? "."}/outlook-probe.txt`,
			lines.join("\n"),
		);
	},
);
