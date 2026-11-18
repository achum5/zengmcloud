import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// What the GM's outlook gives each team's title chances, season by season
// over its horizon - to check bad teams' futures against how often bad
// teams in finished leagues went on to win.
//   HZ_LEAGUE=<league.json> HZ_OUT=<file> npx vitest --run \
//     --project basketball src/test/smart/horizonProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.HZ_LEAGUE)(
	"horizon probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.HZ_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.HZ_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const outlook: any = await currentOutlook();
		const season = g.get("season");
		const teams = await idb.cache.teams.getAll();
		const last = new Map<number, number>();
		for (const ts of await idb.cache.teamSeasons.getAll()) {
			if (ts.season === season - 1) {
				last.set(ts.tid, ts.won / Math.max(1, ts.won + ts.lost));
			}
		}
		const rows = teams
			.filter((t) => !t.disabled)
			.map((t) => ({
				t,
				winp: last.get(t.tid) ?? 0.5,
				title: outlook.teams.get(t.tid).title as number[],
				strength: outlook.teams.get(t.tid).strength as number[] | undefined,
			}))
			.sort((a, b) => a.winp - b.winp);
		log(`${season}: title chances by season ahead (k=0 is this season)`);
		for (const r of rows) {
			log(
				`${r.t.abbrev} ${r.winp.toFixed(3)}: ${r.title.map((x) => (100 * x).toFixed(1)).join(" ")}${r.strength ? ` | strength ${r.strength.map((x) => x.toFixed(1)).join(" ")}` : ""}`,
			);
		}
		const third = Math.floor(rows.length / 3);
		for (const [label, group] of [
			["bad third", rows.slice(0, third)],
			["middle third", rows.slice(third, 2 * third)],
			["top third", rows.slice(2 * third)],
		] as const) {
			const n = group[0]!.title.length;
			const avg = Array.from(
				{ length: n },
				(_, k) =>
					group.reduce((s, r) => s + (r.title[k] ?? 0), 0) / group.length,
			);
			log(
				`${label} average: ${avg.map((x) => (100 * x).toFixed(2)).join(" ")}`,
			);
		}
	},
);
