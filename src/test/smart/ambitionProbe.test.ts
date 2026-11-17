import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Ambition on vanilla's rules (outlookSettings.vanillaAmbition): for every
// team, its title and playoff odds and what a star, a starter and a bench
// player from elsewhere are worth to it (use, % of a title) at each weight -
// who would buy in season (docs/smart-cpu.md).
//   AMB_LEAGUE=<league.json> AMB_OUT=<file> npx vitest --run \
//     --project basketball src/test/smart/ambitionProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.AMB_LEAGUE)(
	"ambition probe",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => lines.push(x);
		await loadLeagueFile(env.AMB_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			realisticFrontOffices: false,
			smartCpu: true,
			spectator: true,
		});
		const { buildOutlook, outlookSettings } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const season = g.get("season");
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const all = (await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		])) as any[];
		// A star, a starter and a bench player, each the closest to the mark.
		const near = (ovr: number, age: [number, number]) =>
			all
				.filter((p) => {
					const a = season - p.born.year;
					return a >= age[0] && a <= age[1];
				})
				.sort(
					(a, b) =>
						Math.abs(a.ratings.at(-1).ovr - ovr) -
							Math.abs(b.ratings.at(-1).ovr - ovr) || a.pid - b.pid,
				)[0];
		const sample = [near(70, [25, 29]), near(62, [25, 29]), near(55, [24, 30])];
		log(
			`sample: ${sample.map((p) => `${p.firstName} ${p.lastName} ${p.ratings.at(-1).ovr} ${season - p.born.year}y ${abbrev.get(p.tid)}`).join(" | ")}`,
		);
		const weights = (env.AMB_WEIGHTS ?? "0,0.1,0.25,0.5")
			.split(",")
			.map(Number);
		const rows = new Map<number, string[]>();
		const odds = new Map<number, string>();
		const titleOf = new Map<number, number>();
		for (const w of weights) {
			outlookSettings.vanillaAmbition = w;
			const outlook: any = await buildOutlook();
			const view: any = viewFor(outlook);
			await view.prime?.();
			for (const t of teams) {
				const o = outlook.teams.get(t.tid);
				if (w === weights[0]) {
					titleOf.set(t.tid, o.title[0]);
					odds.set(
						t.tid,
						`title ${(100 * o.title[0]).toFixed(1).padStart(5)}% playoffs ${(100 * o.playoffs[0]).toFixed(0).padStart(3)}%`,
					);
				}
				const uses: string[] = [];
				for (const p of sample) {
					if (p.tid === t.tid) {
						uses.push("   -  ");
						continue;
					}
					const a = { players: [p], picks: [] };
					uses.push((100 * (await view.useOf(t.tid, a))).toFixed(2).padStart(6));
				}
				const row = rows.get(t.tid) ?? [];
				row.push(uses.join(" "));
				rows.set(t.tid, row);
			}
		}
		log(`weights: ${weights.join(" | ")}   (use of star / starter / bench, % of a title)`);
		const order = [...teams].sort(
			(a, b) => titleOf.get(b.tid)! - titleOf.get(a.tid)!,
		);
		for (const t of order) {
			log(
				`${t.abbrev.padEnd(4)} ${odds.get(t.tid)} | ${rows.get(t.tid)!.join(" | ")}`,
			);
		}
		fs.writeFileSync(env.AMB_OUT ?? "ambitionProbe.txt", `${lines.join("\n")}\n`);
	},
);
