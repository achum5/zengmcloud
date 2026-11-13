import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// SMART_LEAGUE=<league.json> SMART_PATH="CLE:Jaxson Hayes|Mark Williams|..." :
// a team's strength season by season with each player (or package, "+"
// between names) added, and what that is worth - to see where a player's
// price comes from.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.SMART_LEAGUE || !env.SMART_PATH)(
	"strength paths",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const {
			buildOutlook,
			ensureRetention,
			franchiseValue,
			teamStrengths,
			retentionOf,
		} =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		await loadLeagueFile(env.SMART_LEAGUE!);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const outlook = await buildOutlook({ iterations: 1000 });
		const out: string[] = [];
		const all = await idb.cache.players.getAll();
		const byName = (n: string) =>
			all.find((p) => `${p.firstName} ${p.lastName}` === n)!;
		for (const spec of env.SMART_PATH!.split(";")) {
			const [abbrev, list] = spec.split(":");
			const t = (await idb.cache.teams.getAll()).find(
				(x) => x.abbrev === abbrev,
			)!;
			const players = await idb.cache.players.indexGetAll(
				"playersByTid",
				t.tid,
			);
			const picks = await idb.cache.draftPicks.indexGetAll(
				"draftPicksByTid",
				t.tid,
			);
			const base = teamStrengths(outlook, t.tid, players, picks);
			const v0 = franchiseValue(outlook, base, t.tid);
			out.push(
				`${abbrev} base: value ${(v0 * 1000).toFixed(1)} | ${base.map((x) => x.toFixed(2)).join(" ")}`,
			);
			for (const item of list!.split("|")) {
				const add = item.split("+").map(byName);
				await ensureRetention(outlook, add, t.tid);
				const s = teamStrengths(
					outlook,
					t.tid,
					[...players.filter((p) => !add.includes(p)), ...add],
					picks,
				);
				const v = franchiseValue(outlook, s, t.tid);
				const ret = add
					.map(
						(p) =>
							`${p.lastName} keeps ${(retentionOf(outlook, p.pid, t.tid) ?? NaN).toFixed?.(2)}`,
					)
					.join(", ");
				out.push(
					`  + ${item}: value +${((v - v0) * 1000).toFixed(1)} | gain by season ${s.map((x, i) => (x - base[i]!).toFixed(2)).join(" ")} | ${ret}`,
				);
			}
		}
		await fs.writeFile(
			`${env.SMART_OUT ?? "."}/path-probe.txt`,
			out.join("\n"),
		);
	},
);
