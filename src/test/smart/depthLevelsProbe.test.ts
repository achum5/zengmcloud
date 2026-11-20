import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// What is always there for the minimum: free agents' overalls, and the
// overall at each roster place averaged over teams.
//   DL_LEAGUE=<league.json | arena:<lid>> npx vitest --run --project \
//     basketball src/test/smart/depthLevelsProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.DL_LEAGUE)(
	"depth levels probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		await loadLeagueFile(env.DL_LEAGUE!);
		const { idb } = await import("../../worker/db/index.ts");
		const { g } = await import("../../worker/util/index.ts");
		const { PLAYER } = await import("../../common/constants.ts");
		const all = await idb.cache.players.getAll();
		const fa = all
			.filter((p) => p.tid === PLAYER.FREE_AGENT)
			.map((p) => p.ratings.at(-1)!.ovr)
			.sort((a, b) => b - a);
		const byTeam = new Map<number, number[]>();
		for (const p of all) {
			if (p.tid >= 0) {
				const list = byTeam.get(p.tid) ?? [];
				list.push(p.ratings.at(-1)!.ovr);
				byTeam.set(p.tid, list);
			}
		}
		const places: number[][] = [];
		for (const list of byTeam.values()) {
			list.sort((a, b) => b - a);
			list.forEach((x, i) => (places[i] ??= []).push(x));
		}
		(await importNode("node:fs")).appendFileSync(
			env.DL_OUT ?? "/dev/stdout",
			`${env.DL_LEAGUE} ${g.get("season")} ph${g.get("phase")}: free agents ${fa.length}, best ${fa.slice(0, 20).join(" ")}\nmean by roster place ${places.map((l) => (l.reduce((a, x) => a + x, 0) / l.length).toFixed(1)).join(" ")}\n`,
		);
	},
);
