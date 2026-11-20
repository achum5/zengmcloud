import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// Every rostered player's mood toward his team and toward two others, as
// moodInfo has it - written out to compare one build's against another's.
//   MS_LEAGUE=<league.json> MS_OUT=<file> npx vitest --run --project \
//     basketball src/test/smart/moodSameProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.MS_LEAGUE)(
	"mood same probe",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		await loadLeagueFile(env.MS_LEAGUE!);
		const { league, player } = await import("../../worker/core/index.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const rows: string[] = [];
		for (const spectator of [true, false]) {
			await league.setGameAttributes({
				spectator,
				smartCpu: true,
				realisticFrontOffices: false,
			});
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			for (const p of players) {
				for (const tid of [p.tid, 0, 7]) {
					const m: any = await player.moodInfo(p, tid);
					rows.push(
						`${spectator} ${p.pid} ${tid} ${JSON.stringify(m.components)} ${m.probWilling.toFixed(9)} ${m.willing}`,
					);
				}
			}
		}
		fs.writeFileSync(env.MS_OUT!, rows.join("\n"));
	},
);
