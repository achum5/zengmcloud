import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Re-signing on vanilla's rules: what the smart CPU expects (its outlook's
// chance of keeping each player whose deal runs out this season) against
// what vanilla does (the player's chance of being willing, from his mood,
// with vanilla's -3 for CPU teams). Every CPU team's expiring players.
//   RET_LEAGUE=<league.json> RET_OUT=<file> npx vitest --run \
//     --project basketball src/test/smart/retentionProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.RET_LEAGUE)(
	"retention probe",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => lines.push(x);
		await loadLeagueFile(env.RET_LEAGUE!);
		const { league, player } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			realisticFrontOffices: false,
			smartCpu: true,
			spectator: false,
		});
		const { buildOutlook, ensureRetention, retentionKey } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const season = g.get("season");
		const userTids = g.get("userTids") as number[];
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const outlook: any = await buildOutlook();
		log(
			`season ${season} phase ${g.get("phase")}; columns: team player age ovr/pot value rookie | GM's chance of keeping him | vanilla's chance he is willing`,
		);
		const buckets = new Map<string, { n: number; gm: number; van: number }>();
		for (const t of teams) {
			const roster = await idb.cache.players.indexGetAll("playersByTid", t.tid);
			const expiring = roster.filter((p) => contractOf(p).exp <= season);
			await ensureRetention(outlook, expiring, t.tid);
			for (const p of expiring) {
				const gm = outlook.retention.get(retentionKey(p.pid, t.tid)) as number;
				const mood = await player.moodInfo(p, t.tid);
				const r = p.ratings.at(-1)!;
				const rookie = !!contractOf(p).rookie;
				const user = userTids.includes(t.tid);
				log(
					`${abbrev.get(t.tid)}${user ? "*" : ""} ${p.firstName} ${p.lastName} ${season - p.born.year}y ${r.ovr}/${r.pot} v${Math.round(p.value)} ${rookie ? "rookie" : "vet"} | GM ${(100 * gm).toFixed(0)}% | vanilla willing ${(100 * mood.probWilling).toFixed(0)}% (${mood.willing ? "yes" : "no"})`,
				);
				if (user || r.ovr < 50) {
					continue;
				}
				const key = `${rookie ? "rookie" : "vet"} ${r.ovr >= 60 ? "60+" : r.ovr >= 55 ? "55-59" : "50-54"}`;
				const b = buckets.get(key) ?? { n: 0, gm: 0, van: 0 };
				b.n += 1;
				b.gm += gm;
				b.van += mood.probWilling;
				buckets.set(key, b);
			}
		}
		log("");
		log(
			"CPU teams, players rated 50+: bucket | n | GM's mean chance of keeping | vanilla's mean chance willing",
		);
		for (const [k, b] of [...buckets].sort()) {
			log(
				`${k} | ${b.n} | ${((100 * b.gm) / b.n).toFixed(0)}% | ${((100 * b.van) / b.n).toFixed(0)}%`,
			);
		}
		fs.writeFileSync(env.RET_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
	},
);
