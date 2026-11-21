import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// HOW LONG A HUMAN WAITS: the CPU's answer to a human's trade offer
// (smartJudgeOffer), timed - each of the CPU's best few players asked for,
// for the human's two least valuable.
//   JT_LEAGUE=<league.json> JT_OUT=<file> \
//     npx vitest --run --project basketball src/test/smart/judgeTimeProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.JT_LEAGUE)(
	"judge time probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.JT_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.JT_LEAGUE!);
		const { g } = await import("../../worker/util/index.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const { smartJudgeOffer } =
			await import("../../worker/core/smartCpu/humanTrades.ts");
		const human = g.get("userTid");
		const humanRoster = (
			await idb.cache.players.indexGetAll("playersByTid", human)
		).sort((a, b) => a.value - b.value);
		const teams = await idb.cache.teams.getAll();
		const times: number[] = [];
		for (const t of teams.filter((x) => x.tid !== human).slice(0, 10)) {
			const best = (await idb.cache.players.indexGetAll("playersByTid", t.tid))
				.sort((a, b) => b.value - a.value)
				.slice(0, 2);
			for (const p of best) {
				const t0 = performance.now();
				const v = await smartJudgeOffer(
					t.tid,
					human,
					{ pids: [p.pid], dpids: [] },
					{
						pids: (env.JT_GENEROUS
							? humanRoster.slice(-2)
							: humanRoster.slice(0, 2)
						).map((x) => x.pid),
						dpids: [],
					},
				);
				const ms = performance.now() - t0;
				times.push(ms);
				log(
					`${t.abbrev} ${p.firstName} ${p.lastName}: ${ms.toFixed(0)} ms, gain ${v.gain.toFixed(4)}`,
				);
			}
		}
		times.sort((a, b) => a - b);
		log(
			`median ${times[Math.floor(times.length / 2)]!.toFixed(0)} ms, worst ${times.at(-1)!.toFixed(0)} ms (first includes priming)`,
		);
	},
);
