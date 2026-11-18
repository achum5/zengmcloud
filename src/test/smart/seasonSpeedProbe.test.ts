import { test } from "vitest";
import { PHASE } from "../../common/constants.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Where a season's time goes on a real league, from where its export stands
// to the same point a year on: wall time by phase (games and offseason
// steps), and the smart CPU's own clock by kind (smartCpu/clock.ts).
//   SPD_LEAGUE=<league.json> SPD_OUT=<file> [SPD_SMART=0] npx vitest --run \
//     --project basketball src/test/smart/seasonSpeedProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.SPD_LEAGUE)(
	"season speed probe",
	{ timeout: 20_000_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.SPD_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.SPD_LEAGUE!);
		const { game, phase, draft, freeAgents, league } =
			await import("../../worker/core/index.ts");
		const { smartClock, valueCount } =
			await import("../../worker/core/smartCpu/clock.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: env.SPD_SMART !== "0",
			realisticFrontOffices: false,
		});
		const conditions = {};
		const start = { season: g.get("season"), phase: g.get("phase") };
		log(`from ${start.season} phase ${start.phase}`);
		const byPhase: Record<string, number> = {};
		const t0 = performance.now();
		let guard = 0;
		while (
			!(
				g.get("season") === start.season + 1 && g.get("phase") === start.phase
			) &&
			guard++ < 2000
		) {
			const ph = g.get("phase");
			const t = performance.now();
			if (
				ph === PHASE.PLAYOFFS ||
				ph === PHASE.REGULAR_SEASON ||
				ph === PHASE.AFTER_TRADE_DEADLINE
			) {
				await game.play(ph === PHASE.PLAYOFFS ? 100 : 7, conditions);
			} else if (ph === PHASE.DRAFT) {
				await draft.runPicks({ type: "untilEnd" }, conditions);
				if (g.get("phase") === PHASE.DRAFT) {
					await phase.newPhase(PHASE.AFTER_DRAFT, conditions);
				}
			} else if (ph === PHASE.FREE_AGENCY) {
				await freeAgents.play(g.get("daysLeft"), conditions);
				if (g.get("phase") === PHASE.FREE_AGENCY) {
					await phase.newPhase(PHASE.PRESEASON, conditions);
				}
			} else {
				await phase.newPhase((ph + 1) as any, conditions);
			}
			byPhase[ph] = (byPhase[ph] ?? 0) + (performance.now() - t);
			if (g.get("phase") !== ph) {
				log(
					`  phase ${ph} done: ${(byPhase[ph]! / 1000).toFixed(0)} s (total ${((performance.now() - t0) / 60000).toFixed(1)} min)`,
				);
			}
		}
		log(`total ${((performance.now() - t0) / 60000).toFixed(1)} min`);
		log(`franchise values worked out: ${valueCount.n}`);
		const rows = Object.entries(smartClock)
			.filter(([k]) => !k.includes("@"))
			.sort((a, b) => b[1].ms - a[1].ms);
		for (const [k, c] of rows) {
			log(
				`${k}: ${(c.ms / 1000).toFixed(0)} s in ${c.n} calls, ${c.values} values`,
			);
		}
		log("");
		const byPh = Object.entries(smartClock)
			.filter(([k]) => k.includes("@"))
			.sort((a, b) => b[1].ms - a[1].ms)
			.slice(0, 25);
		for (const [k, c] of byPh) {
			log(`${k}: ${(c.ms / 1000).toFixed(0)} s in ${c.n} calls`);
		}
	},
);
