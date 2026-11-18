import { test } from "vitest";
import { PHASE } from "../../common/constants.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// One market moment on a real league, CPU-profiled: the league advanced to
// a phase (ML_PHASE, the draft's end by default), then a moment run as an
// offseason window runs one.
//   ML_LEAGUE=<league.json> ML_OUT=<dir> npx vitest --run \
//     --project basketball src/test/smart/momentL6Probe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.ML_LEAGUE)(
	"real-league market moment profile",
	{ timeout: 20_000_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const out = env.ML_OUT ?? ".";
		fs.mkdirSync(out, { recursive: true });
		await loadLeagueFile(env.ML_LEAGUE!);
		const { game, phase, draft, league } =
			await import("../../worker/core/index.ts");
		const { marketMoment } =
			await import("../../worker/core/smartCpu/market.ts");
		const { smartClock, valueCount } =
			await import("../../worker/core/smartCpu/clock.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const to = Number(env.ML_PHASE ?? PHASE.AFTER_DRAFT);
		let guard = 0;
		while (g.get("phase") !== to && guard++ < 500) {
			const ph = g.get("phase");
			if (
				ph === PHASE.PLAYOFFS ||
				ph === PHASE.REGULAR_SEASON ||
				ph === PHASE.AFTER_TRADE_DEADLINE
			) {
				await game.play(ph === PHASE.PLAYOFFS ? 100 : 7, {});
			} else if (ph === PHASE.DRAFT) {
				await draft.runPicks({ type: "untilEnd" }, {});
				if (g.get("phase") === PHASE.DRAFT) {
					await phase.newPhase(PHASE.AFTER_DRAFT, {});
				}
			} else {
				await phase.newPhase((ph + 1) as any, {});
			}
		}
		for (const k of Object.keys(smartClock)) {
			delete smartClock[k];
		}
		valueCount.n = 0;
		const inspector = await importNode("node:inspector/promises");
		const session = new inspector.Session();
		session.connect();
		await session.post("Profiler.enable");
		await session.post("Profiler.setSamplingInterval", { interval: 500 });
		await session.post("Profiler.start");
		const t = performance.now();
		const made = await marketMoment();
		const ms = performance.now() - t;
		const { profile } = await session.post("Profiler.stop");
		fs.writeFileSync(`${out}/moment.cpuprofile`, JSON.stringify(profile));
		session.disconnect();
		const lines = [
			`${g.get("season")} phase ${g.get("phase")}: ${(ms / 1000).toFixed(1)} s, ${valueCount.n} values, ${made.length} deals`,
			...Object.entries(smartClock)
				.filter(([k]) => !k.includes("@"))
				.sort((a, b) => b[1].ms - a[1].ms)
				.map(
					([k, c]) =>
						`  ${k}: ${(c.ms / 1000).toFixed(1)} s in ${c.n} calls, ${c.values} values`,
				),
		];
		fs.writeFileSync(`${out}/report.txt`, lines.join("\n") + "\n");
	},
);
