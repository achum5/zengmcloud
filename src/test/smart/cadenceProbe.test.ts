import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// How often the in-season market meets, on an owner's league: each night's
// moment, its time and the trades so far, from opening night to the trade
// deadline - CAD_LEAGUE=<league.json> CAD_OUT=<file> [CAD_EVERY=<games a
// team between moments>] [CAD_SEED=77].
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.CAD_LEAGUE)(
	"market cadence",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.CAD_OUT!, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.CAD_LEAGUE!);
		Math.random = makeSeededRandom(Number(env.CAD_SEED ?? 77));
		const { g } = await import("../../worker/util/index.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const { phase, league, game } = await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { smartHumans } =
			await import("../../worker/core/smartCpu/signing.ts");
		const { marketSettings } =
			await import("../../worker/core/smartCpu/market.ts");
		const { smartClock } = await import("../../worker/core/smartCpu/clock.ts");
		if (env.CAD_EVERY) {
			marketSettings.vanillaGamesBetween = Number(env.CAD_EVERY);
		}
		smartHumans.tids = [...g.get("userTids")];
		await league.setGameAttributes({ spectator: true });
		await phase.newPhase(PHASE.REGULAR_SEASON, {});
		const season = g.get("season");
		const trades = async () =>
			(await idb.merged.events.getAll({ season })).filter(
				(e: any) => e.type === "trade",
			).length;
		const before = await trades();
		log(
			g.get("nbaCalendar")
				? `season ${season}, on the NBA calendar`
				: `season ${season}, a moment every ${marketSettings.vanillaGamesBetween} games a team`,
		);
		const start = performance.now();
		let moments = 0;
		for (let night = 1; night < 300; night++) {
			if (g.get("phase") !== PHASE.REGULAR_SEASON) {
				break;
			}
			const next = (await idb.cache.schedule.getAll())
				.map((r) => r.date)
				.filter((d): d is string => d !== undefined)
				.sort()[0];
			const had = g.get("smartMarketMoment");
			const marketBefore = smartClock["market"]?.ms ?? 0;
			const t = performance.now();
			await game.play(1, {});
			if (g.get("smartMarketMoment") !== had) {
				moments += 1;
				log(
					`${next}: moment ${g.get("smartMarketMoment")}, market ${(((smartClock["market"]?.ms ?? 0) - marketBefore) / 1000).toFixed(1)} s, night ${((performance.now() - t) / 1000).toFixed(1)} s, trades ${(await trades()) - before}`,
				);
			}
		}
		log(
			`to the deadline: ${moments} moments, ${(await trades()) - before} trades, market ${((smartClock["market"]?.ms ?? 0) / 1000) | 0} s, all ${((performance.now() - start) / 60000).toFixed(1)} min`,
		);
	},
);
