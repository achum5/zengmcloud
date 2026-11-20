import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE RE-SIGNING FORECAST, against what the game decides (resignForecast.ts).
// The league is played on RF_SEASONS seasons; at each preseason, trade
// deadline and draft's end, every rostered player whose contract runs out
// within three seasons is forecast to be willing to re-sign with his team -
// as the outlook has it (the forecast for the season it runs out) and by his
// mood now (the old way: willingChance). Just before each re-signing phase,
// every player whose contract runs out is asked as the game asks him then
// (moodInfo: his chance of being willing, and whether he is). Forecasts for
// players still with that team then are set against it, by seasons ahead.
//   RF_LEAGUE=<league.json | arena:<lid>> RF_OUT=<file> [RF_SEASONS=3] \
//     npx vitest --run --project basketball \
//     src/test/smart/resignForecastProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.RF_LEAGUE)(
	"re-signing forecast probe",
	{ timeout: 14_400_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.RF_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.RF_LEAGUE!);
		const { league, game, phase, draft, freeAgents, player } =
			await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { idb } = await import("../../worker/db/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { ensureRetention, retentionKey } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { willingChance } =
			await import("../../worker/core/realisticFrontOffice/offers.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		type Row = {
			key: string;
			when: string;
			ahead: number;
			forecast: number;
			old: number;
			rookie: boolean;
			ovr: number;
		};
		const rows: Row[] = [];
		const asked = new Map<string, { prob: number; willing: boolean }>();
		const snapshot = async (when: string) => {
			const outlook: any = await currentOutlook();
			const season = g.get("season");
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			let n = 0;
			for (const p of players as any[]) {
				const c = contractOf(p);
				const ahead = c.exp - season;
				if (ahead < 0 || ahead > 3) {
					continue;
				}
				if (ahead === 0 && g.get("phase") > PHASE.AFTER_DRAFT) {
					continue;
				}
				await ensureRetention(outlook, [p], p.tid);
				rows.push({
					key: `${p.pid}:${p.tid}:${c.exp}`,
					when,
					ahead,
					forecast: outlook.retention.get(retentionKey(p.pid, p.tid)),
					old: await willingChance(p, p.tid),
					rookie: !!c.rookie,
					ovr: p.ratings.at(-1).ovr,
				});
				n += 1;
			}
			log(`${when}: ${n} forecasts`);
		};
		const askNow = async () => {
			const season = g.get("season");
			const players = await idb.cache.players.indexGetAll("playersByTid", [
				0,
				Infinity,
			]);
			let n = 0;
			for (const p of players as any[]) {
				if (contractOf(p).exp !== season) {
					continue;
				}
				const mood = await player.moodInfo(p, p.tid);
				asked.set(`${p.pid}:${p.tid}:${season}`, {
					prob: mood.probWilling,
					willing: mood.willing,
				});
				n += 1;
			}
			log(`${season} re-signing: ${n} asked`);
		};
		const startSeason = g.get("season");
		const seasons = Number(env.RF_SEASONS ?? 3);
		await snapshot(`${startSeason} ph${g.get("phase")} start`);
		const t0 = performance.now();
		let guard = 0;
		let deadlineSeen = -1;
		let preseasonSeen = g.get("phase") === PHASE.PRESEASON ? startSeason : -1;
		while (
			!(
				g.get("season") >= startSeason + seasons &&
				g.get("phase") === PHASE.PRESEASON
			) &&
			guard++ < 5000
		) {
			const ph = g.get("phase");
			if (
				ph === PHASE.PLAYOFFS ||
				ph === PHASE.REGULAR_SEASON ||
				ph === PHASE.AFTER_TRADE_DEADLINE
			) {
				if (
					ph === PHASE.AFTER_TRADE_DEADLINE &&
					deadlineSeen !== g.get("season")
				) {
					deadlineSeen = g.get("season");
					await snapshot(`${g.get("season")} deadline`);
				}
				await game.play(ph === PHASE.PLAYOFFS ? 100 : 7, {});
			} else if (ph === PHASE.DRAFT) {
				await draft.runPicks({ type: "untilEnd" }, {});
				if (g.get("phase") === PHASE.DRAFT) {
					await phase.newPhase(PHASE.AFTER_DRAFT, {});
				}
			} else if (ph === PHASE.FREE_AGENCY) {
				await freeAgents.play(g.get("daysLeft"), {});
				if (g.get("phase") === PHASE.FREE_AGENCY) {
					await phase.newPhase(PHASE.PRESEASON, {});
				}
			} else if (ph === PHASE.AFTER_DRAFT) {
				await askNow();
				await snapshot(`${g.get("season")} after draft`);
				await phase.newPhase(PHASE.RESIGN_PLAYERS, {});
			} else {
				await phase.newPhase((ph + 1) as any, {});
			}
			if (
				g.get("phase") === PHASE.PRESEASON &&
				preseasonSeen !== g.get("season") &&
				g.get("season") < startSeason + seasons
			) {
				preseasonSeen = g.get("season");
				await snapshot(`${g.get("season")} preseason`);
			}
		}
		log(
			`to ${g.get("season")} preseason in ${((performance.now() - t0) / 60000).toFixed(1)} min`,
		);
		const joined = rows
			.map((r) => ({ r, a: asked.get(r.key) }))
			.filter((x) => x.a !== undefined) as {
			r: Row;
			a: { prob: number; willing: boolean };
		}[];
		const loss = (q: number, y: boolean) => {
			const c = Math.min(1 - 1e-3, Math.max(1e-3, q));
			return -(y ? Math.log(c) : Math.log(1 - c));
		};
		const show = (label: string, sel: typeof joined) => {
			if (sel.length === 0) {
				return;
			}
			const mean = (f: (x: (typeof sel)[number]) => number) =>
				sel.reduce((s, x) => s + f(x), 0) / sel.length;
			log(
				`${label}: n=${sel.length} | forecast ${(100 * mean((x) => x.r.forecast)).toFixed(0)}% old ${(100 * mean((x) => x.r.old)).toFixed(0)}% | asked then ${(100 * mean((x) => x.a.prob)).toFixed(0)}%, willing ${(100 * mean((x) => (x.a.willing ? 1 : 0))).toFixed(0)}% | off by ${(100 * mean((x) => Math.abs(x.r.forecast - x.a.prob))).toFixed(0)} against ${(100 * mean((x) => Math.abs(x.r.old - x.a.prob))).toFixed(0)} | log loss ${mean((x) => loss(x.r.forecast, x.a.willing)).toFixed(3)} against ${mean((x) => loss(x.r.old, x.a.willing)).toFixed(3)}`,
			);
		};
		log(`\n${joined.length} forecasts of ${rows.length} still with the team`);
		for (const ahead of [0, 1, 2, 3]) {
			for (const rookie of [true, false]) {
				const sel = joined.filter(
					(x) => x.r.ahead === ahead && x.r.rookie === rookie,
				);
				show(
					`${ahead} season${ahead === 1 ? "" : "s"} ahead, ${rookie ? "rookie deals" : "veterans"}`,
					sel,
				);
				show(
					`  of them 55+`,
					sel.filter((x) => x.r.ovr >= 55),
				);
			}
		}
		log("\nby the forecast (all seasons ahead, 50+)");
		for (const [lo, hi] of [
			[0, 0.2],
			[0.2, 0.4],
			[0.4, 0.6],
			[0.6, 0.8],
			[0.8, 1.01],
		] as const) {
			show(
				`forecast ${(100 * lo).toFixed(0)}-${Math.min(100, 100 * hi).toFixed(0)}%`,
				joined.filter(
					(x) => x.r.ovr >= 50 && x.r.forecast >= lo && x.r.forecast < hi,
				),
			);
		}
		log("\nby the old forecast (all seasons ahead, 50+)");
		for (const [lo, hi] of [
			[0, 0.2],
			[0.2, 0.4],
			[0.4, 0.6],
			[0.6, 0.8],
			[0.8, 1.01],
		] as const) {
			show(
				`old ${(100 * lo).toFixed(0)}-${Math.min(100, 100 * hi).toFixed(0)}%`,
				joined.filter((x) => x.r.ovr >= 50 && x.r.old >= lo && x.r.old < hi),
			);
		}
		// Every joined forecast, for a closer look.
		fs.writeFileSync(
			`${env.RF_OUT ?? "/tmp/rf"}.json`,
			JSON.stringify(joined.map((x) => ({ ...x.r, ...x.a }))),
		);
	},
);
