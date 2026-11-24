import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// The NBA's trade dates on an owner's calendar league (game/tradableOn.ts):
// the summer's signings carried into the season, a signing in season, and
// the night each can first be traded - CD_LEAGUE=<league.json>
// CD_OUT=<file> [CD_UNTIL=2024-12-16].
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.CD_LEAGUE)(
	"cool-off dates",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.CD_OUT!, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.CD_LEAGUE!);
		const { g } = await import("../../worker/util/index.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const { phase, league, game, player } =
			await import("../../worker/core/index.ts");
		const { PHASE, PLAYER } = await import("../../common/constants.ts");
		const { smartHumans } =
			await import("../../worker/core/smartCpu/signing.ts");
		const humans = g.get("userTids");
		smartHumans.tids = [...humans];
		await league.setGameAttributes({ spectator: true });
		await phase.newPhase(PHASE.REGULAR_SEASON, {});
		const schedule = await idb.cache.schedule.getAll();
		const dates = schedule
			.map((r) => r.date)
			.filter((d): d is string => d !== undefined)
			.sort();
		log(`season ${g.get("season")}: opening night ${dates[0]}`);
		const waits = async () => {
			const by = new Map<string, { n: number; games: number; days?: number }>();
			for (const p of await idb.cache.players.getAll()) {
				if (p.tid >= 0 && p.gamesUntilTradable > 0) {
					const k = p.tradableOn ?? "(no date)";
					const had = by.get(k);
					by.set(k, {
						n: (had?.n ?? 0) + 1,
						games: p.gamesUntilTradable,
						days: p.daysUntilTradable,
					});
				}
			}
			return [...by]
				.sort()
				.map(([k, v]) => `${k}: ${v.n} (${v.games} games, ${v.days} days)`)
				.join("; ");
		};
		log(`as the season opens: ${await waits()}`);
		const until = env.CD_UNTIL ?? `${g.get("season") - 1}-12-16`;
		let signed: number | undefined;
		for (let night = 1; night < 200; night++) {
			const next = (await idb.cache.schedule.getAll())
				.map((r) => r.date)
				.filter((d): d is string => d !== undefined)
				.sort()[0];
			if (next === undefined || next > until) {
				break;
			}
			if (night === 3) {
				// A free agent signed in season.
				const fa = (
					await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
				).sort((a, b) => b.value - a.value)[0];
				const tid = (await idb.cache.teams.getAll()).find(
					(t) => !humans.includes(t.tid),
				)!.tid;
				if (fa) {
					await player.sign(
						fa,
						tid,
						{ amount: g.get("minContract"), exp: g.get("season") },
						PHASE.REGULAR_SEASON,
					);
					await idb.cache.players.put(fa);
					signed = fa.pid;
					log(
						`signed ${fa.firstName} ${fa.lastName} before ${next}: tradable ${fa.tradableOn}, ${fa.gamesUntilTradable} games, ${fa.daysUntilTradable} days`,
					);
				}
			}
			await game.play(1, {});
			log(`after ${next}: ${await waits()}`);
		}
		if (signed !== undefined) {
			const p = await idb.cache.players.get(signed);
			log(
				`in-season signing now: ${p?.gamesUntilTradable} games, ${p?.daysUntilTradable} days, ${p?.tradableOn}`,
			);
		}
		log("done");
	},
);
