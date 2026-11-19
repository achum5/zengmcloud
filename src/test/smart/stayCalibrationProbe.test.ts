import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Re-signing on vanilla's rules, forecast against what happens: each
// player's chance of being willing to re-sign as the outlook takes it now
// (ensureRetention), for everyone whose deal runs out within ST_SEASONS
// seasons, then the league played on and who re-signed with his team.
// Players traded or released before their deal ran out are left out. By
// rookie deal or not, and by the chance forecast.
//   ST_LEAGUE=<league.json> ST_OUT=<file> [ST_SEASONS=1] npx vitest --run \
//     --project basketball src/test/smart/stayCalibrationProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.ST_LEAGUE)(
	"stay calibration probe",
	{ timeout: 14_400_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.ST_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.ST_LEAGUE!);
		const { league, game, phase, draft, freeAgents } =
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
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const startSeason = g.get("season");
		const seasons = Number(env.ST_SEASONS ?? 1);
		const outlook: any = await currentOutlook();
		const records = (await idb.cache.teamSeasons.getAll()).filter(
			(ts: any) => ts.season === startSeason,
		);
		const winp = new Map(
			records.map((ts: any) => [
				ts.tid,
				ts.won + ts.lost > 0 ? ts.won / (ts.won + ts.lost) : 0.5,
			]),
		);
		// A season with no games yet: last season's record.
		const lastSeason = (await idb.merged.teamSeasons.getAll({
			season: startSeason - 1,
		})) as any[];
		for (const ts of lastSeason) {
			if (!records.some((r: any) => r.tid === ts.tid && r.won + r.lost > 0)) {
				winp.set(ts.tid, ts.won / Math.max(1, ts.won + ts.lost));
			}
		}
		const tracked: {
			pid: number;
			tid: number;
			exp: number;
			rookie: boolean;
			ovr: number;
			age: number;
			stay: number;
			winp: number;
		}[] = [];
		const players = await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		]);
		const lastExp = startSeason + seasons - 1;
		for (const p of players as any[]) {
			const c = contractOf(p);
			if (c.exp > lastExp) {
				continue;
			}
			if (c.exp === startSeason && g.get("phase") > PHASE.RESIGN_PLAYERS) {
				continue;
			}
			await ensureRetention(outlook, [p], p.tid);
			tracked.push({
				pid: p.pid,
				tid: p.tid,
				exp: c.exp,
				rookie: !!c.rookie,
				ovr: p.ratings.at(-1).ovr,
				age: startSeason - p.born.year,
				stay: outlook.retention.get(retentionKey(p.pid, p.tid)),
				winp: winp.get(p.tid) ?? 0.5,
			});
		}
		log(
			`${startSeason} phase ${g.get("phase")}: ${tracked.length} players with deals running out by ${lastExp}`,
		);
		const firstEid =
			Math.max(
				-1,
				...(await idb.merged.events.getAll({ season: startSeason })).map(
					(e: any) => e.eid,
				),
			) + 1;
		const t0 = performance.now();
		let guard = 0;
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
			} else {
				await phase.newPhase((ph + 1) as any, {});
			}
		}
		log(
			`to ${g.get("season")} preseason in ${((performance.now() - t0) / 60000).toFixed(1)} min`,
		);
		const events: any[] = [];
		for (let s = startSeason; s <= startSeason + seasons; s++) {
			events.push(...(await idb.merged.events.getAll({ season: s })));
		}
		const moved = new Map<number, number>();
		for (const e of events) {
			if (e.eid < firstEid) {
				continue;
			}
			if (e.type === "trade" || e.type === "release") {
				for (const pid of e.pids ?? []) {
					const when = e.season + (e.phase ?? 0) / 10;
					moved.set(pid, Math.min(moved.get(pid) ?? Infinity, when));
				}
			}
		}
		const reSigned = new Set<string>();
		for (const e of events) {
			if (e.eid >= firstEid && e.type === "reSigned") {
				reSigned.add(`${e.pids[0]}:${e.tids[0]}`);
			}
		}
		const rows: { r: (typeof tracked)[number]; stayed: boolean }[] = [];
		for (const r of tracked) {
			// Moved before his deal ran out (its re-signing comes in its last
			// season's phase 7).
			const m = moved.get(r.pid);
			if (m !== undefined && m < r.exp + 0.7) {
				continue;
			}
			rows.push({ r, stayed: reSigned.has(`${r.pid}:${r.tid}`) });
		}
		const show = (label: string, sel: typeof rows) => {
			if (sel.length === 0) {
				return;
			}
			const mean = sel.reduce((s, x) => s + x.r.stay, 0) / sel.length;
			const got = sel.filter((x) => x.stayed).length / sel.length;
			log(
				`${label}: n=${sel.length}, forecast ${(100 * mean).toFixed(0)}%, re-signed ${(100 * got).toFixed(0)}%`,
			);
		};
		for (const rookie of [true, false]) {
			for (const [lo, hi] of [
				[0, 0.2],
				[0.2, 0.4],
				[0.4, 0.6],
				[0.6, 0.8],
				[0.8, 1.01],
			]) {
				show(
					`${rookie ? "rookie deal" : "veteran"}, ${(100 * lo!).toFixed(0)}-${Math.min(100, 100 * hi!).toFixed(0)}% forecast`,
					rows.filter(
						(x) =>
							x.r.rookie === rookie &&
							x.r.ovr >= 50 &&
							x.r.stay >= lo! &&
							x.r.stay < hi!,
					),
				);
			}
			for (const [lo, hi] of [
				[0, 0.4],
				[0.4, 0.6],
				[0.6, 1.01],
			]) {
				show(
					`${rookie ? "rookie deal" : "veteran"}, team ${(100 * lo!).toFixed(0)}-${Math.min(100, 100 * hi!).toFixed(0)}% wins`,
					rows.filter(
						(x) =>
							x.r.rookie === rookie &&
							x.r.ovr >= 50 &&
							x.r.winp >= lo! &&
							x.r.winp < hi!,
					),
				);
			}
		}
		log("\nevery player rated 55+:");
		for (const x of rows.filter((x) => x.r.ovr >= 55)) {
			log(
				`  pid ${x.r.pid} ${x.r.age}y ${x.r.ovr} ${x.r.rookie ? "rookie" : "vet"} exp ${x.r.exp}, team ${(100 * x.r.winp).toFixed(0)}% wins: forecast ${x.r.stay.toFixed(2)}, ${x.stayed ? "RE-SIGNED" : "left"}`,
			);
		}
	},
);
