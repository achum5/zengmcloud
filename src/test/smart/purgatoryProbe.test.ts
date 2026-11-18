import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Teams stuck in the middle: a league played on to a later preseason, then
// every trade written out by what each side gave and got (ages, ratings,
// picks) beside where each team stood when the run began - its title odds,
// its chance of the playoffs and the series it was expected to win - to see
// whether teams out of the race keep their veterans or build, with a run
// counted by series won or by making the playoffs (outlookSettings.
// seriesOnly).
//   PG_LEAGUE=<league.json> PG_OUT=<file> [PG_SERIES=0|1] [PG_SEASONS=1] \
//     npx vitest --run --project basketball src/test/smart/purgatoryProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PG_LEAGUE)(
	"purgatory probe",
	{ timeout: 14_400_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.PG_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.PG_LEAGUE!);
		const { league, game, phase, draft, freeAgents } =
			await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const { outlookSettings } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		if (env.PG_SERIES !== undefined) {
			outlookSettings.seriesOnly = env.PG_SERIES === "1";
		}
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const startSeason = g.get("season");
		const outlook: any = await currentOutlook();
		const standing = new Map<number, string>();
		for (const t of teams) {
			const o = outlook.teams.get(t.tid);
			if (o) {
				standing.set(
					t.tid,
					`${(100 * o.title[0]).toFixed(1)}% title, ${(100 * o.playoffs[0]).toFixed(0)}% playoffs`,
				);
			}
		}
		log(
			`${startSeason} phase ${g.get("phase")}, seriesOnly ${outlookSettings.seriesOnly}`,
		);
		for (const t of teams) {
			log(`  ${t.abbrev}: ${standing.get(t.tid)}`);
		}
		const seasons = Number(env.PG_SEASONS ?? 1);
		const events = async () => {
			const all = [];
			for (let s = startSeason; s <= startSeason + seasons; s++) {
				all.push(...(await idb.merged.events.getAll({ season: s })));
			}
			return all;
		};
		const firstEid =
			Math.max(-1, ...(await events()).map((e: any) => e.eid)) + 1;
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
		const players = new Map<number, any>();
		for (const p of await idb.merged.players.getAll({
			activeAndRetired: true,
		})) {
			players.set(p.pid, p);
		}
		const describe = (a: any, season: number) => {
			if ("dpid" in a) {
				return `${a.season} R${a.round} ${abbrev.get(a.originalTid)}`;
			}
			const p = players.get(a.pid);
			const r = p?.ratings[a.ratingsIndex] ?? p?.ratings.at(-1);
			return `${a.name} ${p ? season - p.born.year : "?"}y ${r ? `${r.ovr}/${r.pot}` : "?"}`;
		};
		const trades = (await events()).filter(
			(e: any) => e.eid >= firstEid && e.type === "trade",
		);
		log(`\n${trades.length} trades`);
		for (const e of trades as any[]) {
			const [a, b] = e.tids;
			log(
				`${e.season} ph${e.phase} ${e.aiTrade?.motivation ?? ""}: ${abbrev.get(a)} (${standing.get(a)}) gets [${e.teams[0].assets.map((x: any) => describe(x, e.season)).join("; ")}] | ${abbrev.get(b)} (${standing.get(b)}) gets [${e.teams[1].assets.map((x: any) => describe(x, e.season)).join("; ")}]`,
			);
		}
		log("\nrecords");
		for (let s = startSeason; s < g.get("season"); s++) {
			const rows = (await idb.merged.teamSeasons.getAll({
				season: s,
			})) as any[];
			log(
				`${s}: ${rows
					.sort((x, y) => y.won - x.won)
					.map(
						(r) =>
							`${abbrev.get(r.tid)} ${r.won}-${r.lost} r${r.playoffRoundsWon}`,
					)
					.join(", ")}`,
			);
		}
	},
);
