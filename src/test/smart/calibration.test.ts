import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";

// LAYER 0 (docs/smart-cpu.md): are the GM's eyes right? Each preseason the
// outlook says every team's title chances this season and the next three;
// then the seasons play out. Said 30%, won about 30%?
//
//   CAL_RUN=1 CAL_SEED=1 CAL_SEASONS=10 CAL_OUT=<dir> \
//     npx vitest --run --project basketball src/test/smart/calibration.test.ts
//
// CAL_FO=1 plays the league with the realistic front offices on (by default
// the game's own CPU runs it; the outlook only watches). CAL_SCALES=1,1.58:
// the odds read at each of these playoff scales (outlookSettings.playoffScale)
// too, each preseason, as titleBy.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.CAL_RUN === "1")(
	"calibration: title chances against who wins",
	{ timeout: 21_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { buildOutlook, OUTLOOK_YEARS, outlookSettings } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const scales = env.CAL_SCALES
			? env.CAL_SCALES.split(",").map(Number)
			: undefined;
		const seed = Number(env.CAL_SEED ?? 1);
		const seasons = Number(env.CAL_SEASONS ?? 10);
		const out = env.CAL_OUT ?? ".";
		Math.random = makeSeededRandom(seed);
		(idb as any).league = undefined;
		await createArenaLeague(800 + seed);
		const fo = env.CAL_FO === "1";
		g.setWithoutSavingToDB("realisticFrontOffices", fo);

		type Row = {
			season: number;
			tid: number;
			title: number[];
			playoffs: number[];
			strength: number[];
			titleBy?: Record<string, number[]>;
			when?: string;
		};
		const rows: Row[] = [];
		const champions: Record<number, number> = {};
		const series: Record<
			number,
			{ h: number; hw: number; a?: number; aw?: number }[][]
		> = {};
		const findChampion = async (season: number) => {
			const rounds = g.get("numGamesPlayoffSeries", season).length;
			const champ = (
				await idb.cache.teamSeasons.indexGetAll("teamSeasonsBySeasonTid", [
					[season],
					[season, "Z"],
				])
			).find((ts) => ts.playoffRoundsWon === rounds);
			if (champ) {
				champions[season] = champ.tid;
			}
			// Every series' teams and games won (higher seed first).
			const ps = await idb.merged.playoffSeries.get(season);
			if (ps) {
				series[season] = ps.series.map((round) =>
					round.map((x) => ({
						h: x.home.tid,
						hw: x.home.won,
						a: x.away?.tid,
						aw: x.away?.won,
					})),
				);
			}
		};
		const timing: number[] = [];
		// Every team's odds now, at each scale.
		const read = async (when?: string) => {
			const t0 = performance.now();
			g.setWithoutSavingToDB("realisticFrontOffices", true);
			const outlook = await buildOutlook({ iterations: 1000 });
			const titleBy = new Map<number, Record<string, number[]>>();
			const baseScale = outlookSettings.playoffScale;
			for (const scale of scales ?? []) {
				outlookSettings.playoffScale = scale;
				const other = await buildOutlook({ iterations: 1000 });
				for (const o of other.teams.values()) {
					const by = titleBy.get(o.tid) ?? {};
					by[String(scale)] = o.title.slice(0, Math.min(4, OUTLOOK_YEARS + 1));
					titleBy.set(o.tid, by);
				}
			}
			outlookSettings.playoffScale = baseScale;
			g.setWithoutSavingToDB("realisticFrontOffices", fo);
			timing.push((performance.now() - t0) / 1000);
			for (const o of outlook.teams.values()) {
				rows.push({
					season: g.get("season"),
					tid: o.tid,
					title: o.title.slice(0, Math.min(4, OUTLOOK_YEARS + 1)),
					playoffs: o.playoffs.slice(0, 4),
					strength: o.strength.slice(0, 4),
					...(scales ? { titleBy: titleBy.get(o.tid) } : {}),
					...(when ? { when } : {}),
				});
			}
		};
		// CAL_DEADLINE=1: the odds the day before the trade deadline too
		// (when: "deadline"); CAL_PLAYOFFS=1, as the playoffs begin (when:
		// "playoffs").
		await advanceSeasons(seasons, {
			...(env.CAL_PLAYOFFS === "1"
				? { onPlayoffs: () => read("playoffs") }
				: {}),
			onPreseason: async () => {
				// Last season's champion, while its season is still in the cache.
				await findChampion(g.get("season") - 1);
				await read();
			},
			...(env.CAL_DEADLINE === "1"
				? { surveyAt: [1], onSurvey: () => read("deadline") }
				: {}),
		});

		// Who won each title (the team that won every round): each season's
		// found at the next preseason, and the last two's here - the cache
		// holds only the latest seasons.
		for (const season of new Set(rows.map((r) => r.season))) {
			if (champions[season] === undefined) {
				await findChampion(season);
			}
		}
		await fs.writeFile(
			`${out}/calibration-${seed}${fo ? "-fo" : ""}.json`,
			JSON.stringify({ seed, fo, rows, champions, series, timing }),
		);
	},
);
