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
// the game's own CPU runs it; the outlook only watches).
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.CAL_RUN === "1")(
	"calibration: title chances against who wins",
	{ timeout: 21_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { buildOutlook, OUTLOOK_YEARS } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
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
		};
		const rows: Row[] = [];
		const timing: number[] = [];
		await advanceSeasons(seasons, {
			onPreseason: async () => {
				const t0 = performance.now();
				g.setWithoutSavingToDB("realisticFrontOffices", true);
				const outlook = await buildOutlook({ iterations: 1000 });
				g.setWithoutSavingToDB("realisticFrontOffices", fo);
				timing.push((performance.now() - t0) / 1000);
				for (const o of outlook.teams.values()) {
					rows.push({
						season: g.get("season"),
						tid: o.tid,
						title: o.title.slice(0, Math.min(4, OUTLOOK_YEARS + 1)),
						playoffs: o.playoffs.slice(0, 4),
						strength: o.strength.slice(0, 4),
					});
				}
			},
		});

		// Who won each title: the team that won every round.
		const champions: Record<number, number> = {};
		const seen = new Set(rows.map((r) => r.season));
		for (const season of seen) {
			const rounds = g.get("numGamesPlayoffSeries", season).length;
			const teamSeasons = await idb.getCopies.teamsPlus(
				{ attrs: ["tid"], seasonAttrs: ["playoffRoundsWon"], season },
				"noCopyCache",
			);
			const champ = teamSeasons.find(
				(t: any) => t.seasonAttrs.playoffRoundsWon === rounds,
			);
			if (champ) {
				champions[season] = champ.tid;
			}
		}
		await fs.writeFile(
			`${out}/calibration-${seed}${fo ? "-fo" : ""}.json`,
			JSON.stringify({ seed, fo, rows, champions, timing }),
		);
	},
);
