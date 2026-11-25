import { test } from "vitest";
import { PLAYER } from "../../common/constants.ts";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { createArenaLeague, importArenaLeague } from "../arena/runLeague.ts";
import { projectRatings } from "../../worker/core/realisticFrontOffice/projection.ts";
import { projectPlayer } from "../../worker/core/realisticFrontOffice/outlook.ts";

// A draft class's careers as the game would play them out: each prospect's
// development run many times with the game's own code. DC_LEAGUE=<file at the
// draft> DC_OUT=<file>.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.DC_OUT)(
	"draft class truth",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const { league } = await import("../../worker/core/index.ts");
		Math.random = makeSeededRandom(1);
		(idb as any).league = undefined;
		await createArenaLeague(1);
		await importArenaLeague(env.DC_LEAGUE!);
		await league.loadGameAttributes();
		const season = g.get("season");
		const prospects = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
		)
			.filter((p) => p.draft.year === season)
			.sort((a, b) => b.value - a.value)
			.slice(0, Number(env.DC_N ?? 60));
		const rows = [];
		const N = 400;
		for (const p of prospects) {
			const r = p.ratings.at(-1)!;
			const age = season - p.born.year;
			const proj = projectRatings({
				ratings: r,
				age,
				years: 12,
				samples: N,
				seed: p.pid * 7 + 3,
				keepCareers: true,
				measure: () => [0],
			});
			const peaks = new Array(N).fill(0);
			for (let y = 0; y <= 12; y++) {
				for (let s = 0; s < N; s++) {
					peaks[s] = Math.max(peaks[s], proj.careers![y]![s]!.ovr);
				}
			}
			const share = (x: number) => peaks.filter((v) => v >= x).length / N;
			// BBGM's pot is the 75th percentile of his peak to 29 over 20 careers.
			const to29 = new Array(N).fill(r.ovr);
			for (let y = 1; age + y < 30 && y <= 12; y++) {
				for (let s = 0; s < N; s++) {
					to29[s] = Math.max(to29[s], proj.careers![y]![s]!.ovr);
				}
			}
			const q75 = [...to29].sort((a, b) => a - b)[Math.floor(0.75 * N)];
			rows.push({
				pid: p.pid,
				age,
				ovr: r.ovr,
				pot: r.pot,
				hgt: r.hgt,
				value: Math.round(p.value * 10) / 10,
				ovrBy: proj.ovr.map((x) => Math.round(x * 10) / 10),
				sdBy: proj.ovrSd.map((x) => Math.round(x * 10) / 10),
				peakMean: peaks.reduce((a, b) => a + b, 0) / N,
				q75,
				cpuOvr: projectPlayer(p, season).ovr.map(
					(x) => Math.round(x * 10) / 10,
				),
				cpuBoom: projectPlayer(p, season).boom.map(
					(x) => Math.round(x.ovr[1] * 10) / 10,
				),
				boomBy: proj.boom.map((x) => Math.round(x.ovr[1] * 10) / 10),
				p55: share(55),
				p60: share(60),
				p65: share(65),
				p70: share(70),
			});
		}
		fs.writeFileSync(
			env.DC_OUT!,
			rows.map((x) => JSON.stringify(x)).join("\n") + "\n",
		);
	},
);
