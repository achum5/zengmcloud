import { test } from "vitest";
import { makeSeededRandom, randInt } from "../../common/random.ts";
import { PHASE } from "../../common/constants.ts";
import type { Player } from "../../common/types.ts";
import { last } from "../../common/utils.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import {
	playerLine,
	rosterPoints,
	SLOT_MINUTES,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { createArenaLeague } from "../arena/runLeague.ts";
import { inGameOrder, play, toSimTeam } from "./simTools.ts";

// STAR SWAPS AGAINST THE MARGIN OF ERROR (owner's call, 2026-11-11: are
// close star-for-star swaps refused for a margin that is too big?). Swaps two
// teams' good players one for one and plays each team's lineup, with and
// without the swap, against the rest of the league many times, the same games
// both ways: what the swap really did to each team's margin a game, against
// what the strength model said it would, and against how far off the margin
// of error (margin.ts) allows it to be this season. Never part of the normal
// test run.
//
//   STAR_SWAPS=<dir> STAR_SEED=<seed> STAR_N=<swaps> STAR_GAMES=<games a side>
//     npx vitest --run src/test/impact/starSwaps.test.ts

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

// margin.ts's on-court error: points a game per full game on the floor.
const valueError = (ovr: number) => 0.4 + 0.037 * Math.max(0, ovr - 52);

const strengthPlayer = (p: Player) => ({
	...playerLine(last(p.ratings) as unknown as ValueRatings),
	valueNoPot: p.valueNoPot,
});

test.runIf(env.STAR_SWAPS !== undefined)(
	"star swaps against the margin of error",
	{ timeout: 12 * 60 * 60 * 1000 },
	async () => {
		const dir = env.STAR_SWAPS!;
		const seed = Number(env.STAR_SEED ?? 1);
		const numSwaps = Number(env.STAR_N ?? 40);
		const games = Number(env.STAR_GAMES ?? 1000);
		const top = Number(env.STAR_TOP ?? 3);
		const fs = await importNode("node:fs/promises");
		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(seed);
		try {
			(idb as any).league = undefined;
			await createArenaLeague(seed);
			g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
			const bases: Player[][] = [];
			for (let tid = 0; tid < g.get("numTeams"); tid++) {
				bases.push(
					(await idb.cache.players.indexGetAll("playersByTid", tid))
						.map((p) => {
							const copy = helpers.deepCopy(p);
							copy.injury = { type: "Healthy", gamesRemaining: 0 };
							return copy;
						})
						.sort((a, b) => b.valueNoPot - a.valueNoPot)
						.slice(0, 13),
				);
			}
			const numTeams = bases.length;

			// A team's margin a game against the others in turn, game i the same
			// game (seed) whatever its roster.
			const marginOf = async (tid: number, roster: Player[], salt: number) => {
				const team = await toSimTeam(tid, inGameOrder(roster));
				const out: number[] = [];
				for (let i = 0; i < games; i++) {
					const oppTid = (tid + 1 + (i % (numTeams - 1))) % numTeams;
					const opp = await toSimTeam(oppTid, inGameOrder(bases[oppTid]!));
					Math.random = makeSeededRandom(salt * 1_000_003 + i);
					const homeGame = i % 2 === 0;
					const result = homeGame
						? play(helpers.deepCopy(team), opp)
						: play(opp, helpers.deepCopy(team));
					const m = result.team[0].stat.pts - result.team[1].stat.pts;
					out.push(homeGame ? m : -m);
				}
				return out;
			};

			// How far off the margin of error allows a team's points this season
			// to be: each moved player's error at his place on the team that has
			// him after, the two added up as if independent (margin.ts).
			const allowed = (after: Player[], moved: Player[]) => {
				const sorted = [...after].sort((a, b) => b.valueNoPot - a.valueNoPot);
				let variance = 0;
				for (const p of moved) {
					const place = sorted.findIndex((q) => q.pid === p.pid);
					const others = sorted.filter((q) => q.pid !== p.pid);
					const at =
						place >= 0
							? place
							: others.filter((q) => q.valueNoPot > p.valueNoPot).length;
					const minutes = SLOT_MINUTES[Math.min(at, SLOT_MINUTES.length - 1)]!;
					variance += (minutes * valueError(last(p.ratings).ovr)) ** 2;
				}
				return Math.sqrt(variance);
			};

			const describe = (p: Player) => ({
				pid: p.pid,
				ovr: last(p.ratings).ovr,
				pos: last(p.ratings).pos,
				age: g.get("season") - p.born.year,
				valueNoPot: Math.round(p.valueNoPot * 100) / 100,
			});

			const rows = [];
			const baseMargins = new Map<number, number[]>();
			// The change a game, and its standard error from the same games
			// played both ways.
			const change = (after: number[], before: number[]) => {
				const d = after.map((x, i) => x - before[i]!);
				const mean = d.reduce((a, x) => a + x, 0) / d.length;
				const variance =
					d.reduce((a, x) => a + (x - mean) ** 2, 0) / (d.length - 1);
				return { mean, se: Math.sqrt(variance / d.length) };
			};
			const salt = (tid: number) => seed * 1000 + tid;
			for (let n = 0; n < numSwaps; n++) {
				Math.random = makeSeededRandom(seed * 7919 + n);
				const tidA = randInt(0, numTeams - 1);
				let tidB = randInt(0, numTeams - 2);
				if (tidB >= tidA) {
					tidB += 1;
				}
				const pa = bases[tidA]![randInt(0, top - 1)]!;
				const pb = bases[tidB]![randInt(0, top - 1)]!;
				const afterA = bases[tidA]!.map((p) => (p === pa ? pb : p));
				const afterB = bases[tidB]!.map((p) => (p === pb ? pa : p));
				const pts = (roster: Player[]) =>
					rosterPoints(roster.map(strengthPlayer));
				const predA = pts(afterA) - pts(bases[tidA]!);
				const predB = pts(afterB) - pts(bases[tidB]!);
				for (const tid of [tidA, tidB]) {
					if (!baseMargins.has(tid)) {
						baseMargins.set(tid, await marginOf(tid, bases[tid]!, salt(tid)));
					}
				}
				const t0 = Date.now();
				const realA = change(
					await marginOf(tidA, afterA, salt(tidA)),
					baseMargins.get(tidA)!,
				);
				const realB = change(
					await marginOf(tidB, afterB, salt(tidB)),
					baseMargins.get(tidB)!,
				);
				rows.push({
					seed,
					tidA,
					tidB,
					pa: describe(pa),
					pb: describe(pb),
					predA,
					predB,
					sdA: allowed(afterA, [pa, pb]),
					sdB: allowed(afterB, [pa, pb]),
					realA: realA.mean,
					seA: realA.se,
					realB: realB.mean,
					seB: realB.se,
					seconds: (Date.now() - t0) / 1000,
				});
				await fs.mkdir(dir, { recursive: true });
				await fs.writeFile(
					`${dir}/swaps-${seed}.json`,
					JSON.stringify({ games, rows }),
				);
			}
		} finally {
			Math.random = realRandom;
		}
	},
);
