import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { last } from "../../common/utils.ts";
import {
	playerLine,
	rosterPoints,
	type StrengthPlayer,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { createArenaLeague } from "../arena/runLeague.ts";
import { inGameOrder, play, toSimTeam, type SimTeam } from "./simTools.ts";

// THE REMOVAL LAB: what losing one player does to a roster in the game's sim,
// against what the strength model says - for every team's top 10, and above
// all for the players the model says a team is better without (a player who
// takes a rotation place and plays it worse than the man behind him would).
// Each roster plays the rest of the league on the same random numbers with
// and without him; rows go to a file for the analysis.
//
//   REMOVAL_RUN=1 REMOVAL_SEED=1 REMOVAL_GAMES=200 REMOVAL_OUT=/tmp/rm \
//     [REMOVAL_MIN_USAGE=0.45] \
//     npx vitest --run --project basketball src/test/impact/removal.test.ts

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

const healthy = (players: readonly Player[]) =>
	players.map((p) => {
		const copy = helpers.deepCopy(p);
		copy.injury = { type: "Healthy", gamesRemaining: 0 };
		return copy;
	});

const lineOf = (p: Player): StrengthPlayer => ({
	...playerLine(
		last(p.ratings) as unknown as ValueRatings,
		last(p.ratings).pos,
	),
	valueNoPot: p.valueNoPot,
});

test.runIf(env.REMOVAL_RUN === "1")(
	"removal: lab",
	{ timeout: 12 * 60 * 60 * 1000 },
	async () => {
		const seed = Number(env.REMOVAL_SEED ?? 1);
		const games = Number(env.REMOVAL_GAMES ?? 200);
		const out = env.REMOVAL_OUT ?? ".";
		const fs = await importNode("node:fs/promises");
		await fs.mkdir(out, { recursive: true });

		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(seed);
		try {
			await createArenaLeague(seed);
			const numTeams = g.get("numTeams");
			const rosters: Player[][] = [];
			for (let tid = 0; tid < numTeams; tid++) {
				rosters.push(
					healthy(await idb.cache.players.indexGetAll("playersByTid", tid)),
				);
			}
			const opponents: SimTeam[] = [];
			for (let tid = 0; tid < numTeams; tid++) {
				opponents.push(await toSimTeam(tid, inGameOrder(rosters[tid]!)));
			}
			const simulate = async (tid: number, roster: Player[]) => {
				const team = await toSimTeam(tid, inGameOrder(roster));
				const margins: number[] = [];
				for (let i = 0; i < games; i++) {
					const oppTid = (tid + 1 + (i % (numTeams - 1))) % numTeams;
					Math.random = makeSeededRandom(seed * 1_000_003 + i);
					const home = i % 2 === 0;
					const result = home
						? play(helpers.deepCopy(team), helpers.deepCopy(opponents[oppTid]!))
						: play(
								helpers.deepCopy(opponents[oppTid]!),
								helpers.deepCopy(team),
							);
					const k = home ? 0 : 1;
					margins.push(result.team[k].stat.pts - result.team[1 - k].stat.pts);
				}
				return margins;
			};

			const rows = [];
			const teamsToRun = Number(env.REMOVAL_TEAMS ?? numTeams);
			for (let tid = 0; tid < teamsToRun; tid++) {
				const base = rosters[tid]!;
				const baseModel = rosterPoints(base.map(lineOf));
				const top = [...base]
					.sort((a, b) => b.valueNoPot - a.valueNoPot)
					.slice(0, 10);
				// REMOVAL_MIN_USAGE: only the rotation players who use at least
				// this many possessions - the ones the model may misprice.
				const minUsage = Number(env.REMOVAL_MIN_USAGE ?? 0);
				const tried = top.filter(
					(p) => top.indexOf(p) < 8 && lineOf(p).usage >= minUsage,
				);
				if (minUsage > 0 && tried.length === 0) {
					continue;
				}
				const baseSim = await simulate(tid, base);
				for (const p of minUsage > 0 ? tried : top) {
					const without = base.filter((x) => x !== p);
					const sim = await simulate(tid, without);
					const diffs = sim.map((m, i) => m - baseSim[i]!);
					const mean = diffs.reduce((a, x) => a + x, 0) / games;
					const sd = Math.sqrt(
						diffs.reduce((a, x) => a + (x - mean) ** 2, 0) / (games - 1),
					);
					const r = last(p.ratings);
					rows.push({
						tid,
						pid: p.pid,
						pos: r.pos,
						ovr: r.ovr,
						valueNoPot: Math.round(p.valueNoPot * 10) / 10,
						rank: top.indexOf(p),
						line: lineOf(p),
						model: rosterPoints(without.map(lineOf)) - baseModel,
						sim: mean,
						se: sd / Math.sqrt(games),
					});
				}
				await fs.writeFile(`${out}/removal-${seed}.json`, JSON.stringify(rows));
			}
		} finally {
			Math.random = realRandom;
		}
	},
);
