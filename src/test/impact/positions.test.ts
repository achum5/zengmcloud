import { test } from "vitest";
import { PLAYER } from "../../common/constants.ts";
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

// THE POSITIONS LAB: what a roster loses in the game's sim when it is short of
// guards, or of forwards and centers - which the strength model, ranking
// players by value alone, does not see. The sim starts 2 guards and 2
// forwards or centers, and won't sub a man in if that leaves fewer than 2
// guards (unless one is a PG) or 2 forwards (unless one is a C) on the floor,
// short of a starter about to drop.
//
// Every team of a random-players league, healthy, in the order the game sorts
// it, has its best guards swapped for the forwards and centers closest to
// them in value from around the league, and its best forwards and centers for
// guards; each roster plays the rest of the league on the same random
// numbers as the team it came from. Rows go to a file for the analysis.
//
//   POSITIONS_RUN=1 POSITIONS_SEED=1 POSITIONS_GAMES=300 POSITIONS_OUT=/tmp/pos \
//     npx vitest --run --project basketball src/test/impact/positions.test.ts

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

const posOf = (p: Player) => last(p.ratings).pos;
const pureGuard = (pos: string) => !pos.includes("F") && pos !== "C";
const pureBig = (pos: string) => !pos.includes("G");

const healthy = (players: readonly Player[]) =>
	players.map((p) => {
		const copy = helpers.deepCopy(p);
		copy.injury = { type: "Healthy", gamesRemaining: 0 };
		return copy;
	});

const lineOf = (p: Player): StrengthPlayer => ({
	...playerLine(last(p.ratings) as unknown as ValueRatings, posOf(p)),
	valueNoPot: p.valueNoPot,
});

const describe = (p: Player) => ({
	pid: p.pid,
	pos: posOf(p),
	ovr: last(p.ratings).ovr,
	valueNoPot: Math.round(p.valueNoPot * 10) / 10,
});

test.runIf(env.POSITIONS_RUN === "1")(
	"positions: lab",
	{ timeout: 12 * 60 * 60 * 1000 },
	async () => {
		const seed = Number(env.POSITIONS_SEED ?? 1);
		const games = Number(env.POSITIONS_GAMES ?? 300);
		const out = env.POSITIONS_OUT ?? ".";
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
			const pool = [
				...rosters.flat(),
				...healthy(
					await idb.cache.players.indexGetAll(
						"playersByTid",
						PLAYER.FREE_AGENT,
					),
				),
			];
			const opponents: SimTeam[] = [];
			for (let tid = 0; tid < numTeams; tid++) {
				opponents.push(await toSimTeam(tid, inGameOrder(rosters[tid]!)));
			}

			// Margin a game against the rest of the league, game i on the same
			// random numbers whatever the roster, and minutes a game by player.
			const simulate = async (tid: number, roster: Player[]) => {
				const team = await toSimTeam(tid, inGameOrder(roster));
				const margins: number[] = [];
				const minutes = new Map<number, number>();
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
					for (const p of result.team[k].player) {
						minutes.set(p.id, (minutes.get(p.id) ?? 0) + p.stat.min / games);
					}
				}
				return { margins, minutes };
			};

			// The pool player closest in value to p, of the other kind, not on
			// the roster already.
			const swapFor = (
				p: Player,
				roster: readonly Player[],
				kind: (pos: string) => boolean,
			) => {
				const taken = new Set(roster.map((x) => x.pid));
				let best: Player | undefined;
				for (const x of pool) {
					if (taken.has(x.pid) || !kind(posOf(x))) {
						continue;
					}
					if (
						!best ||
						Math.abs(x.valueNoPot - p.valueNoPot) <
							Math.abs(best.valueNoPot - p.valueNoPot)
					) {
						best = x;
					}
				}
				return best;
			};

			const rows = [];
			const started = Date.now();
			const teamsToRun = Number(env.POSITIONS_TEAMS ?? numTeams);
			for (let tid = 0; tid < teamsToRun; tid++) {
				const base = rosters[tid]!;
				const baseSim = await simulate(tid, base);
				const baseModel = rosterPoints(base.map(lineOf));
				const byValue = [...base].sort((a, b) => b.valueNoPot - a.valueNoPot);
				const top = byValue.slice(0, 8);
				const variants: {
					name: string;
					out: Player[];
					from: (pos: string) => boolean;
				}[] = [];
				const guards = top.filter((p) => pureGuard(posOf(p)));
				const bigs = top.filter((p) => pureBig(posOf(p)));
				for (const n of [1, 2]) {
					if (guards.length >= n) {
						variants.push({
							name: `guards out ${n}`,
							out: guards.slice(0, n),
							from: pureBig,
						});
					}
					if (bigs.length >= n) {
						variants.push({
							name: `bigs out ${n}`,
							out: bigs.slice(0, n),
							from: pureGuard,
						});
					}
				}
				// And milder ones: a player in the top 8 at random for one of
				// another position at random.
				const random = makeSeededRandom(seed * 7919 + tid);
				const positions = ["PG", "SG", "G", "GF", "SF", "F", "PF", "FC", "C"];
				for (let i = 0; i < 2; i++) {
					const p = top[Math.floor(random() * top.length)]!;
					const others = positions.filter((x) => x !== posOf(p));
					const to = others[Math.floor(random() * others.length)]!;
					variants.push({
						name: "random",
						out: [p],
						from: (pos) => pos === to,
					});
				}

				const team = {
					tid,
					roster: byValue.map((p) => ({
						...describe(p),
						min: Math.round((baseSim.minutes.get(p.pid) ?? 0) * 10) / 10,
					})),
					model: baseModel,
					margin:
						baseSim.margins.reduce((a, x) => a + x, 0) / baseSim.margins.length,
					variants: [] as Record<string, unknown>[],
				};
				for (const v of variants) {
					let roster = [...base];
					const incoming: Player[] = [];
					for (const p of v.out) {
						const x = swapFor(p, roster, v.from);
						if (!x) {
							continue;
						}
						const copy = helpers.deepCopy(x);
						copy.tid = tid;
						roster = [...roster.filter((y) => y.pid !== p.pid), copy];
						incoming.push(copy);
					}
					if (incoming.length !== v.out.length) {
						continue;
					}
					const sim = await simulate(tid, roster);
					const d = sim.margins.map((m, i) => m - baseSim.margins[i]!);
					const mean = d.reduce((a, x) => a + x, 0) / d.length;
					const variance =
						d.reduce((a, x) => a + (x - mean) ** 2, 0) / (d.length - 1);
					team.variants.push({
						name: v.name,
						out: v.out.map(describe),
						in: incoming.map(describe),
						model: rosterPoints(roster.map(lineOf)) - baseModel,
						sim: { mean, se: Math.sqrt(variance / d.length) },
						roster: [...roster]
							.sort((a, b) => b.valueNoPot - a.valueNoPot)
							.map((p) => ({
								...describe(p),
								min: Math.round((sim.minutes.get(p.pid) ?? 0) * 10) / 10,
							})),
					});
				}
				rows.push(team);
				await fs.appendFile(
					`${out}/progress-${seed}.log`,
					`team ${tid}: ${team.variants.length} variants (${((Date.now() - started) / 60000).toFixed(1)} min)\n`,
				);
			}
			await fs.writeFile(
				`${out}/positions-${seed}.json`,
				JSON.stringify({ seed, games, teams: rows }),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);
