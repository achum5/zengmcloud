import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { helpers } from "../../worker/util/index.ts";
import { loadLeagueFile } from "../smart/loadLeague.ts";
import { inGameOrder, play, toSimTeam, type SimTeam } from "./simTools.ts";

// THE MINUTES THAT DECIDE GAMES, by place in the pecking order: each team's
// healthy roster (its best 15 by value) plays every other team LV_GAMES times
// each way in the game's own sim, and every second a man is on the floor is
// weighed by how much a point then moves his team's chance of winning - the
// leverage of the moment, from the score, the time left and how the two
// teams' games go (a normal model of the margin still to come, fit on these
// games). A man who plays at random moments gets his share of the leverage;
// one who plays when the game is decided, none of it. Per place: his share
// of the game on the floor (the strength model's SLOT_MINUTES) and his share
// of the leverage (the minutes that decide games), overall and for the
// league's best teams.
//   LV_LEAGUE=<league.json | arena:<lid>> LV_OUT=<file> [LV_GAMES=2]
//   [LV_TOP=8] npx vitest --run --project basketball
//   src/test/impact/leverageLab.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

const PLACES = 15;
const STEP = 4;

type Game = {
	tids: [number, number];
	timeline: {
		periodEnds: number[];
		stints: { pid: number; spans: [number, number][] }[];
		scoring: { t: number; side: 0 | 1; pts: number }[];
	};
	final: number;
	regulation: number;
};

test.skipIf(!env.LV_LEAGUE)(
	"leverage lab",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.LV_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.LV_LEAGUE!);
		const { g } = await import("../../worker/util/index.ts");
		const numPeriods = g.get("numPeriods");
		// LV_PLAYOFFS=1: playoff games (the sim's starters tire less then).
		if (env.LV_PLAYOFFS === "1") {
			const { PHASE } = await import("../../common/constants.ts");
			g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
		}
		const games = Number(env.LV_GAMES ?? 2);
		const healthy = (ps: Player[]) =>
			ps.map((p) => ({
				...p,
				injury: { type: "Healthy", gamesRemaining: 0 },
			}));
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const rosters = new Map<number, Player[]>();
		const placeOf = new Map<number, number>();
		for (const t of teams) {
			const roster = healthy(
				await idb.cache.players.indexGetAll("playersByTid", t.tid),
			)
				.sort((a, b) => b.valueNoPot - a.valueNoPot)
				.slice(0, PLACES);
			roster.forEach((p, i) => placeOf.set(p.pid, i));
			rosters.set(t.tid, roster);
		}
		const sim = new Map<number, SimTeam>();
		for (const t of teams) {
			sim.set(t.tid, await toSimTeam(t.tid, inGameOrder(rosters.get(t.tid)!)));
		}
		// LV_ABSENT: each man out of each game at this chance, as the strength
		// model's injuries have it (INJURY_SHARE) - the team playing without
		// him - and the model's minutes for each place over the same games
		// (SLOT_MINUTES at his place among those there).
		const absent = Number(env.LV_ABSENT ?? 0);
		const { SLOT_MINUTES } =
			await import("../../worker/core/realisticFrontOffice/strength.ts");
		const modelSec = {
			all: new Array(PLACES).fill(0),
			top: new Array(PLACES).fill(0),
		};
		const modelOf: Map<number, number>[] = [];
		// LV_SWAPS: up to this many of a team's men swapped for others from
		// around the league each game, for rosters of every shape (as the
		// impact model's games were).
		const swaps = Number(env.LV_SWAPS ?? 0);
		const pool = [...rosters.values()].flat();
		// Each team-game's men there, best first by value: for LV_ROWS.
		const gameRosters: { pids: number[]; values: number[] }[] = [];
		const withAbsences = async (tid: number) => {
			let roster = rosters.get(tid)!;
			if (swaps > 0) {
				roster = [...roster];
				const n = Math.floor(Math.random() * (swaps + 1));
				for (let i = 0; i < n; i++) {
					const c = pool[Math.floor(Math.random() * pool.length)]!;
					if (!roster.includes(c)) {
						roster[Math.floor(Math.random() * roster.length)] = c;
					}
				}
				roster.sort((a, b) => b.valueNoPot - a.valueNoPot);
			}
			const there = roster.filter((_p) => Math.random() >= absent);
			const model = new Map<number, number>();
			there.forEach((p, i) => model.set(p.pid, SLOT_MINUTES[i] ?? 0));
			modelOf.push(model);
			gameRosters.push({
				pids: there.map((p) => p.pid),
				values: there.map((p) => p.valueNoPot),
			});
			return absent > 0 || swaps > 0
				? await toSimTeam(tid, inGameOrder(there))
				: sim.get(tid)!;
		};
		const played: Game[] = [];
		const started = Date.now();
		// LV_AMONG=<abbrevs>: only games among these teams (the contenders).
		const among = env.LV_AMONG ? new Set(env.LV_AMONG.split(",")) : undefined;
		const playing = among ? teams.filter((t) => among.has(t.abbrev)) : teams;
		for (const a of playing) {
			for (const b of playing) {
				if (a.tid === b.tid) {
					continue;
				}
				for (let i = 0; i < games; i++) {
					Math.random = makeSeededRandom(
						a.tid * 1_000_003 + b.tid * 1009 + i + 1,
					);
					const t0 = await withAbsences(a.tid);
					const t1 = await withAbsences(b.tid);
					const result = play(helpers.deepCopy(t0), helpers.deepCopy(t1));
					const timeline = result.timeline;
					played.push({
						tids: [a.tid, b.tid],
						timeline,
						final: result.team[0].stat.pts - result.team[1].stat.pts,
						regulation: timeline.periodEnds[numPeriods - 1]!,
					});
				}
			}
		}
		log(
			`${played.length} games in ${((Date.now() - started) / 1000).toFixed(0)} s`,
		);
		// The margin at the end of regulation, team 0's, and each team's
		// strength by least squares on it (home + s0 - s1), for how a game
		// between two teams is expected to go.
		const marginAt = (game: Game, t: number) => {
			let m = 0;
			for (const s of game.timeline.scoring) {
				if (s.t > t) {
					break;
				}
				m += s.side === 0 ? s.pts : -s.pts;
			}
			return m;
		};
		const tids = playing.map((t) => t.tid);
		const index = new Map(tids.map((tid, i) => [tid, i] as const));
		const strength = new Array(tids.length).fill(0);
		let home = 0;
		const regMargins = played.map((game) => marginAt(game, game.regulation));
		for (let iter = 0; iter < 200; iter++) {
			let homeSum = 0;
			for (const [i, game] of played.entries()) {
				const [x, y] = game.tids.map((tid) => index.get(tid)!);
				homeSum += regMargins[i]! - strength[x!] + strength[y!];
			}
			home = homeSum / played.length;
			const sums = new Array(tids.length).fill(0);
			const counts = new Array(tids.length).fill(0);
			for (const [i, game] of played.entries()) {
				const [x, y] = game.tids.map((tid) => index.get(tid)!);
				const r = regMargins[i]! - home;
				sums[x!] += r + strength[y!];
				counts[x!] += 1;
				sums[y!] += -r + strength[x!];
				counts[y!] += 1;
			}
			const mean = sums.reduce((s, v, i) => s + v / counts[i], 0) / tids.length;
			for (let i = 0; i < tids.length; i++) {
				strength[i] = sums[i] / counts[i] - mean;
			}
		}
		const driftOf = (game: Game) => {
			const [x, y] = game.tids.map((tid) => index.get(tid)!);
			return home + strength[x!] - strength[y!];
		};
		let ss = 0;
		for (const [i, game] of played.entries()) {
			ss += (regMargins[i]! - driftOf(game)) ** 2;
		}
		const regulation = played[0]!.regulation;
		const sigma = Math.sqrt(ss / played.length);
		// The spread of the margin still to come, per root second.
		const s = sigma / Math.sqrt(regulation);
		log(
			`home ${home.toFixed(2)}, team strengths ${Math.min(...strength).toFixed(1)} to ${Math.max(...strength).toFixed(1)}, sd of the regulation margin around them ${sigma.toFixed(2)}`,
		);
		const phi = (z: number) => Math.exp(-0.5 * z * z) / Math.sqrt(2 * Math.PI);
		// How much a point moves team 0's chance of winning at time t, the
		// margin m: the density at a tie of the margin to come.
		const leverage = (game: Game, t: number, m: number) => {
			const ends = game.timeline.periodEnds;
			// Regulation, or the overtime period t is in.
			let end = game.regulation;
			if (t >= game.regulation) {
				end = ends.find((e) => e > t) ?? ends.at(-1)!;
			}
			const left = Math.max(STEP, end - t);
			const drift = (driftOf(game) * left) / regulation;
			const sd = s * Math.sqrt(left);
			return phi((m + drift) / sd) / sd;
		};
		// Per place: seconds on the floor and leverage on the floor, all teams
		// and the best LV_TOP; and the games' seconds and leverage.
		const order = [...tids].sort(
			(x, y) => strength[index.get(y)!] - strength[index.get(x)!],
		);
		const top = new Set(order.slice(0, Number(env.LV_TOP ?? 8)));
		const acc = {
			all: { sec: new Array(PLACES).fill(0), lev: new Array(PLACES).fill(0) },
			top: { sec: new Array(PLACES).fill(0), lev: new Array(PLACES).fill(0) },
		};
		// LV_ROWS: a row per man per team-game - his place among those there,
		// their values, his share of the game and of its leverage.
		const rowsOut: string[] | undefined = env.LV_ROWS ? [] : undefined;
		const together = {
			all: Array.from({ length: PLACES }, () => new Array(PLACES).fill(0)),
			top: Array.from({ length: PLACES }, () => new Array(PLACES).fill(0)),
		};
		const totals = {
			all: { sec: 0, lev: 0, n: 0 },
			top: { sec: 0, lev: 0, n: 0 },
		};
		let levCheck = 0;
		let levSteps = 0;
		for (const [gi, game] of played.entries()) {
			const end = game.timeline.periodEnds.at(-1)!;
			const steps = Math.ceil(end / STEP);
			// Leverage at each step's middle.
			const lev = new Float64Array(steps);
			let k = 0;
			let m = 0;
			const scoring = game.timeline.scoring;
			for (let i = 0; i < steps; i++) {
				const t = (i + 0.5) * STEP;
				while (k < scoring.length && scoring[k]!.t <= t) {
					m += scoring[k]!.side === 0 ? scoring[k]!.pts : -scoring[k]!.pts;
					k += 1;
				}
				lev[i] = leverage(game, t, m);
				if (t < game.regulation) {
					levCheck += lev[i]!;
					levSteps += 1;
				}
			}
			let gameLev = 0;
			for (let i = 0; i < steps; i++) {
				gameLev += lev[i]! * STEP;
			}
			for (const [side, tid] of game.tids.entries()) {
				const groups = top.has(tid) ? (["all", "top"] as const) : ["all"];
				const pids = new Set(gameRosters[2 * gi + side]!.pids);
				const here = gameRosters[2 * gi + side]!;
				const placeHere = new Map(here.pids.map((pid, i) => [pid, i] as const));
				const placeIn = (pid: number) =>
					swaps > 0 ? placeHere.get(pid) : placeOf.get(pid);
				for (const [pid, share] of modelOf[2 * gi + side]!) {
					for (const g of groups) {
						modelSec[g as "all"][placeIn(pid)!] += share * end;
					}
				}
				for (const g of groups) {
					totals[g as "all"].sec += end;
					totals[g as "all"].lev += gameLev;
					totals[g as "all"].n += 1;
				}
				// Each place's seconds on the floor in each step.
				const on: (Float64Array | undefined)[] = new Array(PLACES);
				for (const { pid, spans } of game.timeline.stints) {
					if (!pids.has(pid)) {
						continue;
					}
					const place = placeIn(pid)!;
					const row = (on[place] ??= new Float64Array(steps));
					for (const [t0, t1] of spans) {
						for (
							let i = Math.floor(t0 / STEP);
							i < Math.min(steps, Math.ceil(t1 / STEP));
							i++
						) {
							const a = Math.max(t0, i * STEP);
							const b = Math.min(t1, (i + 1) * STEP);
							if (b > a) {
								row[i]! += b - a;
							}
						}
					}
				}
				for (let k = 0; k < PLACES; k++) {
					const rk = on[k];
					if (!rk) {
						if (rowsOut && k < here.pids.length) {
							rowsOut.push(
								`${gi},${side},${tid},${k},${here.values.map((v) => v.toFixed(1)).join(" ")},0,0`,
							);
						}
						continue;
					}
					let sec = 0;
					let l = 0;
					for (let i = 0; i < steps; i++) {
						sec += rk[i]!;
						l += lev[i]! * rk[i]!;
					}
					const pair = new Array(PLACES).fill(0);
					for (let j = 0; j < PLACES; j++) {
						const rj = on[j];
						if (!rj || j === k) {
							continue;
						}
						let x = 0;
						for (let i = 0; i < steps; i++) {
							x += lev[i]! * Math.min(rk[i]!, rj[i]!);
						}
						pair[j] = x;
					}
					if (rowsOut) {
						rowsOut.push(
							`${gi},${side},${tid},${k},${here.values.map((v) => v.toFixed(1)).join(" ")},${(sec / end).toFixed(5)},${(l / gameLev).toFixed(5)}`,
						);
					}
					for (const g of groups) {
						acc[g as "all"].sec[k] += sec;
						acc[g as "all"].lev[k] += l;
						for (let j = 0; j < PLACES; j++) {
							together[g as "all"][k]![j]! += pair[j];
						}
					}
				}
			}
		}
		// A point's average worth over the game's moments, against the density
		// of the final margin at a tie (the same, for a fair model).
		log(
			`average leverage ${(levCheck / levSteps).toFixed(4)} a point; density of the regulation margin at a tie ${(
				played.filter(
					(game, i) => Math.abs(regMargins[i]! + 0 * driftOf(game)) <= 1,
				).length /
				played.length /
				3
			).toFixed(4)}`,
		);
		const json: Record<string, unknown> = {};
		for (const g of ["all", "top"] as const) {
			log(
				`\n${g === "all" ? "every team" : `best ${top.size} teams`} (${totals[g].n} team-games)`,
			);
			log(
				"place | model's share | share of the game | share of the leverage | ratio",
			);
			const out: string[] = [];
			for (let p = 0; p < PLACES; p++) {
				const sec = acc[g].sec[p] / totals[g].sec;
				const lev = acc[g].lev[p] / totals[g].lev;
				out.push(lev.toFixed(4));
				log(
					`${String(p + 1).padStart(5)} | ${(modelSec[g][p] / totals[g].sec).toFixed(4)} | ${sec.toFixed(4)} | ${lev.toFixed(4)} | ${(lev / sec).toFixed(3)}`,
				);
			}
			log(`leverage shares: [${out.join(", ")}]`);
			json[g] = {
				model: modelSec[g].map((x: number) => x / totals[g].sec),
				sec: acc[g].sec.map((x: number) => x / totals[g].sec),
				lev: acc[g].lev.map((x: number) => x / totals[g].lev),
				together: together[g].map((row: number[], k: number) =>
					row.map((x, j) => (j === k ? 1 : x / acc[g].lev[k])),
				),
			};
		}
		if (rowsOut) {
			fs.writeFileSync(env.LV_ROWS!, rowsOut.join("\n") + "\n");
		}
		if (env.LV_JSON) {
			fs.writeFileSync(env.LV_JSON, JSON.stringify(json));
		}
	},
);
