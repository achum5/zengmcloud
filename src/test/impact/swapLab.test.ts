import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { helpers } from "../../worker/util/index.ts";
import { loadLeagueFile } from "../smart/loadLeague.ts";
import { inGameOrder, play, toSimTeam, type SimTeam } from "./simTools.ts";

// THE SWAP LAB: what a trade did to a team in the game's own sim. On a real
// league file, the team's roster with SW_OUT swapped for SW_IN (and the
// other way round, SW_UNDO) plays every other team as it stands, the same
// games on the same random numbers both ways - its average margin with
// each, against what the smart CPU's strength model said the swap was worth.
//   SW_LEAGUE=<league.json> SW_OUT_FILE=<file> SW_TID=<tid> SW_HAS=<pids> \
//     SW_HAD=<pids> [SW_GAMES=60] npx vitest --run --project basketball \
//     src/test/impact/swapLab.test.ts
// SW_HAS: the players it holds now from the trade; SW_HAD: those it gave.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);
const ids = (x: string | undefined) =>
	(x ?? "")
		.split(",")
		.filter((s) => s !== "")
		.map(Number);

// SW_SURVEY=1: instead, for every team from SW_FROM to SW_TO, each of its
// rotation players ranked 4th to 9th swapped for a player of another team
// within 2 of his overall, one at his position group and one across it,
// each swap's sim margin against the model's (rosterPoints), rows to
// SW_OUT_FILE as JSON.
const lineOf = async (p: Player) => {
	const { playerLine } =
		await import("../../worker/core/realisticFrontOffice/strength.ts");
	return {
		...playerLine(p.ratings.at(-1) as any, p.ratings.at(-1)!.pos),
		valueNoPot: p.valueNoPot,
	};
};

test.skipIf(!env.SW_LEAGUE)("swap lab", { timeout: 7_200_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const lines: string[] = [];
	const log = (x: string) => {
		lines.push(x);
		fs.writeFileSync(env.SW_OUT_FILE ?? "/dev/stdout", lines.join("\n") + "\n");
	};
	await loadLeagueFile(env.SW_LEAGUE!);
	const realRandom = Math.random;
	const tid = Number(env.SW_TID);
	const has = ids(env.SW_HAS);
	const had = ids(env.SW_HAD);
	const games = Number(env.SW_GAMES ?? 60);
	const healthy = (ps: Player[]) =>
		ps.map((p) => ({ ...p, injury: { type: "Healthy", gamesRemaining: 0 } }));
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const rosters = new Map<number, Player[]>();
	for (const t of teams) {
		rosters.set(
			t.tid,
			healthy(await idb.cache.players.indexGetAll("playersByTid", t.tid)),
		);
	}
	const all = new Map(
		[...rosters.values()].flat().map((p) => [p.pid, p] as const),
	);
	if (env.SW_SURVEY) {
		const { rosterPoints } =
			await import("../../worker/core/realisticFrontOffice/strength.ts");
		const group = (p: Player) =>
			p.ratings.at(-1)!.pos.includes("G") ? "G" : "F";
		const others = [...rosters.entries()];
		const rows: any[] = [];
		const lineCache = new Map<number, any>();
		const linesOf = async (ps: Player[]) => {
			const out = [];
			for (const p of ps) {
				let l = lineCache.get(p.pid);
				if (!l) {
					l = await lineOf(p);
					lineCache.set(p.pid, l);
				}
				out.push(l);
			}
			return out;
		};
		const allOpp = new Map<number, SimTeam>();
		for (const t of teams) {
			allOpp.set(
				t.tid,
				await toSimTeam(t.tid, inGameOrder(rosters.get(t.tid)!)),
			);
		}
		const simulateVs = async (team0: number, roster: Player[]) => {
			const team = await toSimTeam(team0, inGameOrder(roster));
			const margins: number[] = [];
			for (const [oppTid, opp] of allOpp) {
				if (oppTid === team0) {
					continue;
				}
				for (let i = 0; i < games; i++) {
					Math.random = makeSeededRandom(oppTid * 1_000_003 + i);
					const home = i % 2 === 0;
					const result = home
						? play(helpers.deepCopy(team), helpers.deepCopy(opp))
						: play(helpers.deepCopy(opp), helpers.deepCopy(team));
					const k = home ? 0 : 1;
					margins.push(result.team[k].stat.pts - result.team[1 - k].stat.pts);
				}
			}
			return margins;
		};
		const from = Number(env.SW_FROM ?? 0);
		const to = Number(env.SW_TO ?? teams.length);
		try {
			for (const t of teams.slice(from, to)) {
				const base = rosters.get(t.tid)!;
				const baseModel = rosterPoints(await linesOf(base));
				const baseSim = await simulateVs(t.tid, base);
				const ranked = [...base].sort((a, b) => b.valueNoPot - a.valueNoPot);
				for (const p of ranked.slice(3, 9)) {
					const ovr = p.ratings.at(-1)!.ovr;
					const pool = others
						.filter(([k]) => k !== t.tid)
						.flatMap(([, ps]) => ps)
						.filter((x) => Math.abs(x.ratings.at(-1)!.ovr - ovr) <= 2)
						.sort((a, b) => a.pid - b.pid);
					for (const same of [true, false]) {
						const cands = pool.filter((x) => (group(x) === group(p)) === same);
						if (cands.length === 0) {
							continue;
						}
						const x = cands[(p.pid * 7 + t.tid) % cands.length]!;
						const roster = [...base.filter((y) => y !== p), x];
						const model = rosterPoints(await linesOf(roster)) - baseModel;
						const sim = await simulateVs(t.tid, roster);
						const diffs = sim.map((m, i) => m - baseSim[i]!);
						const mean = diffs.reduce((a, d) => a + d, 0) / diffs.length;
						const sd = Math.sqrt(
							diffs.reduce((a, d) => a + (d - mean) ** 2, 0) /
								(diffs.length - 1),
						);
						rows.push({
							tid: t.tid,
							out: {
								pid: p.pid,
								pos: p.ratings.at(-1)!.pos,
								ovr,
								ratings: p.ratings.at(-1),
							},
							in: {
								pid: x.pid,
								pos: x.ratings.at(-1)!.pos,
								ovr: x.ratings.at(-1)!.ovr,
								ratings: x.ratings.at(-1),
							},
							same,
							model,
							sim: mean,
							se: sd / Math.sqrt(diffs.length),
						});
						fs.writeFileSync(env.SW_OUT_FILE!, JSON.stringify(rows));
					}
				}
			}
		} finally {
			Math.random = realRandom;
		}
		return;
	}
	const now = rosters.get(tid)!;
	const before = [
		...now.filter((p) => !has.includes(p.pid)),
		...had.map((pid) => all.get(pid)!),
	];
	const opponents = new Map<number, SimTeam>();
	for (const t of teams) {
		if (t.tid !== tid) {
			opponents.set(
				t.tid,
				await toSimTeam(
					t.tid,
					inGameOrder(rosters.get(t.tid)!.filter((p) => !had.includes(p.pid))),
				),
			);
		}
	}
	const simulate = async (roster: Player[]) => {
		const team = await toSimTeam(tid, inGameOrder(roster));
		let total = 0;
		let n = 0;
		for (const [oppTid, opp] of opponents) {
			for (let i = 0; i < games; i++) {
				Math.random = makeSeededRandom(oppTid * 1_000_003 + i);
				const home = i % 2 === 0;
				const result = home
					? play(helpers.deepCopy(team), helpers.deepCopy(opp))
					: play(helpers.deepCopy(opp), helpers.deepCopy(team));
				const k = home ? 0 : 1;
				total += result.team[k].stat.pts - result.team[1 - k].stat.pts;
				n += 1;
			}
		}
		return { margin: total / n, n };
	};
	const name = (p: Player) =>
		`${p.firstName} ${p.lastName} ${p.ratings.at(-1)!.pos} ${p.ratings.at(-1)!.ovr}`;
	log(
		`tid ${tid}: has [${has.map((pid) => name(all.get(pid)!)).join("; ")}], gave [${had.map((pid) => name(all.get(pid)!)).join("; ")}], ${games} games against each other team`,
	);
	try {
		const a = await simulate(before);
		const b = await simulate(now);
		log(
			`average margin without the trade ${a.margin.toFixed(2)}, with it ${b.margin.toFixed(2)}: the trade did ${(b.margin - a.margin).toFixed(2)} points a game (${a.n} games each)`,
		);
		// The same split in halves, to see how settled it is.
	} finally {
		Math.random = realRandom;
	}
});
