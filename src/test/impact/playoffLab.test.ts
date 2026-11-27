import { test } from "vitest";
import { PHASE } from "../../common/constants.ts";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { loadLeagueFile } from "../smart/loadLeague.ts";
import { inGameOrder, play, toSimTeam, type SimTeam } from "./simTools.ts";

// WHAT THE PLAYOFFS CHANGE IN THE SIM, team by team: each team's healthy best
// 15 plays every other team PO_GAMES times as regular season games and again
// as playoff games, on the same random numbers - where the sim's starters
// tire less and its lineup synergy counts 2.5 times as much. Each team's
// strength is fit by least squares on the final margin each way, and set
// beside the strength model's points for the roster in their parts (players,
// lineup synergy, positions), for what the model would need to price a
// playoff game.
//   PO_LEAGUE=<league.json> [PO_GAMES=40] PO_OUT=<file> [PO_JSON=<file>]
//   npx vitest --run --project basketball src/test/impact/playoffLab.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PO_LEAGUE)("playoff lab", { timeout: 7_200_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const lines: string[] = [];
	const log = (x: string) => {
		lines.push(x);
		fs.writeFileSync(env.PO_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
	};
	await loadLeagueFile(env.PO_LEAGUE!);
	const { playerLine, rosterParts } =
		await import("../../worker/core/realisticFrontOffice/strength.ts");
	const games = Number(env.PO_GAMES ?? 40);
	const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
	const healthy = (ps: Player[]) =>
		ps.map((p) => ({ ...p, injury: { type: "Healthy", gamesRemaining: 0 } }));
	const rosters = new Map<number, Player[]>();
	const sim = new Map<number, SimTeam>();
	for (const t of teams) {
		const roster = healthy(
			await idb.cache.players.indexGetAll("playersByTid", t.tid),
		)
			.sort((a, b) => b.valueNoPot - a.valueNoPot)
			.slice(0, 15);
		rosters.set(t.tid, roster);
		sim.set(t.tid, await toSimTeam(t.tid, inGameOrder(roster)));
	}
	// PO_MINUTES='[0.82, 0.75, ...]': the model's parts for each roster with
	// these shares of the game by place in SLOT_MINUTES' stead (playoff
	// minutes, say) - written to PO_JSON, no games played.
	if (env.PO_MINUTES) {
		const { SLOT_MINUTES } =
			await import("../../worker/core/realisticFrontOffice/strength.ts");
		const minutes: number[] = JSON.parse(env.PO_MINUTES);
		const saved = [...SLOT_MINUTES];
		const partsOf = () =>
			teams.map((t) => ({
				abbrev: t.abbrev,
				...rosterParts(
					rosters.get(t.tid)!.map((p) => ({
						...playerLine(p.ratings.at(-1) as any, p.ratings.at(-1)!.pos),
						valueNoPot: p.valueNoPot,
					})) as any,
				),
			}));
		const before = partsOf();
		minutes.forEach((x, k) => {
			(SLOT_MINUTES as number[])[k] = x;
		});
		const after = partsOf();
		saved.forEach((x, k) => {
			(SLOT_MINUTES as number[])[k] = x;
		});
		fs.writeFileSync(env.PO_JSON, JSON.stringify({ before, after }));
		log(`parts for ${teams.length} teams, both ways`);
		return;
	}
	const tids = teams.map((t) => t.tid);
	const index = new Map(tids.map((tid, i) => [tid, i] as const));
	// Home + s0 - s1 by least squares on the final margins.
	const fit = (rows: { x: number; y: number; m: number }[]) => {
		const strength = new Array(tids.length).fill(0);
		let home = 0;
		for (let iter = 0; iter < 200; iter++) {
			home =
				rows.reduce((s, r) => s + r.m - strength[r.x] + strength[r.y], 0) /
				rows.length;
			const sums = new Array(tids.length).fill(0);
			const counts = new Array(tids.length).fill(0);
			for (const r of rows) {
				sums[r.x] += r.m - home + strength[r.y];
				counts[r.x] += 1;
				sums[r.y] += -(r.m - home) + strength[r.x];
				counts[r.y] += 1;
			}
			const mean = sums.reduce((s, v, i) => s + v / counts[i], 0) / tids.length;
			for (let i = 0; i < tids.length; i++) {
				strength[i] = sums[i] / counts[i] - mean;
			}
		}
		return { strength, home };
	};
	const started = Date.now();
	const rows: Record<"reg" | "po", { x: number; y: number; m: number }[]> = {
		reg: [],
		po: [],
	};
	// Each pair's games each way (home first): wins and margins, for
	// PO_JSON.
	const pairs: { home: string; away: string; mode: string; m: number[] }[] = [];
	for (const mode of ["reg", "po"] as const) {
		g.setWithoutSavingToDB(
			"phase",
			mode === "po" ? PHASE.PLAYOFFS : PHASE.REGULAR_SEASON,
		);
		for (const a of teams) {
			for (const b of teams) {
				if (a.tid === b.tid) {
					continue;
				}
				const pair = {
					home: a.abbrev,
					away: b.abbrev,
					mode,
					m: [] as number[],
				};
				pairs.push(pair);
				for (let i = 0; i < games; i++) {
					Math.random = makeSeededRandom(
						a.tid * 1_000_003 + b.tid * 1009 + i + 1,
					);
					const result = play(
						helpers.deepCopy(sim.get(a.tid)!),
						helpers.deepCopy(sim.get(b.tid)!),
					);
					const m = result.team[0].stat.pts - result.team[1].stat.pts;
					pair.m.push(m);
					rows[mode].push({ x: index.get(a.tid)!, y: index.get(b.tid)!, m });
				}
			}
		}
	}
	const reg = fit(rows.reg);
	const po = fit(rows.po);
	log(
		`${rows.reg.length} games each way in ${((Date.now() - started) / 1000).toFixed(0)} s; home ${reg.home.toFixed(2)} regular, ${po.home.toFixed(2)} playoffs`,
	);
	const out = teams.map((t, i) => {
		const parts = rosterParts(
			rosters.get(t.tid)!.map((p) => ({
				...playerLine(p.ratings.at(-1) as any, p.ratings.at(-1)!.pos),
				valueNoPot: p.valueNoPot,
			})) as any,
		);
		return {
			abbrev: t.abbrev,
			reg: reg.strength[i],
			po: po.strength[i],
			...parts,
		};
	});
	// The model's parts against the league's average, as the fits are.
	const avg = (k: "total" | "players" | "synergy" | "positions") =>
		out.reduce((s, r) => s + r[k], 0) / out.length;
	const center = {
		total: avg("total"),
		players: avg("players"),
		synergy: avg("synergy"),
		positions: avg("positions"),
	};
	out.sort((a, b) => b.po - a.po);
	log(
		"team | regular | playoffs | po-reg | model | players | synergy | positions (all against the league average)",
	);
	for (const r of out) {
		log(
			`${r.abbrev.padEnd(4)} | ${r.reg.toFixed(2).padStart(6)} | ${r.po.toFixed(2).padStart(6)} | ${(r.po - r.reg).toFixed(2).padStart(5)} | ${(r.total - center.total).toFixed(2).padStart(6)} | ${(r.players - center.players).toFixed(2).padStart(6)} | ${(r.synergy - center.synergy).toFixed(2).padStart(5)} | ${(r.positions - center.positions).toFixed(2).padStart(5)}`,
		);
	}
	if (env.PO_JSON) {
		fs.writeFileSync(
			env.PO_JSON,
			JSON.stringify({ teams: out.map((r) => ({ ...r, center })), pairs }),
		);
	}
});
