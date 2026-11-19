import { test } from "vitest";
import type { Player } from "../../common/types.ts";
import { IMPACT_RATINGS } from "../../worker/core/realisticFrontOffice/playerImpact.ts";
import {
	getReplacement,
	playerLine,
	REPLACEMENT_RATINGS,
	ROSTER_PLACES,
	rosterParts,
	rosterPoints,
	strengthSettings,
	type StrengthPlayer,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { idb } from "../../worker/db/index.ts";
import { loadLeagueFile } from "../smart/loadLeague.ts";

// THE REFIT LAB: the removal and swap labs' changes, played in the sim, as
// rows of a regression on the roster model's coefficients (strength.ts,
// MODEL). Once the pecking order is set a roster's points are linear in
// each player's roleFree and offense - his place's minutes on the one, and
// those minutes times his share of the shots on the other - and those are
// linear in the coefficients; so each roster is written out as its players'
// weights and ratings, with its synergy and positions parts, under the
// model as it is fit, believed in full.
//   RF_RUNS='[{"league":"<league.json>","removals":[...],"swaps":[...]}]' \
//     RF_OUT=<file> npx vitest --run --project basketball \
//     src/test/impact/refitLab.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.RF_RUNS)("refit lab", { timeout: 3_600_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const runs: { league: string; removals: string[]; swaps: string[] }[] =
		JSON.parse(env.RF_RUNS!);
	const before = { ...strengthSettings };
	const raw = { profile: 1, profileSynergy: false, supportTails: true };
	Object.assign(strengthSettings, raw);
	const lineOf = (p: Player): StrengthPlayer => ({
		...playerLine(
			p.ratings.at(-1) as unknown as ValueRatings,
			p.ratings.at(-1)!.pos,
		),
		valueNoPot: p.valueNoPot,
	});
	const out: any[] = [];
	try {
		for (const run of runs) {
			await loadLeagueFile(run.league);
			const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
			const rosters = new Map<number, Player[]>();
			for (const t of teams) {
				rosters.set(
					t.tid,
					await idb.cache.players.indexGetAll("playersByTid", t.tid),
				);
			}
			const all = new Map(
				[...rosters.values()].flat().map((p) => [p.pid, p] as const),
			);
			const described = new Map<string, any>();
			const describe = (ps: Player[]) => {
				const key = ps
					.map((p) => p.pid)
					.sort((a, b) => a - b)
					.join(",");
				let d = described.get(key);
				if (d) {
					return d;
				}
				// Places nobody holds are filled with a minimum player: written
				// out as one, his line the model's too.
				const fill = Math.max(0, ROSTER_PLACES - ps.length);
				const lines = [
					...ps.map(lineOf),
					...Array.from({ length: fill }, () => ({ ...getReplacement() })),
				];
				const parts = rosterParts(lines);
				const players = lines.map((line, i) => {
					const bump = (field: "roleFree" | "offense") => {
						const changed = [...lines];
						changed[i] = { ...line, [field]: line[field] + 1 };
						return rosterPoints(changed) - parts.total;
					};
					const r = (
						i < ps.length ? ps[i]!.ratings.at(-1)! : REPLACEMENT_RATINGS
					) as any;
					return {
						pid: i < ps.length ? ps[i]!.pid : -1,
						m: bump("roleFree"),
						s: bump("offense"),
						ovr: r.ovr,
						r: IMPACT_RATINGS.map((k) => r[k]),
						roleFree: line.roleFree,
						offense: line.offense,
					};
				});
				// The roster's points under the model as shipped, to compare.
				Object.assign(strengthSettings, before);
				const shipped = rosterPoints([
					...ps.map(lineOf),
					...Array.from({ length: fill }, () => ({ ...getReplacement() })),
				]);
				Object.assign(strengthSettings, raw);
				d = {
					shipped,
					total: parts.total,
					players: parts.players,
					synergy: parts.synergy,
					positions: parts.positions,
					weights: players.filter((x) => x.m !== 0 || x.s !== 0),
				};
				described.set(key, d);
				return d;
			};
			const rows: any[] = [];
			for (const file of run.removals) {
				for (const x of JSON.parse(fs.readFileSync(file, "utf8"))) {
					const base = rosters.get(x.tid)!;
					rows.push({
						kind: "removal",
						tid: x.tid,
						out: x.pid,
						sim: x.sim,
						se: x.se,
						base: describe(base),
						after: describe(base.filter((p) => p.pid !== x.pid)),
					});
				}
			}
			for (const file of run.swaps) {
				for (const x of JSON.parse(fs.readFileSync(file, "utf8"))) {
					const base = rosters.get(x.tid)!;
					rows.push({
						kind: x.same ? "swap same" : "swap cross",
						tid: x.tid,
						out: x.out.pid,
						in: x.in.pid,
						sim: x.sim,
						se: x.se,
						base: describe(base),
						after: describe([
							...base.filter((p) => p.pid !== x.out.pid),
							all.get(x.in.pid)!,
						]),
					});
				}
			}
			out.push({ league: run.league, ratings: IMPACT_RATINGS, rows });
		}
	} finally {
		Object.assign(strengthSettings, before);
	}
	fs.writeFileSync(env.RF_OUT!, JSON.stringify(out));
});
