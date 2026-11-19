import { test } from "vitest";
import type { Player } from "../../common/types.ts";
import {
	playerLine,
	rosterPoints,
	strengthSettings,
	type StrengthPlayer,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { idb } from "../../worker/db/index.ts";
import { loadLeagueFile } from "../smart/loadLeague.ts";

// THE PROFILE LAB: how much of what sets a player apart from a typical one
// of his overall the roster model should believe (strength.ts, believed).
// The removal lab's and the swap lab's rows, already played in the sim,
// priced again by the model at each share believed, for each league file:
//   PL_RUNS='[{"league":"<league.json>","removals":["<removal.json>"],
//     "swaps":["<swaps.json>"]}]' PL_OUT=<file> npx vitest --run \
//     --project basketball src/test/impact/profileLab.test.ts
// Rows out: each change's sim margin, its noise, and the model's change at
// each setting.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

// PL_SETTINGS: the settings to try, as JSON (strengthSettings' fields).
const SETTINGS: Record<string, number | boolean>[] = env.PL_SETTINGS
	? JSON.parse(env.PL_SETTINGS)
	: [1, 0.9, 0.8, 0.7, 0.6].flatMap((profile) =>
			[false, true].map((profileSynergy) => ({ profile, profileSynergy })),
		);

test.skipIf(!env.PL_RUNS)("profile lab", { timeout: 3_600_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const runs: { league: string; removals: string[]; swaps: string[] }[] =
		JSON.parse(env.PL_RUNS!);
	const out: any[] = [];
	const lineOf = (p: Player): StrengthPlayer => ({
		...playerLine(
			p.ratings.at(-1) as unknown as ValueRatings,
			p.ratings.at(-1)!.pos,
		),
		valueNoPot: p.valueNoPot,
	});
	const before = { ...strengthSettings };
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
			const changes: {
				kind: string;
				sim: number;
				se: number;
				base: Player[];
				after: Player[];
			}[] = [];
			for (const file of run.removals) {
				for (const r of JSON.parse(fs.readFileSync(file, "utf8"))) {
					const base = rosters.get(r.tid)!;
					changes.push({
						kind: "removal",
						sim: r.sim,
						se: r.se,
						base,
						after: base.filter((p) => p.pid !== r.pid),
					});
				}
			}
			for (const file of run.swaps) {
				for (const r of JSON.parse(fs.readFileSync(file, "utf8"))) {
					const base = rosters.get(r.tid)!;
					changes.push({
						kind: r.same ? "swap same" : "swap cross",
						sim: r.sim,
						se: r.se,
						base,
						after: [
							...base.filter((p) => p.pid !== r.out.pid),
							all.get(r.in.pid)!,
						],
					});
				}
			}
			const rows = changes.map((c) => ({
				kind: c.kind,
				sim: c.sim,
				se: c.se,
				model: [] as number[],
			}));
			for (const setting of SETTINGS) {
				Object.assign(strengthSettings, setting);
				const points = new Map<Player[], number>();
				for (const [i, c] of changes.entries()) {
					let base = points.get(c.base);
					if (base === undefined) {
						base = rosterPoints(c.base.map(lineOf));
						points.set(c.base, base);
					}
					rows[i]!.model.push(rosterPoints(c.after.map(lineOf)) - base);
				}
			}
			out.push({ league: run.league, settings: SETTINGS, rows });
		}
	} finally {
		Object.assign(strengthSettings, before);
	}
	fs.writeFileSync(env.PL_OUT!, JSON.stringify(out));
});
