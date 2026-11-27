import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { helpers } from "../../worker/util/index.ts";
import { loadLeagueFile } from "../smart/loadLeague.ts";
import { inGameOrder, play, toSimTeam, type SimTeam } from "./simTools.ts";

// WHAT A BENCH MAN DOES IN THE SIM, against what the strength model says: a
// team's healthy roster (its best 15 by value) with BS_ADD's players added
// and BS_DROP's taken off (cut back to 15 by value), each way played against
// every other team BS_GAMES times on the same random numbers - the average
// margin and the share of games won, each way, and the model's points for
// both rosters (rosterPoints, healthy). A change the model reads as x points
// should move the margin x and the games won by x times the chance a game
// comes down to a point (the density of the margin at a tie, read off the
// team's own games).
//   BS_LEAGUE=<league.json> BS_TEAM=MIL BS_ADD=<pids> [BS_DROP=<pids>]
//   [BS_GAMES=200] BS_OUT=<file> npx vitest --run --project basketball
//   src/test/impact/benchSwapLab.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);
const ids = (x: string | undefined) =>
	(x ?? "")
		.split(",")
		.filter((s) => s !== "")
		.map(Number);

test.skipIf(!env.BS_LEAGUE)(
	"bench swap lab",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.BS_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.BS_LEAGUE!);
		const { playerLine, rosterPoints, expectedRosterPoints } =
			await import("../../worker/core/realisticFrontOffice/strength.ts");
		// BS_ABSENT: each man out of each game at this chance (the strength
		// model's injuries, INJURY_SHARE) - the same men out both ways - and the
		// model's points priced with the same chance (expectedRosterPoints).
		const absent = Number(env.BS_ABSENT ?? 0);
		const healthy = (ps: Player[]) =>
			ps.map((p) => ({ ...p, injury: { type: "Healthy", gamesRemaining: 0 } }));
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		// BS_PLAYOFFS=1: the games played as playoff games (the sim's starters
		// tire less then, so its benches play less).
		if (env.BS_PLAYOFFS === "1") {
			const { PHASE } = await import("../../common/constants.ts");
			const { g } = await import("../../worker/util/index.ts");
			g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
		}
		const team = teams.find((t) => t.abbrev === env.BS_TEAM)!;
		const games = Number(env.BS_GAMES ?? 200);
		const add = ids(env.BS_ADD);
		const drop = ids(env.BS_DROP);
		const all = healthy(await idb.cache.players.getAll());
		const byPid = new Map(all.map((p) => [p.pid, p] as const));
		const best15 = (ps: Player[]) =>
			[...ps].sort((a, b) => b.valueNoPot - a.valueNoPot).slice(0, 15);
		const own = best15(all.filter((p) => p.tid === team.tid));
		const changed = best15([
			...own.filter((p) => !drop.includes(p.pid)),
			...add.map((pid) => byPid.get(pid)!),
		]);
		// BS_OPP: only these teams played (abbrevs), say the other contenders.
		const only = env.BS_OPP ? new Set(env.BS_OPP.split(",")) : undefined;
		const opponents = new Map<number, SimTeam>();
		for (const t of teams) {
			if (t.tid !== team.tid && (!only || only.has(t.abbrev))) {
				opponents.set(
					t.tid,
					await toSimTeam(
						t.tid,
						inGameOrder(
							best15(
								all.filter((p) => p.tid === t.tid && !add.includes(p.pid)),
							),
						),
					),
				);
			}
		}
		if (env.BS_LIST === "1") {
			const { playerLine: line, rosterPoints: points } =
				await import("../../worker/core/realisticFrontOffice/strength.ts");
			const rows = teams.map((t) => ({
				abbrev: t.abbrev,
				points: points(
					best15(all.filter((p) => p.tid === t.tid)).map((p) => ({
						...line(p.ratings.at(-1) as any, p.ratings.at(-1)!.pos),
						valueNoPot: p.valueNoPot,
					})) as any,
				),
			}));
			rows.sort((a, b) => b.points - a.points);
			log(rows.map((r) => `${r.abbrev} ${r.points.toFixed(1)}`).join(", "));
			return;
		}
		const lineOf = (p: Player) => ({
			...playerLine(p.ratings.at(-1) as any, p.ratings.at(-1)!.pos),
			valueNoPot: p.valueNoPot,
		});
		const model = (ps: Player[]) =>
			absent > 0
				? expectedRosterPoints(
						ps.map((p) => ({ ...lineOf(p), availability: 1 })) as any,
						1 - absent,
					)
				: rosterPoints(ps.map(lineOf) as any);
		const simulate = async (roster: Player[]) => {
			const sim =
				absent > 0 ? undefined : await toSimTeam(team.tid, inGameOrder(roster));
			const margins: number[] = [];
			const won: number[] = [];
			const minutes = new Map<number, number>();
			for (const [oppTid, opp] of opponents) {
				for (let i = 0; i < games; i++) {
					// Who is out: drawn by player, the same both ways.
					let team0 = sim;
					if (absent > 0) {
						const out = makeSeededRandom(oppTid * 7919 + i + 3);
						const outs = new Set(
							[
								...all.filter((p) => p.tid === team.tid),
								...add.map((pid) => byPid.get(pid)!),
							]
								.sort((x, y) => x.pid - y.pid)
								.filter(() => out() < absent)
								.map((p) => p.pid),
						);
						team0 = await toSimTeam(
							team.tid,
							inGameOrder(roster.filter((p) => !outs.has(p.pid))),
						);
					}
					Math.random = makeSeededRandom(oppTid * 1_000_003 + i + 7);
					const home = i % 2 === 0;
					const result = home
						? play(helpers.deepCopy(team0!), helpers.deepCopy(opp))
						: play(helpers.deepCopy(opp), helpers.deepCopy(team0!));
					const k = home ? 0 : 1;
					const m = result.team[k].stat.pts - result.team[1 - k].stat.pts;
					margins.push(m);
					won.push(m > 0 ? 1 : 0);
					for (const p of result.team[k].player) {
						minutes.set(p.id, (minutes.get(p.id) ?? 0) + (p.stat.min ?? 0));
					}
				}
			}
			return { margins, won, minutes };
		};
		const name = (p: Player) =>
			`${p.firstName} ${p.lastName} ${p.ratings.at(-1)!.ovr} (${p.valueNoPot.toFixed(1)})`;
		log(
			`${team.abbrev}: adds [${add.map((pid) => name(byPid.get(pid)!)).join("; ")}], drops [${drop.map((pid) => name(byPid.get(pid)!)).join("; ")}]`,
		);
		log(`  before: ${own.map((p) => p.valueNoPot.toFixed(0)).join(" ")}`);
		log(`  after:  ${changed.map((p) => p.valueNoPot.toFixed(0)).join(" ")}`);
		const a = await simulate(own);
		const b = await simulate(changed);
		const n = a.margins.length;
		const mean = (x: number[]) => x.reduce((s, v) => s + v, 0) / x.length;
		const diffs = b.margins.map((m, i) => m - a.margins[i]!);
		const sd = Math.sqrt(
			diffs.reduce((s, d) => s + (d - mean(diffs)) ** 2, 0) / (n - 1),
		);
		const winDiffs = b.won.map((w, i) => w - a.won[i]!);
		const winSd = Math.sqrt(
			winDiffs.reduce((s, d) => s + (d - mean(winDiffs)) ** 2, 0) / (n - 1),
		);
		// The chance a game comes down to a point: the team's games' margins
		// within half a point of a tie either way, read as a density.
		const near = [...a.margins, ...b.margins].filter(
			(m) => Math.abs(m) <= 2,
		).length;
		const density = near / (2 * n) / 4;
		const dModel = model(changed) - model(own);
		log(
			`  model: ${dModel.toFixed(3)} points; sim margin ${mean(diffs).toFixed(3)} ± ${(sd / Math.sqrt(n)).toFixed(3)}; games won ${(100 * mean(a.won)).toFixed(2)}% -> ${(100 * mean(b.won)).toFixed(2)}% (${(100 * mean(winDiffs)).toFixed(2)} ± ${((100 * winSd) / Math.sqrt(n)).toFixed(2)}), a point is ${(100 * density).toFixed(2)}% of a game here, so the wins are ${(mean(winDiffs) / density).toFixed(3)} ± ${(winSd / Math.sqrt(n) / density).toFixed(3)} points; ${n} games each`,
		);
		const minutesOf = (x: typeof a, ps: Player[]) =>
			ps
				.map((p) => `${((x.minutes.get(p.pid) ?? 0) / n).toFixed(1)}`)
				.join(" ");
		log(`  minutes before: ${minutesOf(a, own)}`);
		log(`  minutes after:  ${minutesOf(b, changed)}`);
	},
);
