import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// WHAT CONTENDERS PUT ON FRINGE PLAYERS (docs/smart-cpu.md, "Bench players by
// the minutes they would get"): for the league's top teams by title odds,
// every player rated FR_MAX or less (on another team or a free agent) priced
// by his use to the team, the best few each with where his use comes from,
// season by season - his projected value, how many of the team's expected
// players stand ahead of him, the places left to the minimum player, and the
// team's strength and season value with and without him.
//   FR_LEAGUE=<league.json | arena:<lid>> FR_OUT=<file> [FR_TEAMS=3]
//   [FR_MAX=50] [FR_SHOW=6] npx vitest --run --project basketball
//   src/test/smart/fringeProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.FR_LEAGUE)(
	"fringe probe",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.FR_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.FR_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			smartCpu: true,
			realisticFrontOffices: false,
			...(env.FR_SPECTATOR ? { spectator: true } : {}),
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { g } = await import("../../worker/util/index.ts");
		const o = await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { SLOT_MINUTES } =
			await import("../../worker/core/realisticFrontOffice/strength.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		const season = g.get("season");
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev] as const));
		const contenders = [...outlook.teams.entries()]
			.sort((a: any, b: any) => b[1].title[0] - a[1].title[0])
			.slice(0, Number(env.FR_TEAMS ?? 3));
		const max = Number(env.FR_MAX ?? 50);
		const all = (await idb.cache.players.getAll()).filter(
			(p) => p.tid >= -1 && p.ratings.at(-1)!.ovr <= max,
		);
		log(
			`${season} phase ${g.get("phase")}: ${all.length} players rated ${max} or less`,
		);
		const describe = (p: any) =>
			`${p.firstName} ${p.lastName} (${abbrev.get(p.tid) ?? "FA"}, ${season - p.born.year}, ${p.ratings.at(-1).ovr}/${p.ratings.at(-1).pot}, $${(contractOf(p).amount / 1000).toFixed(1)}M to ${contractOf(p).exp})`;
		// Everyone the team is expected to have each season (with the chance he is
		// there), and the strengths, for this roster.
		const seasonsOf = (tid: number, players: any[], picks: any[]) => {
			const rosters: any[][] = [];
			const strengths = o.teamStrengths(
				outlook,
				tid,
				players,
				picks,
				undefined,
				undefined,
				undefined,
				undefined,
				(y: number, roster: readonly any[]) => {
					rosters[y] = [...roster];
				},
			);
			return { rosters, strengths };
		};
		const parts = (line: any) =>
			line.halves
				? line.halves.map((h: any) => ({
						v: h.valueNoPot,
						a: Math.min(1, h.availability),
					}))
				: [{ v: line.valueNoPot, a: Math.min(1, line.availability) }];
		const summary: any[] = [];
		for (const [tid, t] of contenders as any) {
			const h = view.heldNow(tid);
			const base = seasonsOf(tid, h.players, h.picks);
			log(
				`\n== ${abbrev.get(tid)}: ${(100 * t.title[0]).toFixed(1)}% title now, ${h.players.length} players`,
			);
			const own = [...h.players]
				.sort((a: any, b: any) => b.valueNoPot - a.valueNoPot)
				.map((p: any) => `${p.ratings.at(-1).ovr}`);
			log(`  roster by value: ${own.join(" ")}`);
			for (let y = 0; y <= 4; y++) {
				const roster = base.rosters[y] ?? [];
				const ps = roster
					.flatMap(parts)
					.filter((x: any) => x.a > 0)
					.sort((a: any, b: any) => b.v - a.v);
				const expected = ps.reduce((s: number, x: any) => s + x.a, 0);
				log(
					`  y${y} expected ${expected.toFixed(1)}: ${ps.map((x: any) => `${x.v.toFixed(0)}@${x.a.toFixed(2)}`).join(" ")}`,
				);
			}
			const priced: { p: any; use: number }[] = [];
			const started = Date.now();
			for (const p of all) {
				if (p.tid === tid) {
					continue;
				}
				const use = view.useOfNow(tid, { players: [p], picks: [] });
				priced.push({ p, use });
			}
			priced.sort((a, b) => b.use - a.use);
			void base;
			const uses = priced.map((x) => x.use);
			const q = (f: number) => uses[Math.floor(f * (uses.length - 1))]!;
			log(
				`  ${priced.length} priced in ${((Date.now() - started) / 1000).toFixed(0)} s: best ${q(0).toFixed(4)}, 10th ${uses[9]?.toFixed(4)}, 50th ${uses[49]?.toFixed(4)}, median ${q(0.5).toFixed(4)}; over 0.01: ${uses.filter((u) => u > 0.01).length}, over 0.03: ${uses.filter((u) => u > 0.03).length}`,
			);
			const show = Number(env.FR_SHOW ?? 6);
			const older = priced.filter((x) => season - x.p.born.year >= 24);
			log(
				`  aged 24+: ${older.length}, best ${older[0]?.use.toFixed(4)}, 10th ${older[9]?.use.toFixed(4)}; over 0.01: ${older.filter((x) => x.use > 0.01).length}`,
			);
			for (const { p, use } of [
				...priced.slice(0, show),
				...older.slice(0, show),
			]) {
				const withHim = seasonsOf(tid, [...h.players, p], h.picks);
				log(`  ${describe(p)}: use ${use.toFixed(4)}`);
				log(
					"    y | his value (halves@chance) | ahead of him | min places | minutes | strength with-without | season value diff",
				);
				let totalDiff = 0;
				for (let y = 0; y < withHim.strengths.length; y++) {
					const roster = withHim.rosters[y] ?? [];
					// His line: his players' place, after the team's own.
					const at = h.players.length;
					const hisParts = roster[at] ? parts(roster[at]) : [];
					const others = roster
						.filter((_l: any, i: number) => i !== at)
						.flatMap(parts);
					const expected = others.reduce((s: number, x: any) => s + x.a, 0);
					const ahead = hisParts.map((hp: any) =>
						others
							.filter((x: any) => x.v > hp.v)
							.reduce((s: number, x: any) => s + x.a, 0),
					);
					const minutes = hisParts.reduce(
						(s: number, hp: any, i: number) =>
							s + hp.a * (SLOT_MINUTES[Math.round(ahead[i]!)] ?? 0),
						0,
					);
					const dv =
						o.seasonValue(outlook, y, withHim.strengths[y]!, tid) -
						o.seasonValue(outlook, y, base.strengths[y]!, tid);
					totalDiff += dv;
					log(
						`   ${String(y).padStart(2)} | ${hisParts.map((x: any) => `${x.v.toFixed(0)}@${x.a.toFixed(2)}`).join(" ") || "-"} | ${ahead.map((x: number) => x.toFixed(1)).join(" ")} | ${Math.max(0, 15 - expected).toFixed(1)} | ${(100 * minutes).toFixed(1)}% | ${(withHim.strengths[y]! - base.strengths[y]!).toFixed(3)} | ${dv.toFixed(4)}`,
					);
				}
				log(`    sum of season values ${totalDiff.toFixed(4)}`);
				summary.push({
					team: abbrev.get(tid),
					pid: p.pid,
					name: `${p.firstName} ${p.lastName}`,
					age: season - p.born.year,
					ovr: p.ratings.at(-1).ovr,
					pot: p.ratings.at(-1).pot,
					use,
				});
			}
		}
		if (env.FR_JSON) {
			fs.writeFileSync(env.FR_JSON, JSON.stringify(summary, null, 1));
		}
	},
);
