import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// THE STRENGTH GAP: each team's strength as the outlook simulated its odds
// against the strength the smart CPU reads off its holdings (teamStrengths),
// and the title chance each gives this season.
//   SG_LEAGUE=<league.json> SG_OUT=<file> \
//     npx vitest --run --project basketball src/test/smart/strengthGapProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.SG_LEAGUE)(
	"strength gap probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.SG_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.SG_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const o = await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		const teams = await idb.cache.teams.getAll();
		log(
			`playoffWeight ${outlook.playoffWeight} playoffGames ${outlook.playoffGames} deadline curve ${JSON.stringify(outlook.deadline?.curve)}`,
		);
		if (env.SG_NO_BLEND) {
			outlook.playoffWeight = undefined;
			view.paths?.clear?.();
		}
		log(
			"team  W-L   | outlook s0 s1 s2 | edge | held s0 s1 s2 | sim title | curve@outlook+edge | curve@held",
		);
		for (const t of teams) {
			const ts = (
				await idb.cache.teamSeasons.indexGetAll("teamSeasonsByTidSeason", [
					[t.tid, outlook.season ?? 0],
					[t.tid, 9999],
				])
			).at(-1) as any;
			const ot = outlook.teams.get(t.tid);
			const h = view.heldNow(t.tid);
			const held = o.teamStrengths(outlook, t.tid, h.players, h.picks);
			const edge = outlook.recordEdge.get(t.tid) ?? 0;
			const c = o.curvesFor(outlook, t.tid);
			log(
				`${t.abbrev.padEnd(4)} ${ts?.won}-${ts?.lost} | ${ot.strength
					.slice(0, 3)
					.map((x: number) => x.toFixed(2))
					.join(" ")} | ${edge.toFixed(2)} | ${held
					.slice(0, 3)
					.map((x: number) => x.toFixed(2))
					.join(
						" ",
					)} | ${(100 * ot.title[0]).toFixed(1)} | ${(100 * o.curveAt(c.title[0]!, ot.strength[0]! + edge)).toFixed(1)} | ${(100 * o.curveAt(c.title[0]!, held[0]!)).toFixed(1)}`,
			);
		}
	},
);
