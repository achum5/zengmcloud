import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// How deep a team's roster is projected each season of the outlook: the
// players it is expected to have (each with the chance he is there), as
// overalls, and how many places are left to the minimum player (43).
//   RD_LEAGUE=<league.json> RD_OUT=<file> RD_TEAMS=DAL,DET npx vitest --run \
//     --project basketball src/test/smart/rosterDepthProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.RD_LEAGUE)(
	"roster depth probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.RD_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.RD_LEAGUE!);
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
		for (const ab of (env.RD_TEAMS ?? "DAL,DET").split(",")) {
			const t = teams.find((x) => x.abbrev === ab)!;
			const h = view.heldNow(t.tid);
			log(
				`\n${ab} (${(100 * outlook.teams.get(t.tid).title[0]).toFixed(1)}% title)`,
			);
			o.teamStrengths(
				outlook,
				t.tid,
				h.players,
				h.picks,
				undefined,
				(y: number, room: number, signings: readonly any[]) => {
					log(
						`  y${y} room ${(100 * room).toFixed(0)}%: signings ${signings.map((s) => `${s.valueNoPot.toFixed(0)}@${s.availability.toFixed(2)}`).join(" ")}`,
					);
				},
				undefined,
				undefined,
				(y: number, roster: readonly any[]) => {
					const expected = roster.reduce(
						(a, p) => a + Math.min(1, p.availability),
						0,
					);
					const top = [...roster]
						.sort((a, b) => b.valueNoPot - a.valueNoPot)
						.map(
							(p) =>
								`${p.valueNoPot.toFixed(0)}@${Math.min(1, p.availability).toFixed(2)}`,
						);
					log(
						`  y${y} expected players ${expected.toFixed(1)}: ${top.join(" ")}`,
					);
				},
			);
		}
		void g;
	},
);
