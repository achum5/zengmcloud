import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE DRAFT, PICK BY PICK: a league file at its draft, the draft run, and for
// each of the first DB_PICKS picks the prospects the team weighed, best first
// by what each is worth to it - his use to its own goal, the market's price
// for him (the second-keenest team's use), and his place on the board (by
// value) - to see why bad teams take older players with less upside.
//   DB_LEAGUE=<league.json> DB_OUT=<file> [DB_PICKS=14] [DB_TOP=6] \
//     npx vitest --run --project basketball src/test/smart/draftBoardProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.DB_LEAGUE)(
	"draft board probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.DB_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.DB_LEAGUE!);
		const { league, draft } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { PLAYER } = await import("../../common/constants.ts");
		const { smartDraftWatchers } =
			await import("../../worker/core/smartCpu/draft.ts");
		const { projectPlayer, retentionOf } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const season = g.get("season");
		const prospects = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
		).filter((p) => p.draft.year === season);
		const board = new Map(
			[...prospects]
				.sort((a, b) => b.value - a.value)
				.map((p, i) => [p.pid, i + 1]),
		);
		const most = Number(env.DB_PICKS ?? 14);
		const top = Number(env.DB_TOP ?? 6);
		let n = 0;
		smartDraftWatchers.push((dp, worths, view: any) => {
			n += 1;
			if (n > most) {
				return;
			}
			const title = (
				100 * (view.outlook.teams.get(dp.tid)?.title?.[0] ?? 0)
			).toFixed(1);
			log(
				`\n#${n} ${abbrev.get(dp.tid)} (${title}% title): takes ${worths[0]?.p.firstName} ${worths[0]?.p.lastName}`,
			);
			for (const { p, worth, drafted } of worths.slice(0, top)) {
				const a = { players: [drafted], picks: [] };
				const use = view.useOfNow(dp.tid, a);
				const price = view.priceNow(a, [dp.tid]);
				const r = p.ratings.at(-1)!;
				const proj = projectPlayer(p, season);
				const path = [1, 3, 5, 7].map((y) => proj.ovr[y]!.toFixed(0)).join(" ");
				log(
					`  ${`${p.firstName} ${p.lastName}`.padEnd(22)} ${season - p.born.year}y ${r.ovr}/${r.pot} board #${board.get(p.pid)} | proj +1/+3/+5/+7 ${path} | stay ${retentionOf(view.outlook, p.pid, dp.tid).toFixed(2)} | use ${use.toFixed(4)} price ${price.toFixed(4)} worth ${worth.toFixed(4)}`,
				);
			}
		});
		await draft.runPicks({ type: "untilEnd" }, {});
		log(`\nthe first ${most} picks against the board:`);
		const picked = (
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
		)
			.filter((p) => p.draft.year === season && p.draft.round === 1)
			.sort((a, b) => a.draft.pick - b.draft.pick);
		for (const p of picked.slice(0, most)) {
			const r = p.ratings.at(-1)!;
			log(
				`  #${p.draft.pick} ${abbrev.get(p.tid)}: ${p.firstName} ${p.lastName} ${season - p.born.year}y ${r.ovr}/${r.pot} (board #${board.get(p.pid)})`,
			);
		}
	},
);
