import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// What picks are priced at: the next draft's class as rookies, by slot, the
// next draft's picks by where their teams are expected to finish, and later
// drafts' picks - against each holder's own use for them.
//   PT_LEAGUE=<league.json> PT_OUT=<file> npx vitest --run \
//     --project basketball src/test/smart/pickTableProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PT_LEAGUE)(
	"pick table probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.PT_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.PT_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const keen: number[] = await view.keenest();
		log(
			`${g.get("season")} phase ${g.get("phase")}; keenest ${keen.map((t) => abbrev.get(t)).join(" ")}`,
		);
		const classes = view.keenestCache.classes as Map<number, number[]>;
		for (const [season, prices] of classes) {
			log(
				`class ${season} as rookies, by slot: ${prices.map((p: number, i: number) => `${i + 1}:${p.toFixed(3)}`).join(" ")}`,
			);
		}
		for (const round of [1, 2]) {
			const slots = view.slotPricesNow(round);
			log(
				`next draft's round ${round} picks by expected place: ${slots?.map((p: number, i: number) => `${i}:${p.toFixed(3)}`).join(" ")}`,
			);
		}
		// Later picks: each team's, with its own use and the floor.
		for (const t of teams.filter((t) => !t.disabled).slice(0, 30)) {
			const h = view.heldNow(t.tid);
			const rows = [];
			for (const dp of h.picks) {
				const one = { players: [], picks: [dp] };
				rows.push(
					`${dp.season} R${dp.round} ${abbrev.get(dp.originalTid)} floor ${view.pickFloorNow(dp).toFixed(3)} use ${view.useOfNow(t.tid, one).toFixed(3)} price ${view.priceNow(one, [t.tid]).toFixed(3)}`,
				);
			}
			log(
				`${t.abbrev} (${(100 * outlook.teams.get(t.tid).title[0]).toFixed(1)}%): ${rows.join(" | ")}`,
			);
		}
	},
);
