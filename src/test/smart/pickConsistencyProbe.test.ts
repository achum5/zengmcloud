import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// Are picks priced as what they turn into? For the players a league's own
// drafts took one to six seasons ago, the market's price for each now, by
// the slot he went at - against the next draft's price for that slot, grown
// back for the seasons since at the GM's patience. A pick priced right is
// worth, on average, what its player is later, discounted for the wait.
//   PC_LEAGUE=<league.json> PC_OUT=<file> npx vitest --run \
//     --project basketball src/test/smart/pickConsistencyProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PC_LEAGUE)(
	"pick consistency probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		await loadLeagueFile(env.PC_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { currentOutlook, PATIENCE } =
			await import("../../worker/core/realisticFrontOffice/market.ts").then(
				async (m) => ({
					...m,
					PATIENCE: (
						await import("../../worker/core/realisticFrontOffice/outlook.ts")
					).PATIENCE,
				}),
			);
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		await view.classPrices();
		const season = g.get("season");
		const numTeams = g.get("numTeams");
		const classes = view.keenestCache?.classes as Map<number, number[]>;
		const next = [...(classes?.keys() ?? [])].sort((a, b) => a - b)[0]!;
		const slotPrice = classes.get(next)!;
		const players = await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		]);
		const rows: any[] = [];
		for (const p of players) {
			const dr = p.draft;
			if (!dr || dr.round < 1 || dr.pick <= 0) {
				continue;
			}
			const years = season - dr.year;
			if (years < 1 || years > 6) {
				continue;
			}
			const slot = (dr.round - 1) * numTeams + dr.pick;
			const price = view.priceNow({ players: [p], picks: [] }, [p.tid]);
			rows.push({
				slot,
				years,
				price,
				slotNow: slotPrice[slot - 1] ?? 0,
				ovr: p.ratings.at(-1)!.ovr,
				age: season - p.born.year,
			});
		}
		const lines: string[] = [
			`${season} phase ${g.get("phase")}, next draft ${next}, patience ${PATIENCE}`,
		];
		for (const [lo, hi] of [
			[1, 5],
			[6, 10],
			[11, 20],
			[21, 30],
			[31, 45],
			[46, 64],
		]) {
			for (const yrs of [
				[1, 2],
				[3, 4],
				[5, 6],
			]) {
				const sel = rows.filter(
					(r) =>
						r.slot >= lo! &&
						r.slot <= hi! &&
						r.years >= yrs[0]! &&
						r.years <= yrs[1]!,
				);
				if (sel.length === 0) {
					continue;
				}
				const later =
					sel.reduce((a, r) => a + r.price * PATIENCE ** r.years, 0) /
					sel.length;
				const now = sel.reduce((a, r) => a + r.slotNow, 0) / sel.length;
				lines.push(
					`picks ${lo}-${hi}, drafted ${yrs[0]}-${yrs[1]} seasons ago: n ${sel.length}, their price now discounted back ${later.toFixed(3)}, the slot's price in the next draft ${now.toFixed(3)} (ratio ${(later / Math.max(1e-6, now)).toFixed(2)})`,
				);
			}
		}
		fs.writeFileSync(env.PC_OUT!, lines.join("\n") + "\n");
	},
);
