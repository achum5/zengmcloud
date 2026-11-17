import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// A pick against the player it becomes: the market price of each next-draft
// first, by where its team is expected to finish, beside the price of last
// draft's first-rounders by where they went - and each one's use to its own
// team (docs/smart-cpu.md). With them priced on different footings, a human
// could buy CPU picks for little and sell the rookies dear.
//   GAP_LEAGUE=<league.json> [GAP_OUTLOOK='{"knownClasses":true}'] npx vitest \
//     --run --project basketball src/test/smart/pickGapProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.GAP_LEAGUE)(
	"pick gap probe",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => lines.push(x);
		await loadLeagueFile(env.GAP_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			realisticFrontOffices: true,
			smartCpu: true,
			spectator: true,
		});
		const { outlookSettings, expectedPlace } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		Object.assign(outlookSettings, JSON.parse(env.GAP_OUTLOOK ?? "{}"));
		const { smartSettings } =
			await import("../../worker/core/smartCpu/worth.ts");
		Object.assign(smartSettings, JSON.parse(env.GAP_SMART ?? "{}"));
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		await view.prime();
		await view.keenest();
		const season = g.get("season");
		const next = season + (g.get("phase") >= 5 ? 1 : 0);
		const abbrev = new Map(
			(await idb.cache.teams.getAll()).map((t) => [t.tid, t.abbrev]),
		);
		const picks = (await idb.cache.draftPicks.getAll()).filter(
			(dp: any) => dp.season === next && dp.round === 1,
		);
		const pickRows = [];
		for (const dp of picks) {
			const a = { players: [], picks: [dp] };
			await view.price(a, [dp.tid]);
			pickRows.push({
				place: expectedPlace(outlook, dp),
				label: `${next} R1 ${abbrev.get(dp.originalTid)} (held by ${abbrev.get(dp.tid)})`,
				price: view.priceNow(a, [dp.tid]),
				use: view.useOfNow(dp.tid, a),
			});
		}
		pickRows.sort((a, b) => a.place - b.place);
		log(
			`PICKS: ${next} firsts by expected place - price / use to holder (% of a title)`,
		);
		for (const r of pickRows) {
			log(
				`  place ${r.place.toFixed(1).padStart(4)}  ${r.label.padEnd(28)} ${(100 * r.price).toFixed(2).padStart(6)} / ${(100 * r.use).toFixed(2)}`,
			);
		}
		const lastDraft = next - 1;
		const rookies = (await idb.cache.players.getAll()).filter(
			(p: any) =>
				p.draft?.year === lastDraft && p.draft.round === 1 && p.tid >= 0,
		);
		rookies.sort((a: any, b: any) => a.draft.pick - b.draft.pick);
		log(
			`ROOKIES: ${lastDraft} first-rounders by pick - price / use to his team`,
		);
		for (const p of rookies) {
			const a = { players: [p], picks: [] };
			await view.price(a, [p.tid]);
			const r = p.ratings.at(-1)!;
			log(
				`  #${String(p.draft.pick).padStart(2)} ${`${p.firstName} ${p.lastName}`.padEnd(24)} ${season - p.born.year}y ${r.ovr}/${r.pot} ${abbrev.get(p.tid)}  ${(100 * view.priceNow(a, [p.tid])).toFixed(2).padStart(6)} / ${(100 * (await view.useOf(p.tid, a))).toFixed(2)}`,
			);
		}
		fs.writeFileSync(
			env.GAP_OUT ?? "pickGapProbe.txt",
			`${lines.join("\n")}\n`,
		);
	},
);
