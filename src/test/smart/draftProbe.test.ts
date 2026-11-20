import { test } from "vitest";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { createArenaLeague } from "../arena/runLeague.ts";

// Why the draft board takes older prospects: for each prospect of a class,
// his projected overall in the seasons ahead (the average career and the best
// quarter), and what drafting him is worth to a bad team and a good one, use
// and price apart (docs/smart-cpu.md).
//   DRAFT_PROBE=1 npx vitest --run --project basketball \
//     src/test/smart/draftProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.DRAFT_PROBE)(
	"draft probe",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => lines.push(x);
		Math.random = makeSeededRandom(Number(env.DRAFT_SEED ?? 7));
		(idb as any).league = undefined;
		if (env.DRAFT_LEAGUE) {
			const { loadLeagueFile } = await import("./loadLeague.ts");
			await loadLeagueFile(env.DRAFT_LEAGUE);
		} else {
			await createArenaLeague(Number(env.DRAFT_SEED ?? 7) + 9000);
		}
		const { league } = await import("../../worker/core/index.ts");
		const { strengthSettings } =
			await import("../../worker/core/realisticFrontOffice/strength.ts");
		strengthSettings.positions = env.DRAFT_POSITIONS !== "0";
		await league.setGameAttributes({
			realisticFrontOffices: env.DRAFT_RFO !== "0",
			smartCpu: true,
			spectator: true,
		});
		g.setWithoutSavingToDB("phase", PHASE.DRAFT);
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { projectPlayer, retentionOf, curvesFor } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { smartDraftChoice } =
			await import("../../worker/core/smartCpu/draft.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const season = g.get("season");
		const prospects = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.UNDRAFTED)
		).filter((p) => p.draft.year === season);
		const outlook: any = await currentOutlook();
		// Teams by their title odds now.
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const odds = teams
			.map((t) => ({ t, odds: outlook.teams?.get?.(t.tid)?.title?.[0] }))
			.map((x) => x);
		const view: any = viewFor(outlook);
		await view.prime();
		await view.keenest();
		const values = new Map<number, number>();
		for (const t of teams) {
			values.set(t.tid, view.value(t.tid, await view.holdingsOf(t.tid)));
		}
		const byValue = [...teams].sort(
			(a, b) => (values.get(a.tid) ?? 0) - (values.get(b.tid) ?? 0),
		);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const keen: number[] = await view.keenest();
		log(
			`keenest: ${keen.map((tid) => `${abbrev.get(tid)} ${(100 * (values.get(tid) ?? 0)).toFixed(1)}`).join(", ")}`,
		);
		log(
			`by franchise value: ${byValue.map((t) => `${t.abbrev} ${(100 * (values.get(t.tid) ?? 0)).toFixed(1)}`).join(", ")}`,
		);
		const picks = [
			{ t: byValue[0]!, pick: 3 },
			{ t: byValue[Math.floor(byValue.length / 3)]!, pick: 12 },
			{ t: byValue.at(-1)!, pick: 25 },
		];
		for (const { t, pick } of picks) {
			const dp = { dpid: -1, tid: t.tid, round: 1, pick };
			const { worths } = await smartDraftChoice(dp, prospects);
			log(
				`\n${t.abbrev} (franchise ${(100 * (values.get(t.tid) ?? 0)).toFixed(1)}% of a title) at #${pick}:`,
			);
			log(
				"  prospect                 age ovr/pot value | proj ovr +1..+7 (best quarter at +5) | ret | use / price / worth (% title)",
			);
			const view2: any = viewFor(await currentOutlook());
			for (const { p, worth, drafted } of worths) {
				const margin = (view2 as any).marginNow
					? view2.marginNow(
							t.tid,
							{ players: [], picks: [] },
							{ players: [drafted], picks: [] },
						)
					: 0;
				const proj = projectPlayer(p, season);
				const path = [1, 2, 3, 4, 5, 6, 7]
					.map((y) => proj.ovr[y]!.toFixed(0))
					.join(" ");
				const boom = proj.boom[5]?.ovr[1];
				const a = { players: [p], picks: [] };
				const use = view2.useOfNow(t.tid, a);
				await view2.price(a, [t.tid]);
				const price = view2.priceNow(a, [t.tid]);
				const r = p.ratings.at(-1)!;
				const bids = keen
					.filter((tid) => tid !== t.tid)
					.map(
						(tid) =>
							`${abbrev.get(tid)} ${(100 * view2.bidNow(tid, a)).toFixed(1)}`,
					)
					.join(" ");
				log(
					`  ${`${p.firstName} ${p.lastName}`.padEnd(24)} ${String(season - p.born.year).padStart(3)} ${r.ovr}/${r.pot} ${p.value.toFixed(0)} | ${path} (${boom?.toFixed(0)}) | ${retentionOf(outlook, p.pid, t.tid).toFixed(2)} | ${(100 * use).toFixed(3)} / ${(100 * price).toFixed(3)} / ${(100 * worth).toFixed(3)} margin ${(100 * margin).toFixed(3)} | bids ${bids}`,
				);
			}
		}
		// Where a contender's use for its best prospect comes from, season by season.
		{
			const outlookMod =
				await import("../../worker/core/realisticFrontOffice/outlook.ts");
			const t = byValue.at(-1)!;
			const o: any = await currentOutlook();
			const v: any = viewFor(o);
			await v.prime();
			const h = await v.holdingsOf(t.tid);
			const best = [...prospects].sort((a, b) => b.value - a.value)[0]!;
			const withHim = { players: [...h.players, best], picks: h.picks };
			const sW = outlookMod.teamStrengths(
				o,
				t.tid,
				withHim.players,
				withHim.picks,
			);
			const sO = outlookMod.teamStrengths(o, t.tid, h.players, h.picks);
			log(
				`\n${t.abbrev}: ${h.players.length} players held, ${h.players.filter((p: any) => (p.contracts?.at?.(-1)?.exp ?? p.contract?.exp) > season).length} signed past ${season}; offset ${o.offset}`,
			);
			for (let y = 0; y < sW.length; y++) {
				log(
					`  y${y} strength ${sW[y]!.toFixed(2)} / ${sO[y]!.toFixed(2)}  value ${(100 * outlookMod.seasonValue(o, y, sW[y]!, t.tid)).toFixed(2)} / ${(100 * outlookMod.seasonValue(o, y, sO[y]!, t.tid)).toFixed(2)}`,
				);
			}
			log(
				`  franchise ${(100 * v.value(t.tid, withHim)).toFixed(2)} / ${(100 * v.value(t.tid, h)).toFixed(2)}`,
			);
			const roster = [...h.players].sort((a: any, b: any) => b.value - a.value);
			log(
				`  roster: ${roster.map((p: any) => `${p.ratings.at(-1).ovr}(${season - p.born.year}y,exp${p.contracts?.at?.(-1)?.exp ?? p.contract?.exp})`).join(" ")}`,
			);
		}
		fs.writeFileSync(
			env.DRAFT_OUT ?? "draftProbe.txt",
			`${lines.join("\n")}\n`,
		);
		void odds;
		void curvesFor;
	},
);
