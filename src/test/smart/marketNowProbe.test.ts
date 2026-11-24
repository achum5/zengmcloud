import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// The smart CPU's preseason market run once on an owner's league file:
// MN_LEAGUE=<league.json> MN_OUT=<file>.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.MN_LEAGUE)("market now", { timeout: 3_600_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const lines: string[] = [];
	const log = (x: string) => {
		lines.push(x);
		fs.writeFileSync(env.MN_OUT!, lines.join("\n") + "\n");
	};
	await loadLeagueFile(env.MN_LEAGUE!);
	const { g } = await import("../../worker/util/index.ts");
	const { idb } = await import("../../worker/db/index.ts");
	const { smartTradeWindow } =
		await import("../../worker/core/smartCpu/market.ts");
	const { smartVanillaOn } =
		await import("../../worker/core/realisticFrontOffice/isOn.ts");
	log(
		`season ${g.get("season")} phase ${g.get("phase")} smartVanillaOn ${smartVanillaOn()} userTids ${JSON.stringify(g.get("userTids"))} spectator ${g.get("spectator")} difficultyTrade ${g.get("difficultyTrade")}`,
	);
	const teams = await idb.cache.teams.getAll();
	const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
	const before = new Set(
		(
			(await idb.merged.events.getAll({
				season: g.get("season"),
			} as any)) as any[]
		).map((e: any) => e.eid),
	);
	const { smartLaterWatchers, marketSettings } =
		await import("../../worker/core/smartCpu/market.ts");
	if (env.MN_SHARE) {
		marketSettings.waitShare = Number(env.MN_SHARE);
	}
	smartLaterWatchers.push((ahead, later) => {
		log(`look past the cool-off: ${ahead.length} deals`);
		for (const d of ahead) {
			const p: any = d.out.players[0];
			log(
				`  ${abbrev.get(d.buyer)} would get ${p ? `${p.firstName} ${p.lastName} ${p.ratings.at(-1).ovr}` : "a pick"} from ${abbrev.get(d.seller)}: seller ${d.sellerGain.toFixed(3)}, buyer ${d.buyerGain.toFixed(3)}`,
			);
		}
		log(
			`  later: ${[...later].map(([tid, x]) => `${abbrev.get(tid)} ${x.toFixed(3)}`).join(", ")}`,
		);
	});
	const start = Date.now();
	await smartTradeWindow();
	log(`market took ${((Date.now() - start) / 1000).toFixed(1)}s`);
	const events = (
		(await idb.merged.events.getAll({
			season: g.get("season"),
		} as any)) as any[]
	).filter((e: any) => !before.has(e.eid) && e.type === "trade");
	log(`${events.length} trades`);
	for (const e of events as any[]) {
		const sides: string[] = [];
		for (const [i, side] of (e.teams ?? []).entries()) {
			const tid = e.tids[i];
			const assets: string[] = [];
			for (const a of side.assets ?? []) {
				if (a.pid !== undefined) {
					const p = (await idb.cache.players.get(a.pid)) as any;
					const r = p?.ratings.at(-1);
					assets.push(
						`${a.name} (${g.get("season") - (p?.born.year ?? 0)}y ${r?.ovr}/${r?.pot}, $${((a.contract?.amount ?? 0) / 1000).toFixed(1)}M thru ${a.contract?.exp})`,
					);
				} else {
					assets.push(
						`${a.season} ${a.round === 1 ? "1st" : "2nd"} (${abbrev.get(a.originalTid)})`,
					);
				}
			}
			sides.push(`${abbrev.get(tid)} gets ${assets.join(", ") || "nothing"}`);
		}
		log(`- ${sides.join(" | ")}`);
	}
	for (const t of teams.filter((t) => !t.disabled)) {
		const n = (await idb.cache.players.indexGetAll("playersByTid", t.tid))
			.length;
		if (n > g.get("maxRosterSize") || n < g.get("minRosterSize")) {
			log(`roster ${abbrev.get(t.tid)}: ${n}`);
		}
	}
	log("done");
});
