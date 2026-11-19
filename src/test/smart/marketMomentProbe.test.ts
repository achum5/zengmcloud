import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// One market moment on a league file, laid out: every player shopped and the
// teams he was put to, and every deal found - for one team (MM_TEAM), the
// deals it could have made, each trimmed as the market would make it, with
// what it would have gained.
//   MM_LEAGUE=<league.json> MM_OUT=<file> [MM_TEAM=<tid>] npx vitest --run \
//     --project basketball src/test/smart/marketMomentProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.MM_LEAGUE)(
	"market moment probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		await loadLeagueFile(env.MM_LEAGUE!);
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
		const { dealsNow, trimDeal, withAlternatives } =
			await import("../../worker/core/smartCpu/market.ts");
		const outlook: any = await currentOutlook();
		const view: any = viewFor(outlook);
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const cpu = new Set(
			teams
				.filter((t) => !t.disabled && !view.humans.has(t.tid))
				.map((t) => t.tid),
		);
		const season = g.get("season");
		const name = (x: any) =>
			"pid" in x
				? `${x.firstName} ${x.lastName} ${season - x.born.year}y ${x.ratings.at(-1).ovr}/${x.ratings.at(-1).pot}`
				: `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
		const lines: string[] = [];
		let shopped: any[] = [];
		const deals = await withAlternatives(
			view,
			await dealsNow(view, cpu, new Map(), (s: any) => {
				shopped = [...s];
			}),
		);
		const team = env.MM_TEAM !== undefined ? Number(env.MM_TEAM) : undefined;
		lines.push(
			`${shopped.length} players shopped, ${deals.length} deals found`,
		);
		for (const c of shopped) {
			if (team === undefined || c.buyers?.includes(team)) {
				lines.push(
					`  shopped: ${abbrev.get(c.seller)} ${name(c.p)} -> ${(c.buyers ?? []).map((t: number) => abbrev.get(t)).join(", ")}`,
				);
			}
		}
		lines.push("deals, best for the seller first:");
		for (const d of deals) {
			if (team !== undefined && d.buyer !== team && d.seller !== team) {
				continue;
			}
			const offer = await trimDeal(view, d);
			const buyerGain = await view.dealWorth(d.buyer, d.seller, offer, d.out);
			const sellerGain = await view.dealWorth(d.seller, d.buyer, d.out, offer);
			const c = shopped.find(
				(x) => x.seller === d.seller && x.p.pid === d.out.players[0]?.pid,
			);
			lines.push(
				`  ${abbrev.get(d.buyer)} gets ${d.out.players.map(name).join("; ")} from ${abbrev.get(d.seller)} for [${[...offer.players, ...offer.picks].map(name).join("; ")}]: seller +${sellerGain.toFixed(4)} (best offer +${d.sellerGain.toFixed(4)}, bar ${d.bar.toFixed(4)} from ${(
					c?.buyers ?? []
				)
					.filter((t: number) => t !== d.buyer)
					.map((t: number) => abbrev.get(t))
					.join(
						", ",
					)}), buyer ${buyerGain >= 0 ? "+" : ""}${buyerGain.toFixed(4)} (next best ${(d.buyerAlt ?? 0).toFixed(4)}); margins: seller ${view.marginNow(d.seller, d.out, offer).toFixed(4)}, buyer ${view.marginNow(d.buyer, offer, d.out).toFixed(4)}`,
			);
		}
		fs.writeFileSync(env.MM_OUT!, lines.join("\n") + "\n");
	},
);
