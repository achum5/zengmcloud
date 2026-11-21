import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// ONE DEAL, TAKEN APART: a trade from a league file (undone first with
// LL_UNDO) as each side's smart CPU counts it - its goal, the premiums on
// what it gets and gives, its sure worth and margin, each piece's use and
// price, and its title chances with and without the deal.
//   DP_LEAGUE=<league.json> DP_OUT=<file> LL_UNDO=<json> \
//   DP_A=<tid> DP_A_GIVES=<pids>/<dpids> DP_B=<tid> DP_B_GIVES=<pids>/<dpids> \
//     npx vitest --run --project basketball src/test/smart/dealProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.DP_LEAGUE)("deal probe", { timeout: 3_600_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const lines: string[] = [];
	const log = (x: string) => {
		lines.push(x);
		fs.writeFileSync(env.DP_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
	};
	await loadLeagueFile(env.DP_LEAGUE!);
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
	const { contractOf } =
		await import("../../worker/core/realisticFrontOffice/contractOf.ts");
	const outlook: any = await currentOutlook();
	// DP_PW: this season's playoff weight set (0: the regular season alone,
	// 1: the playoffs alone; "none": no blend).
	if (env.DP_PW !== undefined) {
		outlook.playoffWeight =
			env.DP_PW === "none" ? undefined : Number(env.DP_PW);
	}
	const view: any = viewFor(outlook);
	await view.prime();
	const teams = await idb.cache.teams.getAll();
	const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
	const season = g.get("season");
	const none = { players: [], picks: [] };
	const read = async (s: string) => {
		const [pids, dpids] = s.split("/");
		const players = [];
		for (const x of (pids ?? "").split(",").filter(Boolean)) {
			players.push((await idb.cache.players.get(Number(x)))!);
		}
		const picks = [];
		for (const x of (dpids ?? "").split(",").filter(Boolean)) {
			picks.push((await idb.cache.draftPicks.get(Number(x)))!);
		}
		return { players, picks };
	};
	const piece = (x: any) =>
		"pid" in x
			? `${x.firstName} ${x.lastName} ${season - x.born.year}y ${x.ratings.at(-1).ovr}/${x.ratings.at(-1).pot} $${(contractOf(x).amount / 1000).toFixed(1)}M->${contractOf(x).exp}${x.injury.gamesRemaining > 0 ? ` (out ${x.injury.gamesRemaining}g)` : ""}`
			: `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
	const A = Number(env.DP_A);
	const B = Number(env.DP_B);
	const aGives = await read(env.DP_A_GIVES ?? "");
	const bGives = await read(env.DP_B_GIVES ?? "");
	const fmt = (xs: number[], k = 3) => xs.map((x) => x.toFixed(k)).join(" ");
	for (const [tid, partner, give, get] of [
		[A, B, aGives, bGives],
		[B, A, bGives, aGives],
	] as const) {
		const worth = await view.dealWorth(tid, partner, give, get);
		const parts = view.dealPartsNow(tid, partner, give, get);
		const sure = view.sureWorthNow(tid, partner, give, get);
		const margin = view.marginNow(tid, give, get);
		const t = outlook.teams.get(tid);
		log(
			`\n${abbrev.get(tid)} gives [${[...give.players, ...give.picks].map(piece).join("; ")}] for [${[...get.players, ...get.picks].map(piece).join("; ")}]`,
		);
		log(
			`  worth ${worth.toFixed(4)} = goal ${parts.goal.toFixed(4)} + got ${parts.got.toFixed(4)} - given ${parts.given.toFixed(4)} - flip ${parts.flip.toFixed(4)}; sure ${sure.toFixed(4)}, margin ${margin.toFixed(4)}`,
		);
		log(
			`  title chance now ${(100 * (t?.title?.[0] ?? 0)).toFixed(1)}%, by season ${(t?.title ?? []).map((x: number) => (100 * x).toFixed(1)).join(" ")}`,
		);
		const h = view.heldNow(tid);
		const after = view.swappedFrom(h, give, get);
		const sa = o.teamStrengths(outlook, tid, h.players, h.picks);
		const sb = o.teamStrengths(outlook, tid, after.players, after.picks);
		const curves = o.curvesFor(outlook, tid);
		const titleBy = (s: number[]) =>
			s.map((x, y) => (100 * o.curveAt(curves.title[y]!, x)).toFixed(2));
		log(`  strength before ${fmt(sa, 2)}`);
		log(`  strength after  ${fmt(sb, 2)}`);
		log(`  title % before ${titleBy(sa).join(" ")}`);
		log(`  title % after  ${titleBy(sb).join(" ")}`);
		const sv = (s: number[]) =>
			s.map((x, y) => o.seasonValue(outlook, y, x, tid));
		const va = sv(sa);
		const vb = sv(sb);
		log(
			`  season value change ${fmt(
				vb.map((x, y) => x - va[y]!),
				4,
			)}`,
		);
		for (const x of [...give.players, ...give.picks]) {
			const one =
				"pid" in x ? { players: [x], picks: [] } : { players: [], picks: [x] };
			await view.price(one, [tid, partner]);
			log(
				`  gives ${piece(x)}: use to it ${view.useOfNow(tid, one).toFixed(4)}, price ${view.priceNow(one, [tid, partner]).toFixed(4)}`,
			);
		}
		for (const x of [...get.players, ...get.picks]) {
			const one =
				"pid" in x ? { players: [x], picks: [] } : { players: [], picks: [x] };
			await view.price(one, [tid, partner]);
			log(
				`  gets ${piece(x)}: use to it ${view.useOfNow(tid, one).toFixed(4)}, price ${view.priceNow(one, [tid, partner]).toFixed(4)}`,
			);
		}
		log(
			`  whole: gives use ${view.useOfNow(tid, give).toFixed(4)} price ${view.priceNow(give, [tid, partner]).toFixed(4)}; gets use ${view.useOfNow(tid, get).toFixed(4)} price ${view.priceNow(get, [tid, partner]).toFixed(4)}`,
		);
		// DP_BIDS=1: each keen team's bid for what this side gives, whole and
		// piece by piece.
		if (env.DP_BIDS) {
			const keen: number[] = await view.keenest();
			for (const k of keen.filter((k) => k !== tid && k !== partner)) {
				const each = [...give.players, ...give.picks].map((x: any) =>
					view.bidNow(
						k,
						"pid" in x
							? { players: [x], picks: [] }
							: { players: [], picks: [x] },
					),
				);
				log(
					`  ${abbrev.get(k)} bids ${view.bidNow(k, give).toFixed(4)} whole; ${each.map((b: number) => b.toFixed(4)).join(" ")}`,
				);
			}
			for (const dp of give.picks) {
				log(`  floor ${piece(dp)} ${view.pickFloorNow(dp).toFixed(4)}`);
			}
		}
		void none;
	}
});
