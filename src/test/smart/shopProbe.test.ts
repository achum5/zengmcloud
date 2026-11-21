import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// WHY A PLAYER IS (NOT) SHOPPED: for each player, his use to his team and to
// each of the keenest teams (as the market's candidates() reads it), the
// top bidder, the price, the surplus, and whether each keen team could take
// his salary in an offer of a few players (matchable) - then the keenest
// teams' real offers for him, and why not.
//   SP_LEAGUE=<league.json> SP_OUT=<file> SP_PIDS=<pids> \
//     npx vitest --run --project basketball src/test/smart/shopProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.SP_LEAGUE)("shop probe", { timeout: 3_600_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const lines: string[] = [];
	const log = (x: string) => {
		lines.push(x);
		fs.writeFileSync(env.SP_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
	};
	await loadLeagueFile(env.SP_LEAGUE!);
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
	const { marketSettings } =
		await import("../../worker/core/smartCpu/market.ts");
	const { offerStats, offerWatchers } =
		await import("../../worker/core/smartCpu/worth.ts");
	const { contractOf } =
		await import("../../worker/core/realisticFrontOffice/contractOf.ts");
	const o = await import("../../worker/core/realisticFrontOffice/outlook.ts");
	const outlook: any = await currentOutlook();
	const view: any = viewFor(outlook);
	await view.prime();
	const teams = await idb.cache.teams.getAll();
	const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
	const keen: number[] = [...view.outlook.teams.keys()];
	// What a team would gain from him once it has sent back his salary.
	const matchable = (buyer: number, p: any) => {
		const m = view.matchedNow(buyer, { players: [p], picks: [] });
		return m ? `net ${m.net.toFixed(3)}` : "can't send his salary back";
	};
	// SP_DEBUG=1: each offer a buyer tries as it cuts one back.
	if (env.SP_DEBUG) {
		offerWatchers.push((t: any) => {
			const pieces = [
				...t.offer.players.map(
					(x: any) =>
						`${x.lastName} ${x.ratings.at(-1).ovr} $${(contractOf(x).amount / 1000).toFixed(1)}M`,
				),
				...t.offer.picks.map(
					(dp: any) =>
						`${dp.season} R${dp.round} ${abbrev.get(dp.originalTid)}`,
				),
			];
			log(
				`    try ${abbrev.get(t.buyer)}: ${pieces.join("; ")} -> worth ${t.worth.toFixed(4)} sure ${t.sure.toFixed(4)} margin ${t.margin.toFixed(4)}${t.clears ? " CLEARS" : ""}`,
			);
		});
	}
	log(
		`keenest: ${keen.map((k) => `${abbrev.get(k)} ${(100 * (outlook.teams.get(k)?.title?.[0] ?? 0)).toFixed(1)}%`).join(", ")}`,
	);
	for (const pid of (env.SP_PIDS ?? "").split(",").map(Number)) {
		const p: any = await idb.cache.players.get(pid);
		if (!p) {
			continue;
		}
		const seller = p.tid;
		for (const t of teams) {
			await o.ensureRetention(outlook, [p], t.tid);
		}
		const one = { players: [p], picks: [] };
		const own = view.roughUseOfNow(seller, one);
		log(
			`\n${p.firstName} ${p.lastName} ${p.ratings.at(-1).ovr} $${(contractOf(p).amount / 1000).toFixed(1)}M->${contractOf(p).exp} of ${abbrev.get(seller)}: own use ${own.toFixed(4)}, stays ${o.retentionOf(outlook, pid, seller).toFixed(2)}`,
		);
		const bids = keen
			.filter((k) => k !== seller)
			.map((k) => ({ k, bid: view.roughUseOfNow(k, one) }))
			.sort((a, b) => b.bid - a.bid);
		for (const b of bids) {
			log(
				`  keen ${abbrev.get(b.k)}: use ${b.bid.toFixed(4)}, stays ${o.retentionOf(outlook, pid, b.k).toFixed(2)}, can take his salary: ${matchable(b.k, p)}`,
			);
		}
		const top = bids[0]!;
		const rest = bids.slice(1);
		const price = rest[1]?.bid ?? rest[0]?.bid ?? -Infinity;
		log(
			`  top ${abbrev.get(top.k)}; price ${price.toFixed(4)}; surplus ${(top.bid - Math.max(own, price)).toFixed(4)}`,
		);
		// All teams' uses (anyBuyer), the keenest first.
		const all = teams
			.filter((t) => t.tid !== seller && !t.disabled)
			.map((t) => ({ k: t.tid, want: view.roughUseOfNow(t.tid, one) }))
			.sort((a, b) => b.want - a.want)
			.slice(0, 8);
		log(
			`  keenest of all: ${all.map((w) => `${abbrev.get(w.k)} ${w.want.toFixed(3)} (${matchable(w.k, p)})`).join(", ")}`,
		);
		// Their real offers.
		for (const { k } of all.slice(0, 5)) {
			const before: Record<string, number> = { ...offerStats };
			const clears = (
				_b: number,
				worth: number,
				sure: number,
				margin: number,
			) =>
				worth > marketSettings.minGain &&
				(!marketSettings.margins ||
					sure > Math.max(marketSettings.minGain, margin));
			const made = await view.offerFrom(k, seller, one, undefined, clears);
			const why = Object.entries(offerStats)
				.filter(([key, v]) => v !== before[key] && key !== "tried")
				.map(([key]) => key)
				.join(",");
			if (!made) {
				log(`  ${abbrev.get(k)}: no offer (${why})`);
				continue;
			}
			const toSeller = await view.dealWorth(seller, k, one, made.offer);
			const sure = view.sureWorthNow(seller, k, one, made.offer);
			const toBuyer = await view.dealWorth(k, seller, made.offer, one);
			const bsure = view.sureWorthNow(k, seller, made.offer, one);
			const pieces = [
				...made.offer.players.map(
					(x: any) =>
						`${x.lastName} ${x.ratings.at(-1).ovr} $${(contractOf(x).amount / 1000).toFixed(1)}M`,
				),
				...made.offer.picks.map(
					(dp: any) =>
						`${dp.season} R${dp.round} ${abbrev.get(dp.originalTid)}`,
				),
			];
			log(
				`  ${abbrev.get(k)} offers ${pieces.join("; ")} - seller ${toSeller.toFixed(4)} (sure ${sure.toFixed(4)}, margin ${view.marginNow(seller, one, made.offer).toFixed(4)}), buyer ${toBuyer.toFixed(4)} (sure ${bsure.toFixed(4)}, margin ${view.marginNow(k, made.offer, one).toFixed(4)})`,
			);
		}
	}
});
