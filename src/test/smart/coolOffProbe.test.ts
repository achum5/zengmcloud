import { test } from "vitest";
import { loadLeagueFile } from "./loadLeague.ts";

// The smart CPU's trades from the preseason to just past the signing
// cool-off (players signed in the offseason can't be traded for 14 games),
// on an owner's league file: CO_LEAGUE=<league.json> CO_OUT=<file>
// [CO_UNLOCK=1: every player tradable from the start] [CO_DAYS=17].
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.CO_LEAGUE)("cool-off", { timeout: 7_200_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const lines: string[] = [];
	const log = (x: string) => {
		lines.push(x);
		fs.writeFileSync(env.CO_OUT!, lines.join("\n") + "\n");
	};
	await loadLeagueFile(env.CO_LEAGUE!);
	const { g } = await import("../../worker/util/index.ts");
	const { idb } = await import("../../worker/db/index.ts");
	const { game, phase } = await import("../../worker/core/index.ts");
	const { PHASE } = await import("../../common/constants.ts");
	const { smartTradeWindow, smartDealWatchers } =
		await import("../../worker/core/smartCpu/market.ts");
	const { contractOf } =
		await import("../../worker/core/realisticFrontOffice/contractOf.ts");
	const teams = await idb.cache.teams.getAll();
	const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
	const humans = new Set(g.get("userTids"));
	// The league mates' teams as people's, while the games play hands-off.
	const { smartHumans } = await import("../../worker/core/smartCpu/signing.ts");
	const { league } = await import("../../worker/core/index.ts");
	smartHumans.tids = [...humans];
	await league.setGameAttributes({ spectator: true });
	const r = (p: any) => p.ratings.at(-1);
	const name = (p: any) =>
		`${p.firstName} ${p.lastName} ${r(p).ovr}/${r(p).pot} $${(contractOf(p).amount / 1000).toFixed(1)}M`;

	const players = (await idb.cache.players.getAll()).filter((p) => p.tid >= 0);
	const byValue = [...players].sort((a, b) => b.value - a.value);
	const locked = players.filter((p) => p.gamesUntilTradable > 0);
	log(
		`season ${g.get("season")} phase ${g.get("phase")}: ${locked.length} of ${players.length} players locked (games left: ${[...new Set(locked.map((p) => p.gamesUntilTradable))].join(",")})`,
	);
	for (const n of [10, 30, 60, 100]) {
		log(
			`  top ${n} by value: ${byValue.slice(0, n).filter((p) => p.gamesUntilTradable > 0).length} locked`,
		);
	}
	for (const t of teams.filter((t) => !t.disabled)) {
		const mine = locked
			.filter((p) => p.tid === t.tid)
			.sort((a, b) => b.value - a.value);
		const roster = players.filter((p) => p.tid === t.tid);
		log(
			`  ${abbrev.get(t.tid)}${humans.has(t.tid) ? " (human)" : ""}: ${mine.length}/${roster.length} locked, $${(mine.reduce((s, p) => s + contractOf(p).amount, 0) / 1000).toFixed(1)}M${mine.length ? `; best ${name(mine[0])}` : ""}`,
		);
	}

	if (env.CO_UNLOCK) {
		for (const p of await idb.cache.players.getAll()) {
			if (p.gamesUntilTradable > 0) {
				p.gamesUntilTradable = 0;
				delete p.daysUntilTradable;
				await idb.cache.players.put(p);
			}
		}
		log("(every player unlocked)");
	}

	let day = 0;
	smartDealWatchers.push((deal) => {
		const out = deal.out.players.map(name);
		const offerP = deal.offer.players.map(name);
		const picks = (xs: any[]) =>
			xs.map(
				(dp) => `${dp.season} r${dp.round} (${abbrev.get(dp.originalTid)})`,
			);
		log(
			`day ${day}: ${abbrev.get(deal.buyer)} gets ${[...out, ...picks(deal.out.picks)].join(", ")} | ${abbrev.get(deal.seller)} gets ${[...offerP, ...picks(deal.offer.picks)].join(", ")} | gains seller ${deal.sellerGain.toFixed(4)} buyer ${deal.buyerGain.toFixed(4)}`,
		);
	});

	const { smartTraceWatchers } =
		await import("../../worker/core/smartCpu/market.ts");
	smartTraceWatchers.push((t) => {
		if (t.stage === "waits") {
			log(
				`day ${day}: ${abbrev.get(t.buyer!)} and ${abbrev.get(t.seller)} wait on ${name(t.p)} (${t.why})`,
			);
		}
	});
	const { smartClock } = await import("../../worker/core/smartCpu/clock.ts");
	const start = Date.now();
	await smartTradeWindow();
	log(`preseason market ${((Date.now() - start) / 1000).toFixed(1)}s`);
	await phase.newPhase(PHASE.REGULAR_SEASON, {});
	const days = Number(env.CO_DAYS ?? 17);
	for (day = 1; day <= days; day++) {
		await game.play(1, {});
		const left = (await idb.cache.players.getAll()).filter(
			(p) => p.tid >= 0 && p.gamesUntilTradable > 0,
		);
		const { gamesPlayed } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		log(
			`after day ${day}: ${left.length} locked (most games left ${Math.max(0, ...left.map((p) => p.gamesUntilTradable))}), ${((await gamesPlayed()) / (2 * 15)).toFixed(1)} games a team`,
		);
	}
	for (const k of ["market", "market: after the cool-off"]) {
		const c = smartClock[k];
		log(`${k}: ${c ? `${c.n} times, ${(c.ms / 1000).toFixed(1)}s` : "never"}`);
	}
	log("done");
});
