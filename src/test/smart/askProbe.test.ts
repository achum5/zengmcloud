import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { createArenaLeague } from "../arena/runLeague.ts";
import { contractOf } from "../../worker/core/realisticFrontOffice/contractOf.ts";

// The trading block's "What are you looking for?" with the smart CPU on:
// every team's offer under each setting.
//   ASK_PROBE=1 npx vitest --run --project basketball \
//     src/test/smart/askProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.ASK_PROBE === "1")("asks", { timeout: 1_800_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const { smartTradingBlock } =
		await import("../../worker/core/smartCpu/screens.ts");
	Math.random = makeSeededRandom(5);
	(idb as any).league = undefined;
	await createArenaLeague(9177);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("smartCpu", true);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", 0);
	g.setWithoutSavingToDB("userTids", [0]);
	const mine = (await idb.cache.players.indexGetAll("playersByTid", 0)).sort(
		(a, b) => b.value - a.value,
	);
	const star = mine[0]!;
	const age = (born: number) => g.get("season") - born;
	console.log(
		"star",
		star.ratings.at(-1)!.ovr,
		star.ratings.at(-1)!.pot,
		age(star.born.year),
	);
	const none = () => ({
		positions: new Set<string>(),
		skills: new Set<string>(),
		draftPicks: false,
		prospects: false,
		bestCurrentPlayers: false,
	});
	const show = async (label: string, lf: any) => {
		const t = performance.now();
		const offers = await smartTradingBlock(0, [star.pid], [], lf);
		console.log(
			`--- ${label}: ${offers.length} offers, ${((performance.now() - t) / 1000).toFixed(1)} s`,
		);
		for (const o of offers) {
			const ps = (
				await Promise.all(o[1].pids.map((pid) => idb.cache.players.get(pid)))
			).map(
				(p) =>
					`${p!.ratings.at(-1)!.pos} ${age(p!.born.year)}y ${p!.ratings.at(-1)!.ovr}/${p!.ratings.at(-1)!.pot} $${(contractOf(p!).amount / 1000).toFixed(1)}M`,
			);
			const dps = (
				await Promise.all(o[1].dpids.map((d) => idb.cache.draftPicks.get(d)))
			).map((d) => `${d!.season} r${d!.round}`);
			console.log(`  tid ${o[1].tid}: ${[...ps, ...dps].join(" | ")}`);
		}
	};
	await show("nothing", undefined);
	await show("picks", { ...none(), draftPicks: true });
	await show("prospects", { ...none(), prospects: true });
	await show("best", { ...none(), bestCurrentPlayers: true });
	await show("C", { ...none(), positions: new Set(["C"]) });
	await show("3 + G", {
		...none(),
		skills: new Set(["shootingThreePointer"]),
		positions: new Set(["G"]),
	});
	await show("prospects + picks", {
		...none(),
		prospects: true,
		draftPicks: true,
	});
	await show("PER 15, <= $20M", {
		...none(),
		player: { per: 15, salaryMost: 20000 },
	});
});
