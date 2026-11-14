import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { createArenaLeague } from "../arena/runLeague.ts";

// The trading block in a new league with the smart CPU on, before a game is
// played (where the app's trading block never answered, 2026-11-14): each
// step timed.
//   FRESH_BLOCK=1 npx vitest --run --project basketball \
//     src/test/smart/freshBlockProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.FRESH_BLOCK === "1")(
	"trading block in a new league",
	{ timeout: 1_800_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		const { smartTradingBlock } =
			await import("../../worker/core/smartCpu/screens.ts");
		Math.random = makeSeededRandom(5);
		(idb as any).league = undefined;
		await createArenaLeague(Number(env.FRESH_LEAGUE ?? 9177));
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		g.setWithoutSavingToDB("smartCpu", true);
		g.setWithoutSavingToDB("spectator", false);
		g.setWithoutSavingToDB("userTid", 0);
		g.setWithoutSavingToDB("userTids", [0]);
		console.log("season", g.get("season"), "phase", g.get("phase"));
		let t = performance.now();
		const lap = (label: string) => {
			const now = performance.now();
			console.log(`${label}: ${((now - t) / 1000).toFixed(1)} s`);
			t = now;
		};
		const outlook = await currentOutlook();
		lap("outlook");
		const view = viewFor(outlook);
		await view.prime();
		lap("prime");
		const mine = (await idb.cache.players.indexGetAll("playersByTid", 0)).sort(
			(a, b) => b.value - a.value,
		);
		const offers = await smartTradingBlock(0, [mine[0]!.pid], []);
		lap(`trading block: ${offers.length} offers`);
	},
);
