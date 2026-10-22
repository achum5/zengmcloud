import { assert, test, vi } from "vitest";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";

// Realistic front offices trade as offseason phases open, inside the phase
// change (phase/finalize.ts). Waiting there for the UI to refresh waited for
// the phase change to finish - the UI loads the page's view again, and a view
// waits for any phase change (lock.runView) - while the phase change waited
// for the trade, so it never finished.
test("a trade made during a phase change does not wait for the UI to refresh", async () => {
	// What the UI does with a realtimeUpdate. Fresh modules, since the test
	// setup has already loaded the real toUI.
	vi.resetModules();
	vi.doMock("../../util/toUI.ts", () => ({
		default: async (name: string) => {
			if (name === "realtimeUpdate") {
				const { default: lock } = await import("../../util/lock.ts");
				await lock.runView(() => undefined);
			}
		},
	}));
	try {
		const { resetCache, resetG } = await import("../../../test/helpers.ts");
		const { idb } = await import("../../db/index.ts");
		const { g, lock } = await import("../../util/index.ts");
		const { player } = await import("../index.ts");
		const { default: processTrade } = await import("./processTrade.ts");

		resetG();
		(idb as any).league = undefined;
		g.setWithoutSavingToDB("numTeams", 3);
		g.setWithoutSavingToDB("numActiveTeams", 3);
		await resetCache({
			players: [
				player.generate(0, 25, 2015, true, DEFAULT_LEVEL),
				player.generate(1, 25, 2015, true, DEFAULT_LEVEL),
			],
		});
		const [p0, p1] = await idb.cache.players.getAll();

		await lock.set("newPhase", true);
		try {
			const trade = processTrade(
				[
					{
						tid: 0,
						pids: [p0!.pid],
						pidsExcluded: [],
						dpids: [],
						dpidsExcluded: [],
					},
					{
						tid: 1,
						pids: [p1!.pid],
						pidsExcluded: [],
						dpids: [],
						dpidsExcluded: [],
					},
				],
				undefined,
			).then(() => "done" as const);
			const timeout = new Promise<"stuck">((resolve) => {
				setTimeout(() => resolve("stuck"), 2000);
			});
			assert.strictEqual(await Promise.race([trade, timeout]), "done");
		} finally {
			await lock.set("newPhase", false);
		}
		assert.strictEqual((await idb.cache.players.get(p0!.pid))!.tid, 1);
	} finally {
		vi.doUnmock("../../util/toUI.ts");
		vi.resetModules();
	}
});
