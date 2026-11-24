import { assert, beforeEach, describe, test, vi } from "vitest";

// updatePlayMenu is a no-op under test (it is what pushes today's date and the
// Play menu to the screen), so count its calls instead. The setup already
// loaded the real one, hence a fresh module graph with it swapped out.
const playMenuCalls = { count: 0 };
vi.resetModules();
vi.doMock("../../util/updatePlayMenu.ts", () => ({
	default: async () => {
		playMenuCalls.count += 1;
	},
}));
const { resetCache, resetG } = await import("../../../test/helpers.ts");
const { local } = await import("../../util/index.ts");
const {
	beginCoalescedRefresh,
	endCoalescedRefresh,
	flushDeferredRefreshAfterLive,
	refreshAfterApply,
} = await import("./changeset.ts");

// A league mate's day sim as a receiving device gets it: games and schedule
// rows, no phase change. The header's date and the Play menu come from
// updatePlayMenu, which a receiver used to run on a phase change only - so
// the auto-play scheduler simming a night on the simmer's device left every
// other device's header on the night already played.
const daySim = {
	touchedSeason: false,
	touchedGameAttributes: true,
	touchedGames: true,
	touchedPhase: false,
	touchedStatus: false,
	touchedStores: new Set<any>(["games", "schedule", "gameAttributes"]),
	refreshUI: false,
	sweepGames: false,
	rebuildAggregates: false,
	redirect: false,
};

const settle = () => new Promise((resolve) => setTimeout(resolve, 50));

describe("a receiving device's header after a league mate's sim", () => {
	beforeEach(async () => {
		resetG();
		await resetCache();
		local.liveSimGid = undefined;
		playMenuCalls.count = 0;
	});

	test("a day simmed elsewhere moves today's date and the Play menu", async () => {
		await refreshAfterApply(daySim);
		assert.strictEqual(playMenuCalls.count, 1);
	});

	test("a catch-up walk moves them once, at its end", async () => {
		beginCoalescedRefresh();
		try {
			await refreshAfterApply(daySim);
			await refreshAfterApply(daySim);
			assert.strictEqual(playMenuCalls.count, 0);
		} finally {
			endCoalescedRefresh();
		}
		await settle();
		assert.strictEqual(playMenuCalls.count, 1);
	});

	test("not mid live game here: only once it is over", async () => {
		local.liveSimGid = 7;
		await refreshAfterApply(daySim);
		assert.strictEqual(playMenuCalls.count, 0);

		local.liveSimGid = undefined;
		flushDeferredRefreshAfterLive();
		await settle();
		assert.strictEqual(playMenuCalls.count, 1);
	});
});
