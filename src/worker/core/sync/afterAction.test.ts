import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { PHASE } from "../../../common/constants.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { afterAction } from "./afterAction.ts";
import { setSyncEngine } from "./engineHolder.ts";

// A minimal stand-in for the connected SyncEngine, recording what afterAction
// fans out to it.
const makeEngine = () => {
	const published: { label: string; count: number }[] = [];
	const notifications: { title: string; body: string }[] = [];
	const engine = {
		onLocalChangeset: async (changeset: any, label: string) => {
			published.push({ label, count: changeset.changes.length });
		},
		getIsHost: () => true,
		localName: "Alex",
		publishNotification: async (n: any) => {
			notifications.push(n);
		},
	};
	return { engine, published, notifications };
};

describe("afterAction silent publishing", () => {
	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		g.setWithoutSavingToDB("season", 2026);
		g.setWithoutSavingToDB("userTids", [0]);
		await resetCache({
			teams: [
				{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
				{ tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
			],
		});
		changeTracker.reset();
		changeTracker.enable();
	});

	afterEach(() => {
		changeTracker.disable();
		changeTracker.reset();
		setSyncEngine(undefined);
	});

	// Record one finished game into the cache (with tracking on, so it lands in
	// the change tracker just like a real sim would).
	const seedOneGame = async () => {
		await idb.cache.games.add({
			gid: 0,
			season: 2026,
			day: 5,
			teams: [{ tid: 0 }, { tid: 1 }],
			won: { tid: 0, pts: 110 },
			lost: { tid: 1, pts: 105 },
		} as any);
	};

	test("a normal sim publishes AND pushes a notification", async () => {
		const { engine, published, notifications } = makeEngine();
		setSyncEngine(engine as any);

		await seedOneGame();
		await afterAction("playMenu", "sim");

		assert.strictEqual(published.length, 1, "changeset must reach the room");
		assert.ok(published[0]!.count > 0);
		assert.ok(
			notifications.length >= 1,
			"a normal sim should push at least one notification",
		);
	});

	test("a silent sim still publishes but pushes NO notification", async () => {
		const { engine, published, notifications } = makeEngine();
		setSyncEngine(engine as any);

		await seedOneGame();
		// silent: true is how a single-game sim (Sim one game / live game) drains.
		await afterAction("playMenu", "sim", { silent: true });

		assert.strictEqual(
			published.length,
			1,
			"sync must stay sound - the game still reaches the room",
		);
		assert.ok(published[0]!.count > 0);
		assert.strictEqual(
			notifications.length,
			0,
			"a single-game sim must not ping anyone",
		);
	});

	test("the single-game action labels are silent even without the flag", async () => {
		// The generic worker wrapper fires afterAction("actions", "simGame"/
		// "liveGame") without the silent flag - these labels must self-silence so a
		// fire-and-forget live game can't ping anyone.
		for (const name of ["simGame", "liveGame"]) {
			const { engine, published, notifications } = makeEngine();
			setSyncEngine(engine as any);
			changeTracker.reset();
			await resetCache({
				teams: [
					{ tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
					{ tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
				],
			});

			await seedOneGame();
			await afterAction("actions", name);

			assert.strictEqual(published.length, 1, `${name} must still sync`);
			assert.strictEqual(
				notifications.length,
				0,
				`${name} must not push a notification`,
			);
		}
	});

	test("a full-day sim to a game (simToGame) DOES notify", async () => {
		// simToGame sims whole days up to a target game, so it is a real sim and
		// must still notify - it must NOT be caught by the single-game silencing.
		const { engine, published, notifications } = makeEngine();
		setSyncEngine(engine as any);

		await seedOneGame();
		await afterAction("actions", "simToGame");

		assert.strictEqual(published.length, 1);
		assert.ok(
			notifications.length >= 1,
			"simToGame is a multi-day sim and should notify",
		);
	});
});
