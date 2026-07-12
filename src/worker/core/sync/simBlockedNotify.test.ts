import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { player } from "../index.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { setSyncEngine } from "./engineHolder.ts";
import {
	clearRosterBlockNotice,
	notifyRosterBlockedSim,
} from "./simBlockedNotify.ts";

const makeEngine = () => {
	const notifications: { title: string; body: string; targetTids: any }[] = [];
	const engine = {
		localName: "Alex",
		publishNotification: async (n: any) => {
			notifications.push(n);
		},
	};
	return { engine, notifications };
};

// n players on team 0 (the user team). maxRosterSize is 15, min 10 by default.
const seedRoster = async (n: number) => {
	const players = [];
	for (let i = 0; i < n; i++) {
		players.push(player.generate(0, 30, 2017, true, DEFAULT_LEVEL));
	}
	await resetCache({ players });
	g.setWithoutSavingToDB("teamInfoCache", [
		{ tid: 0, region: "St. Louis", name: "Spirits", abbrev: "STL" },
	] as any);
	g.setWithoutSavingToDB("userTids", [0]);
};

describe("notifyRosterBlockedSim", () => {
	beforeEach(() => {
		resetG();
		clearRosterBlockNotice();
	});

	afterEach(() => {
		setSyncEngine(undefined);
		clearRosterBlockNotice();
	});

	test("announces the over-the-limit team to the room", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);
		await seedRoster(16); // max is 15

		await notifyRosterBlockedSim();

		assert.strictEqual(notifications.length, 1);
		assert.ok(notifications[0]!.body.includes("St. Louis Spirits"));
		assert.ok(notifications[0]!.body.includes("16 players"));
		assert.ok(notifications[0]!.body.includes("max 15"));
		assert.strictEqual(notifications[0]!.targetTids, null); // room-wide
	});

	test("under the minimum is also announced", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);
		await seedRoster(9); // min is 10

		await notifyRosterBlockedSim();

		assert.strictEqual(notifications.length, 1);
		assert.ok(notifications[0]!.title.includes("under"));
		assert.ok(notifications[0]!.body.includes("min 10"));
	});

	test("the same persistent block only pings once", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);
		await seedRoster(16);

		await notifyRosterBlockedSim();
		await notifyRosterBlockedSim();
		await notifyRosterBlockedSim();

		assert.strictEqual(notifications.length, 1);
	});

	test("clearRosterBlockNotice lets the same block announce again", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);
		await seedRoster(16);

		await notifyRosterBlockedSim();
		// A successful sim in between clears the dedup.
		clearRosterBlockNotice();
		await notifyRosterBlockedSim();

		assert.strictEqual(notifications.length, 2);
	});

	test("a changed roster count re-announces without an explicit clear", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);
		await seedRoster(16);
		await notifyRosterBlockedSim();

		// Still over the cap, but a different count - a real change worth re-pinging.
		await seedRoster(17);
		await notifyRosterBlockedSim();

		assert.strictEqual(notifications.length, 2);
	});

	test("no notification when no user team breaks a limit", async () => {
		const { engine, notifications } = makeEngine();
		setSyncEngine(engine as any);
		await seedRoster(12); // within 10-15

		await notifyRosterBlockedSim();

		assert.strictEqual(notifications.length, 0);
	});

	test("no-op (no throw) when not connected to a room", async () => {
		setSyncEngine(undefined);
		await seedRoster(16);
		await notifyRosterBlockedSim();
		// Reaching here without throwing is the assertion.
		assert.ok(true);
	});
});
