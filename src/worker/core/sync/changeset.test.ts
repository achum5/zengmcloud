import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { player } from "../index.ts";
import { g, helpers, local } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { changeTracker, runExclusive } from "../../db/changeTracker.ts";
import { applyChangeset, captureChangeset } from "./changeset.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";

const genPlayer = () =>
	player.generate(g.get("userTid"), 30, 2017, true, DEFAULT_LEVEL);

// Simulate shipping a changeset over the network (JSON round-trip), the same as
// Firebase would.
const overTheWire = (changeset: unknown) =>
	JSON.parse(JSON.stringify(changeset));

describe("sync changeset", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("captures only changes made while enabled", async () => {
		resetG();
		await resetCache({ players: [genPlayer()] });

		// Setup happened while disabled, so nothing should be pending.
		changeTracker.enable();
		assert.strictEqual(changeTracker.size(), 0);

		const p = (await idb.cache.players.getAll())[0]!;
		p.tid = 5;
		await idb.cache.players.put(p);

		const changeset = await captureChangeset();
		assert.strictEqual(changeset.changes.length, 1);
		assert.strictEqual(changeset.changes[0]!.store, "players");
		assert.strictEqual(changeset.changes[0]!.type, "put");
	});

	test("excludes device-local userTid but syncs userTids and league settings", async () => {
		resetG();
		await resetCache({});
		changeTracker.enable();
		changeTracker.reset();

		await idb.cache.gameAttributes.put({ key: "userTid", value: 5 });
		await idb.cache.gameAttributes.put({ key: "userTids", value: [5, 6, 7] });
		await idb.cache.gameAttributes.put({ key: "salaryCap", value: 100000 });

		const changeset = await captureChangeset();
		const keys = changeset.changes.map((c) => c.id);

		assert.ok(!keys.includes("userTid"), "userTid must NOT sync (per-device)");
		assert.ok(keys.includes("userTids"), "userTids must sync (multi-team set)");
		assert.ok(keys.includes("salaryCap"), "league settings must sync");
	});

	test("round-trips put, add, and delete to another device", async () => {
		// --- Source device: start with two players (pids 0 and 1) ---
		resetG();
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();

		const players = await idb.cache.players.getAll();
		const pA = players.find((p) => p.pid === 0)!;
		const pB = players.find((p) => p.pid === 1)!;

		// Modify pA, delete pB, add a brand new pC (gets pid 2).
		pA.tid = 5;
		await idb.cache.players.put(pA);
		await idb.cache.players.delete(pB.pid);
		const pCid = await idb.cache.players.add(genPlayer());

		const changeset = overTheWire(await captureChangeset());
		assert.strictEqual(changeset.changes.length, 3);

		// --- Target device: starts from the ORIGINAL state (pids 0 and 1) ---
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.disable(); // Receiver applies, doesn't record.

		await applyChangeset(changeset, { refreshUI: false });

		const after = await idb.cache.players.getAll();
		const byId = new Map(after.map((p) => [p.pid, p]));

		// pA updated
		assert.strictEqual(byId.get(0)!.tid, 5);
		// pB deleted
		assert.strictEqual(byId.has(1), false);
		// pC added
		assert.strictEqual(byId.has(pCid as number), true);
		assert.strictEqual(after.length, 2);
	});

	test("advancing daysLeft refreshes the free-agency status text on the receiver", async () => {
		resetG();
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		await resetCache({});
		// Receiver is currently on day 29 of free agency.
		await idb.cache.gameAttributes.put({
			key: "phase",
			value: PHASE.FREE_AGENCY,
		});
		await idb.cache.gameAttributes.put({ key: "daysLeft", value: 29 });
		local.statusText = helpers.daysLeft(true, 29);
		assert.ok(local.statusText.includes("29"), local.statusText);

		// The wheel device simmed a day and synced daysLeft → 28.
		changeTracker.disable();
		await applyChangeset(
			{
				changes: [
					{
						store: "gameAttributes",
						id: "daysLeft",
						type: "put",
						value: { key: "daysLeft", value: 28 },
					},
				],
			},
			{ refreshUI: false },
		);

		// g advanced AND the status line was recomputed (not frozen on 29).
		assert.strictEqual(g.get("daysLeft"), 28);
		assert.strictEqual(local.statusText, helpers.daysLeft(true, 28));
		assert.ok(local.statusText.includes("28"), local.statusText);
	});

	test("a local action's capture is NOT eaten by a concurrent remote apply", async () => {
		// The bug: applyChangeset suppresses recording while it writes. If a local
		// sim's writes land in that window, they're dropped from capture and never
		// published (e.g. a sim right after taking the wheel while still catching
		// up). The sync lock must serialize the two so this can't happen.
		resetG();
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();

		const p0 = (await idb.cache.players.getAll()).find((p) => p.pid === 0)!;
		const remote = {
			changes: [
				{
					store: "players" as const,
					id: 0,
					type: "put" as const,
					value: { ...p0, tid: 5 },
				},
			],
		};

		// Fire an apply and a "local action" (dispatch-style: write then capture
		// under the lock) concurrently. Whichever wins the lock, they never
		// interleave, so the local edit to pid 1 must survive in the capture.
		const localAction = runExclusive(async () => {
			const p1 = (await idb.cache.players.getAll()).find((p) => p.pid === 1)!;
			p1.tid = 9;
			await idb.cache.players.put(p1);
			return captureChangeset();
		});
		const applyPromise = applyChangeset(remote, { refreshUI: false });

		const [captured] = await Promise.all([localAction, applyPromise]);

		const ids = captured.changes.map((c) => c.id);
		assert.ok(ids.includes(1), JSON.stringify(captured.changes));
	});

	test("applying a changeset does not itself get recorded", async () => {
		resetG();
		await resetCache({ players: [genPlayer()] });

		const p = (await idb.cache.players.getAll())[0]!;
		p.tid = 9;
		const changeset: any = {
			changes: [{ store: "players", id: p.pid, type: "put", value: p }],
		};

		// Receiver has tracking ON (e.g. it also makes local edits), but applying
		// a remote changeset must not re-capture those writes.
		changeTracker.enable();
		changeTracker.reset();
		await applyChangeset(changeset, { refreshUI: false });

		assert.strictEqual(changeTracker.size(), 0);
	});
});
