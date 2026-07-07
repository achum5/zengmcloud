import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { player } from "../index.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { captureChangeset } from "./changeset.ts";
import { SyncEngine } from "./SyncEngine.ts";
import type { ChangesetEntry, SyncSubscriber, SyncTransport } from "./types.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";

// In-memory shared log that several transports connect to, like a Firestore
// collection every client listens on. Assigns the ordering seq itself.
class FakeBus {
	entries: ChangesetEntry[] = [];
	private listeners = new Set<(entry: ChangesetEntry) => void>();
	private seq = 0;

	publish(entry: Omit<ChangesetEntry, "seq">) {
		const full: ChangesetEntry = { ...entry, seq: ++this.seq };
		this.entries.push(full);
		for (const listener of this.listeners) {
			listener(full);
		}
	}

	subscribe(listener: (entry: ChangesetEntry) => void) {
		this.listeners.add(listener);
		return () => {
			this.listeners.delete(listener);
		};
	}
}

class FakeTransport implements SyncTransport {
	readonly clientId: string;
	private bus: FakeBus;

	constructor(clientId: string, bus: FakeBus) {
		this.clientId = clientId;
		this.bus = bus;
	}

	async publish(entry: Omit<ChangesetEntry, "seq">) {
		this.bus.publish(entry);
	}

	async fetchAllEntries(): Promise<ChangesetEntry[]> {
		// Over-the-wire copy, like the real transport (JSON round-trip).
		return this.bus.entries.map((e) => JSON.parse(JSON.stringify(e)));
	}

	subscribe(subscriber: SyncSubscriber) {
		return this.bus.subscribe((entry) => {
			// Mimic the real transport: apply the entry, then signal the batch is
			// processed (a watermark-advance point).
			void (async () => {
				await subscriber.onEntry(entry);
				subscriber.onBatchProcessed?.();
			})();
		});
	}
}

const genPlayer = () =>
	player.generate(g.get("userTid"), 30, 2017, true, DEFAULT_LEVEL);

describe("SyncEngine", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("publishes local changes to the shared log with an ordering seq", async () => {
		const bus = new FakeBus();
		const received: ChangesetEntry[] = [];
		new FakeTransport("B", bus).subscribe({
			onEntry: (entry) => {
				received.push(entry);
				return true;
			},
		});

		const engineA = new SyncEngine(new FakeTransport("A", bus));
		await engineA.onLocalChangeset(
			{ changes: [{ store: "trade", id: 0, type: "delete" }] },
			"main.clearTrade",
		);

		assert.strictEqual(received.length, 1);
		assert.strictEqual(received[0]!.authorId, "A");
		assert.strictEqual(received[0]!.seq, 1);
		assert.strictEqual(received[0]!.action, "main.clearTrade");
	});

	test("a trade on device A is applied to device B's cache", async () => {
		const bus = new FakeBus();
		const engineA = new SyncEngine(new FakeTransport("A", bus));
		const engineB = new SyncEngine(new FakeTransport("B", bus));

		// Device A: two players (pids 0, 1). Move player 0 to another team and
		// publish the resulting changeset.
		resetG();
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();
		const pA = (await idb.cache.players.getAll()).find((p) => p.pid === 0)!;
		pA.tid = 7;
		await idb.cache.players.put(pA);
		const changeset = await captureChangeset();
		await engineA.onLocalChangeset(changeset, "main.proposeTrade");

		// Device B: independent starting state (player 0 still on original team),
		// then receives A's entry over the (JSON-serialized) wire.
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.disable();
		const wireEntry: ChangesetEntry = JSON.parse(
			JSON.stringify(bus.entries[0]),
		);
		const applied = await engineB.handleEntry(wireEntry);

		assert.strictEqual(applied, true);
		const after = await idb.cache.players.getAll();
		assert.strictEqual(after.find((p) => p.pid === 0)!.tid, 7);
	});

	test("host chunks a bulk (sim) change; receiver reassembles and applies it", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		// isHost now means "claim the wheel on start" - starting is what makes this
		// device the advance-authority allowed to broadcast bulk sims.
		host.start();
		const receiver = new SyncEngine(new FakeTransport("R", bus));

		resetG();
		await resetCache({});

		// A bulk changeset (> the single-entry threshold), like a day's sim.
		const N = 260;
		const changes = Array.from({ length: N }, (_, i) => ({
			store: "events" as const,
			id: i + 1,
			type: "put" as const,
			value: { eid: i + 1, type: "test", text: "x".repeat(40) },
		}));
		await host.onLocalChangeset({ changes }, "playMenu.day");

		// It was split into multiple chunks sharing one batchId.
		assert.ok(bus.entries.length > 1, "should be chunked");
		const batchId = bus.entries[0]!.batchId;
		assert.ok(batchId);
		assert.ok(bus.entries.every((entry) => entry.batchId === batchId));
		assert.strictEqual(bus.entries[0]!.chunkCount, bus.entries.length);

		// Receiver applies only once the whole batch has arrived.
		let appliedCount = 0;
		for (const entry of bus.entries) {
			const wire: ChangesetEntry = JSON.parse(JSON.stringify(entry));
			if (await receiver.handleEntry(wire)) {
				appliedCount += 1;
			}
		}
		assert.strictEqual(appliedCount, 1, "applies exactly once, on completion");

		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, N);
	});

	test("advances the persisted watermark as it catches up", async () => {
		const bus = new FakeBus();
		const watermarks: number[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onWatermark: (seq) => watermarks.push(seq),
		});
		receiver.start();

		resetG();
		await resetCache({});

		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		await host.onLocalChangeset(
			{ changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }] },
			"main.proposeTrade",
		);
		// Let the receiver's async apply + batch-processed callback run.
		await new Promise((resolve) => setTimeout(resolve, 0));

		assert.ok(watermarks.length >= 1);
		assert.strictEqual(watermarks.at(-1), bus.entries.at(-1)!.seq);
	});

	test("a non-host does not broadcast bulk (sim) changes", async () => {
		const bus = new FakeBus();
		const nonHost = new SyncEngine(new FakeTransport("N", bus));

		const changes = Array.from({ length: 260 }, (_, i) => ({
			store: "events" as const,
			id: i + 1,
			type: "put" as const,
			value: { eid: i + 1 },
		}));
		await nonHost.onLocalChangeset({ changes }, "playMenu.day");

		assert.strictEqual(bus.entries.length, 0);
	});

	test("does NOT advance the watermark past a changeset that failed to apply", async () => {
		const bus = new FakeBus();
		const watermarks: number[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onWatermark: (seq) => watermarks.push(seq),
		});
		receiver.start();

		resetG();
		await resetCache({});

		const host = new FakeTransport("H", bus);

		// A good entry: applies, watermark advances to its seq.
		await host.publish({
			id: "good",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
			},
		});
		await new Promise((resolve) => setTimeout(resolve, 0));
		const afterGood = watermarks.at(-1);
		assert.ok(afterGood !== undefined);

		// A poison entry targeting a store that doesn't exist → apply throws.
		await host.publish({
			id: "poison",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [{ store: "nope" as any, id: 1, type: "put", value: {} }],
			},
		});
		await new Promise((resolve) => setTimeout(resolve, 0));

		// The watermark must NOT have moved past the failed entry - otherwise a
		// reconnect would skip it forever (the silent-divergence bug).
		assert.strictEqual(watermarks.at(-1), afterGood);
	});

	test("resyncAll re-reads the whole log and applies every entry from scratch", async () => {
		const bus = new FakeBus();

		resetG();
		await resetCache({});

		// Two changes land in the shared log (from another device) while we're not
		// listening - simulating a device that fell behind.
		const host = new FakeTransport("H", bus);
		await host.publish({
			id: "a",
			authorId: "H",
			action: "main.proposeTrade",
			changeset: {
				changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
			},
		});
		await host.publish({
			id: "b",
			authorId: "H",
			action: "main.signFreeAgent",
			changeset: {
				changes: [{ store: "events", id: 2, type: "put", value: { eid: 2 } }],
			},
		});

		// A fresh device that never subscribed forces a full resync.
		const watermarks: number[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onWatermark: (seq) => watermarks.push(seq),
		});
		const result = await receiver.resyncAll();

		assert.strictEqual(result.total, 2);
		assert.strictEqual(result.applied, 2);
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, 2);
		// Watermark caught up to the newest entry.
		assert.strictEqual(watermarks.at(-1), bus.entries.at(-1)!.seq);
	});

	test("skips self-authored and duplicate entries", async () => {
		const bus = new FakeBus();
		const engineA = new SyncEngine(new FakeTransport("A", bus));
		const engineB = new SyncEngine(new FakeTransport("B", bus));

		resetG();
		await resetCache({});

		const entry: ChangesetEntry = {
			id: "e1",
			authorId: "A",
			seq: 1,
			action: "test",
			changeset: { changes: [] },
		};

		// A authored it → A ignores it.
		assert.strictEqual(await engineA.handleEntry(entry), false);
		// B applies it the first time...
		assert.strictEqual(await engineB.handleEntry(entry), true);
		// ...but not a second time (dedup by id).
		assert.strictEqual(await engineB.handleEntry(entry), false);
	});
});
