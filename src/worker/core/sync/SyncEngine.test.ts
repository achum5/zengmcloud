// The durable outbox opens its own IndexedDB via `openDB`; the node test env has
// no IndexedDB globals (the league cache is mocked), so install fake ones. Must
// come before other imports that might touch IndexedDB.
import "fake-indexeddb/auto";
import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { player } from "../index.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { captureChangeset } from "./changeset.ts";
import { SyncEngine } from "./SyncEngine.ts";
import type {
	Authority,
	ChangesetEntry,
	SyncSubscriber,
	SyncTransport,
} from "./types.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { afterAction } from "./afterAction.ts";
import { setSyncEngine } from "./engineHolder.ts";

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

	// Shared authority ("sim authority") doc, like leagues/{code}/control/authority.
	authority: Authority | undefined;
	private authorityListeners = new Set<(a: Authority | undefined) => void>();

	setAuthority(a: Authority | undefined) {
		this.authority = a;
		for (const listener of this.authorityListeners) {
			listener(a);
		}
	}

	subscribeAuthority(listener: (a: Authority | undefined) => void) {
		this.authorityListeners.add(listener);
		listener(this.authority);
		return () => {
			this.authorityListeners.delete(listener);
		};
	}
}

class FakeTransport implements SyncTransport {
	readonly clientId: string;
	private bus: FakeBus;
	failPing = false;
	failPublish = false;
	pingCount = 0;

	constructor(clientId: string, bus: FakeBus) {
		this.clientId = clientId;
		this.bus = bus;
	}

	async ping() {
		this.pingCount += 1;
		if (this.failPing) {
			throw new Error("not ready");
		}
	}

	async publish(entry: Omit<ChangesetEntry, "seq">) {
		if (this.failPublish) {
			throw new Error("publish failed");
		}
		this.bus.publish(entry);
	}

	fetchAllCount = 0;

	async fetchAllEntries(): Promise<ChangesetEntry[]> {
		this.fetchAllCount += 1;
		// Over-the-wire copy, like the real transport (JSON round-trip).
		return this.bus.entries.map((e) => JSON.parse(JSON.stringify(e)));
	}

	async fetchEntriesSince(
		sinceMs: number,
		pageLimit?: number,
	): Promise<ChangesetEntry[]> {
		const all = this.bus.entries
			.filter((e) => e.seq > sinceMs)
			.sort((a, b) => a.seq - b.seq)
			.map((e) => JSON.parse(JSON.stringify(e)));
		return pageLimit === undefined ? all : all.slice(0, pageLimit);
	}

	async countEntriesSince(sinceMs: number): Promise<number> {
		return this.bus.entries.filter((e) => e.seq > sinceMs).length;
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

	async claimAuthority(holderId: string, holderName: string) {
		this.bus.setAuthority({
			holderId,
			holderName,
			busyUntil: this.bus.authority?.busyUntil,
		});
	}

	subscribeAuthority(onChange: (a: Authority | undefined) => void) {
		return this.bus.subscribeAuthority(onChange);
	}

	async publishBusy(busyUntil: number) {
		const a = this.bus.authority;
		if (a) {
			this.bus.setAuthority({ ...a, busyUntil });
		}
	}
}

const genPlayer = () =>
	player.generate(g.get("userTid"), 30, 2017, true, DEFAULT_LEVEL);

describe("SyncEngine", () => {
	beforeEach(() => {
		setSyncEngine(undefined);
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
		engineA.start();
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
		engineA.start();

		// Device A: two players (pids 0, 1). Move player 0 to another team and
		// publish the resulting changeset.
		resetG();
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();
		await changeTracker.runCaptured(async () => {
			const pA = (await idb.cache.players.getAll()).find((p) => p.pid === 0)!;
			pA.tid = 7;
			await idb.cache.players.put(pA);
		});
		const changeset = await captureChangeset();
		await engineA.onLocalChangeset(changeset, "main.proposeTrade");

		// Device B: independent starting state (player 0 still on original team),
		// then receives A's entry over the (JSON-serialized) wire.
		await resetCache({ players: [genPlayer(), genPlayer()] });
		changeTracker.disable();
		const wireEntry: ChangesetEntry = structuredClone(bus.entries[0]!);
		const applied = await engineB.handleEntry(wireEntry);

		assert.strictEqual(applied, true);
		const after = await idb.cache.players.getAll();
		assert.strictEqual(after.find((p) => p.pid === 0)!.tid, 7);
	});

	test("host chunks a bulk (sim) change; receiver reassembles and applies it", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus));
		host.start();
		await host.claimAuthority();
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
			const wire: ChangesetEntry = structuredClone(entry);
			if (await receiver.handleEntry(wire)) {
				appliedCount += 1;
			}
		}
		assert.strictEqual(appliedCount, 1, "applies exactly once, on completion");

		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, N);
	});

	test("chunks a changeset that's few records but too big for one Firestore doc", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		host.start();
		const receiver = new SyncEngine(new FakeTransport("R", bus));

		resetG();
		await resetCache({});

		// Only 10 records - well under the record-count threshold - but each is
		// large, so the whole changeset far exceeds Firestore's ~1 MB/doc limit.
		// This is the shape of the free-agency phase advance that used to silently
		// fail to publish (a single oversized doc throws).
		const N = 10;
		const big = "x".repeat(120_000);
		const changes = Array.from({ length: N }, (_, i) => ({
			store: "events" as const,
			id: i + 1,
			type: "put" as const,
			value: { eid: i + 1, type: "test", text: big },
		}));
		await host.onLocalChangeset({ changes }, "playMenu.untilFreeAgency");

		// It was split into multiple chunks (not published as one oversized doc).
		assert.ok(bus.entries.length > 1, "should be chunked");
		const batchId = bus.entries[0]!.batchId;
		assert.ok(batchId);
		assert.ok(bus.entries.every((entry) => entry.batchId === batchId));

		// Receiver reassembles and applies every record.
		for (const entry of bus.entries) {
			await receiver.handleEntry(JSON.parse(JSON.stringify(entry)));
		}
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, N);
	});

	test("an upload interrupted mid-publish is retried from the durable outbox", async () => {
		const bus = new FakeBus();

		// A transport that fails its first publish (like the tab closing / a dropped
		// connection mid-send), then works.
		let failOnce = true;
		const flaky: SyncTransport = {
			clientId: "H",
			async publish(entry) {
				if (failOnce) {
					failOnce = false;
					throw new Error("interrupted");
				}
				bus.publish(entry);
			},
			subscribe() {
				return () => {};
			},
			async fetchAllEntries() {
				return [];
			},
			async fetchEntriesSince() {
				return [];
			},
		};

		resetG();
		await resetCache({});

		// A unique code so this test's outbox entries never collide with others'.
		const engine = new SyncEngine(flaky, { code: `test-outbox-${Date.now()}` });

		// The publish fails - nothing reached the bus, but it's now sitting in the
		// durable outbox instead of being lost.
		await engine
			.onLocalChangeset(
				{
					changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
				},
				"main.proposeTrade",
			)
			.catch(() => {});
		assert.strictEqual(bus.entries.length, 0, "failed publish reaches nobody");

		// Next launch/reconnect flushes the outbox → the change finally lands.
		await engine.flushOutbox();
		assert.strictEqual(bus.entries.length, 1, "outbox retry delivers it");
		assert.strictEqual(bus.entries[0]!.changeset.changes.length, 1);
	});

	test("verifyConnection delegates to the transport, defaulting to live", async () => {
		const bus = new FakeBus();

		// A transport with no probe (like the in-memory fake) is treated as live.
		const live = new SyncEngine(new FakeTransport("A", bus));
		assert.strictEqual(await live.verifyConnection(), true);

		// A transport that reports a dead connection is respected → the guard blocks.
		const deadTransport: SyncTransport = {
			clientId: "D",
			async publish() {},
			subscribe() {
				return () => {};
			},
			async verifyConnection() {
				return false;
			},
		};
		const dead = new SyncEngine(deadTransport);
		assert.strictEqual(await dead.verifyConnection(), false);
	});

	test("advances the persisted watermark as it catches up", async () => {
		const bus = new FakeBus();
		const watermarks: number[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onWatermark: (seq) => watermarks.push(seq),
		});
		receiver.start();
		receiver.startChangesSubscription();

		resetG();
		await resetCache({});

		const host = new SyncEngine(new FakeTransport("H", bus));
		host.start();
		await host.claimAuthority();
		await host.onLocalChangeset(
			{ changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }] },
			"main.proposeTrade",
		);
		// Let the receiver's async apply + batch-processed callback run.
		await new Promise((resolve) => setTimeout(resolve, 0));

		assert.ok(watermarks.length >= 1);
		assert.strictEqual(watermarks.at(-1), bus.entries.at(-1)!.seq);
	});

	test("does not silently drop a local bulk change if authority state drifted", async () => {
		const bus = new FakeBus();
		const nonHost = new SyncEngine(new FakeTransport("N", bus));
		nonHost.start();

		const changes = Array.from({ length: 260 }, (_, i) => ({
			store: "events" as const,
			id: i + 1,
			type: "put" as const,
			value: { eid: i + 1 },
		}));
		await nonHost.onLocalChangeset({ changes }, "playMenu.day");

		assert.ok(bus.entries.length > 0);
		assert.strictEqual(bus.entries[0]!.authorId, "N");
	});

	test("does NOT advance the watermark past a changeset that failed to apply", async () => {
		const bus = new FakeBus();
		const watermarks: number[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onWatermark: (seq) => watermarks.push(seq),
		});
		receiver.start();
		receiver.startChangesSubscription();

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

	test("catchUp fetches and applies entries past the watermark", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		// Two changes are in the log (from another device) but this receiver never
		// subscribed - it must pick them up via a targeted catch-up.
		const host = new FakeTransport("H", bus);
		await host.publish({
			id: "a",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
			},
		});
		await host.publish({
			id: "b",
			authorId: "H",
			action: "y",
			changeset: {
				changes: [{ store: "events", id: 2, type: "put", value: { eid: 2 } }],
			},
		});

		const watermarks: number[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onWatermark: (seq) => watermarks.push(seq),
		});
		await receiver.catchUp();

		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, 2);
		assert.strictEqual(watermarks.at(-1), bus.entries.at(-1)!.seq);
	});

	test("catchUp drains a large backlog page by page, banking progress", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		// A backlog far bigger than one page (like a device away for many sims).
		const host = new FakeTransport("H", bus);
		const N = 130; // > several CATCH_UP_PAGE_SIZE (25) pages
		for (let i = 1; i <= N; i++) {
			await host.publish({
				id: `e${i}`,
				authorId: "H",
				action: "x",
				changeset: {
					changes: [{ store: "events", id: i, type: "put", value: { eid: i } }],
				},
			});
		}

		const watermarks: number[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onWatermark: (seq) => watermarks.push(seq),
		});
		await receiver.catchUp();

		// Every entry applied, and the watermark reached the head.
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, N);
		assert.strictEqual(receiver.getPersistedSeq(), bus.entries.at(-1)!.seq);
		assert.strictEqual(receiver.isCaughtUp(), true);
		// Progress was banked incrementally (multiple pages), not just once at the end.
		assert.ok(
			watermarks.length > 1,
			`expected multiple watermark advances, got ${watermarks.length}`,
		);
	});

	test("catchUp reports drain progress and clears when caught up", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		const host = new FakeTransport("H", bus);
		const N = 130;
		for (let i = 1; i <= N; i++) {
			await host.publish({
				id: `e${i}`,
				authorId: "H",
				action: "x",
				changeset: {
					changes: [{ store: "events", id: i, type: "put", value: { eid: i } }],
				},
			});
		}

		const progress: ({ done: number; total: number } | undefined)[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onCatchUpProgress: (p) => progress.push(p),
		});
		await receiver.catchUp();

		const withTotal = progress.filter(Boolean) as {
			done: number;
			total: number;
		}[];
		// Reported a total equal to the backlog, advanced `done` across pages...
		assert.ok(withTotal.length > 1);
		assert.strictEqual(withTotal[0]!.total, N);
		assert.ok(withTotal.some((p) => p.done > 0 && p.done < N));
		assert.strictEqual(withTotal.at(-1)!.done, N);
		// ...and cleared (undefined) once caught up.
		assert.strictEqual(progress.at(-1), undefined);
	});

	test("catchUp shows no progress bar for a trivial gap", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		const host = new FakeTransport("H", bus);
		// Only a couple entries behind - below the progress threshold.
		for (let i = 1; i <= 3; i++) {
			await host.publish({
				id: `e${i}`,
				authorId: "H",
				action: "x",
				changeset: {
					changes: [{ store: "events", id: i, type: "put", value: { eid: i } }],
				},
			});
		}

		const progress: ({ done: number; total: number } | undefined)[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onCatchUpProgress: (p) => progress.push(p),
		});
		await receiver.catchUp();

		// Never surfaced a progress total for a tiny catch-up.
		assert.strictEqual(
			progress.some((p) => p !== undefined),
			false,
		);
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

	test("a failed readiness probe blocks NEW actions (guard) but never a captured delta", async () => {
		const bus = new FakeBus();
		const transport = new FakeTransport("A", bus);
		transport.failPing = true;
		const engine = new SyncEngine(transport);
		engine.start();

		// The action guard's preflight fails, so a new action is refused before it
		// mutates anything...
		let rejected = false;
		try {
			await engine.ensureReady(true);
		} catch {
			rejected = true;
		}
		assert.strictEqual(rejected, true);
		assert.strictEqual(transport.pingCount, 1);

		// ...but a delta that already EXISTS (e.g. a sim that was mid-flight when
		// the connection went bad) must still be handed off and published - the
		// readiness probe must never sit between a local mutation and the log.
		const outcome = await engine.onLocalChangeset(
			{ changes: [{ store: "trade", id: 0, type: "delete" }] },
			"main.clearTrade",
		);
		assert.strictEqual(outcome, "confirmed");
		assert.strictEqual(bus.entries.length, 1);
	});

	test("afterAction keeps a change durably queued (not re-captured) when publishing fails, then delivers it", async () => {
		const bus = new FakeBus();
		const transport = new FakeTransport("A", bus);
		transport.failPublish = true;
		const engine = new SyncEngine(transport);
		engine.start();
		setSyncEngine(engine);

		resetG();
		await resetCache({ players: [genPlayer()] });
		changeTracker.enable();
		changeTracker.reset();

		await changeTracker.runCaptured(async () => {
			const p = (await idb.cache.players.getAll())[0]!;
			p.tid = 7;
			await idb.cache.players.put(p);
		});

		assert.strictEqual(changeTracker.size(), 1);
		// Publish fails → afterAction reports "not confirmed"...
		assert.strictEqual(await afterAction("main", "proposeTrade"), false);
		assert.strictEqual(bus.entries.length, 0);
		// ...but the delta now lives in the upload queue, NOT back in the tracker
		// (the tracker copy was the in-memory buffer a refresh could destroy).
		assert.strictEqual(changeTracker.size(), 0);
		assert.strictEqual(await engine.pendingUploadCount(), 1);

		// Connection recovers → the drain delivers the exact same delta.
		transport.failPublish = false;
		assert.strictEqual(await engine.drainOutbox(), true);
		assert.strictEqual(bus.entries.length, 1);
		assert.strictEqual(await engine.pendingUploadCount(), 0);
	});

	test("queued uploads drain strictly in order across failures", async () => {
		const bus = new FakeBus();
		const transport = new FakeTransport("A", bus);
		const engine = new SyncEngine(transport);
		engine.start();

		// First change fails to upload and stays queued.
		transport.failPublish = true;
		assert.strictEqual(
			await engine.onLocalChangeset(
				{
					changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
				},
				"main.first",
			),
			"queued",
		);

		// A second change made while the connection is bad queues BEHIND it.
		assert.strictEqual(
			await engine.onLocalChangeset(
				{
					changes: [{ store: "events", id: 2, type: "put", value: { eid: 2 } }],
				},
				"main.second",
			),
			"queued",
		);
		assert.strictEqual(await engine.pendingUploadCount(), 2);

		// Recovery publishes them oldest-first - never inverted, so an older value
		// can't clobber a newer one under last-write-wins.
		transport.failPublish = false;
		assert.strictEqual(await engine.drainOutbox(), true);
		assert.deepStrictEqual(
			bus.entries.map((e) => e.action),
			["main.first", "main.second"],
		);
	});

	test("a dead changes listener is re-created by ensureReady instead of wedging forever", async () => {
		const bus = new FakeBus();
		let subscribeCount = 0;
		let lastSubscriber: SyncSubscriber | undefined;
		const transport: SyncTransport = {
			clientId: "A",
			async ping() {},
			async publish(entry) {
				bus.publish(entry);
			},
			subscribe(subscriber) {
				subscribeCount += 1;
				lastSubscriber = subscriber;
				return () => {};
			},
		};
		const engine = new SyncEngine(transport);
		engine.start();
		engine.startChangesSubscription();
		assert.strictEqual(subscribeCount, 1);

		// The Firestore listener dies (terminal - it will never fire again).
		lastSubscriber!.onError?.(new Error("stream died"));
		assert.strictEqual(engine.isReady(), false);

		// The next readiness check re-creates the listener and recovers - this used
		// to throw forever until a page refresh, permanently blocking all uploads.
		await engine.ensureReady(true);
		assert.strictEqual(subscribeCount, 2);
		assert.strictEqual(engine.isReady(), true);

		// And publishing works again.
		const outcome = await engine.onLocalChangeset(
			{ changes: [{ store: "trade", id: 0, type: "delete" }] },
			"main.clearTrade",
		);
		assert.strictEqual(outcome, "confirmed");
		assert.strictEqual(bus.entries.length, 1);

		engine.stop();
	});

	test("a failed apply self-heals on a later catch-up instead of wedging until manual resync", async () => {
		const bus = new FakeBus();

		resetG();
		await resetCache({});

		// A poison entry (nonexistent store) is in the log; the first catch-up
		// fails to apply it → not caught up, watermark pinned before it.
		const host = new FakeTransport("H", bus);
		await host.publish({
			id: "p1",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [{ store: "nope" as any, id: 1, type: "put", value: {} }],
			},
		});

		const receiver = new SyncEngine(new FakeTransport("R", bus));
		assert.strictEqual(await receiver.catchUp(), false);
		assert.strictEqual(receiver.isCaughtUp(), false);
		assert.strictEqual(receiver.getPersistedSeq(), 0);

		// The transient cause clears (here: the entry becomes applicable). The next
		// periodic catch-up re-fetches from the pinned watermark and must genuinely
		// re-apply the SAME entry id - previously `seen` dedup skipped every retry,
		// so one bad apply paused edits forever until a manual full resync.
		bus.entries[0]!.changeset = {
			changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
		};
		assert.strictEqual(await receiver.catchUp(), true);
		assert.strictEqual(receiver.isCaughtUp(), true);
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, 1);
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

	test("the busy lease gates followers but never the sim authority", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		const follower = new SyncEngine(new FakeTransport("F", bus));
		host.start();
		follower.start();

		await host.claimAuthority();
		assert.strictEqual(host.isAuthority(), true);
		assert.strictEqual(follower.isAuthority(), false);

		// Idle: nobody advancing and the follower is caught up → free to act.
		assert.strictEqual(follower.isRoomBusy(), false);
		assert.strictEqual(follower.isCaughtUp(), true);

		// Host starts advancing → marks the room busy; the follower sees it.
		host.markRoomBusy();
		assert.strictEqual(follower.isRoomBusy(), true);
		// The holder is the one advancing, so it never blocks itself.
		assert.strictEqual(host.isRoomBusy(), false);

		// Host finishes publishing → clears the lease; the follower is free again.
		host.clearRoomBusy();
		assert.strictEqual(follower.isRoomBusy(), false);
	});

	test("only the sim authority can mark the room busy", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		const follower = new SyncEngine(new FakeTransport("F", bus));
		host.start();
		follower.start();
		await host.claimAuthority();

		// A follower calling markRoomBusy is a no-op (it isn't the holder), so it
		// can't lock everyone else out.
		follower.markRoomBusy();
		assert.strictEqual(bus.authority?.busyUntil ?? 0, 0);
		assert.strictEqual(host.isRoomBusy(), false);
	});
});
