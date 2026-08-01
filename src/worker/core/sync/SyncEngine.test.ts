// The durable outbox opens its own IndexedDB via `openDB`; the node test env has
// no IndexedDB globals (the league cache is mocked), so install fake ones. Must
// come before other imports that might touch IndexedDB.
import "fake-indexeddb/auto";
import { assert, beforeEach, describe, test, vi } from "vitest";
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
import { PHASE } from "../../../common/constants.ts";
import { afterAction } from "./afterAction.ts";
import { setSyncEngine } from "./engineHolder.ts";

// In-memory shared log that several transports connect to, like a Firestore
// collection every client listens on. Assigns the ordering seq itself.
// Incompressible filler. Bulk payloads are gzipped before chunking now, so
// repeated characters ("x".repeat(n)) collapse to almost nothing and a payload
// meant to span several Firestore docs would arrive as a single chunk - which
// would quietly stop these tests from exercising the multi-chunk machinery at
// all. Deterministic pseudo-random base36 keeps them genuinely large on the wire.
const bulkText = (n: number, salt = 0): string => {
	let out = "";
	let seed = n + 1 + salt * 7919;
	while (out.length < n) {
		seed = (seed * 1103515245 + 12345) & 0x7fffffff;
		out += seed.toString(36);
	}
	return out.slice(0, n);
};

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
	// Make the by-batchId rescue come up empty, the way it does when the query
	// can't run or hasn't caught up yet.
	blindBatchFetch = false;

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

	// By-batchId rescue fetch, like the real transport's Firestore equality
	// query - no seq range, so it reaches chunks below any watermark.
	async fetchBatchEntries(batchId: string): Promise<ChangesetEntry[]> {
		if (this.blindBatchFetch) {
			return [];
		}
		return this.bus.entries
			.filter((e) => e.batchId === batchId)
			.map((e) => JSON.parse(JSON.stringify(e)));
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

		// A bulk changeset (> the single-entry threshold), like a day's sim. Each
		// record is large enough that the serialized whole exceeds one part.
		const N = 260;
		const changes = Array.from({ length: N }, (_, i) => ({
			store: "events" as const,
			id: i + 1,
			type: "put" as const,
			value: { eid: i + 1, type: "test", text: bulkText(2000, i) },
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

	test("bulk payloads go on the wire compressed, and shrink the doc count", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus));
		host.start();
		await host.claimAuthority();
		resetG();
		await resetCache({});

		// Sim-shaped: many records that repeat the same KEYS (the real reason a
		// sim day compresses well - every player record has identical field names).
		const N = 400;
		const changes = Array.from({ length: N }, (_, i) => ({
			store: "events" as const,
			id: i + 1,
			type: "put" as const,
			value: {
				eid: i + 1,
				type: "gameAttribute",
				season: 2003,
				text: `Player ${i} recorded a stat line in game ${i % 15}`,
				pids: [i, i + 1, i + 2],
				tids: [i % 30, (i + 1) % 30],
			},
		}));
		const rawChars = JSON.stringify({ changes }).length;
		await host.onLocalChangeset({ changes }, "playMenu.sim");

		// Every chunk is a slice of ONE gzip payload, so the marker sits at the
		// front of chunk 0.
		assert.ok(bus.entries.length >= 1);
		assert.ok(
			bus.entries[0]!.payloadPart?.startsWith("GZ1:"),
			"bulk payload must be gzipped on the wire",
		);

		const wireChars = bus.entries.reduce(
			(sum, entry) => sum + (entry.payloadPart?.length ?? 0),
			0,
		);
		assert.ok(
			wireChars < rawChars / 2,
			`expected >2x smaller on the wire, got ${rawChars} -> ${wireChars}`,
		);

		// And it still round-trips to exactly the same records.
		const receiver = new SyncEngine(new FakeTransport("R", bus));
		for (const entry of bus.entries) {
			await receiver.handleEntry(structuredClone(entry));
		}
		assert.strictEqual((await idb.cache.events.getAll()).length, N);
	});

	test("a plain (uncompressed) batch from an older client still applies", async () => {
		// Mixed-version room: entries already in the log predate compression, and
		// a device that never got the update keeps publishing plain JSON. The
		// payload is self-describing, so both must keep working forever.
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		const changes = Array.from({ length: 3 }, (_, i) => ({
			store: "events" as const,
			id: i + 1,
			type: "put" as const,
			value: { eid: i + 1 },
		}));
		const serialized = JSON.stringify({ changes });
		const mid = Math.floor(serialized.length / 2);

		const old = new FakeTransport("OLD", bus);
		const batchId = "PLAINBATCH";
		for (const [i, part] of [
			serialized.slice(0, mid),
			serialized.slice(mid),
		].entries()) {
			await old.publish({
				id: `plain${i}`,
				authorId: "OLD",
				action: "playMenu.sim",
				batchId,
				chunkIndex: i,
				chunkCount: 2,
				changeset: { changes: [] },
				payloadPart: part,
			});
		}

		const receiver = new SyncEngine(new FakeTransport("R", bus));
		for (const entry of bus.entries) {
			await receiver.handleEntry(structuredClone(entry));
		}
		assert.strictEqual((await idb.cache.events.getAll()).length, 3);
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
		const big = bulkText(120_000);
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

	test("a single record larger than one Firestore doc still ships (string parts)", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		host.start();
		const receiver = new SyncEngine(new FakeTransport("R", bus));

		resetG();
		await resetCache({});

		// ONE record whose serialized form far exceeds the per-doc part size. The
		// old per-record chunking made this an unshippable chunk that Firestore
		// rejected forever - and, at the head of the FIFO queue, it blocked every
		// upload behind it permanently.
		const huge = bulkText(800_000);
		await host.onLocalChangeset(
			{
				changes: [
					{
						store: "events",
						id: 1,
						type: "put",
						value: { eid: 1, text: huge },
					},
				],
			},
			"playMenu.day",
		);

		assert.ok(bus.entries.length > 1, "must be split across multiple docs");
		assert.ok(bus.entries.every((e) => typeof e.payloadPart === "string"));

		// Receiver reassembles the parts and applies the full record.
		for (const entry of bus.entries) {
			await receiver.handleEntry(structuredClone(entry));
		}
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, 1);
		assert.strictEqual((events[0] as any).text.length, huge.length);
	});

	test("a partially-received bulk batch is rescued by batchId instead of resetting/abandoning", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		host.start();
		const receiver = new SyncEngine(new FakeTransport("R", bus));

		resetG();
		await resetCache({});

		// A bulk (sim-day-sized) changeset, fully uploaded to the log.
		const N = 260;
		const changes = Array.from({ length: N }, (_, i) => ({
			store: "events" as const,
			id: i + 1,
			type: "put" as const,
			value: { eid: i + 1, type: "test", text: bulkText(2000, i) },
		}));
		await host.onLocalChangeset({ changes }, "playMenu.day");
		assert.ok(bus.entries.length > 1, "should be chunked");

		// The receiver only ever RECEIVES the first chunk via ordered delivery
		// (e.g. it restarted mid-batch and its watermark moved past the rest).
		await receiver.handleEntry(structuredClone(bus.entries[0]!));

		// Two sweep passes: first records the stale sighting, second rescues the
		// batch by batchId - completing and applying it, with no reset cycles.
		await (receiver as any).sweepStaleBatches();
		await (receiver as any).sweepStaleBatches();

		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, N, "rescue must apply the full batch");
		assert.strictEqual((receiver as any).pendingBatches.size, 0);
	});

	test("late chunks after abandonment resurrect the batch and apply the missed day", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		host.start();
		const receiver = new SyncEngine(new FakeTransport("R", bus));

		resetG();
		await resetCache({});

		const N = 260;
		const changes = Array.from({ length: N }, (_, i) => ({
			store: "events" as const,
			id: i + 1,
			type: "put" as const,
			value: { eid: i + 1, type: "test", text: bulkText(2000, i) },
		}));
		await host.onLocalChangeset({ changes }, "playMenu.day");
		const allChunks = [...bus.entries];
		assert.ok(allChunks.length > 1, "should be chunked");
		const batchId = allChunks[0]!.batchId!;

		// The simmer was killed mid-upload: only the FIRST chunk made it to the
		// log. (The rest sit in its outbox.)
		bus.entries.length = 0;
		bus.entries.push(allChunks[0]!);

		// The receiver gets that chunk, then another author's entry lands - the
		// log has "moved past" the half-uploaded batch.
		await receiver.handleEntry(structuredClone(allChunks[0]!));
		await receiver.handleEntry({
			id: "other-1",
			authorId: "X",
			action: "main.sign",
			seq: allChunks[allChunks.length - 1]!.seq + 10,
			changeset: { changes: [{ store: "trade", id: 0, type: "delete" }] },
		});

		// Sweeps judge it provably dead (rescue confirms the chunks are NOT in
		// the log), reset once, then abandon - remembering the batchId.
		await (receiver as any).sweepStaleBatches();
		await receiver.handleEntry(structuredClone(allChunks[0]!)); // reset refetch
		await (receiver as any).sweepStaleBatches();
		await (receiver as any).sweepStaleBatches();
		assert.ok(
			(receiver as any).abandonedBatches.has(batchId),
			"batch should be abandoned with memory",
		);
		assert.strictEqual((await idb.cache.events.getAll()).length, 0);

		// The simmer comes back and drains its outbox: the remaining chunks land
		// in the log. Delivery of ONE of them must resurrect the whole batch via
		// the by-batchId rescue - the missed day applies with no manual resync.
		bus.entries.push(...allChunks.slice(1));
		await receiver.handleEntry(structuredClone(allChunks[1]!));

		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, N, "resurrected batch must fully apply");
		assert.strictEqual((receiver as any).abandonedBatches.has(batchId), false);
	});

	test("a resurrected batch cannot drag the phase backward", async () => {
		// The reported bug, end to end. A device sitting in RESIGN_PLAYERS snapped
		// back to AFTER_DRAFT the moment it took an action: an old bulk batch from
		// around the draft had been abandoned with chunks missing, the room moved
		// on a phase, the author later uploaded the rest, and the by-batchId
		// rescue (which bypasses the watermark by design) replayed that stale
		// changeset - gameAttributes and all - over the newer phase.
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		host.start();
		const receiver = new SyncEngine(new FakeTransport("R", bus));

		resetG();
		g.setWithoutSavingToDB("phase", PHASE.AFTER_DRAFT);
		await resetCache({});
		await idb.cache.gameAttributes.put({
			key: "phase",
			value: PHASE.AFTER_DRAFT,
		});

		// A big changeset from the draft era, carrying the phase of its moment.
		const N = 260;
		const changes = [
			...Array.from({ length: N }, (_, i) => ({
				store: "events" as const,
				id: i + 1,
				type: "put" as const,
				value: { eid: i + 1, type: "test", text: bulkText(2000, i) },
			})),
			{
				store: "gameAttributes" as const,
				id: "phase",
				type: "put" as const,
				value: { key: "phase", value: PHASE.AFTER_DRAFT },
			},
		];
		await host.onLocalChangeset({ changes }, "playMenu.untilResignPlayers");
		const allChunks = [...bus.entries];
		assert.ok(allChunks.length > 1, "should be chunked");
		const batchId = allChunks[0]!.batchId!;

		// The author died mid-upload: only chunk 0 reached the log.
		bus.entries.length = 0;
		bus.entries.push(allChunks[0]!);
		await receiver.handleEntry(structuredClone(allChunks[0]!));

		// The room moves on a phase, and the receiver applies THAT in order.
		const advanceEntry: ChangesetEntry = {
			id: "advance-1",
			authorId: "X",
			action: "playMenu.untilResignPlayers",
			seq: allChunks.at(-1)!.seq + 10,
			changeset: {
				changes: [
					{
						store: "gameAttributes",
						id: "phase",
						type: "put",
						value: { key: "phase", value: PHASE.RESIGN_PLAYERS },
					},
				],
			},
		};
		bus.entries.push(advanceEntry);
		await receiver.handleEntry(structuredClone(advanceEntry));
		assert.strictEqual(
			g.get("phase"),
			PHASE.RESIGN_PLAYERS,
			"receiver should have advanced",
		);

		// The half-batch is judged dead, so the watermark moves past it.
		await (receiver as any).sweepStaleBatches();
		await receiver.handleEntry(structuredClone(allChunks[0]!));
		await (receiver as any).sweepStaleBatches();
		await (receiver as any).sweepStaleBatches();
		assert.ok((receiver as any).abandonedBatches.has(batchId));

		// The author comes back and drains its outbox. A late chunk resurrects
		// the batch - which used to replay the old phase straight over the new.
		bus.entries.push(...allChunks.slice(1));
		await receiver.handleEntry(structuredClone(allChunks[1]!));

		assert.strictEqual(
			g.get("phase"),
			PHASE.RESIGN_PLAYERS,
			"a stale batch must never move the phase backward",
		);
		// And the batch's DATA is not lost - the ordered resync delivered it.
		assert.strictEqual(
			(await idb.cache.events.getAll()).length,
			N,
			"the missed data should still land, just in order",
		);
	});

	test("an oversized legacy outbox entry is migrated instead of wedging the queue", async () => {
		const bus = new FakeBus();
		const transport = new FakeTransport("H", bus);
		const engine = new SyncEngine(transport, { isHost: true });
		engine.start();

		resetG();
		await resetCache({});

		// A stuck legacy-format chunk (as produced before string chunking): chunk
		// 1 of 2 of a batch whose sibling (chunk 0) already published. Too big for
		// one doc, it could never publish and blocked the queue forever.
		const huge = bulkText(800_000);
		const legacy = {
			id: "legacy-big",
			authorId: "H",
			action: "playMenu.sim",
			batchId: "old-batch",
			chunkIndex: 1,
			chunkCount: 2,
			changeset: {
				changes: [
					{
						store: "events" as const,
						id: 7,
						type: "put" as const,
						value: { eid: 7, text: huge },
					},
				],
			},
		};
		// Inject it as a stranded queued upload (memory queue - no room code).
		(engine as any).memoryQueue.push(legacy);

		assert.strictEqual(await engine.drainOutbox(), true, "queue must drain");

		// An empty stand-in completed the legacy batch position...
		const standIn = bus.entries.find((e) => e.id === "legacy-big");
		assert.ok(standIn);
		assert.strictEqual(standIn!.changeset.changes.length, 0);
		assert.strictEqual(standIn!.batchId, "old-batch");
		assert.strictEqual(standIn!.chunkIndex, 1);
		// ...and the actual content shipped as a new string-part batch.
		const parts = bus.entries.filter((e) => e.payloadPart !== undefined);
		assert.ok(parts.length >= 2);

		// A receiver holding the batch's other chunk ends up with ALL the data.
		const receiver = new SyncEngine(new FakeTransport("R", bus));
		await receiver.handleEntry({
			id: "legacy-small",
			authorId: "H",
			seq: 0.5,
			action: "playMenu.sim",
			batchId: "old-batch",
			chunkIndex: 0,
			chunkCount: 2,
			changeset: {
				changes: [{ store: "events", id: 6, type: "put", value: { eid: 6 } }],
			},
		});
		for (const entry of bus.entries) {
			await receiver.handleEntry(structuredClone(entry));
		}
		assert.strictEqual(
			(receiver as any).pendingBatches.size,
			0,
			"no half-open batch left",
		);
		const events = await idb.cache.events.getAll();
		const byId = new Map(events.map((e: any) => [e.eid, e]));
		assert.ok(byId.has(6), "sibling chunk's record applied");
		assert.strictEqual(byId.get(7)!.text.length, huge.length);
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

	test("tracks live changes-listener deliveries for the catch-up poll gate", async () => {
		const bus = new FakeBus();
		const receiver = new SyncEngine(new FakeTransport("R", bus));
		receiver.start();
		receiver.startChangesSubscription();

		resetG();
		await resetCache({});

		// No delivery yet - the poll must read this as stale and keep probing. The
		// transport's GLOBAL contact time is not a substitute: any listener (e.g.
		// the authority doc during a sim) refreshes that, so gating on it starved a
		// behind follower of its backstop exactly while the room was active.
		assert.strictEqual(receiver.getLastChangesDeliveryAt(), 0);

		const host = new SyncEngine(new FakeTransport("H", bus));
		host.start();
		await host.claimAuthority();
		await host.onLocalChangeset(
			{ changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }] },
			"main.proposeTrade",
		);
		await new Promise((resolve) => setTimeout(resolve, 0));

		assert.ok(receiver.getLastChangesDeliveryAt() > 0);
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

	test("isCaughtUp is confidently-wrong after a missed delivery; the forced catchUp corrects it (mid-day sim-handoff guard)", async () => {
		// This is the invariant the sim/advance guard relies on: because a silently
		// stalled changes listener (socket live, deliveries stopped) leaves this
		// device unaware of new entries, isCaughtUp() - which is relative to what the
		// device has SEEN - reports a confidently-wrong "true". A timeline advance run
		// on that stale state can clobber a result another device just recorded (the
		// mid-day simmer-handoff hazard). The guard forces catchUp() before advancing;
		// this proves that closes the window.
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		// A receiver that is "caught up" with nothing pending. (Not subscribed, which
		// is behaviorally identical to a silently-dead listener for a missed entry.)
		const receiver = new SyncEngine(new FakeTransport("R", bus));
		assert.strictEqual(receiver.isCaughtUp(), true);

		// Another device records a live-sim result and publishes it, but this device's
		// listener never delivers it.
		const other = new FakeTransport("A", bus);
		await other.publish({
			id: "s1",
			authorId: "A",
			action: "actions.liveGame",
			changeset: {
				changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
			},
		});

		// The trap: still reports caught-up, and the cache is stale.
		assert.strictEqual(receiver.isCaughtUp(), true);
		assert.strictEqual((await idb.cache.events.getAll()).length, 0);

		// The guard's forced drain reaches the head, applies the missed result...
		assert.strictEqual(await receiver.catchUp(), true);
		// ...so an advance now runs on fresh state instead of clobbering it.
		assert.strictEqual((await idb.cache.events.getAll()).length, 1);
		assert.strictEqual(receiver.isCaughtUp(), true);
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

	test("a stop() mid-fetch stops a zombie pass re-showing the catch-up bar", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		const host = new FakeTransport("H", bus);
		const N = 130; // big enough to page and surface the progress bar
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

		// Gate the SECOND page fetch so we can stop() the engine while a fetch is
		// in flight - the reconnect race where teardown replaces the engine and
		// clears the pill, then this in-flight pass resolves and would re-push it.
		let calls = 0;
		let signalSecondFetch: () => void = () => {};
		const secondFetchStarted = new Promise<void>((resolve) => {
			signalSecondFetch = resolve;
		});
		let releaseSecondFetch: () => void = () => {};
		const gate = new Promise<void>((resolve) => {
			releaseSecondFetch = resolve;
		});
		class GatedTransport extends FakeTransport {
			async fetchEntriesSince(sinceMs: number, pageLimit?: number) {
				calls += 1;
				if (calls === 2) {
					signalSecondFetch();
					await gate;
				}
				return super.fetchEntriesSince(sinceMs, pageLimit);
			}
		}

		const progress: ({ done: number; total: number } | undefined)[] = [];
		const receiver = new SyncEngine(new GatedTransport("R", bus), {
			onCatchUpProgress: (p) => progress.push(p),
		});

		const drain = receiver.catchUp();
		await secondFetchStarted; // page 1 applied + bar shown; page 2 fetch pending
		assert.ok(
			progress.some((p) => p !== undefined),
			"the bar should have shown after page 1",
		);

		const pushesBeforeStop = progress.length;
		receiver.stop();
		releaseSecondFetch();
		await drain;

		// The stopped (zombie) pass must not push any more progress - otherwise it
		// re-shows a bar the new session already cleared, leaving it stuck.
		assert.strictEqual(
			progress.length,
			pushesBeforeStop,
			`pushed after stop: ${JSON.stringify(progress.slice(pushesBeforeStop))}`,
		);
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

	test("a bulk batch stuck missing a chunk is reset and rebuilds once the chunk exists", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		// A 3-chunk bulk batch with chunk 1 missing from the log (lost upload).
		const host = new FakeTransport("H", bus);
		const chunk = (i: number) => ({
			id: `c${i}`,
			authorId: "H",
			action: "playMenu.sim",
			batchId: "B",
			chunkIndex: i,
			chunkCount: 3,
			changeset: {
				changes: [
					{
						store: "events" as const,
						id: i + 1,
						type: "put" as const,
						value: { eid: i + 1 },
					},
				],
			},
		});
		await host.publish(chunk(0));
		await host.publish(chunk(2));

		const receiver = new SyncEngine(new FakeTransport("R", bus), {});

		// Two full walks to the head with the batch making no progress: the
		// watermark stays pinned (incomplete batch), then the batch is reset so a
		// later pass can rebuild it from a clean re-fetch.
		assert.strictEqual(await receiver.catchUp(), true);
		assert.strictEqual(receiver.isCaughtUp(), false);
		assert.strictEqual(await receiver.catchUp(), true);

		// The missing chunk finally lands in the log (e.g. the author's outbox
		// recovered). The next pass re-fetches all three and applies the batch.
		await host.publish(chunk(1));
		assert.strictEqual(await receiver.catchUp(), true);

		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, 3);
		assert.strictEqual(receiver.isCaughtUp(), true);
		assert.strictEqual(receiver.getPersistedSeq(), bus.entries.at(-1)!.seq);
	});

	test("a dead batch is abandoned once its author has published past it", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		// An orphaned batch: chunk 1 of 3 was never enqueued by the author (a
		// partial-enqueue failure), and the author then MOVED ON - it re-published
		// the data as a fresh complete batch and kept simming. FIFO means the
		// missing chunk can never arrive.
		const host = new FakeTransport("H", bus);
		const chunk = (i: number) => ({
			id: `dead${i}`,
			authorId: "H",
			action: "playMenu.sim",
			batchId: "DEAD",
			chunkIndex: i,
			chunkCount: 3,
			changeset: {
				changes: [
					{
						store: "events" as const,
						id: 100 + i,
						type: "put" as const,
						value: { eid: 100 + i },
					},
				],
			},
		});
		await host.publish(chunk(0));
		await host.publish(chunk(2));
		// The author's later, healthy entry - proof it moved past the dead batch.
		await host.publish({
			id: "after",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
			},
		});

		const receiver = new SyncEngine(new FakeTransport("R", bus), {});

		// Fast abandonment: pass 1 sights the stale batch, pass 2 resets it for
		// one clean rebuild, pass 3 sees it STILL didn't complete despite proof it
		// never can - abandoned. More passes than that means the old slow churn
		// (5 rebuild cycles, each re-fetching the whole pinned tail with a visible
		// progress bar) has crept back.
		let passes = 0;
		for (let i = 0; i < 20; i++) {
			passes += 1;
			assert.strictEqual(await receiver.catchUp(), true);
			if (receiver.isCaughtUp()) {
				break;
			}
		}
		assert.ok(passes <= 3, `abandonment took ${passes} passes`);

		// The dead batch was abandoned: watermark advanced to the head, the
		// healthy entry applied, and sync is no longer wedged.
		assert.strictEqual(receiver.isCaughtUp(), true);
		assert.strictEqual(receiver.getPersistedSeq(), bus.entries.at(-1)!.seq);
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, 1);
	});

	test("abandoning a batch fires onResyncNeeded so the skip can self-heal after a reload", async () => {
		// The field failure: a follower never received all chunks of the ready-up
		// phase-advance batch, abandoned it, and banked the watermark past it. The
		// in-memory recovery is lost on reload, so the device stays behind (stuck on
		// the old phase). onResyncNeeded is the hook that lets connect persist a
		// durable marker and self-heal with a full resync next connect.
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		const host = new FakeTransport("H", bus);
		const chunk = (i: number) => ({
			id: `dead${i}`,
			authorId: "H",
			action: "playMenu.sim",
			batchId: "DEAD",
			chunkIndex: i,
			chunkCount: 3,
			changeset: {
				changes: [
					{
						store: "events" as const,
						id: 100 + i,
						type: "put" as const,
						value: { eid: 100 + i },
					},
				],
			},
		});
		await host.publish(chunk(0));
		await host.publish(chunk(2));
		await host.publish({
			id: "after",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
			},
		});

		let resyncNeeded = 0;
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onResyncNeeded: () => {
				resyncNeeded += 1;
			},
		});
		for (let i = 0; i < 20; i++) {
			await receiver.catchUp();
			if (receiver.isCaughtUp()) {
				break;
			}
		}

		assert.strictEqual(receiver.isCaughtUp(), true);
		assert.ok(resyncNeeded >= 1, "onResyncNeeded should fire on abandonment");
	});

	test("resyncAll self-heals a batch this device abandoned, once its chunks are in the log", async () => {
		// The recovery for the field failure: the author's phase-advance chunks ARE
		// in the cloud (the simmer stayed correct), but this follower abandoned the
		// batch on a timing hiccup and banked past it. A full resync re-reads the
		// whole log - reaching the chunks below its watermark - and re-applies them.
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		const host = new FakeTransport("H", bus);
		const chunk = (i: number) => ({
			id: `dead${i}`,
			authorId: "H",
			action: "playMenu.sim",
			batchId: "DEAD",
			chunkIndex: i,
			chunkCount: 3,
			changeset: {
				changes: [
					{
						store: "events" as const,
						id: 100 + i,
						type: "put" as const,
						value: { eid: 100 + i },
					},
				],
			},
		});
		await host.publish(chunk(0));
		await host.publish(chunk(2));
		await host.publish({
			id: "after",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
			},
		});

		const receiver = new SyncEngine(new FakeTransport("R", bus), {});
		for (let i = 0; i < 20; i++) {
			await receiver.catchUp();
			if (receiver.isCaughtUp()) {
				break;
			}
		}
		// Abandoned: the batch's records are NOT applied (only the healthy entry is).
		let events = await idb.cache.events.getAll();
		assert.deepStrictEqual(
			events.map((e: any) => e.eid).sort((a: number, b: number) => a - b),
			[1],
		);

		// The missing chunk finally lands in the log (author finished uploading).
		await host.publish(chunk(1));

		// Reload + reconnect: a FRESH engine (no in-memory abandoned-batch state)
		// runs the self-healing full resync, exactly as connect does when the
		// durable syncResyncNeeded marker is set. It re-reads the whole log - all
		// three chunks now present - and applies the skipped batch.
		const reloaded = new SyncEngine(new FakeTransport("R", bus), {});
		const result = await reloaded.resyncAll();
		assert.strictEqual(result.failed, false);
		assert.strictEqual(result.incomplete, 0);
		events = await idb.cache.events.getAll();
		assert.deepStrictEqual(
			events.map((e: any) => e.eid).sort((a: number, b: number) => a - b),
			[1, 100, 101, 102],
		);
	});

	test("a dead batch is abandoned when the LOG moves past it, even if its own author never did", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		// The wedge from the field: author H uploaded chunk 0 and 2 of a 3-chunk
		// batch (chunk 1 lost mid-upload), then H's session ENDED - H never
		// published anything after. But the room kept going: a DIFFERENT device X
		// published tens of entries past it. authorProgress(H) never exceeds the
		// dead batch, so the old author-only rule pinned the watermark forever.
		const H = new FakeTransport("H", bus);
		const chunk = (i: number) => ({
			id: `dead${i}`,
			authorId: "H",
			action: "playMenu.day",
			batchId: "DEAD",
			chunkIndex: i,
			chunkCount: 3,
			changeset: {
				changes: [
					{
						store: "events" as const,
						id: 200 + i,
						type: "put" as const,
						value: { eid: 200 + i },
					},
				],
			},
		});
		await H.publish(chunk(0));
		await H.publish(chunk(2));

		// A different device's healthy entries land well past the dead batch.
		const X = new FakeTransport("X", bus);
		for (let i = 0; i < 5; i++) {
			await X.publish({
				id: `x${i}`,
				authorId: "X",
				action: "main.proposeTrade",
				changeset: {
					changes: [{ store: "events", id: i, type: "put", value: { eid: i } }],
				},
			});
		}

		const receiver = new SyncEngine(new FakeTransport("R", bus), {});
		let passes = 0;
		for (let i = 0; i < 20; i++) {
			passes += 1;
			assert.strictEqual(await receiver.catchUp(), true);
			if (receiver.isCaughtUp()) {
				break;
			}
		}
		assert.ok(passes <= 3, `abandonment took ${passes} passes`);

		// The dead batch was abandoned via the log-moved-past rule: caught up to
		// the head, X's five entries applied (H's orphaned data skipped).
		assert.strictEqual(receiver.isCaughtUp(), true);
		assert.strictEqual(receiver.getPersistedSeq(), bus.entries.at(-1)!.seq);
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, 5);
	});

	test("a dead batch is still abandoned when the head is FURTHER than one page budget away", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		// The field wedge (2003 playoffs league): a device sat ~1300 entries behind
		// with one bulk sim batch stuck at 14/21 chunks. The catch-up budget is 40
		// pages x 25 = 1000 entries, so the head was UNREACHABLE in a single pass -
		// and the stale-batch sweep only ran on the reached-head paths. With the
		// watermark pinned by the incomplete batch, every pass restarted from the
		// same pinned seq, re-fetched the identical 1000 entries, applied none of
		// them (all already `seen`), hit the page cap, and returned - forever. The
		// sweep that could rescue-or-abandon the batch could never run.
		const H = new FakeTransport("H", bus);
		const chunk = (i: number) => ({
			id: `dead${i}`,
			authorId: "H",
			action: "playMenu.sim",
			batchId: "DEAD",
			chunkIndex: i,
			chunkCount: 3,
			changeset: {
				changes: [
					{
						store: "events" as const,
						id: 900_000 + i,
						type: "put" as const,
						value: { eid: 900_000 + i },
					},
				],
			},
		});
		// Chunk 1 was lost mid-upload and is never in the log.
		await H.publish(chunk(0));
		await H.publish(chunk(2));

		// The room kept playing well past it - more entries than ONE catch-up pass
		// can walk (40 pages x 25 = 1000), which is what made the head unreachable.
		const X = new FakeTransport("X", bus);
		const N = 1100;
		for (let i = 0; i < N; i++) {
			await X.publish({
				id: `x${i}`,
				authorId: "X",
				action: "main.proposeTrade",
				changeset: {
					changes: [{ store: "events", id: i, type: "put", value: { eid: i } }],
				},
			});
		}

		const receiver = new SyncEngine(new FakeTransport("R", bus), {});
		// catchUp() returns true only on genuinely reaching the head - the check
		// that matters here. (isCaughtUp() compares against the highest seq SEEN,
		// which a page-capped pass satisfies while entries remain unfetched.)
		let reachedHead = false;
		for (let i = 0; i < 20; i++) {
			if (await receiver.catchUp()) {
				reachedHead = true;
				break;
			}
		}

		// Converges: the unrecoverable batch is abandoned, the watermark moves off
		// the pin, and the rest of the room's history lands.
		assert.ok(
			reachedHead,
			"catch-up must reach the head even when it is beyond one page budget",
		);
		assert.strictEqual(receiver.getPersistedSeq(), bus.entries.at(-1)!.seq);
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, N);
	});

	test("a pinned incomplete batch doesn't re-show the catching-up bar on later passes", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		// An incomplete bulk batch (chunk 1 of 3 lost) pins the watermark...
		const H = new FakeTransport("H", bus);
		const chunk = (i: number) => ({
			id: `pin${i}`,
			authorId: "H",
			action: "playMenu.day",
			batchId: "PIN",
			chunkIndex: i,
			chunkCount: 3,
			changeset: {
				changes: [
					{
						store: "events" as const,
						id: 300 + i,
						type: "put" as const,
						value: { eid: 300 + i },
					},
				],
			},
		});
		await H.publish(chunk(0));
		await H.publish(chunk(2));
		// ...under a backlog big enough to surface the progress bar on pass 1.
		for (let i = 0; i < 40; i++) {
			await H.publish({
				id: `e${i}`,
				authorId: "H",
				action: "x",
				changeset: {
					changes: [
						{
							store: "events" as const,
							id: 1000 + i,
							type: "put" as const,
							value: { eid: 1000 + i },
						},
					],
				},
			});
		}

		const progressCalls: ({ done: number; total: number } | undefined)[] = [];
		const receiver = new SyncEngine(new FakeTransport("R", bus), {
			onCatchUpProgress: (p) => progressCalls.push(p),
		});

		// Pass 1: a real drain - the bar shows, then clears at the head.
		assert.strictEqual(await receiver.catchUp(), true);
		assert.ok(progressCalls.length > 0);
		assert.strictEqual(progressCalls.at(-1), undefined);

		// The watermark is pinned behind the incomplete batch, so a count from the
		// WATERMARK would see the whole already-fetched tail as "behind" again and
		// flash the bar every 15s tick forever. Counting from the fetch frontier
		// must keep the bar hidden while the reset/abandon cycle runs.
		progressCalls.length = 0;
		for (let i = 0; i < 5 && !receiver.isCaughtUp(); i++) {
			assert.strictEqual(await receiver.catchUp(), true);
		}
		assert.deepStrictEqual(progressCalls, []);
		// And the dead batch got abandoned along the way, so we ARE caught up.
		assert.strictEqual(receiver.isCaughtUp(), true);
	});

	test("catchUp is a no-op on a stopped engine", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		const H = new FakeTransport("H", bus);
		await H.publish({
			id: "e1",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [{ store: "events", id: 1, type: "put", value: { eid: 1 } }],
			},
		});

		// A reconnect replaces the engine and stops the old one; any drain the old
		// engine still runs (or has queued) must do nothing - two live drains were
		// interleaving their catch-up passes and churning each other's state.
		const receiver = new SyncEngine(new FakeTransport("R", bus), {});
		receiver.stop();
		assert.strictEqual(await receiver.catchUp(), false);
		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, 0);
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
	// A batch that was abandoned and then RESURRECTED - its author finally
	// uploaded the missing chunk - used to lose that chunk outright if the
	// by-batchId rescue couldn't run at that moment. The chunk was already marked
	// `seen`, so no re-fetch would ever redeliver it, and nothing was buffered to
	// pin the watermark, so the engine declared itself caught up with a whole
	// changeset silently missing. That is how a device ends up holding a day's
	// schedule rows with no games and no idea anything is wrong.
	test("a resurrected chunk is never dropped when the rescue can't run", async () => {
		const bus = new FakeBus();
		resetG();
		await resetCache({});

		const host = new FakeTransport("H", bus);
		const chunk = (i: number) => ({
			id: `res${i}`,
			authorId: "H",
			action: "playMenu.sim",
			batchId: "RES",
			chunkIndex: i,
			chunkCount: 3,
			changeset: {
				changes: [
					{
						store: "events" as const,
						id: 200 + i,
						type: "put" as const,
						value: { eid: 200 + i },
					},
				],
			},
		});
		// Two of three chunks, then unrelated activity so the log provably moves
		// past the batch and it gets abandoned.
		await host.publish(chunk(0));
		await host.publish(chunk(2));
		await host.publish({
			id: "res-after",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [{ store: "events", id: 9, type: "put", value: { eid: 9 } }],
			},
		});

		const receiverTransport = new FakeTransport("R", bus);
		const receiver = new SyncEngine(receiverTransport, {});
		// Blind the rescue so the batch can only be abandoned, not completed.
		receiverTransport.blindBatchFetch = true;
		for (let i = 0; i < 20; i++) {
			await receiver.catchUp();
			if (receiver.isCaughtUp()) {
				break;
			}
		}
		assert.strictEqual(
			receiver.isCaughtUp(),
			true,
			"the abandoned batch should stop pinning the watermark",
		);

		// Now the author uploads the missing chunk, while the rescue is still
		// unavailable. The engine must NOT swallow it.
		await host.publish(chunk(1));
		await receiver.catchUp();
		assert.strictEqual(
			receiver.isCaughtUp(),
			false,
			"the resurrected chunk was dropped - nothing is pinning the watermark, so the changeset is silently lost",
		);

		// And once the rescue works again, the batch completes and lands.
		receiverTransport.blindBatchFetch = false;
		for (let i = 0; i < 20; i++) {
			await receiver.catchUp();
			if (receiver.isCaughtUp()) {
				break;
			}
		}
		assert.strictEqual(receiver.isCaughtUp(), true);
		const events = await idb.cache.events.getAll();
		const eids = events.map((event) => event.eid).sort((a, b) => a - b);
		assert.deepStrictEqual(
			eids,
			[9, 200, 201, 202],
			"every chunk of the resurrected batch should have applied",
		);
	});
	// The SAME failure, modelled the way it actually happens - and the reason it
	// kept coming back every offseason after being "fixed".
	//
	// The test above re-pushes the missing chunks with the seqs they were
	// originally created with, i.e. it models chunks that were always in the log
	// and merely unreachable. But the real sequence is the author's outbox
	// draining LATER: publish() stamps serverTimestamp() at send time, so the
	// chunk that finally lands carries a CURRENT timestamp. Judging the batch by
	// its newest chunk therefore made a draft-era changeset read as brand new,
	// the guard declined to fire, and the phase went backwards - the exact case
	// the guard exists for defeated by the exact act it was written to catch.
	test("a late-uploaded chunk cannot make a stale batch look current", async () => {
		const bus = new FakeBus();
		const host = new SyncEngine(new FakeTransport("H", bus), { isHost: true });
		host.start();
		const receiver = new SyncEngine(new FakeTransport("R", bus));

		resetG();
		g.setWithoutSavingToDB("phase", PHASE.AFTER_DRAFT);
		await resetCache({});
		await idb.cache.gameAttributes.put({
			key: "phase",
			value: PHASE.AFTER_DRAFT,
		});

		const N = 260;
		await host.onLocalChangeset(
			{
				changes: [
					...Array.from({ length: N }, (_, i) => ({
						store: "events" as const,
						id: i + 1,
						type: "put" as const,
						value: { eid: i + 1, type: "test", text: bulkText(2000, i) },
					})),
					{
						store: "gameAttributes" as const,
						id: "phase",
						type: "put" as const,
						value: { key: "phase", value: PHASE.AFTER_DRAFT },
					},
				],
			},
			"playMenu.untilResignPlayers",
		);
		const allChunks = [...bus.entries];
		assert.ok(allChunks.length > 1, "should be chunked");
		const batchId = allChunks[0]!.batchId!;

		// Only chunk 0 reached the log; the rest are stuck in the author's outbox.
		bus.entries.length = 0;
		bus.entries.push(allChunks[0]!);
		await receiver.handleEntry(structuredClone(allChunks[0]!));

		// The room advances a phase and the receiver applies it in order. Published
		// through the bus so it takes the next seq, keeping the log's ordering
		// honest for the re-upload below.
		bus.publish({
			id: "advance-late",
			authorId: "X",
			action: "playMenu.untilResignPlayers",
			changeset: {
				changes: [
					{
						store: "gameAttributes",
						id: "phase",
						type: "put",
						value: { key: "phase", value: PHASE.RESIGN_PLAYERS },
					},
				],
			},
		});
		const advanceEntry = bus.entries.at(-1)!;
		await receiver.handleEntry(structuredClone(advanceEntry));
		assert.strictEqual(g.get("phase"), PHASE.RESIGN_PLAYERS);

		// The half-batch is judged dead and the watermark moves past it.
		await (receiver as any).sweepStaleBatches();
		await receiver.handleEntry(structuredClone(allChunks[0]!));
		await (receiver as any).sweepStaleBatches();
		await (receiver as any).sweepStaleBatches();
		assert.ok((receiver as any).abandonedBatches.has(batchId));

		// NOW the author's outbox drains. Each chunk is PUBLISHED, so it gets a
		// current seq - above the receiver's watermark, exactly as Firestore's
		// server timestamp would give it.
		const republished: ChangesetEntry[] = [];
		for (const chunk of allChunks.slice(1)) {
			const { seq: _oldSeq, ...withoutSeq } = chunk;
			bus.publish(withoutSeq);
			republished.push(bus.entries.at(-1)!);
		}
		assert.ok(
			republished[0]!.seq > advanceEntry.seq,
			"a re-uploaded chunk must carry a newer seq than the phase advance",
		);

		await receiver.handleEntry(structuredClone(republished[0]!));

		assert.strictEqual(
			g.get("phase"),
			PHASE.RESIGN_PLAYERS,
			"a draft-era batch must not drag the phase back, however new its last chunk looks",
		);
		// And the data still lands - the ordered resync delivers it.
		assert.strictEqual(
			(await idb.cache.events.getAll()).length,
			N,
			"the missed data should still land, just in order",
		);
	});
	// The same ordering hazard from the other direction. A resync replays the
	// whole log oldest-first while the live listener is still running underneath
	// it, so anything delivered mid-replay is NEWER than everything being
	// replayed - and applying it right then lets the replay land an old record on
	// top of it. That matters much more now that resyncs are automatic (a device
	// that notices it is behind the room runs one), not just a button.
	test("a live entry arriving mid-resync is applied after the replay, not during", async () => {
		const bus = new FakeBus();
		resetG();
		g.setWithoutSavingToDB("phase", PHASE.AFTER_DRAFT);
		await resetCache({});
		await idb.cache.gameAttributes.put({
			key: "phase",
			value: PHASE.AFTER_DRAFT,
		});

		const host = new FakeTransport("H", bus);
		// One old entry in the log, setting the old phase.
		await host.publish({
			id: "old-phase",
			authorId: "H",
			action: "x",
			changeset: {
				changes: [
					{
						store: "gameAttributes",
						id: "phase",
						type: "put",
						value: { key: "phase", value: PHASE.AFTER_DRAFT },
					},
				],
			},
		});

		const receiver = new SyncEngine(new FakeTransport("R", bus));

		// The room advances WHILE the resync is replaying. Delivered straight to
		// handleEntry, the way the live listener does.
		const advance: ChangesetEntry = {
			id: "advance-mid-resync",
			authorId: "H",
			action: "x",
			seq: 999,
			changeset: {
				changes: [
					{
						store: "gameAttributes",
						id: "phase",
						type: "put",
						value: { key: "phase", value: PHASE.RESIGN_PLAYERS },
					},
				],
			},
		};
		const original = (receiver as any).resyncAllInner.bind(receiver);
		(receiver as any).resyncAllInner = async (
			entries: ChangesetEntry[],
			epoch: number,
		) => {
			await receiver.handleEntry(advance);
			return original(entries, epoch);
		};

		await receiver.resyncAll();

		assert.strictEqual(
			g.get("phase"),
			PHASE.RESIGN_PLAYERS,
			"the newer live entry must win - the replay must not land on top of it",
		);
	});
});

// The download side had no timeout, and the reentrancy guard is only cleared by
// catchUp()'s `finally` - so a fetch that never settles (a phone that slept
// mid-request, a socket that died without an error) latched the guard forever.
// Every later pass returned at the guard doing nothing: the catch-up bar sat at
// 0%, the live subscription never started, and reloading didn't help because the
// next session wedged the same way. Seen in the field as "stuck on 0% catching
// up every time I load in", with a device seven days behind the room while the
// engine reported itself caught up.
describe("a catch-up fetch that never comes back", () => {
	test("times out instead of latching the guard forever", async () => {
		// Only this test needs them, and the timeouts are a minute apiece.
		vi.useFakeTimers();
		try {
			await hungFetchScenario();
		} finally {
			vi.useRealTimers();
		}
	});
});

const hungFetchScenario = async () => {
	{
		const bus = new FakeBus();

		// One entry to find, once fetching works again.
		const author = new SyncEngine(new FakeTransport("A", bus));
		author.start();
		await author.onLocalChangeset(
			{ changes: [{ store: "trade", id: 0, type: "delete" }] },
			"main.clearTrade",
		);

		let hang = true;
		class HangingTransport extends FakeTransport {
			override async fetchEntriesSince(sinceMs: number, pageLimit?: number) {
				if (hang) {
					// Never settles - not a rejection, which the existing retry path
					// already handles.
					return new Promise<ChangesetEntry[]>(() => {});
				}
				return super.fetchEntriesSince(sinceMs, pageLimit);
			}
		}

		const receiver = new SyncEngine(new HangingTransport("B", bus));

		// Deliberately not awaited: on the old code this promise never settles.
		const wedged = receiver.catchUp();

		// Both the full-page fetch and the small-page retry have to time out.
		await vi.advanceTimersByTimeAsync(46_000);
		await vi.advanceTimersByTimeAsync(16_000);

		assert.strictEqual(await wedged, false, "the hung pass should give up");

		// The guard is free again, so a later pass does real work.
		hang = false;
		assert.strictEqual(
			await receiver.catchUp(),
			true,
			"a later pass should reach the head",
		);
		assert.strictEqual(receiver.getPersistedSeq() > 0, true);
	}
};

// A bulk change whose chunks were re-uploaded late sits in the log at its
// RE-UPLOAD time, so "replay in seq order" applies it after changes that came
// months later. That is how a reimported device watched the replay run the
// offseason and then park in the preseason while the room was on day 75 of the
// regular season. The replay now orders units by the era their CONTENT
// declares, and refuses what it cannot place below the room's announced
// position.
describe("a resync replays by the era a change declares, not by raw seq", () => {
	const attr = (key: "phase" | "season", value: number) => ({
		store: "gameAttributes" as const,
		id: key,
		type: "put" as const,
		value: { key, value },
	});

	const publishAdvance = (
		bus: FakeBus,
		id: string,
		changes: ReturnType<typeof attr>[],
	) => {
		bus.publish({
			id,
			authorId: "OTHER",
			action: "sim.newPhase",
			changeset: { changes },
		});
	};

	test("a rollover re-uploaded at the head does not drag the league back to the preseason", async () => {
		resetG();
		await resetCache({});
		g.setWithoutSavingToDB("season", 2005);
		g.setWithoutSavingToDB("phase", PHASE.PRESEASON);

		const bus = new FakeBus();
		// The log as the room really lived it...
		publishAdvance(bus, "rollover", [
			attr("season", 2005),
			attr("phase", PHASE.PRESEASON),
		]);
		publishAdvance(bus, "start-season", [attr("phase", PHASE.REGULAR_SEASON)]);
		// ...plus the rollover AGAIN at the head, the way a late re-upload lands.
		// It carries data of its own (a real rollover is a draft class, a
		// schedule, progression - and the re-upload may be the only surviving
		// copy), so the right treatment is to apply it IN ITS TRUE POSITION, not
		// to skip it.
		bus.publish({
			id: "rollover-reupload",
			authorId: "OTHER",
			action: "sim.newPhase",
			changeset: {
				changes: [
					attr("season", 2005),
					attr("phase", PHASE.PRESEASON),
					{
						store: "events",
						id: 1,
						type: "put",
						value: { eid: 1, type: "newPhase", text: "rollover", season: 2005 },
					},
				],
			},
		});

		const engine = new SyncEngine(new FakeTransport("ME", bus));
		// Registered like production, so the replay runs with the live-path
		// regression guard off - only the replay's own ordering protects it here.
		setSyncEngine(engine);
		const result = await engine.resyncAll();

		assert.strictEqual(result.failed, false);
		assert.strictEqual(
			g.get("phase"),
			PHASE.REGULAR_SEASON,
			"the re-uploaded rollover must replay in its true position, not last",
		);
		assert.strictEqual(
			(await idb.cache.events.getAll()).length,
			1,
			"the re-uploaded rollover's own data must still land - repositioned, not dropped",
		);
	});

	test("a season-less offseason entry at the head is refused past the room's announced position", async () => {
		resetG();
		await resetCache({});
		g.setWithoutSavingToDB("season", 2005);
		g.setWithoutSavingToDB("phase", PHASE.PRESEASON);

		const bus = new FakeBus();
		publishAdvance(bus, "rollover", [
			attr("season", 2005),
			attr("phase", PHASE.PRESEASON),
		]);
		publishAdvance(bus, "start-season", [attr("phase", PHASE.REGULAR_SEASON)]);
		// A previous season's free agency, re-uploaded late. It names no season
		// (phases within a season don't), so by content alone it reads as THIS
		// season's free agency - which would be a move forward. Only the room's
		// announced position says it cannot be.
		publishAdvance(bus, "stale-fa", [attr("phase", PHASE.FREE_AGENCY)]);

		const engine = new SyncEngine(new FakeTransport("ME", bus));
		(engine as any).authority = {
			holderId: "H",
			holderName: "Host",
			position: { season: 2005, phase: PHASE.REGULAR_SEASON, day: 40 },
		};
		setSyncEngine(engine);
		const result = await engine.resyncAll();

		assert.strictEqual(result.failed, false);
		assert.strictEqual(
			g.get("phase"),
			PHASE.REGULAR_SEASON,
			"nothing in a replay may advance the league past where the room says it is",
		);
	});

	test("a day's games stamp their unit, so a re-uploaded rollover sorts before them", async () => {
		resetG();
		await resetCache({});
		g.setWithoutSavingToDB("season", 2005);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

		const bus = new FakeBus();
		// Both units write the SAME record, so whichever applies last wins - the
		// only way to observe replay order from the final state. A day's sim
		// (games carry their season) must beat the rollover that re-uploaded
		// after it in seq order but happened before it in league time.
		bus.publish({
			id: "day-sim",
			authorId: "OTHER",
			action: "sim.games",
			changeset: {
				changes: [
					{
						store: "games",
						id: 500,
						type: "put",
						value: { gid: 500, season: 2005, playoffs: false, day: 40 },
					},
					{
						store: "events",
						id: 1,
						type: "put",
						value: { eid: 1, type: "x", text: "from the day sim" },
					},
				],
			},
		});
		bus.publish({
			id: "rollover-reupload",
			authorId: "OTHER",
			action: "sim.newPhase",
			changeset: {
				changes: [
					attr("season", 2005),
					attr("phase", PHASE.PRESEASON),
					{
						store: "events",
						id: 1,
						type: "put",
						value: { eid: 1, type: "x", text: "from the rollover" },
					},
				],
			},
		});

		const engine = new SyncEngine(new FakeTransport("ME", bus));
		setSyncEngine(engine);
		await engine.resyncAll();

		const events = await idb.cache.events.getAll();
		assert.strictEqual(events.length, 1);
		assert.strictEqual(
			(events[0] as any).text,
			"from the day sim",
			"the day's sim happened after the rollover in league time, so its write must win",
		);
	});

	test("after a clean replay, the room's announced season/phase is adopted when the window cannot supply it", async () => {
		resetG();
		await resetCache({});
		g.setWithoutSavingToDB("season", 2005);
		g.setWithoutSavingToDB("phase", PHASE.PRESEASON);

		const bus = new FakeBus();
		// The ONLY phase-bearing entry the window has is the stale re-uploaded
		// rollover - the real "start the season" advance is months below the
		// window. Perfect ordering still leaves the phase parked at preseason.
		publishAdvance(bus, "rollover-reupload", [
			attr("season", 2005),
			attr("phase", PHASE.PRESEASON),
		]);

		const engine = new SyncEngine(new FakeTransport("ME", bus));
		(engine as any).authority = {
			holderId: "H",
			holderName: "Host",
			position: { season: 2005, phase: PHASE.REGULAR_SEASON, day: 40 },
		};
		setSyncEngine(engine);
		const result = await engine.resyncAll();

		assert.strictEqual(result.failed, false);
		assert.strictEqual(
			g.get("season"),
			2005,
		);
		assert.strictEqual(
			g.get("phase"),
			PHASE.REGULAR_SEASON,
			"the announced position is the simmer's own statement of the current phase - adopt it",
		);
	});
});

// One bulk applier at a time. Two passes working the same backlog dedup-skip
// each other's entries, so they interleave non-monotonically and the SLOWER
// pass writes last - concurrent appliers are how a healthy months-long drain
// turned into a league dragged backwards.
describe("bulk appliers are serialized", () => {
	test("two concurrent resyncAll calls share one replay", async () => {
		resetG();
		await resetCache({});

		const bus = new FakeBus();
		bus.publish({
			id: "e1",
			authorId: "OTHER",
			action: "x",
			changeset: { changes: [{ store: "trade", id: 0, type: "delete" }] },
		});

		const transport = new FakeTransport("ME", bus);
		const engine = new SyncEngine(transport);
		setSyncEngine(engine);
		const [a, b] = await Promise.all([engine.resyncAll(), engine.resyncAll()]);

		assert.strictEqual(
			transport.fetchAllCount,
			1,
			"the second call must coalesce onto the replay already running",
		);
		assert.strictEqual(a, b);
	});

	test("a second catch-up call while one is progressing yields instead of stealing the guard", async () => {
		resetG();
		await resetCache({});

		const bus = new FakeBus();
		bus.publish({
			id: "e1",
			authorId: "OTHER",
			action: "x",
			changeset: { changes: [{ store: "trade", id: 0, type: "delete" }] },
		});

		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		class GatedTransport extends FakeTransport {
			override async fetchEntriesSince(sinceMs: number, pageLimit?: number) {
				await gate;
				return super.fetchEntriesSince(sinceMs, pageLimit);
			}
		}

		const engine = new SyncEngine(new GatedTransport("ME", bus));
		setSyncEngine(engine);

		const first = engine.catchUp();
		assert.strictEqual(
			await engine.catchUp(),
			false,
			"a pass is in flight and healthy - the newcomer must not start a second one",
		);
		release();
		assert.strictEqual(await first, true, "the original pass finishes its work");
	});
});
