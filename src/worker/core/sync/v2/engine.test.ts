import "fake-indexeddb/auto";
import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../../test/helpers.ts";
import { g } from "../../../util/index.ts";
import { idb } from "../../../db/index.ts";
import { changeTracker } from "../../../db/changeTracker.ts";
import { serializeChangeset } from "../serialize.ts";
import type { Authority, SyncTransport, V2StateDoc } from "../types.ts";
import type { SyncNotification } from "../notifications.ts";
import { SyncEngineV2 } from "./engine.ts";
import { readAppliedVersion } from "./applyVersion.ts";
import { APPLIED_VERSION_KEY } from "./protocol.ts";

// End-to-end over an in-memory room: the version chain running between
// devices. Two "devices" are modeled by swapping the global league DB between
// phases - the engines never act concurrently, exactly like the turn-based
// game they serve.

// ---------------------------------------------------------------------------
// In-memory room + transport
// ---------------------------------------------------------------------------

class Room {
	state: V2StateDoc | undefined;
	deltas = new Map<
		number,
		{ serialized: string; authorId: string; action: string; at: number }
	>();
	// Keyed "version:generation" - chunks live under generation-unique ids.
	checkpoints = new Map<string, string>();
	authority: Authority | undefined;
	private stateListeners = new Set<(s: V2StateDoc) => void>();
	private authorityListeners = new Set<(a: Authority | undefined) => void>();

	setState(state: V2StateDoc) {
		this.state = state;
		for (const listener of this.stateListeners) {
			listener(state);
		}
	}

	onState(listener: (s: V2StateDoc) => void) {
		this.stateListeners.add(listener);
		if (this.state) {
			listener(this.state);
		}
		return () => this.stateListeners.delete(listener);
	}

	setAuthority(a: Authority | undefined) {
		this.authority = a;
		for (const listener of this.authorityListeners) {
			listener(a);
		}
	}

	onAuthority(listener: (a: Authority | undefined) => void) {
		this.authorityListeners.add(listener);
		listener(this.authority);
		return () => this.authorityListeners.delete(listener);
	}
}

class V2Transport implements SyncTransport {
	readonly clientId: string;
	room: Room;
	notifications: SyncNotification[] = [];
	// Force the next commit to lose, as if another writer won the transaction.
	forceCasLoss = false;

	constructor(clientId: string, room: Room) {
		this.clientId = clientId;
		this.room = room;
	}

	async ping() {}

	async publish() {
		throw new Error("v1 publish must never be called on a v2 room");
	}

	async fetchAllEntries() {
		return [];
	}

	async fetchEntriesSince() {
		return [];
	}

	async countEntriesSince() {
		return 0;
	}

	subscribe() {
		return () => {};
	}

	async claimAuthority(holderId: string, holderName: string) {
		this.room.setAuthority({ holderId, holderName });
	}

	subscribeAuthority(
		onChange: (a: Authority | undefined) => void,
		_onError?: (error: unknown) => void,
	) {
		return this.room.onAuthority(onChange);
	}

	async publishBusy() {}

	async publishNotification(
		notification: SyncNotification & { authorId: string; authorName: string },
	) {
		this.notifications.push(notification);
	}

	async fetchRoomV2State() {
		return this.room.state;
	}

	subscribeRoomV2State(
		onChange: (s: V2StateDoc) => void,
		_onError?: (error: unknown) => void,
	) {
		return this.room.onState(onChange);
	}

	// Same slot rule as the real transport: a version the pointer has already
	// committed can never have its payload overwritten.
	async publishV2Delta(
		meta: { version: number; authorId: string; action: string; at: number },
		serialized: string,
	) {
		const current = this.room.state?.version ?? 0;
		if (current >= meta.version) {
			throw new Error("v2-slot-taken");
		}
		this.room.deltas.set(meta.version, { ...meta, serialized });
		return 1;
	}

	async commitV2Version(
		next: {
			version: number;
			authorId: string;
			byName: string;
			at: number;
			action: string;
			inlineDelta?: string;
		},
		expectedVersion: number,
	) {
		if (this.forceCasLoss) {
			this.forceCasLoss = false;
			return false;
		}
		const current = this.room.state?.version ?? 0;
		if (current !== expectedVersion) {
			return false;
		}
		// Ownership check, mirroring the real transport EXACTLY: never bless a
		// payload some other publish overwrote. Version 0 is the room-init
		// commit and has no payload by definition - being lenient here instead
		// of faithful is why a broken v2 room creation went uncaught.
		if (next.version > 0) {
			const delta = this.room.deltas.get(next.version);
			if (!delta || delta.authorId !== next.authorId || delta.at !== next.at) {
				return false;
			}
		}
		this.room.setState({
			...next,
			checkpointVersion: this.room.state?.checkpointVersion,
			checkpointChunkCount: this.room.state?.checkpointChunkCount,
		});
		return true;
	}

	async fetchV2Delta(version: number) {
		return this.room.deltas.get(version);
	}

	async publishV2Checkpoint(
		version: number,
		serialized: string,
		generation?: string,
	) {
		this.room.checkpoints.set(`${version}:${generation ?? ""}`, serialized);
		return 1;
	}

	async commitV2Checkpoint(
		version: number,
		chunkCount: number,
		generation?: string,
	) {
		if (!this.room.state) {
			return false;
		}
		this.room.setState({
			...this.room.state,
			checkpointVersion: version,
			checkpointChunkCount: chunkCount,
			checkpointGeneration: generation,
		});
		return true;
	}

	async fetchV2Checkpoint(
		version: number,
		_chunkCount?: number,
		generation?: string,
	) {
		return this.room.checkpoints.get(`${version}:${generation ?? ""}`);
	}

	async deleteV2DeltasBefore() {
		return 0;
	}
}

// ---------------------------------------------------------------------------
// Device DB harness (same transactional model as applyVersion.test.ts)
// ---------------------------------------------------------------------------

const makeLeagueDb = (initial: Record<string, any[]>) => {
	const stores = new Map<string, Map<any, any>>();
	const pkOf = (row: any) =>
		row.key ?? row.pid ?? row.tid ?? row.gid ?? row.rid ?? JSON.stringify(row);
	for (const [store, rows] of Object.entries(initial)) {
		const m = new Map();
		for (const row of rows) {
			m.set(pkOf(row), row);
		}
		stores.set(store, m);
	}
	const ensure = (store: string) => {
		if (!stores.has(store)) {
			stores.set(store, new Map());
		}
		return stores.get(store)!;
	};
	return {
		get: async (store: string, key: any) => stores.get(store)?.get(key),
		getAll: async (store: string) => [...(stores.get(store)?.values() ?? [])],
		transaction: (storeNames?: string | string[]) => {
			if (storeNames === undefined) {
				return { done: Promise.resolve() };
			}
			const buffered: {
				store: string;
				op: "clear" | "put" | "delete";
				row?: any;
				key?: any;
			}[] = [];
			return {
				objectStore: (name: string) => ({
					clear: () => buffered.push({ store: name, op: "clear" }),
					put: (row: any) => buffered.push({ store: name, op: "put", row }),
					delete: (key: any) =>
						buffered.push({ store: name, op: "delete", key }),
				}),
				get done() {
					for (const entry of buffered) {
						const target = ensure(entry.store);
						if (entry.op === "clear") {
							target.clear();
						} else if (entry.op === "put") {
							target.set(pkOf(entry.row), entry.row);
						} else {
							target.delete(entry.key);
						}
					}
					return Promise.resolve();
				},
			};
		},
	};
};

const stubCacheLifecycle = () => {
	const original = {
		flush: idb.cache.flush,
		fill: idb.cache.fill,
		discardForRestore: idb.cache.discardForRestore,
	};
	idb.cache.flush = async () => {};
	idb.cache.fill = (async () => {}) as any;
	idb.cache.discardForRestore = (() => {}) as any;
	return () => {
		idb.cache.flush = original.flush;
		idb.cache.fill = original.fill;
		idb.cache.discardForRestore = original.discardForRestore;
	};
};

const tradePut = (pid: number, tid: number) => ({
	store: "players" as any,
	id: pid,
	type: "put" as const,
	value: { pid, tid },
});

const changesetOf = (...changes: any[]) => ({ changes });

const initRoom = (room: Room) => {
	room.setState({
		version: 0,
		authorId: "init",
		byName: "Init",
		at: 1,
	});
};

describe("SyncEngineV2", () => {
	let restoreCache: () => void;

	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2006);
		g.setWithoutSavingToDB("phase", 1);
		await resetCache({});
		restoreCache = stubCacheLifecycle();
		changeTracker.disable();
		changeTracker.reset();
	});

	afterEach(() => {
		restoreCache();
	});

	test("the authority mints version 1 and a follower applies it", async () => {
		const room = new Room();
		initRoom(room);

		// DEVICE A (authority) publishes a trade.
		const dbA = makeLeagueDb({
			players: [{ pid: 1, tid: 2 }], // its local action already moved him
		});
		(idb as any).league = dbA;
		const transportA = new V2Transport("A", room);
		const engineA = new SyncEngineV2(transportA);
		engineA.start();
		await engineA.claimAuthority();

		const outcome = await engineA.onLocalChangeset(
			changesetOf(tradePut(1, 2)),
			"main.proposeTrade",
		);
		assert.strictEqual(outcome, "confirmed");
		assert.strictEqual(room.state!.version, 1);
		assert.strictEqual(await readAppliedVersion(), 1);
		engineA.stop();

		// DEVICE B (follower), still at version 0 with the old roster.
		const dbB = makeLeagueDb({
			players: [{ pid: 1, tid: 9 }],
		});
		(idb as any).league = dbB;
		await resetCache({});
		const transportB = new V2Transport("B", room);
		const engineB = new SyncEngineV2(transportB);

		const caught = await engineB.catchUp();
		assert.ok(caught);
		assert.strictEqual(await readAppliedVersion(), 1);
		const players = await dbB.getAll("players");
		assert.strictEqual(
			players[0].tid,
			2,
			"the follower's database must now be the authority's",
		);
	});

	test("a chain of versions applies in order, all or stop", async () => {
		const room = new Room();
		initRoom(room);
		const authorTransport = new V2Transport("A", room);
		// Seed three versions directly.
		for (let v = 1; v <= 3; v++) {
			await authorTransport.publishV2Delta(
				{ version: v, authorId: "A", action: `step${v}`, at: v },
				serializeChangeset(changesetOf(tradePut(v, v * 10))),
			);
			await authorTransport.commitV2Version(
				{ version: v, authorId: "A", byName: "A", at: v, action: `step${v}` },
				v - 1,
			);
		}

		(idb as any).league = makeLeagueDb({ players: [] });
		const engine = new SyncEngineV2(new V2Transport("B", room));
		assert.ok(await engine.catchUp());
		assert.strictEqual(await readAppliedVersion(), 3);
		const players = await (idb as any).league.getAll("players");
		assert.strictEqual(players.length, 3);
	});

	// THE SKIP, made impossible: if version 2's payload is unreachable, version
	// 3 must NOT apply. v1 skipped the missing day and forked; v2 stops.
	test("a missing middle version stops the walk - later versions never apply", async () => {
		const room = new Room();
		initRoom(room);
		const authorTransport = new V2Transport("A", room);
		for (let v = 1; v <= 3; v++) {
			await authorTransport.publishV2Delta(
				{ version: v, authorId: "A", action: `step${v}`, at: v },
				serializeChangeset(changesetOf(tradePut(v, v * 10))),
			);
			await authorTransport.commitV2Version(
				{ version: v, authorId: "A", byName: "A", at: v, action: `step${v}` },
				v - 1,
			);
		}
		room.deltas.delete(2);

		(idb as any).league = makeLeagueDb({ players: [] });
		const engine = new SyncEngineV2(new V2Transport("B", room));
		const caught = await engine.catchUp();
		assert.strictEqual(caught, false, "an unreachable link is a hard stop");
		assert.strictEqual(
			await readAppliedVersion(),
			1,
			"the walk applied version 1 and STOPPED - version 3 must never land over the hole",
		);
		const players = await (idb as any).league.getAll("players");
		assert.deepStrictEqual(
			players.map((p: any) => p.pid),
			[1],
		);
	});

	test("a device far behind restores the checkpoint, then walks the tail", async () => {
		const room = new Room();
		initRoom(room);
		const authorTransport = new V2Transport("A", room);

		// The room is at version 10; a checkpoint captures version 9; only the
		// tail delta (10) still exists. Seed in real publish order: payloads
		// first, pointer last (the slot rule refuses payload writes for
		// already-committed versions).
		await authorTransport.publishV2Delta(
			{ version: 10, authorId: "A", action: "playMenu.day", at: 10 },
			serializeChangeset(changesetOf(tradePut(5, 7))),
		);
		await authorTransport.publishV2Checkpoint(
			9,
			serializeChangeset({
				version: 1,
				stores: {
					players: [
						{ pid: 1, tid: 0 },
						{ pid: 2, tid: 0 },
						{ pid: 3, tid: 0 },
						{ pid: 4, tid: 0 },
						{ pid: 5, tid: 0 },
					],
					teams: [{ tid: 0 }],
					gameAttributes: [
						{ key: "season", value: 2006 },
						{ key: "phase", value: 1 },
					],
				},
			}),
		);
		await authorTransport.commitV2Checkpoint(9, 1);
		room.setState({
			...room.state!,
			version: 10,
			authorId: "A",
			byName: "A",
			at: 10,
		});

		// Fresh device at version 0.
		(idb as any).league = makeLeagueDb({ players: [] });
		const engine = new SyncEngineV2(new V2Transport("B", room));
		assert.ok(await engine.catchUp());
		assert.strictEqual(await readAppliedVersion(), 10);
		const players = await (idb as any).league.getAll("players");
		assert.strictEqual(players.length, 5, "checkpoint roster landed");
		assert.strictEqual(
			players.find((p: any) => p.pid === 5).tid,
			7,
			"and the tail delta applied on top of it",
		);
	});

	// A race between two edits has a loser, and the loser's answer is retry,
	// not discard: the edit is a whole-record statement of user intent, so it
	// catches up and lands on top of whatever won.
	test("an edit that loses the CAS retries and lands as the next version", async () => {
		const room = new Room();
		initRoom(room);
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 2 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 0 }],
		});
		const transport = new V2Transport("A", room);
		const engine = new SyncEngineV2(transport);
		engine.start();
		await engine.claimAuthority();
		transport.forceCasLoss = true;

		const outcome = await engine.onLocalChangeset(
			changesetOf(tradePut(1, 2)),
			"main.proposeTrade",
		);
		assert.strictEqual(outcome, "confirmed", "the retry won the second CAS");
		assert.strictEqual(room.state!.version, 1);
		assert.strictEqual(await readAppliedVersion(), 1);
		assert.strictEqual(await engine.pendingUploadCount(), 0);
		engine.stop();
	});

	// The mirror the listener keeps fresh is the publish target - a warm
	// device pays ZERO state reads to publish an edit. The slot-taken
	// transaction and the commit CAS stay the arbiters, so this costs nothing
	// in safety; the server fetch happens only on a retry (a lost race) or on
	// a device that has never seen the state doc.
	test("a warm device publishes an edit without reading the state doc", async () => {
		const room = new Room();
		initRoom(room);
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 2 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 0 }],
		});
		const transport = new V2Transport("A", room);
		let stateFetches = 0;
		const realFetch = transport.fetchRoomV2State.bind(transport);
		transport.fetchRoomV2State = async () => {
			stateFetches += 1;
			return realFetch();
		};
		const engine = new SyncEngineV2(transport);
		// start() subscribes to the pointer, which (like Firestore's initial
		// snapshot) delivers the current state - that warms the mirror.
		engine.start();
		await engine.claimAuthority();

		const outcome = await engine.onLocalChangeset(
			changesetOf(tradePut(1, 2)),
			"main.proposeTrade",
		);
		assert.strictEqual(outcome, "confirmed");
		assert.strictEqual(room.state!.version, 1);
		assert.strictEqual(await readAppliedVersion(), 1);
		assert.strictEqual(
			stateFetches,
			0,
			"a warm publish is write-only: no state doc read",
		);
		engine.stop();
	});

	// No device's change ever waits on another device being online: a device
	// that is NOT in charge of simming still publishes its edit as the next
	// version itself, directly, gated only by the CAS.
	test("a non-simming device publishes its edit as the next version directly", async () => {
		const room = new Room();
		initRoom(room);
		room.setAuthority({ holderId: "A", holderName: "Alex" });

		// DEVICE B (not the simmer) files a roster edit while A is offline.
		(idb as any).league = makeLeagueDb({ players: [{ pid: 4, tid: 1 }] });
		const follower = new SyncEngineV2(new V2Transport("B", room));
		follower.start();
		const outcome = await follower.onLocalChangeset(
			changesetOf(tradePut(4, 1)),
			"main.updatePlayingTime",
		);
		assert.strictEqual(outcome, "confirmed");
		assert.strictEqual(
			room.state!.version,
			1,
			"the edit became version 1 without any other device involved",
		);
		assert.strictEqual(await readAppliedVersion(), 1);
		follower.stop();

		// DEVICE A (the simmer) comes online later and just applies it.
		(idb as any).league = makeLeagueDb({ players: [{ pid: 4, tid: 3 }] });
		await resetCache({});
		const authority = new SyncEngineV2(new V2Transport("A", room));
		assert.ok(await authority.catchUp());
		assert.strictEqual(
			(await (idb as any).league.getAll("players"))[0].tid,
			1,
			"the simmer's database took the edit on its next catch-up",
		);
		assert.strictEqual(await readAppliedVersion(), 1);
	});

	// The v1-fork preventer, kept: a timeline advance authored on a world the
	// room has moved past is DISCARDED, and the device snaps back to the
	// chain's truth via the checkpoint - it is never republished.
	test("a stale timeline advance is discarded and the device recovers from the checkpoint", async () => {
		const room = new Room();
		initRoom(room);

		// The room is at version 1 (someone else simmed), with a checkpoint
		// capturing version 1.
		const seedTransport = new V2Transport("C", room);
		// Payload first, then the pointer - the order a real publish uses, and
		// now the order the transport enforces.
		await seedTransport.publishV2Delta(
			{ version: 1, authorId: "C", action: "playMenu.day", at: 5 },
			serializeChangeset(changesetOf(tradePut(2, 0))),
		);
		await seedTransport.commitV2Version(
			{ version: 1, authorId: "C", byName: "C", at: 5, action: "playMenu.day" },
			0,
		);
		await seedTransport.publishV2Checkpoint(
			1,
			serializeChangeset({
				version: 1,
				stores: {
					players: [
						{ pid: 1, tid: 0 },
						{ pid: 2, tid: 0 },
						{ pid: 3, tid: 0 },
						{ pid: 4, tid: 0 },
						{ pid: 5, tid: 0 },
					],
					teams: [{ tid: 0 }],
					gameAttributes: [
						{ key: "season", value: 2006 },
						{ key: "phase", value: 1 },
					],
				},
			}),
		);
		await seedTransport.commitV2Checkpoint(1, 1);

		// DEVICE A, still at version 0, simmed its own day locally (pid 1 moved
		// to tid 99 - a record the chain will never carry).
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 99 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 0 }],
		});
		const transport = new V2Transport("A", room);
		const engine = new SyncEngineV2(transport);
		engine.start();
		await engine.claimAuthority();

		const outcome = await engine.onLocalChangeset(
			changesetOf(tradePut(1, 99)),
			"playMenu.day",
		);
		assert.strictEqual(outcome, "queued");
		assert.strictEqual(
			await engine.pendingUploadCount(),
			0,
			"the stale advance was discarded, not left to merge later",
		);
		assert.strictEqual(
			room.state!.version,
			1,
			"the chain never saw the stale advance",
		);

		// The recovery (kicked fire-and-forget by the discard) snaps the device
		// back to the chain: checkpoint restored, marker at 1, tid 99 gone.
		assert.ok(await engine.catchUp());
		assert.strictEqual(await readAppliedVersion(), 1);
		const players = await (idb as any).league.getAll("players");
		assert.strictEqual(players.length, 5, "the checkpoint roster landed");
		assert.strictEqual(
			players.find((p: any) => p.pid === 1).tid,
			0,
			"the discarded advance's mutation was rolled back to the chain's state",
		);
		engine.stop();
	});

	test("notifications fire only when the version actually committed", async () => {
		const room = new Room();
		initRoom(room);
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 2 }],
		});
		const transport = new V2Transport("A", room);
		const engine = new SyncEngineV2(transport);
		engine.start();
		await engine.claimAuthority();
		transport.forceCasLoss = true;

		await engine.onLocalChangeset(changesetOf(tradePut(1, 2)), "playMenu.day", [
			{ title: "Celtics 115", body: "..." } as any,
		]);
		assert.strictEqual(
			transport.notifications.length,
			0,
			"a lost commit must not announce anything",
		);

		await engine.onLocalChangeset(changesetOf(tradePut(1, 2)), "playMenu.day", [
			{ title: "Celtics 115", body: "..." } as any,
		]);
		assert.strictEqual(transport.notifications.length, 1);
		// Drain the discard's fire-and-forget recovery before this test ends, so
		// it can't run against the NEXT test's freshly-swapped league DB.
		await engine.catchUp();
		engine.stop();
	});

	// Firestore writes never reject while offline - they buffer forever. The
	// drain must therefore fast-fail to "queued" rather than hanging the action
	// (and the Play button) behind a write that may never resolve.
	test("an offline publish times out to 'queued' instead of hanging the action", async () => {
		const room = new Room();
		initRoom(room);
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 2 }],
		});
		const transport = new V2Transport("A", room);
		// Offline: the delta write buffers and never resolves.
		transport.publishV2Delta = () => new Promise(() => {});
		const engine = new SyncEngineV2(transport, { publishTimeoutMs: 50 });
		engine.start();
		await engine.claimAuthority();

		const outcome = await engine.onLocalChangeset(
			changesetOf(tradePut(1, 2)),
			"playMenu.day",
		);

		assert.strictEqual(outcome, "queued");
		assert.strictEqual(
			await engine.pendingUploadCount(),
			1,
			"the sim stays safely queued for the next drain kick",
		);
		assert.strictEqual(room.state!.version, 0, "nothing half-published");

		// The connection returns: the ordinary drain kick lands it.
		delete (transport as any).publishV2Delta;
		assert.ok(await engine.drainOutbox());
		assert.strictEqual(room.state!.version, 1);
		assert.strictEqual(await engine.pendingUploadCount(), 0);
		engine.stop();
	});

	// The field bug behind the cas-lost storm: a stale pointer read (cache
	// lagging this device's own commit) made every entry in a burst target the
	// version its predecessor just took. The engine must trust its own mirror
	// over a lagging read and walk the burst 1, 2, 3 with no conflicts.
	test("a burst of edits publishes sequential versions even when the pointer read is stale", async () => {
		const room = new Room();
		initRoom(room);
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 2 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 0 }],
		});
		const transport = new V2Transport("A", room);
		// The stale read: always report the room where it was at connect.
		const frozenState = { ...room.state! };
		transport.fetchRoomV2State = async () => frozenState;
		const engine = new SyncEngineV2(transport);
		engine.start();
		await engine.claimAuthority();

		for (let i = 1; i <= 3; i++) {
			const outcome = await engine.onLocalChangeset(
				changesetOf(tradePut(1, i)),
				"main.updatePlayingTime",
			);
			assert.strictEqual(outcome, "confirmed", `edit ${i} confirmed`);
			assert.strictEqual(
				room.state!.version,
				i,
				`edit ${i} became version ${i}`,
			);
		}
		assert.strictEqual(await readAppliedVersion(), 3);
		assert.strictEqual(await engine.pendingUploadCount(), 0);
		engine.stop();
	});

	// A wedged backend channel (Safari killing the stream) makes the head probe
	// fail while the listener sits quiet. Two consecutive failed probes must
	// cycle the connection and rebuild the listener - the one known cure.
	test("two failed head probes cycle the network and rebuild the listener", async () => {
		const room = new Room();
		initRoom(room);
		(idb as any).league = makeLeagueDb({ players: [] });
		const transport = new V2Transport("A", room);
		let cycles = 0;
		let subscribes = 0;
		(transport as any).cycleNetwork = async () => {
			cycles += 1;
		};
		const originalSubscribe = transport.subscribeRoomV2State.bind(transport);
		transport.subscribeRoomV2State = (
			onChange: (s: V2StateDoc) => void,
			onError?: (error: unknown) => void,
		) => {
			subscribes += 1;
			return originalSubscribe(onChange, onError);
		};
		transport.fetchRoomV2State = () =>
			Promise.reject(new Error("channel wedged"));
		const engine = new SyncEngineV2(transport);
		engine.start();
		const subscribesAfterStart = subscribes;

		await engine.probeHead();
		assert.strictEqual(cycles, 0, "one failure is a blip, not a wedge");
		await engine.probeHead();
		assert.strictEqual(cycles, 1, "the second consecutive failure cycles");
		assert.strictEqual(
			subscribes,
			subscribesAfterStart + 1,
			"and the listener is rebuilt on the fresh channel",
		);
		engine.stop();
	});

	// Creating a v2 room is a commit of version 0 with NO payload - it is the
	// write that brings the room into existence, before any delta can exist.
	// The chunk-ownership check added for the CAS-storm fix had no exemption
	// for it, so it looked for a delta document that is never written, refused
	// the commit, and room creation silently produced a v1 room instead. Ticking
	// "new sync engine" and getting v1 was exactly this.
	test("a room is created by committing version 0 with no payload", async () => {
		const room = new Room();
		const transport = new V2Transport("A", room);
		const initialized = await transport.commitV2Version(
			{
				version: 0,
				authorId: "A",
				byName: "Host",
				at: Date.now(),
				action: "init",
			},
			0,
		);
		assert.ok(initialized, "the room-init commit must succeed");
		assert.strictEqual(
			(await transport.fetchRoomV2State())?.version,
			0,
			"and the room must now exist on v2",
		);
	});

	// A restore that fails for a reason that will not change - a payload this
	// build refuses - must not be retried on the health tick. Each attempt
	// downloads and parses the whole league, so a tight retry loop is fatal on
	// a phone long before it is useful anywhere.
	test("a failed checkpoint restore backs off instead of looping", async () => {
		const room = new Room();
		initRoom(room);
		const seed = new V2Transport("A", room);
		await seed.publishV2Checkpoint(5, "not a usable payload", "gen1");
		await seed.commitV2Checkpoint(5, 1, "gen1");
		room.setState({ ...room.state!, version: 6 });

		(idb as any).league = makeLeagueDb({ players: [] });
		const transport = new V2Transport("B", room);
		let fetches = 0;
		const realFetch = transport.fetchV2Checkpoint.bind(transport);
		transport.fetchV2Checkpoint = (v: number, c?: number, g?: string) => {
			fetches += 1;
			return realFetch(v, c, g);
		};
		const engine = new SyncEngineV2(transport);

		for (let i = 0; i < 5; i++) {
			assert.strictEqual(await engine.catchUp(), false);
		}
		assert.strictEqual(
			fetches,
			1,
			"the whole league must be fetched once, not once per attempt",
		);
		engine.stop();
	});

	// Rebuilding the checkpoint means reading the ENTIRE league into memory,
	// stringifying it and gzipping it - the most expensive thing the sync layer
	// does. At 25 versions (one version = one user action) a phone was doing
	// that every few minutes of play and iOS killed the tab for it. Two
	// independent brakes now: a version interval well clear of a session's
	// worth of actions, and a wall-clock throttle on even considering it.
	test("the checkpoint rebuild is rare enough for a phone to survive", async () => {
		const room = new Room();
		initRoom(room);
		(idb as any).league = makeLeagueDb({ players: [] });
		const transport = new V2Transport("A", room);
		let builds = 0;
		transport.publishV2Checkpoint = async () => {
			builds += 1;
			return 1;
		};
		const engine = new SyncEngineV2(transport);
		engine.start();
		await engine.claimAuthority();

		// A busy session: dozens of versions land in quick succession.
		(engine as any).roomState = { ...room.state!, version: 60 };
		(engine as any).appliedMirror = 60;
		for (let i = 0; i < 10; i++) {
			await engine.maybePublishCheckpoint({ enabled: true });
		}
		assert.strictEqual(
			builds,
			0,
			"60 versions must not trigger a full-database rebuild",
		);

		// Even once the interval IS exceeded, the wall-clock throttle keeps the
		// rebuild from firing repeatedly off the 5-second health tick.
		(engine as any).roomState = { ...room.state!, version: 5000 };
		(engine as any).appliedMirror = 5000;
		(engine as any).lastCheckpointCheckAt = Date.now();
		for (let i = 0; i < 10; i++) {
			await engine.maybePublishCheckpoint({ enabled: true });
		}
		assert.strictEqual(
			builds,
			0,
			"the throttle must hold even when the version interval has elapsed",
		);
		engine.stop();
	});

	// The other Safari shape from the field: the pointer arrived instantly, the
	// gap was one version, nothing failed - and the apply silently took 30
	// seconds (an IndexedDB stall) with NOTHING on screen. The health-tick
	// watchdog is the only indicator for that case: two ticks of known-behind
	// shows the pill, without needing anything to fail first.
	test("known-behind across ticks shows the indicator even when nothing fails", () => {
		const room = new Room();
		initRoom(room);
		(idb as any).league = makeLeagueDb({ players: [] });
		const transport = new V2Transport("B", room);
		const progress: ({ done: number; total: number } | undefined)[] = [];
		const engine = new SyncEngineV2(transport, {
			onCatchUpProgress: (p) => {
				progress.push(p);
			},
		});
		// The stuck state itself: the listener delivered the pointer (head 5),
		// the device has applied 3, and the walk is in flight but hung - so
		// nothing errors and the walk never reports.
		(engine as any).roomVersion = 5;
		(engine as any).appliedMirror = 3;
		(engine as any).catchingUp = true;

		engine.reportIfStuckBehind();
		assert.strictEqual(
			progress.length,
			0,
			"one tick stays quiet - ordinary live applies finish well inside it",
		);
		engine.reportIfStuckBehind();
		assert.deepStrictEqual(progress.at(-1), { done: 3, total: 5 });
		engine.reportIfStuckBehind();
		assert.strictEqual(
			progress.length,
			1,
			"unchanged figures do not re-report every tick",
		);

		// The gap closes with no walk left to clear the pill - the watchdog
		// mops up its own indicator.
		(engine as any).appliedMirror = 5;
		(engine as any).catchingUp = false;
		engine.reportIfStuckBehind();
		assert.strictEqual(progress.at(-1), undefined, "indicator cleared");
		engine.stop();
	});

	// The Safari-backgrounded shape from the field: the pointer read works but
	// the DELTA reads hang/fail, so probes keep succeeding and only catch-up
	// fails. Those failures must (a) surface the catching-up indicator so the
	// user sees "working on it" instead of nothing, (b) arm the network cycle
	// themselves, and (c) clear the indicator once the fetch finally lands.
	test("failing delta fetches show progress, cycle the network, and clear on success", async () => {
		const room = new Room();
		initRoom(room);
		const seedTransport = new V2Transport("C", room);
		await seedTransport.publishV2Delta(
			{ version: 1, authorId: "C", action: "main.releasePlayer", at: 1 },
			serializeChangeset(changesetOf(tradePut(1, 5))),
		);
		await seedTransport.commitV2Version(
			{
				version: 1,
				authorId: "C",
				byName: "C",
				at: 1,
				action: "main.releasePlayer",
			},
			0,
		);

		(idb as any).league = makeLeagueDb({ players: [] });
		const transport = new V2Transport("B", room);
		let cycles = 0;
		(transport as any).cycleNetwork = async () => {
			cycles += 1;
		};
		const realFetchV2Delta = transport.fetchV2Delta.bind(transport);
		let deltaReadsFail = true;
		transport.fetchV2Delta = (version: number) =>
			deltaReadsFail
				? Promise.reject(new Error("read hung"))
				: realFetchV2Delta(version);
		const progressEvents: ({ done: number; total: number } | undefined)[] = [];
		const engine = new SyncEngineV2(transport, {
			onCatchUpProgress: (progress) => {
				progressEvents.push(progress);
			},
		});

		assert.strictEqual(await engine.catchUp(), false);
		assert.deepStrictEqual(
			progressEvents.at(-1),
			{ done: 0, total: 1 },
			"a failing fetch while behind shows the catching-up indicator",
		);
		assert.strictEqual(cycles, 0, "one failure is a blip");
		assert.strictEqual(await engine.catchUp(), false);
		assert.strictEqual(
			cycles,
			1,
			"consecutive catch-up failures cycle the network even though probes succeed",
		);

		// The connection heals; the next pass lands the delta and clears the pill.
		deltaReadsFail = false;
		assert.ok(await engine.catchUp());
		assert.strictEqual(await readAppliedVersion(), 1);
		assert.strictEqual(
			progressEvents.at(-1),
			undefined,
			"caught up clears the indicator",
		);
		engine.stop();
	});

	// A deterministic failure (an unreadable checkpoint) fails identically on
	// every attempt; after three in a row the engine must back off instead of
	// hammering the network every second.
	test("repeated identical catch-up failures back off", async () => {
		const room = new Room();
		initRoom(room);
		const seedTransport = new V2Transport("C", room);
		await seedTransport.publishV2Delta(
			{ version: 1, authorId: "C", action: "main.releasePlayer", at: 1 },
			serializeChangeset(changesetOf(tradePut(1, 5))),
		);
		await seedTransport.commitV2Version(
			{
				version: 1,
				authorId: "C",
				byName: "C",
				at: 1,
				action: "main.releasePlayer",
			},
			0,
		);

		(idb as any).league = makeLeagueDb({ players: [] });
		const transport = new V2Transport("B", room);
		let stateFetches = 0;
		const realFetchState = transport.fetchRoomV2State.bind(transport);
		transport.fetchRoomV2State = () => {
			stateFetches += 1;
			return realFetchState();
		};
		transport.fetchV2Delta = () => Promise.reject(new Error("unreadable"));
		// No cycleNetwork on this transport, so failures accumulate cleanly.
		const engine = new SyncEngineV2(transport);

		assert.strictEqual(await engine.catchUp(), false);
		assert.strictEqual(await engine.catchUp(), false);
		assert.strictEqual(await engine.catchUp(), false);
		const fetchesBefore = stateFetches;
		assert.strictEqual(
			await engine.catchUp(),
			false,
			"still failing, but from the backoff",
		);
		assert.strictEqual(
			stateFetches,
			fetchesBefore,
			"the backed-off attempt never touched the network",
		);
		engine.stop();
	});

	// Small payloads ride the pointer doc, so a receiver applies them with ZERO
	// further reads - even when every read path is wedged (the Safari shape).
	test("a small edit applies straight from the pointer push, no reads needed", async () => {
		const room = new Room();
		initRoom(room);
		const seedTransport = new V2Transport("A", room);
		const serialized = serializeChangeset(changesetOf(tradePut(1, 5)));
		await seedTransport.publishV2Delta(
			{ version: 1, authorId: "A", action: "main.proposeTrade", at: 1 },
			serialized,
		);
		await seedTransport.commitV2Version(
			{
				version: 1,
				authorId: "A",
				byName: "A",
				at: 1,
				action: "main.proposeTrade",
				inlineDelta: serialized,
			},
			0,
		);

		(idb as any).league = makeLeagueDb({ players: [] });
		const transport = new V2Transport("B", room);
		// EVERYTHING that reads is dead; only the already-delivered pointer state
		// (the hint) is available.
		transport.fetchRoomV2State = () => Promise.reject(new Error("wedged"));
		transport.fetchV2Delta = () => Promise.reject(new Error("wedged"));
		const engine = new SyncEngineV2(transport);

		assert.ok(await engine.catchUp(room.state!));
		assert.strictEqual(await readAppliedVersion(), 1);
		assert.strictEqual(
			(await (idb as any).league.getAll("players"))[0].tid,
			5,
			"the edit landed from the pointer push alone",
		);
		engine.stop();
	});

	test("resyncAll is just catch-up: one recovery path", async () => {
		const room = new Room();
		initRoom(room);
		const authorTransport = new V2Transport("A", room);
		await authorTransport.publishV2Delta(
			{ version: 1, authorId: "A", action: "step", at: 1 },
			serializeChangeset(changesetOf(tradePut(1, 5))),
		);
		await authorTransport.commitV2Version(
			{ version: 1, authorId: "A", byName: "A", at: 1, action: "step" },
			0,
		);

		(idb as any).league = makeLeagueDb({ players: [] });
		const engine = new SyncEngineV2(new V2Transport("B", room));
		const result = await engine.resyncAll();
		assert.deepStrictEqual(result, {
			total: 1,
			applied: 1,
			incomplete: 0,
			failed: false,
		});
	});

	// THE BUG THIS ENDS: Force Resync used to route through an ordinary
	// catch-up, which finds nothing to do the moment applied === roomVersion -
	// exactly the state of a device whose counter is right and whose DATABASE is
	// wrong, which is the only reason anyone presses the button. The one big
	// hammer in the app was a no-op in its only use case.
	test("force resync restores the checkpoint even when the version says caught up", async () => {
		const room = new Room();
		initRoom(room);
		const authorTransport = new V2Transport("A", room);

		await authorTransport.publishV2Checkpoint(
			1,
			serializeChangeset({
				version: 1,
				stores: {
					players: [
						{ pid: 1, tid: 0 },
						{ pid: 2, tid: 0 },
						{ pid: 3, tid: 0 },
						{ pid: 4, tid: 0 },
						{ pid: 5, tid: 0 },
					],
					teams: [{ tid: 0 }],
					gameAttributes: [
						{ key: "season", value: 2006 },
						{ key: "phase", value: 1 },
					],
				},
			}),
		);
		await authorTransport.commitV2Checkpoint(1, 1);
		room.setState({
			...room.state!,
			version: 1,
			authorId: "A",
			byName: "A",
			at: 1,
		});

		// A device that believes it is fully caught up (marker at the head) but
		// whose league is missing two thirds of its roster.
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 0 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 1 }],
		});
		const engine = new SyncEngineV2(new V2Transport("B", room));

		// The ordinary path agrees there is nothing to do - which is the whole
		// problem, and why the button needs its own path. (Its reported count is
		// no use as evidence here: the version mirror starts at zero, so merely
		// READING the marker looks like progress. The database is the evidence.)
		await engine.resyncAll();
		assert.strictEqual(
			(await (idb as any).league.getAll("players")).length,
			1,
			"plain catch-up leaves the damaged league exactly as it was",
		);

		const result = await engine.forceCheckpointRestore();
		assert.strictEqual(result.failed, false);
		assert.strictEqual(result.incomplete, 0);
		const players = await (idb as any).league.getAll("players");
		assert.strictEqual(players.length, 5, "the checkpoint roster landed");
		assert.strictEqual(await readAppliedVersion(), 1);
	});

	// Reporting success after doing nothing is the failure mode above wearing a
	// disguise, so a room with no recovery point has to say so out loud.
	test("force resync refuses when the room has no checkpoint yet", async () => {
		const room = new Room();
		initRoom(room);
		(idb as any).league = makeLeagueDb({ players: [] });
		const engine = new SyncEngineV2(new V2Transport("B", room));

		let message = "";
		try {
			await engine.forceCheckpointRestore();
			assert.fail("expected a refusal");
		} catch (error) {
			message = (error as Error).message;
		}
		assert.ok(
			message.includes("hasn't published a checkpoint yet"),
			`unexpected message: ${message}`,
		);
		// And it must not leave the recovery flag armed - every later catch-up
		// would drag the whole league down again.
		assert.strictEqual(
			(engine as any).mustRecoverFromCheckpoint,
			false,
			"the forced-recovery flag is not left armed",
		);
	});

	// Readiness gates every sim. It used to be a one-way latch: the first
	// successful ping set it true and nothing ever set it false, so
	// "Cloud sync is not ready" could never fire again for the session however
	// dead the connection got.
	test("readiness expires and a dead listener revokes it", async () => {
		const room = new Room();
		initRoom(room);
		const transport = new V2Transport("B", room);
		const readyChanges: boolean[] = [];
		const engine = new SyncEngineV2(transport, {
			onReadyChange: (ready) => readyChanges.push(ready),
		});
		engine.start();

		await engine.ensureReady();
		assert.strictEqual(engine.isReady(), true);
		assert.deepStrictEqual(readyChanges, [true]);

		// The lease runs out on its own.
		(engine as any).readyUntil = Date.now() - 1;
		assert.strictEqual(engine.isReady(), false, "readiness expires");

		// And a re-probe renews it without needing a reconnect.
		await engine.ensureReady();
		assert.strictEqual(engine.isReady(), true);

		// A dead listener revokes it outright, and says so.
		(engine as any).stateListenerHealthy = false;
		(engine as any).markNotReady();
		assert.strictEqual(engine.isReady(), false);
		assert.strictEqual(readyChanges.at(-1), false);
		engine.stop();
	});

	// v2 shipped subscribeAuthority with no error handler. A Firestore listener
	// is terminal once it errors, so one blip left the device believing forever
	// that whoever was simming at that moment still was - and isRoomBusy()
	// frozen with it.
	test("a dead authority listener clears the holder and is rebuilt", async () => {
		const room = new Room();
		initRoom(room);
		const transport = new V2Transport("B", room);
		// Hand the engine a listener we can kill on demand.
		let fail: ((error: unknown) => void) | undefined;
		let subscribeCount = 0;
		transport.subscribeAuthority = (
			onChange: (a: Authority | undefined) => void,
			onError?: (error: unknown) => void,
		) => {
			subscribeCount += 1;
			fail = onError;
			return room.onAuthority(onChange);
		};

		const engine = new SyncEngineV2(transport);
		engine.start();
		room.setAuthority({ holderId: "A", holderName: "Alex" });
		assert.strictEqual(engine.getAuthority()?.holderName, "Alex");
		assert.strictEqual(subscribeCount, 1);

		fail?.(new Error("listener died"));
		assert.strictEqual(
			engine.getAuthority(),
			undefined,
			"a holder we can no longer vouch for is dropped, not remembered",
		);
		assert.strictEqual(engine.isReady(), false);

		await new Promise((resolve) => setTimeout(resolve, 5100));
		assert.strictEqual(subscribeCount, 2, "and the listener is rebuilt");
		assert.strictEqual(engine.getAuthority()?.holderName, "Alex");
		engine.stop();
	}, 15000);

	// The sync page's activity list and the debug capture both read
	// fetchRecentLog, which returned [] on v2 - so the one screen a person
	// checks to see whether anything is reaching their device showed nothing.
	test("recent versions are recorded for the activity list", async () => {
		const room = new Room();
		initRoom(room);
		const authorTransport = new V2Transport("A", room);
		await authorTransport.publishV2Delta(
			{ version: 1, authorId: "A", action: "playMenu.day", at: 1 },
			serializeChangeset(
				changesetOf(tradePut(1, 5), {
					store: "gameAttributes",
					id: "phase",
					type: "put",
					value: { key: "phase", value: 3 },
				}),
			),
		);
		await authorTransport.commitV2Version(
			{
				version: 1,
				authorId: "A",
				byName: "A",
				at: 1,
				action: "playMenu.day",
			},
			0,
		);

		(idb as any).league = makeLeagueDb({ players: [] });
		const engine = new SyncEngineV2(new V2Transport("B", room));
		assert.ok(await engine.catchUp());

		const recent = await engine.fetchRecentLog(10);
		assert.strictEqual(recent.length, 1);
		assert.strictEqual(recent[0]!.seq, 1);
		assert.strictEqual(recent[0]!.action, "playMenu.day");
		assert.strictEqual(recent[0]!.authorId, "A");
		assert.strictEqual(recent[0]!.records, 2);
		assert.deepStrictEqual(recent[0]!.attrs, ["phase"]);
	});

	// An idle room and a dead listener both leave the room's `at` stamp old.
	// Telling them apart is the only thing this field is for, so it has to
	// report DELIVERY, not authorship.
	test("last-delivery reports when the listener fired, not when the room wrote", async () => {
		const room = new Room();
		const engine = new SyncEngineV2(new V2Transport("B", room));
		assert.strictEqual(engine.getLastChangesDeliveryAt(), 0);

		const before = Date.now();
		engine.start();
		room.setState({
			version: 0,
			authorId: "init",
			byName: "Init",
			// Written long ago - an idle room. Delivery is what just happened.
			at: 1,
		});
		assert.ok(
			engine.getLastChangesDeliveryAt() >= before,
			"delivery is now, not the room's ancient write stamp",
		);
		engine.stop();
	});
});
