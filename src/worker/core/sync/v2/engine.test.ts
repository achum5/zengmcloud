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
	checkpoints = new Map<number, string>();
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

	subscribeAuthority(onChange: (a: Authority | undefined) => void) {
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

	subscribeRoomV2State(onChange: (s: V2StateDoc) => void) {
		return this.room.onState(onChange);
	}

	async publishV2Delta(
		meta: { version: number; authorId: string; action: string; at: number },
		serialized: string,
	) {
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

	async publishV2Checkpoint(version: number, serialized: string) {
		this.room.checkpoints.set(version, serialized);
		return 1;
	}

	async commitV2Checkpoint(version: number, chunkCount: number) {
		if (!this.room.state) {
			return false;
		}
		this.room.setState({
			...this.room.state,
			checkpointVersion: version,
			checkpointChunkCount: chunkCount,
		});
		return true;
	}

	async fetchV2Checkpoint(version: number) {
		return this.room.checkpoints.get(version);
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
		// tail delta (10) still exists.
		room.setState({
			version: 10,
			authorId: "A",
			byName: "A",
			at: 10,
		});
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
		await authorTransport.publishV2Delta(
			{ version: 10, authorId: "A", action: "playMenu.day", at: 10 },
			serializeChangeset(changesetOf(tradePut(5, 7))),
		);

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
});
