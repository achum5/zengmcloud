import "fake-indexeddb/auto";
import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../../test/helpers.ts";
import { g } from "../../../util/index.ts";
import { idb } from "../../../db/index.ts";
import { changeTracker } from "../../../db/changeTracker.ts";
import { serializeChangeset } from "../serialize.ts";
import type {
	Authority,
	SyncTransport,
	V2Request,
	V2StateDoc,
} from "../types.ts";
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
	requests = new Map<string, V2Request>();
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

	async publishV2Request(request: V2Request) {
		this.room.requests.set(request.id, request);
	}

	async fetchV2Requests() {
		return [...this.room.requests.values()].sort((a, b) => a.at - b.at);
	}

	async deleteV2Request(id: string) {
		this.room.requests.delete(id);
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

	// One writer means one writer: losing the compare-and-set means the local
	// unpublished mutation is discarded, never merged.
	test("losing the CAS drops the entry and does not advance the marker", async () => {
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
		assert.strictEqual(outcome, "queued");
		assert.strictEqual(
			await readAppliedVersion(),
			0,
			"a lost race must not stamp the loser's marker",
		);
		assert.strictEqual(room.state!.version, 0);
		assert.strictEqual(
			await engine.pendingUploadCount(),
			0,
			"the losing entry is DROPPED (one writer), not retried into a fork",
		);
		engine.stop();
	});

	test("a follower's edit travels as a request; the authority folds it into the chain", async () => {
		const room = new Room();
		initRoom(room);

		// FOLLOWER files a roster edit.
		(idb as any).league = makeLeagueDb({ players: [{ pid: 4, tid: 1 }] });
		const followerTransport = new V2Transport("B", room);
		const follower = new SyncEngineV2(followerTransport);
		room.setAuthority({ holderId: "A", holderName: "Alex" });
		const outcome = await follower.onLocalChangeset(
			changesetOf(tradePut(4, 1)),
			"main.updatePlayingTime",
		);
		assert.strictEqual(outcome, "confirmed");
		assert.strictEqual(room.requests.size, 1);
		assert.strictEqual(room.state!.version, 0, "a follower never mints");

		// AUTHORITY drains the request queue.
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 4, tid: 3 }],
		});
		await resetCache({});
		const authorityTransport = new V2Transport("A", room);
		const authority = new SyncEngineV2(authorityTransport);
		authority.start();
		await authority.drainRequests();

		assert.strictEqual(room.state!.version, 1, "the edit became version 1");
		assert.strictEqual(room.requests.size, 0, "the request was consumed");
		assert.strictEqual(
			(await (idb as any).league.getAll("players"))[0].tid,
			1,
			"and the authority's own database took the edit",
		);
		assert.strictEqual(await readAppliedVersion(), 1);
		authority.stop();
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

		await engine.onLocalChangeset(
			changesetOf(tradePut(1, 2)),
			"playMenu.day",
			[{ title: "Celtics 115", body: "..." } as any],
		);
		assert.strictEqual(
			transport.notifications.length,
			0,
			"a lost commit must not announce anything",
		);

		await engine.onLocalChangeset(
			changesetOf(tradePut(1, 2)),
			"playMenu.day",
			[{ title: "Celtics 115", body: "..." } as any],
		);
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
