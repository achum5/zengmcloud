import "fake-indexeddb/auto";
import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../../test/helpers.ts";
import { g } from "../../../util/index.ts";
import { idb } from "../../../db/index.ts";
import { changeTracker } from "../../../db/changeTracker.ts";
import { serializeChangeset } from "../serialize.ts";
import type { Authority, SyncTransport, V2StateDoc } from "../types.ts";
import { SyncEngineV2 } from "./engine.ts";
import { APPLIED_VERSION_KEY } from "./protocol.ts";

// Realistic free agency keeps a human team's offers ON its team record
// (t.offerBoard, freeAgency.ts makeOffer), and a free-agency day rewrites that
// same record (pruning resolved offers). Sync is whole-record, version-ordered,
// so a publish that had to catch up first merges the boards
// (offerBoardMerge.ts). Harness trimmed from engine.test.ts (in-memory room,
// one league DB per device, swapped in turn).

class Room {
	state: V2StateDoc | undefined;
	deltas = new Map<
		number,
		{ serialized: string; authorId: string; action: string; at: number }
	>();
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
	async registerMember() {}
	subscribeAuthority(onChange: (a: Authority | undefined) => void) {
		return this.room.onAuthority(onChange);
	}
	async publishBusy() {}
	async publishNotification() {}
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
		if ((this.room.state?.version ?? 0) >= meta.version) {
			throw new Error("v2-slot-taken");
		}
		this.room.deltas.set(meta.version, { ...meta, serialized });
		return 1;
	}
	async publishAndCommitV2Version(
		next: {
			version: number;
			authorId: string;
			byName: string;
			at: number;
			action: string;
			inlineDelta?: string;
		},
		serialized: string,
		expectedVersion: number,
	) {
		if ((this.room.state?.version ?? 0) !== expectedVersion) {
			return false;
		}
		this.room.deltas.set(next.version, {
			serialized,
			authorId: next.authorId,
			action: next.action,
			at: next.at,
		});
		this.room.setState({ ...next });
		return true;
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
		if ((this.room.state?.version ?? 0) !== expectedVersion) {
			return false;
		}
		this.room.setState({ ...next });
		return true;
	}
	async fetchV2Delta(version: number) {
		return this.room.deltas.get(version);
	}
	async fetchV2Checkpoint() {
		return undefined;
	}
	async deleteV2DeltasBefore() {
		return 0;
	}
}

const makeLeagueDb = (initial: Record<string, any[]>) => {
	const stores = new Map<string, Map<any, any>>();
	const pkOf = (row: any) =>
		row.key ?? row.pid ?? row.tid ?? row.gid ?? row.rid ?? JSON.stringify(row);
	for (const [store, rows] of Object.entries(initial)) {
		stores.set(store, new Map(rows.map((row) => [pkOf(row), row])));
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
			const buffered: { store: string; op: string; row?: any; key?: any }[] =
				[];
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

// Team 1 is person A's. Before the day it has one offer out, for pid 7.
const offer = (pid: number) => ({ pid, amount: 1000, exp: 2027 });
const team1 = (pids: number[], ticketPrice: number) => ({
	tid: 1,
	offerBoard: pids.map(offer),
	budget: { ticketPrice },
});
const put = (store: string, value: any) => ({
	store: store as any,
	id: value.tid ?? value.pid,
	type: "put" as const,
	value,
});

// The day, as the simmer's changeset carries it: pid 7 signed with team 3,
// so freeAgency.ts:1105-1109 pruned him from team 1's board (and, in season,
// writeTeamStats would have set a ticket price on the same record).
const DAY = () => ({
	changes: [put("players", { pid: 7, tid: 3 }), put("teams", team1([], 35))],
});
// A's offer, as afterAction captures it: the whole team record.
const OFFER = () => ({ changes: [put("teams", team1([7, 50], 30))] });

const atVersion = (v: number, rows: Record<string, any[]>) =>
	makeLeagueDb({
		...rows,
		gameAttributes: [{ key: APPLIED_VERSION_KEY, value: v }],
	});

const team1On = async (db: ReturnType<typeof makeLeagueDb>) =>
	(await db.getAll("teams")).find((t: any) => t.tid === 1);

// What every device that was NOT party to the race ends up with.
const replayFromZero = async (room: Room) => {
	const db = atVersion(0, {
		teams: [team1([7], 30)],
		players: [
			{ pid: 7, tid: -1 },
			{ pid: 50, tid: -1 },
		],
	});
	(idb as any).league = db;
	await resetCache({});
	const engine = new SyncEngineV2(new V2Transport("C", room));
	assert.ok(await engine.catchUp());
	engine.stop();
	return team1On(db);
};

describe("realistic FA offers vs a free-agency day", () => {
	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2026);
		g.setWithoutSavingToDB("phase", 8);
		await resetCache({});
		changeTracker.disable();
		changeTracker.reset();
	});
	afterEach(() => {
		(idb as any).league = undefined;
	});

	// (b) A's offer reaches the room while the simmer's day is running (A had
	// not yet received the busy stamp, so the gate let it through). The day
	// was computed without it and lands on top - whole team record and all.
	test("an offer that lands mid-day survives the day", async () => {
		const room = new Room();
		room.setState({ version: 0, authorId: "init", byName: "Init", at: 1 });
		room.setAuthority({ holderId: "S", holderName: "Sam", busyUntil: 0 });

		// A publishes its offer: confirmed, version 1.
		const dbA = atVersion(0, {
			teams: [team1([7, 50], 30)],
			players: [{ pid: 7, tid: -1 }],
		});
		(idb as any).league = dbA;
		await resetCache({});
		const engineA = new SyncEngineV2(new V2Transport("A", room));
		engineA.start();
		assert.strictEqual(
			await engineA.onLocalChangeset(OFFER(), "main.makeFreeAgentOffer"),
			"confirmed",
		);
		await engineA.catchUp();
		engineA.stop();
		assert.strictEqual(room.state!.version, 1);

		// The simmer finishes its day (state already on its disk) and publishes.
		const dbS = atVersion(0, {
			teams: [team1([], 35)],
			players: [
				{ pid: 7, tid: 3 },
				{ pid: 50, tid: -1 },
			],
		});
		(idb as any).league = dbS;
		await resetCache({});
		const engineS = new SyncEngineV2(new V2Transport("S", room));
		engineS.start();
		await engineS.claimAuthority();
		assert.strictEqual(
			await engineS.onLocalChangeset(DAY(), "playMenu.day"),
			"confirmed",
		);
		await engineS.catchUp();
		engineS.stop();
		assert.strictEqual(room.state!.version, 2);
		assert.strictEqual(room.state!.action, "playMenu.day");

		// Everyone: A's offer for pid 50 stands, pid 7 (signed elsewhere) is
		// gone, and the day's ticket price holds.
		const elsewhere = await replayFromZero(room);
		assert.deepEqual(
			elsewhere.offerBoard.map((o: any) => o.pid),
			[50],
		);
		assert.strictEqual(elsewhere.budget.ticketPrice, 35);

		(idb as any).league = dbA;
		await resetCache({});
		const engineA2 = new SyncEngineV2(new V2Transport("A", room));
		assert.ok(await engineA2.catchUp());
		engineA2.stop();
		assert.deepEqual(
			(await team1On(dbA)).offerBoard.map((o: any) => o.pid),
			[50],
		);

		// The simmer's own disk agrees with the room.
		const onS = await team1On(dbS);
		assert.deepEqual(
			onS.offerBoard.map((o: any) => o.pid),
			[50],
		);
		assert.strictEqual(onS.budget.ticketPrice, 35);
	});

	// (b), other order: A's offer could not go up before the day landed (held
	// for the busy stamp, or a publish that timed out to "queued"). The drain
	// catches A up - overwriting A's team with the day's - then republishes
	// the team record A captured BEFORE the day.
	test("a queued offer goes up on the day's team, not over it", async () => {
		const room = new Room();
		room.setState({ version: 0, authorId: "init", byName: "Init", at: 1 });
		room.setAuthority({
			holderId: "S",
			holderName: "Sam",
			busyUntil: Date.now() + 60_000,
		} as any);

		const dbA = atVersion(0, {
			teams: [team1([7, 50], 30)],
			players: [
				{ pid: 7, tid: -1 },
				{ pid: 50, tid: -1 },
			],
		});
		(idb as any).league = dbA;
		await resetCache({});
		const engineA = new SyncEngineV2(new V2Transport("A", room));
		engineA.start();
		assert.strictEqual(
			await engineA.onLocalChangeset(OFFER(), "main.makeFreeAgentOffer"),
			"queued",
		);

		// The simmer's day lands as version 1 and the stamp is cleared.
		const day = serializeChangeset(DAY());
		await new V2Transport("S", room).publishAndCommitV2Version(
			{
				version: 1,
				authorId: "S",
				byName: "Sam",
				at: 2,
				action: "playMenu.day",
				inlineDelta: day,
			},
			day,
			0,
		);
		room.setAuthority({ holderId: "S", holderName: "Sam", busyUntil: 0 });
		assert.ok(await engineA.drainOutbox());
		await engineA.catchUp();
		engineA.stop();
		assert.strictEqual(room.state!.version, 2);

		// The room and A's own device: A's offer for pid 50, not the one for
		// pid 7 (signed elsewhere), and the day's ticket price.
		for (const t of [await replayFromZero(room), await team1On(dbA)]) {
			assert.deepEqual(
				t.offerBoard.map((o: any) => o.pid),
				[50],
			);
			assert.strictEqual(t.budget.ticketPrice, 35);
		}
	});

	// (a) Two people, two teams, same moment: different records, both survive.
	test("two teams' offers at the same moment both survive", async () => {
		const room = new Room();
		room.setState({ version: 0, authorId: "init", byName: "Init", at: 1 });
		const team2 = (pids: number[]) => ({
			tid: 2,
			offerBoard: pids.map(offer),
			budget: { ticketPrice: 30 },
		});

		// B wins the slot first.
		const b = serializeChangeset({ changes: [put("teams", team2([60]))] });
		await new V2Transport("B", room).publishAndCommitV2Version(
			{
				version: 1,
				authorId: "B",
				byName: "B",
				at: 2,
				action: "main.makeFreeAgentOffer",
				inlineDelta: b,
			},
			b,
			0,
		);

		// A, still at version 0, publishes: loses the CAS, catches up, retries.
		const dbA = atVersion(0, {
			teams: [team1([7, 50], 30), team2([])],
			players: [7, 50, 60].map((pid) => ({ pid, tid: -1 })),
		});
		(idb as any).league = dbA;
		await resetCache({});
		const engineA = new SyncEngineV2(new V2Transport("A", room));
		await engineA.onLocalChangeset(OFFER(), "main.makeFreeAgentOffer");
		await engineA.catchUp();
		engineA.stop();
		assert.strictEqual(room.state!.version, 2);
		const teams = await dbA.getAll("teams");
		assert.deepEqual(
			teams.map((t: any) => t.offerBoard.map((o: any) => o.pid)),
			[[7, 50], [60]],
		);
	});
});
