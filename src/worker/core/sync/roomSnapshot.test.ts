import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import {
	applyRoomSnapshotPayload,
	buildRoomSnapshotPayload,
} from "./roomSnapshot.ts";
import { serializeChangeset, deserializeChangeset } from "./serialize.ts";

// ---------------------------------------------------------------------------
// The snapshot round trip: what the authority publishes must, applied on a
// far-behind device, produce the authority's state - including the parts a
// delta replay can never fix (rows DELETED since the stale device's state) and
// excluding the parts that must never travel (this device's identity, personal
// scratch stores).
// ---------------------------------------------------------------------------

// An in-memory stand-in for idb.league (the durable DB): getAll/clear/put per
// store, which is the exact surface the snapshot layer uses.
const makeLeagueDb = (initial: Record<string, any[]>) => {
	const stores = new Map<string, Map<any, any>>();
	const pkOf = (store: string, row: any) =>
		row.key ?? row.pid ?? row.tid ?? row.gid ?? row.rid ?? JSON.stringify(row);
	for (const [store, rows] of Object.entries(initial)) {
		const m = new Map();
		for (const row of rows) {
			m.set(pkOf(store, row), row);
		}
		stores.set(store, m);
	}
	return {
		getAll: async (store: string) => [...(stores.get(store)?.values() ?? [])],
		clear: async (store: string) => {
			stores.get(store)?.clear();
			if (!stores.has(store)) {
				stores.set(store, new Map());
			}
		},
		put: async (store: string, row: any) => {
			if (!stores.has(store)) {
				stores.set(store, new Map());
			}
			stores.get(store)!.set(pkOf(store, row), row);
		},
		// For the snapshot BUILD path, which flushes the cache first; our stub
		// cache flush is a no-op, so nothing else is needed.
		transaction: () => ({ done: Promise.resolve() }),
	};
};

const stubCacheLifecycle = () => {
	const original = { flush: idb.cache.flush, fill: idb.cache.fill };
	idb.cache.flush = async () => {};
	idb.cache.fill = (async () => {}) as any;
	return () => {
		idb.cache.flush = original.flush;
		idb.cache.fill = original.fill;
	};
};

describe("room snapshot round trip", () => {
	let restoreCache: () => void;

	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2008);
		g.setWithoutSavingToDB("phase", 1);
		await resetCache({});
		restoreCache = stubCacheLifecycle();
	});

	afterEach(() => {
		restoreCache();
	});

	test("a stale device ends up with the authority's state, deletions included", async () => {
		// The AUTHORITY, three seasons ahead: current rosters, and a negotiation
		// store that is EMPTY because those rows were deleted long ago.
		(idb as any).league = makeLeagueDb({
			players: [
				{ pid: 1, name: "Kept", tid: 0 },
				{ pid: 9, name: "Drafted Later", tid: 1 },
			],
			negotiations: [],
			gameAttributes: [
				{ key: "season", value: 2008 },
				{ key: "phase", value: 1 },
				{ key: "userTid", value: 3 },
			],
		});
		const payload = await buildRoomSnapshotPayload();

		// Cross the wire exactly as production does, Infinity-safe serializer and
		// all - retiredYear: Infinity corrupting to null is a real historic bug.
		const wire = deserializeChangeset(serializeChangeset(payload)) as any;

		// The STALE DEVICE: an old roster, a lingering negotiation the room
		// deleted seasons ago (deltas can never remove it - the delete entry is
		// long pruned), and crucially ITS OWN identity: this user controls tid 7.
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, name: "Old Version", tid: 5 }],
			negotiations: [{ pid: 55, tid: 7, resigning: false }],
			gameAttributes: [
				{ key: "season", value: 2005 },
				{ key: "phase", value: 3 },
				{ key: "userTid", value: 7 },
			],
		});

		await applyRoomSnapshotPayload(wire);

		const players = await (idb as any).league.getAll("players");
		assert.deepStrictEqual(
			players.map((p: any) => p.pid).sort((a: number, b: number) => a - b),
			[1, 9],
			"restore must produce the authority's roster, not a merge with the stale one",
		);
		assert.strictEqual(
			players.find((p: any) => p.pid === 1).tid,
			0,
			"an updated record must take the snapshot's version",
		);

		assert.deepStrictEqual(
			await (idb as any).league.getAll("negotiations"),
			[],
			"a row the room deleted seasons ago must not survive the restore - this is the one thing no delta replay can do",
		);

		const ga = await (idb as any).league.getAll("gameAttributes");
		const byKey = Object.fromEntries(ga.map((r: any) => [r.key, r.value]));
		assert.strictEqual(byKey.season, 2008, "shared attributes come across");
		assert.strictEqual(
			byKey.userTid,
			7,
			"WHICH TEAM THIS USER CONTROLS is per-device identity and must survive a restore - clobbering it hands the user someone else's franchise",
		);
	});

	test("personal scratch stores never travel in a snapshot", async () => {
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 0 }],
			trade: [{ rid: 0, teams: "my half-built trade" }],
			savedTrades: [{ hash: "abc" }],
			gameAttributes: [{ key: "season", value: 2008 }],
		});
		const payload = await buildRoomSnapshotPayload();
		assert.strictEqual(
			payload.stores.trade,
			undefined,
			"the authority's in-progress trade page must not be broadcast to the room",
		);
		assert.strictEqual(payload.stores.savedTrades, undefined);
		assert.ok(Array.isArray(payload.stores.players));
	});

	test("a future format version refuses cleanly instead of half-applying", async () => {
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 0 }],
		});
		let threw = false;
		try {
			await applyRoomSnapshotPayload({ version: 99, stores: {} } as any);
		} catch {
			threw = true;
		}
		assert.ok(threw, "an unknown snapshot format must throw, not clear stores");
		assert.strictEqual(
			(await (idb as any).league.getAll("players")).length,
			1,
			"nothing may be cleared before the version check passes",
		);
	});
});
