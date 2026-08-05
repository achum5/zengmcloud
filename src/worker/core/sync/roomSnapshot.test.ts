import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import {
	applyRoomSnapshotPayload,
	buildRoomSnapshotPayload,
	maybePublishRoomSnapshot,
	resetSnapshotCadenceForTesting,
	validateRoomSnapshotPayload,
} from "./roomSnapshot.ts";
import { serializeChangeset as ser } from "./serialize.ts";
import { serializeChangeset, deserializeChangeset } from "./serialize.ts";
import { setApplyGuard } from "./applyGuard.ts";

// ---------------------------------------------------------------------------
// The snapshot round trip: what the authority publishes must, applied on a
// far-behind device, produce the authority's state - including the parts a
// delta replay can never fix (rows DELETED since the stale device's state) and
// excluding the parts that must never travel (this device's identity, personal
// scratch stores).
//
// And, above all, it must never be able to half-happen. A restore replaces the
// whole database; an interrupted one used to leave stores cleared and partly
// refilled, permanently, which is how a league came back with two players on
// every roster.
// ---------------------------------------------------------------------------

// An in-memory stand-in for idb.league. Models TRANSACTIONS the way IndexedDB
// really behaves, because that is the property under test: operations buffer,
// and a transaction that fails applies NOTHING.
const makeLeagueDb = (
	initial: Record<string, any[]>,
	// Simulate the browser killing a write transaction partway - iOS does this
	// whenever a PWA is backgrounded mid-write.
	killAfterPuts?: { store: string; puts: number },
) => {
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
		getAll: async (store: string) => [...(stores.get(store)?.values() ?? [])],
		transaction: (storeName?: string) => {
			// The cache-flush stub calls this with no store.
			if (storeName === undefined) {
				return { done: Promise.resolve() };
			}
			const buffered: ({ op: "clear" } | { op: "put"; row: any })[] = [];
			let putCount = 0;
			let killed = false;
			return {
				objectStore: () => ({
					clear: () => {
						buffered.push({ op: "clear" });
					},
					put: (row: any) => {
						putCount += 1;
						if (
							killAfterPuts &&
							killAfterPuts.store === storeName &&
							putCount > killAfterPuts.puts
						) {
							killed = true;
							return;
						}
						buffered.push({ op: "put", row });
					},
				}),
				get done() {
					if (killed) {
						// An aborted IndexedDB transaction rolls everything back.
						return Promise.reject(
							new Error("Attempt to write without an in-progress transaction"),
						);
					}
					const target = ensure(storeName);
					for (const entry of buffered) {
						if (entry.op === "clear") {
							target.clear();
						} else {
							target.set(pkOf(entry.row), entry.row);
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

const AUTHORITY_GA = [
	{ key: "season", value: 2008 },
	{ key: "phase", value: 1 },
	{ key: "userTid", value: 3 },
];

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
		// The AUTHORITY, three seasons ahead: current rosters (full ones - a
		// mid-season payload with thin rosters is now REFUSED by design), and a
		// negotiation store that is EMPTY because those rows were deleted long
		// ago.
		(idb as any).league = makeLeagueDb({
			players: [
				{ pid: 1, name: "Kept", tid: 0 },
				{ pid: 2, tid: 0 },
				{ pid: 3, tid: 0 },
				{ pid: 4, tid: 0 },
				{ pid: 5, tid: 0 },
				{ pid: 9, name: "Drafted Later", tid: 1 },
				{ pid: 10, tid: 1 },
				{ pid: 11, tid: 1 },
				{ pid: 12, tid: 1 },
				{ pid: 13, tid: 1 },
			],
			teams: [{ tid: 0 }, { tid: 1 }],
			negotiations: [],
			gameAttributes: AUTHORITY_GA,
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
			teams: [{ tid: 0 }],
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
			[1, 2, 3, 4, 5, 9, 10, 11, 12, 13],
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
});

// ---------------------------------------------------------------------------
// The safety properties. Each of these is a way a league actually got wrecked.
// ---------------------------------------------------------------------------

describe("a restore can never leave the league worse than it found it", () => {
	let restoreCache: () => void;

	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2008);
		await resetCache({});
		restoreCache = stubCacheLifecycle();
	});

	afterEach(() => {
		restoreCache();
		local.autoSave = true;
	});

	const healthyLocalLeague = () => ({
		players: [
			{ pid: 1, tid: 0 },
			{ pid: 2, tid: 0 },
			{ pid: 3, tid: 1 },
			{ pid: 4, tid: 1 },
		],
		teams: [{ tid: 0 }, { tid: 1 }],
		gameAttributes: [{ key: "season", value: 2005 }],
	});

	// THE BUG THAT ATE A LEAGUE. The restore used to clear a store and then write
	// rows back one at a time, each its own auto-committing transaction. When the
	// browser killed the writes partway - backgrounding a phone mid-restore is
	// enough - the store stayed cleared and partly filled forever.
	test("a restore killed partway leaves every roster exactly as it was", async () => {
		(idb as any).league = makeLeagueDb(healthyLocalLeague(), {
			store: "players",
			puts: 2,
		});

		let threw = false;
		try {
			await applyRoomSnapshotPayload({
				version: 1,
				stores: {
					players: [
						{ pid: 1, tid: 4 },
						{ pid: 2, tid: 4 },
						{ pid: 3, tid: 5 },
						{ pid: 4, tid: 5 },
						{ pid: 5, tid: 5 },
					],
					teams: [{ tid: 0 }, { tid: 1 }],
					gameAttributes: [{ key: "season", value: 2008 }],
				},
			} as any);
		} catch {
			threw = true;
		}

		assert.ok(threw, "an interrupted restore must surface as a failure");
		const players = await (idb as any).league.getAll("players");
		assert.strictEqual(
			players.length,
			4,
			"the rosters must survive intact - a half-applied restore is what strips a league down to two players a team",
		);
		assert.deepStrictEqual(
			players.map((p: any) => p.tid),
			[0, 0, 1, 1],
			"and they must still be the PRE-restore rows, not a mixture",
		);
	});

	// The cache holds the pre-restore league plus a queue of rows it intends to
	// write back. If it flushes into a store the restore has just emptied, those
	// stale rows become the entire store.
	test("the write-back cache is silenced while the database is being replaced", async () => {
		(idb as any).league = makeLeagueDb(healthyLocalLeague());

		let autoSaveDuringRestore: boolean | undefined;
		const realFill = idb.cache.fill;
		// fill() runs at the very end; sample the flag from inside the replace
		// loop instead, via the transaction the loop opens.
		const db = (idb as any).league;
		const realTransaction = db.transaction;
		db.transaction = (...args: any[]) => {
			if (args[0] !== undefined) {
				autoSaveDuringRestore = local.autoSave;
			}
			return realTransaction.apply(db, args);
		};

		await applyRoomSnapshotPayload({
			version: 1,
			stores: {
				players: [{ pid: 7, tid: 2 }],
				teams: [{ tid: 2 }],
				gameAttributes: [{ key: "season", value: 2008 }],
			},
		} as any);
		idb.cache.fill = realFill;

		assert.strictEqual(
			autoSaveDuringRestore,
			false,
			"a flush landing mid-restore would refill a just-emptied store with rows from the database being replaced",
		);
		assert.strictEqual(
			local.autoSave,
			true,
			"and saving must be back on afterwards, or the league silently stops persisting",
		);
	});

	test("saving is restored even when the replace throws", async () => {
		(idb as any).league = makeLeagueDb(healthyLocalLeague(), {
			store: "players",
			puts: 0,
		});
		try {
			await applyRoomSnapshotPayload({
				version: 1,
				stores: {
					players: [{ pid: 7, tid: 2 }],
					teams: [{ tid: 2 }],
					gameAttributes: [{ key: "season", value: 2008 }],
				},
			} as any);
		} catch {
			// Expected.
		}
		assert.strictEqual(local.autoSave, true);
	});

	test("a future format version refuses cleanly instead of half-applying", async () => {
		(idb as any).league = makeLeagueDb(healthyLocalLeague());
		let threw = false;
		try {
			await applyRoomSnapshotPayload({ version: 99, stores: {} } as any);
		} catch {
			threw = true;
		}
		assert.ok(threw, "an unknown snapshot format must throw, not clear stores");
		assert.strictEqual(
			(await (idb as any).league.getAll("players")).length,
			4,
			"nothing may be cleared before the version check passes",
		);
	});

	// A publisher whose own database was broken, or a download that came back
	// short, produces a payload that is structurally valid and empty. Applying it
	// is how one broken device breaks everybody.
	test("an empty payload is refused before anything is destroyed", async () => {
		(idb as any).league = makeLeagueDb(healthyLocalLeague());
		let message = "";
		try {
			await applyRoomSnapshotPayload({
				version: 1,
				stores: { players: [], teams: [], gameAttributes: [] },
			} as any);
		} catch (error) {
			message = (error as Error).message;
		}
		assert.match(message, /players store is empty/);
		assert.strictEqual(
			(await (idb as any).league.getAll("players")).length,
			4,
			"a league must never be traded for an empty snapshot",
		);
	});

	// A snapshot restore replaces the WHOLE database, so landing one in the wrong
	// league file is the worst possible version of a bug ordinary changesets are
	// already guarded against.
	test("a restore aimed at the wrong league file is refused", async () => {
		(idb as any).league = makeLeagueDb(healthyLocalLeague());
		setApplyGuard(() => false);
		let message = "";
		try {
			await applyRoomSnapshotPayload({
				version: 1,
				stores: {
					players: [{ pid: 7, tid: 2 }],
					teams: [{ tid: 2 }],
					gameAttributes: [{ key: "season", value: 2008 }],
				},
			} as any);
		} catch (error) {
			message = (error as Error).message;
		} finally {
			setApplyGuard(undefined);
		}
		assert.match(message, /not the one this sync session belongs to/);
		assert.strictEqual(
			(await (idb as any).league.getAll("players")).length,
			4,
			"another league's snapshot must never overwrite the loaded one",
		);
	});

	test("a payload missing a critical store is refused too", async () => {
		(idb as any).league = makeLeagueDb(healthyLocalLeague());
		let threw = false;
		try {
			await applyRoomSnapshotPayload({
				version: 1,
				stores: { teams: [{ tid: 0 }], gameAttributes: [{ key: "season" }] },
			} as any);
		} catch {
			threw = true;
		}
		assert.ok(threw);
		assert.strictEqual((await (idb as any).league.getAll("players")).length, 4);
	});
});

describe("validateRoomSnapshotPayload", () => {
	const ok = {
		version: 1,
		stores: {
			players: [{ pid: 1 }],
			teams: [{ tid: 0 }],
			gameAttributes: [{ key: "season", value: 2008 }],
		},
	} as any;

	test("passes a real payload", () => {
		assert.deepStrictEqual(validateRoomSnapshotPayload(ok), []);
	});

	test("names every missing piece, so the log says what was wrong", () => {
		const problems = validateRoomSnapshotPayload({
			version: 1,
			stores: { teams: [] },
		} as any);
		assert.ok(problems.some((p) => p.includes("players")));
		assert.ok(problems.some((p) => p.includes("teams")));
		assert.ok(problems.some((p) => p.includes("gameAttributes")));
	});

	test("a version mismatch short-circuits the rest", () => {
		const problems = validateRoomSnapshotPayload({
			version: 99,
			stores: {},
		} as any);
		assert.strictEqual(problems.length, 1);
	});
});

// ---------------------------------------------------------------------------
// Poisoned-checkpoint eviction. A checkpoint published by a damaged device
// (before the publish gates existed) sits in the room as a landmine: old-build
// devices that fall behind restore it and get wiped; new-build devices refuse
// it and are left with no usable checkpoint at all. The healthy authority must
// REPLACE it immediately, not wait out the normal cadence.
// ---------------------------------------------------------------------------

describe("a healthy authority evicts a poisoned checkpoint", () => {
	let restoreCache: () => void;

	const healthyDb = () => ({
		players: [
			{ pid: 1, tid: 0 },
			{ pid: 2, tid: 0 },
			{ pid: 3, tid: 0 },
			{ pid: 4, tid: 0 },
			{ pid: 5, tid: 0 },
		],
		teams: [{ tid: 0 }],
		gameAttributes: [{ key: "season", value: 2006 }],
	});

	const poisonedPayload = () =>
		ser({
			version: 1,
			stores: {
				// The actual poison from the incident: one player on the roster,
				// mid regular season.
				players: [{ pid: 9, tid: 0 }],
				teams: [{ tid: 0 }],
				gameAttributes: [
					{ key: "season", value: 2006 },
					{ key: "phase", value: 1 },
				],
			},
		});

	const healthyPayload = () =>
		ser({
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
		});

	const makeEngine = (snapshotData: string) => {
		const published: unknown[] = [];
		const transport = {
			publishRoomSnapshot: async (meta: unknown, _serialized: string) => {
				published.push(meta);
				return 1;
			},
			fetchRoomSnapshotMeta: async () => ({
				seq: 500,
				at: 1,
				byName: "Old",
				chunkCount: 1,
			}),
			fetchRoomSnapshotData: async () => snapshotData,
			// Far below the 1200-entry cadence: only eviction can justify a publish.
			countEntriesSince: async () => 3,
			deleteEntriesBefore: async () => 0,
		};
		const engine = {
			transport,
			isAuthority: () => true,
			isBusyApplying: () => false,
			getPersistedSeq: () => 900,
			localName: "Healthy",
		};
		return { engine: engine as any, published };
	};

	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2006);
		g.setWithoutSavingToDB("phase", 1);
		await resetCache({});
		restoreCache = stubCacheLifecycle();
		resetSnapshotCadenceForTesting();
		(idb as any).league = makeLeagueDb(healthyDb());
	});

	afterEach(() => {
		restoreCache();
	});

	test("a poisoned checkpoint is replaced immediately, cadence be damned", async () => {
		const { engine, published } = makeEngine(poisonedPayload());
		await maybePublishRoomSnapshot(engine);
		assert.strictEqual(
			published.length,
			1,
			"the landmine must be replaced the moment a healthy authority sees it",
		);
	});

	test("a healthy checkpoint below cadence publishes nothing", async () => {
		const { engine, published } = makeEngine(healthyPayload());
		await maybePublishRoomSnapshot(engine);
		assert.strictEqual(published.length, 0);
	});

	test("an unreadable checkpoint counts as poisoned", async () => {
		const { engine, published } = makeEngine("not json at all {{{");
		await maybePublishRoomSnapshot(engine);
		assert.strictEqual(published.length, 1);
	});

	// The checkpoint is now the ONLY automatic recovery - the replay-over-live
	// fallbacks are gone - so a room without one has no self-heal at all. The
	// first checkpoint publishes promptly, not after 1200 entries.
	test("a room with no checkpoint at all gets its first one promptly", async () => {
		const { engine, published } = makeEngine(healthyPayload());
		(engine.transport as any).fetchRoomSnapshotMeta = async () => undefined;
		await maybePublishRoomSnapshot(engine);
		assert.strictEqual(published.length, 1);
	});
});
