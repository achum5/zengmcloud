import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../../test/helpers.ts";
import { g } from "../../../util/index.ts";
import { idb } from "../../../db/index.ts";
import { changeTracker } from "../../../db/changeTracker.ts";
import { setApplyGuard } from "../applyGuard.ts";
import {
	applyCheckpointV2,
	applyVersionedChangeset,
	readAppliedVersion,
} from "./applyVersion.ts";
import { APPLIED_VERSION_KEY, type VersionedChangeset } from "./protocol.ts";

// The soundness core of v2: the data and the applied-version marker commit in
// the same transaction, so no kill at any moment can manufacture a marker
// that lies about the data. Every v1 wipe reduces to exactly that lie.

// In-memory league DB modeling IndexedDB transaction semantics across
// MULTIPLE stores: operations buffer, and a killed transaction applies
// NOTHING - not "everything before the kill".
const makeLeagueDb = (
	initial: Record<string, any[]>,
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
			const putCounts = new Map<string, number>();
			let killed = false;
			return {
				objectStore: (name: string) => ({
					clear: () => {
						buffered.push({ store: name, op: "clear" });
					},
					put: (row: any) => {
						const count = (putCounts.get(name) ?? 0) + 1;
						putCounts.set(name, count);
						if (
							killAfterPuts &&
							killAfterPuts.store === name &&
							count > killAfterPuts.puts
						) {
							killed = true;
							return;
						}
						buffered.push({ store: name, op: "put", row });
					},
					delete: (key: any) => {
						buffered.push({ store: name, op: "delete", key });
					},
				}),
				get done() {
					if (killed) {
						return Promise.reject(
							new Error("Attempt to write without an in-progress transaction"),
						);
					}
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

const vcs = (
	version: number,
	changes: VersionedChangeset["changeset"]["changes"],
): VersionedChangeset => ({
	version,
	authorId: "author-1",
	action: "playMenu.day",
	changeset: { changes },
	at: 1,
});

const tradePut = (pid: number, tid: number) => ({
	store: "players" as any,
	id: pid,
	type: "put" as const,
	value: { pid, tid },
});

describe("applyVersionedChangeset", () => {
	let restoreCache: () => void;

	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2006);
		await resetCache({});
		restoreCache = stubCacheLifecycle();
		changeTracker.disable();
		changeTracker.reset();
	});

	afterEach(() => {
		restoreCache();
		setApplyGuard(undefined);
	});

	test("data and marker commit together, and the cache follows", async () => {
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 5 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 7 }],
		});

		const outcome = await applyVersionedChangeset(
			vcs(8, [tradePut(1, 2), tradePut(9, 2)]),
		);

		assert.strictEqual(outcome, "apply");
		assert.strictEqual(await readAppliedVersion(), 8);
		const players = await (idb as any).league.getAll("players");
		assert.strictEqual(players.find((p: any) => p.pid === 1).tid, 2);
		assert.strictEqual(players.find((p: any) => p.pid === 9).tid, 2);
		// And the in-memory cache mirrors the committed truth.
		assert.strictEqual((await idb.cache.players.get(1) as any)?.tid, 2);
	});

	test("a device with no marker starts at 0 and applies version 1", async () => {
		(idb as any).league = makeLeagueDb({ players: [] });
		assert.strictEqual(await readAppliedVersion(), 0);
		const outcome = await applyVersionedChangeset(vcs(1, [tradePut(1, 0)]));
		assert.strictEqual(outcome, "apply");
		assert.strictEqual(await readAppliedVersion(), 1);
	});

	test("a duplicate is skipped without touching anything", async () => {
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 5 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 8 }],
		});
		const outcome = await applyVersionedChangeset(vcs(8, [tradePut(1, 2)]));
		assert.strictEqual(outcome, "duplicate");
		const players = await (idb as any).league.getAll("players");
		assert.strictEqual(
			players[0].tid,
			5,
			"a duplicate must not rewrite records",
		);
	});

	// THE MISSED-DAY FORK, MADE UNREPRESENTABLE. v1 applied day 12 over a
	// skipped day 11; v2 cannot construct that state.
	test("a gap refuses, applies nothing, and moves no marker", async () => {
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 5 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 8 }],
		});
		const outcome = await applyVersionedChangeset(vcs(10, [tradePut(1, 2)]));
		assert.strictEqual(outcome, "gap");
		assert.strictEqual(await readAppliedVersion(), 8);
		const players = await (idb as any).league.getAll("players");
		assert.strictEqual(players[0].tid, 5);
	});

	// THE KILL. iOS backgrounds the app mid-write: v1's separated writes
	// manufactured a marker that lied about the data. Here both roll back.
	test("a kill mid-apply rolls back data AND marker together", async () => {
		(idb as any).league = makeLeagueDb(
			{
				players: [
					{ pid: 1, tid: 5 },
					{ pid: 2, tid: 5 },
				],
				gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 8 }],
			},
			// Dies after one players put - before the marker put ever runs.
			{ store: "players", puts: 1 },
		);

		let threw = false;
		try {
			await applyVersionedChangeset(vcs(9, [tradePut(1, 2), tradePut(2, 2)]));
		} catch {
			threw = true;
		}

		assert.ok(threw);
		assert.strictEqual(
			await readAppliedVersion(),
			8,
			"the marker must still say 8 - a marker at 9 over version-8 data is the lie every v1 wipe was made of",
		);
		const players = await (idb as any).league.getAll("players");
		assert.deepStrictEqual(
			players.map((p: any) => p.tid),
			[5, 5],
			"and the data must still be version 8's, whole",
		);
	});

	test("the wrong league is refused before anything is read or written", async () => {
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 1, tid: 5 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 8 }],
		});
		setApplyGuard(() => false);
		let threw = false;
		try {
			await applyVersionedChangeset(vcs(9, [tradePut(1, 2)]));
		} catch {
			threw = true;
		}
		assert.ok(threw);
		assert.strictEqual(await readAppliedVersion(), 8);
	});

	test("deletes ride in the same transaction as puts and the marker", async () => {
		(idb as any).league = makeLeagueDb({
			negotiations: [{ pid: 55 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 3 }],
		});
		const outcome = await applyVersionedChangeset(
			vcs(4, [
				{ store: "negotiations" as any, id: 55, type: "delete", value: undefined },
			]),
		);
		assert.strictEqual(outcome, "apply");
		assert.deepStrictEqual(await (idb as any).league.getAll("negotiations"), []);
		assert.strictEqual(await readAppliedVersion(), 4);
	});
});

describe("applyCheckpointV2", () => {
	let restoreCache: () => void;

	const checkpointPayload = () => ({
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

	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2006);
		await resetCache({});
		restoreCache = stubCacheLifecycle();
	});

	afterEach(() => {
		restoreCache();
	});

	test("a good checkpoint lands, and the marker is written last", async () => {
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 99, tid: 3 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 2 }],
		});

		await applyCheckpointV2(checkpointPayload(), 30);

		assert.strictEqual(await readAppliedVersion(), 30);
		const players = await (idb as any).league.getAll("players");
		assert.strictEqual(players.length, 5);
	});

	// A kill during the store replacement leaves the OLD marker, so the next
	// launch re-runs the whole checkpoint instead of trusting a half-restored
	// database. The marker only ever advances over a database that fully IS
	// the checkpoint.
	test("a kill mid-checkpoint leaves the marker behind, forcing a clean retry", async () => {
		(idb as any).league = makeLeagueDb(
			{
				players: [
					{ pid: 99, tid: 3 },
					{ pid: 98, tid: 3 },
				],
				gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 2 }],
			},
			{ store: "players", puts: 2 },
		);

		let threw = false;
		try {
			await applyCheckpointV2(checkpointPayload(), 30);
		} catch {
			threw = true;
		}

		assert.ok(threw);
		assert.strictEqual(
			await readAppliedVersion(),
			2,
			"a marker at 30 over a half-restored database would be a lie; it must still say 2",
		);
	});

	test("a poisoned checkpoint is refused with everything untouched", async () => {
		(idb as any).league = makeLeagueDb({
			players: [{ pid: 99, tid: 3 }],
			gameAttributes: [{ key: APPLIED_VERSION_KEY, value: 2 }],
		});

		let message = "";
		try {
			await applyCheckpointV2(
				{
					version: 1,
					stores: {
						players: [{ pid: 1, tid: 0 }],
						teams: [{ tid: 0 }],
						gameAttributes: [
							{ key: "season", value: 2006 },
							{ key: "phase", value: 1 },
						],
					},
				},
				30,
			);
		} catch (error) {
			message = (error as Error).message;
		}

		assert.match(message, /rosters stripped/);
		assert.strictEqual(await readAppliedVersion(), 2);
		assert.strictEqual(
			((await (idb as any).league.getAll("players")) as any[]).length,
			1,
		);
	});
});
