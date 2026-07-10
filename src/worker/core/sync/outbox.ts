import { openDB, type IDBPDatabase } from "@dumbmatter/idb";
import type { ChangesetEntry } from "./types.ts";

// A durable, room-scoped queue of change-log entries that have been produced but
// not yet CONFIRMED uploaded to Firestore.
//
// Publishing is fire-and-forget and can be interrupted: the tab is backgrounded
// or closed mid-upload, which kills any in-flight writes. That's especially bad
// for a chunked bulk change like a season rollover, where some chunks land and
// others don't - leaving a broken batch in the shared log that strands every
// follower. Persisting each entry here BEFORE the upload, and removing it only
// once Firestore confirms, means an interrupted upload simply finishes on the
// next launch instead of corrupting the room.
//
// This lives in its OWN IndexedDB database, isolated from the league and meta
// DBs, so it can never affect existing data. Adding an entry is a required part
// of publishing in a synced room: if we can't durably record the upload intent,
// the caller must treat the sync handoff as failed rather than pretending the
// change is safely queued. Cleanup operations are allowed to be best-effort,
// because a leftover entry simply retries idempotently later.

const DB_NAME = "bbgm-sync-outbox";
const STORE = "entries";

type OutboxRow = {
	key: string; // the entry's stable id (unique)
	code: string; // room code, so we only ever flush the current room's backlog
	entry: Omit<ChangesetEntry, "seq">;
	createdAt: number;
	// Strictly monotonic enqueue position. createdAt alone can collide within a
	// millisecond (a chunked bulk enqueues many rows back to back), and the drain
	// MUST publish in exact enqueue order - with whole-record last-write-wins, an
	// older value published after a newer one would roll the room back.
	order?: number;
};

// Monotonic order allocator: never goes backwards, even across sessions (a new
// session starts from the current clock, which is past any previous order
// unless the clock regressed - and then max() still keeps it monotonic).
let lastOrder = 0;
const nextOrder = () => {
	lastOrder = Math.max(Date.now(), lastOrder + 1);
	return lastOrder;
};

const rowOrder = (row: OutboxRow) => row.order ?? row.createdAt;

let dbPromise: Promise<IDBPDatabase> | undefined;

const getDB = () => {
	if (!dbPromise) {
		dbPromise = openDB(DB_NAME, 1, {
			upgrade(db) {
				const store = db.createObjectStore(STORE, { keyPath: "key" });
				store.createIndex("code", "code");
			},
		});
	}
	return dbPromise;
};

const bestEffort = async <T>(fn: () => Promise<T>): Promise<T | undefined> => {
	try {
		return await fn();
	} catch (error) {
		console.error("[sync] outbox operation failed", error);
		return undefined;
	}
};

export const outbox = {
	// Record an entry as "trying to upload". NOT best-effort: if this throws, the
	// caller must treat the change as not-yet-safe (and keep it for retry another
	// way), because durability here is the whole guarantee.
	async add(code: string, entry: Omit<ChangesetEntry, "seq">) {
		const db = await getDB();
		await db.put(STORE, {
			key: entry.id,
			code,
			entry,
			createdAt: Date.now(),
			order: nextOrder(),
		} satisfies OutboxRow);
	},

	// Record a whole batch of entries in ONE IndexedDB transaction, so a chunked
	// bulk change is durable all-or-nothing. Enqueueing chunks one by one left a
	// window where a failure (or a killed tab) mid-loop stranded a PARTIAL batch
	// in the outbox: its chunks published, but the batch could never complete on
	// receivers - and an incomplete batch pins every follower's watermark. Same
	// durability contract as add(): if this throws, NOTHING was queued and the
	// caller must retain the changes for retry.
	async addAll(code: string, entries: Omit<ChangesetEntry, "seq">[]) {
		const db = await getDB();
		const tx = db.transaction(STORE, "readwrite");
		for (const entry of entries) {
			void tx.store.put({
				key: entry.id,
				code,
				entry,
				createdAt: Date.now(),
				order: nextOrder(),
			} satisfies OutboxRow);
		}
		await tx.done;
	},

	// Mark an entry as confirmed uploaded.
	async remove(id: string) {
		await bestEffort(async () => {
			const db = await getDB();
			await db.delete(STORE, id);
		});
	},

	// Everything still pending for a room, in exact enqueue order (i.e. the order
	// they MUST publish in).
	async pending(code: string): Promise<Omit<ChangesetEntry, "seq">[]> {
		const db = await getDB();
		const rows = (await db.getAllFromIndex(STORE, "code", code)) as OutboxRow[];
		rows.sort((a, b) => rowOrder(a) - rowOrder(b));
		return rows.map((r) => r.entry);
	},

	// How many entries are still pending for a room (drives the header's
	// "queued" indicator).
	async count(code: string): Promise<number> {
		const db = await getDB();
		const rows = await db.getAllKeysFromIndex(STORE, "code", code);
		return rows.length;
	},

	// Drop entries older than maxAgeMs (e.g. a room the user never came back to),
	// so a permanently-failed upload can't make the outbox grow forever.
	async prune(maxAgeMs: number) {
		await bestEffort(async () => {
			const db = await getDB();
			const cutoff = Date.now() - maxAgeMs;
			const rows = (await db.getAll(STORE)) as OutboxRow[];
			const stale = rows.filter((r) => r.createdAt < cutoff);
			if (stale.length === 0) {
				return;
			}
			const tx = db.transaction(STORE, "readwrite");
			for (const row of stale) {
				await tx.store.delete(row.key);
			}
			await tx.done;
		});
	},
};
