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
// DBs, so it can never affect existing data. Every operation is best-effort - a
// failure here degrades to the old behavior; it never breaks a publish.

const DB_NAME = "bbgm-sync-outbox";
const STORE = "entries";

type OutboxRow = {
	key: string; // the entry's stable id (unique)
	code: string; // room code, so we only ever flush the current room's backlog
	entry: Omit<ChangesetEntry, "seq">;
	createdAt: number;
};

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

// Never throw into the publish path - the outbox is a safety net, not a
// dependency.
const safe = async <T>(fn: () => Promise<T>): Promise<T | undefined> => {
	try {
		return await fn();
	} catch (error) {
		console.error("[sync] outbox operation failed", error);
		return undefined;
	}
};

export const outbox = {
	// Record an entry as "trying to upload".
	async add(code: string, entry: Omit<ChangesetEntry, "seq">) {
		await safe(async () => {
			const db = await getDB();
			await db.put(STORE, {
				key: entry.id,
				code,
				entry,
				createdAt: Date.now(),
			} satisfies OutboxRow);
		});
	},

	// Mark an entry as confirmed uploaded.
	async remove(id: string) {
		await safe(async () => {
			const db = await getDB();
			await db.delete(STORE, id);
		});
	},

	// Everything still pending for a room, oldest first (i.e. publish order).
	async pending(code: string): Promise<Omit<ChangesetEntry, "seq">[]> {
		const rows = await safe(async () => {
			const db = await getDB();
			return (await db.getAllFromIndex(STORE, "code", code)) as OutboxRow[];
		});
		if (!rows) {
			return [];
		}
		rows.sort((a, b) => a.createdAt - b.createdAt);
		return rows.map((r) => r.entry);
	},

	// Drop entries older than maxAgeMs (e.g. a room the user never came back to),
	// so a permanently-failed upload can't make the outbox grow forever.
	async prune(maxAgeMs: number) {
		await safe(async () => {
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
