import {
	collection,
	deleteDoc,
	doc,
	type DocumentReference,
	type Firestore,
	getDocs,
	getFirestore,
	limit,
	query,
	Timestamp,
	where,
	writeBatch,
} from "firebase/firestore";
import { getFirebaseApp } from "./firebaseApp.ts";
import { ensureAnonymousAuth } from "./auth.ts";

// Room-admin Firestore ops, run in the WORKER (which already has a working,
// authenticated Firebase). Doing this in the UI thread meant a second Firebase
// app on the same origin, which fought the worker's over IndexedDB and threw
// NoModificationAllowedError.

export type SyncRoom = {
	code: string;
	updatedAt?: number;
};

const getDb = async () => {
	await ensureAnonymousAuth();
	return getFirestore(getFirebaseApp());
};

// The subcollections a room accumulates. Deleting a room clears all of these,
// then the registry doc itself.
const SUBCOLLECTIONS = ["changes", "control", "members", "notifications"];

export const listSyncRooms = async (): Promise<SyncRoom[]> => {
	const db = await getDb();
	const snap = await getDocs(collection(db, "leagues"));
	return snap.docs
		.map((d) => {
			const data = d.data() as { updatedAt?: { toMillis?: () => number } };
			return {
				code: d.id,
				updatedAt:
					typeof data.updatedAt?.toMillis === "function"
						? data.updatedAt.toMillis()
						: undefined,
			};
		})
		.sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0));
};

// Adaptive cap on how many docs we delete per commit. Firestore caps a commit at
// 500 writes AND ~10 MiB - and the `changes` docs are big (each up to ~700 KB of
// serialized changeset, plus their index entries), so a batch blows past the SIZE
// cap long before it reaches 500 writes. The backend surfaces that as
// "Transaction too big. Decrease transaction size.", which is why deletes
// silently failed. We start optimistic and only ever SHRINK this cap: once a
// commit is rejected for size we remember the smaller size, so we don't keep
// re-hitting the ceiling on every subsequent page or room.
let maxDeleteBatch = 100;

// Delete the given doc refs, shrinking maxDeleteBatch and retrying whenever a
// commit is rejected for being too large - down to one doc if need be, which
// self-tunes to whatever the real ceiling is with no magic number.
const commitDeletes = async (
	db: Firestore,
	refs: DocumentReference[],
): Promise<void> => {
	let i = 0;
	while (i < refs.length) {
		const chunk = refs.slice(i, i + maxDeleteBatch);
		const batch = writeBatch(db);
		for (const ref of chunk) {
			batch.delete(ref);
		}

		try {
			await batch.commit();
			i += chunk.length;
		} catch (error) {
			if (chunk.length === 1) {
				// Can't split a single delete any further - genuinely stuck.
				throw error;
			}
			// Too big: shrink the cap and retry this chunk (from the same offset).
			maxDeleteBatch = Math.max(1, Math.floor(chunk.length / 2));
		}
	}
};

export const deleteSyncRoom = async (code: string): Promise<void> => {
	const trimmed = code.trim();
	if (!trimmed) {
		return;
	}
	const db = await getDb();

	for (const sub of SUBCOLLECTIONS) {
		// Read a modest page, then delete it (adaptively split if too big). Because
		// we delete what we read, the next identical query returns the following
		// page, so no cursor is needed.
		// eslint-disable-next-line no-constant-condition
		while (true) {
			const snap = await getDocs(
				query(collection(db, "leagues", trimmed, sub), limit(100)),
			);
			if (snap.empty) {
				break;
			}
			await commitDeletes(
				db,
				snap.docs.map((d) => d.ref),
			);
			if (snap.size < 100) {
				break;
			}
		}
	}

	// Finally the registry doc.
	await deleteDoc(doc(db, "leagues", trimmed));
};

// Delete every listed room. Returns how many were deleted.
export const deleteAllSyncRooms = async (): Promise<number> => {
	const rooms = await listSyncRooms();
	for (const room of rooms) {
		await deleteSyncRoom(room.code);
	}
	return rooms.length;
};

// Trim a room's change log back to a retention window.
//
// The TTL policy only reaches entries that carry a `ttlAt`, and every entry
// written before that field existed has none - so a long-running room's real
// storage bill is history the policy will never touch. This is the one-time
// (or occasional) sweep that clears it, reusing the same adaptive batch
// deleter as room deletion because change docs are big enough to blow
// Firestore's ~10 MiB commit ceiling long before its 500-write one.
//
// Safe with respect to catch-up for the same reason retention is: entries this
// old have been applied by every device that is keeping up, and a device that
// ISN'T gets caught by the too-far-behind guard on connect rather than
// silently diverging.
export const pruneSyncRoomChanges = async (
	code: string,
	olderThanDays: number,
): Promise<number> => {
	const trimmed = code.trim();
	if (!trimmed || !(olderThanDays > 0)) {
		return 0;
	}
	const db = await getDb();
	const cutoff = Timestamp.fromMillis(
		Date.now() - olderThanDays * 24 * 60 * 60 * 1000,
	);

	let deleted = 0;
	// Read a page, delete it, repeat. Deleting what we read means the next
	// identical query returns the following page, so no cursor is needed. A
	// single-field inequality, so no composite index either.
	while (true) {
		const snap = await getDocs(
			query(
				collection(db, "leagues", trimmed, "changes"),
				where("ts", "<", cutoff),
				limit(100),
			),
		);
		if (snap.empty) {
			break;
		}
		await commitDeletes(
			db,
			snap.docs.map((d) => d.ref),
		);
		deleted += snap.size;
		if (snap.size < 100) {
			break;
		}
	}
	return deleted;
};

// Same sweep across every room. Returns how many change docs went.
export const pruneAllSyncRoomChanges = async (
	olderThanDays: number,
): Promise<number> => {
	const rooms = await listSyncRooms();
	let deleted = 0;
	for (const room of rooms) {
		deleted += await pruneSyncRoomChanges(room.code, olderThanDays);
	}
	return deleted;
};
