import {
	collection,
	deleteDoc,
	doc,
	getDocs,
	getFirestore,
	limit,
	query,
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

export const deleteSyncRoom = async (code: string): Promise<void> => {
	const trimmed = code.trim();
	if (!trimmed) {
		return;
	}
	const db = await getDb();

	for (const sub of SUBCOLLECTIONS) {
		// Delete in batches (Firestore caps a batch at 500 writes).
		// eslint-disable-next-line no-constant-condition
		while (true) {
			const snap = await getDocs(
				query(collection(db, "leagues", trimmed, sub), limit(400)),
			);
			if (snap.empty) {
				break;
			}
			const batch = writeBatch(db);
			for (const d of snap.docs) {
				batch.delete(d.ref);
			}
			await batch.commit();
			if (snap.size < 400) {
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
