import { initializeApp, getApps } from "firebase/app";
import { getAuth, signInAnonymously } from "firebase/auth";
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
import { firebaseConfig } from "../../common/firebaseConfig.ts";

// Admin tools for the shared-league rooms in Firestore: list every code that's
// been used and delete rooms (all their changes/control/members/notifications
// docs plus the registry doc). Runs in the UI thread.
//
// SECURITY NOTE: the password gate in the UI is cosmetic. The real boundary is
// the Firestore rules, which allow any signed-in (anonymous) user to delete.
// That's acceptable for a tiny private league with obscure codes; it is NOT a
// hardened admin surface.

export type SyncRoom = {
	code: string;
	updatedAt?: number;
};

const getApp = () =>
	getApps().length > 0 ? getApps()[0]! : initializeApp(firebaseConfig);

const getDb = async () => {
	const app = getApp();
	// Firestore rules require an authenticated request; sign in anonymously if we
	// haven't already (this UI-thread app is separate from the worker's).
	const auth = getAuth(app);
	if (!auth.currentUser) {
		await signInAnonymously(auth);
	}
	return getFirestore(app);
};

// The subcollections a room accumulates. Deleting a room clears all of these,
// then the registry doc itself.
const SUBCOLLECTIONS = ["changes", "control", "members", "notifications"];

export const listRooms = async (): Promise<SyncRoom[]> => {
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

export const deleteRoom = async (code: string): Promise<void> => {
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
export const deleteAllRooms = async (): Promise<number> => {
	const rooms = await listRooms();
	for (const room of rooms) {
		await deleteRoom(room.code);
	}
	return rooms.length;
};
