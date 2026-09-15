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
	writeBatch,
} from "firebase/firestore";
import { getFirebaseApp } from "./firebaseApp.ts";
import { ensureAnonymousAuth } from "./auth.ts";

// Clearing a league's cloud data: every document the room accumulated, then the
// room itself. Runs in the WORKER, which already has a working, authenticated
// Firebase - doing it in the UI thread meant a second Firebase app on the same
// origin, which fought the worker's over IndexedDB and threw
// NoModificationAllowedError.
//
// This takes the room code and deletes THAT room. There is deliberately no
// "list every room" and no "delete them all": those existed for a hidden admin
// panel, they were the only reason the rules had to allow querying the room
// registry, and allowing that query let any signed-in stranger enumerate every
// league in the project. See the note at the top of firestore.rules.

const getDb = async () => {
	await ensureAnonymousAuth();
	return getFirestore(getFirebaseApp());
};

// The subcollections a room accumulates. Deleting a room clears all of these,
// then the registry doc itself.
const SUBCOLLECTIONS = ["changes", "control", "members", "notifications"];

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
