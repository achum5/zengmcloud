import {
	getFirestore,
	collection,
	addDoc,
	onSnapshot,
	query,
	orderBy,
	where,
	Timestamp,
	serverTimestamp,
	type CollectionReference,
} from "firebase/firestore";
import { getFirebaseApp } from "./firebaseApp.ts";
import { deserializeChangeset, serializeChangeset } from "./serialize.ts";
import type { ChangesetEntry, SyncSubscriber, SyncTransport } from "./types.ts";

// Firestore-backed transport. Each shared league is a room keyed by its code;
// changes live in `leagues/{code}/changes`, ordered by server timestamp. The
// changeset is stored as a serialized string (see serialize.ts) to preserve
// Infinity/NaN and avoid Firestore's nested-array restrictions.
export class FirebaseTransport implements SyncTransport {
	readonly clientId: string;

	private changesRef: CollectionReference;

	// Server-timestamp watermark: we only fetch changes newer than this, so a
	// reconnecting device catches up on exactly what it missed.
	private sinceTs: number;

	constructor(
		code: string,
		clientId: string,
		options: { sinceTs?: number } = {},
	) {
		this.clientId = clientId;
		this.sinceTs = options.sinceTs ?? 0;
		const db = getFirestore(getFirebaseApp());
		this.changesRef = collection(db, "leagues", code, "changes");
	}

	async publish(entry: Omit<ChangesetEntry, "seq">) {
		await addDoc(this.changesRef, {
			id: entry.id,
			authorId: entry.authorId,
			action: entry.action,
			changeset: serializeChangeset(entry.changeset),
			// Chunk metadata (present only for bulk changes). Firestore rejects
			// undefined, so only include when set.
			...(entry.batchId !== undefined
				? {
						batchId: entry.batchId,
						chunkIndex: entry.chunkIndex,
						chunkCount: entry.chunkCount,
					}
				: {}),
			ts: serverTimestamp(),
		});
	}

	subscribe(subscriber: SyncSubscriber) {
		// Only entries after our watermark - the initial snapshot is the catch-up
		// (everything we missed), and later snapshots are live updates. Pending
		// local writes have a null ts and simply don't match `ts > x` until the
		// server confirms them.
		const q = query(
			this.changesRef,
			where("ts", ">", Timestamp.fromMillis(this.sinceTs)),
			orderBy("ts"),
		);

		// Process snapshots one at a time, in order, since applying is async.
		let chain: Promise<void> = Promise.resolve();

		const unsub = onSnapshot(q, (snapshot) => {
			const entries: ChangesetEntry[] = [];
			for (const change of snapshot.docChanges()) {
				if (change.type !== "added") {
					continue;
				}
				const data = change.doc.data();
				if (!data.ts) {
					continue;
				}
				entries.push({
					id: data.id,
					authorId: data.authorId,
					action: data.action,
					seq: typeof data.ts.toMillis === "function" ? data.ts.toMillis() : 0,
					changeset: deserializeChangeset(data.changeset),
					batchId: data.batchId,
					chunkIndex: data.chunkIndex,
					chunkCount: data.chunkCount,
				});
			}

			if (entries.length === 0) {
				return;
			}

			chain = chain.then(async () => {
				for (const entry of entries) {
					await subscriber.onEntry(entry);
				}
				subscriber.onBatchProcessed?.();
			});
		});

		return unsub;
	}
}
