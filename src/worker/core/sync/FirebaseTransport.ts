import {
	getFirestore,
	collection,
	doc,
	addDoc,
	setDoc,
	getDoc,
	getDocs,
	onSnapshot,
	query,
	orderBy,
	where,
	Timestamp,
	serverTimestamp,
	type CollectionReference,
	type Firestore,
	type QueryDocumentSnapshot,
} from "firebase/firestore";
import { getFirebaseApp } from "./firebaseApp.ts";
import { deserializeChangeset, serializeChangeset } from "./serialize.ts";
import type { SyncedAutoPlay } from "../../../common/types.ts";
import type { SyncNotification } from "./notifications.ts";
import type {
	Authority,
	ChangesetEntry,
	SyncMember,
	SyncSubscriber,
	SyncTransport,
} from "./types.ts";

// A room has exactly one "wheel" doc recording who may advance the league.
const AUTHORITY_DOC_ID = "authority";

// Firestore-backed transport. Each shared league is a room keyed by its code;
// changes live in `leagues/{code}/changes`, ordered by server timestamp. The
// changeset is stored as a serialized string (see serialize.ts) to preserve
// Infinity/NaN and avoid Firestore's nested-array restrictions.
export class FirebaseTransport implements SyncTransport {
	readonly clientId: string;

	private db: Firestore;

	private code: string;

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
		this.code = code;
		this.db = getFirestore(getFirebaseApp());
		this.changesRef = collection(this.db, "leagues", code, "changes");
	}

	// Publish the simmer's auto-play schedule so every device can show the same
	// schedule + countdown. We ride it on the authority ("who's simming") doc,
	// which every device already reads reliably - only the simmer can auto-play,
	// and it already holds this doc, so the merge write passes the same rule that
	// governs the wheel (holderId stays == our uid). This avoids depending on a
	// separate read rule for the registry doc.
	async publishAutoPlay(state: SyncedAutoPlay) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			{ autoPlay: state },
			{ merge: true },
		);
	}

	// Watch the room's auto-play schedule (carried on the authority doc). Fires
	// with the current value, then on every change. Undefined when nobody is
	// auto-playing. An error handler surfaces permission problems instead of
	// silently delivering nothing.
	subscribeAutoPlay(onChange: (state: SyncedAutoPlay | undefined) => void) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			(snapshot) => {
				const data = snapshot.data();
				onChange((data?.autoPlay as SyncedAutoPlay | undefined) ?? undefined);
			},
			(error) => {
				console.error("Auto-play schedule subscription failed", error);
			},
		);
	}

	// Read the room's registry doc, if any. `leagueId` is the fingerprint of the
	// league file this room belongs to (see connect.ts). Undefined if the room
	// has no registry doc yet.
	async getRoomInfo(): Promise<{ leagueId?: string } | undefined> {
		const snap = await getDoc(doc(this.db, "leagues", this.code));
		if (!snap.exists()) {
			return undefined;
		}
		const data = snap.data();
		return {
			leagueId: typeof data.leagueId === "string" ? data.leagueId : undefined,
		};
	}

	// Upsert the room's registry doc (leagues/{code}) so the admin page can list
	// every code that's been used, and stamp the league fingerprint so a device
	// can refuse to connect a different league to this room. Rooms are otherwise
	// created implicitly by writing to subcollections, leaving no listable parent.
	async touchRoom(leagueId?: string) {
		await setDoc(
			doc(this.db, "leagues", this.code),
			{
				code: this.code,
				updatedAt: serverTimestamp(),
				...(leagueId ? { leagueId } : {}),
			},
			{ merge: true },
		);
	}

	// Record (or refresh) this device's push registration in the room, so the
	// Cloud Function knows where to send notifications. Keyed by uid, so each
	// device has exactly one entry that updates in place.
	async registerMember(uid: string, member: SyncMember) {
		await setDoc(
			doc(this.db, "leagues", this.code, "members", uid),
			{ ...member, updatedAt: serverTimestamp() },
			{ merge: true },
		);
	}

	// Enqueue a push. The Cloud Function triggers on this doc, looks up member
	// tokens, and delivers to everyone else's phones - so it works even when
	// their app is fully closed.
	async publishNotification(
		notification: SyncNotification & { authorId: string; authorName: string },
	) {
		await addDoc(collection(this.db, "leagues", this.code, "notifications"), {
			title: notification.title,
			body: notification.body,
			authorId: notification.authorId,
			authorName: notification.authorName,
			// Firestore rejects `undefined`; null means "everyone in the room".
			targetTids: notification.targetTids ?? null,
			// League-relative deep-link path ("" = app root), resolved to the
			// recipient's own lid on their device.
			path: notification.path ?? "",
			ts: serverTimestamp(),
		});
	}

	// Claim the wheel: become the sole device allowed to advance the league. The
	// security rules only permit writing holderId === your own uid, so you can
	// only ever take the wheel for yourself, never assign it to someone else.
	async claimAuthority(holderId: string, holderName: string) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			{ holderId, holderName, updatedAt: serverTimestamp() },
		);
	}

	// Watch who currently holds the wheel. Fires immediately with the current
	// holder (or undefined if nobody has claimed it yet), then on every change.
	subscribeAuthority(onChange: (authority: Authority | undefined) => void) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			(snapshot) => {
				const data = snapshot.data();
				if (data && typeof data.holderId === "string") {
					onChange({
						holderId: data.holderId,
						holderName:
							typeof data.holderName === "string" ? data.holderName : "Someone",
					});
				} else {
					onChange(undefined);
				}
			},
		);
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

	// Parse a stored change doc into a ChangesetEntry, or undefined if it has no
	// server timestamp yet (a pending local write not yet confirmed).
	private parseEntry(
		docSnap: QueryDocumentSnapshot,
	): ChangesetEntry | undefined {
		const data = docSnap.data();
		if (!data.ts) {
			return undefined;
		}
		return {
			id: data.id,
			authorId: data.authorId,
			action: data.action,
			seq: typeof data.ts.toMillis === "function" ? data.ts.toMillis() : 0,
			changeset: deserializeChangeset(data.changeset),
			batchId: data.batchId,
			chunkIndex: data.chunkIndex,
			chunkCount: data.chunkCount,
		};
	}

	// One-shot read of the whole log, oldest-first, for the activity panel and
	// full-resync recovery.
	async fetchAllEntries(): Promise<ChangesetEntry[]> {
		const snapshot = await getDocs(query(this.changesRef, orderBy("ts")));
		const entries: ChangesetEntry[] = [];
		for (const docSnap of snapshot.docs) {
			const entry = this.parseEntry(docSnap);
			if (entry) {
				entries.push(entry);
			}
		}
		return entries;
	}

	// One-shot read of just the entries after a given server-timestamp - a cheap
	// targeted catch-up that doesn't wait on Firestore's (phone-throttled)
	// real-time push.
	async fetchEntriesSince(sinceMs: number): Promise<ChangesetEntry[]> {
		const snapshot = await getDocs(
			query(
				this.changesRef,
				where("ts", ">", Timestamp.fromMillis(sinceMs)),
				orderBy("ts"),
			),
		);
		const entries: ChangesetEntry[] = [];
		for (const docSnap of snapshot.docs) {
			const entry = this.parseEntry(docSnap);
			if (entry) {
				entries.push(entry);
			}
		}
		return entries;
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
				const entry = this.parseEntry(change.doc);
				if (entry) {
					entries.push(entry);
				}
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
