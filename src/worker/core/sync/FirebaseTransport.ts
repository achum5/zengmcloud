import {
	getFirestore,
	collection,
	doc,
	addDoc,
	setDoc,
	getDoc,
	getDocFromServer,
	getCountFromServer,
	getDocs,
	deleteDoc,
	limit,
	onSnapshot,
	query,
	orderBy,
	runTransaction,
	where,
	Timestamp,
	serverTimestamp,
	type CollectionReference,
	type Firestore,
	type QueryDocumentSnapshot,
} from "firebase/firestore";
import { getFirebaseApp } from "./firebaseApp.ts";
import {
	decideAdvanceClaim,
	type AdvanceClaimDoc,
} from "./advanceClaimPolicy.ts";
import { decideSimDayClaim, type SimDayClaimDoc } from "./simDayClaimPolicy.ts";
import { syncDebugLog } from "./debugLog.ts";
import { deserializeChangeset, serializeChangeset } from "./serialize.ts";
import type { SyncedAutoPlay } from "../../../common/types.ts";
import type { SyncNotification } from "./notifications.ts";
import type {
	Authority,
	ChangesetEntry,
	DraftReadyEntry,
	FaBoardEntry,
	LiveBroadcastMeta,
	LiveBroadcastUpdate,
	LotteryRevealMeta,
	LotteryRevealUpdate,
	SyncMember,
	SyncSubscriber,
	SyncTransport,
} from "./types.ts";

// A room has exactly one "sim authority" doc recording who may advance the league.
const AUTHORITY_DOC_ID = "authority";

// The live-sim broadcast cursor/meta doc, and the prefix for its payload chunks
// (control/liveBroadcastData0, ...1, ...). All under control/, so the generic
// control-doc security rule (write requires holderId == auth.uid) already covers
// them - no rules change needed.
const LIVE_BROADCAST_DOC_ID = "liveBroadcast";
const LIVE_BROADCAST_DATA_PREFIX = "liveBroadcastData";

// Draft ready-up docs, also under control/ so the same holderId rule covers
// them. draftReady holds everyone's ready entries (each device merges only its
// own, keyed by uid - Firestore merge writes are per-field, so concurrent
// merges by different devices never clobber each other); draftAdvance is the
// atomic claim for who sims the next pick.
const DRAFT_READY_DOC_ID = "draftReady";
const FA_BOARD_DOC_ID = "faBoard";
const DRAFT_ADVANCE_DOC_ID = "draftAdvance";

// The per-season fence over schedule-day sims: which day (and which of its
// games) has been claimed, ever. See simDayClaimPolicy.ts for why day sims
// need a server-side fence in addition to the advisory authority doc.
const SIM_DAY_DOC_ID = "simDay";

// The live lottery-reveal cursor doc: whoever runs the lottery heartbeats how
// many picks they've revealed, and every other device replays the reveal in
// lockstep. The RESULT travels through the normal change log; this doc only
// carries the reveal position.
const LOTTERY_REVEAL_DOC_ID = "lotteryReveal";

// Keep each payload chunk well under Firestore's 1 MB/doc limit.
const LIVE_BROADCAST_CHUNK_BYTES = 700_000;

// If we've had confirmed contact with Firestore within this window, treat the
// connection as live without a round-trip; otherwise verifyConnection() probes.
// Sized to comfortably cover the 15s background catch-up poll (which does a
// real server read and refreshes contact), so a healthy connection answers
// non-forced checks instantly - otherwise every interactive worker call in an
// idle room would pay a blocking probe.
const CONNECTION_FRESH_MS = 20_000;
// A publish attempt that the server hasn't acked within this long counts as
// failed. Critical: while offline, Firestore's setDoc does NOT reject - it
// buffers the write and resolves only on server ack - so an untimed publish
// hangs forever and its retry loop never even gets to retry. If the buffered
// write does land later, the retry overwrites the same doc id, so no duplicate.
const PUBLISH_ACK_TIMEOUT_MS = 12_000;
// A liveness probe that doesn't answer within this long counts as "not connected"
// (a silently-dropped listener won't error, it just never responds).
const CONNECTION_PROBE_TIMEOUT_MS = 6000;

const withTimeout = <T>(promise: Promise<T>, ms: number): Promise<T> =>
	new Promise((resolve, reject) => {
		const id = setTimeout(() => reject(new Error("timeout")), ms);
		promise.then(
			(value) => {
				clearTimeout(id);
				resolve(value);
			},
			(error) => {
				clearTimeout(id);
				reject(error);
			},
		);
	});

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

	// When we last confirmed a live round-trip to Firestore (any successful read,
	// write, or real-time delivery). Powers verifyConnection().
	private lastContactAt = Date.now();

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

	private markContact() {
		this.lastContactAt = Date.now();
	}

	// When we last confirmed live contact (ms epoch). Powers the header status dot.
	getLastContactAt(): number {
		return this.lastContactAt;
	}

	// Is the cloud connection ACTUALLY live right now? Cheap when we've had recent
	// confirmed contact; otherwise does a real, timed round-trip. This is what lets
	// the sim/advance guard refuse to advance when the app only *looks* connected
	// (a silently-dropped listener, an expired token, a resumed-from-suspend tab) -
	// which would otherwise advance locally and never reach the shared log.
	//
	// `force` skips the recent-contact shortcut and ALWAYS does the real round-trip.
	// The sim/advance guard passes force, because "we heard from the server 5s ago"
	// is not proof the connection is live NOW - a socket can die silently between a
	// snapshot and the sim, and a sim that then fails to upload strands every other
	// device. For a high-stakes, room-forking advance, the round-trip is worth it.
	async verifyConnection(force = false): Promise<boolean> {
		if (!force && Date.now() - this.lastContactAt < CONNECTION_FRESH_MS) {
			return true;
		}
		try {
			// getDocFromServer, not getDoc: a plain getDoc can be answered from
			// Firestore's offline cache and falsely report "connected". We want a
			// real server round-trip.
			await withTimeout(
				getDocFromServer(doc(this.db, "leagues", this.code)),
				CONNECTION_PROBE_TIMEOUT_MS,
			);
			this.markContact();
			return true;
		} catch {
			return false;
		}
	}

	// Publish the simmer's auto-play schedule so every device can show the same
	// schedule + countdown. We ride it on the authority ("who's simming") doc,
	// which every device already reads reliably - only the simmer can auto-play,
	// and it already holds this doc, so the merge write passes the same rule that
	// governs sim authority (holderId stays == our uid). This avoids depending on a
	// separate read rule for the registry doc.
	async publishAutoPlay(state: SyncedAutoPlay) {
		// Firestore rejects ANY write containing `undefined`, and the "off"/paused
		// snapshots carry nextRunAt: undefined - so they silently failed and
		// followers kept a stale countdown forever. Store null instead.
		await setDoc(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			{
				autoPlay: {
					enabled: state.enabled,
					nextRunAt: state.nextRunAt ?? null,
					rules: state.rules,
				},
			},
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
				const raw = data?.autoPlay as any;
				// nextRunAt is stored as null when paused/off (Firestore can't store
				// undefined); normalize back for the UI.
				onChange(
					raw
						? {
								enabled: !!raw.enabled,
								nextRunAt: raw.nextRunAt ?? undefined,
								rules: Array.isArray(raw.rules) ? raw.rules : [],
							}
						: undefined,
				);
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
	// device has exactly one entry that updates in place; a partial member
	// merges onto the stored one (e.g. refreshing just the tid).
	async registerMember(uid: string, member: Partial<SyncMember>) {
		await setDoc(
			doc(this.db, "leagues", this.code, "members", uid),
			{ ...member, updatedAt: serverTimestamp() },
			{ merge: true },
		);
	}

	async ping() {
		await setDoc(
			doc(this.db, "leagues", this.code, "members", this.clientId),
			{ lastPingAt: serverTimestamp() },
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

	// Claim sim authority: become the sole device allowed to advance the league. The
	// security rules only permit writing holderId === your own uid, so you can
	// only ever sim here for yourself, never assign it to someone else.
	async claimAuthority(holderId: string, holderName: string) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			{ holderId, holderName, updatedAt: serverTimestamp() },
		);
	}

	// Stamp (or clear, with 0) the "actively advancing" lease. Merges onto the
	// authority doc the holder already owns, so it passes the same rule as the
	// sim authority itself. Followers read this to know a sim is in flight (see
	// Authority.busyUntil).
	async publishBusy(busyUntil: number) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			{ busyUntil },
			{ merge: true },
		);
	}

	// Merge THIS device's draft ready entry onto the shared ready doc (null
	// clears it). Firestore merges are per-field, so devices writing their own
	// uid-keyed entries never clobber each other; holderId is stamped to our own
	// uid on every write so the control-doc rule passes.
	async publishDraftReady(entry: DraftReadyEntry | null, clearUids?: string[]) {
		// Readiness is per TEAM, so a device may also clear the entries of its
		// OWN team's other devices (clearUids) - otherwise "Not ready" on one
		// device couldn't revoke a ready published from another. The control-doc
		// rule only requires holderId to be our own uid.
		const ready: Record<string, DraftReadyEntry | null> = {
			[this.clientId]: entry,
		};
		for (const uid of clearUids ?? []) {
			ready[uid] = null;
		}
		await setDoc(
			doc(this.db, "leagues", this.code, "control", DRAFT_READY_DOC_ID),
			{
				holderId: this.clientId,
				ready,
				updatedAt: serverTimestamp(),
			},
			{ merge: true },
		);
		this.markContact();
	}

	// Merge THIS device's free-agency board entry onto the shared board doc
	// (null clears it). Same per-uid merge semantics as publishDraftReady.
	async publishFaBoard(entry: FaBoardEntry | null) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", FA_BOARD_DOC_ID),
			{
				holderId: this.clientId,
				boards: { [this.clientId]: entry },
				updatedAt: serverTimestamp(),
			},
			{ merge: true },
		);
		this.markContact();
	}

	// Watch everyone's free-agency board entries. Fires with the current map,
	// then on every change.
	subscribeFaBoard(
		onChange: (boards: Record<string, FaBoardEntry | null> | undefined) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", FA_BOARD_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				onChange(
					data && typeof data.boards === "object" && data.boards !== null
						? (data.boards as Record<string, FaBoardEntry | null>)
						: undefined,
				);
			},
			(error) => {
				console.error("FA board subscription failed", error);
			},
		);
	}

	// Watch everyone's draft ready entries. Fires with the current map, then on
	// every change.
	subscribeDraftReady(
		onChange: (
			ready: Record<string, DraftReadyEntry | null> | undefined,
		) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", DRAFT_READY_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				onChange(
					data && typeof data.ready === "object" && data.ready !== null
						? (data.ready as Record<string, DraftReadyEntry | null>)
						: undefined,
				);
			},
			(error) => {
				console.error("Draft ready subscription failed", error);
			},
		);
	}

	// Merge the lottery-reveal cursor doc (see LOTTERY_REVEAL_DOC_ID). holderId
	// is stamped so the control-doc rule passes.
	async publishLotteryReveal(update: LotteryRevealUpdate) {
		const clean: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(update)) {
			if (value !== undefined) {
				clean[key] = value;
			}
		}
		await setDoc(
			doc(this.db, "leagues", this.code, "control", LOTTERY_REVEAL_DOC_ID),
			{ ...clean, holderId: this.clientId, updatedAt: serverTimestamp() },
			{ merge: true },
		);
		this.markContact();
	}

	// Watch the lottery-reveal cursor. Fires with the current value, then on
	// every heartbeat. Undefined when no reveal is being broadcast.
	subscribeLotteryReveal(
		onChange: (meta: LotteryRevealMeta | undefined) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", LOTTERY_REVEAL_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				if (
					data &&
					data.active &&
					typeof data.holderId === "string" &&
					typeof data.season === "number"
				) {
					onChange({
						holderId: data.holderId,
						active: true,
						season: data.season,
						revealed: typeof data.revealed === "number" ? data.revealed : -1,
						byName: typeof data.byName === "string" ? data.byName : "Someone",
						startedAt: typeof data.startedAt === "number" ? data.startedAt : 0,
						expiresAt: typeof data.expiresAt === "number" ? data.expiresAt : 0,
					});
				} else {
					onChange(undefined);
				}
			},
			(error) => {
				console.error("Lottery reveal subscription failed", error);
			},
		);
	}

	// Atomically claim the right to sim one specific step. The transaction
	// applies decideAdvanceClaim: exactly one device per (draftKey, step) wins
	// within the lease window, AND a step at or below the stage's high-water mark
	// can never be claimed again (except crash recovery of the newest,
	// uncompleted step) - so a stale rejoining device can't re-sim finished
	// steps and publish regressed state as new history.
	async claimDraftAdvance(
		draftKey: string,
		pick: number,
		leaseMs: number,
	): Promise<boolean> {
		const ref = doc(
			this.db,
			"leagues",
			this.code,
			"control",
			DRAFT_ADVANCE_DOC_ID,
		);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const data = snap.data() as AdvanceClaimDoc | undefined;
				const decision = decideAdvanceClaim(data, {
					draftKey,
					pick,
					now: Date.now(),
					leaseMs,
				});
				if (!decision.grant) {
					throw new Error(`Advance claim rejected: ${decision.reason}`);
				}
				tx.set(ref, {
					holderId: this.clientId,
					draftKey,
					pick,
					at: Date.now(),
					maxPick: decision.maxPick,
					completed: false,
				});
			});
			this.markContact();
			return true;
		} catch {
			return false;
		}
	}

	// Mark this device's claimed step as finished, closing its crash-recovery
	// re-claim window. Best-effort: a failure just leaves the lease to expire.
	async completeDraftAdvance(draftKey: string, pick: number): Promise<void> {
		const ref = doc(
			this.db,
			"leagues",
			this.code,
			"control",
			DRAFT_ADVANCE_DOC_ID,
		);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const data = snap.data() as AdvanceClaimDoc | undefined;
				if (
					data &&
					data.draftKey === draftKey &&
					data.pick === pick &&
					data.holderId === this.clientId
				) {
					tx.set(ref, { ...data, completed: true });
				}
			});
			this.markContact();
		} catch {
			// Lease expiry covers it.
		}
	}

	// Atomically claim the right to sim one slice of a schedule day (the whole
	// day, or a single live-simmed game). The transaction applies
	// decideSimDayClaim: a day below the season's high-water mark can never be
	// claimed again, and within the newest day an already-claimed game can never
	// be claimed again (except crash recovery of an uncompleted claim). This is
	// what makes a concurrent double-sim of the same day - which doubles every
	// read-modify-write aggregate while the game records collide by gid -
	// impossible, no matter what the advisory authority doc says.
	async claimSimDay(
		stageKey: string,
		day: number,
		gids: number[],
		leaseMs: number,
	): Promise<boolean> {
		const ref = doc(this.db, "leagues", this.code, "control", SIM_DAY_DOC_ID);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const data = snap.data() as SimDayClaimDoc | undefined;
				const decision = decideSimDayClaim(data, {
					stageKey,
					day,
					gids,
					now: Date.now(),
					leaseMs,
				});
				if (!decision.grant) {
					throw new Error(`Sim day claim rejected: ${decision.reason}`);
				}
				tx.set(ref, {
					holderId: this.clientId,
					stageKey,
					day: decision.day,
					gids: decision.gids,
					at: Date.now(),
					maxDay: decision.maxDay,
					completed: false,
				});
			});
			this.markContact();
			return true;
		} catch {
			return false;
		}
	}

	// Mark this device's claimed day-slice as finished, closing its
	// crash-recovery re-claim window. Best-effort: a failure just leaves the
	// lease to expire.
	async completeSimDay(stageKey: string, day: number): Promise<void> {
		const ref = doc(this.db, "leagues", this.code, "control", SIM_DAY_DOC_ID);
		try {
			await runTransaction(this.db, async (tx) => {
				const snap = await tx.get(ref);
				const data = snap.data() as SimDayClaimDoc | undefined;
				if (
					data &&
					data.stageKey === stageKey &&
					data.day === day &&
					data.holderId === this.clientId
				) {
					tx.set(ref, { ...data, completed: true });
				}
			});
			this.markContact();
		} catch {
			// Lease expiry covers it.
		}
	}

	// Watch who currently is in charge of simming. Fires immediately with the current
	// holder (or undefined if nobody has claimed it yet), then on every change.
	subscribeAuthority(
		onChange: (authority: Authority | undefined) => void,
		onError?: (error: unknown) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", AUTHORITY_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				if (data && typeof data.holderId === "string") {
					onChange({
						holderId: data.holderId,
						holderName:
							typeof data.holderName === "string" ? data.holderName : "Someone",
						busyUntil:
							typeof data.busyUntil === "number" ? data.busyUntil : undefined,
					});
				} else {
					onChange(undefined);
				}
			},
			onError,
		);
	}

	// Merge the live-sim broadcast cursor/meta doc. Always stamps holderId (== our
	// uid) so the control-doc rule passes, and strips undefined (Firestore rejects
	// it). Called once to open the broadcast and then rapidly to heartbeat the
	// cursor, so the payload is kept in SEPARATE docs (below) - this doc stays tiny.
	async publishLiveBroadcast(update: LiveBroadcastUpdate) {
		const clean: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(update)) {
			if (value !== undefined) {
				clean[key] = value;
			}
		}
		await setDoc(
			doc(this.db, "leagues", this.code, "control", LIVE_BROADCAST_DOC_ID),
			{ ...clean, holderId: this.clientId, updatedAt: serverTimestamp() },
			{ merge: true },
		);
		this.markContact();
	}

	// Write the immutable play-by-play payload (a serialized string) as one or
	// more docs, each under Firestore's size limit. Written BEFORE the meta doc
	// flips active, so a follower reacting to active:true can always read a
	// complete payload. Returns the number of chunks (recorded in the meta doc).
	async publishLiveBroadcastData(gid: number, serialized: string) {
		const chunks: string[] = [];
		for (let i = 0; i < serialized.length; i += LIVE_BROADCAST_CHUNK_BYTES) {
			chunks.push(serialized.slice(i, i + LIVE_BROADCAST_CHUNK_BYTES));
		}
		if (chunks.length === 0) {
			chunks.push("");
		}
		for (let i = 0; i < chunks.length; i++) {
			await setDoc(
				doc(
					this.db,
					"leagues",
					this.code,
					"control",
					`${LIVE_BROADCAST_DATA_PREFIX}${i}`,
				),
				{
					holderId: this.clientId,
					gid,
					data: chunks[i],
					updatedAt: serverTimestamp(),
				},
			);
		}
		this.markContact();
		return chunks.length;
	}

	// Watch the live-sim broadcast cursor/meta doc. Fires with the current value,
	// then on every heartbeat. Undefined when no broadcast is active.
	subscribeLiveBroadcast(
		onChange: (meta: LiveBroadcastMeta | undefined) => void,
	) {
		return onSnapshot(
			doc(this.db, "leagues", this.code, "control", LIVE_BROADCAST_DOC_ID),
			(snapshot) => {
				this.markContact();
				const data = snapshot.data();
				if (
					data &&
					data.active &&
					typeof data.holderId === "string" &&
					typeof data.gid === "number"
				) {
					onChange({
						holderId: data.holderId,
						active: true,
						gid: data.gid,
						byName: typeof data.byName === "string" ? data.byName : "Someone",
						cursor: typeof data.cursor === "number" ? data.cursor : 0,
						paused: !!data.paused,
						speed: typeof data.speed === "number" ? data.speed : 7,
						gameOver: !!data.gameOver,
						startedAt: typeof data.startedAt === "number" ? data.startedAt : 0,
						chunkCount:
							typeof data.chunkCount === "number" ? data.chunkCount : 0,
						expiresAt: typeof data.expiresAt === "number" ? data.expiresAt : 0,
					});
				} else {
					onChange(undefined);
				}
			},
			(error) => {
				console.error("Live broadcast subscription failed", error);
			},
		);
	}

	// Reassemble the play-by-play payload from its chunk docs, in order. Returns
	// undefined if any chunk is missing (a broadcast that was cleared, or a
	// partial write) so the caller can bail instead of playing a corrupt payload.
	async fetchLiveBroadcastData(chunkCount: number) {
		let out = "";
		for (let i = 0; i < chunkCount; i++) {
			const snap = await getDoc(
				doc(
					this.db,
					"leagues",
					this.code,
					"control",
					`${LIVE_BROADCAST_DATA_PREFIX}${i}`,
				),
			);
			if (!snap.exists()) {
				return undefined;
			}
			out += (snap.data().data as string | undefined) ?? "";
		}
		this.markContact();
		return out;
	}

	// End the broadcast: flip the meta doc inactive (so followers unlock now) and
	// delete the payload chunks. Best-effort on the deletes - the lease would
	// expire them anyway if a delete fails.
	async clearLiveBroadcast(chunkCount: number) {
		await setDoc(
			doc(this.db, "leagues", this.code, "control", LIVE_BROADCAST_DOC_ID),
			{ holderId: this.clientId, active: false, updatedAt: serverTimestamp() },
			{ merge: true },
		);
		for (let i = 0; i < chunkCount; i++) {
			try {
				await deleteDoc(
					doc(
						this.db,
						"leagues",
						this.code,
						"control",
						`${LIVE_BROADCAST_DATA_PREFIX}${i}`,
					),
				);
			} catch {
				// Best-effort; the lease handles a leftover payload.
			}
		}
		this.markContact();
	}

	async publish(entry: Omit<ChangesetEntry, "seq">) {
		const payload = {
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
			// String-part payload + display metadata (new-format bulk chunks).
			...(entry.payloadPart !== undefined
				? { payloadPart: entry.payloadPart }
				: {}),
			...(entry.records !== undefined ? { records: entry.records } : {}),
			...(entry.attrs !== undefined ? { attrs: entry.attrs } : {}),
			ts: serverTimestamp(),
		};

		// Retry transient failures (a network blip, a brief Firestore hiccup). A
		// dropped publish is unrecoverable - the change tracker was already drained,
		// so catch-up/resync can't replay a change that never reached the log.
		// A permanent failure exhausts the retries and throws, which leaves the
		// entry queued in the outbox for a later drain.
		const ref = doc(this.changesRef, entry.id);

		// Did a PREVIOUS attempt's buffered write actually land? The change log is
		// append-only (rules: update = false), so once the doc exists, every
		// re-send is rejected with permission-denied forever - even though OUR
		// content is already there. Without this check, one ack timeout whose
		// write later landed permanently wedged the FIFO upload queue behind an
		// "unpublishable" entry. Entry ids are uuids only we generated, so an
		// existing doc with our authorId IS this publish, delivered.
		const alreadyDelivered = async (): Promise<boolean> => {
			try {
				const snap = await withTimeout(
					getDocFromServer(ref),
					PUBLISH_ACK_TIMEOUT_MS,
				);
				return snap.exists() && snap.data()?.authorId === this.clientId;
			} catch {
				return false;
			}
		};

		let lastError: unknown;
		for (let attempt = 0; attempt < 3; attempt++) {
			try {
				await withTimeout(setDoc(ref, payload), PUBLISH_ACK_TIMEOUT_MS);
				this.markContact();
				return;
			} catch (error) {
				lastError = error;
				if (await alreadyDelivered()) {
					this.markContact();
					return;
				}
				await new Promise((resolve) => setTimeout(resolve, 300 * 2 ** attempt));
			}
		}
		throw lastError;
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
			payloadPart:
				typeof data.payloadPart === "string" ? data.payloadPart : undefined,
			records: typeof data.records === "number" ? data.records : undefined,
			attrs: Array.isArray(data.attrs) ? data.attrs : undefined,
		};
	}

	// One-shot read of the whole log, oldest-first, for the activity panel and
	// full-resync recovery.
	async fetchAllEntries(): Promise<ChangesetEntry[]> {
		const snapshot = await getDocs(query(this.changesRef, orderBy("ts")));
		this.markContact();
		const entries: ChangesetEntry[] = [];
		for (const docSnap of snapshot.docs) {
			const entry = this.parseEntry(docSnap);
			if (entry) {
				entries.push(entry);
			}
		}
		return entries;
	}

	// Read the entries after a given server-timestamp, oldest-first. With
	// `pageLimit` this returns just ONE bounded page (the oldest that many),
	// letting the engine drain a huge backlog page by page instead of pulling the
	// whole thing in one query - which on a phone that's been away for weeks would
	// time out / run out of memory and never complete. Without a limit it reads
	// everything after `sinceMs` (used where the set is known to be small).
	async fetchEntriesSince(
		sinceMs: number,
		pageLimit?: number,
	): Promise<ChangesetEntry[]> {
		const constraints = [
			where("ts", ">", Timestamp.fromMillis(sinceMs)),
			orderBy("ts"),
			...(pageLimit !== undefined ? [limit(pageLimit)] : []),
		];
		const snapshot = await getDocs(query(this.changesRef, ...constraints));
		this.markContact();
		const entries: ChangesetEntry[] = [];
		for (const docSnap of snapshot.docs) {
			const entry = this.parseEntry(docSnap);
			if (entry) {
				entries.push(entry);
			}
		}
		return entries;
	}

	// Move the watermark the live subscription starts from. Called after the
	// paginated backlog drain so the real-time listener's INITIAL snapshot is
	// small (just the live tail), instead of re-loading the whole backlog we just
	// drained.
	updateSince(ts: number) {
		this.sinceTs = ts;
	}

	// How many entries are still after our watermark, via a cheap server-side
	// aggregate count (no docs read). Powers the "catching up …%" progress total
	// so a returning device can show how far it has to go.
	async countEntriesSince(sinceMs: number): Promise<number> {
		const snap = await getCountFromServer(
			query(this.changesRef, where("ts", ">", Timestamp.fromMillis(sinceMs))),
		);
		this.markContact();
		return snap.data().count;
	}

	// The most recent `n` entries (returned oldest-first, like fetchAllEntries).
	// The sync-activity panel only shows recent activity, so it must never read the
	// whole (possibly enormous) log just to render a list - that's what left it
	// stuck on "Loading…" for a device far behind.
	async fetchRecentEntries(n: number): Promise<ChangesetEntry[]> {
		const snapshot = await getDocs(
			query(this.changesRef, orderBy("ts", "desc"), limit(n)),
		);
		this.markContact();
		const entries: ChangesetEntry[] = [];
		for (const docSnap of snapshot.docs) {
			const entry = this.parseEntry(docSnap);
			if (entry) {
				entries.push(entry);
			}
		}
		return entries.reverse();
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

		const unsub = onSnapshot(
			q,
			(snapshot) => {
				// Any delivery (even an empty one) means the listener is live.
				this.markContact();
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

				syncDebugLog("transport:changes-snapshot", {
					added: entries.length,
					firstSeq: entries[0]!.seq,
					lastSeq: entries[entries.length - 1]!.seq,
				});

				chain = chain.then(async () => {
					for (const entry of entries) {
						await subscriber.onEntry(entry);
					}
					subscriber.onBatchProcessed?.();
				});
			},
			(error) => {
				// A failed listener (e.g. it choked on a huge initial snapshot, or the
				// token expired) must not silently kill sync: the paginated catch-up
				// timer is the backstop that keeps draining regardless.
				console.error("Changes subscription failed", error);
				subscriber.onError?.(error);
			},
		);

		return unsub;
	}
}
